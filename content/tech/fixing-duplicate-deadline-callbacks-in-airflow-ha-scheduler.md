+++
date = '2026-04-06'
draft = false
title = "Fixing duplicate deadline callbacks in Airflow's HA scheduler"
description = "A three-line fix, a subtle Postgres lock mode, and why HA bugs hide in single-session tests."
+++

Apache Airflow supports running multiple scheduler replicas for high availability. The idea is simple — if one scheduler dies, another takes over. No downtime, no lost work. Most of Airflow's critical paths (DAG parsing, task scheduling, trigger processing) are carefully designed to coordinate safely across replicas using row-level locks.

Most of them.

Last week I shipped [a fix](https://github.com/apache/airflow/pull/64737) for one that wasn't. The scheduler's deadline-check loop was missing a `FOR UPDATE` lock, which meant two schedulers could pick up the same expired deadline at the same time and fire the breach callback twice. For DAGs that alert on-call when a deadline is missed, you'd page your engineer twice. For DAGs that auto-remediate, you'd run the remediation twice. Neither is great.

Here's how I found it and fixed it.

## What deadlines are, briefly

Airflow has a feature called DAG run deadlines. You attach a deadline to a DAG run, and if the run takes longer than the deadline, Airflow fires a callback — typically an alert. Under the hood, a row is created in a `deadline` table, and the scheduler periodically scans for expired rows and processes them.

The processing loop lives in `scheduler_job_runner.py` and looks roughly like this:

```python
expired_deadlines = (
    session.query(Deadline)
    .filter(Deadline.deadline_time < utcnow(), Deadline.callback_state.is_(None))
    .all()
)

for deadline in expired_deadlines:
    deadline.handle_miss(session)
```

Read the expired rows, call `handle_miss` on each one. `handle_miss` creates a `Trigger` record that eventually runs the breach callback.

Simple, clear, and completely broken under HA.

## The race condition

Picture two scheduler replicas running against the same Postgres instance. Both are executing this loop on a schedule. The timeline:

```
t=0  Scheduler A: SELECT expired deadlines → finds row 42
t=1  Scheduler B: SELECT expired deadlines → finds row 42 (same row!)
t=2  Scheduler A: handle_miss(row 42) → creates Trigger
t=3  Scheduler B: handle_miss(row 42) → creates ANOTHER Trigger
t=4  Both triggers eventually fire the deadline breach callback
```

There's no lock, no coordination, nothing stopping both schedulers from processing the same row. `handle_miss` itself doesn't check whether a trigger already exists. So you get two callbacks where you expected one.

The bug report ([#64710](https://github.com/apache/airflow/issues/64710)) described this as "duplicate deadline callbacks under HA" — a vague symptom. The root cause only becomes obvious once you know how Airflow protects other parts of the scheduler under HA.

## How the rest of Airflow handles this

Other scheduler loops that read and mutate shared state — task queueing, trigger processing, DAG parsing — all use a helper called `with_row_locks`. It wraps a SQLAlchemy query and adds a `SELECT ... FOR UPDATE SKIP LOCKED` clause.

`FOR UPDATE` is a row-level lock that says "I'm about to modify these rows, hold them for me until I commit." `SKIP LOCKED` says "if another transaction already has a row locked, don't wait for it — just skip it and move on." Combined, it means each row is processed by exactly one scheduler, and no scheduler blocks on another.

The deadline loop was the only one that had forgotten this pattern.

## The fix

The fix is mechanically small:

```python
expired_deadlines = (
    with_row_locks(
        session.query(Deadline).filter(
            Deadline.deadline_time < utcnow(),
            Deadline.callback_state.is_(None),
        ),
        of=Deadline,
        skip_locked=True,
        key_share=False,
    )
    .all()
)
```

Three keyword arguments and the problem goes away. But one of them deserves a paragraph.

## Why `key_share=False` matters

`with_row_locks` defaults to `FOR KEY SHARE` on Postgres. `FOR KEY SHARE` is a weaker lock — it only blocks locks that would modify the primary key or foreign keys. Crucially, **it does not conflict with itself**. Two transactions can both hold `FOR KEY SHARE` on the same row at the same time.

That's the whole bug, one level down. If I had just passed `skip_locked=True` and left the default, both schedulers would still have been able to acquire `FOR KEY SHARE` on the same deadline row, `SKIP LOCKED` wouldn't have triggered because neither transaction was blocked, and the duplicate callback would still have fired.

`key_share=False` downgrades the lock to a real `FOR UPDATE`, which *does* conflict with itself. Now when Scheduler A holds the lock, Scheduler B's `SKIP LOCKED` sees it and moves on.

This is the kind of subtlety that makes HA bugs so gnarly. The fix is three lines, but if you get any of them wrong, it looks like it's working until production proves otherwise.

## Writing a test that actually reproduces HA

The hardest part of the PR was writing a regression test. Unit tests run in a single process against a single database session — the whole point of the bug is that it requires two *independent* sessions contending for the same row.

I solved it by opening a second, unscoped session inside the test:

```python
from airflow.utils.session import create_session

def test_expired_deadline_locked_by_other_scheduler_is_skipped(session, dag_maker):
    # Arrange: create an expired deadline
    deadline = make_expired_deadline(...)
    session.add(deadline)
    session.commit()

    # Simulate a second scheduler holding the row lock
    with create_session(scoped=False) as other_session:
        other_session.query(Deadline).filter(
            Deadline.id == deadline.id
        ).with_for_update().one()

        # Now run the real scheduler loop in the main session
        with mock.patch.object(Deadline, "handle_miss") as mock_handle:
            scheduler_job_runner._check_deadlines(session)

        # Assert: handle_miss was never called, because the row was locked
        mock_handle.assert_not_called()
```

The `scoped=False` bit is important. Airflow's default session is scoped to the current thread, so `create_session()` in the same thread gives you the *same* connection — which means no contention. `scoped=False` gets you a fresh connection so the lock actually exists across two distinct transactions.

I verified the test fails without the fix (`Expected 'handle_miss' to not have been called. Called 1 times`) and passes with the fix applied. The test is skipped on SQLite because SQLite is a single-writer database that doesn't support row locking — there's no way to simulate HA on it.

## What I learned

A few things stuck with me from this one:

**HA bugs hide in non-HA tests.** Every scheduler loop that touches shared state under HA needs a test that simulates *two* schedulers. Existing tests ran both queries in the same session and silently passed even though production was broken. If the test can't express contention, it can't catch the bug.

**Defaults are opinions.** `with_row_locks` defaults to `FOR KEY SHARE` for good reason in most places — it's cheaper and safer for reads. But the moment you're using it for mutual exclusion, you need to know about `key_share=False`. An API that makes the common case easy and the uncommon case invisible is a bug generator.

**The interesting part of a PR is rarely the diff.** The diff here is small enough to fit in a tweet. The interesting part is: why didn't anyone catch this? How did the surrounding codebase handle the same problem correctly? What does a test look like when it has to reproduce concurrency? That's the stuff worth writing down.

## Links

- **The PR:** [apache/airflow#64737](https://github.com/apache/airflow/pull/64737)
- **The issue:** [apache/airflow#64710](https://github.com/apache/airflow/issues/64710)
- **`with_row_locks`:** [airflow/utils/sqlalchemy.py](https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/utils/sqlalchemy.py)
- **Postgres lock modes:** [PostgreSQL docs](https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS)
