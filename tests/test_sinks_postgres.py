"""SPEC-011 — PostgresSink: transactional chunked insert, projection, rollback/retry (fake conn)."""

from __future__ import annotations

import json
import sys
import types
from typing import Self

import pytest

from log_foundry.sinks.base import Sink, SinkDeliveryError
from log_foundry.sinks.postgres import DEFAULT_CONNECT_TIMEOUT, PostgresSink


class FakeCursor:
    def __init__(self, owner: FakeConnection) -> None:
        self._owner = owner

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None) -> None:
        self._owner.executed.append((sql, params))

    def executemany(self, sql, rows) -> None:
        self._owner.executemany_calls.append((sql, [tuple(r) for r in rows]))
        if self._owner._fail_times != 0:
            if self._owner._fail_times > 0:
                self._owner._fail_times -= 1
            raise RuntimeError("insert failed")


class FakeConnection:
    def __init__(self, fail_times: int = 0) -> None:
        self.executed: list[tuple] = []
        self.executemany_calls: list[tuple] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self._fail_times = fail_times

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    # ``wait`` is bound into each sink at import, and its Event branch never reaches
    # ``time.sleep`` — patching either centrally would leave this fixture inert.
    monkeypatch.setattr("log_foundry.sinks.postgres.wait", lambda _delay, _stop=None: None)


def test_is_a_sink() -> None:
    assert isinstance(PostgresSink("logs", connection=FakeConnection()), Sink)


def test_batch_insert_projects_columns_and_commits() -> None:
    conn = FakeConnection()
    PostgresSink("logs", connection=conn).emit(
        [
            {
                "timestamp": "t",
                "level": "INFO",
                "trace_id": "tr",
                "span_id": "sp",
                "function": "fn",
                "service": "svc",
                "extra": 1,
            }
        ]
    )
    sql, rows = conn.executemany_calls[0]
    assert "INSERT INTO logs" in sql
    assert "%s::jsonb" in sql
    row = rows[0]
    assert row[:6] == ("t", "INFO", "tr", "sp", "fn", "svc")
    assert json.loads(row[6])["extra"] == 1
    assert conn.commits == 1


def test_missing_keys_become_none() -> None:
    conn = FakeConnection()
    PostgresSink("logs", connection=conn).emit([{"level": "INFO"}])
    row = conn.executemany_calls[0][1][0]
    assert row[:6] == (None, "INFO", None, None, None, None)


def test_chunks_within_one_transaction() -> None:
    conn = FakeConnection()
    PostgresSink("logs", connection=conn, chunk_size=2).emit([{"i": i} for i in range(5)])
    assert [len(rows) for _sql, rows in conn.executemany_calls] == [2, 2, 1]
    assert conn.commits == 1  # a single transaction spans all chunks


def test_rollback_and_retry_then_succeed() -> None:
    conn = FakeConnection(fail_times=1)
    sink = PostgresSink("logs", connection=conn, max_retries=2)
    sink.emit([{"a": 1}])
    assert conn.rollbacks == 1
    assert conn.commits == 1
    assert sink.failed == 0


def test_persistent_failure_counted() -> None:
    conn = FakeConnection(fail_times=-1)
    sink = PostgresSink("logs", connection=conn, max_retries=1)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}, {"a": 2}])  # the transaction rolled back: nothing was inserted
    assert conn.rollbacks == 2
    assert conn.commits == 0
    assert sink.failed == 2


def test_create_table_runs_ddl() -> None:
    conn = FakeConnection()
    PostgresSink("logs", connection=conn, create_table=True)
    assert any("CREATE TABLE IF NOT EXISTS logs" in sql for sql, _ in conn.executed)


def test_invalid_table_name_raises() -> None:
    with pytest.raises(ValueError):
        PostgresSink("logs; DROP TABLE x", connection=FakeConnection())


def test_close_commits_injected_but_does_not_close() -> None:
    conn = FakeConnection()
    sink = PostgresSink("logs", connection=conn)
    sink.close()
    assert conn.commits == 1
    assert conn.closed is False
    sink.close()  # idempotent


def test_owned_connection_is_closed(monkeypatch) -> None:
    conn = FakeConnection()
    monkeypatch.setitem(
        sys.modules, "psycopg", types.SimpleNamespace(connect=lambda dsn, **kwargs: conn)
    )
    sink = PostgresSink("logs", dsn="postgresql://x")
    sink.close()
    assert conn.closed is True


# -- SPEC-029 FR-002: the diagnostic must not reprint the event ------------------------------


class LeakyDriverError(Exception):
    """A psycopg-shaped error: its ``repr`` reprints the statement *and* its bound parameters.

    Not an exaggeration of the real thing — psycopg's ``DiagnosticsMixin`` errors routinely carry
    the failing query, and ``_row`` binds the whole ``json.dumps(event)`` as a parameter. Under the
    old line the diagnostic for a failed insert reprinted the event, PII included.
    """

    def __init__(self, statement: str, params: object) -> None:
        super().__init__(f"insert failed: {statement} with params {params!r}")
        self.statement = statement
        self.params = params

    def __repr__(self) -> str:
        return f"LeakyDriverError({self.args[0]!r})"


class LeakyCursor(FakeCursor):
    def executemany(self, sql, rows) -> None:
        self._owner.executemany_calls.append((sql, [tuple(r) for r in rows]))
        raise LeakyDriverError(sql, list(rows))


class LeakyConnection(FakeConnection):
    def cursor(self) -> LeakyCursor:
        return LeakyCursor(self)


def test_an_abandoned_insert_never_reprints_the_event(capsys) -> None:
    """The leak the type-name rule prevents, stated as the test that would have caught it."""
    conn = LeakyConnection()
    sink = PostgresSink("logs", connection=conn, max_retries=0)

    with pytest.raises(SinkDeliveryError):
        sink.emit(
            [{"message": "hi", "fields": {"email": "user@example.com", "card": "4111111111111"}}]
        )

    err = capsys.readouterr().err
    assert "user@example.com" not in err, "the event must not reach stderr through a driver repr"
    assert "4111111111111" not in err
    assert "INSERT INTO" not in err, "nor the statement the event was bound to"
    assert "insert failed:" not in err, "nor the exception's own message"

    assert "insert failed" not in err, "nor through the SinkDeliveryError this now raises"
    assert "LeakyDriverError" in err, "the type is what an operator gets, and is enough"
    assert "lost 1 event(s)" in err, "and the count it cost"
    assert sink.failed == 1
    assert err.count("\n") == 1


# --- SPEC-038 FR-002: a rollback that raises must not hijack the error path ---------------


class RollbackRaisesConnection(FakeConnection):
    """A connection whose session the server has closed: the insert fails, and so does the undo.

    This is the common case, not a contrived one -- when the server closes the session, psycopg
    raises from `rollback()` for the same reason it raised from the insert. AC-3 asks for exactly
    this double, so the FR closes without a running Postgres.
    """

    def __init__(self) -> None:
        super().__init__(fail_times=-1)

    def rollback(self) -> None:
        # A *distinct* type from the insert's RuntimeError, so the assertions below can tell
        # which exception the diagnostic and the raise actually describe. With both the same
        # type the tests passed whichever one won, which is no test at all.
        self.rollbacks += 1
        raise TypeError("the server closed this session")


def test_a_rollback_that_raises_does_not_consume_the_remaining_attempts() -> None:
    """AC-1. Measured before the fix: attempts dropped from 4 to 1."""
    conn = RollbackRaisesConnection()
    sink = PostgresSink("logs", connection=conn, max_retries=3)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"i": 1}])
    assert len(conn.executemany_calls) == 4, "one initial attempt plus three retries"
    assert conn.rollbacks == 4, "each failed attempt still tries to undo its transaction"


def test_a_total_failure_still_counts_announces_and_raises_the_sink_error(capsys) -> None:
    """AC-2. Before the fix: `failed` stayed 0, no `lost` line, and a raw RuntimeError escaped."""
    conn = RollbackRaisesConnection()
    sink = PostgresSink("logs", connection=conn, max_retries=1)
    with pytest.raises(SinkDeliveryError, match="inserted none of 3 event"):
        sink.emit([{"i": i} for i in range(3)])
    assert sink.failed == 3
    assert sink.losses().failed == 3
    err = capsys.readouterr().err
    assert "lost 3 event(s)" in err
    assert "RuntimeError" in err, "the diagnostic names the *insert's* failure"
    assert "TypeError" not in err.split("lost 3 event(s)")[1], (
        "the rollback's own failure must not displace the one being handled"
    )
    assert "the server closed this session" not in err, "the type, never the driver's message"


def test_the_failing_rollback_is_announced_as_absorbed_by_type(capsys) -> None:
    conn = RollbackRaisesConnection()
    sink = PostgresSink("logs", connection=conn, max_retries=0)
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"i": 1}])
    err = capsys.readouterr().err
    assert "PostgresSink.rollback" in err
    assert "TypeError" in err, "named by the rollback's own type, distinct from the insert's"
    assert "closed this session" not in err


def test_the_original_insert_failure_is_what_the_diagnostic_names() -> None:
    """The rollback's exception must not displace the failure actually being handled."""
    conn = RollbackRaisesConnection()
    sink = PostgresSink("logs", connection=conn, max_retries=0)
    with pytest.raises(SinkDeliveryError) as caught:
        sink.emit([{"i": 1}])
    assert caught.value.__cause__ is None, "raised `from None`, as before"


def test_a_healthy_rollback_path_is_unchanged() -> None:
    """The guard must not change what happens when the rollback works, which is the normal case."""
    conn = FakeConnection(fail_times=1)
    sink = PostgresSink("logs", connection=conn, max_retries=2)
    sink.emit([{"i": 1}])
    assert conn.rollbacks == 1 and conn.commits == 1 and sink.failed == 0


# -- SPEC-041 FR-002: a broken connection is replaced, and only when this sink owns it --------


class BreakableConnection(FakeConnection):
    """A connection that reports itself broken once the server has "closed" it."""

    def __init__(self, fail_times: int = 0) -> None:
        super().__init__(fail_times)
        self.broken = False

    def kill(self) -> None:
        self.broken = True
        self._fail_times = -1   # every later insert fails, as a dead handle's does


def _psycopg_stub(monkeypatch, connections):
    """Installs a fake `psycopg` handing out `connections` in order, recording the kwargs."""
    seen: list[dict] = []

    def connect(dsn, **kwargs):
        seen.append(kwargs)
        return connections.pop(0)

    monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(connect=connect))
    return seen


def test_a_broken_owned_connection_is_reopened_on_the_next_attempt(monkeypatch) -> None:
    first, second = BreakableConnection(), FakeConnection()
    _psycopg_stub(monkeypatch, [first, second])
    sink = PostgresSink("logs", dsn="postgresql://x", max_retries=0)

    first.kill()
    sink.emit([{"a": 1}])          # max_retries=0: one attempt, and it must still recover

    assert sink._conn is second, "a broken owned connection must be replaced"
    assert second.executemany_calls, "the batch must land on the new connection"


def test_a_broken_borrowed_connection_is_never_reopened(monkeypatch) -> None:
    borrowed, replacement = BreakableConnection(), FakeConnection()
    # A reconnect must be able to SUCCEED here, or the test proves nothing: an earlier version
    # removed `psycopg` from sys.modules so a reconnect would raise, be absorbed, and leave
    # `sink._conn` pointing at the borrowed object either way -- it passed against its own
    # mutation. The signal only exists when the guard is the reason nothing happened.
    seen = _psycopg_stub(monkeypatch, [replacement])
    sink = PostgresSink("logs", connection=borrowed, max_retries=0)
    assert seen == [], "an injected connection must not open one at construction"

    borrowed.kill()
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])

    assert sink._conn is borrowed, "a caller's connection is theirs (arch §13)"
    # The harm the identity assertion alone does not catch: closing the caller's object out from
    # under them. Measured against the unguarded version -- `borrowed.closed` went True.
    assert borrowed.closed is False, "a borrowed connection must not be closed by this sink"
    assert not replacement.executemany_calls, "and nothing may be routed to a replacement"


def test_a_broken_owned_connection_is_closed_before_it_is_replaced(monkeypatch) -> None:
    # Without this, a flapping server leaves one abandoned psycopg connection -- and its fd --
    # behind per reconnect, unbounded. The mutant (deleting the close) survived the whole module.
    first, second = BreakableConnection(), FakeConnection()
    _psycopg_stub(monkeypatch, [first, second])
    sink = PostgresSink("logs", dsn="postgresql://x", max_retries=0)

    first.kill()
    sink.emit([{"a": 1}])

    assert sink._conn is second
    assert first.closed is True, "the dead connection must be released, not abandoned"


def test_the_documented_default_connect_timeout_is_the_one_used(monkeypatch) -> None:
    # The default is quoted in the class docstring's worst case ("20 s more at the defaults"),
    # so it is pinned rather than left to drift with the constant.
    seen = _psycopg_stub(monkeypatch, [FakeConnection()])
    PostgresSink("logs", dsn="postgresql://x")

    assert seen == [{"connect_timeout": DEFAULT_CONNECT_TIMEOUT}]
    assert DEFAULT_CONNECT_TIMEOUT == 5


def test_the_connect_timeout_is_passed_and_floored(monkeypatch) -> None:
    seen = _psycopg_stub(monkeypatch, [FakeConnection(), FakeConnection()])
    PostgresSink("logs", dsn="postgresql://x", connect_timeout=30)
    # Zero means "wait forever" to libpq, which is the unbounded connect this argument exists to
    # remove, so it is floored rather than honoured.
    PostgresSink("logs", dsn="postgresql://x", connect_timeout=0)

    assert [kwargs["connect_timeout"] for kwargs in seen] == [30, 2]


def test_close_does_not_raise_when_the_connection_is_already_broken(monkeypatch) -> None:
    conn = BreakableConnection()
    _psycopg_stub(monkeypatch, [conn])
    sink = PostgresSink("logs", dsn="postgresql://x")

    def explode() -> None:
        raise RuntimeError("the server closed this session")

    conn.commit = explode
    sink.close()                   # must not raise out of a release path

    assert conn.closed is True, "the connection is still released"


def test_a_failing_reconnect_is_announced_once_per_outage_not_per_attempt(
    monkeypatch, capsys
) -> None:
    # Unthrottled this fires on every attempt of every batch: a down server turned one stderr
    # line into five per batch, indefinitely.
    conn = BreakableConnection()

    def connect(dsn, **kwargs):
        if getattr(connect, "opened", False):
            raise RuntimeError("server is down")
        connect.opened = True
        return conn

    monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(connect=connect))
    sink = PostgresSink("logs", dsn="postgresql://x", max_retries=3)
    conn.kill()

    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 2}])

    lines = capsys.readouterr().err.splitlines()
    assert sum("PostgresSink.reconnect" in line for line in lines) == 1, lines


def test_either_broken_signal_alone_triggers_the_reconnect(monkeypatch) -> None:
    # The docstring argues for probing BOTH `closed` and `broken`. psycopg3 defines them in terms
    # of the same underlying status, so the redundancy is for the "psycopg-shaped" objects this
    # sink also accepts -- an injected double may publish only one. Pinned so the claim is not
    # merely asserted: dropping either probe reddens one of these.
    for attribute in ("closed", "broken"):
        conn, replacement = FakeConnection(), FakeConnection()
        conn._fail_times = -1
        setattr(conn, attribute, True)
        _psycopg_stub(monkeypatch, [conn, replacement])
        sink = PostgresSink("logs", dsn="postgresql://x", max_retries=0)
        sink.emit([{"a": 1}])
        assert sink._conn is replacement, f"a connection reporting {attribute} must be replaced"


def test_a_later_outage_is_announced_again_after_a_successful_reconnect(
    monkeypatch, capsys
) -> None:
    # The half of the throttle that fails SILENTLY. Without the reset, the first outage announces
    # once and every later one for the life of the process is silent -- no counter moves, nothing
    # reddens. Deleting the reset survived the entire 1779-test suite before this test existed.
    connections = [BreakableConnection(), BreakableConnection()]
    state = {"down": False}

    def connect(dsn, **kwargs):
        if state["down"]:
            raise RuntimeError("server is down")
        return connections.pop(0)

    monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(connect=connect))
    sink = PostgresSink("logs", dsn="postgresql://x", max_retries=0)

    # First outage: the reconnect fails and is announced.
    sink._conn.kill()
    state["down"] = True
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 1}])
    assert capsys.readouterr().err.count("PostgresSink.reconnect") == 1

    # Recovery, then a second, distinct outage: it must be announced too.
    state["down"] = False
    sink._conn.kill()
    sink.emit([{"a": 2}])
    capsys.readouterr()
    sink._conn.kill()
    state["down"] = True
    with pytest.raises(SinkDeliveryError):
        sink.emit([{"a": 3}])

    assert capsys.readouterr().err.count("PostgresSink.reconnect") == 1, (
        "a new outage after a recovery must be announced again"
    )


def test_the_borrowed_clause_appears_only_when_the_connection_is_broken(
    monkeypatch, capsys
) -> None:
    # The clause tells an operator why delivery stopped. Appending it to every borrowed-connection
    # failure puts "may not reopen it" on a constraint violation, where reopening was never the
    # question. Reverting the condition survived the whole suite before this test existed.
    monkeypatch.delitem(sys.modules, "psycopg", raising=False)

    ordinary = FakeConnection(fail_times=-1)          # fails, but the connection is fine
    with pytest.raises(SinkDeliveryError):
        PostgresSink("logs", connection=ordinary, max_retries=0).emit([{"a": 1}])
    assert "may not reopen it" not in capsys.readouterr().err

    broken = BreakableConnection()
    broken.kill()
    with pytest.raises(SinkDeliveryError):
        PostgresSink("logs", connection=broken, max_retries=0).emit([{"a": 1}])
    assert "may not reopen it" in capsys.readouterr().err
