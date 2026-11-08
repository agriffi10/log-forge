"""SPEC-073: the HTTP family on the delivery engine, against scripted openers.

FR-001's rounds, budgets, classification and hook guards; FR-002's counters through the run
context; FR-004's eligibility of a third-party subclass; and FR-005's twin paths and forms over
every shipped class. The engine's run context itself is proven in ``tests/test_run_context.py``.
"""

from __future__ import annotations

import ast
import json
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest

import log_foundry
from log_foundry.sinks import _engine, _retry
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.http import HTTPSink, _PayloadTooLarge
from log_foundry.sinks.sentry import SentrySink
from log_foundry.sinks.verdict import Outcome, Verdict, read_deliver
from log_foundry.worker import Worker
from test_public_surface import _http_sink_subclasses
from test_sinks_http import FakeOpener, FakeResponse

A, R, J, D, U = (
    Outcome.ACCEPTED,
    Outcome.RETRYABLE,
    Outcome.REJECTED,
    Outcome.DROPPED,
    Outcome.UNKNOWN,
)
_REAL_WAIT = _retry.wait
_SINKS = Path(log_foundry.__file__).parent / "sinks"


@pytest.fixture(autouse=True)
def waits(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Records every wait the engine takes and returns at once; the timing test restores it."""
    seen: list[float] = []
    monkeypatch.setattr(_retry, "wait", lambda delay, _stop=None: seen.append(delay))
    return seen


def _events(n: int) -> list[dict[str, object]]:
    return [{"message": f"m{i}", "i": i} for i in range(n)]


def _ids(call: dict[str, Any]) -> list[int]:
    """The ``i`` of every event an ndjson request carried."""
    return [json.loads(line)["i"] for line in call["body"].decode().splitlines() if line]


def _lost(err: str) -> list[str]:
    return [line for line in err.splitlines() if "log-foundry: lost" in line]


def _requests_lost(err: str) -> list[str]:
    return [line for line in err.splitlines() if "lost 1 request(s)" in line]


def _through_the_worker(
    sink: Any, batch: list[dict[str, object]], max_retries: int = 3
) -> tuple[bool, int]:
    """Hands ``batch`` to a real worker as one batch; whether it was delivered, and abandonments."""
    worker = Worker(sink, batch_size=len(batch), max_retries=max_retries)
    try:
        worker.submit(batch)
        delivered = bool(worker.flush(timeout=30.0))
        failed_batches = worker.health().failed_batches
    finally:
        worker.stop()
    return delivered, failed_batches


class Scripted:
    """An opener answering each request with the next item of a script, the last repeating.

    An item is a ``FakeResponse``, an exception to raise, or a callable given the call's index
    and the request, returning either. Each call records the request as ``FakeOpener`` does.
    """

    def __init__(self, *script: Any) -> None:
        self.script = list(script)
        self.calls: list[dict[str, Any]] = []
        self.threads: list[int] = []

    def __call__(self, request: urllib.request.Request, timeout: float | None = None) -> Any:
        index = len(self.calls)
        self.calls.append(
            {"body": request.data, "headers": dict(request.header_items()), "url": request.full_url}
        )
        self.threads.append(threading.get_ident())
        item = self.script[min(index, len(self.script) - 1)]
        if callable(item) and not isinstance(item, FakeResponse):
            item = item(index, request)
        if isinstance(item, BaseException):
            raise item
        return item


def _ok() -> FakeResponse:
    return FakeResponse(200, b"")


def _status(code: int, retry_after: str | None = None) -> FakeResponse:
    return FakeResponse(code, b"", {"Retry-After": retry_after} if retry_after else None)


# --- FR-001 1.1: no retry loop, backoff constant or wait left in the family --------------------


def _family_modules() -> dict[str, str]:
    """``http.py`` and every module the family scan finds a subclass of ``HTTPSink`` in."""
    stems = {"http"} | {stem for stem, _ in _http_sink_subclasses()}
    return {stem: (_SINKS / f"{stem}.py").read_text(encoding="utf-8") for stem in sorted(stems)}


def family_wait_findings(sources: dict[str, str]) -> list[str]:
    """Every retry loop, backoff constant and wait call in the family's modules (FR-001 AC-1).

    Args:
      sources: Module text by stem.

    Returns:
      One line per finding.
    """
    findings: list[str] = []
    for stem, source in sources.items():
        for node in ast.walk(ast.parse(source)):
            if (
                isinstance(node, ast.For)
                and isinstance(node.iter, ast.Call)
                and isinstance(node.iter.func, ast.Name)
                and node.iter.func.id == "range"
                and "retries" in ast.unparse(node.iter)
            ):
                findings.append(f"{stem}.py:{node.lineno}: a retry loop")
            if isinstance(node, ast.Name) and node.id == "_BACKOFF_BASE":
                findings.append(f"{stem}.py:{node.lineno}: _BACKOFF_BASE")
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name) and func.id == "wait":
                    findings.append(f"{stem}.py:{node.lineno}: a wait call")
                if isinstance(func, ast.Attribute) and func.attr in {"wait", "sleep"}:
                    findings.append(f"{stem}.py:{node.lineno}: a {func.attr} call")
    return findings


def test_the_family_holds_no_retry_loop_backoff_or_wait() -> None:
    sources = _family_modules()
    assert len(sources) == 8, sorted(sources)
    assert family_wait_findings(sources) == []


@pytest.mark.parametrize(
    ("planted", "finding"),
    [
        ("        for attempt in range(self.max_retries + 1):\n            pass\n", "a retry loop"),
        ("        x = _BACKOFF_BASE\n", "_BACKOFF_BASE"),
        ("        wait(0.1, None)\n", "a wait call"),
        ("        self.log_foundry_stop_signal.wait(1)\n", "a wait call"),
        ("        time.sleep(1)\n", "a sleep call"),
    ],
    ids=["loop", "constant", "wait", "event-wait", "sleep"],
)
def test_the_family_wait_check_names_each_planted_shape(planted: str, finding: str) -> None:
    sources = _family_modules()
    anchor = "        return _engine.one_round(self._round, batch)\n"
    assert sources["http"].count(anchor) == 1
    sources["http"] = sources["http"].replace(anchor, planted + anchor)
    found = family_wait_findings(sources)
    assert len(found) == 1 and found[0].startswith("http.py:") and found[0].endswith(finding)


# --- FR-001 1.2 to 1.9: budgets, resends and classification ------------------------------------


def test_a_default_worker_delivers_on_the_tenth_request() -> None:
    opener = Scripted(*([_status(503)] * 9), _ok())
    sink = HTTPSink("http://h", opener=opener)
    worker = Worker(sink, batch_size=1)
    try:
        worker.submit(_events(1))
        assert worker.flush(timeout=30.0)
        assert worker.health().failed_batches == 0
    finally:
        worker.stop()
    assert len(opener.calls) == 10


@pytest.mark.parametrize("mode", ["recovers", "down"])
def test_a_zero_retry_sink_keeps_the_workers_share(mode: str) -> None:
    opener = Scripted(_status(503), _ok()) if mode == "recovers" else Scripted(_status(503))
    sink = HTTPSink("http://h", max_retries=0, opener=opener)
    delivered, failed_batches = _through_the_worker(sink, _events(1), max_retries=3)
    if mode == "recovers":
        assert delivered and failed_batches == 0
        assert len(opener.calls) == 2
    else:
        assert not delivered and failed_batches == 1
        assert len(opener.calls) == 4


def test_an_orphan_call_retries_on_the_callers_thread() -> None:
    opener = Scripted(_status(503))
    sink = HTTPSink("http://h", max_retries=2, opener=opener)
    log_foundry.configure(service="t", sink=sink)

    log_foundry.info("outside a span")

    assert len(opener.calls) == 3
    assert set(opener.threads) == {threading.get_ident()}
    assert log_foundry.health().orphan_lost == 1


@pytest.mark.parametrize("path", ["emit", "worker"])
def test_a_partial_batch_is_two_requests(path: str) -> None:
    opener = Scripted(_ok(), _status(400))
    sink = HTTPSink("http://h", max_batch_count=1, opener=opener)
    if path == "emit":
        sink.emit(_events(2))
    else:
        delivered, failed_batches = _through_the_worker(sink, _events(2), max_retries=3)
        assert delivered and failed_batches == 0
    assert len(opener.calls) == 2, "fda134a made 2 as well"
    assert sink.failed == 1


def test_a_rejected_batch_is_sent_once_through_a_worker() -> None:
    opener = Scripted(_status(400))
    sink = HTTPSink("http://h", opener=opener)
    delivered, failed_batches = _through_the_worker(sink, _events(1), max_retries=3)
    assert not delivered and failed_batches == 1
    assert len(opener.calls) == 1, "fda134a made 4: the worker resent the raise whole"


@pytest.mark.parametrize("mode", ["recovers", "down"])
def test_emit_retries_within_the_sinks_budget(
    mode: str, waits: list[float], capsys: pytest.CaptureFixture[str]
) -> None:
    opener = Scripted(_status(503), _ok()) if mode == "recovers" else Scripted(_status(503))
    sink = HTTPSink("http://h", max_retries=2, opener=opener)
    if mode == "recovers":
        sink.emit(_events(1))
        assert len(opener.calls) == 2
        assert waits == [0.1]
        assert sink.failed == 0
        assert _lost(capsys.readouterr().err) == []
    else:
        with pytest.raises(SinkDeliveryError):
            sink.emit(_events(1))
        assert len(opener.calls) == 3
        assert sink.failed == 1
        assert _lost(capsys.readouterr().err) == [
            "log-foundry: lost 1 request(s); HTTPSink, 3 attempt(s), HTTP 503"
        ]


def test_a_resend_carries_only_the_retryable_chunk() -> None:
    opener = Scripted(_status(503), _ok(), _ok())
    sink = HTTPSink("http://h", max_batch_count=1, max_retries=1, opener=opener)
    sink.emit(_events(2))
    assert len(opener.calls) == 3
    assert [_ids(call) for call in opener.calls] == [[0], [1], [0]]


@pytest.mark.parametrize("case", ["URLError", "RuntimeError-emit", "RuntimeError-worker"])
def test_an_opener_error_is_classified_by_the_predicate(case: str) -> None:
    if case == "URLError":
        alone = HTTPSink("http://h", opener=Scripted(urllib.error.URLError("refused")))
        assert alone.deliver(_events(1)) == Verdict(outcomes=(R,))
        assert alone.failed == 1, "outside any run the round is its own last"
        opener = Scripted(urllib.error.URLError("refused"), _ok())
        sink = HTTPSink("http://h", max_retries=1, opener=opener)
        sink.emit(_events(1))
        assert len(opener.calls) == 2, "resent, and landed"
        assert sink.failed == 0
        return
    opener = Scripted(RuntimeError("injected"))
    sink = HTTPSink("http://h", max_retries=3, opener=opener)
    if case == "RuntimeError-emit":
        with pytest.raises(SinkDeliveryError):
            sink.emit(_events(1))
    else:
        delivered, failed_batches = _through_the_worker(sink, _events(1), max_retries=3)
        assert not delivered and failed_batches == 1
    assert len(opener.calls) == 1
    assert sink.failed == 1


# --- FR-001 1.10, 1.11: the round's hint and the stop signal -----------------------------------


@pytest.mark.parametrize(
    ("ceiling", "hints", "expected"),
    [
        (1.0, ["2.5"], [1.0]),
        (30.0, ["1", "4"], [4.0]),
        (0, ["100"], [0.1]),
        (-1.0, ["100"], [0.1]),
        (float("nan"), ["100"], [0.1]),
        (float("inf"), ["100"], [100.0]),
    ],
    ids=["clamped", "largest-once", "zero", "negative", "nan", "inf"],
)
def test_a_rounds_wait_is_its_largest_clamped_hint(
    ceiling: float, hints: list[str], expected: list[float], waits: list[float]
) -> None:
    first = [_status(503, hint) for hint in hints]
    opener = Scripted(*first, *([_ok()] * len(hints)))
    sink = HTTPSink(
        "http://h", max_batch_count=1, max_retries=1, max_retry_after=ceiling, opener=opener
    )
    sink.emit(_events(len(hints)))
    assert waits == expected, "one round has one wait"
    assert len(opener.calls) == 2 * len(hints)


def test_a_stop_cuts_a_hinted_wait_inside_emit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Measured through ``tests/test_worker_exit_budget.py``'s helper: wall and CPU both."""
    from test_worker_exit_budget import _measured

    monkeypatch.setattr(_retry, "wait", _REAL_WAIT)
    opener = Scripted(_status(503, "10"), _ok())
    sink = HTTPSink("http://h", max_retries=1, max_retry_after=60.0, opener=opener)
    sink.log_foundry_stop_signal = threading.Event()
    timer = threading.Timer(0.1, sink.log_foundry_stop_signal.set)
    timer.start()
    try:
        elapsed = _measured(lambda: sink.emit(_events(1)))
    finally:
        timer.cancel()
    assert elapsed < 0.5, elapsed
    assert len(opener.calls) == 2, "a shutdown shortens a wait and never skips the round"


# --- FR-001 1.12: the 413 search, against the recorded 1.x sequence ----------------------------

_SEQUENCE_AT_BASE = [
    (413, 0, 39),
    (413, 0, 19),
    (413, 0, 9),
    (200, 0, 4),
    (200, 5, 9),
    (200, 10, 13),
    (200, 14, 17),
    (200, 18, 21),
    (200, 22, 25),
    (200, 26, 29),
    (200, 30, 33),
    (200, 34, 37),
    (200, 38, 39),
]
"""``(status, first, last)`` per request, recorded from ``HTTPSink.emit`` at the build's base.

Recorded at ``1b881f1a`` (``c4e51fbd``'s ``http.py``) with forty ``{"i": n, "message": "small
event"}`` events, ndjson, ``max_batch_bytes=5_000_000`` and a destination refusing any body over
300 bytes; ``git diff fda134a c4e51fbd`` touches no line of the search, so it is ``fda134a``'s
sequence too. A literal, so the expectation is never recomputed from the code under test.
"""


def _refusing(index_answering_503: int | None) -> Scripted:
    def answer(index: int, request: urllib.request.Request) -> FakeResponse:
        if len(request.data or b"") > 300:
            return _status(413)
        if index == index_answering_503:
            return _status(503)
        return _ok()

    return Scripted(answer)


@pytest.mark.parametrize("mode", ["as-recorded", "resent-under-the-found-budget"])
def test_the_413_search_sends_the_1x_requests(mode: str) -> None:
    opener = _refusing(3 if mode != "as-recorded" else None)
    sink = HTTPSink("http://h", max_batch_bytes=5_000_000, max_retries=1, opener=opener)
    sink.emit([{"i": n, "message": "small event"} for n in range(40)])
    sent = [
        (413 if len(call["body"]) > 300 else 200, _ids(call)[0], _ids(call)[-1])
        for call in opener.calls
    ]
    if mode == "as-recorded":
        assert sent == _SEQUENCE_AT_BASE
        return
    assert sent[:13] == _SEQUENCE_AT_BASE, "round one is the 1x sequence, its fourth answered 503"
    assert len(sent) == 14
    assert _ids(opener.calls[13]) == [0, 1, 2, 3, 4], "round two resends only that chunk"
    assert all(len(call["body"]) <= 300 for call in opener.calls[13:]), (
        "round two starts from the budget round one found"
    )


# --- FR-001 1.14, 1.15 and FR-002 2.7: a hook building a request costs its chunk ---------------


class _Faulty(HTTPSink):
    """A subclass whose chosen request-building hook raises for a chunk carrying ``bad``."""

    hook = "_body"

    def _body(self, items: list[Any]) -> tuple[bytes, str]:
        if self.hook == "_body" and any(item.event.get("bad") for item in items):
            raise ValueError("cannot build this body")
        return super()._body(items)

    def _prepare(
        self, body: bytes, content_type: str, extra_headers: dict[str, str] | None
    ) -> tuple[dict[str, str], bytes]:
        if self.hook == "_prepare" and b'"bad"' in body:
            raise ValueError("cannot prepare this body")
        return super()._prepare(body, content_type, extra_headers)

    def _apply_auth(self, headers: dict[str, str]) -> None:
        if self.hook == "_apply_auth" and headers.get("X-Bad"):
            raise ValueError("cannot authorise this request")
        super()._apply_auth(headers)


_FAULTY_HOOKS = ["_body", "_prepare", "_apply_auth", "Request"]


def _faulty(hook: str, opener: Any, monkeypatch: pytest.MonkeyPatch) -> _Faulty:
    sink = _Faulty("http://h", max_batch_count=1, opener=opener)
    sink.hook = hook
    if hook == "_apply_auth":
        original = sink._prepare

        def marking(
            body: bytes, content_type: str, extra_headers: dict[str, str] | None
        ) -> tuple[dict[str, str], bytes]:
            return original(body, content_type, {"X-Bad": "1"} if b'"bad"' in body else {})

        monkeypatch.setattr(sink, "_prepare", marking)
    if hook == "Request":
        real = urllib.request.Request

        def refusing(url: str, data: bytes | None = None, **kwargs: Any) -> Any:
            if data and b'"bad"' in data:
                raise ValueError("cannot build this Request")
            return real(url, data=data, **kwargs)

        monkeypatch.setattr("log_foundry.sinks.http.urllib.request.Request", refusing)
    return sink


@pytest.mark.parametrize("form", ["deliver", "emit"])
@pytest.mark.parametrize("hook", _FAULTY_HOOKS)
def test_a_hook_building_a_request_costs_its_chunk(
    hook: str, form: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    opener = Scripted(_ok())
    sink = _faulty(hook, opener, monkeypatch)
    batch = [{"i": 0}, {"i": 1, "bad": True}, {"i": 2}]
    if form == "deliver":
        assert sink.deliver(batch) == Verdict(outcomes=(A, D, A))
    else:
        sink.emit(batch)
    assert len(opener.calls) == 2, "the other two chunks are sent"
    assert sink.losses() == SinkLosses(dropped=1, failed=0)
    assert _lost(capsys.readouterr().err) == [
        "log-foundry: lost 1 event(s); _Faulty, rendering raised ValueError"
    ]


def test_a_batch_whose_every_body_raises_settles_counted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """FR-002 move 3: no request, ``dropped == n`` with one line, settled, ``flush()`` truthy."""
    opener = Scripted(_ok())
    sink = _faulty("_body", opener, monkeypatch)
    delivered, failed_batches = _through_the_worker(sink, [{"i": n, "bad": True} for n in range(3)])
    assert delivered and failed_batches == 0
    assert opener.calls == []
    assert sink.losses() == SinkLosses(dropped=3, failed=0)
    assert _lost(capsys.readouterr().err) == [
        "log-foundry: lost 3 event(s); _Faulty, rendering raised ValueError"
    ]


# --- FR-001 1.16: _send keeps its 1.x contract -------------------------------------------------


@pytest.mark.parametrize("case", ["400", "503", "413", "413-splittable"])
def test_send_keeps_its_1x_contract(case: str, capsys: pytest.CaptureFixture[str]) -> None:
    status = {"400": 400, "503": 503, "413": 413, "413-splittable": 413}[case]
    opener = Scripted(_status(status))
    sink = HTTPSink("http://h", max_retries=2, opener=opener)
    if case == "413-splittable":
        with pytest.raises(_PayloadTooLarge) as info:
            sink._send(b"x" * 10, content_type="text/plain", splittable=True)
        assert info.value.size == 10
        assert sink.failed == 0 and _lost(capsys.readouterr().err) == []
        assert len(opener.calls) == 1
        return
    with pytest.raises(SinkDeliveryError) as raised:
        sink._send(b"x", content_type="text/plain")
    assert type(raised.value) is SinkDeliveryError
    attempts = 3 if case == "503" else 1
    assert len(opener.calls) == attempts
    assert sink.failed == 1
    detail = f"HTTPSink, {attempts} attempt(s), HTTP {status}"
    assert str(raised.value) == detail
    assert _lost(capsys.readouterr().err) == [f"log-foundry: lost 1 request(s); {detail}"]


def test_send_reraises_an_unexpected_error_by_identity(capsys: pytest.CaptureFixture[str]) -> None:
    error = RuntimeError("injected")
    opener = Scripted(error)
    sink = HTTPSink("http://h", max_retries=3, opener=opener)
    with pytest.raises(RuntimeError) as raised:
        sink._send(b"x", content_type="text/plain", count_unexpected=False)
    assert raised.value is error
    assert len(opener.calls) == 1
    assert sink.failed == 0
    assert capsys.readouterr().err == ""


def test_send_returns_the_2xx_payload_and_counts_into_no_event_totals() -> None:
    opener = Scripted(_status(503), FakeResponse(200, b"payload"))
    sink = HTTPSink("http://h", max_retries=1, opener=opener)
    assert sink._send(b"x", content_type="text/plain") == b"payload"
    assert sink.failed == 0
    assert sink.log_foundry_counters.losses() == SinkLosses(dropped=0, failed=0)


# --- FR-001 1.17, 1.20: the request builders and SentrySink -----------------------------------

_BUILDERS = ("_prepare", "_apply_auth", "_send")


def builder_override_findings(sources: dict[str, str]) -> list[str]:
    """Every shipped family subclass that overrides a request builder (FR-001 AC).

    Args:
      sources: Module text by stem.

    Returns:
      One line per override.
    """
    return [
        f"{stem}.{node.name}.{item.name}"
        for stem, source in sources.items()
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ClassDef) and node.name != "HTTPSink"
        for item in node.body
        if isinstance(item, ast.FunctionDef) and item.name in _BUILDERS
    ]


def test_no_family_class_overrides_a_request_builder() -> None:
    assert builder_override_findings(_family_modules()) == []


def test_the_builder_check_names_a_planted_override() -> None:
    sources = _family_modules()
    sources["datadog"] += (
        "\n\nclass Planted(DatadogSink):\n    def _prepare(self, *a):\n        pass\n"
    )
    assert builder_override_findings(sources) == ["datadog.Planted._prepare"]


class _Client:
    """An SDK client stand-in that can deliver."""

    transport = object()

    def capture_event(self, event: dict[str, object]) -> None:
        return None


@pytest.mark.parametrize("backend", ["http", "sdk"])
def test_sentry_offers_no_deliver(backend: str) -> None:
    if backend == "http":
        sink = SentrySink("http://k@sentry.local/1", backend="http", opener=FakeOpener())
    else:
        sink = SentrySink(client=_Client())
    assert read_deliver(sink) is None


# --- SPEC-070's four loop sweeps, re-expressed against HTTPSink.emit ---------------------------


def test_a_type_error_is_one_attempt_counted_as_exhaustion_counts(
    waits: list[float], capsys: pytest.CaptureFixture[str]
) -> None:
    exhausted = HTTPSink("http://h", max_retries=0, opener=Scripted(ConnectionError("down")))
    with pytest.raises(SinkDeliveryError):
        exhausted.emit(_events(1))
    capsys.readouterr()
    opener = Scripted(TypeError("the caller's own code"))
    sink = HTTPSink("http://h", max_retries=3, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink.emit(_events(1))
    assert len(opener.calls) == 1 and waits == []
    assert sink.losses() == exhausted.losses()
    lines = _lost(capsys.readouterr().err)
    assert lines == [
        "log-foundry: lost 1 request(s); HTTPSink, 1 attempt(s), connection error, TypeError"
    ]


def test_an_accepted_exception_is_attempted_max_retries_plus_one(
    waits: list[float], capsys: pytest.CaptureFixture[str]
) -> None:
    opener = Scripted(ConnectionError("down"))
    sink = HTTPSink("http://h", max_retries=2, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink.emit(_events(1))
    assert len(opener.calls) == 3 and len(waits) == 2
    lines = _lost(capsys.readouterr().err)
    assert len(lines) == 1 and "3 attempt(s)" in lines[0], lines


def test_a_keyboard_interrupt_propagates_and_moves_no_counter() -> None:
    opener = Scripted(KeyboardInterrupt())
    sink = HTTPSink("http://h", max_retries=3, opener=opener)
    with pytest.raises(KeyboardInterrupt):
        sink.emit(_events(1))
    assert len(opener.calls) == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_a_predicate_that_raises_fails_at_once_naming_the_original(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    opener = Scripted(ConnectionError("down"))
    sink = HTTPSink("http://h", max_retries=3, opener=opener)

    def broken(err: Exception) -> bool:
        raise LookupError("the predicate broke")

    monkeypatch.setattr(sink, "_retryable_error", broken)
    with pytest.raises(SinkDeliveryError):
        sink.emit(_events(1))
    assert len(opener.calls) == 1
    lines = _lost(capsys.readouterr().err)
    assert len(lines) == 1 and "ConnectionError" in lines[0] and "LookupError" not in lines[0]


# --- FR-002: the family's counters keep their 1.x names and units ------------------------------


@pytest.mark.parametrize("case", ["emit", "worker", "three-chunks"])
def test_failed_counts_abandoned_requests_of_the_last_round(
    case: str, capsys: pytest.CaptureFixture[str]
) -> None:
    opener = Scripted(_status(503))
    if case == "three-chunks":
        sink = HTTPSink("http://h", max_batch_count=1, max_retries=2, opener=opener)
        with pytest.raises(SinkDeliveryError):
            sink.emit(_events(3))
        assert sink.failed == 3
        assert (
            _requests_lost(capsys.readouterr().err)
            == ["log-foundry: lost 1 request(s); HTTPSink, 3 attempt(s), HTTP 503"] * 3
        )
        return
    sink = HTTPSink("http://h", max_retries=2, opener=opener)
    if case == "emit":
        with pytest.raises(SinkDeliveryError):
            sink.emit(_events(1))
        assert len(opener.calls) == 3
        assert sink.failed == 1, "as at fda134a"
        return
    delivered, failed_batches = _through_the_worker(sink, _events(1), max_retries=3)
    assert not delivered and failed_batches == 1
    assert len(opener.calls) == 12, "as at fda134a"
    assert sink.failed == 1, "fda134a left 4: one per worker attempt"
    assert _requests_lost(capsys.readouterr().err) == [
        "log-foundry: lost 1 request(s); HTTPSink, 12 attempt(s), HTTP 503"
    ]


def test_a_late_400_names_its_round(capsys: pytest.CaptureFixture[str]) -> None:
    opener = Scripted(_status(503), _status(503), _status(400))
    sink = HTTPSink("http://h", max_retries=2, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink.emit(_events(1))
    assert sink.failed == 1
    assert _lost(capsys.readouterr().err) == [
        "log-foundry: lost 1 request(s); HTTPSink, 3 attempt(s), HTTP 400"
    ]


def _oversized_pair() -> list[dict[str, object]]:
    return [{"i": 0, "big": "x" * 400}, {"i": 1}]


@pytest.mark.parametrize("beside", ["delivered", "rejected"])
def test_an_oversized_event_counts_only_when_the_run_settles(
    beside: str, capsys: pytest.CaptureFixture[str]
) -> None:
    opener = Scripted(_ok() if beside == "delivered" else _status(400))
    sink = HTTPSink("http://h", max_batch_bytes=200, opener=opener)
    if beside == "delivered":
        sink.emit(_oversized_pair())
        assert sink.dropped_oversized == 1
        lines = _lost(capsys.readouterr().err)
        assert lines == [
            (
                "log-foundry: lost 1 event(s); HTTPSink, up to 420 bytes exceeds the 200-byte "
                "per-event ceiling"
            )
        ]
        return
    with pytest.raises(SinkDeliveryError):
        sink.emit(_oversized_pair())
    assert sink.dropped_oversized == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


@pytest.mark.parametrize("beside", ["rejected-worker", "delivered"])
def test_a_lone_413_counts_only_when_the_run_settles(beside: str) -> None:
    second = _status(400) if beside == "rejected-worker" else _ok()
    opener = Scripted(_status(413), second)
    sink = HTTPSink("http://h", max_batch_count=1, max_retries=0, opener=opener)
    batch = [{"i": 0, "big": "x" * 2000}, {"i": 1}]
    if beside == "delivered":
        sink.emit(batch)
        assert sink.dropped_oversized == 1
        return
    delivered, failed_batches = _through_the_worker(sink, batch)
    assert not delivered and failed_batches == 1
    assert sink.dropped_oversized == 0


def test_a_settled_family_run_writes_only_its_1x_lines(capsys: pytest.CaptureFixture[str]) -> None:
    """No engine settled line beside the sink's own: a loss is announced once (SPEC-072 FR-003)."""
    opener = Scripted(_ok(), _status(400))
    sink = HTTPSink("http://h", max_batch_count=1, opener=opener)
    sink.emit(_events(2))
    assert _lost(capsys.readouterr().err) == [
        "log-foundry: lost 1 request(s); HTTPSink, 1 attempt(s), HTTP 400"
    ]
    assert sink.log_foundry_counters.losses() == SinkLosses(dropped=0, failed=1), (
        "the engine's event totals still land, unannounced"
    )


@pytest.mark.parametrize("lock", ["owner-lock", "engine-counters-lock"])
def test_a_forked_child_commits_a_family_round_past_a_held_lock(lock: str) -> None:
    from test_fork_lifecycle import _held_by_a_parent_thread, run_in_child

    sink = HTTPSink("http://h", opener=Scripted(_status(400)))
    log_foundry.configure(service="fork", version="0", env="test", sink=sink)
    with pytest.raises(SinkDeliveryError):
        sink.emit(_events(1))
    held = sink._counter_lock if lock == "owner-lock" else sink.log_foundry_counters._counter_lock
    released, holder = _held_by_a_parent_thread(held)

    def emit_then_read() -> str:
        try:
            sink.emit(_events(1))
        except SinkDeliveryError:
            pass
        return f"failed={sink.losses().failed}"

    try:
        started = time.monotonic()
        child = run_in_child(emit_then_read, timeout=5)
    finally:
        released.set()
        holder.join(5.0)
    assert child.finished, f"the child {child.ending}, having written {child.output!r}"
    assert time.monotonic() - started < 5.0
    assert child.output == "failed=2", "the child's count adds to the inherited one"


def test_the_engine_reads_httpsinks_own_max_retries_and_stop_signal() -> None:
    """The engine's static reads see the instance attributes ``HTTPSink.__init__`` sets."""
    sink = HTTPSink("http://h", max_retries=4)
    event = threading.Event()
    sink.log_foundry_stop_signal = event
    assert _engine.stop_signal_of(sink) is event
    assert read_deliver(sink) is not None


# --- FR-003: the three readers return per-event outcomes ---------------------------------------


def _unadjudicated_writers(sources: dict[str, str]) -> list[str]:
    """Every place in the family's modules that counts or stores ``dropped_unadjudicated``.

    Args:
      sources: Module text by stem.

    Returns:
      ``module.Class.method`` per writer, ``HTTPSink.__init__``'s zero included.
    """
    found: list[str] = []
    for stem, source in sources.items():
        for cls in ast.walk(ast.parse(source)):
            if not isinstance(cls, ast.ClassDef):
                continue
            for fn in cls.body:
                if not isinstance(fn, ast.FunctionDef):
                    continue
                for node in ast.walk(fn):
                    counts = (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr == "count"
                        and any(
                            isinstance(arg, ast.Constant) and arg.value == "dropped_unadjudicated"
                            for arg in node.args
                        )
                    )
                    stores = (
                        isinstance(node, ast.Attribute)
                        and node.attr == "dropped_unadjudicated"
                        and isinstance(node.ctx, ast.Store)
                    )
                    if counts or stores:
                        found.append(f"{stem}.{cls.name}.{fn.name}")
    return sorted(set(found))


def test_only_unadjudicated_counts_dropped_unadjudicated() -> None:
    sources = _family_modules()
    assert not any("_abandon_unadjudicated" in source for source in sources.values())
    assert _unadjudicated_writers(sources) == [
        "http.HTTPSink.__init__",
        "http.HTTPSink._unadjudicated",
    ]


@pytest.mark.parametrize(
    "planted",
    [
        '        run_context().count(self, "dropped_unadjudicated", 1, when=When.ALWAYS)\n',
        "        self.dropped_unadjudicated += 1\n",
    ],
    ids=["count", "store"],
)
def test_the_unadjudicated_roster_names_a_planted_writer(planted: str) -> None:
    sources = _family_modules()
    anchor = "        sent = len(items)\n        if _read_failed(payload):\n"
    assert sources["honeycomb"].count(anchor) == 1
    sources["honeycomb"] = sources["honeycomb"].replace(anchor, planted + anchor)
    assert "honeycomb.HoneycombSink._outcomes" in _unadjudicated_writers(sources)


def _build(name: str, opener: Any, **kwargs: Any) -> HTTPSink:
    from test_sinks_http_render import _RECIPES

    sink: HTTPSink = _RECIPES[name](opener=opener, **kwargs)
    return sink


def _es_bulk(*errors: bool) -> bytes:
    items = [{"index": {"error": "x"} if error else {"status": 201}} for error in errors]
    return json.dumps({"errors": True, "items": items}).encode()


def _honeycomb(*statuses: int) -> bytes:
    return json.dumps([{"status": status} for status in statuses]).encode()


def _otlp(count: str) -> bytes:
    return json.dumps({"partialSuccess": {"rejectedLogRecords": count}}).encode()


@pytest.mark.parametrize("case", ["partial", "all-errors"])
def test_elasticsearch_outcomes_are_per_item(case: str) -> None:
    if case == "partial":
        sink = _build(
            "ElasticsearchSink", Scripted(FakeResponse(200, _es_bulk(False, True, False)))
        )
        assert sink.deliver(_events(3)) == Verdict(outcomes=(A, J, A))
        assert sink.item_errors == 1  # type: ignore[attr-defined]
        return
    sink = _build("ElasticsearchSink", Scripted(FakeResponse(200, _es_bulk(True, True))))
    with pytest.raises(SinkDeliveryError):
        sink.emit(_events(2))
    assert sink.item_errors == 2  # type: ignore[attr-defined]


@pytest.mark.parametrize("case", ["partial", "all-rejected"])
def test_honeycomb_outcomes_are_per_status(case: str) -> None:
    if case == "partial":
        sink = _build("HoneycombSink", Scripted(FakeResponse(200, _honeycomb(202, 400, 202))))
        assert sink.deliver(_events(3)) == Verdict(outcomes=(A, J, A))
        assert sink.rejected_events == 1  # type: ignore[attr-defined]
        return
    sink = _build("HoneycombSink", Scripted(FakeResponse(200, _honeycomb(400, 400, 400))))
    with pytest.raises(SinkDeliveryError):
        sink.emit(_events(3))


@pytest.mark.parametrize("case", ["2-of-5", "5-of-5", "unreadable-count"])
def test_otlp_outcomes_are_positional_by_count(case: str) -> None:
    count = {"2-of-5": "2", "5-of-5": "5", "unreadable-count": "9" * 30}[case]
    sink = _build("OTLPSink", Scripted(FakeResponse(200, _otlp(count))))
    if case == "5-of-5":
        with pytest.raises(SinkDeliveryError):
            sink.emit(_events(5))
        return
    verdict = sink.deliver(_events(5))
    if case == "2-of-5":
        assert verdict == Verdict(outcomes=(J, J, A, A, A))
        assert sink.rejected_records == 2  # type: ignore[attr-defined]
    else:
        assert verdict == Verdict(outcomes=(U,) * 5)
        assert sink.dropped_unadjudicated == 5


_READERS = ["ElasticsearchSink", "OpenSearchSink", "HoneycombSink", "OTLPSink"]


@pytest.mark.parametrize(
    "case", ["IncompleteRead", "TimeoutError", "ValueError", "short:200", "chunked:200"]
)
@pytest.mark.parametrize("name", _READERS)
def test_every_reader_reads_an_unreadable_body_as_unknown(name: str, case: str) -> None:
    from test_sinks_http_body_read import (
        _READ_ERRORS,
        _READERS_AT,
        _TIMEOUT,
        _serving,
        _UnreadableResponse,
    )

    if ":" in case:
        with _serving([case]) as server:
            sink = _READERS_AT[name](server.url, max_retries=2, timeout=_TIMEOUT)
            verdict = sink.deliver(_events(2))
            posts = len(server.posts)
    else:
        error = next(err for err in _READ_ERRORS if type(err).__name__ == case)
        opener = Scripted(_UnreadableResponse(200, error))
        sink = _build(name, opener, max_retries=2)
        verdict = sink.deliver(_events(2))
        posts = len(opener.calls)
    assert verdict == Verdict(outcomes=(U, U))
    assert posts == 1
    assert sink.dropped_unadjudicated == 2


@pytest.mark.parametrize("name", _READERS)
def test_an_empty_2xx_is_accepted_only_by_otlp(name: str) -> None:
    sink = _build(name, Scripted(FakeResponse(200, b"")))
    expected = A if name == "OTLPSink" else U
    assert sink.deliver(_events(2)) == Verdict(outcomes=(expected, expected))


@pytest.mark.parametrize("mode", ["recovers", "down"])
def test_an_idempotent_reader_counts_an_unknown_once_per_run(
    mode: str, capsys: pytest.CaptureFixture[str]
) -> None:
    from log_foundry.sinks.elasticsearch import ElasticsearchSink

    class Idempotent(ElasticsearchSink):
        log_foundry_idempotent = True

    unreadable = FakeResponse(200, b"not json")
    opener = (
        Scripted(unreadable, FakeResponse(200, b"{}"))
        if mode == "recovers"
        else Scripted(unreadable)
    )
    sink = Idempotent("http://es", index="i", max_retries=2, opener=opener)
    sink.emit(_events(2))
    lines = [line for line in _lost(capsys.readouterr().err) if "may have landed" in line]
    if mode == "recovers":
        assert len(opener.calls) == 2
        assert sink.dropped_unadjudicated == 0
        assert lines == []
    else:
        assert len(opener.calls) == 3
        assert sink.dropped_unadjudicated == 2
        assert len(lines) == 1


_UNADJUDICATED_BODIES = {
    ("ElasticsearchSink", "short-items"): _es_bulk(True),
    ("ElasticsearchSink", "undecodable"): b"\xff\xfe",
    ("ElasticsearchSink", "not-an-object"): b"[1, 2]",
    ("ElasticsearchSink", "empty"): b"",
    ("OpenSearchSink", "undecodable"): b"\xff\xfe",
    ("HoneycombSink", "short"): _honeycomb(202),
    ("HoneycombSink", "undecodable"): b"\xff",
    ("HoneycombSink", "empty"): b"",
    ("OTLPSink", "partial-not-object"): json.dumps({"partialSuccess": [1]}).encode(),
    ("OTLPSink", "too-many-digits"): _otlp("9" * 30),
}


@pytest.mark.parametrize(
    ("name", "route"), sorted(_UNADJUDICATED_BODIES), ids=lambda part: str(part)
)
def test_every_unadjudicated_route_settles_through_a_worker(name: str, route: str) -> None:
    opener = Scripted(FakeResponse(200, _UNADJUDICATED_BODIES[(name, route)]))
    sink = _build(name, opener)
    delivered, failed_batches = _through_the_worker(sink, _events(2))
    assert delivered and failed_batches == 0
    assert len(opener.calls) == 1, "nothing resent"
    assert sink.dropped_unadjudicated == 2


_MIB = 1024 * 1024
_SHIM_PAYLOADS: dict[tuple[str, str], tuple[bytes, int]] = {
    ("ElasticsearchSink", "ok"): (b'{"errors": false, "items": []}', 2),
    ("ElasticsearchSink", "one-error"): (_es_bulk(False, True, False), 3),
    ("ElasticsearchSink", "all-errors"): (_es_bulk(True, True), 2),
    ("ElasticsearchSink", "errors-without-error-items"): (_es_bulk(False, False), 2),
    ("ElasticsearchSink", "short-items"): (_es_bulk(True), 2),
    ("ElasticsearchSink", "long-items"): (_es_bulk(True, True, True), 2),
    ("ElasticsearchSink", "no-errors-key"): (b"{}", 2),
    ("ElasticsearchSink", "not-an-object"): (b"[1, 2]", 2),
    ("ElasticsearchSink", "undecodable"): (b"\xff\xfe", 2),
    ("ElasticsearchSink", "empty"): (b"", 2),
    ("ElasticsearchSink", "items-not-objects"): (b'{"errors": true, "items": [1, 2]}', 2),
    ("HoneycombSink", "all-accepted"): (_honeycomb(202, 202), 2),
    ("HoneycombSink", "one-rejected"): (_honeycomb(202, 400, 202), 3),
    ("HoneycombSink", "all-rejected"): (_honeycomb(400, 400, 400), 3),
    ("HoneycombSink", "short"): (_honeycomb(202), 2),
    ("HoneycombSink", "bool-status"): (b'[{"status": true}, {"status": 202}]', 2),
    ("HoneycombSink", "float-status"): (b'[{"status": 202.0}, {"status": 202}]', 2),
    ("HoneycombSink", "not-objects"): (b"[1, 2]", 2),
    ("HoneycombSink", "object-not-array"): (b"{}", 2),
    ("HoneycombSink", "undecodable"): (b"\xff", 2),
    ("HoneycombSink", "empty"): (b"", 2),
    ("OTLPSink", "empty"): (b"", 5),
    ("OTLPSink", "no-partial"): (b"{}", 5),
    ("OTLPSink", "partial-null"): (b'{"partialSuccess": null}', 5),
    ("OTLPSink", "partial-not-object"): (b'{"partialSuccess": [1]}', 5),
    ("OTLPSink", "count-absent"): (b'{"partialSuccess": {}}', 5),
    ("OTLPSink", "two-of-five"): (_otlp("2"), 5),
    ("OTLPSink", "five-of-five"): (_otlp("5"), 5),
    ("OTLPSink", "five-as-number"): (b'{"partialSuccess": {"rejectedLogRecords": 5}}', 5),
    ("OTLPSink", "zero"): (_otlp("0"), 5),
    ("OTLPSink", "over-count-clamped"): (_otlp("9"), 5),
    ("OTLPSink", "too-many-digits"): (_otlp("9" * 30), 5),
    ("OTLPSink", "negative"): (_otlp("-1"), 5),
    ("OTLPSink", "over-the-bound"): (b" " * (4 * _MIB + 1), 5),
    ("OTLPSink", "hostile-ff"): (b"\xff\xfe", 0),
    ("OTLPSink", "hostile-dict-count"): (
        b'{"partialSuccess": {"rejectedLogRecords": {"a": 1}}}',
        0,
    ),
    ("OTLPSink", "hostile-deep"): (b"{" * 100_000, 0),
    ("OTLPSink", "hostile-null"): (b"null", 0),
    ("OTLPSink", "hostile-1"): (b"1", 0),
    ("OTLPSink", "hostile-s"): (b'"s"', 0),
}
"""Payloads the readers' existing tests use, with the chunk size each was read for."""

_SHIM_AT_BASE: dict[tuple[str, str], tuple[bool, dict[str, int], int]] = {
    ("ElasticsearchSink", "ok"): (True, {}, 0),
    ("ElasticsearchSink", "one-error"): (True, {"item_errors": 1}, 1),
    ("ElasticsearchSink", "all-errors"): (False, {"item_errors": 2}, 1),
    ("ElasticsearchSink", "errors-without-error-items"): (True, {"dropped_unadjudicated": 2}, 1),
    ("ElasticsearchSink", "short-items"): (True, {"dropped_unadjudicated": 2}, 1),
    ("ElasticsearchSink", "long-items"): (True, {"dropped_unadjudicated": 2}, 1),
    ("ElasticsearchSink", "no-errors-key"): (True, {}, 0),
    ("ElasticsearchSink", "not-an-object"): (True, {"dropped_unadjudicated": 2}, 1),
    ("ElasticsearchSink", "undecodable"): (True, {"dropped_unadjudicated": 2}, 1),
    ("ElasticsearchSink", "empty"): (True, {"dropped_unadjudicated": 2}, 1),
    ("ElasticsearchSink", "items-not-objects"): (True, {"dropped_unadjudicated": 2}, 1),
    ("HoneycombSink", "all-accepted"): (True, {}, 0),
    ("HoneycombSink", "one-rejected"): (True, {"rejected_events": 1}, 1),
    ("HoneycombSink", "all-rejected"): (False, {"rejected_events": 3}, 1),
    ("HoneycombSink", "short"): (True, {"dropped_unadjudicated": 2}, 1),
    ("HoneycombSink", "bool-status"): (True, {"dropped_unadjudicated": 2}, 1),
    ("HoneycombSink", "float-status"): (True, {"dropped_unadjudicated": 2}, 1),
    ("HoneycombSink", "not-objects"): (True, {"dropped_unadjudicated": 2}, 1),
    ("HoneycombSink", "object-not-array"): (True, {"dropped_unadjudicated": 2}, 1),
    ("HoneycombSink", "undecodable"): (True, {"dropped_unadjudicated": 2}, 1),
    ("HoneycombSink", "empty"): (True, {"dropped_unadjudicated": 2}, 1),
    ("OTLPSink", "empty"): (True, {}, 0),
    ("OTLPSink", "no-partial"): (True, {}, 0),
    ("OTLPSink", "partial-null"): (True, {}, 0),
    ("OTLPSink", "partial-not-object"): (True, {"dropped_unadjudicated": 5}, 1),
    ("OTLPSink", "count-absent"): (True, {}, 0),
    ("OTLPSink", "two-of-five"): (True, {"rejected_records": 2}, 1),
    ("OTLPSink", "five-of-five"): (False, {"rejected_records": 5}, 1),
    ("OTLPSink", "five-as-number"): (False, {"rejected_records": 5}, 1),
    ("OTLPSink", "zero"): (True, {}, 0),
    ("OTLPSink", "over-count-clamped"): (False, {"rejected_records": 5}, 1),
    ("OTLPSink", "too-many-digits"): (True, {"dropped_unadjudicated": 5}, 1),
    ("OTLPSink", "negative"): (True, {"dropped_unadjudicated": 5}, 1),
    ("OTLPSink", "over-the-bound"): (True, {"dropped_unadjudicated": 5}, 1),
    ("OTLPSink", "hostile-ff"): (True, {}, 0),
    ("OTLPSink", "hostile-dict-count"): (True, {}, 1),
    ("OTLPSink", "hostile-deep"): (True, {}, 0),
    ("OTLPSink", "hostile-null"): (True, {}, 0),
    ("OTLPSink", "hostile-1"): (True, {}, 0),
    ("OTLPSink", "hostile-s"): (True, {}, 0),
}
"""``(bool, counter deltas, lost lines)`` per payload, recorded from the 1.x readers.

Recorded by calling each reader's ``_handle_response`` at the build's Phase 2 commit
(``caaaba85``), where the readers were still their 1.x code, so the expectation is a
differential oracle against the old readers, never recomputed from the shims under test. The
``ok`` case there used ``{"errors": false}`` with two non-error items; any body without a truthy
``errors`` reads the same.
"""

_COUNTERS = (
    "failed",
    "item_errors",
    "rejected_events",
    "rejected_records",
    "dropped_unadjudicated",
)


@pytest.mark.parametrize(("name", "case"), sorted(_SHIM_PAYLOADS), ids=lambda part: str(part))
def test_each_shim_returns_its_1x_bool(
    name: str, case: str, capsys: pytest.CaptureFixture[str]
) -> None:
    payload, n = _SHIM_PAYLOADS[(name, case)]
    sink: Any = _build(name, FakeOpener())
    before = {counter: getattr(sink, counter, 0) for counter in _COUNTERS}
    result = sink._handle_response(payload, [object()] * n)
    moved = {
        counter: getattr(sink, counter, 0) - before[counter]
        for counter in _COUNTERS
        if getattr(sink, counter, 0) != before[counter]
    }
    lines = len(_lost(capsys.readouterr().err))
    assert (result, moved, lines) == _SHIM_AT_BASE[(name, case)]


def test_the_shim_oracle_covers_every_recorded_payload() -> None:
    assert set(_SHIM_PAYLOADS) == set(_SHIM_AT_BASE)


# --- FR-004: a third-party subclass that overrides emit or _handle_response keeps its 1.x path --


def _every_shipped_class() -> list[type[HTTPSink]]:
    from test_sinks_http_render import _shipped_http_subclasses

    return [HTTPSink, *_shipped_http_subclasses()]


def _shipped(cls: type[HTTPSink], opener: Any, **kwargs: Any) -> HTTPSink:
    if cls is HTTPSink:
        return HTTPSink("http://h", opener=opener, **kwargs)
    return _build(cls.__name__, opener, **kwargs)


@pytest.mark.parametrize("cls", _every_shipped_class(), ids=lambda cls: cls.__name__)
def test_every_shipped_family_class_offers_deliver(cls: type[HTTPSink]) -> None:
    sink = _shipped(cls, FakeOpener())
    assert read_deliver(sink) is not None
    assert cls._READS_BOOL is False
    assert "_READS_BOOL" in vars(cls), "written on every class, False included"


def shipped_deliver_findings(sources: dict[str, str]) -> list[str]:
    """Every shipped family subclass that defines ``emit`` or ``deliver``, or sets ``deliver``.

    Args:
      sources: Module text by stem.

    Returns:
      ``module.Class.member`` per finding.
    """
    found: list[str] = []
    for stem, source in sources.items():
        for node in ast.walk(ast.parse(source)):
            if not isinstance(node, ast.ClassDef) or node.name == "HTTPSink":
                continue
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name in {"emit", "deliver"}:
                    found.append(f"{stem}.{node.name}.{item.name}")
                targets = item.targets if isinstance(item, ast.Assign) else []
                if isinstance(item, ast.AnnAssign):
                    targets = [item.target]
                found.extend(
                    f"{stem}.{node.name}.deliver="
                    for target in targets
                    if isinstance(target, ast.Name) and target.id == "deliver"
                )
    return found


def test_no_shipped_family_class_defines_emit_or_deliver() -> None:
    assert shipped_deliver_findings(_family_modules()) == []


@pytest.mark.parametrize(
    ("planted", "finding"),
    [
        ("    deliver = None\n", "loki.LokiSink.deliver="),
        ("    def deliver(self, batch):\n        return None\n", "loki.LokiSink.deliver"),
        ("    def emit(self, batch):\n        return None\n", "loki.LokiSink.emit"),
    ],
    ids=["opt-out", "deliver", "emit"],
)
def test_the_eligibility_roster_names_a_planted_member(planted: str, finding: str) -> None:
    sources = _family_modules()
    anchor = "    def _render(self, event: dict[str, object]) -> str:\n"
    assert sources["loki"].count(anchor) == 1
    sources["loki"] = sources["loki"].replace(anchor, planted + "\n" + anchor)
    assert shipped_deliver_findings(sources) == [finding]


class _Stamped(HTTPSink):
    """A 1.x subclass whose ``emit`` stamps each event and calls ``super().emit``."""

    emits = 0

    def emit(self, batch: list[dict[str, object]]) -> None:
        type(self).emits += 1
        super().emit([{**event, "stamp": 1} for event in batch])


class _BoolReader(HTTPSink):
    """A 1.x subclass whose bool ``_handle_response`` reads ``ok`` in the body as delivered."""

    def _handle_response(self, payload: bytes, items: list[Any]) -> bool:
        return b"ok" in payload


def _bool_reader_of_elasticsearch() -> type[HTTPSink]:
    from log_foundry.sinks.elasticsearch import ElasticsearchSink

    class _StrictBulk(ElasticsearchSink):
        reads = 0

        def _handle_response(self, payload: bytes, items: list[Any]) -> bool:
            type(self).reads += 1
            return b"ok" in payload

    return _StrictBulk


@pytest.mark.parametrize("case", ["rejected", "recovers", "orphan"])
def test_an_emit_override_keeps_its_1x_path(case: str) -> None:
    _Stamped.emits = 0
    script = {
        "rejected": [_status(400)],
        "recovers": [_status(503), _ok()],
        "orphan": [_status(503)],
    }[case]
    opener = Scripted(*script)
    sink = _Stamped("http://h", max_retries=2, opener=opener)
    assert read_deliver(sink) is None
    if case == "orphan":
        log_foundry.configure(service="t", sink=sink)
        log_foundry.info("outside a span")
        assert len(opener.calls) == 3, "s + 1 requests through its own emit"
        assert log_foundry.health().orphan_lost == 1
        return
    delivered, failed_batches = _through_the_worker(sink, _events(1), max_retries=2)
    if case == "rejected":
        assert not delivered and failed_batches == 1
        assert _Stamped.emits == 3 and len(opener.calls) == 3, "the worker's 1.x resend"
    else:
        assert delivered and failed_batches == 0
        assert len(opener.calls) == 2
        assert b'"stamp": 1' in opener.calls[1]["body"], "landed once, through its own emit"


@pytest.mark.parametrize("case", ["one-chunk", "beside-a-true-chunk", "orphan"])
@pytest.mark.parametrize("base", ["HTTPSink", "ElasticsearchSink"])
def test_a_bool_reader_override_keeps_its_1x_path(base: str, case: str) -> None:
    cls = _BoolReader if base == "HTTPSink" else _bool_reader_of_elasticsearch()
    script = {
        "one-chunk": [FakeResponse(200, b"no")],
        "beside-a-true-chunk": [FakeResponse(200, b"ok"), FakeResponse(200, b"no")],
        "orphan": [_status(503)],
    }[case]
    opener = Scripted(*script)
    kwargs: dict[str, Any] = {"max_batch_count": 1, "max_retries": 2, "opener": opener}
    sink = cls("http://h", **kwargs) if base == "HTTPSink" else cls("http://h", index="i", **kwargs)
    assert read_deliver(sink) is None
    if case == "orphan":
        log_foundry.configure(service="t", sink=sink)
        log_foundry.info("outside a span")
        assert len(opener.calls) == 3
        assert log_foundry.health().orphan_lost == 1
        return
    if case == "one-chunk":
        with pytest.raises(SinkDeliveryError):
            sink.emit(_events(1))
        opener.calls.clear()
        delivered, failed_batches = _through_the_worker(sink, _events(1), max_retries=2)
        assert not delivered and failed_batches == 1
        assert len(opener.calls) == 3, "the worker makes 3 attempts, as 1.x did"
        return
    sink.emit(_events(2))
    assert len(opener.calls) == 2, "emit returned and neither chunk was resent"


def test_a_bool_reader_below_a_shipped_reader_reads_through_its_bool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from log_foundry.sinks.elasticsearch import ElasticsearchSink

    cls: Any = _bool_reader_of_elasticsearch()
    called: list[bytes] = []

    def spy(self: Any, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
        called.append(payload)
        return (A,) * len(items)

    monkeypatch.setattr(ElasticsearchSink, "_outcomes", spy)
    sink = cls("http://h", index="i", max_batch_count=1, opener=Scripted(FakeResponse(200, b"ok")))
    sink.emit(_events(3))
    assert cls.reads == 3, "its override is called on every 2xx"
    assert called == [], "ElasticsearchSink._outcomes is never called for it"


def _offered(cls: type) -> bool:
    return read_deliver(cls("http://h", opener=FakeOpener())) is not None


def test_third_party_eligibility() -> None:
    class RenderOnly(HTTPSink):
        def _render(self, event: dict[str, object]) -> str:
            return super()._render(event)

    class BodyOnly(HTTPSink):
        def _body(self, items: list[Any]) -> tuple[bytes, str]:
            return super()._body(items)

    class BothHooks(HTTPSink):
        def _handle_response(self, payload: bytes, items: list[Any]) -> bool:
            return True

        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            return (A,) * len(items)

    class BoolA(HTTPSink):
        def _handle_response(self, payload: bytes, items: list[Any]) -> bool:
            return True

    class OutcomesB(BoolA):
        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            return (A,) * len(items)

    class EmitA(HTTPSink):
        def emit(self, batch: list[dict[str, object]]) -> None:
            super().emit(batch)

    class OutcomesBelowEmit(EmitA):
        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            return (A,) * len(items)

    class BoolAndEmitA(HTTPSink):
        def _handle_response(self, payload: bytes, items: list[Any]) -> bool:
            return True

        def emit(self, batch: list[dict[str, object]]) -> None:
            super().emit(batch)

    class OutcomesBelowBoolAndEmit(BoolAndEmitA):
        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            return (A,) * len(items)

    class OptOut(HTTPSink):
        deliver = None

    class OptOutChild(OptOut):
        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            return (A,) * len(items)

    class Native(HTTPSink):
        def deliver(self, batch: list[dict[str, object]]) -> Verdict:
            return Verdict.uniform(A, len(batch))

    class NativeChild(Native):
        pass

    assert _offered(RenderOnly) and _offered(BodyOnly) and _offered(BothHooks)
    assert not _offered(BoolA) and BoolA._READS_BOOL
    assert _offered(OutcomesB) and not OutcomesB._READS_BOOL
    assert not _offered(EmitA) and not _offered(OutcomesBelowEmit)
    assert not _offered(OutcomesBelowBoolAndEmit), (
        "restoring deliver below a withdrawn class must never bypass an emit override"
    )
    assert vars(OutcomesBelowBoolAndEmit)["deliver"] is None
    assert not _offered(OptOut) and not _offered(OptOutChild), "an opt-out is never restored"
    assert _offered(Native) and _offered(NativeChild)
    assert NativeChild.deliver is Native.deliver, "a plain child keeps its parent's deliver"
    assert "_READS_BOOL" in vars(RenderOnly) and RenderOnly._READS_BOOL is False


def test_a_raising_reader_costs_only_its_chunk(capsys: pytest.CaptureFixture[str]) -> None:
    class Raising(HTTPSink):
        def _handle_response(self, payload: bytes, items: list[Any]) -> bool:
            if b"boom" in payload:
                raise ValueError("cannot read")
            return True

    opener = Scripted(FakeResponse(200, b"fine"), FakeResponse(200, b"boom"))
    sink = Raising("http://h", max_batch_count=1, opener=opener)
    sink.emit(_events(2))
    assert len(opener.calls) == 2
    assert sink.dropped_unadjudicated == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=1)
    assert _lost(capsys.readouterr().err) == [
        (
            "log-foundry: lost 1 event(s); Raising reading a 2xx response raised ValueError; "
            "they may have landed; not retried"
        )
    ]


def test_handle_response_keeps_its_1x_signature() -> None:
    import inspect

    from log_foundry.sinks.http import _Item

    signature = inspect.signature(HTTPSink._handle_response, eval_str=True)
    assert list(signature.parameters) == ["self", "payload", "items"]
    assert signature.parameters["payload"].annotation is bytes
    assert signature.parameters["items"].annotation == list[_Item]
    assert signature.return_annotation is bool


# --- FR-001 1.18, 1.19: a run inside a run is independent --------------------------------------


def test_a_run_nested_in_a_wrapper_round_is_independent(waits: list[float]) -> None:
    from log_foundry.sinks.verdict import SinkCounters

    inner_rounds: list[int] = []

    def answer(index: int, request: urllib.request.Request) -> FakeResponse:
        inner_rounds.append(_engine.run_context().round)
        return _status(503) if index == 0 else _ok()

    opener = Scripted(answer)
    inner = SentrySink("http://k@sentry.local/1", backend="http", max_retries=3, opener=opener)
    after: list[tuple[int, object]] = []

    class Wrapper:
        def __init__(self) -> None:
            self.log_foundry_counters = SinkCounters()

        def deliver(self, batch: list[dict[str, object]]) -> Verdict:
            context = _engine.run_context()
            context.notes(self)["mine"] = "kept"
            inner.emit(batch)
            current = _engine.run_context()
            after.append((current.round, current.notes(self).get("mine")))
            return Verdict.uniform(A, len(batch))

        def emit(self, batch: list[dict[str, object]]) -> None:
            _engine.emit_via_deliver(self, batch)

    delivery = _engine.run_delivery(
        Wrapper(), [{"level": "ERROR", "message": "m"}], worker_retries=None, stop=None
    )
    assert delivery is not None and not delivery.failed
    assert len(opener.calls) == 2 and len(waits) == 1
    assert inner.sent == 1
    assert inner_rounds == [1, 2]
    assert after == [(1, "kept")]


def test_a_send_inside_the_sinks_own_round_is_its_own_run() -> None:
    rounds: list[int] = []
    sent: list[bytes] = []

    class Hooked(HTTPSink):
        def _outcomes(self, payload: bytes, items: list[Any]) -> tuple[Outcome, ...]:
            rounds.append(_engine.run_context().round)
            sent.append(self._send(b"nested", content_type="text/plain"))
            rounds.append(_engine.run_context().round)
            return (J,) * len(items) if sent[-1] == b"payload" else (A,) * len(items)

    opener = Scripted(_ok(), _status(503), FakeResponse(200, b"payload"))
    sink = Hooked("http://h", max_retries=3, opener=opener)
    with pytest.raises(SinkDeliveryError):
        sink.emit(_events(1))
    assert len(opener.calls) == 3, "the chunk's request and the nested send's two"
    assert sent == [b"payload"]
    assert rounds == [1, 1], "the outer run is still in its round one"
    assert sink.failed == 0, "the nested 503 was not its run's last round"


# --- FR-005 and FR-002 2.9: both paths and both forms over every shipped class ----------------

_STAMP = "2026-08-10T00:00:00.000Z"
_RESPONSES = ["200", "400", "503-then-200", "unreadable-2xx", "oversized"]


def _event(i: int, **fields: object) -> dict[str, object]:
    """An event every platform subclass renders: a message, a level, a timestamp, ``fields``."""
    return {"message": f"m{i}", "level": "INFO", "timestamp": _STAMP, "fields": dict(fields)}


def _success(cls: type[HTTPSink]) -> FakeResponse:
    """A ``2xx`` each class reads as one event accepted."""
    if cls.__name__ == "HoneycombSink":
        return FakeResponse(200, b'[{"status": 202}]')
    return FakeResponse(200, b"{}")


def _script(cls: type[HTTPSink], response: str) -> list[Any]:
    import http.client

    from test_sinks_http_body_read import _UnreadableResponse

    return {
        "200": [_success(cls)],
        "400": [_status(400)],
        "503-then-200": [_status(503), _success(cls)],
        "unreadable-2xx": [_UnreadableResponse(200, http.client.IncompleteRead(b""))],
        "oversized": [_success(cls)],
    }[response]


def _expected(cls: type[HTTPSink], response: str, n: int) -> SinkLosses:
    """What ``n`` events cost under each response, so the twins cannot agree on nothing."""
    readers = {"ElasticsearchSink", "OpenSearchSink", "HoneycombSink", "OTLPSink"}
    if response == "400":
        return SinkLosses(dropped=0, failed=n)
    if response == "unreadable-2xx" and cls.__name__ in readers:
        return SinkLosses(dropped=0, failed=n)
    if response == "oversized":
        return SinkLosses(dropped=1, failed=0)
    return SinkLosses(dropped=0, failed=0)


def _formula(sink: Any) -> SinkLosses:
    """``losses()`` by its 1.x formula over the 1.x attributes, independently of ``losses()``."""
    failed = sum(
        getattr(sink, name, 0)
        for name in (
            "failed",
            "item_errors",
            "rejected_events",
            "rejected_records",
            "dropped_unadjudicated",
        )
    )
    return SinkLosses(dropped=sink.dropped_oversized + sink._unrenderable, failed=failed)


@pytest.mark.parametrize("response", _RESPONSES)
@pytest.mark.parametrize("cls", _every_shipped_class(), ids=lambda cls: cls.__name__)
def test_worker_and_orphan_paths_leave_equal_ledgers(cls: type[HTTPSink], response: str) -> None:
    """Invariant 6 and FR-002 2.9: equal ``losses()`` and requests, and ``health()`` reads 1.x."""
    kwargs: dict[str, Any] = {"max_batch_bytes": 60} if response == "oversized" else {}
    through_worker = Scripted(*_script(cls, response))
    worker_sink = _shipped(cls, through_worker, **kwargs)
    _through_the_worker(worker_sink, [_event(0)])

    orphan = Scripted(*_script(cls, response))
    orphan_sink = _shipped(cls, orphan, **kwargs)
    log_foundry.configure(service="t", sink=orphan_sink)
    log_foundry.info("outside a span")

    assert worker_sink.losses() == orphan_sink.losses() == _expected(cls, response, 1)
    assert len(through_worker.calls) == len(orphan.calls)
    assert log_foundry.health().sink == _formula(orphan_sink) == orphan_sink.losses()


@pytest.mark.parametrize("response", _RESPONSES)
@pytest.mark.parametrize("cls", _every_shipped_class(), ids=lambda cls: cls.__name__)
def test_emit_and_deliver_leave_equal_ledgers_bodies_and_lines(
    cls: type[HTTPSink], response: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Twin forms: ``emit`` and an orphan run through ``deliver`` share the budget ``s``."""
    kwargs: dict[str, Any] = {"max_batch_count": 1, "max_retries": 1}
    if response == "oversized":
        kwargs["max_batch_bytes"] = 1000
        batch = [_event(0, big="x" * 2000), _event(1)]
    else:
        batch = [_event(0), _event(1)]
    capsys.readouterr()

    by_emit = Scripted(*_script(cls, response))
    emit_sink = _shipped(cls, by_emit, **kwargs)
    try:
        emit_sink.emit(list(batch))
    except SinkDeliveryError:
        pass
    emit_lines = capsys.readouterr().err

    by_deliver = Scripted(*_script(cls, response))
    deliver_sink = _shipped(cls, by_deliver, **kwargs)
    _engine.run_delivery(deliver_sink, list(batch), worker_retries=None, stop=None)
    deliver_lines = capsys.readouterr().err

    assert emit_sink.losses() == deliver_sink.losses() == _expected(cls, response, 2)
    assert [call["body"] for call in by_emit.calls] == [call["body"] for call in by_deliver.calls]
    assert emit_lines == deliver_lines
    assert log_foundry.sinks.base.read_losses(emit_sink) == _formula(emit_sink)


# --- The unreduced 413 path and a permanent bool reader's settled line --------------------------


class _Free(HTTPSink):
    """Charges every item nothing, so no budget reduction ever shrinks a multi-item chunk."""

    def _item_size(self, rendered: str, event: dict[str, object]) -> int:
        return 0


class _OverridesUnreduced(_Free):
    """A third-party override of ``_abandon_unreduced`` written to its 1.x signature."""

    seen: list[int]

    def _abandon_unreduced(self, chunk: list[Any]) -> None:  # type: ignore[override]
        self.seen.append(len(chunk))
        super()._abandon_unreduced(chunk)


@pytest.mark.parametrize("cls", [_Free, _OverridesUnreduced], ids=["base", "override"])
def test_an_unreduced_413_chunk_is_rejected_and_counted_once(
    cls: type[_Free], capsys: pytest.CaptureFixture[str]
) -> None:
    """The base counts ``failed`` once with its 1.x line; a 1.x-signature override is still called."""
    from log_foundry.sinks.http import MAX_BUDGET_REDUCTIONS

    opener = Scripted(_status(413))
    sink = cls("http://h", max_batch_count=8, max_retries=0, opener=opener)
    sink.seen = []  # type: ignore[attr-defined]
    with pytest.raises(SinkDeliveryError):
        sink.emit(_events(8))
    assert len(opener.calls) == MAX_BUDGET_REDUCTIONS + 1
    if cls is _OverridesUnreduced:
        assert sink.seen == [8]  # type: ignore[attr-defined]
    assert sink.failed == 1
    assert sink.dropped_oversized == 0
    assert _lost(capsys.readouterr().err) == [
        (
            f"log-foundry: lost 1 request(s); {cls.__name__}, 8 event(s) still 413 after "
            f"{MAX_BUDGET_REDUCTIONS} reduction(s); lower max_batch_bytes to match the destination"
        )
    ]


def test_a_permanent_bool_readers_partial_loss_is_announced_by_the_engine_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A bool reader's ``False`` counts nothing of its own, so the engine's settled line says it.

    Ineligible, so ``emit`` runs the engine inline over its round: one chunk read ``True`` is
    ``ACCEPTED``, one read ``False`` is ``REJECTED``, and the run settles. The sink registers no
    line for the rejected chunk — a 1.x bool override counted its own, if anything — so the
    engine's settled line is the run's only announcement.
    """
    opener = Scripted(FakeResponse(200, b"ok"), FakeResponse(200, b"no"))
    sink = _BoolReader("http://h", max_batch_count=1, opener=opener)
    assert read_deliver(sink) is None
    sink.emit(_events(2))
    assert len(opener.calls) == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=0), "the 1.x counters are untouched"
    assert sink.log_foundry_counters.losses() == SinkLosses(dropped=0, failed=1)
    assert _lost(capsys.readouterr().err) == [
        (
            "log-foundry: lost 1 event(s); rejected 1, unresolved 0, raised 0, unknown 0, "
            "dropped 0 after 1 delivery round(s) to _BoolReader"
        )
    ]


# --- Frame-1 review: the mapping, the per-run search state and the untested branches -----------


class _Enriching(HTTPSink):
    """A third-party ``_items`` override that rebuilds every event before rendering it."""

    def _items(self, batch: list[dict[str, object]], pending: Any) -> list[Any]:
        return super()._items([{**event, "host": "h1"} for event in batch], pending)


@pytest.mark.parametrize("answer", ["400", "200", "503-then-200"])
def test_an_items_override_that_rebuilds_events_maps_each_one(
    answer: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """One rebuilt item per event, in order, maps exactly: no event is left ``DROPPED``."""
    script = {
        "400": [_status(400)],
        "200": [_ok()],
        "503-then-200": [_status(503), _ok()],
    }[answer]
    opener = Scripted(*script)
    sink = _Enriching("http://h", max_retries=0, opener=opener)
    delivered, failed_batches = _through_the_worker(sink, _events(2), max_retries=3)
    err = capsys.readouterr().err
    assert sink.log_foundry_counters.losses().dropped == 0
    assert sink.losses().dropped == 0
    if answer == "400":
        assert not delivered and failed_batches == 1, "a run that delivered nothing fails"
        assert len(opener.calls) == 1
        return
    assert delivered and failed_batches == 0
    assert len(opener.calls) == (1 if answer == "200" else 2)
    assert b'"host": "h1"' in opener.calls[-1]["body"] and _ids(opener.calls[-1]) == [0, 1]
    assert [line for line in _lost(err) if "dropped" in line] == []


def test_an_event_no_request_carries_is_rejected_never_dropped() -> None:
    """An override that silently omits an event: unplaced, uncounted, so ``REJECTED``."""

    class Omitting(HTTPSink):
        def _items(self, batch: list[dict[str, object]], pending: Any) -> list[Any]:
            return super()._items([{**batch[0], "host": "h1"}], pending)

    sink = Omitting("http://h", opener=Scripted(_ok()))
    assert sink.deliver(_events(2)) == Verdict(outcomes=(A, J))


def test_a_resend_starts_from_the_budget_round_one_found() -> None:
    """Criterion 1.12's second clause: two adjacent ``503``s are resent with no new ``413``."""
    opener = _refusing_at({3, 4})
    sink = HTTPSink("http://h", max_batch_bytes=5_000_000, max_retries=1, opener=opener)
    sink.emit([{"i": n, "message": "small event"} for n in range(40)])
    second = opener.calls[13:]
    assert [_ids(call) for call in second] == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
    assert all(len(call["body"]) <= 300 for call in second), "round two makes no 413"


def _refusing_at(indexes: set[int]) -> Scripted:
    def answer(index: int, request: urllib.request.Request) -> FakeResponse:
        if len(request.data or b"") > 300:
            return _status(413)
        return _status(503) if index in indexes else _ok()

    return Scripted(answer)


def test_a_resend_keeps_the_smallest_refused_size() -> None:
    """A lone event no smaller than one round one refused is dropped in round two, unsent."""
    opener = Scripted(_status(503), _status(413), _ok())
    sink = HTTPSink("http://h", max_batch_count=1, max_retries=1, opener=opener)
    batch = [{"i": 0, "big": "x" * 2000}, {"i": 1, "big": "x" * 1000}]
    sink.emit(batch)
    assert len(opener.calls) == 2, "round two sends nothing: event 0 is known refused"
    assert sink.dropped_oversized == 2


def test_a_resend_keeps_the_reductions_already_spent(capsys: pytest.CaptureFixture[str]) -> None:
    """A run searches no more than one 1.x ``emit`` did: round two gets no fresh 24 halvings."""
    from log_foundry.sinks.http import MAX_BUDGET_REDUCTIONS

    def answer(index: int, request: urllib.request.Request) -> FakeResponse:
        return _status(503) if index == 0 else _status(413)

    opener = Scripted(answer)
    sink = _Free("http://h", max_batch_count=4, max_retries=1, opener=opener)
    sink.seen = []  # type: ignore[attr-defined]
    with pytest.raises(SinkDeliveryError):
        sink.emit(_events(8))
    assert len(opener.calls) == 1 + (MAX_BUDGET_REDUCTIONS + 1) + 1
    assert sink.failed == 2, "both chunks abandoned unreduced, the retryable one not counted"


def test_an_unreduced_chunk_is_counted_though_its_round_was_not_the_last(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``_abandon_unreduced`` counts ``ALWAYS``: a later round landing the other chunk keeps it."""
    from log_foundry.sinks.http import MAX_BUDGET_REDUCTIONS

    def answer(index: int, request: urllib.request.Request) -> FakeResponse:
        if index == 0:
            return _status(503)
        return _status(413) if index <= MAX_BUDGET_REDUCTIONS + 1 else _ok()

    opener = Scripted(answer)
    sink = _Free("http://h", max_batch_count=4, max_retries=1, opener=opener)
    sink.emit(_events(8))
    assert len(opener.calls) == MAX_BUDGET_REDUCTIONS + 3
    assert sink.failed == 1
    assert _requests_lost(capsys.readouterr().err) == [
        (
            f"log-foundry: lost 1 request(s); _Free, 4 event(s) still 413 after "
            f"{MAX_BUDGET_REDUCTIONS} reduction(s); lower max_batch_bytes to match the destination"
        )
    ]


def test_a_retryable_413_is_retried_before_the_search() -> None:
    """S10: a subclass whose ``_retryable_status`` accepts ``413`` keeps 1.x's order."""

    class RetriesA413(HTTPSink):
        def _retryable_status(self, status: int) -> bool:
            return status == 413 or super()._retryable_status(status)

    opener = Scripted(_status(413), _ok())
    sink = RetriesA413("http://h", max_retries=1, opener=opener)
    sink.emit(_events(1))
    assert len(opener.calls) == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_send_reraises_a_raise_building_its_request_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = ValueError("cannot build this request")

    def refusing(*args: Any, **kwargs: Any) -> Any:
        raise error

    opener = Scripted(_ok())
    sink = HTTPSink("http://h", opener=opener)
    monkeypatch.setattr("log_foundry.sinks.http.urllib.request.Request", refusing)
    with pytest.raises(ValueError) as raised:
        sink._send(b"x", content_type="text/plain")
    assert raised.value is error
    assert opener.calls == []
    assert sink.failed == 0


def test_a_plain_child_of_an_engine_native_subclass_with_its_own_emit_keeps_deliver() -> None:
    class Native(HTTPSink):
        def deliver(self, batch: list[dict[str, object]]) -> Verdict:
            return Verdict.uniform(A, len(batch))

        def emit(self, batch: list[dict[str, object]]) -> None:
            _engine.emit_via_deliver(self, batch)

    class NativeChild(Native):
        pass

    assert _offered(Native) and _offered(NativeChild)
    assert NativeChild.deliver is Native.deliver


def test_the_request_building_line_names_the_rounds_first_exception(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class TwoFaults(HTTPSink):
        def _body(self, items: list[Any]) -> tuple[bytes, str]:
            kind = items[0].event.get("raise")
            if kind == "value":
                raise ValueError("first")
            if kind == "key":
                raise KeyError("second")
            return super()._body(items)

    opener = Scripted(_ok())
    sink = TwoFaults("http://h", max_batch_count=1, opener=opener)
    sink.emit([{"i": 0, "raise": "value"}, {"i": 1, "raise": "key"}, {"i": 2}])
    assert len(opener.calls) == 1
    assert _lost(capsys.readouterr().err) == [
        "log-foundry: lost 2 event(s); TwoFaults, rendering raised ValueError"
    ]


@pytest.mark.parametrize(
    "result",
    [None, {Outcome.ACCEPTED}, ["x"], (Outcome.ACCEPTED,)],
    ids=["none", "set", "list-of-str", "wrong-length"],
)
def test_a_reader_result_that_is_not_one_outcome_per_event_is_unadjudicated(
    result: object, capsys: pytest.CaptureFixture[str]
) -> None:
    class Odd(HTTPSink):
        def _outcomes(self, payload: bytes, items: list[Any]) -> Any:
            return result

    sink = Odd("http://h", opener=Scripted(_ok()))
    assert sink.deliver(_events(2)) == Verdict(outcomes=(U, U))
    assert _lost(capsys.readouterr().err) == [
        (
            "log-foundry: lost 2 event(s); Odd reading a 2xx response gave no outcome per event; "
            "they may have landed; not retried"
        )
    ]


def test_a_shim_called_inside_a_round_commits_at_once() -> None:
    """``_handle_response`` called by a bool override inside its round commits its own counts."""
    from log_foundry.sinks.elasticsearch import ElasticsearchSink

    seen: list[int] = []

    class Strict(ElasticsearchSink):
        def _handle_response(self, payload: bytes, items: list[Any]) -> bool:
            ok = super()._handle_response(payload, items)
            seen.append(self.item_errors)
            return ok

    sink = Strict("http://h", index="i", opener=Scripted(FakeResponse(200, _es_bulk(False, True))))
    sink.emit(_events(2))
    assert seen == [1], "committed when the shim returned, not when the outer run ends"
    assert sink.item_errors == 1
