"""SPEC-092 prototype: the conformance corpus's classes, faults, backend and assertion.

Written on 2026-11-15 while authoring SPEC-092, against 6bd0b66e; not one of the audit's probes.
It asserts nothing and exits 0: `spec092_run.py` prints each row's verdict. The build turns it
into `tests/conformance/`; this file is the evidence the spec's tables were measured from.
"""

from __future__ import annotations

import collections
import contextlib
import dataclasses
import gzip
import io
import json
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

N = 8
"""Events per batch: halves and quarters are whole, so `partial` splits 4, 2 and 2."""

TOKEN = re.compile(r"c92e\d{4}")
"""Every event's `log_id`; the backend counts what it holds by these, never by position."""

CLASSES = {
    "keyed-reply": "one reply per call naming each failed entry by key; an entry not named landed",
    "positional-reply": "one reply per call with one result per entry, in order",
    "request-reply": "one status or return per call, for the whole call",
    "transactional": "the call commits all of its rows or none of them",
    "local-write": "a local file or stream; each write reports what it took",
    "socket-stream": "frames written to a socket; nothing confirms any frame",
    "per-event-ack": "the backend acknowledges each event on its own call",
    "client-buffered": "a client library buffers the events and reports on them later",
    "in-process": "an in-process callable, logger or buffer; its return is the hand-off",
    "wrapper": "forwards to an inner sink and adds only its own decisions",
}

FAULTS = (
    "clean",
    "refused-once",
    "refused-always",
    "partial",
    "taken-then-raised",
    "reply-lost",
    "peer-closed",
    "no-verdict-shape",
    "never-acknowledged",
    "autocommit",
    "caller-code-raises",
)
"""Every fault script; `APPLIES` says which a class runs."""

UNKNOWABLE = frozenset({"taken-then-raised", "reply-lost", "peer-closed", "no-verdict-shape"})
"""Faults after which the sink cannot know whether the call's events landed."""

APPLIES: dict[str, tuple[str, ...]] = {
    "keyed-reply": (
        "clean",
        "refused-once",
        "refused-always",
        "partial",
        "taken-then-raised",
        "reply-lost",
        "no-verdict-shape",
    ),
    "positional-reply": (
        "clean",
        "refused-once",
        "refused-always",
        "partial",
        "taken-then-raised",
        "reply-lost",
        "no-verdict-shape",
    ),
    "request-reply": (
        "clean",
        "refused-once",
        "refused-always",
        "taken-then-raised",
        "reply-lost",
    ),
    "transactional": (
        "clean",
        "refused-once",
        "refused-always",
        "taken-then-raised",
        "reply-lost",
        "autocommit",
    ),
    "local-write": ("clean", "refused-once", "refused-always", "taken-then-raised"),
    "socket-stream": (
        "clean",
        "refused-once",
        "refused-always",
        "taken-then-raised",
        "peer-closed",
    ),
    "per-event-ack": ("clean", "refused-once", "refused-always", "reply-lost"),
    "client-buffered": ("clean", "refused-once", "refused-always", "never-acknowledged"),
    "in-process": ("clean", "refused-once", "refused-always", "taken-then-raised"),
}
"""A wrapper runs its inner sink's class's faults and `caller-code-raises`."""


def events(batch: int) -> list[dict[str, object]]:
    """One batch of `N` events whose `log_id`s are unique across the row."""
    return [
        {
            "timestamp": "2026-11-15T00:00:00.000Z",
            "level": "ERROR",
            "service": "svc",
            "message": "m",
            "trace_id": "0af7651916cd43dd8448eb211c80319c",
            "span_id": "b7ad6b7169203331",
            "kind": "log",
            "log_id": f"c92e{batch * 100 + i:04d}",
        }
        for i in range(N)
    ]


def tokens(payload: object) -> list[str]:
    """The `log_id`s a payload carries, each once per payload, in first-seen order."""
    if isinstance(payload, (bytes, bytearray, memoryview)):
        raw = bytes(payload)
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        text = raw.decode("utf-8", "replace")
    elif isinstance(payload, str):
        text = payload
    else:
        text = json.dumps(payload, default=repr)
    return list(dict.fromkeys(TOKEN.findall(text)))


@dataclasses.dataclass
class Call:
    """One client call the backend saw: what it carried and whether its answer was knowable."""

    carried: list[str]
    knowable: bool


class Backend:
    """The stateful fake every shim of a row writes through: what it durably holds, by `log_id`.

    `decide` is the class's fault script. It is asked once per client call with the call's
    tokens and answers one action; the shim then takes the tokens the action says and answers in
    its client's own shape. Calls are numbered within the faulted batch, so a script can fault
    the first call only.
    """

    def __init__(self, fault: str) -> None:
        self.fault = fault
        self.held: collections.Counter[str] = collections.Counter()
        self.calls: list[Call] = []
        self.batch = 0
        self.in_batch = 0
        self.faulted_batch = 1 if fault == "peer-closed" else 0
        self.peer_closed = False
        self.on_close: list[Callable[[], None]] = []
        self.unknowable_faults: frozenset[str] = UNKNOWABLE

    def begin_batch(self, number: int) -> None:
        self.batch = number
        self.in_batch = 0
        if self.fault == "peer-closed" and number == self.faulted_batch:
            self.peer_closed = True

    def after_close(self) -> None:
        """Runs whatever a shim deferred to `close()`: a client buffer's late report."""
        for hook in self.on_close:
            hook()

    def decide(self, carried: list[str], *, knowable: bool | None = None) -> str:
        """Returns `ok`, `refuse`, `partial`, `taken-raise`, `reply-lost`, `noshape` or `nack`."""
        first = self.batch == self.faulted_batch and self.in_batch == 0
        self.in_batch += 1
        action = "ok"
        if self.fault == "refused-always":
            action = "refuse"
        elif first:
            action = {
                "refused-once": "refuse",
                "partial": "partial",
                "taken-then-raised": "taken-raise",
                "reply-lost": "reply-lost",
                "no-verdict-shape": "noshape",
            }.get(self.fault, "ok")
        if self.fault == "never-acknowledged":
            action = "nack"
        if knowable is None:
            knowable = action not in ("taken-raise", "reply-lost", "noshape")
        self.calls.append(Call(list(carried), knowable))
        return action

    def take(self, carried: list[str]) -> None:
        for token in carried:
            self.held[token] += 1

    def partial_split(self, carried: list[str]) -> tuple[list[str], list[str], list[str]]:
        """`partial`'s split of one call: landed, refused once (retryable), refused for good."""
        half = len(carried) // 2
        quarter = max(len(carried) // 4, 1)
        return carried[:half], carried[half : half + quarter], carried[half + quarter :]

    def permanent(self) -> set[str]:
        """Tokens `partial` refuses on every call, retryable code or not."""
        if self.fault != "partial":
            return set()
        first = next((c for c in self.calls), None)
        if first is None:
            return set()
        return set(self.partial_split(first.carried)[2])

    def unknowable(self) -> set[str]:
        """Tokens carried by at least one call whose outcome the sink could not know."""
        return {t for c in self.calls if not c.knowable for t in c.carried}


@dataclasses.dataclass
class Report:
    """What the sink and its catcher reported for the row, per event where the path allows."""

    per_event: dict[str, str] | None
    """`landed`, `lost`, `ambiguous` or `skipped` per `log_id`; None for an `emit`-only sink."""
    claimed_landed: int
    counted_lost: int
    counted_ambiguous: int
    counted_late: int
    """Counts that moved after the batch's verdict was in: at a later batch or at `close()`."""
    lines: int
    disagreement: str | None = None
    """Set when the per-event reconstruction disagrees with the engine's own `Delivery`."""


CHECKS = (
    "partition",
    "silent-loss",
    "duplicate",
    "lost-but-held",
    "unannounced",
    "ambiguous-when-knowable",
)
"""The assertion's checks, in the order a verdict lists them."""


def judge(
    batch_tokens: list[str],
    backend: Backend,
    report: Report,
    disabled: frozenset[str] = frozenset(),
) -> tuple[str, ...]:
    """Every check the row fails, in `CHECKS` order; an empty tuple is a pass.

    `disabled` removes checks, which is how the spec's mutation criteria are measured: a check
    that no row needs is a check whose removal changes no verdict.
    """
    failed: list[str] = []
    held = backend.held
    total = len(batch_tokens)
    once = sum(1 for t in batch_tokens if held[t] == 1)
    many = sum(1 for t in batch_tokens if held[t] > 1)
    none = total - once - many
    if report.per_event is not None:
        fates = report.per_event
        if (
            sorted(fates) != sorted(batch_tokens)
            or report.counted_late
            or report.disagreement is not None
        ):
            failed.append("partition")
        if any(fates.get(t) == "landed" and held[t] == 0 for t in batch_tokens):
            failed.append("silent-loss")
        if many:
            failed.append("duplicate")
        if any(fates.get(t) in ("lost", "skipped") and held[t] > 0 for t in batch_tokens):
            failed.append("lost-but-held")
        lost = sum(1 for t in batch_tokens if fates.get(t) in ("lost", "ambiguous"))
        if lost and not report.lines:
            failed.append("unannounced")
        unknowable = backend.unknowable()
        if any(fates.get(t) == "ambiguous" and t not in unknowable for t in batch_tokens):
            failed.append("ambiguous-when-knowable")
    else:
        if report.claimed_landed + report.counted_lost + report.counted_late != total or (
            report.counted_late
        ):
            failed.append("partition")
        if report.claimed_landed > once + many:
            failed.append("silent-loss")
        if many:
            failed.append("duplicate")
        if report.counted_lost + report.counted_late > none:
            failed.append("lost-but-held")
        if (report.counted_lost or report.counted_late) and not report.lines:
            failed.append("unannounced")
    return tuple(c for c in CHECKS if c in failed and c not in disabled)


def _graph(sink: object) -> list[object]:
    """Every sink reachable from `sink` through the attributes the shipped wrappers keep."""
    found: list[object] = []
    stack = [sink]
    while stack:
        current = stack.pop()
        if all(current is not f for f in found):
            found.append(current)
        for name in ("inner", "_inner", "_sinks", "sinks", "_socket", "_backend", "_built"):
            child = vars(current).get(name) if hasattr(current, "__dict__") else None
            if child is None:
                continue
            if isinstance(child, (list, tuple)):
                stack.extend(child)
            else:
                stack.append(child)
    return found


@contextlib.contextmanager
def runs() -> Iterator[tuple[list[tuple[object, object]], list[tuple[object, list[str], object]]]]:
    """Captures every engine run as `(sink, Delivery)` and every round it made.

    Patched at `_engine._run`, the one function the worker's `run_delivery` and the inline
    adapter `emit_via_deliver` both call, so a run is seen whichever path made it, and each round
    is recorded by wrapping the `deliver` callable the run was handed: `(sink, the round's
    log_ids, its verdict or exception)`. Nothing the engine decides is reimplemented here; the
    final fate of an event is its outcome in the last round that carried it.
    """
    from log_foundry.sinks import _engine

    seen: list[tuple[object, object]] = []
    rounds: list[tuple[object, list[str], object]] = []
    original = _engine._run

    def wrapper(sink: object, deliver: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        def recorded(batch: list[dict[str, object]]) -> Any:
            carried = [str(e.get("log_id")) for e in batch]
            try:
                result = deliver(batch)
            except BaseException as err:
                rounds.append((sink, carried, err))
                raise
            rounds.append((sink, carried, result))
            return result

        result = original(sink, recorded, *args, **kwargs)
        seen.append((sink, result))
        return result

    _engine._run = wrapper  # type: ignore[assignment]
    try:
        yield seen, rounds
    finally:
        _engine._run = original  # type: ignore[assignment]


def fates_from(records: list[tuple[object, list[str], object]], owner: object) -> dict[str, str]:
    """Each event's final fate: its outcome in the last round of `owner` that carried it."""
    from log_foundry.sinks.verdict import Outcome, Verdict

    fates: dict[str, str] = {}
    for self, carried, result in records:
        if self is not owner:
            continue
        if isinstance(result, BaseException):
            for token in carried:
                fates[token] = "lost"
            continue
        outcomes = getattr(result, "outcomes", None) if isinstance(result, Verdict) else None
        if outcomes is None or len(outcomes) != len(carried):
            for token in carried:
                fates[token] = "ambiguous"
            continue
        for token, outcome in zip(carried, outcomes, strict=True):
            fates[token] = {
                Outcome.ACCEPTED: "landed",
                Outcome.RETRYABLE: "lost",
                Outcome.REJECTED: "lost",
                Outcome.DROPPED: "lost",
                Outcome.UNKNOWN: "ambiguous",
                Outcome.SKIPPED: "skipped",
            }[outcome]
    return fates


def agrees(fates: dict[str, str], delivery: object) -> str | None:
    """The differential oracle: the reconstruction's counts against the engine's `Delivery`."""
    counts = collections.Counter(fates.values())
    lost = sum(getattr(delivery, name) for name in ("rejected", "dropped", "unresolved", "raised"))
    want = {
        "landed": delivery.accepted,
        "ambiguous": delivery.unknown,
        "skipped": delivery.skipped,
        "lost": lost,
    }
    got = {k: counts.get(k, 0) for k in want}
    if delivery.failed:
        return None
    return None if got == want else f"reconstructed {got} against Delivery {want}"


def losses_total(sink: object) -> tuple[int, int]:
    """`(dropped, failed)` from the sink's `losses()`, or zeros when it reports none."""
    reader = getattr(sink, "losses", None)
    value = reader() if callable(reader) else None
    if value is None:
        return (0, 0)
    return (value.dropped, value.failed)


LOST = re.compile(r"^log-foundry: lost \d+ ")


def lines_of(text: str) -> int:
    """How many loss lines the row wrote: lines starting `log-foundry: lost N `.

    Counted as lines, not summed as events: the HTTP family names its losses in requests, its
    recorded unit, so a sum of the named counts cannot be compared with events (a `MultiSink`
    child that failed every request names `lost 1 request(s)` for eight events).
    """
    return sum(1 for line in text.splitlines() if LOST.match(line))


@dataclasses.dataclass
class Row:
    """One sink variant under one fault script.

    `build` returns `(sink, target)`: the sink the worker is given, and the sink whose engine runs
    are judged per event. They differ only for a wrapper with no `deliver` of its own
    (`MultiSink`, `_FactorySink`), whose judged sink is the engine child behind it.
    """

    sink: str
    variant: str
    klass: str
    fault: str
    build: Callable[[Backend], tuple[object, object]]
    batches: int = 1

    @property
    def name(self) -> str:
        suffix = f"[{self.variant}]" if self.variant else ""
        return f"{self.sink}{suffix}-{self.fault}"


def _fresh_process_state() -> None:
    """What `tests/conftest.py`'s autouse `_reset_config` does before each test."""
    import conftest
    from log_foundry import config, context

    config._config = config.Config()
    context.reset_context()
    conftest._reset_worker()


@dataclasses.dataclass
class Batch:
    """One batch as the worker's `_emit` handled it: what it carried and what it moved."""

    tokens: list[str]
    runs: tuple[int, int]
    failed_batches: int
    moved: int
    losses_after: tuple[int, int]


def run(row: Row, disabled: frozenset[str] = frozenset()) -> tuple[tuple[str, ...], dict[str, Any]]:
    """Runs one row through the public path and judges it.

    `configure(sink=…)`, one `@trace`d call per batch that logs `N - 2` events inside its span
    (with `span.start` and `span.end`, `N` events in one submission), a `flush()` between batches
    when there are two, and `shutdown(timeout=None)`: the exit drain delivers the last batch with
    the stop event set, so every backoff returns at once and no retry is declined, which shortens
    a row and changes no verdict (*A shutdown shortens a wait; it must never skip work*), and
    `shutdown()` then closes the sink, which is where a client buffer's late report lands.
    `log_id`s come from a patched `model.new_log_id`, so the backend can count them; the worker's
    own `_emit` is wrapped to see each batch it handled, and `_engine._run` to see every run.
    """
    import log_foundry
    from log_foundry import model
    from log_foundry.worker import Worker

    _fresh_process_state()
    backend = Backend(row.fault)
    stderr = io.StringIO()
    counter = iter(range(10**6))
    current = [0]
    handled: list[Batch] = []
    original_id = model.new_log_id
    original_emit = Worker._emit
    model.new_log_id = lambda: f"c92e{current[0] * 100 + next(counter) % 100:04d}"  # type: ignore[assignment]
    with contextlib.redirect_stderr(stderr), runs() as (seen, records):
        sink, target = row.build(backend)

        def emit(self: Worker, event_lists: list[list[dict[str, object]]]) -> None:
            carried = [str(e.get("log_id")) for events in event_lists for e in events]
            mark = len(seen)
            failed = self.failed_batches
            start = losses_total(sink)
            try:
                original_emit(self, event_lists)
            finally:
                after = losses_total(sink)
                if carried:
                    handled.append(
                        Batch(
                            carried,
                            (mark, len(seen)),
                            self.failed_batches - failed,
                            (after[0] - start[0]) + (after[1] - start[1]),
                            after,
                        )
                    )

        Worker._emit = emit  # type: ignore[method-assign]
        try:
            log_foundry.configure(service="spec092", sink=sink)

            @log_foundry.trace
            def work() -> None:
                for _ in range(N - 2):
                    log_foundry.error("m")

            for number in range(row.batches):
                current[0] = number
                counter = iter(range(10**6))
                backend.begin_batch(number)
                work()
                if number < row.batches - 1:
                    log_foundry.flush(timeout=10.0)
            log_foundry.shutdown(timeout=None)
            backend.after_close()
        finally:
            Worker._emit = original_emit  # type: ignore[method-assign]
            model.new_log_id = original_id  # type: ignore[assignment]
    after_close = losses_total(sink)
    batch_tokens = [t for b in handled for t in b.tokens]
    per_event: dict[str, str] | None = {}
    claimed = counted = 0
    disagreement = None
    for batch in handled:
        judged = [d for s, d in seen[batch.runs[0] : batch.runs[1]] if s is target]
        if judged and per_event is not None:
            delivery = judged[-1]
            mine = set(batch.tokens)
            fates = {t: f for t, f in fates_from(records, target).items() if t in mine}
            if delivery.failed:
                fates = dict.fromkeys(mine, "lost")
            elif len(judged) == 1:
                disagreement = disagreement or agrees(fates, delivery)
            per_event.update(fates)
        else:
            per_event = None
            if batch.failed_batches:
                counted += len(batch.tokens)
            else:
                claimed += len(batch.tokens) - min(batch.moved, len(batch.tokens))
                counted += min(batch.moved, len(batch.tokens))
    last = handled[-1].losses_after if handled else (0, 0)
    late = (after_close[0] - last[0]) + (after_close[1] - last[1])
    ambiguous = 0
    if per_event is not None:
        claimed = sum(1 for f in per_event.values() if f == "landed")
        counted = sum(1 for f in per_event.values() if f in ("lost", "skipped"))
        ambiguous = sum(1 for f in per_event.values() if f == "ambiguous")
    report = Report(
        per_event=per_event,
        claimed_landed=claimed,
        counted_lost=counted,
        counted_ambiguous=ambiguous,
        counted_late=late,
        lines=lines_of(stderr.getvalue()),
        disagreement=disagreement,
    )
    expected = row.batches * N
    verdict = judge(batch_tokens, backend, report, disabled)
    if len(batch_tokens) != expected:
        verdict = ("harness-error:batch-count",)
    detail = {
        "held": {t: backend.held[t] for t in batch_tokens if backend.held[t] != 1},
        "claimed": claimed,
        "counted": counted,
        "ambiguous": ambiguous,
        "late": late,
        "lines": lines_of(stderr.getvalue()),
        "disagreement": disagreement,
        "first_line": next(
            (ln for ln in stderr.getvalue().splitlines() if ln.startswith("log-foundry")), ""
        ),
    }
    return verdict, detail
