"""SPEC-043 — which backend `SentrySink` selects, and what it does when neither can deliver.

The doubles here are deliberately not `sentry_sdk`-shaped by accident: each publishes exactly the
members the predicate probes, so a test naming one unusable state cannot pass against a predicate
that only implements another. The real SDK is exercised at the bottom of the file, because an
inactive client is a property of the real library that a double asserts rather than demonstrates.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import textwrap
import threading
from pathlib import Path

import pytest

from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.sentry import SentrySink
from test_sinks_http import FakeOpener

DSN = "https://pubkey@o123.ingest.sentry.io/456"
ERROR = {"level": "ERROR", "message": "boom"}

_ROOT = Path(__file__).resolve().parents[1]


class BareClient:
    """Publishes only `capture_event`, as an injected double or a pre-2.0 SDK does."""

    def __init__(self) -> None:
        self.events: list = []

    def capture_event(self, event: dict) -> None:
        self.events.append(event)


class InactiveClient(BareClient):
    """The uninitialised process's `NonRecordingClient`: inactive, and no transport."""

    transport = None

    def is_active(self) -> bool:
        return False


class ActiveWithoutTransport(BareClient):
    """`init()` with no DSN, and a `close()`d client: active by class, nothing to send through."""

    transport = None

    def is_active(self) -> bool:
        return True


class UsableClient(BareClient):
    """A client that reports itself active and holds a transport."""

    transport = object()

    def is_active(self) -> bool:
        return True


class RaisingProbe(BareClient):
    """A client whose probe raises, which may never be the reason a batch fails (SPEC-025)."""

    @property
    def is_active(self):
        raise RuntimeError("probe exploded")


class FakeModule:
    """A `sentry_sdk`-shaped module: `capture_event` at the top, the members one level down."""

    def __init__(self, client) -> None:
        self._client = client
        self.events: list = []

    def get_client(self):
        return self._client

    def capture_event(self, event: dict) -> None:
        self.events.append(event)


def _fallback(**kwargs) -> tuple[SentrySink, FakeOpener]:
    """Builds a sink whose HTTP fallback records into a returned opener."""
    opener = FakeOpener()
    return SentrySink(DSN, opener=opener, **kwargs), opener


# --- FR-001: an SDK that cannot deliver is not a usable backend --------------------------


def test_an_unusable_sdk_delivers_through_the_fallback_instead() -> None:
    """AC-1. The defect: the SDK's no-op `capture_event` counted a delivery that never happened."""
    module = FakeModule(InactiveClient())
    sink, opener = _fallback(client=module)
    sink.emit([dict(ERROR)])
    assert len(opener.calls) == 1
    assert module.events == []
    assert module._client.events == []
    assert sink.sent == 1


@pytest.mark.parametrize(
    ("state", "client"),
    [
        ("never initialised", InactiveClient),
        ("init() with no dsn", ActiveWithoutTransport),
        ("client.close()d", ActiveWithoutTransport),
    ],
)
def test_every_undeliverable_client_state_is_unusable(state: str, client: type) -> None:
    """AC-2. Two of the three report `is_active()` true, so `is_active` alone misses them."""
    sink, opener = _fallback(client=FakeModule(client()))
    sink.emit([dict(ERROR)])
    assert len(opener.calls) == 1, f"{state} was treated as a usable backend"


def test_a_client_publishing_neither_member_stays_usable() -> None:
    """AC-3. An injected double, a pre-SPEC-043 client and a pre-2.0 SDK all look like this."""
    bare = BareClient()
    sink, opener = _fallback(client=bare)
    sink.emit([dict(ERROR)])
    assert len(bare.events) == 1
    assert opener.calls == []


def test_a_usable_client_is_preferred_over_the_fallback() -> None:
    """AC-1's other half: the predicate must not send a working SDK to the HTTP path."""
    client = UsableClient()
    sink, opener = _fallback(client=FakeModule(client))
    sink.emit([dict(ERROR)])
    assert opener.calls == []
    assert sink.sent == 1


def test_the_backend_is_reconsidered_on_every_emit() -> None:
    """AC-4. An application may call `sentry_sdk.init()` after building the sink."""
    module = FakeModule(InactiveClient())
    sink, opener = _fallback(client=module)
    sink.emit([dict(ERROR)])
    module._client = UsableClient()
    sink.emit([dict(ERROR)])
    assert len(opener.calls) == 1, "the first emit should have used the fallback"
    assert len(module.events) == 1, "the second should have used the now-usable SDK"


def test_a_probe_that_raises_leaves_the_client_usable(capsys) -> None:
    """AC-5. A probe may never be the reason a batch fails, and the fault is named by type."""
    client = RaisingProbe()
    sink, opener = _fallback(client=client)
    sink.emit([dict(ERROR)])
    assert len(client.events) == 1
    assert opener.calls == []
    assert "RuntimeError" in capsys.readouterr().err


def test_the_fallback_is_built_even_when_the_sdk_is_held() -> None:
    """AC-6. Built lazily it would pass AC-1 and miss the worker's one-shot stop-signal offer."""
    sink = SentrySink(DSN, client=FakeModule(UsableClient()), opener=FakeOpener())
    assert sink._http is not None
    signal = threading.Event()
    sink.log_foundry_stop_signal = signal
    assert sink._http.log_foundry_stop_signal is signal


# --- FR-002: a caller can select the backend explicitly ----------------------------------


def test_the_default_prefers_a_usable_sdk() -> None:
    """AC-1. `auto` must keep today's behaviour for a caller who passes nothing.

    Through the opener helper, so the SDK preference is read off a fallback that recorded
    nothing rather than off a real request to sentry.io failing.
    """
    module = FakeModule(UsableClient())
    sink, opener = _fallback(client=module)
    sink.emit([dict(ERROR)])
    assert len(module.events) == 1
    assert opener.calls == [], "a usable SDK must not be sent to the HTTP fallback"
    assert sink.sent == 1


def test_explicit_http_never_consults_the_client() -> None:
    """AC-1. And it holds none, so `flush()` cannot push an application's own SDK transport.

    `client is None` alone cannot tell "did not hold the SDK" from "there was no SDK to hold",
    so the environment supplies the complement (FR-004 AC-2): only where `_import_sdk()` returns
    something is the assertion evidence about this backend rather than about the install.
    """
    from log_foundry.sinks.sentry import _import_sdk

    sink, opener = _fallback(backend="http")
    assert sink.client is None
    if _import_sdk() is not None:
        assert sink.client is None, "an importable SDK must still not be held under backend=http"
    sink.emit([dict(ERROR)])
    assert len(opener.calls) == 1


def test_explicit_sdk_refuses_rather_than_diverting_to_http() -> None:
    """AC-1 + FR-003. Substituting a backend the caller did not name is the original defect."""
    sink = SentrySink(client=FakeModule(InactiveClient()), backend="sdk")
    assert sink._http is None
    with pytest.raises(SinkDeliveryError):
        sink.emit([dict(ERROR)])


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"backend": "postgres"}, "must be one of"),
        ({"backend": "http"}, "backend='http'"),
    ],
    ids=["an unknown backend name", "http with no dsn"],
)
def test_a_selection_that_cannot_be_built_raises(kwargs: dict, message: str) -> None:
    """AC-2. Each row matches its own message, or the wrong guard satisfies the assertion.

    Without `match=`, the unknown-backend row was answered by the *no-DSN* refusal below it and
    stayed green with the name check deleted -- in the gating environment, which is the contract.
    """
    with pytest.raises(ValueError, match=re.escape(message)):
        SentrySink(**kwargs)


def test_the_http_refusal_names_the_selection_not_the_environment() -> None:
    """AC-2. The old wording blames a missing sentry-sdk, which is false when one is installed."""
    with pytest.raises(ValueError, match=r"backend='http'"):
        SentrySink(backend="http")


@pytest.mark.parametrize(
    ("kwargs", "argument"),
    [
        ({"dsn": DSN, "backend": "http", "client": BareClient()}, "client="),
        ({"dsn": DSN, "backend": "sdk", "client": BareClient(), "opener": FakeOpener()}, "opener="),
        ({"backend": "http", "opener": FakeOpener()}, "opener="),
    ],
)
def test_an_argument_the_selection_cannot_use_raises(kwargs: dict, argument: str) -> None:
    """AC-3. Accepting and then ignoring `opener=` is the defect this rule generalises."""
    with pytest.raises(ValueError, match=re.escape(argument)):
        SentrySink(**kwargs)


def test_an_argument_conflict_is_reported_ahead_of_the_refusal() -> None:
    """AC-3 precedence: both fire here, and the conflict names the argument to drop."""
    with pytest.raises(ValueError, match="opener="):
        SentrySink(backend="sdk", opener=FakeOpener())


def test_explicit_sdk_with_no_client_available_raises() -> None:
    """AC-2's third refusal, and the second message that must name the selection.

    Reachable only where the extra is genuinely absent, so it asks the production function which
    leg it is in and asserts the complement -- a question, not a pin (FR-004 AC-2). Without the
    raise, `SentrySink(backend="sdk")` on a host without the extra constructs happily and then
    refuses every batch at emit: a startup misconfiguration deferred into runtime.
    """
    from log_foundry.sinks.sentry import _import_sdk

    if _import_sdk() is None:
        with pytest.raises(ValueError, match=r"backend='sdk'"):
            SentrySink(backend="sdk")
    else:
        assert SentrySink(backend="sdk").client is not None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"dsn": DSN, "backend": "http", "client": BareClient()},
        {"backend": "http", "client": BareClient()},
        {"backend": "sdk", "client": BareClient(), "opener": FakeOpener()},
    ],
    ids=["conflict only", "conflict and refusal", "conflict and no-fallback"],
)
def test_a_conflict_is_reported_ahead_of_any_refusal(kwargs: dict) -> None:
    """AC-3 precedence. Only the middle row has both rules firing; the others are its controls.

    The conflict wins because it names an argument the caller can drop, where the refusal only
    says the selection cannot be built. Asserted here rather than left to the `__init__`
    docstring, which is the only other place the ordering is written down.
    """
    with pytest.raises(ValueError, match=r"client=|opener="):
        SentrySink(**kwargs)


def test_a_get_client_returning_none_leaves_the_held_object_as_the_target() -> None:
    """The descent's failure mode: we asked for a client and got none, so probe what we have.

    The held object reports itself inactive, which is what makes the two possible outcomes
    distinguishable -- probing `None` instead would find no members and call it usable.
    """

    class InactiveModule(FakeModule):
        transport = None

        def is_active(self) -> bool:
            return False

    module = InactiveModule(None)
    sink, opener = _fallback(client=module)
    sink.emit([dict(ERROR)])
    assert module.events == []
    assert len(opener.calls) == 1


def test_the_backend_is_chosen_once_per_batch_not_once_per_event() -> None:
    """The `emit` docstring's claim, which nothing else holds in place.

    A per-event choice would let one batch split across transports partway through, and would
    probe the client once per event on the hot path.
    """
    probes = []

    class CountingModule(FakeModule):
        def get_client(self):
            probes.append(True)
            return self._client

    sink, _ = _fallback(client=CountingModule(UsableClient()))
    sink.emit([dict(ERROR), dict(ERROR), dict(ERROR)])
    assert len(probes) == 1, f"the client was probed {len(probes)} times for one batch"


def test_a_non_callable_is_active_is_treated_as_absent() -> None:
    """Absence means usable, and a bare attribute is not the method the predicate calls."""

    class OddClient(BareClient):
        is_active = False
        transport = object()

    sink, opener = _fallback(client=OddClient())
    sink.emit([dict(ERROR)])
    assert opener.calls == []


def test_max_retries_is_outside_the_conflict_rule() -> None:
    """AC-3's stated bound: an explicit `3` cannot be told from the default."""
    SentrySink(client=BareClient(), backend="sdk", max_retries=17)


def test_opener_alone_does_not_select_the_fallback() -> None:
    """AC-4. A single argument must not both inject a double and switch production behaviour."""
    client = UsableClient()
    sink, opener = _fallback(client=FakeModule(client))
    sink.emit([dict(ERROR)])
    assert opener.calls == []


def test_the_default_selection_is_not_refused_at_construction() -> None:
    """AC-5. `init()` may follow, so refusing here would forbid an ordering the README permits."""
    sink = SentrySink(client=FakeModule(InactiveClient()))
    with pytest.raises(SinkDeliveryError):
        sink.emit([dict(ERROR)])


def test_the_two_lint_asserted_exemption_claims_survive() -> None:
    """AC-6, matched the way the lints match: whitespace-normalised, not a raw `in`."""
    documented = " ".join((SentrySink.__doc__ or "").split())
    for claim, spec in (
        ("**adds no post-close guard**", "SPEC-032 FR-003"),
        ("**no** transport lock", "SPEC-028 FR-002"),
    ):
        assert claim in documented and spec in documented, claim


def test_the_class_docstring_states_the_selection() -> None:
    """AC-6. The table is the only place a caller can read what each combination gives them."""
    documented = " ".join((SentrySink.__doc__ or "").split())
    for name in ("``auto``", "``sdk``", "``http``"):
        assert name in documented, name


def test_flush_still_pushes_a_client_the_predicate_reads_as_unusable() -> None:
    """The Data Model's flush() line: it may become usable before the next batch.

    Without this the claim is prose no test checks -- and an early return keyed on the predicate
    leaves the rest of the suite green, because every other flush test holds a client that
    publishes neither probe member and so reads as usable either way.
    """
    flushed = []

    class FlushingModule(FakeModule):
        def flush(self) -> None:
            flushed.append(True)

    sink, _ = _fallback(client=FlushingModule(InactiveClient()))
    sink.flush()
    assert flushed == [True]


# --- FR-003: neither backend able to deliver is reported, not absorbed -------------------


def test_no_backend_refuses_the_batch_naming_the_qualifying_count() -> None:
    """AC-1. A sink that absorbs total failure is a sink the worker believes (SPEC-026)."""
    sink = SentrySink(client=FakeModule(InactiveClient()))
    with pytest.raises(SinkDeliveryError, match="none of 2"):
        sink.emit([dict(ERROR), {"level": "DEBUG"}, dict(ERROR)])


def test_an_empty_batch_is_still_a_no_op_with_no_backend() -> None:
    """AC-2. A guard at the top of `emit` passes AC-1 and breaks this."""
    SentrySink(client=FakeModule(InactiveClient())).emit([])


def test_an_all_skipped_batch_is_a_success_with_no_backend() -> None:
    """AC-3. The existing test for this builds a working fallback, so it never enters the case."""
    sink = SentrySink(client=FakeModule(InactiveClient()))
    sink.emit([{"level": "DEBUG"}, {"level": "INFO"}])
    assert (sink.skipped, sink.sent) == (2, 0)
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


def test_a_refusal_moves_no_counter_and_writes_no_diagnostic(capsys) -> None:
    """AC-4. Counting a reported failure here as well would report one loss twice (SPEC-032)."""
    sink = SentrySink(client=FakeModule(InactiveClient()))
    with pytest.raises(SinkDeliveryError):
        sink.emit([dict(ERROR)])
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert sink.transport_errors == 0
    assert capsys.readouterr().err == ""


# --- FR-004: the suite selects the backend the way production does -----------------------


def test_no_test_pins_the_sentry_backend_by_patching_the_import() -> None:
    """AC-1. Both shipped spellings are in scope; a name-only grep hits prose in two files."""
    offenders = []
    scanned = 0
    for path in sorted((_ROOT / "tests").rglob("*.py")):
        if path.name == Path(__file__).name:
            continue
        scanned += 1
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if re.search(r"setattr\(.*_import_sdk", line):
                offenders.append(f"{path.relative_to(_ROOT)}:{lineno}: {line.strip()}")
    assert scanned > 60, f"the scan collapsed to {scanned} files -- an absence it cannot see"
    assert not offenders, (
        "a test still pins the Sentry backend by patching the import rather than selecting "
        "it:\n" + "\n".join(offenders)
    )


# --- FR-001 AC-7/AC-8: the real SDK ------------------------------------------------------

_EXTRAS_EXPECTED = os.environ.get("LOG_FOUNDRY_EXTRAS") == "1"


def _real_sdk():
    """Imports `sentry_sdk`, failing rather than skipping where the extras are expected.

    A bare `importorskip` skips silently in the gating leg by design, and would skip just as
    silently in the extras leg if that install regressed -- which is the failure SPEC-041 found
    and this spec exists to stop recurring.
    """
    try:
        import sentry_sdk
    except ImportError:
        if _EXTRAS_EXPECTED:
            raise
        pytest.skip("the sentry extra is not installed in this environment")
    return sentry_sdk


def test_the_real_uninitialised_sdk_is_not_a_usable_backend() -> None:
    """AC-7. Measured on 2.68.1: two emits reported `sent=2` with nothing leaving the process."""
    sentry_sdk = _real_sdk()
    assert not sentry_sdk.get_client().is_active(), "this test needs an uninitialised SDK"
    sink, opener = _fallback(client=sentry_sdk)
    sink.emit([dict(ERROR)])
    assert len(opener.calls) == 1


def _in_a_fresh_interpreter(setup: str, why: str) -> None:
    """Builds a real client per `setup`, then asserts the sink routed around it.

    In a subprocess because both ways of getting a real client -- `init()` and a direct
    `Client(...)` -- replace `sys.excepthook` and register `atexit` callbacks, measured, and
    neither is undone by restoring the global client. This repo has `atexit`-ordering-sensitive
    tests sharing the process.

    Args:
      setup: Statements binding `client` to the real client under test.
      why: What the assertion failure should say about the state being built.

    Returns:
      None.

    Raises:
      AssertionError: If the subprocess did not route around the client.
    """
    program = textwrap.dedent(
        f"""
        import sys
        sys.path.insert(0, {str(_ROOT / "tests")!r})
        import sentry_sdk
        {setup}
        assert client.is_active(), {why!r}
        assert client.transport is None, {why!r}
        from test_sinks_http import FakeOpener
        from log_foundry.sinks.sentry import SentrySink
        opener = FakeOpener()
        sink = SentrySink({DSN!r}, client=client, opener=opener)
        sink.emit([{{"level": "ERROR", "message": "boom"}}])
        assert len(opener.calls) == 1, opener.calls
        print("ok")
        """
    )
    done = subprocess.run(  # noqa: S603
        [sys.executable, "-c", program],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=str(_ROOT),
    )
    assert done.returncode == 0, done.stderr
    assert "ok" in done.stdout


def test_the_real_sdk_initialised_without_a_dsn_is_not_a_usable_backend() -> None:
    """AC-7 + AC-2. `init()` with SENTRY_DSN unset reports itself active and drops every event."""
    _real_sdk()
    _in_a_fresh_interpreter(
        "sentry_sdk.init()\n        client = sentry_sdk.get_client()",
        "init() with no dsn should be active with no transport",
    )


def test_a_real_closed_client_is_not_a_usable_backend() -> None:
    """AC-7 + AC-2's third state, which no double can demonstrate is what the SDK really does.

    A caller who shuts the SDK down before this library's `atexit` drain leaves exactly this
    behind: `is_active()` still true, transport gone, every event dropped in silence.
    """
    _real_sdk()
    _in_a_fresh_interpreter(
        f"client = sentry_sdk.Client(dsn={DSN!r})\n        client.close()",
        "a closed client should be active with no transport",
    )


class FlushingClient(UsableClient):
    """A usable client that counts the SDK-transport pushes `flush()` performs."""

    def __init__(self) -> None:
        super().__init__()
        self.flushes = 0

    def flush(self, *args, **kwargs) -> None:
        self.flushes += 1


def test_close_pushes_the_sdk_transport() -> None:
    """SPEC-048 FR-005. `shutdown()` alone stranded every captured event in the SDK's worker.

    `capture_event` hands to the SDK's **background transport** and returns. `flush()` pushed that
    queue; `close()` forwarded only to the `urllib` fallback, which holds nothing -- so the
    frozen-Lambda path, where the SDK's own timer never fires again, lost the lot. Measured before
    the fix: 25 events captured, `flush() calls after close() = 0`.
    """
    client = FlushingClient()
    sink = SentrySink(client=client)
    sink.emit([{"level": "error", "message": f"m{i}"} for i in range(25)])
    assert len(client.events) == 25 and client.flushes == 0
    sink.close()
    assert client.flushes == 1, "close() pushes the SDK transport"


def test_a_raising_sdk_flush_does_not_stop_the_fallback_release(capsys) -> None:
    """close() is an isolation boundary: a failing flush must not skip the release below it.

    Asserted on the release itself rather than on the absence of an exception -- "close() did not
    raise" would pass against a close that returned early and released nothing.
    """

    class RaisingFlush(UsableClient):
        def flush(self, *args, **kwargs):
            raise RuntimeError("the transport is wedged")

    from log_foundry.sinks import sentry as sentry_mod

    released: list[object] = []
    real_release = sentry_mod._lifecycle.release

    def spy(sink_obj, *, owner):
        released.append(sink_obj)
        return real_release(sink_obj, owner=owner)

    opener = FakeOpener()
    sink = SentrySink(dsn=DSN, client=RaisingFlush(), opener=opener)
    fallback = sink._http
    assert fallback is not None
    monkeypatched = pytest.MonkeyPatch()
    monkeypatched.setattr(sentry_mod._lifecycle, "release", spy)
    try:
        sink.close()
    finally:
        monkeypatched.undo()
    assert released == [fallback], (
        "the fallback is released even though the SDK flush raised; asserting only that close() "
        "did not raise would pass against a close that returned early and released nothing"
    )
    assert "RuntimeError" in capsys.readouterr().err, "and the failure is announced by type"


def test_a_client_less_sink_attempts_no_sdk_flush(capsys) -> None:
    """`backend="http"` holds no SDK queue, so close() must not push one on the app's behalf.

    Asserted on the *release*, not on `client is None` — that last is state the test arranged.

    **What this can and cannot show**, because the difference was got wrong once here. It cannot
    show that `flush()`'s `None` guard is what protects this path: mutating that guard away leaves
    the test green, since `getattr(None, "flush", None)` returns `None` and `callable(None)` is
    `False`, so the call is inert either way. With no client there is nothing a flush *could*
    reach, which makes "no SDK flush attempted" close to tautological. What is not tautological,
    and what this pins, is that adding the flush to `close()` did not short-circuit the release
    below it — a `close()` that returned early, or raised into its own absorbing guard, would fail
    both assertions.
    """
    from log_foundry.sinks import sentry as sentry_mod

    released: list[object] = []
    real_release = sentry_mod._lifecycle.release

    def spy(sink_obj, *, owner):
        released.append(sink_obj)
        return real_release(sink_obj, owner=owner)

    opener = FakeOpener()
    sink = SentrySink(dsn=DSN, backend="http", opener=opener)
    assert sink.client is None, "the http backend holds no SDK client"
    patch = pytest.MonkeyPatch()
    patch.setattr(sentry_mod._lifecycle, "release", spy)
    try:
        sink.close()
    finally:
        patch.undo()

    assert capsys.readouterr().err == "", "close() absorbed and announced nothing"
    assert released == [sink._http], "and close() still ran through to the fallback release"


def test_the_close_flush_is_not_suppressed_on_a_repeat_close() -> None:
    """A second close flushes again, deliberately.

    This sink adds **no post-close guard** (SPEC-032 FR-003), so a batch emitted after close()
    still reaches Sentry -- which means events can legitimately be captured *between* two closes.
    A flag suppressing the second flush would strand exactly what FR-005 exists to un-strand. A
    repeat flush of a drained queue is a no-op, so the idempotence that matters is preserved.
    """
    client = FlushingClient()
    sink = SentrySink(client=client)
    sink.emit([{"level": "error", "message": "first"}])
    sink.close()
    sink.emit([{"level": "error", "message": "captured after the close"}])
    sink.close()
    assert client.flushes == 2, "the second close flushes what the second emit captured"
    assert len(client.events) == 2
