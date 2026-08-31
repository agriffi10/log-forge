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
    """AC-1. `auto` must keep today's behaviour for a caller who passes nothing."""
    client = UsableClient()
    sink = SentrySink(DSN, client=FakeModule(client))
    sink.emit([dict(ERROR)])
    assert len(client.events) == 0
    assert sink.sent == 1


def test_explicit_http_never_consults_the_client() -> None:
    """AC-1. And it holds none, so `flush()` cannot push an application's own SDK transport."""
    sink, opener = _fallback(backend="http")
    assert sink.client is None
    sink.emit([dict(ERROR)])
    assert len(opener.calls) == 1


def test_explicit_sdk_refuses_rather_than_diverting_to_http() -> None:
    """AC-1 + FR-003. Substituting a backend the caller did not name is the original defect."""
    sink = SentrySink(client=FakeModule(InactiveClient()), backend="sdk")
    assert sink._http is None
    with pytest.raises(SinkDeliveryError):
        sink.emit([dict(ERROR)])


@pytest.mark.parametrize(
    ("kwargs", "why"),
    [
        ({"backend": "postgres"}, "an unknown backend name"),
        ({"backend": "http"}, "http with no dsn"),
    ],
)
def test_a_selection_that_cannot_be_built_raises(kwargs: dict, why: str) -> None:
    """AC-2. Silent substitution is what this spec exists to remove."""
    with pytest.raises(ValueError):
        SentrySink(**kwargs)


def test_the_http_refusal_names_the_selection_not_the_environment() -> None:
    """AC-2. The old wording blames a missing sentry-sdk, which is false when one is installed."""
    with pytest.raises(ValueError, match=r"backend='http'"):
        SentrySink(backend="http")


@pytest.mark.parametrize(
    ("kwargs", "argument"),
    [
        ({"dsn": DSN, "backend": "http", "client": BareClient()}, "client="),
        ({"backend": "sdk", "client": BareClient(), "opener": FakeOpener()}, "opener="),
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


def test_the_real_sdk_initialised_without_a_dsn_is_not_a_usable_backend() -> None:
    """AC-7. `init()` with SENTRY_DSN unset reports itself active and drops every event.

    In a subprocess because `init()` replaces `sys.excepthook` and registers `atexit` callbacks
    that restoring the global client does not undo, and this repo has `atexit`-ordering-sensitive
    tests sharing the process.
    """
    _real_sdk()
    program = textwrap.dedent(
        f"""
        import sys
        sys.path.insert(0, {str(_ROOT / "tests")!r})
        import sentry_sdk
        sentry_sdk.init()
        client = sentry_sdk.get_client()
        assert client.is_active(), "this case needs an SDK that reports itself active"
        assert client.transport is None, "this case needs an SDK with nowhere to send"
        from test_sinks_http import FakeOpener
        from log_foundry.sinks.sentry import SentrySink
        opener = FakeOpener()
        sink = SentrySink({DSN!r}, client=sentry_sdk, opener=opener)
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
