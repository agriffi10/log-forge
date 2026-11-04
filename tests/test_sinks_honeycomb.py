"""HoneycombSink reads the per-event verdict in its ``200`` (architecture.md §12, invariants 2 and 7)."""

from __future__ import annotations

import json
from typing import Any

import pytest

import log_foundry
from log_foundry.sinks.base import SinkDeliveryError, SinkLosses
from log_foundry.sinks.honeycomb import HoneycombSink
from log_foundry.worker import Worker
from test_sinks_http import FakeOpener, FakeResponse
from test_sinks_http_url import _assert_chains_nothing, _assert_never_echoes, _assert_printable

_SERVER_TEXT = "Event has too many columns."
"""An ``error`` Honeycomb writes beside a rejected event; it must never reach stderr."""


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neutralize backoff waits, as ``test_sinks_http`` does for its own module."""
    monkeypatch.setattr("log_foundry.sinks.http.wait", lambda _delay, _stop=None: None)


def _verdict(*statuses: object) -> FakeResponse:
    """A ``200`` whose body is Honeycomb's per-event array, one entry per status given."""
    entries = [
        {"status": s} if s in (200, 202) else {"status": s, "error": _SERVER_TEXT}
        for s in statuses
    ]
    return FakeResponse(200, json.dumps(entries).encode())


def _sink(*responses: Any, **kwargs: Any) -> tuple[HoneycombSink, FakeOpener]:
    opener = FakeOpener(list(responses))
    kwargs.setdefault("max_retries", 0)
    return HoneycombSink("key", "dataset", opener=opener, **kwargs), opener


def _batch(n: int) -> list[dict[str, object]]:
    return [{"i": i} for i in range(n)]


def _lost_lines(err: str) -> list[str]:
    return [line for line in err.splitlines() if line.startswith("log-foundry: lost")]


class _EchoOpener(FakeOpener):
    """Answers every request with one entry per event it carried, rejecting the ``i`` listed."""

    def __init__(self, reject: frozenset[int] = frozenset()) -> None:
        super().__init__()
        self._reject = reject

    def __call__(self, request: Any, timeout: float | None = None) -> FakeResponse:
        super().__call__(request, timeout)
        events = [entry["data"] for entry in json.loads(request.data)]
        return _verdict(*(400 if event.get("i") in self._reject else 202 for event in events))


# --- the verdict is read --------------------------------------------------------------------


def test_every_event_accepted_counts_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    sink, opener = _sink(_verdict(202, 202, 202))
    sink.emit(_batch(3))
    assert len(opener.calls) == 1
    assert sink.losses() == SinkLosses(dropped=0, failed=0)
    assert (sink.rejected_events, sink.dropped_unadjudicated) == (0, 0)
    assert _lost_lines(capsys.readouterr().err) == []


def test_a_rejected_event_is_counted_announced_and_never_resent(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The defect: this response used to count nothing and write nothing (invariant 2)."""
    sink, opener = _sink(_verdict(202, 400, 202), max_retries=3)
    sink.emit(_batch(3))
    assert len(opener.calls) == 1, "a partial success is never re-sent (invariant 7)"
    assert (sink.rejected_events, sink.dropped_unadjudicated) == (1, 0)
    assert sink.losses() == SinkLosses(dropped=0, failed=1)
    err = capsys.readouterr().err
    assert _lost_lines(err) == [
        "log-foundry: lost 1 event(s); HoneycombSink, rejected by the server"
    ]
    assert _SERVER_TEXT not in err, "the server's text is never written (invariant 11)"


@pytest.mark.parametrize("status", [200, 202, 299])
def test_a_2xx_status_is_an_acceptance(status: int) -> None:
    sink, _ = _sink(_verdict(status, 202))
    sink.emit(_batch(2))
    assert sink.losses() == SinkLosses(dropped=0, failed=0)


@pytest.mark.parametrize("status", [199, 300, 400, 429, 500, -1])
def test_any_other_integer_status_is_a_rejection(status: int) -> None:
    sink, _ = _sink(_verdict(status, 202))
    sink.emit(_batch(2))
    assert (sink.rejected_events, sink.dropped_unadjudicated) == (1, 0)


def test_every_event_rejected_raises() -> None:
    """Nothing landed, so the raise is safe and is how the worker sees it (invariant 7)."""
    sink, opener = _sink(_verdict(400, 400))
    with pytest.raises(SinkDeliveryError):
        sink.emit(_batch(2))
    assert len(opener.calls) == 1
    assert (sink.rejected_events, sink.dropped_unadjudicated) == (2, 0)


def test_a_rejected_chunk_beside_an_accepted_one_does_not_raise() -> None:
    sink, opener = _sink(_verdict(400, 400), _verdict(202, 202), max_batch_count=2)
    sink.emit(_batch(4))
    assert len(opener.calls) == 2
    assert sink.losses() == SinkLosses(dropped=0, failed=2)


def test_a_413_split_reads_each_smaller_chunk_against_its_own_length() -> None:
    """The re-chunked requests are adjudicated at their own sizes, not the original chunk's."""
    opener = _EchoOpener(reject=frozenset({1}))
    responses = iter([FakeResponse(413, b"")])

    def first_413(request: Any, timeout: float | None = None) -> FakeResponse:
        response = next(responses, None)
        if response is None:
            return opener(request, timeout)
        opener.calls.append({"body": request.data})
        return response

    sink = HoneycombSink("key", "dataset", opener=first_413, max_retries=0)
    sink.emit(_batch(4))
    sizes = [len(json.loads(call["body"])) for call in opener.calls]
    assert sizes[0] == 4 and sum(sizes[1:]) == 4 and len(sizes) > 2
    assert (sink.rejected_events, sink.dropped_unadjudicated) == (1, 0)


# --- a body that is no verdict --------------------------------------------------------------

_NO_VERDICT = {
    "empty, or unreadable after the status line": b"",
    "an object": b"{}",
    "not JSON": b"<html>ok</html>",
    "a scalar": b"202",
    "shorter than the chunk": b'[{"status": 202}, {"status": 202}]',
    "longer than the chunk": b'[{"status": 202}, {"status": 202}, {"status": 202}, {"status": 400}]',
    "longer than the chunk by one entry with no status": (
        b'[{"status": 400}, {"error": "x"}, {"status": 202}, {"status": 202}]'
    ),
    "an entry that is not an object": b'[{"status": 202}, 202, {"status": 202}]',
    "an entry with no status": b'[{"status": 202}, {"error": "x"}, {"status": 400}]',
    "a string status": b'[{"status": 202}, {"status": "400"}, {"status": 400}]',
    "a bool status": b'[{"status": 202}, {"status": true}, {"status": 400}]',
    "a float status": b'[{"status": 202}, {"status": 400.0}, {"status": 400}]',
    "a null status": b'[{"status": 202}, {"status": null}, {"status": 400}]',
    "nested past the recursion guard": b"[" * 100_000,
}


@pytest.mark.parametrize("body", _NO_VERDICT.values(), ids=list(_NO_VERDICT))
def test_a_2xx_with_no_readable_verdict_is_counted_whole_and_never_resent(
    body: bytes, capsys: pytest.CaptureFixture[str]
) -> None:
    """All of the chunk or none of it (SPEC-018): no readable entry is charged as a rejection."""
    sink, opener = _sink(FakeResponse(200, body), max_retries=3)
    sink.emit(_batch(3))
    assert len(opener.calls) == 1, "the request succeeded, so a re-send could duplicate"
    assert (sink.rejected_events, sink.dropped_unadjudicated) == (0, 3)
    assert sink.losses() == SinkLosses(dropped=0, failed=3)
    lines = _lost_lines(capsys.readouterr().err)
    assert len(lines) == 1
    assert lines[0].startswith("log-foundry: lost 3 event(s); HoneycombSink could not adjudicate")
    assert "they may have landed; not retried" in lines[0]


# --- both delivery paths --------------------------------------------------------------------


def test_through_a_worker_a_partial_success_is_delivered_once() -> None:
    sink, opener = _sink(_verdict(202, 400, 202))
    worker = Worker(sink, batch_size=3, max_retries=3)
    try:
        worker.submit(_batch(3))
        delivered = bool(worker.flush(timeout=5.0))
        failed_batches = worker.health().failed_batches
    finally:
        worker.stop()
    assert delivered and failed_batches == 0
    assert len(opener.calls) == 1, "no event landed twice"
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


def test_through_a_worker_every_event_rejected_moves_failed_batches() -> None:
    """The worker's retry recounts per attempt — an upper bound, like ``failed``."""
    sink, opener = _sink(_verdict(400, 400, 400))
    worker = Worker(sink, batch_size=3, max_retries=3)
    try:
        worker.submit(_batch(3))
        delivered = bool(worker.flush(timeout=5.0))
        failed_batches = worker.health().failed_batches
    finally:
        worker.stop()
    assert not delivered and failed_batches > 0
    assert sink.rejected_events == 3 * len(opener.calls) == 12


def test_on_the_orphan_path_a_rejected_event_is_an_orphan_loss() -> None:
    sink, _ = _sink(_verdict(400))
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("outside a span")
    assert log_foundry.health().orphan_lost == 1
    assert (sink.rejected_events, sink.dropped_unadjudicated) == (1, 0)


def test_on_the_orphan_path_an_unadjudicated_event_is_the_sinks_loss_alone() -> None:
    sink, _ = _sink(FakeResponse(200, b""))
    log_foundry.configure(service="t", sink=sink)
    log_foundry.info("outside a span")
    assert log_foundry.health().orphan_lost == 0
    assert sink.losses() == SinkLosses(dropped=0, failed=1)


_KEY = "hc-secret-key"
"""A distinctive API key, so a refusal that echoed it would be visible."""

_REFUSED_DATASETS = [
    "d#x",
    "d?x",
    "a/b",
    "/d",
    "d/",
    "d\\x",
    ".",
    "..",
    ".. ",
    "%2e",
    ".%2E",
    "%2e.",
    "%2E%2e",
    "",
    " ",
    "\n",
]
"""Each dataset that constructed and then sent its events somewhere other than the one named."""


@pytest.mark.parametrize("dataset", _REFUSED_DATASETS)
def test_a_dataset_that_is_not_one_path_segment_is_refused_at_construction(dataset: str) -> None:
    with pytest.raises(
        ValueError, match=r"^HoneycombSink dataset must be one path segment: "
    ) as caught:
        HoneycombSink(_KEY, dataset)

    assert _KEY not in str(caught.value)


@pytest.mark.parametrize(
    ("dataset", "url"),
    [
        ("d", "https://api.honeycomb.io/1/batch/d"),
        ("d.x", "https://api.honeycomb.io/1/batch/d.x"),
        (".d", "https://api.honeycomb.io/1/batch/.d"),
        ("...", "https://api.honeycomb.io/1/batch/..."),
        ("%2e%2e%2e", "https://api.honeycomb.io/1/batch/%2e%2e%2e"),
        ("d%20x", "https://api.honeycomb.io/1/batch/d%20x"),
        ("d%2Fx", "https://api.honeycomb.io/1/batch/d%2Fx"),
        ("d;x", "https://api.honeycomb.io/1/batch/d;x"),
        ("d:x", "https://api.honeycomb.io/1/batch/d:x"),
        ("d%40x", "https://api.honeycomb.io/1/batch/d%40x"),
        ("d-x_1", "https://api.honeycomb.io/1/batch/d-x_1"),
        ("d\n", "https://api.honeycomb.io/1/batch/d"),
    ],
)
def test_a_dataset_that_delivered_before_still_goes_where_it_went(dataset: str, url: str) -> None:
    """The positive control: every working dataset constructs, and its URL is unchanged.

    ``d@x`` delivered as written too, and is refused now as every HTTP sink URL holding an ``@``
    outside its host is (the test below); ``d%40x`` is how that dataset is named.
    """
    opener = FakeOpener([_verdict(202)])
    sink = HoneycombSink(_KEY, dataset, opener=opener, max_retries=0)

    sink.emit(_batch(1))

    assert sink.url == url
    assert opener.calls[0]["url"] == url


@pytest.mark.parametrize("dataset", ["d@x", "SECRET@d", "d@SECRET x"])
def test_a_dataset_holding_an_at_sign_is_refused_with_the_dataset_remedy(dataset: str) -> None:
    """Not the shared URL reason, which is about a password ending a host: a dataset has none."""
    with pytest.raises(
        ValueError,
        match=r"^HoneycombSink dataset must not hold an '@'; write an '@' in a dataset as '%40'$",
    ) as caught:
        HoneycombSink(_KEY, dataset)

    _assert_never_echoes(caught.value, "SECRET", _KEY, "localhost", "host")
    _assert_chains_nothing(caught.value)


_UNCARRIED = [
    ("d x", "InvalidURL"),
    ("d\x00x", "InvalidURL"),
    ("d\ud800", "UnicodeEncodeError"),
    ("d\u20ac", "UnicodeEncodeError"),
    ("\u4f8b", "UnicodeEncodeError"),
]
"""Datasets ``http.client`` cannot carry in a request path, and the error it raises for each."""


@pytest.mark.parametrize(("dataset", "kind"), _UNCARRIED)
def test_a_dataset_a_request_path_cannot_carry_is_refused_quoting_the_dataset(
    dataset: str, kind: str
) -> None:
    with pytest.raises(
        ValueError,
        match=rf"^HoneycombSink dataset cannot be carried in a request path \({kind}\): ",
    ) as caught:
        HoneycombSink(_KEY, dataset)

    assert str(caught.value).endswith(f": {dataset!r}")
    assert "localhost" not in str(caught.value)
    _assert_chains_nothing(caught.value)
    _assert_printable(caught.value)


@pytest.mark.parametrize(
    ("dataset", "kind"),
    [("a%40SECRET x", "InvalidURL"), ("a\uff20SECRET", "UnicodeEncodeError")],
)
def test_an_uncarried_dataset_holding_an_encoded_at_sign_is_not_quoted(
    dataset: str, kind: str
) -> None:
    """``%40`` and a fullwidth ``@`` are ``@`` to :func:`_holds_at`, so what precedes is hidden."""
    with pytest.raises(
        ValueError,
        match=rf"^HoneycombSink dataset cannot be carried in a request path \({kind}\); ",
    ) as caught:
        HoneycombSink(_KEY, dataset)

    assert str(caught.value).endswith(
        "; the dataset is not quoted, since it holds an '@', '/', '?' or '#'"
    )
    _assert_never_echoes(caught.value, "SECRET", _KEY, "localhost")
    _assert_chains_nothing(caught.value)


@pytest.mark.parametrize("dataset", ["d\uff03SECRET", "d\uff1fapi_key=SECRET", "d\uff0fSECRET"])
def test_an_uncarried_dataset_holding_a_fullwidth_delimiter_is_not_quoted(dataset: str) -> None:
    """NFKC turns these into ``#``, ``?`` and ``/``, so what follows is hidden, as for ASCII."""
    with pytest.raises(
        ValueError,
        match=r"^HoneycombSink dataset cannot be carried in a request path "
        r"\(UnicodeEncodeError\); ",
    ) as caught:
        HoneycombSink(_KEY, dataset)

    assert str(caught.value).endswith(
        "; the dataset is not quoted, since it holds an '@', '/', '?' or '#'"
    )
    _assert_never_echoes(caught.value, "SECRET", "api_key", _KEY, "localhost")
    _assert_chains_nothing(caught.value)


@pytest.mark.parametrize("dataset", ["d?api_key=SECRET", "d#SECRET", "SECRET/d", "a@SECRET/d"])
def test_a_refused_dataset_holding_a_delimiter_or_an_at_sign_is_not_quoted(dataset: str) -> None:
    with pytest.raises(
        ValueError, match=r"^HoneycombSink dataset must be one path segment: "
    ) as caught:
        HoneycombSink(_KEY, dataset)

    assert "SECRET" not in str(caught.value)
    assert str(caught.value).endswith(
        "; the dataset is not quoted, since it holds an '@', '/', '?' or '#'"
    )


@pytest.mark.parametrize("dataset", ["d\\x", "..", "%2e", " "])
def test_a_refused_dataset_with_no_delimiter_or_at_sign_is_still_quoted(dataset: str) -> None:
    with pytest.raises(ValueError, match=r"^HoneycombSink dataset must be one path segment: ") as e:
        HoneycombSink(_KEY, dataset)

    assert str(e.value).endswith(f"backslash: {dataset!r}")
