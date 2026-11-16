"""SPEC-089 FR-005: the README's Lambda handler idiom reads the counts a `flush()` result carries."""

import ast
import io
import pathlib
import re
import tokenize

import log_foundry as lf
from conftest import install_worker
from ledger_sinks import PlanSink, every
from log_foundry import Outcome
from log_foundry import worker as worker_mod

README = pathlib.Path(__file__).resolve().parent.parent / "README.md"


def _handler_block(text: str) -> str:
    """The one fenced Python block holding the Lambda handler and its `flush()`."""
    blocks = re.findall(r"```python\n(.*?)```", text, flags=re.DOTALL)
    found = [
        b for b in blocks if "def handler(event, context):" in b and "drained = lf.flush()" in b
    ]
    assert len(found) == 1, f"expected one handler block, found {len(found)}"
    return found[0]


def _condition(text: str) -> ast.expr:
    """The test of the `if` in the handler block that reads `drained`."""
    tree = ast.parse(_handler_block(text))
    tests = [
        node.test
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and any(isinstance(n, ast.Name) and n.id == "drained" for n in ast.walk(node.test))
    ]
    assert len(tests) == 1, f"expected one condition reading `drained`, found {len(tests)}"
    return tests[0]


def _fires(text: str, drained: object, health: object) -> bool:
    """Evaluates the idiom's condition against a `flush()` result and a `health()` snapshot.

    `eval` runs only the condition this repo's own README holds, with no builtins, which is the
    point: the test reads the idiom a caller would copy rather than a restatement of it.
    """
    code = compile(ast.Expression(_condition(text)), "README.md", "eval")
    scope = {"drained": drained, "h": health}
    return bool(eval(code, {"__builtins__": {}}, scope))  # noqa: S307


def _flush_ten_spans(outcome: Outcome) -> object:
    """FR-002's case: ten spans on a worker no submission triggers, then one `flush()`."""

    @lf.trace
    def span(i: int) -> None:
        lf.info("inside", i=i)

    sink = PlanSink(every(outcome))
    lf.configure(sink=sink)
    install_worker(worker_mod.Worker(sink, batch_size=10_000, flush_interval=3600.0, max_retries=0))
    for i in range(10):
        span(i)
    return lf.flush(10)


def test_the_lambda_idiom_fires_on_an_all_dropped_flush() -> None:
    """FR-005 AC-1: a truthy flush over a total loss is seen by the handler written from it."""
    drained = _flush_ten_spans(Outcome.DROPPED)
    assert drained and drained.sink_lost == 30, drained
    assert _fires(README.read_text(encoding="utf-8"), drained, lf.health())


def test_the_lambda_idiom_is_quiet_on_a_clean_flush() -> None:
    """FR-005 AC-2: in a process that has lost nothing, the idiom says nothing."""
    drained = _flush_ten_spans(Outcome.ACCEPTED)
    assert drained == lf.FlushResult(ok=True), drained
    assert lf.health().events.lost == 0
    assert not _fires(README.read_text(encoding="utf-8"), drained, lf.health())


def test_the_lambda_idiom_reads_lost_and_the_sink_term() -> None:
    """FR-005 AC-3: the condition reads both terms, and its comment says a truthy result can lose."""
    text = README.read_text(encoding="utf-8")
    condition = _condition(text)
    rendered = ast.unparse(condition)
    assert "drained.lost" in rendered, rendered
    assert "h.sink and (h.sink.dropped or h.sink.failed)" in rendered, rendered
    comments = " ".join(
        token.string.lstrip("#").strip()
        for token in tokenize.generate_tokens(io.StringIO(_handler_block(text)).readline)
        if token.type == tokenize.COMMENT
    )
    assert "a truthy `drained` can still carry a non-zero `lost`" in comments, comments


def test_the_readme_drops_the_delivery_wording_and_names_the_counts() -> None:
    """FR-005 AC-4."""
    text = README.read_text(encoding="utf-8")
    for phrase in ("truthy when everything landed", "evidence of delivery"):
        assert phrase not in text, phrase
    rows = [line for line in text.splitlines() if line.startswith('| `"sink-flush"`')]
    assert len(rows) == 1 and "Everything queued reached the sink" not in rows[0], rows
    section = text[text.index("`flush(timeout=5.0)` returns a **`FlushResult`**") :]
    section = section[: section.index("Falsy carries a `reason`")]
    for name in ("`lost`", "`overflow_lost`", "`batch_lost`", "`sink_lost`", "`drain_lost`"):
        assert name in section, name
    assert "`stranded`" in section
    assert "**window**" in section
    assert "**over-count**" in section
    assert "A **zero** count covers\nonly the terms the ledger counts" in section
