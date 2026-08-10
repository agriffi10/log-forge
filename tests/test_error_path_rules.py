"""SPEC-038 FR-002 AC-4 — the error-path rule, enforced rather than swept once.

FR-002 fixes one unguarded `rollback()`. Its AC-4 asks for the same audit across every sink "as a
*rule*, per `docs/process.md`", because a sweep settles today and a lint settles every day after.

The rule: **a cleanup call made on an object the sink holds must not be unguarded inside an
`except` handler.** Cleanup runs while a failure is already being handled, so a cleanup that
raises replaces the exception the handler was written for -- which in `PostgresSink` meant the
remaining retries were skipped, the counter never moved, no diagnostic was written, and a raw
driver exception reached the worker in place of `SinkDeliveryError`.

Two design notes, because each is the kind of thing that rots:

- **The handler roster is derived and complete**: every `except` in `src/log_foundry`, found by
  walking the AST rather than by listing files. That half cannot silently shrink.
- **The cleanup *predicate* is a vocabulary**, and deliberately so. Whether a method is "cleanup"
  is semantic, not syntactic -- `self._stop.wait()` inside an `except` is a backoff, not cleanup,
  and no rule derived from shape alone can tell the two apart. A driver cleanup method outside
  `_CLEANUP` is therefore outside this rule; that is a stated bound, not an oversight. The scope
  is complete and the predicate is honest about being a list, which is the opposite arrangement
  to the hand-written *rosters* SPEC-032 and SPEC-035 had to replace.

The shape matched is `self.<attr>.<method>(...)` -- a call on something reached *through* `self`.
`self.<method>(...)` is excluded because that is this library's own code, bound by its own
docstring contract; a driver reached instead through a local alias is outside the shape, and no
sink writes one today.
"""

from __future__ import annotations

import ast
import pathlib

# Derived from this file's location rather than from `log_foundry.__file__`, deliberately: an
# installed package can resolve to a different tree than the one under test -- an editable install
# in a worktree resolves back to the original checkout -- and a lint that scans the wrong tree
# passes because it found nothing, which is the one failure mode a lint must not have.
_PKG = pathlib.Path(__file__).resolve().parents[1] / "src" / "log_foundry"

# Methods that release, discard or unwind something a failure handler is cleaning up after.
_CLEANUP = frozenset(
    {
        "rollback",
        "close",
        "abort",
        "disconnect",
        "cancel",
        "release",
        "reset",
        "quit",
        "unsubscribe",
        "shutdown",
    }
)


def _except_handlers(root: pathlib.Path) -> list[tuple[pathlib.Path, ast.ExceptHandler]]:
    """Every `except` handler in the package, by source rather than by import.

    Reading the source keeps modules behind an uninstalled optional extra in scope, which is what
    CI is: no extras are installed there, so an import-driven scan would skip most of `sinks/`.

    Args:
      root: The directory to scan.

    Returns:
      One ``(path, handler node)`` pair per handler.

    Raises:
      None.
    """
    found: list[tuple[pathlib.Path, ast.ExceptHandler]] = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        found.extend(
            (path, node) for node in ast.walk(tree) if isinstance(node, ast.ExceptHandler)
        )
    return found


def _unguarded_cleanup_calls(root: pathlib.Path) -> list[str]:
    """Cleanup calls on a held object inside an `except`, not themselves wrapped in a `try`.

    A call is "guarded" when it sits anywhere inside a nested `try` within the handler, which is
    what the fix looks like: the call moves into a helper whose body is `try: ... except
    Exception: _diag.absorbed(...)`.

    Args:
      root: The directory to scan.

    Returns:
      ``"path:line: source"`` for each violation, in file order.

    Raises:
      None.
    """
    offenders: list[str] = []
    for path, handler in _except_handlers(root):
        guarded = {
            id(sub)
            for node in ast.walk(handler)
            if isinstance(node, ast.Try)
            for sub in ast.walk(node)
        }
        for node in ast.walk(handler):
            if not isinstance(node, ast.Call) or id(node) in guarded:
                continue
            func = node.func
            if (
                isinstance(func, ast.Attribute)
                and func.attr in _CLEANUP
                and isinstance(func.value, ast.Attribute)
                and isinstance(func.value.value, ast.Name)
                and func.value.value.id == "self"
            ):
                offenders.append(
                    f"{path.relative_to(root)}:{node.lineno}: {ast.unparse(node)}"
                )
    return offenders


def test_the_handler_scan_is_not_empty() -> None:
    """The rule below is negative, and an empty scan satisfies a negative perfectly.

    The package carried 71 `except` handlers when this was written. The floor is set below that
    with room to spare, because it guards against the walk *collapsing* -- a bad `rglob`, a parse
    that silently yields nothing, a scan pointed at the wrong tree -- and not against a handler
    being removed in the ordinary course of work.
    """
    handlers = _except_handlers(_PKG)
    assert len(handlers) >= 50, f"the except-handler scan collapsed to {len(handlers)}"


def test_no_cleanup_call_inside_an_except_is_unguarded() -> None:
    """FR-002 AC-4. The rule, applied to every error path in the package.

    `PostgresSink._insert_batch` was the only violation when this was written, and it cost a
    totally lost batch its retries, its counter, its diagnostic and its error type.
    """
    offenders = _unguarded_cleanup_calls(_PKG)
    assert not offenders, (
        "a cleanup call inside an `except` must be guarded, or a failing cleanup replaces the "
        f"failure being handled: {offenders}"
    )


def test_the_rule_rejects_a_real_module_that_reintroduces_the_shape(tmp_path) -> None:
    """The end-to-end path, against a real module rather than a synthetic node.

    This exercises the scan that feeds the judgment -- file discovery, the nested-`try` exclusion
    and the call shape -- which is the half a refactor would silently break, leaving the rule
    above passing because it found nothing rather than because there was nothing to find.
    """
    (tmp_path / "newsink.py").write_text(
        '"""A brand-new sink module."""\n'
        "\n"
        "class BrandNewSink:\n"
        '    """Rolls back inside its own handler, unguarded."""\n'
        "    def emit(self, batch):\n"
        "        try:\n"
        "            self._conn.execute(batch)\n"
        "        except Exception:\n"
        "            self._conn.rollback()\n"
        "\n"
        "class GuardedSink:\n"
        '    """The same cleanup, guarded, which is the fix."""\n'
        "    def emit(self, batch):\n"
        "        try:\n"
        "            self._conn.execute(batch)\n"
        "        except Exception:\n"
        "            try:\n"
        "                self._conn.rollback()\n"
        "            except Exception:\n"
        "                pass\n"
        "\n"
        "class NotCleanup:\n"
        '    """A wait inside a handler is backoff, not cleanup, and is out of scope."""\n'
        "    def emit(self, batch):\n"
        "        try:\n"
        "            self._conn.execute(batch)\n"
        "        except Exception:\n"
        "            self._stop.wait(0.1)\n",
        encoding="utf-8",
    )

    offenders = _unguarded_cleanup_calls(tmp_path)
    assert len(offenders) == 1, f"expected exactly the unguarded rollback, got {offenders}"
    assert "newsink.py" in offenders[0] and "rollback" in offenders[0]
