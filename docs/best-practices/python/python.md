# Python Best Practices — Agent Reference

> Token-efficient rulebook for LLM coding agents writing/refactoring Python in **log-forge** (runtime **Python ≥ 3.13**; a `src`-layout library, distribution `log-forge` / import `log_forge`). Distilled from **PEP 8** (+ PEP 257 docstrings, PEP 484/526 typing) and the **[Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)**, adapted to this repo. Each section is self-contained; load only what the task needs. Rules are imperative; ✅ = do, 🔴 = don't.
>
> **Run the formatter/linter — don't hand-format.** Mechanics (§2, §3) are owned by `ruff`; this doc is for the choices a tool can't make (naming, interfaces, idioms) and for review. Style is *guidance*: the repo's configured tools win (line length **100**, quote style), and "a foolish consistency is the hobgoblin of little minds" — when a rule hurts readability in a specific case, deviate and flag it (§1).
>
> **The core stays dependency-free.** New runtime deps belong behind an optional extra (as `sqs`/`boto3` is); note them in `CLAUDE.md` first. This doc governs *language* style; the *design* rules (non-swallowing decorator, no arg/return auto-capture, structured-JSON-only) live in `CLAUDE.md` / `architecture.md` — when both apply, follow both.

## How to use this doc
- Find the relevant section ID(s) in the Index below; read only those. Sections are cross-linked by ID (e.g. "see §7").
- Defer to the repo's configured tools (line length, formatter, import order, typing strictness) over the defaults here; when a rule conflicts with existing code, follow the rule and **flag the conflict** unless the user says otherwise.
- Repo defaults: format/lint with **`ruff`** (line-length 100), type-check with **`mypy --strict`** over `src`, test with **`pytest`** (`asyncio_mode=auto`, `--strict-markers`) + `pytest-asyncio`. Runtime = 3.13; core has **no runtime deps** (optional `sqs` extra pulls `boto3`).

## Index
- **§1 Tooling & the "foolish consistency" rule** — formatter/linter/type-checker own the mechanics; when to deviate; don't churn diffs.
- **§2 Layout & formatting** — indentation, line length, continuation, operator breaks, blank lines, one statement per line.
- **§3 Whitespace** — in expressions and statements; slices; the `=` rule.
- **§4 Imports** — module-level, absolute, one per line, grouped stdlib→third-party→local; no wildcards; `typing`/`collections.abc` exempt.
- **§5 Naming** — `lower_with_under` funcs/vars, `CapWords` classes, `CAPS_WITH_UNDER` constants; `_` internal; descriptive intent, no cryptic abbreviations.
- **§6 Public vs internal interfaces** — `__all__`, leading underscore, backward-compat promise.
- **§7 Exceptions** — built-ins for precondition violations; subclass for domain errors; no bare `except`; no `assert` for logic; small `try`; chain with `from`.
- **§8 Types & annotations** — annotate public APIs; `X | None` explicit; abstract container types in signatures; no mutable default args; annotated-`=` spacing.
- **§9 Functions, classes & properties** — one responsibility; small focused functions; properties only for cheap derived access; avoid `staticmethod`; `return` over `print`.
- **§10 Comprehensions, iterators & generators** — simple comprehensions only; default iterators; generators for streaming; `Yields:`.
- **§11 Strings, logging & errors** — f-strings/`%`/`.format`, never `+` in loops; lazy `%`-logging; precise, greppable error messages.
- **§12 Truthiness & conditionals** — implicit falsiness with caveats; `is None`; `isinstance`; `startswith`/`endswith`; simple ternaries.
- **§13 Resources, global state & threading** — `with` for files/clients/closeables; avoid mutable global state; module-level constants OK; `contextvars`/`queue`/`threading` for concurrency.
- **§14 Docstrings & comments** — docstrings are the primary source of context; triple-quoted summary line; `Args`/`Returns`/`Raises`; no verbose comments, comment *why* not *what*; `TODO:` + tracked ref.
- **§15 Modules, main & power features** — module docstring; `main()` behind `if __name__ == '__main__'`; avoid metaclasses/reflection/`__del__` cleanup.

---

## §1 Tooling & the "foolish consistency" rule
Every file is auto-formatted, linted, and type-checked — style is not hand-argued.

- ✅ Run the **formatter** (`ruff format`, Black-compatible) and **linter** (`ruff check`) on every file; let them own the §2/§3 mechanics. Keep both green before a PR (the repo's format/lint/test gates CI). ✅ Run **`mypy --strict`** over `src` (§8) — no untyped defs; ship the `py.typed` marker.
- ✅ Suppress a warning **narrowly and with a reason** — a line-level `# noqa: <rule>` / `# type: ignore[<code>]` with a short explanation when the rule name isn't self-explanatory. Searchable suppressions can be revisited.
- 🔴 Don't blanket-disable rules for a whole file to dodge one line, and don't leave an unexplained `# noqa`. ✅ Fix the issue or justify the exception.
- ✅ For an unused-but-required argument (callback/interface signature), delete it at the top (`del unused_arg  # Unused.`) or prefix `unused_`.
- ✅ **Style is guidance, not law:** when a rule would *reduce* readability, or clash with surrounding code or an existing (style-violating) API, **deviate and flag it** rather than churn the codebase.
- 🔴 Don't reformat unrelated code to satisfy style inside a feature PR — it buries the real change in diff noise. **Be consistent** with the surrounding file; prefer converging on the newer style over perpetuating an old one.

## §2 Layout & formatting
Formatter-driven; the rules below are what the formatter enforces — know them for hand-edits.

- ✅ **4-space indent**, never tabs; never mix tabs and spaces. One statement per line (a single `if foo: bar()` with no `else` is the only same-line allowance; never for `try`/`except`). 🔴 No `;`-joined statements.
- ✅ **Line length: 100** (the repo's configured `ruff` limit) — don't argue with the configured value. Long imports, URLs/paths in comments, and `# noqa`-style directives may exceed.
- 🔴 **No backslash line continuation.** ✅ Use implicit joining inside `()`/`[]`/`{}`; add parens around an expression if needed. Continuation lines either align with the opening delimiter or use a 4-space hanging indent (no argument on the opening line). Break at the highest syntactic level.
- ✅ **Break *before* binary operators** (Knuth style) so operator and operand line up:
  ```python
  income = (gross_wages
            + taxable_interest
            - ira_deduction)
  ```
- ✅ Blank lines: **2** around top-level functions/classes, **1** between methods; use single blank lines sparingly inside a function to separate logical steps. No blank line right after a `def`.
- ✅ Trailing comma in a multi-line sequence only when the closing bracket is on its own line (and for 1-tuples: `(foo,)`); it also cues the formatter to one-item-per-line. Don't vertically align tokens (`=`, `:`, `#`) across lines. End the file with a single newline; no trailing whitespace.

## §3 Whitespace
Standard typographic spacing; a few Python-specific traps.

- 🔴 No spaces just inside brackets/parens/braces: `spam(ham[1], {'eggs': 2})`, not `spam( ham[ 1 ] )`.
- 🔴 No space before `,` `;` `:`; ✅ one space after them (except at line end). No space before a call's `(` or an index's `[`: `fn()`, `data['key']`.
- ✅ **Slices:** treat `:` as a binary operator with equal spacing on both sides; drop spaces when a slot is omitted — `ham[1:9]`, `ham[lower : upper + 1]`, `ham[: n]`.
- ✅ One space around binary operators (assignment, comparisons `== < > != <= >= in not in is is not`, booleans `and or not`). Use judgment around arithmetic; for mixed precedence you may space only the lowest-precedence operators.
- 🔴 **No spaces around `=` for keyword args / unannotated defaults:** `f(x=1)`, `def configure(service, env='dev')`. ✅ **But** add spaces when the parameter is **annotated**: `def f(x: int = 0)` (§8).

## §4 Imports
Import modules, not individual names (typing is the exception); keep them absolute and ordered.

- ✅ `import x` for packages/modules; `from x import y` where `y` is a **module** (`from log_forge import context`), then reference `context.current_span()`. 🔴 No `import os, sys` on one line (`from pkg import a, b` is fine).
- ✅ **Exception — typing:** import symbols directly from `typing` / `collections.abc` (`from collections.abc import Mapping, Sequence`; `from typing import Any, Protocol, cast, TYPE_CHECKING`) — multiple per line allowed.
- ✅ `import y as z` only for standard abbreviations (`import numpy as np`) or to resolve a genuine collision / inconveniently long name.
- ✅ Prefer **absolute imports** (full package path). Explicit relative (`from . import context`) is tolerable within the package; 🔴 never implicit relative imports. **Break import cycles with a local (function-body) import** — e.g. `config.configure()` imports `StdoutSink` locally, and `model`/`decorator` import `config` lazily where needed (see the module map's dependency arrows).
- 🔴 Avoid wildcard imports (`from x import *`) — they obscure what's in scope.
- ✅ **One import per line**; group and order with a blank line between groups: (1) `from __future__`, (2) stdlib, (3) third-party (e.g. `boto3`, only inside the SQS sink), (4) local sub-packages — sorted lexicographically within each group. Imports go at the top, after the module docstring and any module dunders (`__all__`, `__version__`), before constants — **except** the deliberate cycle-breaking local imports above.

## §5 Naming
Names describe **intent**; casing follows the standard table; visibility governs the leading underscore.

- ✅ Descriptive names, proportional to scope — `new_trace_id()` over `nti()`. Casing rules are necessary, not sufficient.
- ✅ `module_name`/`package_name` (`lower_with_under`; packages prefer no underscores), `function_name`, `method_name`, `local_var_name`, `parameter_name`, `instance_var_name`, `global_var_name`.
- ✅ `ClassName`, `ExceptionName`, type variables in `CapWords`; exception classes end in `Error`. `GLOBAL_CONSTANT_NAME` in `CAPS_WITH_UNDER` at module level.
- ✅ First arg of instance methods is `self`; of class methods, `cls`.
- ✅ Prepend **one** `_` for module-internal / class-protected names; `trailing_` avoids a keyword clash (`class_`, `id_`). 🔴 Avoid `__dunder` name-mangling — hurts readability/testability and isn't really private.
- ✅ Single-char names only for counters/iterators (`i`, `k`, `v`), `e` in `except`, `f` in `with open`, or unconstrained private typevars (`_T`). 🔴 Never name anything `l`, `O`, or `I` (indistinguishable from `1`/`0`); no letter-deleting abbreviations; no type-in-name (`names_dict`); no dashes in filenames — always `.py`, `lower_with_under.py`.
- ✅ Repo convention: single-concept modules matching the module map (`config`, `ids`, `model`, `context`, `console`, `api`, `decorator`, `worker`, `sinks/…`); snake_case functions, `CAPS_WITH_UNDER` module constants — match it.

## §6 Public vs internal interfaces
Make the public surface explicit; everything else carries no compatibility promise.

- ✅ Declare the public API in **`__all__`** (the `log_forge` façade: `configure`, `trace`, the level functions, `set_baggage`, `shutdown`, …). Anything not in `__all__`, or prefixed with `_`, is internal and may change without notice.
- ✅ Mark internal modules/functions with a **leading underscore** rather than relying on documentation alone (e.g. the module-level `_config` singleton, `_log`, `_open_span`).

## §7 Exceptions
Exceptions are allowed but disciplined; never swallow, never use `assert` for real logic.

- ✅ Raise a **built-in** for a violated precondition / bad argument (`raise ValueError(f'Not a probability: {p=}')`). Prefer specific built-ins over generic ones.
- ✅ Define **domain exceptions** by subclassing an existing exception (from `Exception`, not `BaseException`); name them `…Error` without stutter (`ConfigError`, not `config.ConfigError`). Distinct types let callers branch retryable vs terminal.
- 🔴 Never `except:` bare, and don't catch `Exception` broadly — **unless** re-raising or forming a deliberate isolation boundary that records/suppresses (e.g. the worker thread's outermost block, which must survive a sink failure). Bare `except:` also swallows `SystemExit`, `KeyboardInterrupt`, typos. ✅ Catch the **specific** exception.
- 🔴 Never `except: pass` or return success on failure. ✅ Catch narrowly, add context, re-raise or convert to a typed failure; **chain** with `raise NewError(...) from err`. **The `@trace` decorator is the deliberate exception to "catch narrowly":** it catches `BaseException` to *record* the error end event, then **re-raises unchanged** (never swallows) — architecture §4.
- ✅ Keep the **`try` body minimal** — only the line(s) that can raise — so a real error isn't hidden by an unrelated one. Use `finally` (or `with`, §13) for cleanup (e.g. `pop_span(token)` in `finally`).
- 🔴 Don't use `assert` to validate inputs or enforce control flow — asserts can be stripped (`-O`) and aren't guaranteed to run. (In `pytest` tests, `assert` is the expected way to check expectations.)

## §8 Types & annotations
Annotate public surfaces; make `None` explicit; prefer abstract types in signatures.

- ✅ Annotate **every def** (the repo runs `mypy --strict` — no untyped defs) and module/class-level variables where it adds clarity. Don't annotate `self`/`cls` or `__init__`'s `None` return beyond what strict requires. Ship the `py.typed` marker so consumers get the types.
- ✅ **Explicit `X | None`** for nullable args (3.10+ union syntax preferred over `Optional[X]`); a nullable arg *must* be declared nullable — no implicit `a: str = None`.
- ✅ In signatures prefer **abstract containers** (`collections.abc.Sequence`, `Mapping`) over concrete `list`/`dict`; use built-in generics (`list[dict[str, object]]`, `tuple[str, ...]`) over `typing.List`/`Tuple`. Always parameterize generics (`Mapping[int, str]`, not bare `Mapping` → implicit `Any`).
- 🔴 **Never use a mutable default argument** (`def f(a, b=[])`, `={}`) — created once and shared across every call. ✅ Default to `None` and build inside, or use `dataclasses.field(default_factory=dict)` for dataclass fields (as `Config.defaults` / `Span.events` do). (Empty tuple `()` is fine — immutable, which is why the span-stack `ContextVar` defaults to `()`.)
- ✅ **Spacing:** `def f(x: int = 0) -> str:` — space after `:`, and spaces around `=` *because* the parameter is annotated (§3).
- ✅ `CapWords` type aliases (`_Private` if module-local); forward refs via `from __future__ import annotations` or string names; `str` for text, `bytes` for binary; typing-only imports under `if TYPE_CHECKING:` (as `config.py` imports `Sink` to avoid a runtime cycle).

## §9 Functions, classes & properties
One job per function; small and testable; classes and properties only where they earn their keep.

- ✅ **One responsibility per function** — if it does two unrelated things, split it (easier to name, test, reuse); factor shared logic out (DRY). Prefer **small focused functions**; no hard limit, but past ~40 lines consider splitting. (`model.py` only *builds* records; it doesn't know where the current span lives — that separation is deliberate.)
- ✅ Prefer **`return` over `print()`** for results — side-effect-free functions are testable; print only at the program's edges (the sinks / `ConsoleWriter` are those edges).
- ✅ Be consistent about `return`: either every return in a function returns a value (write `return None` explicitly) or none do.
- ✅ Put related classes/functions together in one module — no one-class-per-file rule. Nested functions/classes are fine **only** to close over a local (the `@trace` wrapper closes over `fn`/`name`/`defaults` — legitimate); don't nest just to hide a helper — prefix `_` at module level instead so tests can reach it.
- ✅ A **`@property`** is only for cheap, straightforward, unsurprising derived access. 🔴 Don't wrap a plain get/set in a property — make the attribute public (as the `Config`/`Span` dataclass fields are). Use getter/setter methods only when get/set does real work.
- 🔴 Never `staticmethod` (write a module-level function) unless an external API forces it; use `classmethod` only for named constructors or class-wide state. ✅ Bind names with `def`, not `name = lambda ...` (better tracebacks and `repr`). Use decorators judiciously with a clear payoff.

## §10 Comprehensions, iterators & generators
Concise container/iterator idioms — but readability wins over cleverness.

- ✅ Comprehensions/generator expressions are fine for **simple** cases: `[e for batch in sink.batches for e in batch]` at the edge of a test is tolerable, but 🔴 no multiple `for` clauses or multiple filters in production logic — expand to a loop. Optimize for readability, not brevity.
- ✅ Use **default iterators/operators**: `for k in adict`, `if x in alist`, `for k, v in adict.items()`, `for line in afile`. 🔴 Not `adict.keys()` / `afile.readlines()` for plain iteration. Don't mutate a container while iterating it.
- ✅ For large or streaming data, prefer a **generator expression** (`sum(x * x for x in xs)`) over materializing a list first — holds one item in memory. Document generators with `Yields:` not `Returns:` (§14); wrap one holding an expensive resource in a context manager so cleanup is forced.
- ✅ Lambdas are fine for one-liners; if it spans lines or exceeds ~60–80 chars, use a named `def`. Prefer `operator.mul` etc. over `lambda x, y: x * y`.

## §11 Strings, logging & errors
Format explicitly; log lazily; write precise, greppable messages.

- ✅ Format with **f-strings**, `%`, or `.format()` — pick per readability. A single `a + b` join is fine; 🔴 don't build strings with chained `+` (`'a: ' + name + '; ' + str(n)`).
- 🔴 Don't accumulate a string with `+=` **in a loop** (risks quadratic time). ✅ Append substrings to a list and `''.join(items)` after the loop, or write to `io.StringIO`.
- ✅ **Lazy logging** *when using the stdlib `logging` module* (e.g. an internal diagnostic logger): pass a **literal `%`-pattern** + args — `logger.info('dropped %s events', n)` — never a pre-rendered f-string. (This is distinct from log-forge's own structured events, which are always named JSON fields, never free-form text — `CLAUDE.md` / architecture §6.)
- ✅ **Error messages** (exceptions and console output): match the actual condition precisely, mark interpolated pieces clearly (`f'{p=}'`, `%r`), and stay greppable. 🔴 Don't assert a cause you didn't verify.
- ✅ Be consistent with one quote character per file (`ruff format` normalizes to `"`); use `"""` for multi-line strings and all docstrings.

## §12 Truthiness & conditionals
Use implicit falsiness and identity comparisons — with the standard traps in mind.

- ✅ `if seq:` / `if not seq:` for empty sequences (not `if len(seq) == 0:`); `if foo:` over `if foo != []:`.
- ✅ Compare to `None` with **`is` / `is not`**, never `==`; write `x is not None`, not `not x is None`. Always `if x is None:` for None checks — a falsy-but-not-None value (`0`, `''`, `[]`) would otherwise be misread (this matters where `parent_span_id` is `None` vs an empty string). 🔴 Don't `x = x or []` when `x` could be legitimately falsy.
- 🔴 Never compare booleans with `==`: `if active:` not `if active == True:` (chain `if not x and x is not None:` if you must separate `False` from `None`).
- ✅ `isinstance(obj, T)` over `type(obj) == T`. ✅ `str.startswith()` / `.endswith()` for prefix/suffix checks, not slicing.
- ✅ For integers, compare explicitly (`if i % 10 == 0:`) rather than relying on implicit false — avoids treating `None` as `0`. Note `'0'` (string) is truthy.
- ✅ Ternaries only when each of the three parts fits on one line; otherwise a full `if`.

## §13 Resources, global state & threading
Close what you open; avoid mutable global state; use the right concurrency primitive.

- ✅ Manage files/sockets/clients and other closeables with a **`with` statement** (or `contextlib.closing()` for non-context-manager closeables); otherwise explicit `try/finally`. Don't rely on `__del__`/GC for cleanup — timing is unguaranteed; the worker's graceful drain + `sink.close()` runs from an explicit `atexit`/`shutdown()`, not a finalizer.
- 🔴 **Avoid mutable global state.** log-forge's `_config` singleton is a deliberate, documented exception: declared at module level, internal (`_`), and read through `get_config()`. Don't add more mutable module state casually.
- ✅ **Module-level constants are encouraged** — `_MAX_RETRIES = 3` (internal) / `MAX_BYTES = 256 * 1024` (public on `SQSSink`), `CAPS_WITH_UNDER`.
- ✅ Init reusable SDK clients (`boto3.client(...)`) once and reuse — not per call. Keep the `boto3` import **local to the SQS sink** so the core stays dependency-free.
- ✅ **Context propagation uses `contextvars`, not thread-locals** — correct under threads *and* asyncio. Never mutate a `ContextVar`'s default mutable value; always `.set()` a new tuple/dict, and use the token/`reset(token)` pattern (architecture §5).
- 🔴 Don't rely on atomicity of built-in types across threads; use `queue.Queue` / `threading` primitives to hand work to the background worker (the app→worker handoff is a `queue.Queue`).

## §14 Docstrings & comments
Docstrings describe the interface and are the **primary source of context**; comments explain the non-obvious and should be rare.

- ✅ Triple-double-quoted `"""` docstrings (PEP 257) for every public module, function, class, and method — plus any nontrivial or non-obvious function. First line a summary ending in a period, ≤ the docstring line limit; multi-line = summary, blank line, details, closing `"""` on its own line. 🔴 No docstring on a `lambda`. Enough info to call it without reading the body.
- ✅ Use the section format when it adds info: **`Args:`** (each param + description; note types if not annotated), **`Returns:`** (or **`Yields:`** for generators; omit if it only returns `None` or the summary already says it), **`Raises:`** (exceptions relevant to the interface).
- ✅ Class docstring below the `class` line summarizing what an **instance represents** (an `Exception` subclass says what it *represents*, not when it's raised); document public attributes in an `Attributes:` section.
- ✅ **When code needs explaining, prefer expanding the docstring over adding comments.** Docstrings are discoverable (`help()`, hover, generated docs); scattered comments are not.
- ✅ **Comments explain *why*, not *what*** — tricky logic, non-obvious decisions (like the `TYPE_CHECKING` cycle-break, or catching `BaseException`); assume the reader knows Python. Complete sentences, kept in sync with the code. 🔴 No verbose or paragraph-length comments, and never a comment that just narrates *what* the next line does. 🔴 Never leave a comment that contradicts the code. Block comments at the code's indent (`# ` prefix); inline comments ≥2 spaces from code, used sparingly.
- ✅ `TODO:` in caps + colon + a **tracked reference** (spec ID or issue link preferred) + `-` explanation: `# TODO: SPEC-004 - swap direct flush for worker.submit`. Don't attribute TODOs to a person as the context.

## §15 Modules, main & power features
Modules stay importable; avoid Python's fancy machinery.

- ✅ Start each module with a **docstring** describing contents and usage (the existing `config.py` / `sinks/base.py` docstrings, with their `(arch §N)` back-references, are the model). Test modules need a docstring only if there's something extra to say.
- ✅ This is a **library** — imported, never run as a script — so no module does real work at import time and no `__main__` guard is needed. 🔴 Don't do real work at module top level (the lazy `StdoutSink` default and lazy worker creation exist precisely to defer side effects out of import).
- 🔴 **Avoid power features** — custom metaclasses, bytecode access, dynamic inheritance, reflection tricks (`getattr` hacks), `__del__` cleanup, import hacks. Stdlib/framework internals that use them (`dataclasses`, `enum`, `contextvars`, `typing.Protocol`) are fine to *use*.
- ✅ `from __future__ import annotations` is encouraged to adopt modern semantics per-file and to make forward refs / `TYPE_CHECKING`-only imports free (as `config.py` already does).
