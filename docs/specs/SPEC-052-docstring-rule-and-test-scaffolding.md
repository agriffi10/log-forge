# Spec: The Ungated Docstring Rule, and the Scaffolding It Left Behind

**ID:** SPEC-052
**Status:** In Progress
**Last Updated:** 2026-09-02
**Depends On:** None

## Overview

Two rules in this repo stopped being true and nothing noticed, because nothing checks either
one. `CLAUDE.md` requires every docstring in `src/` to open with "a description of **≤3
sentences**" — and the sentence beside it invites the opposite, telling authors that reasoning
which would have been an inline comment belongs *in* the docstring. The practice followed the
second sentence: at `451edf9`, of the 506 documented defs in `src/log_foundry/`, 158 exceed the
cap on its narrowest reading and 492 on its widest. Separately, the test suite still wears the
scaffolding it was built with — it was written ahead of the implementation, so every test
guarded on the module it needed with `pytest.importorskip`, and those guards were never removed
once the modules shipped. All 54 modules of `log_foundry` now import cleanly with no extras
installed, so not one of those guards can fire for its original reason. What they can still do
is convert a real `ImportError` into a silently skipped file — not in the core modules, which a
bare `import log_foundry` already loads, but in the 38 that it does not, every sink among them:
break `sinks/sqs.py` today and its suite exits **5** with "1 skipped" rather than failing.

Both are the same defect wearing two costumes: a rule with no gate rots, and a guard that can
no longer fire is worse than none, because it is still advertised as one. This spec reconciles
the docstring rule to what the codebase actually does, gates the parts of it that survive, and
removes the dead guards so an import failure is a failure again.

## Scope

### In Scope

- Re-scoping `CLAUDE.md`'s docstring sentence cap from the whole description to the **summary
  line**, reconciling the same bullet's `Args:`/`Returns:`/`Raises:` clause with the 58 of 59
  classes that carry none of them, and recording the decision in `docs/decisions.md`.
- A checker at `scripts/docstring-lint.py` enforcing the four parts of the rule that are alive,
  and its registration as a local pre-push gate beside `scripts/docs-lint.sh`.
- A fixture corpus at `scripts/docstring-lint-test.sh` that proves each of those checks still
  fires, and that each stays silent when it should.
- **One line of `src/`**, and only this one: a module docstring for
  `src/log_foundry/sinks/__init__.py`, which is 0 bytes at `451edf9` and is the single
  violation of FR-002's module check in the whole tree. Held back rather than fixed, the gate
  would ship carrying a waiver for its only finding, which is not a gate. No branch touches
  that file and it is empty, so a one-line addition cannot conflict with a content change; the
  session that owns `src/log_foundry/sinks/**` has been told this spec takes it.
- Replacing every `pytest.importorskip("log_foundry…")` call in `tests/` with a plain import,
  and removing the four pre-implementation skip sites and the prose that describes them.
- Rewriting `tests/README.md`, which still describes the suite as written ahead of the code.

### Out of Scope

- **Rewriting any docstring in `src/`.** Every check here is at zero violations already once
  the one line above is added (see each FR's measured baseline), which is what makes them cheap
  to adopt. This spec changes no existing prose in `src/`.
- **Enforcing these checks in CI.** The gate is local, for the same reason
  `scripts/docs-lint.sh` is: the failure belongs in front of whoever caused it while they can
  still fix it quietly. Promoting it is a separate decision and belongs to whoever owns
  `.github/`.
- **`tests/` docstrings.** The rule `CLAUDE.md` states is scoped to `src/`, and widening it is
  a rule change nobody has asked for. The checker's root is `src/log_foundry` and it takes no
  argument to widen it.
- **The three `importorskip("nats", …)` calls in `tests/test_sinks_nats.py`.** Those guard a
  genuinely optional extra, can fire, and removing them would red the no-extras suite. They are
  the only `importorskip` calls in `tests/` that survive this spec —
  `tests/integration/` contains none, and its own rule (`tests/integration/conftest.py:401`,
  from SPEC-041) is that an absent service must **fail**, never skip.
- **Class docstrings gaining `Args:`/`Returns:`/`Raises:`.** 58 of 59 classes in `src/` carry
  none of the three. FR-001 reconciles `CLAUDE.md` to that rather than reconciling the code to
  `CLAUDE.md`; the trio is a callable's contract, and
  `docs/best-practices/python/python.md:165` already says so.
- **`docs/best-practices/python/python.md`.** Its §14 was corrected on its own branch
  immediately before this spec; nothing here re-opens it.

---

## Functional Requirements

### FR-001: The cap moves to the summary line, and the class clause is reconciled

#### Description:

`CLAUDE.md` → *Code Conventions* currently reads "Every function, method and class carries a
Google-style docstring: a description of **≤3 sentences**, then `Args:` / `Returns:` /
`Raises:`, each filled with `None.` where it doesn't apply." Two of its claims are false in
practice and neither is checked. Both are re-scoped in the same bullet:

1. **The sentence cap moves to the summary line.** The summary line is one sentence; what
   follows it is unbounded. This keeps the constraint where it earns its keep — the line a
   reader meets in a hover, an index or a generated doc — and stops it contradicting its own
   neighbour ("reasoning that would have been an inline comment belongs *in* the docstring"),
   which is the sentence the codebase actually followed.
2. **The `Args:`/`Returns:`/`Raises:` trio binds functions and methods, not classes.** 58 of 59
   classes in `src/` carry none of the three; a class docstring's sections are `Attributes:`.
   Leaving this unreconciled would ship a gate that declines to enforce a rule `CLAUDE.md`
   still states, which is the exact defect this spec's Overview names.

The same edit makes two smaller corrections in the same bullet. It adds `# pragma:` to the
list of directive comments, which today names only `# noqa` and `# type: ignore` while `src/`
also carries a `# pragma: no cover`. And it replaces "Three docstrings are asserted by tests —
`_diag`'s module docstring, `sinks/sqs`'s, and `Sink.emit`'s" — a third false claim in the same
sentence. There are **seven**: those three plus `Sink`'s class docstring, `SentrySink`'s,
`Sink.close`'s and `_lifecycle.releasable`'s. An enumeration that was wrong by four is replaced
by the principle and the query that answers it (`grep -rn '__doc__' tests/`), which is shorter,
true, and cannot rot the way a count does.

The decision gets a full entry in `docs/decisions.md` under the existing working-rules area,
with a row in that file's `## Contents`, and **one** line in `CLAUDE.md`'s Key Decisions
replacing or extending that area's clause.

#### Acceptance Criteria:

- [ ] `CLAUDE.md`'s Code Conventions bullet states the one-sentence **summary line** rule and no
      longer states a sentence cap on the description.
- [ ] The same bullet scopes `Args:`/`Returns:`/`Raises:` to functions and methods, and names
      `Attributes:` as the class section — so `CLAUDE.md`, `python.md:165-166` and
      `scripts/docstring-lint.py` all say the same thing.
- [ ] `CLAUDE.md`'s directive list names `# pragma:` alongside `# noqa` and `# type: ignore`.
- [ ] `CLAUDE.md` no longer claims three docstrings are asserted by tests. `grep -rn '__doc__'
      tests/` returns assertions against seven, and the bullet points at that query rather than
      carrying a list.
- [ ] `docs/decisions.md` carries a `###` entry for the decision **and** a matching row in its
      `## Contents`; `sh scripts/docs-lint.sh` exits 0 (it fails an entry the Contents cannot
      reach, and fails a digest line with no entry behind it).
- [ ] `CLAUDE.md`'s Key Decisions gains **one** line, under the existing working-rules area,
      and `sh scripts/docs-lint.sh` exits 0 on the byte budget and the digest-unit cap.

### FR-002: A checker for the four parts of the rule that are alive

#### Description:

Ship `scripts/docstring-lint.py` — standard library only, so it runs in the no-extras
environment CI uses. It **parses** with `ast` and `tokenize`; it does not grep the source,
because a regex over source is the hazard `docs/process.md` §3 names ("parse instead where a
parser exists"). It reads `src/log_foundry/` and exits 1 on any violation, printing one
`FAIL  <path>:<line>  <message>` line per finding.

**The definition of a sentence is load-bearing and is fixed here**, because the obvious
implementation is wrong: counting `.` characters reports 39 of 506 summary lines as multi-sentence,
every one of them a false positive from a dotted name or a Sphinx role —
``A :class:`~log_foundry.sinks.base.Sink` that…``, `json.dumps`, `arch §9.2`, `_diag.py`. A
sentence boundary is **`.`, `!` or `?`, followed by whitespace, followed by an optional opening
quote or bracket and then a capital letter or a digit.** A dot with no space after it never
splits. Under this definition all 506 summary lines are single sentences, and it is the
definition that produces the Overview's 158 and 492 as well.

The four checks, each with its measured baseline at `451edf9`:

1. **Summary line present and well-formed** — every function, method and class **has** a
   docstring, and its first line is a single sentence by the definition above, is non-empty,
   ends in `.`, is **≤100 characters measured on the docstring text** (not the raw source line,
   which reaches exactly 100 at `_lifecycle.py:925` and would leave the budget no headroom),
   and is followed by a blank line when the docstring continues. **Presence is part of this
   check, not an assumption of it** — without it the `@overload` exemption below is
   unreachable, since nothing else would ever ask whether a docstring exists, and `CLAUDE.md`'s
   "every function, method and class carries a docstring" would stay ungated. *Baseline: 0 of
   506 documented defs violate any sub-condition; the longest summary line is 93 characters.*
2. **No comments in `src/`** — no comment token that is not a directive. Directives are
   `# noqa`, `# type:`, `# pragma:` and a `#!` shebang. *Baseline: 36 comment tokens — 31
   `# type: ignore`, 4 `# noqa`, 1 `# pragma: no cover` — and 0 violations.*
3. **`Args:` / `Returns:` / `Raises:` present** on every function and method, with `Yields:`
   accepted in place of `Returns:`. Classes are exempt, per FR-001. *Baseline: 0 of 447
   documented functions violate this.*
4. **Module docstrings present and one line**, with `_diag.py` and `sinks/sqs.py` exempt from
   the one-line half because tests assert their multi-line text. *Baseline: 1 violation,
   `sinks/__init__.py`, fixed by the one line this spec adds; exactly 2 multi-line module
   docstrings, both exempt.*

`@overload`-decorated defs are exempt from needing a docstring at all: the two stubs at
`decorator.py:608,610` carry none, and the implementation below them does.

#### Acceptance Criteria:

- [ ] `python3 scripts/docstring-lint.py` exits **0** against `src/` on the branch, and its
      summary line names the number of defs and modules it examined, so a run that silently
      examined nothing is distinguishable from a clean one.
- [ ] It exits **1** and names the offending `path:line` for each of the four checks when that
      construct is present — proved by FR-003's corpus, not by this criterion.
- [ ] Its imports are drawn only from `ast`, `tokenize`, `re`, `pathlib` and `sys`. Reading a
      file for `tokenize` uses `path.open()` rather than `io.StringIO`, so `io` is not needed.
      Checked by reading the import block, not by `-X importtime`, which lists a machine's
      `sitecustomize`/`usercustomize` and so cannot answer this mechanically.
- [ ] The file is executable (`chmod 755`, matching `scripts/make-sbom.py`) if it carries a
      shebang: `poetry run ruff check .` covers `scripts/`, and `EXE001` reds a shebanged file
      that is not.
- [ ] Running it from any working directory produces the same result — it resolves its own
      repo root rather than trusting `cwd`, as `docs-lint.sh` does.
- [ ] A summary line containing a Sphinx role, a dotted module name, or a section reference
      (`arch §9.2`) is **not** reported — asserted as a silence case in FR-003, since this is
      where the naive implementation produces 39 false positives.
- [ ] `CLAUDE.md`'s Common Commands and pre-push gate list name it, and `docs/process.md` §3's
      gate list names it, so the two do not disagree about what the gates are.

### FR-003: A fixture corpus that proves each check still fires

#### Description:

`scripts/docstring-lint-test.sh`, modelled on `scripts/docs-lint-test.sh`: `@@@`-directive
`.case` fixtures under `tests/docstring-lint/`, each asserting the specific **failure text**
rather than an exit code, because a check that fails for the wrong reason gets fixed by
changing the wrong thing.

Running the checker against `src/` proves `src/` passes and proves nothing about the checker.
So the corpus carries, for **every** check and **every** way that check can terminate, one
failing case; and for every check, at least one `*-ok.case` asserting **silence**, because half
of a linter's regressions are false positives and a corpus of only-failures cannot see one.
**Count the ways each check can stop, and write that many cases.** Check 1 has **six** — no
docstring at all, an empty first line, more than one sentence, no final period, over 100
characters, and no blank line after a continuing summary. The empty-first-line case is not
optional: a naive implementation reports it as "does not end in `.`", which is exactly the
misleading-message failure the assert-the-text rule exists to catch. Check 3 has **three** —
no `Args:`, no `Returns:`/`Yields:`, no `Raises:` — and a corpus with one case for it can be
shipped while two of the three have gone quiet. Check 4 has two (absent, multi-line at a
non-exempt path); check 2 has one.

The exemptions are themselves silence cases: an `@overload` with no docstring, a `# noqa`, a
`# type: ignore`, a `# pragma:`, a shebang, a multi-line module docstring at an exempt path, a
class with no `Args:` trio, a function using `Yields:` instead of `Returns:`, and the Sphinx-role
and dotted-name summary lines from FR-002 must each produce no output.

#### Acceptance Criteria:

- [ ] `sh scripts/docstring-lint-test.sh` exits 0, and prints a per-case `ok`/`FAIL` line and a
      total.
- [ ] Every check is defeated in turn — the logic inverted or removed, one at a time — and the
      corpus goes **red** each time, with the *named* case failing. Reverted after each. Run for
      every check, every terminating condition **and every exemption branch**: the exemptions
      are where a false positive hides, and a corpus that never mutates them cannot tell a live
      exemption from one that has silently become unconditional. That means `@overload`, each
      of the four directive prefixes, `Yields:`-for-`Returns:`, the class exemption, the
      exempt-path set — **and `SENTENCE_SPLIT` itself**, whose `\s+` is the single character
      standing between this gate and the 39 false positives FR-002 exists to prevent.
- [ ] The corpus contains at least one `*-ok.case` per check, and an `-ok` case failing is
      reported as "expected silence", distinctly from a wrong exit code.
- [ ] The corpus parses the checker before exercising it, so a syntax error cannot end a run
      at status 0.
- [ ] A case whose fixture file is missing or empty is **rejected** by the harness rather than
      passing vacuously. This is proved by a fixture carrying `@@@ expect harness-reject`, an
      inverted expectation that passes when the harness refuses it — a plain empty fixture in
      the corpus would make the harness exit 1 and contradict AC-1. The flag's own parse is
      the trap: `sed -n 's/^@@@ expect harness-reject//p'` yields an empty string on a match,
      so a truth test on its output never fires and the guard ships inert. Test the flag by
      defeating it, like any other check.

### FR-004: The dead `log_foundry` import guards become plain imports

#### Description:

Every `pytest.importorskip("log_foundry…")` **call** in `tests/` becomes a plain import. At
`451edf9` there are **147** of them across 26 files, against 22 distinct module paths; all 54
modules of the package import with zero extras installed, so none can fire for the reason it
was written. What each can still do is turn a real `ImportError` into a **silently skipped file**, and the
population that matters is narrower than it looks: `src/log_foundry/__init__.py:19` imports
`worker`, so 16 of the 54 modules — `worker`, `_lifecycle` and `config` among them — are
already loaded by a bare `import log_foundry` and a break in one is a collection error today.
The 38 modules **not** reachable that way are where the guard still bites, and every sink is
one of them: breaking `sinks/sqs.py` today makes `tests/test_sinks_sqs.py` exit **5** with
"1 skipped" instead of failing. `tests/test_worker_predicate_roster.py:51` already says in a
comment why that is wrong. Every one of the 147 is a simple assignment with a single
positional argument and no `reason=`, so there is no inline use and no rebinding hazard.

**The substitution has three forms, not two** — the path has one, two or three segments:

| Site | Becomes |
|---|---|
| `X = pytest.importorskip("log_foundry")` | `import log_foundry as X` |
| `X = pytest.importorskip("log_foundry.worker")` | `from log_foundry import worker as X` |
| `X = pytest.importorskip("log_foundry.sinks.sqs")` | `from log_foundry.sinks import sqs as X` |

The 23 three-segment sites are the ones a two-form rule breaks: `from log_foundry import
sinks.sqs as X` is a `SyntaxError`, and applying a two-form rule leaves 8 files that do not
parse.

**The edit is not confined to the substituted line, and this FR does not pretend otherwise.**
An assignment becoming an import changes what ruff sees, and ruff's selected rules then require
three follow-on edits, each of which is a mechanical fix ruff itself can apply and verify:

- **`I001`** — a substituted line adjacent to an existing import block joins that block, which
  must then be sorted. Import blocks are reordered.
- **`F401`, from two different causes, and the second is the dangerous one.** In files where
  the guards were the only use of `pytest`, `import pytest` becomes unused and is removed —
  routine. But at two sites the **substituted import itself** is unused, because the guard
  existed only for its skip side-effect and nothing referenced the name:
  `test_owed_closes.py` (`sinks.stdout`) and `test_shutdown_lifecycle.py` (`worker`). A blind
  `ruff check --fix` deletes those two conversions outright, replacing a dead guard with
  nothing — and **the collected-name diff cannot see it**, because no test name changes.
  Each such site is reported and decided by hand, never removed as a fix side-effect.
- **`E402`** — a module-level substitution landing after non-import code is a mid-file import.
  `# noqa` in ruff is **per line**; there is no block inheritance, so a substitution near an
  existing exempted block (`test_config.py:119-120` carry the noqa, not the `importorskip`
  lines at `:122-125`) gets its own `# noqa: E402` appended per line, and elsewhere the import
  is hoisted into the file's top block. A **function-body** site stays in the function body and
  raises no `E402` at all.
- **`TC002`** — `tests/test_concurrent_owed_closes.py`, where the guard was the only *runtime*
  use of `pytest` and the survivor is an annotation, so ruff asks for a `TYPE_CHECKING` block.
  One site, and it needs `--unsafe-fixes`.

The rewriter **reports before it applies**: it prints every intended substitution with its
before/after line, that list is read, and only then is it re-run with `--apply`. It is
AST-driven, not a regex over source.

#### Acceptance Criteria:

- [ ] `grep -rn 'pytest\.importorskip("log_foundry' tests/` returns nothing. (Scoped to the
      **call**: the prose mentions in `tests/conftest.py`'s docstring and `tests/README.md` are
      FR-005's and FR-006's, and this FR cannot make them go away.)
- [ ] The only `pytest.importorskip(` **calls** left anywhere in `tests/` are the three
      `importorskip("nats", …)` in `tests/test_sinks_nats.py`, unchanged. A bare
      `grep importorskip` is **not** the check — it also matches the prose at
      `test_sentry_backend.py:463` and `test_worker_predicate_roster.py:51`, the second of which
      this FR's own Description cites approvingly.
- [ ] Every site where `ruff` reports the **substituted** import as unused is listed and
      decided explicitly, not auto-fixed. Expected: exactly two.
- [ ] Every file under `tests/` parses: `python3 -m compileall -q tests/` exits 0.
- [ ] The set of collected test **names** — `pytest --collect-only -q`, sorted — is
      byte-identical before and after. The baseline is captured on **this branch's own start
      commit**, not quoted as an absolute: two concurrent sessions are editing files in the
      rewritten set, so the invariant is "unchanged", never "1923". A pass count is not
      evidence — a scripted edit in this repo once removed four tests with the suite green.
- [ ] `poetry run pytest` exits 0 and the skip **identities** are unchanged, captured with
      `-rs` and diffed as `nodeid: reason` pairs before and after. A count alone is not the
      check, and neither is the collected-name diff — a skipped test is still *collected*, so a
      guard that widened would leave both unchanged. At the branch point the identities are the
      three species FR-006 names: `test_sinks_nats.py:474,492,657`, `test_sentry_backend.py:472`
      ×3, and `test_fork_lifecycle.py:1569,1954`.
- [ ] Renaming `src/log_foundry/sinks/sqs.py` makes **`poetry run pytest tests/test_sinks_sqs.py`**
      exit **2** (a collection error), where the same break before this FR gives exit **5**,
      "1 skipped". Reverted after. Two things make this criterion precise rather than
      approximate: the module must be one **not** reachable from `import log_foundry` — a core
      module such as `worker.py` already fails today via `__init__.py`, so using one would tick
      without the FR — and the run must be **scoped to that one file**, because at whole-suite
      scope other tests reach `sinks.sqs` by unguarded paths and the break already surfaces as
      exit 1 today.
- [ ] `poetry run ruff check .` exits 0, including `I`, `F401`, `E402` and `TC002`.

### FR-005: The pre-implementation skip marks go

#### Description:

Four sites remain once FR-004 lands, each of which asks whether a feature that shipped many
specs ago exists. `grep -rn 'not implemented yet' tests/` returns five hits at `451edf9`; four
are these, and the fifth is `tests/README.md:26`, which FR-006 owns.

- `tests/test_decorator_sync.py:17` — a module-level `skipif` on
  `hasattr(log_foundry, "trace") and hasattr(log_foundry, "configure")`, reason "not
  implemented yet".
- `tests/test_decorator_async.py:28` — a module-level `skipif` calling
  `_async_trace_supported()`, which probes whether `@trace` returns a coroutine function,
  reason "async `@trace` not implemented yet (SPEC-003)". The helper becomes dead with the mark
  and goes with it; it is used nowhere else.
- `tests/test_decorator.py:75-76` — a **body-level** `pytest.skip` guarded on
  `hasattr(lf, "set_baggage")`. It carries no `skipif` mark, so a sweep for `skipif` misses it;
  it is here because a sweep for the reason string finds it.
- `tests/conftest.py:194-200` — the `lf` fixture's loop over `("configure", "trace", "info")`
  calling `pytest.skip(f"log_foundry.{attr} not implemented yet")`.

Two pieces of prose describing the removed convention go with them: `tests/conftest.py`'s
module docstring, which tells the reader "these tests are written *ahead* of the
implementation" and that every test guards with `importorskip`, and the paragraph in
`tests/test_decorator_sync.py`'s module docstring explaining that a sibling suite "stays skipped
through SPEC-001".

These four sites and two docstrings are edited **by hand**, not by a script. They are six edits,
and a script would be operating on files FR-004 had just rewritten — which is the one thing
`docs/process.md` §3 says an automated repair must never do.

#### Acceptance Criteria:

- [ ] `grep -rn 'not implemented yet' tests/` returns nothing **outside `tests/README.md`**,
      which FR-006 owns and which this FR cannot reach.
- [ ] `grep -rn 'pytest\.mark\.skipif' tests/` returns nothing. A bare `grep skipif` is not the
      check — it matches SPEC-041's own rule text at `tests/integration/conftest.py:395`, which
      Out of Scope protects.
- [ ] `grep -rn 'importorskip' tests/conftest.py` returns nothing, including its docstring.
- [ ] `_async_trace_supported` no longer exists in the tree, and `poetry run ruff check .`
      exits 0 (its `F` rules would flag an unused helper left behind).
- [ ] The collected-name set and the `-rs` skip identities are unchanged by this FR as well,
      captured and diffed the same way as FR-004 — twice in total across the two FRs, because
      two separate edits touched the same files and a single check at the end cannot say which
      one moved something.

### FR-006: `tests/README.md` describes the suite that exists

#### Description:

The file opens "These tests are written **ahead of the implementation**", carries a table of
five files and what each "skips until", and says "Skipped tests are expected early on". None of
that has been true since the package was finished. It is rewritten to describe the suite as it
is: what the conventions are, how to run it, and — replacing the skip table — the rule that
replaces it. That rule is **not** "a test skips only for an absent third-party service": the
integration suite's rule, from SPEC-041, is that an absent service must **fail**. Nor is it
"only the three `nats` guards" — the suite's 8 skips are **three species**, and a README naming
one of them would be false on the day it shipped: 3 for the absent `nats` extra, 3 in
`test_sentry_backend.py:472` where a body-level `pytest.skip` sits in a `try`/`except` this
spec does not touch, and 2 in `test_fork_lifecycle.py:1569,1954` excluding one parametrisation
that a sibling test covers. The correct statement names all three and says which is which; and
that in `tests/integration/`, a skip is a defect.

It also carries a bare runtime measurement ("~35 s"), which the standing rule in
`docs/process.md` §5 forbids: state the principle or anchor both ends to a commit a reader can
re-measure from.

#### Acceptance Criteria:

- [ ] `tests/README.md` contains no "ahead of the implementation" claim, no "Skips until"
      table, and no "expected early on" sentence.
- [ ] It states both skip rules — the `tests/` one and `tests/integration/`'s no-skips-ever —
      and does not state the first in a way that contradicts the second.
- [ ] It carries no bare wall-clock or line-count measurement; where a number is genuinely
      useful it names the commit it was measured at.
- [ ] Every path, file, fixture and command name it mentions in backticks exists in the tree or
      runs — checked by a sweep over the file's backticked identifiers, not by reading. (The
      file is outside `scripts/docs-lint.sh`'s scope, so that gate cannot answer this and is
      not cited as if it could.)

---

## Data Model

No runtime types. The checker's internal shape, at `scripts/docstring-lint.py`:

```python
Finding = tuple[str, int, str, str]   # (path, line, check_id, message)

CHECKS = ("summary", "comment", "sections", "module")

# A sentence boundary: terminator, whitespace, optional opener, then capital or digit.
SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[\"'`(\[]?[A-Z0-9])")

EXEMPT_MULTILINE_MODULE_DOC = frozenset({"_diag.py", "sinks/sqs.py"})
DIRECTIVE_PREFIXES = ("noqa", "type:", "pragma:", "!")
SUMMARY_MAX_CHARS = 100
```

---

## API / Interface Contract

```sh
python3 scripts/docstring-lint.py          # exit 0 clean, 1 on any violation
sh      scripts/docstring-lint-test.sh     # the corpus; exit 0 when every check still fires
sh      scripts/docstring-lint-test.sh sum # ...filtered to case names containing "sum"
```

Output shape, mirroring `docs-lint.sh`:

```
FAIL  src/log_foundry/example.py:12  summary line does not end in '.'
----
docstring-lint: 1 failure over 508 defs in 54 modules.
```

The FR-004 rewriter is a build-time tool, not a shipped one, and lives in the session's
scratchpad rather than in `scripts/`: it runs twice in this spec's life and never again.

## Configuration / Environment

None. The checker takes no environment variable and no flag — a budget or a root a caller can
override is one CI can be told to ignore, which is the reasoning already recorded beside
`docs-lint-test.sh`'s in-copy `sed` rewriting.

## File & Folder Structure

```
scripts/
├── docstring-lint.py          # new — the gate
└── docstring-lint-test.sh     # new — the corpus that proves it fires
tests/
├── docstring-lint/            # new — *.case fixtures, one construct each
│   ├── summary-no-period.case
│   ├── summary-sphinx-role-ok.case
│   └── ...
├── README.md                  # rewritten
├── conftest.py                # `lf` fixture's hasattr skip + module docstring
├── test_decorator.py          # body-level skip
├── test_decorator_sync.py     # skipif mark + docstring paragraph
└── test_decorator_async.py    # skipif mark + `_async_trace_supported`
src/log_foundry/sinks/__init__.py   # one module docstring — the only src/ edit
CLAUDE.md                      # Code Conventions, Key Decisions, gate list
docs/decisions.md              # the new entry + its Contents row
docs/process.md                # §3's gate list
```

## Implementation Phases

### Phase 1: The rule and its record

- Re-scope the Code Conventions bullet in `CLAUDE.md`: summary line, class clause, `# pragma:`.
- Write the `docs/decisions.md` entry and its `## Contents` row; add the single Key Decisions
  digest line.
- Run `sh scripts/docs-lint.sh` — the byte budget and the digest-unit cap both bind here.

### Phase 2: The checker

- `scripts/docstring-lint.py`, the four checks, stdlib only, resolving its own root.
- The one-line module docstring for `src/log_foundry/sinks/__init__.py`.
- Register it in `CLAUDE.md`'s Common Commands and gate list, and in `docs/process.md` §3.
- Confirm it exits 0 against `src/` — and treat that as proof of nothing until Phase 3.

### Phase 3: The corpus

- `tests/docstring-lint/*.case` — one failing case per terminating condition of each check,
  plus a silence case per check and one per exemption.
- `scripts/docstring-lint-test.sh` to run them, asserting failure text.
- Defeat each check and each terminating condition in turn, watch the corpus redden, revert.

### Phase 4: The sweep

- Capture the collected-name set at the branch point.
- The report-then-apply rewriter for FR-004; read the report, then `--apply`; then `ruff check
  --fix` for `I001`/`F401` and the `E402` decisions by hand.
- Re-capture the collected-name set and diff it.
- FR-005's four sites and two docstrings by hand.
- Diff the collected-name set again after FR-005 — twice, because two separate edits touched
  the same files.

### Phase 5: The docs the sweep invalidates

- Rewrite `tests/README.md`.
- Sweep its backticked identifiers against the tree.
- Full gate run, including the new one.
