---
type: Decision
title: "A sink imports its extra through `import_extra`, and a missing one is refused naming it"
description: "every lazy import of an optional client is `import_extra(module, extra)` (`sinks/_extras.py`): a `ModuleNotFoundError` naming the module or a dotted parent becomes `MissingExtraError`, a private `ModuleNotFoundError` subclass keeping `.name`, `.path` and the interpreter's text and adding the extra and its `pip install`; anything else propagates as the same object. A sink that needs the extra beside an injected client (`AzureEventHubsSink`'s `EventData`) resolves it at construction. A guard reports any other third-party import. (SPEC-071)"
---

**Every lazy import of an optional client goes through one helper, so a missing extra says which
one to install.** Measured at `fda134a`, the sinks held sixteen lazy imports of a third-party
module, and fourteen of them raised the interpreter's bare `ModuleNotFoundError: No module named
'boto3'`, which never says that the fix is `pip install "log-foundry[aws]"`. `import_extra(module,
extra)` calls `importlib.import_module` and returns the module. When the import fails with a
`ModuleNotFoundError` whose `name` is the module or one of its dotted parents (`azure` for
`azure.eventhub`, `google.cloud` for `google.cloud.pubsub_v1`), it raises `MissingExtraError`, whose
message is `The "<extra>" extra is not installed (<the interpreter's text>): pip install
"log-foundry[<extra>]"`, with the original as its `__cause__` and its `.name` and `.path`.

**Only the module's own absence is converted.** A `ModuleNotFoundError` naming another module means
the client is installed and one of its own dependencies is missing (`botocore` under `boto3`, say),
so a message saying the extra is not installed would be false, even where reinstalling the extra
would repair it; one naming nothing, and an `ImportError` that is not a `ModuleNotFoundError` (a
broken binary), say something else again. Each propagates as the same object, so the message
claims the extra is absent only when its module is.

**The type is private.** `MissingExtraError` lives in `sinks/_extras.py`, in no `__all__`, and is
not re-exported. It is a `ModuleNotFoundError` and so an `ImportError`, so every `except` clause
written against either still matches and `.name` still reads the missing module, which is all a
caller could rely on before. A public name would be frozen for all of 1.x (invariant 10), while a
private one can still be promoted later by an addition.

**A `from X import Y` site became an attribute read** on what `import_extra` returns, and a
submodule is passed whole (`google.cloud.pubsub_v1`), never read as an attribute. A module that is
present but lacks the name therefore raises `AttributeError` where `from … import` raised
`ImportError: cannot import name`; only a stub or an incompatible version reaches that, and it was
accepted. The soft sites (`sentry._import_sdk`, `RabbitMQSink._persistent_properties`,
`MongoDBSink`'s `bson` encoder) keep their fallbacks, since each catches `ImportError` or wider,
which `MissingExtraError` is.

**A sink that needs the extra with an injected client resolves it at construction.**
`AzureEventHubsSink(producer=p)` without `azure-eventhub` constructed and then raised
`ModuleNotFoundError` out of every `emit`, because every batch is built from the extra's `EventData`
class (probed at `fda134a`). The constructor now resolves that class once, after its argument
checks and before anything is built, so the configuration is refused where it is written
(invariant 13); `emit` still resolves it per batch through `_event_data_cls`, the seam the tests
patch. Every other injected-client sink imports nothing it did not import before:
`SQSSink(client=c)` still leaves `boto3` out of `sys.modules`.

**What the move removed.** `import_module` returns an untyped module, so the
`# type: ignore[import-not-found]` comments the static imports carried are gone, and with them the
`google.cloud.*` mypy override and the trap that a venv with extras installed broke `mypy --strict`
(measured clean with every extra installed at `aeb0f0ab` and `607f1f46`).

**The guard.** `tests/test_extras_roster.py` derives the sites from the AST of
`src/log_foundry/**/*.py`. It reports, by `file:line`, any import of a module outside the standard
library and the package outside `if TYPE_CHECKING:`; holds the derived `(file, function, module,
extra)` sites to a set written out by name; requires every extra to be a key of
`[project.optional-dependencies]`; requires both arguments to be string literals; and reports any
call to `importlib.import_module` or `__import__` outside `sinks/_extras.py`, an aliased
`from importlib import import_module as X` included, leaving `importlib.util.find_spec`,
`getattr(importlib, ...)` and `pkgutil.resolve_name` as deliberate edges it does not see. A mutation
sweep rewrites each call back into the bare import in memory and requires a report for each.

**Rejected.**

- *A public `MissingExtraError`.* Freezes a name for 1.x that no caller needs to catch by name.
- *Converting every `ModuleNotFoundError`.* It would tell a caller the extra is not installed when
  it is, and the module missing is one of the client's own dependencies.
- *Resolving `EventData` only per batch.* That is the defect: the sink constructed and then failed
  every `emit`.

**Do NOT** write an `import` of a third-party module anywhere under `src/` outside
`if TYPE_CHECKING:`; route it through `import_extra` with a literal module and a real extra.
(SPEC-071, invariants 10, 11 and 13)

## History

### 2026-11-07

- **Settled (SPEC-071 FR-002).**
