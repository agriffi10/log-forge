"""The one way a sink imports the client its optional extra installs (SPEC-071 FR-002)."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


class MissingExtraError(ModuleNotFoundError):
    """An optional extra a sink needs is not installed.

    A ``ModuleNotFoundError`` and so an ``ImportError``, so every ``except`` clause written against
    either keeps matching, and ``.name`` and ``.path`` are the interpreter's own. Only the message
    differs: it names the extra and the command that installs it. Private, and in no ``__all__``:
    a public name would be frozen for every 1.x release (invariant 10), and a private one can still
    be promoted later by an addition.
    """


def import_extra(module: str, extra: str) -> ModuleType:
    """Imports an optional extra's module, or refuses with the extra named.

    Every lazy import of a third-party client goes through here, so a missing extra says which one
    to install rather than ``No module named 'boto3'``, and a new site cannot be written locally:
    ``tests/test_extras_roster.py`` reports any other import of a module outside the standard
    library and the package.

    **Only the module being absent is converted.** The ``ModuleNotFoundError`` must name the module
    or one of its dotted parents (``google.cloud`` for ``google.cloud.pubsub_v1``). One naming
    another module means the client is installed and its own dependency is missing, which the
    extra would not fix; one naming nothing, an ``ImportError`` that is not a
    ``ModuleNotFoundError`` (a broken binary), and anything else propagate unchanged, as the same
    object.

    A ``from X import Y`` site becomes an attribute read on what this returns, so a module present
    but lacking the name raises ``AttributeError`` where the import statement raised
    ``ImportError: cannot import name``. Only a stub or an incompatible version reaches that.

    Args:
      module: The module to import, by its full dotted name; a submodule is passed whole.
      extra: The ``[project.optional-dependencies]`` key that installs it.

    Returns:
      The imported module, or whatever ``sys.modules`` already holds for that name.

    Raises:
      MissingExtraError: When the module, or a parent of it, is not installed. Chained from the
        interpreter's own exception, whose message it keeps.
      Exception: Anything else the import raises, unchanged.
    """
    try:
        return importlib.import_module(module)
    except ModuleNotFoundError as exc:
        name = exc.name
        if name is None or not (module == name or module.startswith(name + ".")):
            raise
        raise MissingExtraError(
            f'The "{extra}" extra is not installed ({exc.msg}): pip install "log-foundry[{extra}]"',
            name=name,
            path=exc.path,
        ) from exc
