"""Makes this directory a package, which stops its `conftest.py` shadowing `tests/conftest.py`.

Not cosmetic. Without it both files are imported as the top-level module `conftest`, the second
wins in `sys.modules`, and six modules that do `from conftest import run_concurrently` --
`test_fork_lifecycle`, `test_sink_concurrency`, `test_sink_ownership`, `test_sink_release_roster`,
`test_span_sweep`, `test_synchronous_loss` -- fail at collection. Measured with this file absent
and an otherwise **empty** `integration/conftest.py`: `Interrupted: 6 errors during collection`.

The consequence, stated because it is the thing that surprises: modules in this package import
their own fixtures as `from integration.conftest import ...`. A bare `from conftest import ...`
here silently resolves to the *root* `tests/conftest.py` instead of failing.
"""
