"""The suite's ledger balance check (SPEC-088 FR-006). A helper module, not collected."""


def begin() -> None:
    """Starts a test on an empty ledger."""
    from log_foundry import _ledger

    _ledger.restart(0)
