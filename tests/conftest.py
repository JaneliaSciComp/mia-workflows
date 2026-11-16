"""What every test gets."""

import pytest


@pytest.fixture(autouse=True)
def no_live_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    """No test reads the mia.yaml of the checkout it runs in (a live deployment's, with its pause switch): the
    manifest is a file that is not there unless the test names one."""
    monkeypatch.setenv("MIA_CONFIG", "/nonexistent/mia.yaml")
