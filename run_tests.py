"""Run tests. Mainly for "run button" in replit."""

import pytest

pytest.main(["--black", "--mypy", "--ruff"])
