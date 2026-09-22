from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import antfly_lite  # noqa: E402
from antfly_lite._library import LibraryNotFoundError  # noqa: E402

REQUIRE_LIBRARY = os.environ.get("ANTFLY_LITE_REQUIRE_LIBRARY") == "1"


def ensure_native_library() -> None:
    """Skip the calling test if libantfly cannot be found, unless
    ANTFLY_LITE_REQUIRE_LIBRARY=1, in which case fail instead."""
    try:
        antfly_lite.validate_abi()
    except LibraryNotFoundError as exc:
        if REQUIRE_LIBRARY:
            pytest.fail(f"libantfly required (ANTFLY_LITE_REQUIRE_LIBRARY=1) but not found: {exc}")
        pytest.skip(f"libantfly not found: {exc}")


@pytest.fixture(scope="session")
def require_native() -> None:
    ensure_native_library()


@pytest.fixture()
def aflite_path(tmp_path: Path) -> Path:
    return tmp_path / "db.aflite"
