from pathlib import Path

import pytest

from fix_generated_client import FILES, NDJSON_HEADER, fix_generated_client


def write_generated_files(root: Path, signature_count: int) -> None:
    signature = "body: StatefulQueryRequest | File | Unset = UNSET"
    for relative in FILES:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join([NDJSON_HEADER, *([signature] * signature_count)]))


def test_required_ndjson_body_is_not_made_optional(tmp_path: Path) -> None:
    write_generated_files(tmp_path, signature_count=5)

    fix_generated_client(tmp_path)

    for relative in FILES:
        source = (tmp_path / relative).read_text()
        assert source.count("body: StatefulQueryRequest | File") == 5
        assert "Unset = UNSET" not in source


def test_generator_shape_drift_fails_before_writing(tmp_path: Path) -> None:
    write_generated_files(tmp_path, signature_count=4)
    before = {relative: (tmp_path / relative).read_text() for relative in FILES}

    with pytest.raises(RuntimeError, match="unexpected generated shape"):
        fix_generated_client(tmp_path)

    assert {relative: (tmp_path / relative).read_text() for relative in FILES} == before
