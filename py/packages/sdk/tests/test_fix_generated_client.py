from pathlib import Path

import pytest

from fix_generated_client import (
    FILES,
    EMBED_REQUEST,
    NDJSON_HEADER,
    NDJSON_RESPONSE,
    RELATIONAL_QUERY,
    SQL_OPERATIONS,
    fix_generated_client,
)


def write_generated_files(root: Path, signature_count: int) -> None:
    signature = "body: StatefulQueryRequest | File | Unset = UNSET"
    for relative in FILES:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join([NDJSON_HEADER, *([signature] * signature_count)]))
    path = root / RELATIONAL_QUERY
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(NDJSON_RESPONSE)
    for relative in SQL_OPERATIONS:
        path = root / relative
        path.write_text(
            "from ...client import Client\n"
            "response = client.get_httpx_client().request(**kwargs)\n"
            "response = await client.get_async_httpx_client().request(**kwargs)\n"
        )
    path = root / EMBED_REQUEST
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "        input_: list[str] | str\n"
        + "        if isinstance(self.input_, list):\n" * 3
        + "        model_identity = self.model_identity\n"
        + "                input_type_1 = cast(list[str], data)\n"
        + "            return cast(\n"
    )


def test_required_ndjson_body_is_not_made_optional(tmp_path: Path) -> None:
    write_generated_files(tmp_path, signature_count=5)

    fix_generated_client(tmp_path)

    for relative in FILES:
        source = (tmp_path / relative).read_text()
        assert source.count("body: StatefulQueryRequest | File") == 5
        assert "Unset = UNSET" not in source
    assert (tmp_path / RELATIONAL_QUERY).read_text() == "response_200 = response.text"
    for relative in SQL_OPERATIONS:
        source = (tmp_path / relative).read_text()
        assert "from ....sql_transport import sql_request, sql_request_async" in source
        assert "response = sql_request(client.get_httpx_client(),**kwargs)" in source
        assert "response = await sql_request_async(client.get_async_httpx_client(),**kwargs)" in source


def test_generator_shape_drift_fails_before_writing(tmp_path: Path) -> None:
    write_generated_files(tmp_path, signature_count=4)
    before = {relative: (tmp_path / relative).read_text() for relative in FILES}

    with pytest.raises(RuntimeError, match="unexpected generated shape"):
        fix_generated_client(tmp_path)

    assert {relative: (tmp_path / relative).read_text() for relative in FILES} == before


@pytest.mark.parametrize("operation", SQL_OPERATIONS)
def test_sql_generator_shape_drift_fails_before_any_writes(tmp_path: Path, operation: Path) -> None:
    write_generated_files(tmp_path, signature_count=5)
    path = tmp_path / operation
    path.write_text(
        path.read_text().replace("await client.get_async_httpx_client().request(", "await changed_request(")
    )
    files = [*FILES, RELATIONAL_QUERY, *SQL_OPERATIONS]
    before = {relative: (tmp_path / relative).read_bytes() for relative in files}

    with pytest.raises(RuntimeError, match="unexpected generated shape"):
        fix_generated_client(tmp_path)

    assert {relative: (tmp_path / relative).read_bytes() for relative in files} == before
