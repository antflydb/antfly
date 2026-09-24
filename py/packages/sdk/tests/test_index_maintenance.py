"""Shared-resource maintenance proofs are exact, bounded, and never refreshed."""

import json
from unittest.mock import MagicMock, patch

import httpx
import pytest

from antfly import (
    AntflyClient,
    AntflyException,
    IndexMaintenanceOwnerProof,
    IndexMaintenanceRequest,
    IndexMaintenanceResponse,
)


def fixture() -> IndexMaintenanceRequest:
    return IndexMaintenanceRequest(
        table_id="9007199254740999",
        schema_version=7,
        owners=[
            IndexMaintenanceOwnerProof(
                group_id=group,
                generation="9007199254740995",
                slot=0,
                maintenance_epoch="0",
                owner="a" * 64,
                comparison="b" * 64,
                progress_digest="c" * 64,
            )
            for group in ("9007199254740993", "9007199254740994")
        ],
    )


@patch("antfly.client.Client")
def test_maintenance_escaped_paths_and_exact_proofs(mock_client_class: MagicMock) -> None:
    calls: list[httpx.Request] = []
    bodies: list[object] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"acknowledged_groups": ["9007199254740994", "9007199254740993"]})

    with httpx.Client(base_url="http://antfly.test", transport=httpx.MockTransport(handle)) as transport:
        mock_client_class.return_value.get_httpx_client.return_value = transport
        client = AntflyClient(base_url="http://antfly.test")
        request = fixture()
        request.schema_version = 0
        original = request.to_dict()
        result = client.indexes.retry("wiki/media", "by id/#", request)
        assert isinstance(result, IndexMaintenanceResponse)
        client.indexes.repair("wiki/media", "by id/#", request)
        assert [call.url.raw_path for call in calls] == [
            b"/db/v1/tables/wiki%2Fmedia/indexes/by%20id%2F%23/retry",
            b"/db/v1/tables/wiki%2Fmedia/indexes/by%20id%2F%23/repair",
        ]
        assert all(call.method == "POST" for call in calls)
        assert bodies == [original, original]
        assert request.to_dict() == original


@pytest.mark.parametrize(
    "response",
    [
        {},
        {"acknowledged_groups": []},
        {"acknowledged_groups": ["9007199254740993"]},
        {"acknowledged_groups": ["9007199254740993", "9007199254740993"]},
        {"acknowledged_groups": ["9007199254740993", "12"]},
        {"acknowledged_groups": [9007199254740993, "9007199254740994"]},
        {"acknowledged_groups": None},
        {"acknowledged_groups": [["9007199254740993"], "9007199254740994"]},
    ],
)
@patch("antfly.client.Client")
def test_maintenance_rejects_partial_or_malformed_ack_without_retry(
    mock_client_class: MagicMock, response: object
) -> None:
    calls = 0

    def handle(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=response)

    with httpx.Client(base_url="http://antfly.test", transport=httpx.MockTransport(handle)) as transport:
        mock_client_class.return_value.get_httpx_client.return_value = transport
        client = AntflyClient(base_url="http://antfly.test")
        with pytest.raises(AntflyException, match="acknowledgement"):
            client.indexes.retry("rows", "by_id", fixture())
        assert calls == 1


@patch("antfly.client.Client")
def test_maintenance_rejects_excessive_and_invalid_proofs_before_io(mock_client_class: MagicMock) -> None:
    client = AntflyClient(base_url="http://antfly.test")
    excessive = fixture()
    excessive.owners = [excessive.owners[0]] * 129
    with pytest.raises(ValueError, match="128"):
        client.indexes.repair("rows", "by_id", excessive)
    duplicate = fixture()
    duplicate.owners[1].group_id = duplicate.owners[0].group_id
    with pytest.raises(ValueError, match="duplicate"):
        client.indexes.retry("rows", "by_id", duplicate)
    invalid = fixture()
    invalid.owners[0].maintenance_epoch = "18446744073709551616"
    with pytest.raises(ValueError, match="invalid"):
        client.indexes.retry("rows", "by_id", invalid)
    mock_client_class.return_value.get_httpx_client.assert_not_called()


@pytest.mark.parametrize("status, padding, expected", [(200, "x" * (33 << 10), "32768"), (202, "", "202")])
@patch("antfly.client.Client")
def test_maintenance_bounds_ack_bytes_and_http_outcome(
    mock_client_class: MagicMock,
    status: int,
    padding: str,
    expected: str,
) -> None:
    def handle(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status, json={"acknowledged_groups": ["9007199254740993", "9007199254740994"], "padding": padding}
        )

    with httpx.Client(base_url="http://antfly.test", transport=httpx.MockTransport(handle)) as transport:
        mock_client_class.return_value.get_httpx_client.return_value = transport
        client = AntflyClient(base_url="http://antfly.test")
        with pytest.raises(AntflyException, match=expected):
            client.indexes.repair("rows", "by_id", fixture())
