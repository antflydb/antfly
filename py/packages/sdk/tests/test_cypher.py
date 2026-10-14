# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json

import httpx
import pytest

from antfly import (
    AntflyClient,
    GraphExecutionError,
    decode_graph_value,
    encode_graph_parameter,
)
from antfly.client_generated.models import CommittedMutationOutcome
from antfly.exceptions import AntflyException


def test_cypher_values_preserve_integer_boundaries_and_user_tags():
    source = {"type": "node", "n": (1 << 63) - 1, "minimum": -(1 << 63)}
    assert decode_graph_value(encode_graph_parameter(source)) == source
    with pytest.raises(ValueError, match="64-bit"):
        encode_graph_parameter(1 << 63)
    with pytest.raises(ValueError, match="finite"):
        encode_graph_parameter(float("nan"))


def test_cypher_write_receipt_inspection_never_replays():
    calls = []
    transaction_id = "a" * 32

    def respond(request):
        calls.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"transaction_id": transaction_id, "disposition": "committed"})
        body = json.loads(request.content)
        assert body["typed_parameters"]["value"]["n"]["value"] == "9223372036854775807"
        assert body["write_request_id"] == transaction_id
        return httpx.Response(
            200,
            json={
                "wire_version": 1,
                "columns": ["n"],
                "rows": [[{"type": "integer", "value": "9223372036854775807"}]],
                "plan": [],
                "stats": {},
                "graph_incarnation": "1",
                "graph_revision": "1",
                "complete": False,
                "commit": {"transaction_id": transaction_id, "status": "unknown"},
            },
        )

    client = AntflyClient("http://graph.test")
    with httpx.Client(base_url=client.base_url, transport=httpx.MockTransport(respond)) as http:
        client._client.set_httpx_client(http)
        result = client.graphs.execute(
            "knowledge",
            "RETURN $n",
            parameters={"n": (1 << 63) - 1},
            write_request_id=transaction_id,
        )
        assert decode_graph_value(result.rows[0][0]) == (1 << 63) - 1
        assert client.graphs.write_status(transaction_id)["disposition"] == "committed"
        with pytest.raises(ValueError, match="transaction ID"):
            client.graphs.write_status("bad")
        assert [request.method for request in calls] == ["POST", "GET"]


def test_cypher_diagnostics_preserve_committed_delivery_outcome():
    client = AntflyClient("http://graph.test")
    with httpx.Client(
        base_url=client.base_url,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                429,
                json={
                    "code": "OutOfMemory",
                    "message": "Encoding failed",
                    "write_outcome": "committed",
                },
            )
        ),
    ) as http:
        client._client.set_httpx_client(http)
        with pytest.raises(GraphExecutionError) as failed:
            client.graphs.execute("knowledge", "CREATE (n:Document) RETURN n")
        assert failed.value.diagnostic["write_outcome"] == "committed"


def test_graph_revision_update_is_lossless_and_never_replays_conflicts():
    calls = []

    def respond(request):
        calls.append(request)
        assert request.method == "PUT"
        assert json.loads(request.content) == {
            "expected_revision": "9223372036854775807",
            "definition": {"labels": [{"name": "Document", "table": "docs"}]},
        }
        return httpx.Response(409, json={"code": "CatalogGenerationChanged"})

    client = AntflyClient("http://graph.test")
    with httpx.Client(base_url=client.base_url, transport=httpx.MockTransport(respond)) as http:
        client._client.set_httpx_client(http)
        with pytest.raises(AntflyException, match="CatalogGenerationChanged"):
            client.graphs.update(
                "knowledge",
                "9223372036854775807",
                {"labels": [{"name": "Document", "table": "docs"}]},
            )
        assert len(calls) == 1
        with pytest.raises(ValueError, match="revision"):
            client.graphs.update("knowledge", "01", {"labels": []})
        assert len(calls) == 1


@pytest.mark.parametrize("operation", ["create", "update"])
def test_graph_catalog_mutations_preserve_committed_visibility_pending(operation):
    client = AntflyClient("http://graph.test")
    with httpx.Client(
        base_url=client.base_url,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(202, json={"status": "committed_visibility_pending"})
        ),
    ) as http:
        client._client.set_httpx_client(http)
        definition = {"labels": [{"name": "Document", "table": "docs"}]}
        result = (
            client.graphs.create("knowledge", definition)
            if operation == "create"
            else client.graphs.update("knowledge", "1", definition)
        )
        assert isinstance(result, CommittedMutationOutcome)
        assert str(result.status) == "committed_visibility_pending"
