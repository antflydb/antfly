"""Resolver resources preserve accepted replay and encoded identity keys."""

from unittest.mock import MagicMock

from antfly import AntflyClient, ResolverUnmergeRequest


def test_unmerge_keeps_accepted_revision_and_scoped_resource() -> None:
    client = AntflyClient("https://antfly.test")
    client._request = MagicMock(
        return_value={
            "resolver": "concepts",
            "changed": True,
            "revision": 3,
            "replay_sequence": 3,
            "replay_sequence_kind": "resolver_revision",
            "applied": False,
        }
    )
    result = client.resolvers.in_namespace("app", "private").unmerge(
        "notes",
        "concepts",
        "concept/alpha",
        ResolverUnmergeRequest(pin_distinct=True),
    )
    assert result.revision == 3
    assert result.applied is False
    client._request.assert_called_once_with(
        "DELETE",
        "/db/v1/databases/app/namespaces/private/tables/notes/resolvers/concepts/redirects/concept%2Falpha",
        json={"pin_distinct": True},
        params={},
    )


def test_proposals_and_absorbed_suppression_use_scoped_typed_resources() -> None:
    from antfly import (
        CurationProposalDecisionRequest,
        EntitySuppressRequest,
        EntitySuppressRequestAbsorbed,
    )

    outcomes = [
        {
            "key_template": "concept/{{_entity.text}}",
            "revision": 3,
            "changed": True,
            "absorbed": [{"key": "concept/notes", "disposition": "suppress"}],
        }
    ]
    client = AntflyClient("https://antfly.test")
    client._request = MagicMock(
        return_value={
            "entity_table": "entities",
            "key": "concept/noise",
            "complete": True,
            "atomicity": "namespace",
            "namespaces": outcomes,
            "results": [
                {"source_table": "notes", "resolver": "concepts", "key_template": outcomes[0]["key_template"]},
                {"source_table": "history", "resolver": "concepts", "key_template": outcomes[0]["key_template"]},
            ],
        }
    )
    suppressed = client.entities.suppress(
        "entities", "concept/noise", EntitySuppressRequest(absorbed=EntitySuppressRequestAbsorbed.SUPPRESS)
    )
    assert [item.key for namespace in suppressed.namespaces for item in namespace.absorbed] == ["concept/notes"]
    assert all("absorbed" not in row.to_dict() for row in suppressed.results)
    client._request.assert_called_once_with(
        "POST",
        "/db/v1/tables/entities/entities/concept%2Fnoise:suppress",
        json={"absorbed": "suppress", "pin_distinct": False},
    )
    client._request.reset_mock()
    client._request.return_value = {
        "proposal": {
            "id": "proposal/one",
            "action": "suppress",
            "key": "concept/noise",
            "state": "accepted",
            "origin": {},
        },
        "complete": True,
        "applied": False,
        "namespaces": outcomes,
        "results": [],
    }
    result = client.curation_proposals.in_namespace("app", "private").accept(
        "entities",
        "proposal/one",
        CurationProposalDecisionRequest(actor="curator"),
    )
    assert result.applied is False
    assert [item.key for namespace in result.namespaces for item in namespace.absorbed] == ["concept/notes"]
    client._request.assert_called_once_with(
        "POST",
        "/db/v1/databases/app/namespaces/private/tables/entities/curation/proposals/proposal%2Fone:accept",
        json={"actor": "curator", "pin_distinct": False},
    )


def test_entity_progress_pages_keep_scope_cursor_and_encoded_keys() -> None:
    client = AntflyClient("https://antfly.test")
    client._request = MagicMock(
        return_value={
            "entity_table": "notes",
            "key": "concept/alpha",
            "catalog_revision": 7,
            "complete": False,
            "results": [],
            "next_cursor": "page-two",
        }
    )
    page = client.entities.in_namespace("app", "private").progress(
        "notes", "concept/alpha", cursor="opaque-next", limit=7
    )
    assert page.next_cursor == "page-two"
    client._request.assert_called_once_with(
        "GET",
        "/db/v1/databases/app/namespaces/private/tables/notes/entities/concept%2Falpha/progress",
        params={"limit": 7, "cursor": "opaque-next"},
    )
