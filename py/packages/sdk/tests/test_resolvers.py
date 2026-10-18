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
