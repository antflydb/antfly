import httpx
import pytest

from antfly.sources import SourceServiceError, StandaloneSourcesClient


def test_source_client_uses_companion_protocol_and_structured_errors():
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(202, json={"version": 1, "id": "job", "state": "running", "started_at": 1})
        return httpx.Response(409, json={"version": 1, "code": "sync_in_progress"})

    with StandaloneSourcesClient(
        "https://sources.example", "operator-token", transport=httpx.MockTransport(handler)
    ) as client:
        assert client.sync(restart=True)["id"] == "job"
        assert requests[0].url.path == "/v1/sync"
        assert requests[0].headers["authorization"] == "Bearer operator-token"
        assert requests[0].content == b'{"restart":true}'
        with pytest.raises(SourceServiceError) as error:
            client.job("job")
        assert error.value.status == 409
        assert error.value.code == "sync_in_progress"
        assert "operator-token" not in str(error.value)


def test_source_client_does_not_follow_redirects():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(307, headers={"location": "https://other.example/steal-token"})

    with StandaloneSourcesClient(
        "https://sources.example", "operator-token", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(SourceServiceError) as error:
            client.profile()
        assert error.value.status == 307
    assert len(requests) == 1


@pytest.mark.parametrize(
    "url", ["http://remote.example", "https://user:pass@example.com", "https://example.com?token=x"]
)
def test_source_client_rejects_unsafe_endpoints(url):
    with pytest.raises(ValueError):
        StandaloneSourcesClient(url, "token")


def test_source_client_queues_explicit_purge():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"version": 1, "state": "configured"})

    with StandaloneSourcesClient(
        "https://sources.example", "operator-token", transport=httpx.MockTransport(handler)
    ) as client:
        client.remove_source("a/b", purge=True)
        client.run_purges()
    assert requests[0].url.raw_path == b"/v1/sources/a%2Fb?purge=true"
    assert requests[0].method == "DELETE"
    assert requests[1].url.path == "/v1/sync"
    assert requests[1].content == b'{"purge_only":true}'


def test_common_management_client_preserves_hosted_prefix_and_idempotency():
    from antfly.sources import SourcesClient

    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(202, json={"id": "run", "source_id": "source", "state": "running"})

    prefix = "https://colony.example/api/v1/organizations/org/cloud/instances/instance/sources/v1"
    with SourcesClient(prefix, "hosted-token", transport=httpx.MockTransport(handler)) as client:
        assert client.start_run("source", "submission-1")["id"] == "run"
        client.runs("source", cursor="opaque/cursor")
        client.cancel_run("source", "run")
    assert str(requests[0].url) == prefix + "/sources/source/runs"
    assert requests[0].headers["idempotency-key"] == "submission-1"
    assert requests[1].url.params["cursor"] == "opaque/cursor"
    assert str(requests[2].url) == prefix + "/sources/source/runs/run/cancel"
    assert len(requests) == 3
