import httpx
import pytest

from antfly import SourcesClient, SourceServiceError


def test_source_client_uses_companion_protocol_and_structured_errors():
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(202, json={"version": 1, "id": "job", "state": "running", "started_at": 1})
        return httpx.Response(409, json={"version": 1, "code": "sync_in_progress"})

    with SourcesClient("https://sources.example", "operator-token", transport=httpx.MockTransport(handler)) as client:
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

    with SourcesClient("https://sources.example", "operator-token", transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(SourceServiceError) as error:
            client.profile()
        assert error.value.status == 307
    assert len(requests) == 1


@pytest.mark.parametrize(
    "url", ["http://remote.example", "https://user:pass@example.com", "https://example.com?token=x"]
)
def test_source_client_rejects_unsafe_endpoints(url):
    with pytest.raises(ValueError):
        SourcesClient(url, "token")
