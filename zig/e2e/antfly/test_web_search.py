# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Web-search retrieval through the public API, with deterministic HTTP providers."""

from __future__ import annotations

import json
import os
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import requests
from conftest import DEFAULT_ANTFLY_BIN, resolve_binary_path
from helpers import start_http_server
from port_reservations import LoopbackPortReservations

pytestmark = pytest.mark.e2e_resource("antfly_process")


@pytest.fixture
def web_runtime(tmp_path, request):
    search_provider = getattr(request, "param", "exa")
    live = search_provider == "tavily-live"
    if live:
        search_provider = "tavily"
        if not os.environ.get("TAVILY_API_KEY"):
            pytest.skip("Set TAVILY_API_KEY to run the live Tavily smoke test")
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).is_file():
        pytest.skip("Build Antfly or set ANTFLY_BIN")
    state = {
        "provider": search_provider,
        "live": live,
        "searches": [],
        "generations": [],
        "status": 200,
        "malformed": False,
        "delay": 0,
        "search_calls": 1,
        "parallel_searches": 1,
        "database_first": False,
        "answer_summary": False,
        "binary": binary,
    }

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            status = 200
            if self.path == "/search":
                state["searches"].append((dict(self.headers), payload))
                status = state["status"]
                time.sleep(state["delay"])
                if status != 200:
                    body = {"error": "provider echoed secret: test-exa-secret"}
                elif state["malformed"]:
                    body = {"not_results": []}
                else:
                    body = {
                        "results": [
                            {
                                "url": "https://example.com/evidence",
                                "title": "Exa evidence",
                                "text": "EXA-HTTP-CANARY-731",
                                "highlights": ["The canary came from Exa."],
                                "score": 0.9,
                            },
                            {
                                "url": "https://example.com/evidence",
                                "text": "duplicate",
                            },
                            {
                                "url": "https://outside.example/doc",
                                "text": "outside policy",
                            },
                        ]
                    }
                if (
                    search_provider == "tavily"
                    and status == 200
                    and not state["malformed"]
                ):
                    for item in body["results"]:
                        item["content"] = item.pop("text", "")
                        item["raw_content"] = None
                        item["published_date"] = None
                        item.pop("highlights", None)
                    body["answer"] = "UNSOURCED-PROVIDER-ANSWER"
            elif self.path == "/v1/chat/completions":
                state["generations"].append(payload)
                previous = [m for m in payload["messages"] if m["role"] == "tool"]
                if len(previous) >= state["search_calls"]:
                    answer = (
                        "EXA-HTTP-CANARY-731 [source](https://example.com/evidence)"
                    )
                    if live:
                        answer = "Live Tavily search completed."
                    if state["answer_summary"]:
                        summary = json.loads(previous[-1]["content"])["results"][0]
                        count = (
                            summary["aggregations"]["doc_count"]["value"]
                            if "aggregations" in summary
                            else summary["hits"]["total"]["value"]
                        )
                        answer = f"Document count: {count:g}"
                    message = {"role": "assistant", "content": answer}
                else:
                    message = {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call-exa"
                                if not previous and call_index == 0
                                else f"call-exa-{len(previous) + call_index}",
                                "type": "function",
                                "function": {
                                    "name": "search"
                                    if state["database_first"] and not previous
                                    else "web_search",
                                    "arguments": json.dumps(
                                        {"query_index": 0}
                                        if state["database_first"] and not previous
                                        else {
                                            "query": "Tavily search API documentation"
                                            if live
                                            else "Antfly evidence"
                                        }
                                    ),
                                },
                            }
                            for call_index in range(state["parallel_searches"])
                        ],
                    }
                body = {
                    "id": "test-completion",
                    "object": "chat.completion",
                    "created": 1,
                    "model": "test-model",
                    "choices": [
                        {"index": 0, "message": message, "finish_reason": "stop"}
                    ],
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 5,
                        "total_tokens": 15,
                    },
                }
            else:
                self.send_error(404)
                return
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            try:
                self.wfile.write(encoded)
            except BrokenPipeError:
                pass

        def log_message(self, *_args):
            pass

    provider = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = start_http_server(provider)
    provider_url = f"http://127.0.0.1:{provider.server_port}"
    state["provider_url"] = provider_url
    config = {
        "connections": {
            "exa-test": {
                "kind": "web_search",
                "provider": search_provider,
                "capabilities": ["web.search", "agents.use"],
                "web_search": {
                    "endpoint": provider_url + "/search",
                    "api_key": "${secret:exa.test}",
                    "include_content": True,
                    "include_highlights": search_provider == "exa",
                    "include_domains": ["example.com"],
                    "max_results": 2,
                },
            }
        }
    }
    if live:
        config["connections"]["exa-test"]["web_search"].update(
            endpoint="https://api.tavily.com/search",
            api_key="${secret:tavily.api_key}",
            include_domains=["tavily.com"],
        )
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    ports = LoopbackPortReservations()
    port = ports.reserve()
    url = f"http://127.0.0.1:{port}/db/v1"
    env = dict(os.environ, EXA_TEST="test-exa-secret")
    log_path = tmp_path / "server.log"
    proc = None
    with log_path.open("w") as log:
        try:
            proc = ports.handoff_to(
                (port,),
                lambda: subprocess.Popen(
                    [
                        binary,
                        "standalone",
                        "--config",
                        str(config_path),
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                        "--health",
                        "false",
                        "--data-dir",
                        str(tmp_path / "data"),
                        "--models-dir",
                        str(tmp_path / "models"),
                        "--ml-dir",
                        str(tmp_path / "ml"),
                    ],
                    stdout=log,
                    stderr=log,
                    env=env,
                    cwd=tmp_path,
                ),
            )
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                assert proc.poll() is None, log_path.read_text()
                try:
                    if requests.get(url + "/tables", timeout=1).status_code == 200:
                        break
                except requests.RequestException:
                    pass
                time.sleep(0.1)
            else:
                pytest.fail("Antfly startup timed out: " + log_path.read_text())
            payload = {
                "query": "Use web_search to find Antfly evidence and cite the source URL.",
                "queries": [],
                "stream": False,
                "max_internal_iterations": 3,
                "generator": {
                    "provider": "openai",
                    "model": "test-model",
                    "api_key": "test-openai-key",
                    "url": provider_url + "/v1",
                },
                "steps": {"generation": {}},
                "tools": {
                    "enabled_tools": ["web_search"],
                    "web_search_connection": "exa-test",
                },
            }
            yield url, payload, state
        finally:
            if proc is not None:
                proc.terminate()
                try:
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
            ports.close()
            provider.shutdown()
            provider.server_close()
            thread.join(timeout=5)


@pytest.mark.parametrize("stream", [False, True])
def test_exa_web_only_returns_sources_and_tool_history(web_runtime, stream):
    url, payload, state = web_runtime
    payload["stream"] = stream
    # Exercise provider-specific options which aren't in the shared base type.
    payload["tools"]["web_search_config"] = {
        "provider": "exa",
        "num_results": 1,
        "start_published_date": "2026-01-01T00:00:00Z",
    }
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == 200, response.text
    if stream:
        events = []
        for frame in response.text.strip().split("\n\n"):
            fields = dict(
                line.split(": ", 1) for line in frame.splitlines() if ": " in line
            )
            if "event" in fields and "data" in fields:
                events.append((fields["event"], json.loads(fields["data"])))
        assert sum(name == "hit" for name, _ in events) == 1
        assert sum(name == "done" for name, _ in events) == 1
        assert "EXA-HTTP-CANARY-731" in response.text
    else:
        result = response.json()
        assert result["status"] == "completed", result
        assert result["tool_calls_made"] == 1
        assert len(result["hits"]) == 1
        assert result["hits"][0]["_source"]["url"] == "https://example.com/evidence"
        assert "https://example.com/evidence" in result["generation"]
        assert any(
            s["name"] == "web_search" and s["status"] == "success"
            for s in result["steps"]
        )
    assert len(state["searches"]) == 1
    headers, search = state["searches"][0]
    assert {k.lower(): v for k, v in headers.items()}["x-api-key"] == "test-exa-secret"
    assert search["numResults"] == 1
    assert search["includeDomains"] == ["example.com"]
    assert search["startPublishedDate"] == "2026-01-01T00:00:00Z"
    assert search["contents"]["text"]["maxCharacters"] == 4000
    assert search["moderation"] is True
    assert [t["function"]["name"] for t in state["generations"][0]["tools"]] == [
        "web_search"
    ]
    history = state["generations"][1]["messages"]
    assert history[-1]["role"] == "tool" and history[-1]["tool_call_id"] == "call-exa"
    assert "EXA-HTTP-CANARY-731" in history[-1]["content"]
    assert "test-exa-secret" not in json.dumps(state["generations"])
    assert "test-exa-secret" not in response.text


@pytest.mark.parametrize(
    "status,malformed",
    [(401, False), (429, False), (500, False), (302, False), (200, True)],
)
@pytest.mark.parametrize("web_runtime", ["exa", "tavily"], indirect=True)
def test_exa_failure_never_becomes_grounded_answer(web_runtime, status, malformed):
    url, payload, state = web_runtime
    state.update(status=status, malformed=malformed)
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "incomplete"
    assert result.get("generation") is None
    assert result["hits"] == []
    assert any(
        s["name"] == "web_search" and s["status"] == "error" for s in result["steps"]
    )
    assert "test-exa-secret" not in response.text
    assert "test-exa-secret" not in json.dumps(state["generations"])
    assert len(state["searches"]) == 1
    feedback = json.loads(state["generations"][1]["messages"][-1]["content"])
    assert feedback["provider"] == state["provider"]


def test_exa_connection_cannot_be_redirected_by_request(web_runtime):
    url, payload, state = web_runtime
    payload["tools"]["web_search_config"] = {
        "provider": "exa",
        "endpoint": "http://127.0.0.1:1/search",
    }
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == 403, response.text
    assert state["searches"] == []
    assert state["generations"] == []


def test_exa_deadline_is_bounded(web_runtime):
    url, payload, state = web_runtime
    state["delay"] = 0.3
    payload["tools"]["web_search_config"] = {"provider": "exa", "timeout_ms": 20}
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=10)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == "incomplete"
    assert result["hits"] == []
    assert result.get("generation") is None


def test_exa_mixed_database_and_step_level_configuration(web_runtime):
    url, payload, state = web_runtime
    created = requests.post(
        url + "/tables/web_mixed", json={"num_shards": 1}, timeout=30
    )
    assert created.status_code == 200, created.text
    payload["queries"] = [
        {"table": "web_mixed", "full_text_search": {"query": "antfly"}, "limit": 2}
    ]
    payload["tools"]["enabled_tools"].append("full_text_search")
    payload["steps"]["retrieval"] = {"tools": payload.pop("tools")}
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "completed"
    assert len(response.json()["hits"]) == 1
    assert {t["function"]["name"] for t in state["generations"][0]["tools"]} == {
        "build_query",
        "search",
        "web_search",
    }


def test_exa_cli_supports_web_only_named_connection(web_runtime):
    url, _, state = web_runtime
    provider = state.get("provider_url")
    result = subprocess.run(
        [
            state["binary"],
            "agents",
            "retrieval",
            "--web-search-connection",
            "exa-test",
            "--intent",
            "Find evidence on the web",
            "--no-streaming",
            "--generator",
            json.dumps(
                {
                    "provider": "openai",
                    "model": "test-model",
                    "api_key": "test-openai-key",
                    "url": provider + "/v1",
                }
            ),
        ],
        env={**os.environ, "ANTFLY_URL": url.removesuffix("/db/v1")},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["status"] == "completed"
    assert len(state["searches"]) == 1


@pytest.mark.parametrize(
    "override",
    [
        {"api_key": "replacement"},
        {"endpoint": "http://127.0.0.1:1/search"},
        {"safe_search": False},
        {"include_domains": ["elsewhere.example"]},
        {"include_domains": []},
    ],
)
def test_exa_connection_rejects_policy_expansion(web_runtime, override):
    url, payload, state = web_runtime
    payload["tools"]["web_search_config"] = {"provider": "exa", **override}
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == 403, response.text
    assert not state["searches"]
    assert not state["generations"]


def test_exa_rejects_conflicting_configuration_scopes(web_runtime):
    url, payload, state = web_runtime
    payload["steps"]["retrieval"] = {
        "tools": {"web_search_config": {"provider": "exa"}}
    }
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == 400, response.text
    assert not state["searches"]
    assert not state["generations"]


@pytest.mark.parametrize("parallel", [1, 3])
@pytest.mark.parametrize("stream", [False, True])
def test_exa_repeated_searches_share_context_budget(web_runtime, parallel, stream):
    url, payload, state = web_runtime
    state.update(search_calls=3, parallel_searches=parallel)
    payload.update(
        max_context_tokens=128,
        reserve_tokens=0,
        max_internal_iterations=4,
        stream=stream,
    )
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == 200, response.text
    if stream:
        done = next(
            frame
            for frame in response.text.split("\n\n")
            if frame.startswith("event: done\n")
        )
        result = json.loads(done.split("data: ", 1)[1])
    else:
        result = response.json()
    assert result["status"] == "incomplete"
    assert result.get("generation") is None
    assert len(state["searches"]) == 2
    for generation in state["generations"]:
        evidence_bytes = sum(
            len(m["content"].encode())
            for m in generation["messages"]
            if m["role"] == "tool"
        )
        assert evidence_bytes <= 128 * 4
    assert any(
        step["action"] == "stopped retrieval at the accumulated context budget"
        for step in result["steps"]
    )


def test_exa_and_database_results_share_context_budget(web_runtime):
    url, payload, state = web_runtime
    created = requests.post(
        url + "/tables/web_budget", json={"num_shards": 1}, timeout=30
    )
    assert created.status_code == 200, created.text
    payload["queries"] = [
        {"table": "web_budget", "full_text_search": {"query": "antfly"}, "limit": 2}
    ]
    payload["tools"]["enabled_tools"].append("full_text_search")
    payload["max_internal_iterations"] = 4
    state.update(database_first=True, search_calls=3)
    # Measure the serialized evidence, then allow room for only two results.
    initial = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert initial.status_code == 200, initial.text
    assert initial.json()["status"] == "completed"
    messages = [m for m in state["generations"][-1]["messages"] if m["role"] == "tool"]
    assert len(messages) == 3
    budget = (sum(len(m["content"].encode()) for m in messages[:2]) + 3) // 4
    state["generations"].clear()
    state["searches"].clear()
    payload.update(max_context_tokens=budget, reserve_tokens=0)
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "incomplete"
    assert response.json().get("generation") is None
    for generation in state["generations"]:
        assert (
            sum(
                len(m["content"].encode())
                for m in generation["messages"]
                if m["role"] == "tool"
            )
            <= budget * 4
        )


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("aggregate", [False, True])
def test_pruned_documents_preserve_summary_evidence(web_runtime, stream, aggregate):
    url, payload, state = web_runtime
    state.update(database_first=True, answer_summary=True)
    created = requests.post(
        url + "/tables/summary_budget", json={"num_shards": 1}, timeout=30
    )
    assert created.status_code == 200, created.text
    inserted = requests.post(
        url + "/tables/summary_budget/batch",
        json={
            "inserts": {"a": {"title": "antfly", "body": "A" * 6000}},
            "sync_level": "write",
        },
        timeout=30,
    )
    assert inserted.status_code == 201, inserted.text
    query = {"table": "summary_budget", "query": {"match_all": {}}, "limit": 1}
    if aggregate:
        query["aggregations"] = {"doc_count": {"type": "count", "field": "title"}}
    payload.update(query="How many documents are there?", queries=[query], tools={})
    # Wait for publication and verify the document is actually oversized.
    deadline = time.monotonic() + 10
    while True:
        state["generations"].clear()
        initial = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
        assert initial.status_code == 200, initial.text
        if initial.json()["hits"]:
            break
        assert time.monotonic() < deadline, initial.text
        time.sleep(0.05)
    original = state["generations"][-1]["messages"][-1]["content"]
    assert len(original.encode()) > 1024

    payload.update(max_context_tokens=256, reserve_tokens=0, stream=stream)
    state["generations"].clear()
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == 200, response.text
    if stream:
        done = next(
            frame
            for frame in response.text.split("\n\n")
            if frame.startswith("event: done\n")
        )
        result = json.loads(done.split("data: ", 1)[1])
    else:
        result = response.json()
    assert result["status"] == "completed"
    assert result["generation"] == "Document count: 1"
    messages = [m for m in state["generations"][-1]["messages"] if m["role"] == "tool"]
    assert len(messages) == 1
    assert len(messages[0]["content"].encode()) <= 1024
    evidence = json.loads(messages[0]["content"])
    assert evidence["hits"] == [] and evidence["truncated"]
    summary = evidence["results"][0]
    assert summary["hits"]["total"]["value"] == 1
    if aggregate:
        assert summary["aggregations"]["doc_count"]["value"] == 1

    # A summary which itself exceeds the budget must still stop generation.
    payload.update(max_context_tokens=8, stream=False)
    state["generations"].clear()
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "incomplete"
    assert response.json().get("generation") is None
    assert len(state["generations"]) == 1


@pytest.mark.parametrize("web_runtime", ["tavily"], indirect=True)
@pytest.mark.parametrize("stream", [False, True])
def test_tavily_sources_and_wire_contract(web_runtime, stream):
    url, payload, state = web_runtime
    payload["stream"] = stream
    payload["tools"]["web_search_config"] = {
        "provider": "tavily",
        "max_results": 1,
        "search_depth": "advanced",
        "include_answer": True,
        "include_raw_content": True,
    }
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == 200, response.text
    assert "EXA-HTTP-CANARY-731" in response.text
    assert "UNSOURCED-PROVIDER-ANSWER" not in response.text
    if stream:
        assert "event: hit" in response.text and "event: done" in response.text
    else:
        result = response.json()
        assert result["status"] == "completed"
        assert len(result["hits"]) == 1
        assert result["hits"][0]["_source"]["provider"] == "tavily"
        assert result["hits"][0]["_source"]["text"] == "EXA-HTTP-CANARY-731"
        step = next(s for s in result["steps"] if s["name"] == "web_search")
        assert step["details"]["provider"] == "tavily"
    assert len(state["searches"]) == 1
    headers, wire = state["searches"][0]
    headers = {k.lower(): v for k, v in headers.items()}
    assert headers["authorization"] == "Bearer test-exa-secret"
    assert "x-api-key" not in headers
    assert wire["search_depth"] == "advanced" and wire["max_results"] == 1
    assert wire["include_answer"] is True and wire["include_raw_content"] is True
    assert wire["safe_search"] is True and wire["include_domains"] == ["example.com"]
    assert "api_key" not in wire and "numResults" not in wire
    assert "test-exa-secret" not in response.text
    assert "test-exa-secret" not in json.dumps(state["generations"])
    assert "UNSOURCED-PROVIDER-ANSWER" not in json.dumps(state["generations"])
    history = state["generations"][1]["messages"]
    assert json.loads(history[-1]["content"])["provider"] == "tavily"


@pytest.mark.parametrize("web_runtime", ["tavily"], indirect=True)
@pytest.mark.parametrize(
    "override,status",
    [
        ({"provider": "exa"}, 400),
        ({"provider": "tavily", "endpoint": "http://127.0.0.1:1/search"}, 403),
        ({"provider": "tavily", "api_key": "replacement"}, 403),
        ({"provider": "tavily", "safe_search": False}, 403),
        ({"provider": "tavily", "include_domains": []}, 403),
    ],
)
def test_tavily_connection_policy(web_runtime, override, status):
    url, payload, state = web_runtime
    payload["tools"]["web_search_config"] = override
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=30)
    assert response.status_code == status, response.text
    assert state["searches"] == [] and state["generations"] == []


@pytest.mark.parametrize("web_runtime", ["tavily-live"], indirect=True)
@pytest.mark.parametrize("stream", [False, True])
def test_tavily_live_search(web_runtime, stream):
    """Opt-in real Tavily search through HTTP retrieval; generator is deterministic."""
    url, payload, state = web_runtime
    payload["query"] = "Find Tavily search API documentation."
    payload["stream"] = stream
    payload["tools"]["web_search_config"] = {
        "provider": "tavily",
        "search_depth": "basic",
        "max_results": 2,
        "include_raw_content": True,
        "timeout_ms": 30000,
    }
    response = requests.post(url + "/agents/retrieval", json=payload, timeout=60)
    assert response.status_code == 200
    if stream:
        hits = []
        for frame in response.text.strip().split("\n\n"):
            fields = dict(
                line.split(": ", 1) for line in frame.splitlines() if ": " in line
            )
            if fields.get("event") == "hit":
                hits.append(json.loads(fields["data"]))
        assert "event: done" in response.text
    else:
        result = response.json()
        assert result["status"] == "completed"
        hits = result["hits"]
    assert 1 <= len(hits) <= 2
    for hit in hits:
        assert hit["_source"]["provider"] == "tavily"
        assert hit["_source"]["url"].startswith("https://")
        assert hit["_source"]["text"]
    assert os.environ["TAVILY_API_KEY"] not in response.text
    assert os.environ["TAVILY_API_KEY"] not in json.dumps(state["generations"])
