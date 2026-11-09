// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use antfly_sdk::types;

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: serde_json::Value,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// Serves every connection with `status` and `body`, recording the requests.
fn serve(status: u16, response: &'static str) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut parts = line.split_whitespace();
            let method = parts.next().unwrap_or_default().to_owned();
            let path = parts.next().unwrap_or_default().to_owned();
            let mut headers = Vec::new();
            let mut length = 0;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                let header = header.trim_end();
                if header.is_empty() {
                    break;
                }
                let (name, value) = header.split_once(':').unwrap();
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().unwrap();
                }
                headers.push((name.to_owned(), value.trim().to_owned()));
            }
            let mut raw = vec![0; length];
            reader.read_exact(&mut raw).unwrap();
            let body = serde_json::from_slice(&raw).unwrap_or(serde_json::Value::Null);
            log.lock().unwrap().push(Seen {
                method,
                path,
                headers,
                body,
            });
            write!(
                stream,
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                response.len()
            )
            .unwrap();
        }
    });
    (url, seen)
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

const OPERATION: &str = r#"{"action":"observe","assertions_ready":true,"attempts":1,"config_generation":1,"created_at":1,"error_code":null,"evidence_accepted":true,"id":"op-1","next_retry_at":null,"owner":"me","phase":"succeeded","request_hash":"h","revision":3}"#;

#[test]
fn memory_mutations_generate_idempotency_keys_and_send_scope() {
    let (url, seen) = serve(200, OPERATION);
    let client = antfly_sdk::new_client(&url, reqwest::Client::new());
    let space = client
        .memory_space("agent")
        .database("db")
        .namespace("ns")
        .branch("feature");
    block_on(async {
        let op = space
            .remember("Production deploys with Terraform.", None)
            .await
            .unwrap();
        assert_eq!(op.id, "op-1");
        let outcome: types::MemoryOutcome = serde_json::from_value(serde_json::json!({
            "id": "o1", "task_id": "t1", "success": true, "used_memory_ids": ["m1"],
        }))
        .unwrap();
        space.outcome(&outcome, None).await.unwrap();
        space.pin("m1", Some("needed"), None).await.unwrap();
        space.unpin("m1", None, Some("explicit-key")).await.unwrap();
        assert!(space.remember(" ", None).await.is_err());
        assert!(space.pin("m1", None, Some(&"k".repeat(257))).await.is_err());
    });
    let seen = seen.lock().unwrap();
    let paths: Vec<_> = seen
        .iter()
        .map(|s| (s.method.as_str(), s.path.as_str()))
        .collect();
    assert_eq!(
        paths,
        [
            ("POST", "/memory/v1/spaces/agent/observations"),
            ("POST", "/memory/v1/spaces/agent/outcomes"),
            ("POST", "/memory/v1/spaces/agent/pins"),
            ("POST", "/memory/v1/spaces/agent/pins"),
        ]
    );
    let keys: Vec<_> = seen
        .iter()
        .map(|s| s.header("idempotency-key").unwrap())
        .collect();
    assert!(keys[..3].iter().all(|key| key.len() == 32));
    assert!(keys[0] != keys[1] && keys[1] != keys[2]);
    assert_eq!(keys[3], "explicit-key");
    for request in seen.iter() {
        assert_eq!(request.header("x-antfly-database"), Some("db"));
        assert_eq!(request.header("x-antfly-namespace"), Some("ns"));
        assert_eq!(request.header("x-antfly-memory-branch"), Some("feature"));
    }
    assert_eq!(
        seen[0].body["evidence"][0]["text"],
        "Production deploys with Terraform."
    );
    assert!(
        seen[0].body["evidence"][0]["id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert_eq!(seen[2].body["pinned"], true);
    assert_eq!(seen[2].body["reason"], "needed");
    assert_eq!(seen[3].body["pinned"], false);
}

#[test]
fn memory_reads_omit_idempotency_key() {
    let (url, seen) = serve(
        200,
        r#"{"complete":true,"config_generation":1,"guidance":"g","items":[],"omitted":0,"revision":7,"scanned":2,"subjects":[],"token_upper_bound":10}"#,
    );
    let client = antfly_sdk::new_client(&url, reqwest::Client::new());
    let brief = block_on(client.memory_space("agent").brief(&Default::default())).unwrap();
    assert_eq!(brief.revision, 7);
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].path, "/memory/v1/spaces/agent/brief");
    assert_eq!(seen[0].header("idempotency-key"), None);
    assert_eq!(seen[0].header("x-antfly-database"), None);
    assert_eq!(seen[0].body["limit"], 16);
    assert_eq!(seen[0].body["token_budget"], 1024);
}

#[test]
fn memory_errors_carry_server_code() {
    let (url, _) = serve(
        409,
        r#"{"code":"IdempotencyConflict","error":"key reused with a different body","retryable":false}"#,
    );
    let client = antfly_sdk::new_client(&url, reqwest::Client::new());
    let error = block_on(client.memory_space("agent").pin("a", None, None)).unwrap_err();
    assert_eq!(error.status(), Some(reqwest::StatusCode::CONFLICT));
    match error {
        antfly_sdk::Error::ErrorResponse(response) => {
            assert_eq!(response.code.as_deref(), Some("IdempotencyConflict"));
            assert_eq!(response.error, "key reused with a different body");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn remember_explicit_key_retries_send_identical_evidence() {
    let (url, seen) = serve(200, OPERATION);
    let client = antfly_sdk::new_client(&url, reqwest::Client::new());
    for _ in 0..2 {
        block_on(
            client
                .memory_space("agent")
                .remember("same text", Some("retry")),
        )
        .unwrap();
    }
    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].body, requests[1].body);
    assert_eq!(requests[0].header("idempotency-key"), Some("retry"));
    assert_eq!(requests[1].header("idempotency-key"), Some("retry"));
}

#[test]
fn access_and_review_proposals_reuse_authenticated_scope_and_operation_keys() {
    let (url, seen) = serve(200, OPERATION);
    let client = antfly_sdk::new_client(&url, reqwest::Client::new());
    let space = client.memory_space("agent").branch("feature");
    block_on(async {
        space
            .access(vec!["a".into()], Some("retrieval"))
            .await
            .unwrap();
        let request: types::MemoryConsolidationRequest =
            serde_json::from_value(serde_json::json!({"cursor":"next"})).unwrap();
        space
            .prepare_consolidation(&request, Some("prepare"))
            .await
            .unwrap();
    });
    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].path, "/memory/v1/spaces/agent/accesses");
    assert_eq!(seen[0].header("idempotency-key"), Some("retrieval"));
    assert_eq!(
        seen[1].path,
        "/memory/v1/spaces/agent/consolidation-proposals"
    );
    assert_eq!(seen[1].header("x-antfly-memory-branch"), Some("feature"));
    assert_eq!(seen[1].body["cursor"], "next");
}
