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

#![cfg(all(feature = "libantfly", feature = "serde"))]

use antfly_embedded::Database;
use serde_json::{Value, json};

#[test]
fn graph_writes_preserve_diagnostics_and_receipts() {
    std::thread::Builder::new()
        .stack_size(antfly_embedded::MIN_THREAD_STACK_SIZE)
        .spawn(|| {
            let directory = std::env::temp_dir().join(format!("antfly-rust-cypher-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
            std::fs::create_dir_all(&directory).unwrap();
            let db = Database::create_hosted(directory.join("graph.aflite")).unwrap();
            db.sql_json(br#"{"statement":"CREATE TABLE docs (id TEXT)"}"#).unwrap();
            db.graph_catalog_json(br#"{"action":"put","name":"knowledge","definition":{"labels":[{"name":"Document","table":"docs"}]}}"#).unwrap();
            let id = "aabbccddeeff00112233445566778899";
            let request = serde_json::to_vec(&json!({"query":"CREATE (n:Document {id:'created'}) RETURN n.id","write_request_id":id})).unwrap();
            db.cypher_json("knowledge", &request).unwrap();
            let duplicate = db.cypher_json("knowledge", &request).unwrap_err().diagnostic.unwrap();
            assert_eq!(duplicate["code"], "CypherWriteRequestAlreadyUsed");
            assert_eq!(duplicate["transaction_id"], id);
            let aborted = db.cypher_json("knowledge", br#"{"query":"CREATE (n:Document {id:'aborted'}) RETURN 1/0"}"#).unwrap_err().diagnostic.unwrap();
            assert_eq!(aborted["code"], "CypherDivisionByZero");
            let result: Value = serde_json::from_slice(&db.cypher_json("knowledge", br#"{"query":"MATCH (n:Document {id:'aborted'}) RETURN n"}"#).unwrap()).unwrap();
            assert_eq!(result["rows"], json!([]));
            let receipt: Value = serde_json::from_slice(&db.graph_write_status_json(id).unwrap()).unwrap();
            assert_eq!(receipt["disposition"], "committed");
            assert_eq!(receipt["durable"], true);
            drop(db);
            std::fs::remove_dir_all(directory).unwrap();
        }).unwrap().join().unwrap();
}
