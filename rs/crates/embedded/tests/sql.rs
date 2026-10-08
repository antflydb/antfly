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

//! `Database::sql_json`: statements run against the embedded table, and a
//! failed statement keeps the runtime's SQL diagnostics in `SqlError::body`.
//! Requires linking against the real library (`--features libantfly`).

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use antfly_embedded::{Database, Error, OpenOptions};

fn tmp_db(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "antfly-embedded-sql-{tag}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir.join("db.aflite")
}

/// Runs `f` on a thread with the stack libantfly requires.
fn run_with_stack<F: FnOnce() + Send + 'static>(f: F) {
    std::thread::Builder::new()
        .stack_size(antfly_embedded::MIN_THREAD_STACK_SIZE)
        .spawn(f)
        .expect("spawn thread")
        .join()
        .unwrap_or_else(|payload| std::panic::resume_unwind(payload));
}

fn open(tag: &str, schema: &str) -> (Database, PathBuf) {
    let path = tmp_db(tag);
    let db = Database::create(&path, &OpenOptions::new().no_sync(true)).expect("create");
    db.set_schema_json(schema).expect("set schema");
    (db, path)
}

fn rows(response: &[u8]) -> Vec<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_slice(response).expect("SQL response JSON");
    value["rows"].as_array().expect("rows").clone()
}

fn statement(sql: &str) -> String {
    serde_json::json!({ "statement": sql }).to_string()
}

#[test]
fn sql_json_reads_and_mutates_documents() {
    run_with_stack(|| {
        let (db, path) = open(
            "rw",
            r#"{"version":1,"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":true}}}}"#,
        );
        db.batch_json(r#"{"inserts":{"a":{"n":1,"extra":true}}}"#)
            .expect("batch");

        let selected = db
            .sql_json("items", statement("SELECT n FROM items WHERE _id='a'"))
            .expect("select");
        let selected = rows(&selected);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0][0], "1");

        db.sql_json(
            "items",
            statement("INSERT INTO items (_id,n) VALUES ('b',2) RETURNING n"),
        )
        .expect("insert");
        db.sql_json(
            "items",
            statement("UPDATE items SET n=n+10 WHERE _id='a' RETURNING n"),
        )
        .expect("update");

        let stored: serde_json::Value =
            serde_json::from_slice(&db.lookup_json("a").expect("lookup a")).expect("doc a");
        assert_eq!(stored["n"], 11);
        assert_eq!(stored["extra"], true);
        let stored: serde_json::Value =
            serde_json::from_slice(&db.lookup_json("b").expect("lookup b")).expect("doc b");
        assert_eq!(stored["n"], 2);

        let count = db
            .sql_json("items", statement("SELECT COUNT(*) FROM items"))
            .expect("count");
        assert_eq!(rows(&count)[0][0], "2");

        db.close().expect("close");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    });
}

#[test]
fn sql_json_failures_carry_diagnostics() {
    run_with_stack(|| {
        let (db, path) = open(
            "err",
            r#"{"version":1,"enforce_types":true,"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"n":{"type":"integer","minimum":0}},"required":["n"],"additionalProperties":true}}}}"#,
        );

        let rejected = db
            .sql_json(
                "items",
                statement("INSERT INTO items (_id,n) VALUES ('valid',1),('invalid',-1)"),
            )
            .expect_err("schema rejects n = -1");
        assert!(
            !rejected.body.is_empty(),
            "rejected statement should carry SQL diagnostics: {rejected}"
        );
        assert!(rejected.to_string().contains(&rejected.body));
        let count = db
            .sql_json("items", statement("SELECT COUNT(*) FROM items"))
            .expect("count");
        assert_eq!(rows(&count)[0][0], "0", "the whole statement is rejected");

        let ddl = db
            .sql_json("items", statement("CREATE TABLE other (id INT)"))
            .expect_err("embedded SQL rejects DDL");
        assert_ne!(ddl.error, Error::NotFound);

        db.close().expect("close");
        let closed = db
            .sql_json("items", statement("SELECT 1"))
            .expect_err("closed handle");
        assert!(closed.body.is_empty());

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    });
}
