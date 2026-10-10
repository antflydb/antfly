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

use antfly_embedded::sqlx::AntflyArguments;
use antfly_embedded::sqlx::{Antfly, AntflyConnectOptions};
use futures_util::FutureExt;
use serde_json::Value;
use sqlx_core::sql_str::SqlSafeStr;
use sqlx_core::{
    arguments::Arguments,
    connection::ConnectOptions,
    connection::Connection,
    executor::Executor,
    query::{query, query_with},
    row::Row,
};

#[test]
fn sqlx_conformance_and_streaming() {
    let directory = std::env::temp_dir().join(format!(
        "antfly-sqlx-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../../zig/pkg/antfly-embedded/capi-conformance/sql/search-fixture.json"
        ))
        .unwrap();
        let database =
            antfly_embedded::Database::create_default(directory.join("db.aflite")).unwrap();
        database
            .create_table_json(
                fixture["table"].as_str().unwrap(),
                serde_json::to_vec(&fixture["schema"]).unwrap(),
            )
            .unwrap();
        let table = database
            .open_table(fixture["table"].as_str().unwrap())
            .unwrap();
        database
            .create_table_json(
                "history_items",
                serde_json::to_vec(&fixture["history"]).unwrap(),
            )
            .unwrap();
        for index in fixture["indexes"].as_array().unwrap() {
            table
                .add_index_json(serde_json::to_vec(index).unwrap())
                .unwrap();
        }
        table
            .batch_json(serde_json::to_vec(&fixture["batch"]).unwrap())
            .unwrap();
        table.run_until_idle().unwrap();
    }
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let options = AntflyConnectOptions::new(directory.join("db.aflite")).no_sync(true);
            let mut connection = options.connect().await.unwrap();
            let cases: Value = serde_json::from_str(include_str!(
                "../../../../zig/pkg/antfly-embedded/capi-conformance/sql/cases.json"
            ))
            .unwrap();
            for case in cases.as_array().unwrap() {
                let statement = case["statement"].as_str().unwrap().to_owned();
                let mut args = AntflyArguments::default();
                if let Some(parameters) = case["parameters"].as_array() {
                    for value in parameters {
                        args.add(value.clone()).unwrap()
                    }
                }
                let rows =
                    query_with::<Antfly, _>(sqlx_core::sql_str::AssertSqlSafe(statement), args)
                        .fetch_all(&mut connection)
                        .await;
                if let Some(state) = case["sqlstate"].as_str() {
                    assert_eq!(
                        rows.unwrap_err()
                            .as_database_error()
                            .unwrap()
                            .code()
                            .as_deref(),
                        Some(state)
                    );
                    continue;
                }
                let rows = rows.unwrap();
                if let Some(expected) = case["rows"].as_array() {
                    let actual: Vec<Vec<Value>> = rows
                        .iter()
                        .map(|row| {
                            row.columns()
                                .iter()
                                .enumerate()
                                .map(|(i, col)| {
                                    use sqlx_core::column::Column;
                                    match col.type_info().0.as_str() {
                                        "integer" => row
                                            .try_get::<Option<i64>, _>(i)
                                            .unwrap()
                                            .map(|v| Value::String(v.to_string()))
                                            .unwrap_or(Value::Null),
                                        "boolean" => row
                                            .try_get::<Option<bool>, _>(i)
                                            .unwrap()
                                            .map(Value::Bool)
                                            .unwrap_or(Value::Null),
                                        "number" => row
                                            .try_get::<Option<f64>, _>(i)
                                            .unwrap()
                                            .map(|v| serde_json::json!(v))
                                            .unwrap_or(Value::Null),
                                        "json" => row
                                            .try_get::<Option<Value>, _>(i)
                                            .unwrap()
                                            .unwrap_or(Value::Null),
                                        _ => row
                                            .try_get::<Option<String>, _>(i)
                                            .unwrap()
                                            .map(Value::String)
                                            .unwrap_or(Value::Null),
                                    }
                                })
                                .collect()
                        })
                        .collect();
                    assert_eq!(
                        serde_json::to_value(actual).unwrap(),
                        Value::Array(expected.clone())
                    );
                }
            }
            connection
                .execute("CREATE TABLE numbers (n BIGINT)".into_sql_str())
                .await
                .unwrap();
            let mut transaction = connection.begin().await.unwrap();
            for i in 0..300i64 {
                query::<Antfly>("INSERT INTO numbers (n) VALUES ($1)".into_sql_str())
                    .bind(i)
                    .execute(&mut *transaction)
                    .await
                    .unwrap();
            }
            let mut other = options.connect().await.unwrap();
            assert!(
                query::<Antfly>("SELECT n FROM numbers".into_sql_str())
                    .fetch_all(&mut other)
                    .await
                    .unwrap()
                    .is_empty()
            );
            transaction.commit().await.unwrap();
            assert_eq!(
                query::<Antfly>("SELECT n FROM numbers".into_sql_str())
                    .fetch_all(&mut other)
                    .await
                    .unwrap()
                    .len(),
                300
            );
            let described = connection
                .describe("SELECT n FROM numbers WHERE n=$1".into_sql_str())
                .await
                .unwrap();
            assert_eq!(described.columns().len(), 1);
            // Cancellation during native cursor open must not exhaust the
            // connection's cursor quota or leave its session unusable.
            for _ in 0..100 {
                let _ = query::<Antfly>("SELECT n FROM numbers".into_sql_str())
                    .fetch_all(&mut connection)
                    .now_or_never();
            }
            assert_eq!(
                query::<Antfly>("SELECT n FROM numbers".into_sql_str())
                    .fetch_all(&mut connection)
                    .await
                    .unwrap()
                    .len(),
                300
            );
            {
                let mut outer = connection.begin().await.unwrap();
                {
                    let mut inner = outer.begin().await.unwrap();
                    inner
                        .execute("INSERT INTO numbers (n) VALUES (9000)".into_sql_str())
                        .await
                        .unwrap();
                    // Dropping a nested transaction queues savepoint rollback.
                }
                assert_eq!(
                    query::<Antfly>("SELECT n FROM numbers".into_sql_str())
                        .fetch_all(&mut *outer)
                        .await
                        .unwrap()
                        .len(),
                    300
                );
                outer.commit().await.unwrap();
            }
            connection.close().await.unwrap();
            other.close().await.unwrap();
            let mut reopened = options.connect().await.unwrap();
            assert_eq!(
                query::<Antfly>("SELECT n FROM numbers".into_sql_str())
                    .fetch_all(&mut reopened)
                    .await
                    .unwrap()
                    .len(),
                300
            );
            reopened.close().await.unwrap();
        });
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn sqlx_pool_uses_independent_native_connections() {
    let directory = std::env::temp_dir().join(format!(
        "antfly-sqlx-pool-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let pool = sqlx_core::pool::PoolOptions::<Antfly>::new()
                .min_connections(2)
                .max_connections(2)
                .connect_with(AntflyConnectOptions::new(directory.join("db.aflite")))
                .await
                .unwrap();
            let mut first = pool.acquire().await.unwrap();
            let mut second = pool.acquire().await.unwrap();
            first
                .execute("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)".into_sql_str())
                .await
                .unwrap();
            let mut transaction = first.begin().await.unwrap();
            transaction
                .execute("INSERT INTO items (id,name) VALUES (1, 'pending')".into_sql_str())
                .await
                .unwrap();
            assert!(
                query::<Antfly>("SELECT id FROM items".into_sql_str())
                    .fetch_all(&mut *second)
                    .await
                    .unwrap()
                    .is_empty()
            );
            second
                .execute("INSERT INTO items (id,name) VALUES (2, 'other')".into_sql_str())
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            assert_eq!(
                query::<Antfly>("SELECT id FROM items".into_sql_str())
                    .fetch_all(&mut *second)
                    .await
                    .unwrap()
                    .len(),
                2
            );
            first.close().await.unwrap();
            second
                .execute("INSERT INTO items (id,name) VALUES (3, 'after close')".into_sql_str())
                .await
                .unwrap();
            drop(second);
            pool.close().await;
        });
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn sqlx_shares_an_open_database_with_the_document_api() {
    let directory = std::env::temp_dir().join(format!(
        "antfly-sqlx-shared-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("db.aflite");
    std::thread::Builder::new()
        .stack_size(antfly_embedded::MIN_THREAD_STACK_SIZE)
        .spawn(move || {
            let database = std::sync::Arc::new(
                antfly_embedded::Database::create(
                    &path,
                    &antfly_embedded::OpenOptions::new().no_sync(true),
                )
                .unwrap(),
            );
            database
                .batch_json(r#"{"inserts":{"doc":{"text":"document api"}}}"#)
                .unwrap();
            // Independent path-opened connections also work alongside documents.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let independent = AntflyConnectOptions::new(&path)
                    .no_sync(true)
                    .connect()
                    .await
                    .unwrap();
                independent.close().await.unwrap();
                let options = AntflyConnectOptions::new(&path)
                    .with_database(std::sync::Arc::clone(&database));
                let pool = sqlx_core::pool::PoolOptions::<Antfly>::new()
                    .max_connections(2)
                    .connect_with(options)
                    .await
                    .unwrap();
                pool.execute(
                    "CREATE TABLE threads (id TEXT PRIMARY KEY, title TEXT)".into_sql_str(),
                )
                .await
                .unwrap();
                let mut transaction = pool.begin().await.unwrap();
                transaction
                    .execute(
                        "INSERT INTO threads (id, title) VALUES ('t1', 'first')".into_sql_str(),
                    )
                    .await
                    .unwrap();
                transaction.commit().await.unwrap();
                // Both APIs see each other's writes on the one owner.
                let table = database.open_table("threads").unwrap();
                let scanned = String::from_utf8(table.scan_json("{}").unwrap()).unwrap();
                assert!(scanned.contains("\"hashes\""), "scan: {scanned}");
                drop(table);
                let rows = pool
                    .fetch_all(query::<Antfly>("SELECT title FROM threads"))
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get::<String, _>("title"), "first");
                assert!(database.lookup_json("doc").is_ok());
                pool.close().await;
            });
            // Connections never close a shared handle.
            database
                .batch_json(r#"{"inserts":{"after":{"text":"still open"}}}"#)
                .unwrap();
            assert!(database.lookup_json("after").is_ok());
            database.close().unwrap();
        })
        .unwrap()
        .join()
        .unwrap();
}
