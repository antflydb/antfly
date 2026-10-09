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
