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

use sqlx::postgres::PgPoolOptions;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var("ANTFLY_PGWIRE_URL")?)
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT CAST($1 AS BIGINT)")
            .bind(42i64)
            .fetch_one(&pool)
            .await?,
        42
    );
    for (name, value) in [
        ("SHOW DateStyle", "ISO, MDY"),
        ("SHOW TimeZone", "UTC"),
        ("SHOW extra_float_digits", "3"),
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, String>(name)
                .fetch_one(&pool)
                .await?,
            value
        );
    }
    // Keep a pooled connection and its default SQLx statement cache alive
    // across migrations on another connection.
    sqlx::query("CREATE TABLE cached_sqlx (id TEXT PRIMARY KEY, n BIGINT)")
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO cached_sqlx (id, n) VALUES ('a', 1)")
        .execute(&pool)
        .await?;
    let mut cached = pool.acquire().await?;
    let query = "SELECT n FROM cached_sqlx WHERE id = $1";
    assert_eq!(
        sqlx::query_scalar::<_, i64>(query)
            .bind("a")
            .fetch_one(&mut *cached)
            .await?,
        1
    );
    sqlx::query("ALTER TABLE cached_sqlx ADD COLUMN note TEXT")
        .execute(&pool)
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(query)
            .bind("a")
            .fetch_one(&mut *cached)
            .await?,
        1
    );
    sqlx::query("CREATE INDEX cached_sqlx_n ON cached_sqlx(n)")
        .execute(&pool)
        .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(query)
            .bind("a")
            .fetch_one(&mut *cached)
            .await?,
        1
    );
    drop(cached);
    let mut tx = pool.begin().await?;
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&mut *tx)
            .await?,
        1
    );
    tx.commit().await?;
    pool.close().await;
    println!("sqlx-postgres 0.9: PASS");
    Ok(())
}
