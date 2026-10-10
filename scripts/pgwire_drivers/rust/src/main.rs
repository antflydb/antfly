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
