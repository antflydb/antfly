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

//! Manual qualification for #1075; run with `--ignored --nocapture`.
use antfly_embedded::sqlx::{Antfly, AntflyConnectOptions};
use sqlx_core::{executor::Executor, query::query, sql_str::AssertSqlSafe};
use std::time::{Duration, Instant};

fn run(n: u32, shared: bool) {
    let dir = std::env::temp_dir().join(format!(
        "antfly-sqlscale-{}-{n}-{shared}",
        std::process::id()
    ));
    std::fs::create_dir(&dir).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(16 << 20)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let path = dir.join("db.aflite");
        let db = std::sync::Arc::new(
            antfly_embedded::Database::create(
                &path,
                &antfly_embedded::OpenOptions::new().no_sync(true),
            )
            .unwrap(),
        );
        let mut options = AntflyConnectOptions::new(&path).no_sync(true);
        if shared {
            options = options.with_database(std::sync::Arc::clone(&db));
        }
        let pool = sqlx_core::pool::PoolOptions::<Antfly>::new()
            .min_connections(n)
            .max_connections(n)
            .connect_with(options)
            .await
            .unwrap();
        pool.execute(query::<Antfly>(
            "CREATE TABLE t (id TEXT PRIMARY KEY, n BIGINT NOT NULL)",
        ))
        .await
        .unwrap();
        let start = Instant::now();
        let deadline = start + Duration::from_secs(8);
        let mut tasks = Vec::new();
        for w in 0..n {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move {
                let (mut i, mut done, mut max_ms) = (0u64, 0u64, 0f64);
                while Instant::now() < deadline {
                    i += 1;
                    for st in [
                        format!("INSERT INTO t (id, n) VALUES ('w{w}-{i}', {i})"),
                        format!("SELECT n FROM t WHERE id = 'w{w}-{i}'"),
                        format!("UPDATE t SET n = n + 1 WHERE id = 'w{w}-{i}'"),
                    ] {
                        let s = Instant::now();
                        pool.execute(query::<Antfly>(AssertSqlSafe(st)))
                            .await
                            .unwrap();
                        max_ms = f64::max(max_ms, s.elapsed().as_secs_f64() * 1000.0);
                        done += 1;
                    }
                }
                (done, max_ms)
            }));
        }
        let (mut total, mut worst) = (0u64, 0f64);
        for t in tasks {
            let (d, m) = t.await.unwrap();
            total += d;
            worst = worst.max(m);
        }
        println!(
            "SCALE shared={shared} connections={n}: {:.0} statements/s, worst statement {:.0} ms",
            total as f64 / start.elapsed().as_secs_f64(),
            worst
        );
        pool.close().await;
        db.close().unwrap();
    });
    drop(rt);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
#[ignore = "manual SQL connection throughput qualification"]
fn sql_connection_scaling() {
    std::thread::Builder::new()
        .stack_size(64 << 20)
        .spawn(|| {
            for n in [1, 4] {
                run(n, false);
                run(n, true);
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
