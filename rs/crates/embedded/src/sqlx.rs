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

//! SQLx 0.9 driver for embedded Antfly. Native calls run on a dedicated worker
//! with MIN_THREAD_STACK_SIZE; each pooled connection owns a native SQL session.
//! Use `sqlx::query::<Antfly>(...).bind(...).fetch_all(&mut connection)`.
mod connection;
mod types;
pub use connection::{AntflyConnectOptions, AntflyConnection, AntflyTransactionManager};
pub use types::*;
#[derive(Debug)]
pub struct Antfly;
impl sqlx_core::database::Database for Antfly {
    type Connection = AntflyConnection;
    type TransactionManager = AntflyTransactionManager;
    type Row = AntflyRow;
    type QueryResult = AntflyQueryResult;
    type Column = AntflyColumn;
    type TypeInfo = AntflyTypeInfo;
    type Value = AntflyValue;
    type ValueRef<'r> = AntflyValueRef<'r>;
    type Arguments = AntflyArguments;
    type ArgumentBuffer = Vec<serde_json::Value>;
    type Statement = AntflyStatement;
    const NAME: &'static str = "Antfly";
    const URL_SCHEMES: &'static [&'static str] = &["antfly", "file"];
}
pub type AntflyPool = sqlx_core::pool::Pool<Antfly>;
