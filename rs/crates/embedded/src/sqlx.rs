// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
