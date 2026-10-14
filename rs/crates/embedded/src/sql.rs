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

//! SQLSTATE-preserving SQL, session, and pull-cursor operations.
use crate::ffi::{borrow_slice, check, take_buffer};
use crate::{Database, Error};
use antfly_embedded_sys as sys;
use std::fmt;

#[derive(Debug, Clone)]
pub struct SqlError {
    pub native: Error,
    pub sqlstate: Option<String>,
    pub message: String,
    pub body: Vec<u8>,
}
impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = &self.sqlstate {
            write!(f, "{code}: {}", self.message)
        } else {
            f.write_str(&self.message)
        }
    }
}
impl SqlError {
    fn with_request(self, request: &[u8]) -> Self {
        let statement = serde_json::from_slice::<serde_json::Value>(request)
            .ok()
            .and_then(|value| value["statement"].as_str().map(str::to_owned));
        match statement {
            Some(statement) => self.with_statement(&statement),
            None => self,
        }
    }

    /// Attach bounded SQL text, never parameter values, while preserving SQLSTATE.
    pub(crate) fn with_statement(mut self, statement: &str) -> Self {
        const LIMIT: usize = 512;
        let mut end = statement.len().min(LIMIT);
        while !statement.is_char_boundary(end) {
            end -= 1;
        }
        let excerpt = format!(
            "{}{}",
            &statement[..end],
            if end < statement.len() { "…" } else { "" }
        );
        self.message.push_str(&format!(" [statement: {excerpt}]"));
        if let Ok(mut body) = serde_json::from_slice::<serde_json::Value>(&self.body) {
            if let Some(error) = body
                .get_mut("error")
                .and_then(serde_json::Value::as_object_mut)
            {
                error.insert("statement".into(), excerpt.into());
                self.body = serde_json::to_vec(&body).expect("SQL diagnostic JSON");
            }
        }
        self
    }
}
impl std::error::Error for SqlError {}
impl From<Error> for SqlError {
    fn from(native: Error) -> Self {
        let message = native.to_string();
        Self {
            native,
            sqlstate: None,
            message,
            body: Vec::new(),
        }
    }
}
pub type Result<T> = std::result::Result<T, SqlError>;
fn output(code: i32, body: Vec<u8>) -> Result<Vec<u8>> {
    if code == sys::ANTFLY_OK {
        return Ok(body);
    }
    let diagnostic = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("error").cloned());
    let native = Error::from_native(code);
    let message = diagnostic
        .as_ref()
        .and_then(|value| value["message"].as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| native.to_string());
    Err(SqlError {
        native,
        sqlstate: diagnostic
            .as_ref()
            .and_then(|v| v["code"].as_str())
            .map(str::to_owned),
        message,
        body,
    })
}
impl Database {
    fn sql_call(
        &self,
        f: impl FnOnce(*mut sys::antfly_db, *mut sys::antfly_buffer) -> i32,
    ) -> Result<Vec<u8>> {
        let (code, body) = self.with_handle(|handle| {
            let mut out = sys::antfly_buffer::default();
            let code = f(handle, &mut out);
            Ok((code, unsafe { take_buffer(out) }))
        })?;
        output(code, body)
    }
    pub fn sql_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        let request = request.as_ref();
        self.sql_call(|h, out| unsafe { sys::antfly_db_sql_json(h, borrow_slice(request), out) })
            .map_err(|error| error.with_request(request))
    }
    pub fn sql_describe_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        let request = request.as_ref();
        self.sql_call(|h, out| unsafe {
            sys::antfly_db_sql_describe_json(h, borrow_slice(request), out)
        })
        .map_err(|error| error.with_request(request))
    }
    pub fn sql_session_open(&self) -> crate::Result<u64> {
        self.with_handle(|h| {
            let mut id = 0;
            check(unsafe { sys::antfly_db_sql_session_open(h, &mut id) })?;
            Ok(id)
        })
    }
    pub fn sql_session_close(&self, id: u64) -> crate::Result<()> {
        self.with_handle(|h| check(unsafe { sys::antfly_db_sql_session_close(h, id) }))
    }
    pub fn sql_cursor_open_json(&self, request: impl AsRef<[u8]>) -> Result<u64> {
        let request = request.as_ref();
        let mut id = 0;
        self.sql_call(|h, out| unsafe {
            sys::antfly_db_sql_open_cursor_json(h, borrow_slice(request), &mut id, out)
        })
        .map_err(|error| error.with_request(request))?;
        Ok(id)
    }
    pub fn sql_cursor_fetch_json(&self, id: u64, rows: u32) -> Result<Vec<u8>> {
        self.sql_call(|h, out| unsafe { sys::antfly_db_sql_fetch_cursor_json(h, id, rows, out) })
    }
    pub fn sql_cursor_close(&self, id: u64) -> crate::Result<()> {
        self.with_handle(|h| check(unsafe { sys::antfly_db_sql_close_cursor(h, id) }))
    }
    pub fn create_table_json(&self, name: &str, schema: impl AsRef<[u8]>) -> crate::Result<()> {
        self.with_handle(|h| {
            check(unsafe {
                sys::antfly_db_create_table_json(
                    h,
                    borrow_slice(name.as_bytes()),
                    borrow_slice(schema.as_ref()),
                )
            })
        })
    }
    pub fn drop_table(&self, name: &str) -> crate::Result<()> {
        self.with_handle(|h| {
            check(unsafe { sys::antfly_db_drop_table(h, borrow_slice(name.as_bytes())) })
        })
    }
    pub fn list_tables_json(&self) -> crate::Result<Vec<u8>> {
        self.read_buffer(|h, out| unsafe { sys::antfly_db_list_tables_json(h, out) })
    }
}
