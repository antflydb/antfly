// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
            self.native.fmt(f)
        }
    }
}
impl std::error::Error for SqlError {}
impl From<Error> for SqlError {
    fn from(native: Error) -> Self {
        Self {
            native,
            sqlstate: None,
            message: native.to_string(),
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
    Err(SqlError {
        native: Error::from_code(code),
        sqlstate: diagnostic
            .as_ref()
            .and_then(|v| v["code"].as_str())
            .map(str::to_owned),
        message: diagnostic
            .as_ref()
            .and_then(|v| v["message"].as_str())
            .unwrap_or("Native SQL operation failed")
            .to_owned(),
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
        self.sql_call(|h, out| unsafe {
            sys::antfly_db_sql_json(h, borrow_slice(request.as_ref()), out)
        })
    }
    pub fn sql_describe_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>> {
        self.sql_call(|h, out| unsafe {
            sys::antfly_db_sql_describe_json(h, borrow_slice(request.as_ref()), out)
        })
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
        let mut id = 0;
        self.sql_call(|h, out| unsafe {
            sys::antfly_db_sql_open_cursor_json(h, borrow_slice(request.as_ref()), &mut id, out)
        })?;
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
