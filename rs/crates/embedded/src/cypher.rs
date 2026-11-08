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

use crate::ffi::{borrow_slice, take_buffer};
use crate::{Database, Error};
use antfly_embedded_sys as sys;

#[derive(Debug)]
pub struct GraphError {
    pub native: Error,
    pub diagnostic: Option<serde_json::Value>,
    pub body: Vec<u8>,
}
impl From<Error> for GraphError {
    fn from(native: Error) -> Self {
        Self {
            native,
            diagnostic: None,
            body: Vec::new(),
        }
    }
}
impl std::fmt::Display for GraphError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(diagnostic) = &self.diagnostic {
            if let Some(message) = diagnostic["message"].as_str() {
                return f.write_str(message);
            }
        }
        self.native.fmt(f)
    }
}
impl std::error::Error for GraphError {}
impl Database {
    fn graph_call(
        &self,
        call: impl FnOnce(*mut sys::antfly_db, *mut sys::antfly_buffer) -> i32,
    ) -> Result<Vec<u8>, GraphError> {
        let (code, body) = self.with_handle(|handle| {
            let mut out = sys::antfly_buffer::default();
            let code = call(handle, &mut out);
            Ok((code, unsafe { take_buffer(out) }))
        })?;
        if code == sys::ANTFLY_OK {
            return Ok(body);
        }
        Err(GraphError {
            native: Error::from_code(code),
            diagnostic: serde_json::from_slice(&body).ok(),
            body,
        })
    }
    pub fn graph_catalog_json(&self, request: impl AsRef<[u8]>) -> Result<Vec<u8>, GraphError> {
        self.graph_call(|handle, out| unsafe {
            sys::antfly_db_graph_catalog_json(handle, borrow_slice(request.as_ref()), out)
        })
    }
    pub fn cypher_json(
        &self,
        graph: impl AsRef<[u8]>,
        request: impl AsRef<[u8]>,
    ) -> Result<Vec<u8>, GraphError> {
        self.graph_call(|handle, out| unsafe {
            sys::antfly_db_cypher_json(
                handle,
                borrow_slice(graph.as_ref()),
                borrow_slice(request.as_ref()),
                out,
            )
        })
    }
    pub fn graph_write_status_json(
        &self,
        transaction_id: impl AsRef<[u8]>,
    ) -> Result<Vec<u8>, GraphError> {
        self.graph_call(|handle, out| unsafe {
            sys::antfly_db_graph_write_status_json(
                handle,
                borrow_slice(transaction_id.as_ref()),
                out,
            )
        })
    }
}
