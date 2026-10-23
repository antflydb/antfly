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

const api_integration_tests = @import("api/integration_test.zig");
const sql_catalog = @import("api/sql_catalog.zig");
const httpx_handler = @import("api/httpx_handler.zig");
const schema_ddl = @import("antfly_local_sources").sql_schema_ddl;
const table_schema_impl = @import("antfly_local_sources").schema_table_schema_impl;
const backups = @import("api/backups.zig");
const http_server = @import("api/http_server.zig");
const db = @import("antfly_source_root").antfly_sources.physical_db;

test {
    _ = api_integration_tests;
    _ = sql_catalog;
    _ = httpx_handler;
    _ = schema_ddl;
    _ = table_schema_impl;
    _ = backups;
    _ = http_server;
    _ = db;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
