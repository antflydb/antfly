// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const api_e2e = @import("api/e2e.zig");
const sql_catalog = @import("api/sql_catalog.zig");
const httpx_handler = @import("api/httpx_handler.zig");
const schema_ddl = @import("sql/schema_ddl.zig");
const table_schema_impl = @import("schema/table_schema_impl.zig");
const backups = @import("api/backups.zig");
const http_server = @import("api/http_server.zig");
const db = @import("antfly_source_root").antfly_sources.physical_db;

test {
    _ = api_e2e;
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
