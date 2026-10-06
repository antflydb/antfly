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

//! Embedded lake readers and pinned scans. Formats listed in metadata are not
//! promises of implemented readers: Parquet and Iceberg are implemented today.
pub const source = @import("serverless/query/lake_serving.zig");
pub const stream = @import("serverless/query/lake_stream.zig");
pub const host = @import("serverless/lake_host.zig");
pub const external = @import("serverless/external_source/mod.zig");
pub const parquet = @import("serverless/query/lake_parquet_rowgroup.zig");
pub const rows = @import("storage/rowsource/types.zig");
pub const identity = @import("storage/rowsource/identity.zig");
pub const sql_catalog = @import("sql/catalog.zig");
pub const sql_cursor = @import("sql/lake_cursor.zig");
pub const sql_compiler = @import("sql/compiler.zig");
pub const sql_runtime = @import("sql/runtime.zig");
