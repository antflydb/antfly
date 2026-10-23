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

//! Server credential policy adapter for the shared embedded SQL cursor.
const std = @import("std");
const local = @import("antfly_local_sources").sql_lake_cursor;
const catalog = @import("antfly_local_sources").sql_catalog;
const operation = @import("antfly_local_sources").api_operation;
const configured = @import("../serverless/configured_object_store_support.zig");
pub const openPinned = local.openPinned;
pub fn open(alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan, context: operation.RequestContext, options: configured.BindingObjectStoreOpenOptions) !catalog.Cursor {
    return local.open(alloc, table, request, context, options.lakeOptions());
}
pub fn openWithCache(alloc: std.mem.Allocator, table: catalog.Table, request: catalog.Scan, context: operation.RequestContext, options: configured.BindingObjectStoreOpenOptions, cache: ?*@import("antfly_local_sources").serverless_query_lake_serving_cache.Cache, io: ?std.Io) !catalog.Cursor {
    return local.openWithCache(alloc, table, request, context, options.lakeOptions(), cache, io);
}
