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

//! Source ownership is selected by the build root. Keep physical imports out
//! of control roots: Zig tracks literal imports even in inactive branches.
pub const physical_db = @import("antfly_local_sources").storage_db_db;
pub const selected_db = @import("antfly_local_sources").storage_db_mod;
pub const table_reads = @import("api/table_reads.zig");
pub const table_writes = @import("api/table_writes.zig");
pub const local_query = @import("antfly_local_sources").storage_local_query;
pub const local_write = @import("antfly_local_sources").storage_local_write;

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

/// Engine-selected storage builds (the storage benchmarks) route DB calls
/// through the engine owner, like the storage and embedded-db source roots.
pub const storage_engine = @import("antfly_local_sources").storage_engine;
