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

//! Source ownership is selected by the build root. Keep physical imports out
//! of control roots: Zig tracks literal imports even in inactive branches.
pub const physical_db = struct {};
pub const selected_db = @import("antfly_local_sources").storage_db_control_root;
pub const table_reads = @import("api/table_reads.zig");
pub const table_writes = @import("api/table_writes.zig");
pub const local_query = struct {};
pub const local_write = struct {};

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
