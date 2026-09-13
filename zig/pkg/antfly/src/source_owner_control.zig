// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Source ownership is selected by the build root. Keep physical imports out
//! of control roots: Zig tracks literal imports even in inactive branches.
pub const physical_db = struct {};
pub const selected_db = @import("storage/db/control_root.zig");
pub const table_reads = @import("api/table_reads.zig");
pub const table_writes = @import("api/table_writes.zig");
pub const local_query = struct {};
pub const local_write = struct {};
pub const lite_serve = struct {};
