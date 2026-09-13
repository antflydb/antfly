// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Source ownership is selected by the build root. Keep physical imports out
//! of control roots: Zig tracks literal imports even in inactive branches.
pub const physical_db = @import("storage/db/db.zig");
pub const selected_db = @import("storage/db/mod.zig");
pub const table_reads = @import("api/table_reads.zig");
pub const table_writes = @import("api/table_writes.zig");
pub const local_query = @import("storage/local_query.zig");
pub const local_write = @import("storage/local_write.zig");
pub const lite_serve = @import("cmd/lite_serve.zig");
