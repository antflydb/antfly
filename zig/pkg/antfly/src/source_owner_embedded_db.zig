// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Browser/local DB profile; native owner configuration is not a browser dependency.
pub const physical_db = @import("storage/db/db.zig");
pub const selected_db = @import("storage/db/mod.zig");
pub const local_query = @import("storage/local_query.zig");
