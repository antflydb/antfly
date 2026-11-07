// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
pub const antfly_sources = @import("source_owner_embedded_db.zig");
test {
    _ = @import("storage/db/db.zig");
    _ = @import("storage/db/prepared_graph_effects.zig");
    _ = @import("sql/trigger_native_admission_test.zig");
    _ = @import("sql/trigger_preparation.zig");
    _ = @import("sql/trigger_frame.zig");
    _ = @import("storage/db/mutation_coalescing.zig");
}
