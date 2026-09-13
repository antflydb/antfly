test {
    _ = @import("raft/vopr_harness.zig");
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");
