const transition_runtime = @import("raft/transition_runtime.zig");

test {
    _ = transition_runtime;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");
