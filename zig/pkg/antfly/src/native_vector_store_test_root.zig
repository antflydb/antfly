test {
    _ = @import("storage/vector_fetch_batches_bench.zig");
    _ = @import("storage/vector_member_bindings_bench.zig");
    _ = @import("antfly_local_sources").storage_vector_block_store;
    _ = @import("antfly_local_sources").storage_vector_member_bindings;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
