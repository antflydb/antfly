test {
    _ = @import("antfly_local_sources").storage_artifact_payload;
    _ = @import("antfly_local_sources").common_table_storage;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
