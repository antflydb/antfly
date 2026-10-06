test {
    _ = @import("antfly_local_sources").storage_vector_payload_store;
    _ = @import("antfly_local_sources").storage_lsm_backend;
    _ = @import("antfly_local_sources").storage_lsm_backend_storage_io;
    _ = @import("antfly_local_sources").storage_resource_manager;
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
