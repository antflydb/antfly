const indexes = @import("api/indexes.zig");
const managed_embedder = @import("antfly_local_sources").inference_managed_embedder;
const coverage_policy = @import("antfly_local_sources").api_coverage_policy;
const runtime_status = @import("antfly_local_sources").api_runtime_status;
const http_server = @import("api/http_server.zig");
const backups = @import("api/backups.zig");

test {
    _ = indexes;
    _ = managed_embedder;
    _ = coverage_policy;
    _ = runtime_status;
    _ = http_server;
    _ = backups;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
