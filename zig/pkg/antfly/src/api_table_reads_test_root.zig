const table_reads = @import("antfly_source_root").antfly_sources.table_reads;
const table_router = @import("api/table_router.zig");
const internal_query_operations = @import("api/internal_query_operations.zig");
const internal_group_operations = @import("api/internal_group_operations.zig");
const http_client = @import("api/http_client.zig");
const storage_db = @import("antfly_source_root").antfly_sources.selected_db;

test {
    _ = @import("api/retained_read_owner.zig");
    _ = table_reads;
    _ = table_router;
    _ = internal_query_operations;
    _ = internal_group_operations;
    _ = http_client;
    _ = storage_db;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_control.zig");

pub const consumer_tests_only = true;
