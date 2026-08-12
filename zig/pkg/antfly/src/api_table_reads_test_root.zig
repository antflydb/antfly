const table_reads = @import("api/table_reads.zig");
const table_router = @import("api/table_router.zig");
const internal_query_operations = @import("api/internal_query_operations.zig");
const internal_group_operations = @import("api/internal_group_operations.zig");
const storage_db = @import("storage/db/mod.zig");
const storage_lsm_backend = @import("storage/lsm_backend/mod.zig");

test {
    _ = table_reads;
    _ = table_router;
    _ = internal_query_operations;
    _ = internal_group_operations;
    _ = storage_db;
    _ = storage_lsm_backend;
}
