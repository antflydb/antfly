const table_reads = @import("api/table_reads.zig");
const table_router = @import("api/table_router.zig");
const http_internal_group_read_routes = @import("api/http_internal_group_read_routes.zig");
const storage_db = @import("storage/db/mod.zig");
const storage_lsm_backend = @import("storage/lsm_backend/mod.zig");

test {
    _ = table_reads;
    _ = table_router;
    _ = http_internal_group_read_routes;
    _ = storage_db;
    _ = storage_lsm_backend;
}
