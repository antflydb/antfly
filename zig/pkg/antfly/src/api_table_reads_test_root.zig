const table_reads = @import("api/table_reads.zig");
const http_internal_group_read_routes = @import("api/http_internal_group_read_routes.zig");
const storage_db = @import("storage/db/mod.zig");
const storage_lsm_backend = @import("storage/lsm_backend/mod.zig");

test {
    _ = table_reads;
    _ = http_internal_group_read_routes;
    _ = storage_db;
    _ = storage_lsm_backend;
}
