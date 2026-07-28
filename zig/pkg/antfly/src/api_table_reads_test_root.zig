const table_reads = @import("api/table_reads.zig");
const query = @import("api/query.zig");
const table_router = @import("api/table_router.zig");
const storage_db = @import("storage/db/mod.zig");
const storage_lsm_backend = @import("storage/lsm_backend/mod.zig");

test {
    _ = table_reads;
    _ = query;
    _ = table_router;
    _ = storage_db;
    _ = storage_lsm_backend;
}
