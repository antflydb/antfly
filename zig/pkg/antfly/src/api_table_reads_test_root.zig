const table_reads = @import("api/table_reads.zig");
const table_reads_core = @import("api/table_reads/core.zig");
const table_reads_cache = @import("api/table_reads/cache.zig");
const table_reads_document_sql = @import("api/table_reads/document_sql.zig");
const table_reads_relational_rows = @import("api/table_reads/relational_rows.zig");
const table_reads_external_lake = @import("api/table_reads/external_lake.zig");
const table_reads_remote_wire = @import("api/table_reads/remote_wire.zig");
const table_reads_fanout = @import("api/table_reads/fanout.zig");
const table_reads_graph = @import("api/table_reads/graph.zig");
const table_reads_sources = @import("api/table_reads/sources.zig");
const table_router = @import("api/table_router.zig");
const http_internal_group_read_routes = @import("api/http_internal_group_read_routes.zig");
const storage_db = @import("storage/db/mod.zig");
const storage_lsm_backend = @import("storage/lsm_backend/mod.zig");

test {
    _ = table_reads;
    _ = table_reads_core;
    _ = table_reads_cache;
    _ = table_reads_document_sql;
    _ = table_reads_relational_rows;
    _ = table_reads_external_lake;
    _ = table_reads_remote_wire;
    _ = table_reads_fanout;
    _ = table_reads_graph;
    _ = table_reads_sources;
    _ = table_router;
    _ = http_internal_group_read_routes;
    _ = storage_db;
    _ = storage_lsm_backend;
}
