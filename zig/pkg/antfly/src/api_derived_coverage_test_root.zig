const indexes = @import("api/indexes.zig");
const coverage_policy = @import("api/coverage_policy.zig");
const runtime_status = @import("api/runtime_status.zig");
const http_server = @import("api/http_server.zig");
const backups = @import("api/backups.zig");

test {
    _ = indexes;
    _ = coverage_policy;
    _ = runtime_status;
    _ = http_server;
    _ = backups;
}
