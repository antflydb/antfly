//! Default-off attribution only. Logging changes latency; never use traced
//! runs as performance qualifications. No vector payload or document ID logged.
const std = @import("std");
var next_batch: std.atomic.Value(u64) = .init(0);
const max_batches = 128;

pub fn record(opened: anytype, requests: anytype) void {
    var begin: usize = 0;
    while (begin < requests.len) {
        const id = next_batch.fetchAdd(1, .monotonic);
        if (id >= max_batches) return;
        const end = @min(requests.len, begin + 256);
        std.log.info("antfly_projection_trace {f}", .{std.json.fmt(.{ .batch = id, .count = end - begin }, .{})});
        for (requests[begin..end], 0..) |request, slot| switch (request.located) {
            .wal => std.log.info("antfly_projection_trace {f}", .{std.json.fmt(.{ .batch = id, .slot = slot, .wal = true }, .{})}),
            .block => |block| {
                const owner = block.owner orelse opened;
                std.log.info("antfly_projection_trace {f}", .{std.json.fmt(.{
                    .batch = id,
                    .slot = slot,
                    .root = owner.store.root_dir,
                    .generation = block.reader_generation,
                    .shard = block.reader_shard_id,
                    .offset = block.location.vector_offset,
                    .length = block.location.vector_len,
                    .checksum = block.location.vector_checksum,
                }, .{})});
            },
        };
        begin = end;
    }
}

test "projection read trace is bounded and handles source-owned locations" {
    const Opened = struct { store: struct { root_dir: []const u8 } };
    const Block = struct {
        owner: ?*const Opened = null,
        reader_generation: u64 = 1,
        reader_shard_id: u32 = 0,
        location: struct { vector_offset: usize = 40, vector_len: usize = 4, vector_checksum: u32 = 1 } = .{},
    };
    const Request = struct { located: union(enum) { wal, block: Block } };
    const opened: Opened = .{ .store = .{ .root_dir = "diagnostic root" } };
    next_batch.store(0, .monotonic);
    record(&opened, &[_]Request{ .{ .located = .{ .block = .{} } }, .{ .located = .wal } });
    try std.testing.expectEqual(@as(u64, 1), next_batch.load(.monotonic));
    next_batch.store(max_batches, .monotonic);
    record(&opened, &[_]Request{.{ .located = .{ .block = .{ .owner = &opened } } }});
    try std.testing.expectEqual(@as(u64, max_batches + 1), next_batch.load(.monotonic));
}
