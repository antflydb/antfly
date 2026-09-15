//! Real-file, checksum-validated fetch batch replay; never mutates the store.
const std = @import("std");
const builtin = @import("builtin");
const native = @import("vector_block_store.zig");
const lsm = @import("lsm_backend/mod.zig");
const resources = @import("resource_manager.zig");
const time = @import("antfly_platform").time;
extern "c" fn getenv([*:0]const u8) ?[*:0]const u8;
const Row = struct { generation: u64, shard: u32, offset: usize, length: usize, checksum: u32 };
const Trace = struct { root: []const u8, batches: [][]Row };
const Batch = struct { locations: []native.LocatedValue };
fn cpuNs() u64 {
    var usage: std.c.rusage = undefined;
    std.debug.assert(std.c.getrusage(std.c.rusage.SELF, &usage) == 0);
    return @as(u64, @intCast(usage.utime.sec + usage.stime.sec)) * 1_000_000_000 + @as(u64, @intCast(usage.utime.usec + usage.stime.usec)) * 1000;
}
const Worker = struct {
    opened: *native.Opened,
    io: std.Io,
    batches: []const Batch,
    split: bool,
    repeats: usize,
    stats: native.ReadDispatchStats = .{},
    reads: u64 = 0,
    bytes: u64 = 0,
    checksum: u64 = 0,
    err: ?anyerror = null,
    requests: [256]native.ExactReadRequest = undefined,
    payload: [256 * 1536 * 4]u8 = undefined,
    fn add(self: *@This(), stats: native.ReadBatchStats) void {
        inline for (std.meta.fields(native.ReadDispatchStats)) |f| @field(self.stats, f.name) += @field(stats.dispatch, f.name);
        self.reads += stats.physical_reads;
        self.bytes += stats.physical_bytes;
    }
    fn run(self: *@This()) std.Io.Cancelable!void {
        self.runChecked() catch |err| {
            self.err = err;
        };
    }
    fn runChecked(self: *@This()) !void {
        for (0..self.repeats) |_| for (self.batches) |batch| {
            const manager = self.opened.resource_manager.?;
            var driver = if (manager.dense_aggregate_admission) try manager.dense_driver_admission.acquire(self.io, null) else resources.DenseWorkAdmission.Queue.Lease{};
            defer driver.release();
            for (batch.locations, 0..) |location, i| self.requests[i] = .{ .located = location, .scratch = self.payload[i * 6144 ..][0..6144], .result_position = i };
            // Controlled one-miss batch, same locations/order; only barrier differs.
            const split_at = if (self.split and batch.locations.len > 1) batch.locations.len - 1 else batch.locations.len;
            self.add(try self.opened.readExactIntoBatch(self.io, self.requests[0..split_at]));
            if (split_at < batch.locations.len) self.add(try self.opened.readExactIntoBatch(self.io, self.requests[split_at..batch.locations.len]));
            for (self.requests[0..batch.locations.len]) |request| {
                if (request.err) |err| return err;
                const value = request.value orelse return error.MissingValue;
                std.mem.doNotOptimizeAway(value);
                // Consume payload without adding a second checksum scan to timing.
                self.checksum +%= value.bytes[0];
            }
        };
    }
};
test "vector block complete fetch batch replay benchmark" {
    const path = getenv("ANTFLY_VECTOR_FETCH_REPLAY") orelse return error.SkipZigTest;
    const pipeline = getenv("ANTFLY_VECTOR_FETCH_PIPELINE_REPLAY") != null;
    const alloc = std.heap.page_allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var disk = try lsm.NativeStorage.init(alloc, .threaded);
    defer disk.deinit();
    const json = try disk.storage().readFileAlloc(a, std.mem.span(path), 8 * 1024 * 1024);
    const parsed = try std.json.parseFromSlice(Trace, a, json, .{});
    const trace = parsed.value;
    var opened = try native.Store.openReadOnlyWithBlocks(alloc, disk.storage(), trace.root);
    defer opened.deinit();
    var manager = resources.ResourceManager.init(.{ .dense_read_extra_task_limit = if (pipeline) 28 else 16 });
    defer manager.deinit(alloc);
    opened.resource_manager = &manager;
    const batches = try a.alloc(Batch, trace.batches.len);
    // Untimed matching of authenticated trace locations to admitted metadata.
    var locations: std.AutoHashMap(struct { u64, u32, usize }, native.LocatedValue) = .init(alloc);
    defer locations.deinit();
    for (opened.readers, 0..) |reader, ri| for (0..reader.count) |row| {
        const found = try reader.locationAt(row);
        if (found == .vector) try locations.put(.{ reader.generation, reader.shard_id, found.vector.vector_offset }, .{ .block = .{ .reader_index = ri, .reader_generation = reader.generation, .reader_shard_id = reader.shard_id, .location = found.vector } });
    };
    for (trace.batches, batches) |rows, *batch| {
        try std.testing.expect(rows.len <= 256);
        batch.locations = try a.alloc(native.LocatedValue, rows.len);
        for (rows, batch.locations) |row, *location| {
            location.* = locations.get(.{ row.generation, row.shard, row.offset }) orelse return error.TraceGenerationMissing;
            try std.testing.expectEqual(row.length, location.block.location.vector_len);
            try std.testing.expectEqual(row.checksum, location.block.location.vector_checksum);
        }
    }
    locations.clearAndFree();
    var runtime = std.Io.Threaded.init(alloc, .{ .concurrent_limit = .limited(64) });
    defer runtime.deinit();
    const workers = try a.alloc(Worker, 10);
    workers[0] = .{ .opened = &opened, .io = runtime.io(), .batches = batches, .split = false, .repeats = 1 };
    try workers[0].runChecked();
    const expected_checksum = workers[0].checksum;
    const expected_reads = workers[0].reads;
    const expected_bytes = workers[0].bytes;
    for ([_]bool{ false, true }) |bypass| {
        if (pipeline and bypass) continue;
        if (bypass) {
            if (builtin.os.tag != .macos) continue;
            // Descriptor-local F_NOCACHE. Device caches may persist.
            for (opened.blocks) |b| switch (b.shared.payload) {
                .mapped => |m| if (std.c.fcntl(m.fd, std.c.F.NOCACHE, @as(c_int, 1)) != 0) {
                    return error.CacheBypassFailed;
                },
                .heap => return error.ExpectedMappedFile,
            };
        }
        const concurrencies: []const usize = if (pipeline) &.{ 1, 2, 4, 10 } else &.{ 1, 10 };
        const modes: usize = if (pipeline) 12 else 6;
        for (concurrencies) |concurrency| for (0..4) |round| for (0..modes) |arm| {
            const mode = if (round % 2 == 0) arm else modes - 1 - arm;
            const inline_reads = mode % 3 == 1;
            const single_helper = mode % 3 == 2;
            const split = !pipeline and mode >= 3;
            manager.dense_exact_mapped = pipeline and mode % 6 >= 3;
            manager.dense_aggregate_admission = pipeline and mode >= 6;
            if (pipeline) {
                workers[0] = .{ .opened = &opened, .io = runtime.io(), .batches = batches, .split = false, .repeats = 1 };
                try workers[0].runChecked(); // Untimed policy-specific warmup.
            }
            manager.dense_read_inline = inline_reads;
            manager.dense_read_single_helper = single_helper;
            // Final round is attribution only, excluded from clean medians.
            manager.dense_read_profile = round == 3;
            const repeats: usize = if (bypass) 1 else 8;
            for (workers[0..concurrency]) |*w| w.* = .{ .opened = &opened, .io = runtime.io(), .batches = batches, .split = split, .repeats = repeats };
            const cpu_start = cpuNs();
            const start = time.monotonicNs();
            var group = std.Io.Group.init;
            defer group.cancel(runtime.io());
            for (workers[1..concurrency]) |*w| try group.concurrent(runtime.io(), Worker.run, .{w});
            try workers[0].run();
            try group.await(runtime.io());
            const elapsed = time.monotonicNs() - start;
            const cpu_elapsed = cpuNs() - cpu_start;
            var total: native.ReadDispatchStats = .{};
            for (workers[0..concurrency]) |w| {
                if (w.err) |err| return err;
                try std.testing.expectEqual(expected_checksum *% repeats, w.checksum);
                try std.testing.expectEqual(if (manager.dense_exact_mapped) 0 else expected_reads * repeats, w.reads);
                try std.testing.expectEqual(if (manager.dense_exact_mapped) 0 else expected_bytes * repeats, w.bytes);
                inline for (std.meta.fields(native.ReadDispatchStats)) |f| @field(total, f.name) += @field(w.stats, f.name);
            }
            try std.testing.expectEqual(@as(u32, 0), manager.denseReadTaskStats().active);
            std.debug.print("fetch_replay {f}\n", .{std.json.fmt(.{ .pipeline = pipeline, .mapped = manager.dense_exact_mapped, .aggregate = manager.dense_aggregate_admission, .bypass = bypass, .concurrency = concurrency, .round = round, .inline_reads = inline_reads, .single_helper = single_helper, .split = split, .profiled = manager.dense_read_profile, .logical_batches = batches.len * repeats * concurrency, .reads = if (manager.dense_exact_mapped) 0 else expected_reads * repeats * concurrency, .bytes = if (manager.dense_exact_mapped) 0 else expected_bytes * repeats * concurrency, .elapsed_ns = elapsed, .cpu_ns = cpu_elapsed, .dispatch = total }, .{})});
        };
    }
}
