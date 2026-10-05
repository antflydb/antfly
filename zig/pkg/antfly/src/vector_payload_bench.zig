//! Source-store benchmark on real files. References are a harness sidecar;
//! only Session preparation, resolution and Store checkpointing are timed.
const std = @import("std");
const source = @import("antfly_local_sources").storage_vector_payload_store;
const payload = @import("antfly_local_sources").storage_artifact_payload;
const codec = @import("antfly_local_sources").storage_db_enrichment_artifact_codec;
const keys = @import("antfly_local_sources").storage_internal_keys;
const lsm = @import("antfly_local_sources").storage_lsm_backend_mod;
const time = @import("antfly_platform").time;

pub fn main(init: std.process.Init) !void {
    var allocation_counter: @import("antfly_local_sources").allocation_bench_support.Counter = .{};
    const count_allocations = @import("antfly_platform").env.getenvBool("ANTFLY_COUNT_BENCH_ALLOCATIONS");
    const alloc = if (count_allocations) allocation_counter.allocator() else std.heap.smp_allocator;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 2) return error.ExpectedHashOrIngestOrRead;
    if (std.mem.eql(u8, args[1], "hash")) return hashBench();
    if (args.len != 5) return error.ExpectedModeRootCountDims;
    const root = args[2];
    const count = try std.fmt.parseInt(usize, args[3], 10);
    const dims = try std.fmt.parseInt(usize, args[4], 10);
    const session_only = std.mem.eql(u8, args[1], "session-only");
    const retrying = std.mem.eql(u8, args[1], "retry");
    const mixed = std.mem.eql(u8, args[1], "retry-mixed") or std.mem.eql(u8, args[1], "read-mixed");
    const updating = std.mem.eql(u8, args[1], "update") or std.mem.eql(u8, args[1], "read-updated");
    var disk = try lsm.NativeStorage.init(alloc, .threaded);
    defer disk.deinit();
    const storage = disk.storage();
    const refs_path = try std.fmt.allocPrint(alloc, "{s}{s}.refs", .{ root, if (updating) ".updated" else if (mixed) ".mixed" else "" });
    defer alloc.free(refs_path);
    const writing = session_only or std.mem.eql(u8, args[1], "ingest") or std.mem.eql(u8, args[1], "update") or retrying or std.mem.eql(u8, args[1], "retry-mixed");
    if (!writing and !std.mem.eql(u8, args[1], "read") and !std.mem.eql(u8, args[1], "read-updated") and !std.mem.eql(u8, args[1], "read-mixed")) return error.InvalidMode;
    if (writing) {
        const current = try std.fmt.allocPrint(alloc, "{s}/CURRENT", .{root});
        defer alloc.free(current);
        if (storage.fileSize(current)) |_| {
            if (!updating and !retrying and !mixed) return error.BenchmarkRequiresFreshRoot;
        } else |err| {
            if (err != error.FileNotFound or updating or retrying or mixed) return err;
        }
    }
    const started = time.monotonicNs();
    var budgets = @import("antfly_local_sources").storage_resource_manager.Options.defaultBudgets();
    budgets[@intFromEnum(@import("antfly_local_sources").storage_resource_manager.Slice.dense_source_payload_state)] = .{ .soft_limit_bytes = 320 * 1024 * 1024, .hard_limit_bytes = 384 * 1024 * 1024 };
    var manager = @import("antfly_local_sources").storage_resource_manager.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(alloc);
    var store = try source.Store.openManaged(alloc, &manager, storage, root, !writing);
    defer store.deinit();
    const opened_ns = time.monotonicNs() - started;
    const refs = if (writing) try alloc.alloc(u8, count * payload.reference_len) else try storage.readFileAlloc(alloc, refs_path, count * payload.reference_len + 1);
    defer alloc.free(refs);
    if (refs.len != count * payload.reference_len) return error.InvalidReferenceSidecar;
    const vector = try alloc.alloc(f32, dims);
    defer alloc.free(vector);
    for (vector, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i % 97)) / 97;
    var max_batch_ns: u64 = 0;
    var maintenance = Maintenance{ .store = &store, .io = init.io };
    const background = writing and !session_only and std.c.getenv("ANTFLY_SOURCE_VECTOR_BACKGROUND_CHECKPOINT") != null;
    // The diagnostic counter is deliberately single-threaded so it does not
    // insert locks into every allocation. Keep its use out of concurrent runs.
    if (count_allocations and background) return error.AllocationCountingRequiresForegroundCheckpoint;
    var task = if (background) try init.io.concurrent(Maintenance.run, .{&maintenance}) else null;
    defer if (task) |*future| {
        maintenance.stop.store(true, .release);
        future.await(init.io) catch {};
    };
    const initial_live = allocation_counter.live;
    allocation_counter.resetActivity();
    const run_start = time.monotonicNs();
    var offset: usize = 0;
    while (offset < count) {
        const end = @min(count, offset + 128);
        const session = try payload.Session.create(alloc, store.interface());
        defer session.release();
        const batch_start = time.monotonicNs();
        for (offset..end) |i| {
            var id: [32]u8 = undefined;
            const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, try std.fmt.bufPrint(&id, "doc-{d:0>10}", .{i}), "model-a");
            defer alloc.free(key);
            const ref = refs[i * payload.reference_len ..][0..payload.reference_len];
            const changed = updating or (mixed and i % 128 == 0);
            if (writing) {
                vector[0] = @floatFromInt(i % 10007 + @intFromBool(changed));
                const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, if (changed) 18 else 17, vector);
                defer alloc.free(artifact);
                @memcpy(ref, try session.put(key, artifact));
            } else {
                const artifact = try session.getAlloc(alloc, key, ref);
                defer alloc.free(artifact);
                const values = (try codec.denseEmbeddingVectorView(artifact)) orelse return error.ExpectedFloat32;
                if (values.len != dims or values[0] != @as(f32, @floatFromInt(i % 10007 + @intFromBool(changed)))) return error.PayloadMismatch;
                std.mem.doNotOptimizeAway(artifact.ptr);
            }
        }
        if (writing and !session_only) {
            try session.prepareCommit();
            session.committed = true;
        }
        max_batch_ns = @max(max_batch_ns, time.monotonicNs() - batch_start);
        offset = end;
    }
    const run_ns = time.monotonicNs() - run_start;
    if (task) |*future| {
        maintenance.stop.store(true, .release);
        try future.await(init.io);
        task = null;
    }
    const finish_start = time.monotonicNs();
    if (writing and !session_only) try store.checkpoint();
    const finish_ns = time.monotonicNs() - finish_start;
    if (writing and !session_only) try storage.writeFileAbsolute(refs_path, refs);
    if (session_only and store.statsSnapshot().prepared_payloads != 0) return error.UnexpectedDurablePreparation;
    const reference_checksum = std.hash.Wyhash.hash(0, refs);
    // Reporting is outside the measured work and must not inflate heap metrics.
    const measured_allocations = allocation_counter;
    const stats = try std.json.Stringify.valueAlloc(alloc, store.statsSnapshot(), .{});
    defer alloc.free(stats);
    std.debug.print("payload_bench {{\"mode\":\"{s}\",\"count\":{d},\"dims\":{d},\"open_ns\":{d},\"run_ns\":{d},\"final_checkpoint_ns\":{d},\"max_batch_ns\":{d},\"reference_checksum\":{d},\"stats\":{s}}}\n", .{ args[1], count, dims, opened_ns, run_ns, finish_ns, max_batch_ns, reference_checksum, stats });
    if (count_allocations) std.debug.print("allocation_bench {{\"allocations\":{d},\"resize_calls\":{d},\"remap_calls\":{d},\"moving_remaps\":{d},\"moved_bytes\":{d},\"allocated_bytes\":{d},\"peak_additional_live_bytes\":{d}}}\n", .{
        measured_allocations.calls, measured_allocations.resize_calls, measured_allocations.remap_calls, measured_allocations.moving_remaps, measured_allocations.moved_bytes, measured_allocations.bytes, measured_allocations.peak - initial_live,
    });
}

fn hashBench() !void {
    var data: [12353]u8 = undefined;
    for (&data, 0..) |*v, i| v.* = @truncate(i *% 137 +% 17);
    for ([_]usize{ 128, 3072, 6144, 12288 }) |len| for (0..4) |round| for (0..2) |order| {
        const portable = (order + round) % 2 == 0;
        var result: [32]u8 = undefined;
        const start = time.monotonicNs();
        for (0..20000) |_| {
            if (portable) @import("antfly_hash").RuntimeSha256.hash(data[1..][0..len], &result, .{ .portable = true }) else @import("antfly_hash").RuntimeSha256.hash(data[1..][0..len], &result, .{});
            std.mem.doNotOptimizeAway(&result);
        }
        std.debug.print("hash_bench {{\"bytes\":{d},\"round\":{d},\"portable\":{},\"iterations\":20000,\"ns\":{d}}}\n", .{ len, round, portable, time.monotonicNs() - start });
    };
}

const Maintenance = struct {
    store: *source.Store,
    io: std.Io,
    stop: std.atomic.Value(bool) = .init(false),
    fn run(self: *@This()) !void {
        while (!self.stop.load(.acquire)) {
            if (comptime @hasDecl(source.Store, "checkpointMaintenance")) try self.store.checkpointMaintenance();
            try self.io.sleep(.fromMilliseconds(5), .awake);
        }
    }
};

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
