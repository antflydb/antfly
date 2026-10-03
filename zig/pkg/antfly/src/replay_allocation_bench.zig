//! Measures replay construction and decoding with a synchronous validating
//! consumer. Memory journal/primary-store setup is outside the timed/counting
//! region; select the source with positional argument 7 (journal or primary). Indexing,
//! OpenAI calls and document-body decoding are deliberately excluded.
const std = @import("std");
const worker = @import("storage/db/derived/derived_worker.zig");
const journal = @import("storage/db/derived/change_journal.zig");
const source = @import("storage/db/derived/replay_source.zig");
const mem_backend = @import("storage/mem_backend.zig");
const docstore = @import("storage/docstore.zig");
const types = @import("storage/db/derived/derived_types.zig");
const indexes = @import("storage/db/catalog/index_manager.zig");
const resources = @import("storage/resource_manager.zig");
const time = @import("antfly_platform").time;

const Counter = @import("allocation_bench_support.zig").Counter;

const Consumer = struct {
    count: usize = 0,
    checksum: usize = 0,
    fn apply(ctx: *anyopaque, batch: types.DerivedBatch, index: indexes.ManagedIndexRef) !bool {
        const self: *Consumer = @ptrCast(@alignCast(ctx));
        for (batch.documents) |doc| {
            if (doc.targets.len != 1 or !std.mem.eql(u8, doc.targets[0].index_name, index.name)) return error.InvalidTarget;
            self.count += 1;
            for (doc.key) |byte| self.checksum +%= byte;
        }
        return batch.documents.len != 0;
    }
};
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const count = if (args.len > 1) try std.fmt.parseInt(usize, args[1], 10) else 50_000;
    const batch = if (args.len > 2) try std.fmt.parseInt(usize, args[2], 10) else 256;
    const samples = if (args.len > 3) try std.fmt.parseInt(usize, args[3], 10) else 7;
    const budgeted = args.len > 4 and std.mem.eql(u8, args[4], "budgeted");
    const documents_per_record = if (args.len > 5) try std.fmt.parseInt(usize, args[5], 10) else 1;
    const kind: @FieldType(indexes.ManagedIndexRef, "kind") = if (args.len > 6)
        std.meta.stringToEnum(@FieldType(indexes.ManagedIndexRef, "kind"), args[6]) orelse return error.InvalidIndexKind
    else
        .full_text;
    if (kind != .full_text and kind != .algebraic) return error.UnsupportedBenchmarkIndexKind;
    const primary = args.len > 7 and std.mem.eql(u8, args[7], "primary");
    const index: indexes.ManagedIndexRef = .{ .name = "title_body", .kind = kind };
    if (batch == 0 or documents_per_record == 0) return error.InvalidBatch;
    const setup = std.heap.c_allocator;
    var log = try journal.Journal.open("allocation-benchmark-memory", .{
        .backend = .lsm_memory,
        .lsm_options = .{ .flush_threshold = 512, .compact_threshold_runs = 256, .wal_enabled = false, .obsolete_retention_ns = 0 },
    });
    defer log.close();
    var backend = mem_backend.Backend.init(setup, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(setup, .{});
    var store = try docstore.DocStore.openRuntime(setup, runtime_store);
    defer store.close();
    const replay_source = if (primary) source.Source.fromPrimaryStore(&store, null, null) else source.Source.fromJournal(&log);
    var offset: usize = 0;
    var sequence: u64 = 0;
    while (offset < count) {
        const end = @min(count, offset + documents_per_record);
        const keys = try setup.alloc([]const u8, end - offset);
        defer setup.free(keys);
        var initialized: usize = 0;
        defer for (keys[0..initialized]) |key| setup.free(key);
        for (offset..end, keys) |i, *key| {
            key.* = try std.fmt.allocPrint(setup, "document-{d:0>10}", .{i});
            initialized += 1;
        }
        sequence += 1;
        const encoded = try journal.encodeRecord(setup, .{ .sequence = sequence, .changed_doc_keys = keys, .target_hints = &.{worker.targetHintForManagedIndex(index)} });
        defer setup.free(encoded);
        if (primary) try store.appendReplayOpaque(setup, sequence, encoded) else _ = try log.appendOpaque(encoded);
        offset = end;
    }
    for (0..samples + 1) |sample| {
        var counter: Counter = .{};
        var consumer: Consumer = .{};
        var manager = resources.ResourceManager.init(.{});
        defer manager.deinit(setup);
        const started = time.monotonicNs();
        const stats = try worker.catchUpIndexWithOptions(counter.allocator(), replay_source, index, 0, &consumer, Consumer.apply, .{
            .resource_manager = if (budgeted) &manager else null,
            .max_records_per_window = batch,
            .max_items_per_window = batch,
        });
        const elapsed = time.monotonicNs() - started;
        if (manager.snapshot().memory.used_bytes != 0) return error.LeakedReservation;
        if (consumer.count != count or counter.live != 0) return error.InvalidReplayOrLeakedMemory;
        if (sample != 0) std.debug.print("{{\"sample\":{d},\"documents\":{d},\"batch\":{d},\"budgeted\":{},\"documents_per_record\":{d},\"index_kind\":\"{s}\",\"source\":\"{s}\",\"elapsed_ns\":{d},\"allocations\":{d},\"allocated_bytes\":{d},\"peak_live_bytes\":{d},\"checksum\":{d},\"windows\":{d}}}\n", .{
            sample, count, batch, budgeted, documents_per_record, @tagName(kind), if (primary) "primary" else "journal", elapsed, counter.calls, counter.bytes, counter.peak, consumer.checksum, stats.applied_entries,
        });
    }
}
