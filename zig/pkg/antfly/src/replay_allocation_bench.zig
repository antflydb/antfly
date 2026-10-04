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

const Operation = enum { replay, enrichment, latest, ordinal, scalar_ordinal, scratch_trim };

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
    const requested_budgeted = args.len > 4 and std.mem.eql(u8, args[4], "budgeted");
    const documents_per_record = if (args.len > 5) try std.fmt.parseInt(usize, args[5], 10) else 1;
    const kind: @FieldType(indexes.ManagedIndexRef, "kind") = if (args.len > 6)
        std.meta.stringToEnum(@FieldType(indexes.ManagedIndexRef, "kind"), args[6]) orelse return error.InvalidIndexKind
    else
        .full_text;
    if (kind != .full_text and kind != .algebraic) return error.UnsupportedBenchmarkIndexKind;
    const primary = args.len > 7 and std.mem.eql(u8, args[7], "primary");
    const operation = if (args.len > 8) std.meta.stringToEnum(Operation, args[8]) orelse return error.InvalidOperation else .replay;
    const budgeted = operation == .replay and requested_budgeted;
    const repetitions = if (args.len > 9) try std.fmt.parseInt(usize, args[9], 10) else 1;
    if (repetitions == 0 or (operation == .replay and repetitions != 1)) return error.InvalidRepetitions;
    const index: indexes.ManagedIndexRef = .{ .name = "title_body", .kind = kind };
    if (batch == 0 or documents_per_record == 0) return error.InvalidBatch;
    const counting = args.len <= 10 or !std.mem.eql(u8, args[10], "timing");
    if (operation == .ordinal or operation == .scalar_ordinal) {
        if (!primary or kind != .full_text or documents_per_record != 1 or repetitions != 1 or requested_budgeted) return error.InvalidOrdinalParameters;
        return benchmarkOrdinals(count, batch, samples, counting, if (args.len > 11) args[11] else "short", operation == .scalar_ordinal);
    }
    if (operation == .scratch_trim) return benchmarkScratchTrim(count, samples, counting);
    const setup = std.heap.smp_allocator;
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
    var expected_checksum: usize = 0;
    var offset: usize = 0;
    var sequence: u64 = 0;
    while (offset < count) {
        const end = @min(count, offset + documents_per_record);
        const keys = try setup.alloc([]const u8, end - offset);
        defer setup.free(keys);
        var initialized: usize = 0;
        defer for (keys[0..initialized]) |key| setup.free(key);
        for (offset..end, keys) |i, *key| {
            key.* = try std.fmt.allocPrint(setup, "document-{d:0>10}", .{i / repetitions});
            initialized += 1;
            if (i % repetitions == 0) for (key.*) |byte| {
                expected_checksum +%= byte;
            };
        }
        sequence += 1;
        const encoded = try journal.encodeRecord(setup, .{ .sequence = sequence, .changed_doc_keys = keys, .target_hints = &.{if (operation == .enrichment) .enrichment else worker.targetHintForManagedIndex(index)} });
        defer setup.free(encoded);
        if (primary) try store.appendReplayOpaque(setup, sequence, encoded) else _ = try log.appendOpaque(encoded);
        offset = end;
    }
    for (0..samples + 1) |sample| {
        var counter: Counter = .{};
        const run_alloc = if (counting) counter.allocator() else std.heap.smp_allocator;
        var consumer: Consumer = .{};
        var manager = resources.ResourceManager.init(.{});
        defer manager.deinit(setup);
        const started = time.monotonicNs();
        var windows: usize = 0;
        switch (operation) {
            .ordinal, .scalar_ordinal, .scratch_trim => unreachable,
            .replay => {
                const stats = try worker.catchUpIndexWithOptions(run_alloc, replay_source, index, 0, &consumer, Consumer.apply, .{
                    .resource_manager = if (budgeted) &manager else null,
                    .max_records_per_window = batch,
                    .max_items_per_window = batch,
                });
                windows = stats.applied_entries;
            },
            .enrichment => {
                const groups = try replay_source.collectEnrichmentDocumentGroups(run_alloc, 0);
                defer source.freePendingDocumentGroups(run_alloc, groups);
                for (groups) |group| {
                    const id = try std.fmt.parseInt(usize, group.doc_key["document-".len..], 10);
                    const last_event = @min(count, (id + 1) * repetitions);
                    const unique_count = try std.math.divCeil(usize, count, repetitions);
                    const expected_sequence = try std.math.divCeil(usize, last_event, documents_per_record);
                    if (id >= unique_count or group.sequence != expected_sequence) return error.InvalidEnrichmentGroup;
                    consumer.count += 1;
                    for (group.doc_key) |byte| consumer.checksum +%= byte;
                }
            },
            .latest => {
                const latest = try replay_source.latestMatchingSequence(run_alloc, 0, worker.targetHintForManagedIndex(index));
                if (latest != sequence) return error.InvalidLatestSequence;
            },
        }
        const elapsed = time.monotonicNs() - started;
        if (manager.snapshot().memory.used_bytes != 0) return error.LeakedReservation;
        const expected_count = switch (operation) {
            .ordinal, .scalar_ordinal, .scratch_trim => unreachable,
            .replay => count,
            .enrichment => try std.math.divCeil(usize, count, repetitions),
            .latest => 0,
        };
        if (consumer.count != expected_count or consumer.checksum != (if (operation == .latest) @as(usize, 0) else expected_checksum) or counter.live != 0) return error.InvalidReplayOrLeakedMemory;
        if (sample != 0) std.debug.print("{{\"sample\":{d},\"documents\":{d},\"batch\":{d},\"budgeted\":{},\"documents_per_record\":{d},\"index_kind\":\"{s}\",\"source\":\"{s}\",\"operation\":\"{s}\",\"repetitions\":{d},\"measurement\":\"{s}\",\"elapsed_ns\":{d},\"allocations\":{d},\"resize_calls\":{d},\"remap_calls\":{d},\"moving_remaps\":{d},\"moved_bytes\":{d},\"allocated_bytes\":{d},\"peak_live_bytes\":{d},\"checksum\":{d},\"windows\":{d}}}\n", .{
            sample, count, batch, budgeted, documents_per_record, @tagName(kind), if (primary) "primary" else "journal", @tagName(operation), repetitions, if (counting) "counted" else "timing", elapsed, counter.calls, counter.resize_calls, counter.remap_calls, counter.moving_remaps, counter.moved_bytes, counter.bytes, counter.peak, consumer.checksum, windows,
        });
    }
}

// Measures the production identity lookup helper with a validating borrowed
// sorted-read callback. Backend I/O is excluded so key preparation is isolated.
fn benchmarkOrdinals(count: usize, batch: usize, samples: usize, counting: bool, fixture: []const u8, scalar: bool) !void {
    const identity = @import("storage/db/doc_identity.zig");
    const internal = @import("storage/internal_keys.zig");
    const long = std.mem.eql(u8, fixture, "long");
    const missing = std.mem.eql(u8, fixture, "missing");
    if (!long and !missing and !std.mem.eql(u8, fixture, "short")) return error.InvalidOrdinalFixture;
    const setup = std.heap.smp_allocator;
    const names = try setup.alloc([512]u8, count);
    defer setup.free(names);
    const ids = try setup.alloc([]const u8, count);
    defer setup.free(ids);
    const values = try setup.alloc([4]u8, count);
    defer setup.free(values);
    var expected_checksum: usize = 0;
    for (names, ids, values, 0..) |*name, *id, *value, i| {
        const number = count - 1 - i;
        const prefix = try std.fmt.bufPrint(name, "document-{d:0>10}", .{number});
        if (long) {
            @memset(name[prefix.len..], 'x');
            name[100] = 0;
        }
        id.* = if (long) name else prefix;
        std.mem.writeInt(u32, value, @intCast(i + 1), .big);
        if (!missing or number % 5 == 0) expected_checksum += number + 1;
    }
    const Txn = struct {
        values: []const [4]u8,
        missing: bool,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            var result: [1]?[]const u8 = .{null};
            try self.getManySorted(&.{key}, &result);
            return result[0] orelse error.NotFound;
        }
        pub fn getManySorted(self: *@This(), keys: []const []const u8, outputs: []?[]const u8) !void {
            for (keys, outputs, 0..) |key, *output, i| {
                if (i > 0 and std.mem.order(u8, keys[i - 1], key) == .gt) return error.UnsortedIdentityRead;
                if (key[0] != internal.identity_namespace or key[1] != internal.identity_doc_to_ordinal_kind or !std.mem.startsWith(u8, key[2..], "document-")) return error.InvalidIdentityKey;
                const number = try std.fmt.parseInt(usize, key[11..21], 10);
                if (number >= self.values.len) return error.InvalidIdentityKey;
                output.* = if (self.missing and number % 5 != 0) null else &self.values[number];
            }
        }
    };
    // Values are indexed by document number, independent of input order.
    for (values, 0..) |*value, number| std.mem.writeInt(u32, value, @intCast(number + 1), .big);
    var txn: Txn = .{ .values = values, .missing = missing };
    for (0..samples + 1) |sample| {
        var counter: Counter = .{};
        const alloc = if (counting) counter.allocator() else std.heap.smp_allocator;
        var checksum: usize = 0;
        const started = time.monotonicNs();
        var offset: usize = 0;
        while (offset < count) {
            const end = @min(count, offset + batch);
            if (scalar) {
                for (ids[offset..end], offset..) |id, i| {
                    const ordinal = try identity.lookupOrdinalTxn(alloc, &txn, id);
                    const number = count - 1 - i;
                    const expected: ?u32 = if (missing and number % 5 != 0) null else @intCast(number + 1);
                    if (ordinal != expected) return error.InvalidOrdinalResult;
                    checksum += ordinal orelse 0;
                }
            } else {
                const ordinals = try identity.lookupOrdinalsTxnAlloc(alloc, &txn, ids[offset..end]);
                defer alloc.free(ordinals);
                for (ordinals, offset..) |ordinal, i| {
                    const number = count - 1 - i;
                    const expected: ?u32 = if (missing and number % 5 != 0) null else @intCast(number + 1);
                    if (ordinal != expected) return error.InvalidOrdinalResult;
                    checksum += ordinal orelse 0;
                }
            }
            offset = end;
        }
        const elapsed = time.monotonicNs() - started;
        if (counter.live != 0 or checksum != expected_checksum) return error.InvalidOrdinalsOrLeakedMemory;
        if (sample != 0) std.debug.print("{{\"sample\":{d},\"documents\":{d},\"batch\":{d},\"budgeted\":false,\"documents_per_record\":1,\"index_kind\":\"full_text\",\"source\":\"primary\",\"operation\":\"{s}\",\"repetitions\":1,\"case\":\"{s}\",\"measurement\":\"{s}\",\"elapsed_ns\":{d},\"allocations\":{d},\"resize_calls\":{d},\"remap_calls\":{d},\"moving_remaps\":{d},\"moved_bytes\":{d},\"allocated_bytes\":{d},\"peak_live_bytes\":{d},\"checksum\":{d}}}\n", .{ sample, count, batch, if (scalar) "scalar_ordinal" else "ordinal", fixture, if (counting) "counted" else "timing", elapsed, counter.calls, counter.resize_calls, counter.remap_calls, counter.moving_remaps, counter.moved_bytes, counter.bytes, counter.peak, checksum });
    }
}

// Alternates exceptional records with ordinary records. Encoding and input
// ownership are outside the measured region; returned borrowed fields are
// validated before trimming invalidates them.
fn benchmarkScratchTrim(count: usize, samples: usize, counting: bool) !void {
    const setup = std.heap.smp_allocator;
    const large_keys = try setup.alloc([]const u8, 8192);
    defer setup.free(large_keys);
    @memset(large_keys, "document");
    const large = try journal.encodeRecord(setup, .{ .sequence = 1, .changed_doc_keys = large_keys, .deleted_doc_keys = &.{"deleted"}, .target_hints = &.{.full_text} });
    defer setup.free(large);
    const small = try journal.encodeRecord(setup, .{ .sequence = 2, .changed_doc_keys = large_keys[0..64], .deleted_doc_keys = &.{"deleted"}, .target_hints = &.{.full_text} });
    defer setup.free(small);
    const expected = ((count + 15) / 16) * (8192 + 2) + (count - (count + 15) / 16) * (64 + 2);
    for (0..samples + 1) |sample| {
        var counter: Counter = .{};
        const alloc = if (counting) counter.allocator() else std.heap.smp_allocator;
        var scratch: journal.BorrowedBinaryRecordScratch = .{};
        var checksum: usize = 0;
        const started = time.monotonicNs();
        for (0..count) |i| {
            const record = try journal.decodeBinaryRecordBorrowedScratch(alloc, if (i % 16 == 0) large else small, &scratch);
            if (record.changed_doc_keys.len != (if (i % 16 == 0) @as(usize, 8192) else 64) or
                record.deleted_doc_keys.len != 1 or record.target_hints.len != 1 or
                !std.mem.eql(u8, record.changed_doc_keys[0], "document") or
                !std.mem.eql(u8, record.deleted_doc_keys[0], "deleted") or record.target_hints[0] != .full_text)
                return error.InvalidScratchRecord;
            checksum += record.changed_doc_keys.len + record.deleted_doc_keys.len + record.target_hints.len;
            scratch.trimRetainedCapacity(alloc, 64 * 1024);
            if (scratch.retainedCapacityBytes() > 64 * 1024) return error.UnboundedScratch;
        }
        scratch.deinit(alloc);
        const elapsed = time.monotonicNs() - started;
        if (checksum != expected or counter.live != 0) return error.InvalidScratchResult;
        if (sample != 0) std.debug.print("{{\"sample\":{d},\"operation\":\"scratch_trim\",\"measurement\":\"{s}\",\"elapsed_ns\":{d},\"allocations\":{d},\"resize_calls\":{d},\"remap_calls\":{d},\"moving_remaps\":{d},\"moved_bytes\":{d},\"allocated_bytes\":{d},\"peak_live_bytes\":{d},\"checksum\":{d}}}\n", .{ sample, if (counting) "counted" else "timing", elapsed, counter.calls, counter.resize_calls, counter.remap_calls, counter.moving_remaps, counter.moved_bytes, counter.bytes, counter.peak, checksum });
    }
}
