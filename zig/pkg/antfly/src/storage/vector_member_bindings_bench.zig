//! Metadata-only microbenchmark. Full service qualification owns payload I/O
//! and scoring measurements. Run with ANTFLY_VECTOR_MEMBER_MICROBENCH=1.
const std = @import("std");
const native = @import("vector_block_store.zig");
const block = @import("antfly_vectorindex").vector_block;
const lsm = @import("lsm_backend/mod.zig");
const time = @import("antfly_platform").time;
extern "c" fn getenv([*:0]const u8) ?[*:0]const u8;

test "vector block member binding microbenchmark" {
    if (getenv("ANTFLY_VECTOR_MEMBER_MICROBENCH") == null) return error.SkipZigTest;
    for ([_]usize{ 50_000, 1_000_000 }) |count| try run(count);
}

fn run(count: usize) !void {
    const alloc = std.heap.page_allocator;
    var memory = lsm.MemoryStorage.init(alloc);
    defer memory.deinit();
    var source_store = try native.Store.open(alloc, memory.storage(), "/micro-source");
    defer source_store.deinit();
    var ann_store = try native.Store.open(alloc, memory.storage(), "/micro-ann");
    defer ann_store.deinit();
    const shards = 64;
    var sources: [shards]block.Writer = undefined;
    var references: [shards]block.Writer = undefined;
    for (0..shards) |i| {
        sources[i] = try block.Writer.initWithEncoding(alloc, 1, @intCast(i), shards, 1, .float32);
        references[i] = try block.Writer.initWithEncoding(alloc, 1, @intCast(i), shards, 1, .artifact_reference);
    }
    defer for (&sources) |*writer| writer.deinit();
    defer for (&references) |*writer| writer.deinit();
    const keys = try alloc.alloc([48]u8, count);
    defer alloc.free(keys);
    const digests = try alloc.alloc([32]u8, count);
    defer alloc.free(digests);
    const Item = struct { id: usize, hash: u64 };
    const Order = struct {
        fn less(_: void, a: Item, b: Item) bool {
            return a.hash < b.hash or (a.hash == b.hash and a.id < b.id);
        }
    };
    const order = try alloc.alloc(Item, count);
    defer alloc.free(order);
    for (keys, digests, order, 0..) |*key, *digest, *item, id| {
        @memset(key, 'x');
        std.mem.writeInt(u64, key[0..8], id, .little);
        std.crypto.hash.sha2.Sha256.hash(key, digest, .{});
        item.* = .{ .id = id, .hash = block.keyHash(digest) };
    }
    std.mem.sort(Item, order, {}, Order.less);
    for (order) |item| try sources[item.hash & (shards - 1)].appendVector(&digests[item.id], 1, 1, &.{ 1, 2, 3, 4 });
    for (order, 0..) |*item, id| item.* = .{ .id = id, .hash = block.keyHash(&keys[id]) };
    std.mem.sort(Item, order, {}, Order.less);
    for (order) |item| try references[item.hash & (shards - 1)].appendEncodedVector(&keys[item.id], 1, 1, 4, &digests[item.id], 1);
    var source_blocks: [shards]native.ShardBlock = undefined;
    var ann_blocks: [shards]native.ShardBlock = undefined;
    for (0..shards) |i| {
        source_blocks[i] = .{ .shard_id = @intCast(i), .bytes = try sources[i].build() };
        ann_blocks[i] = .{ .shard_id = @intCast(i), .bytes = try references[i].build() };
    }
    defer for (source_blocks) |b| alloc.free(b.bytes);
    defer for (ann_blocks) |b| alloc.free(b.bytes);
    source_store.covered_source_sequence = 1;
    ann_store.covered_source_sequence = 1;
    try source_store.publishGeneration(1, 1, &source_blocks, true);
    try ann_store.publishGeneration(1, 1, &ann_blocks, true);
    var source = try native.Store.openWithBlocks(alloc, memory.storage(), "/micro-source");
    defer source.deinit();
    var ann = try native.Store.openWithBlocks(alloc, memory.storage(), "/micro-ann");
    defer ann.deinit();
    ann.external_payloads = &source;
    const bindings = try native.member_bindings.Cache.create(alloc, 1048576, null);
    defer bindings.deinit();
    var random = std.Random.DefaultPrng.init(731);
    var ids: [10000]usize = undefined;
    for (&ids) |*id| id.* = random.random().uintLessThan(usize, count);
    // Cold correctness and binding preparation, outside warm timing.
    const cold_start = time.monotonicNs();
    for (ids) |id| {
        const located = (try ann.locateHashed(&keys[id], block.keyHash(&keys[id]), 1, 1)).vector;
        const row = ann.sourceRow(located).?;
        const rebound = try ann.bindSourceRow(row);
        try std.testing.expectEqual(located.block.owner, rebound.block.owner);
        try std.testing.expectEqual(located.block.reader_index, rebound.block.reader_index);
        try std.testing.expectEqualDeep(located.block.location, rebound.block.location);
        bindings.put(1, id, row);
    }
    const cold_ns = time.monotonicNs() - cold_start;
    var checksum: u64 = 0;
    for (0..4) |round| for (0..2) |arm| {
        const bound = (arm == 1) != (round % 2 == 1);
        var hits: usize = 0;
        const start = time.monotonicNs();
        for (0..10) |_| for (ids) |id| {
            const location = if (bound) blk: {
                if (bindings.get(1, id)) |row| {
                    hits += 1;
                    break :blk try ann.bindSourceRow(row);
                }
                break :blk (try ann.locateHashed(&keys[id], block.keyHash(&keys[id]), 1, 1)).vector;
            } else (try ann.locateHashed(&keys[id], block.keyHash(&keys[id]), 1, 1)).vector;
            // Force the complete return value to escape, as it does when
            // constructing real batched reads. An offset-only checksum lets
            // LLVM eliminate most metadata materialization in the bound arm.
            std.mem.doNotOptimizeAway(location);
            checksum +%= location.block.location.vector_offset;
        };
        const elapsed = time.monotonicNs() - start;
        std.debug.print("member_binding_micro {{\"count\":{d},\"round\":{d},\"bound\":{},\"lookups\":100000,\"hits\":{d},\"elapsed_ns\":{d},\"cold_prepare_ns\":{d},\"checksum\":{d}}}\n", .{ count, round, bound, hits, elapsed, cold_ns, checksum });
    };
}
