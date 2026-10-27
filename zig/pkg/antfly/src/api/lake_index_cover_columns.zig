// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cover directories authenticate independently addressable stripes in one pack.
//! Fixed-size key witnesses bind identity without retaining full ordered tuples.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const metadata = @import("lake_index_decoded_metadata.zig");
const ranges = @import("lake_artifact_ranges.zig");
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
const Block = local.sql_spill.ColumnarBlock;
const Ref = artifacts.ChunkRef;
pub const Section = struct { offset: u32, bytes: u32, digest: [32]u8 };
pub const Directory = struct {
    version: u8 = 1,
    rows: u16,
    pack: Ref,
    sections: []const Section,
    pub fn validate(self: Directory) !void {
        if (self.version != 1 or self.rows == 0 or self.rows > 256 or self.sections.len < 2 or self.sections.len > 1025 or self.pack.byte_len > artifacts.max_block_bytes) return error.InvalidNativeLakeRowIndex;
        try stores.validateSha256ArtifactIdentity(self.pack.artifact_id, self.pack.checksum);
        var end: u64 = 0;
        for (self.sections) |section| {
            if (section.offset != end or section.bytes == 0 or section.bytes > self.pack.byte_len -| end) return error.InvalidNativeLakeRowIndex;
            end += section.bytes;
        }
        if (end != self.pack.byte_len) return error.InvalidNativeLakeRowIndex;
    }
};
pub fn witness(domain: [32]u8, key: []const u8) [32]u8 {
    return witnessView(domain, .from(key));
}
pub fn witnessView(domain: [32]u8, key: tree.KeyView) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-lake-cover-key-v1");
    hash.update(&domain);
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, key.len(), .big);
    hash.update(&length);
    hash.update(key.prefix);
    hash.update(key.suffix);
    var out: [32]u8 = undefined;
    hash.final(&out);
    return out;
}
fn put(a: A, store: stores.ArtifactStore, scope: stores.UploadScope, bytes: []const u8, token: Token, writes: *u64) !Ref {
    if (bytes.len == 0 or bytes.len > artifacts.max_block_bytes) return error.InvalidNativeLakeRowIndex;
    writes.* = @max(writes.*, artifacts.max_block_bytes);
    try stores.chargeReadBudget(writes, bytes.len);
    var upload = store;
    upload.allocator = a;
    const ref = try upload.putScoped(scope, bytes, token);
    defer if (ref.provider_version) |value| a.free(value);
    return .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
}
/// At most two objects per batch, independent of projection width. The pack
/// holds independently verified stripes; its small directory is authenticated
/// by the row-index leaf. No complete ordered key is stored in the pack.
pub fn publish(a: A, store: stores.ArtifactStore, scope: stores.UploadScope, rows: []const local.sql_operators.Row, token: Token, writes: *u64) !Ref {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ref = try publishScratch(arena.allocator(), store, scope, rows, token, writes);
    const id = try a.dupe(u8, ref.artifact_id);
    return .{ .artifact_id = id, .checksum = id[7..71], .byte_len = ref.byte_len };
}
fn publishScratch(a: A, store: stores.ArtifactStore, scope: stores.UploadScope, rows: []const local.sql_operators.Row, token: Token, writes: *u64) !Ref {
    if (rows.len == 0 or rows.len > 256 or rows[0].values.len == 0 or rows[0].values.len > 1024) return error.InvalidNativeLakeRowIndex;
    const width = rows[0].values.len;
    const hashes = try a.alloc([32]u8, rows.len);
    const keys = try a.alloc([2]local.sql_scalar.Datum, rows.len);
    const values = try a.alloc([1]local.sql_scalar.Datum, rows.len);
    const batch = try a.alloc(local.sql_operators.Row, rows.len);
    for (rows, hashes, keys, batch) |row, *hash, *key, *out| {
        if (row.values.len != width or row.keys.len != 1 or row.keys[0].sql_null or row.keys[0].value != .string or row.keys[0].value.string.len < 16) return error.InvalidNativeLakeRowIndex;
        hash.* = witness(scope.domain, row.keys[0].value.string);
        key[0] = local.sql_scalar.Datum.fromJson(.{ .string = hash });
        const tuple = row.keys[0].value.string;
        key[1] = local.sql_scalar.Datum.fromJson(.{ .string = tuple[tuple.len - 16 ..] });
        out.* = .{ .keys = key, .values = &.{}, .ordinal = row.ordinal };
    }
    var pack: std.ArrayList(u8) = .empty;
    const sections = try a.alloc(Section, width + 1);
    for (sections, 0..) |*section, index| {
        try token.check();
        if (index != 0) for (batch, rows, values) |*out, row, *value| {
            value[0] = row.values[index - 1];
            out.* = .{ .values = value, .keys = &.{}, .ordinal = row.ordinal };
        };
        const encoded = try local.sql_spill.encodeColumnarBlockAlloc(a, batch, artifacts.max_block_bytes);
        defer a.free(encoded);
        if (encoded.len > artifacts.max_block_bytes -| pack.items.len) return error.InvalidNativeLakeRowIndex;
        section.* = .{ .offset = @intCast(pack.items.len), .bytes = @intCast(encoded.len), .digest = undefined };
        std.crypto.hash.sha2.Sha256.hash(encoded, &section.digest, .{});
        try pack.appendSlice(a, encoded);
    }
    const pack_ref = try put(a, store, scope, pack.items, token, writes);
    const directory: Directory = .{ .rows = @intCast(rows.len), .pack = pack_ref, .sections = sections };
    try directory.validate();
    return put(a, store, scope, try std.json.Stringify.valueAlloc(a, directory, .{}), token, writes);
}
fn parse(a: A, ref: Ref, bytes: []const u8) !Directory {
    const directory = try std.json.parseFromSliceLeaky(Directory, a, bytes, .{ .allocate = .alloc_always });
    try directory.validate();
    const parent = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeRowIndex;
    const pack_scope = (try stores.uploadScopeFromArtifactId(directory.pack.artifact_id)) orelse return error.InvalidNativeLakeRowIndex;
    if (!std.mem.eql(u8, &parent.domain, &pack_scope.domain)) return error.InvalidNativeLakeRowIndex;
    return directory;
}
pub fn readDirectory(a: A, store: stores.ArtifactStore, ref: Ref, token: Token) !Directory {
    return parse(a, ref, try artifacts.readArtifact(a, store, ref, token, null));
}
pub const Prepared = struct { reference: Ref, directory: ?Directory, bytes: u64 };
/// Reserve exact selected wire bytes before scheduling independent reads.
pub fn prepare(a: A, store: stores.ArtifactStore, ref: Ref, striped: bool, columns: []const usize, width: usize, token: Token, cached: ?artifacts.CachedRead, reads: *u64) !Prepared {
    try token.check();
    if (width > 1024) return error.InvalidNativeLakeRowIndex;
    for (columns) |column| if (column >= width) return error.InvalidNativeLakeRowIndex;
    try stores.chargeReadBudget(reads, ref.byte_len);
    if (!striped) return .{ .reference = ref, .directory = null, .bytes = ref.byte_len };
    const directory = try parse(a, ref, try artifacts.readArtifact(a, store, ref, token, cached));
    if (directory.sections.len != width + 1) return error.InvalidNativeLakeRowIndex;
    var bytes: u64 = 0;
    for (directory.sections, 0..) |section, index| if (index == 0 or std.mem.indexOfScalar(usize, columns, index - 1) != null) {
        bytes += section.bytes;
    };
    try stores.chargeReadBudget(reads, bytes);
    return .{ .reference = ref, .directory = directory, .bytes = bytes + ref.byte_len };
}
pub const Loaded = struct {
    a: A,
    backing: ?A = null,
    budget: ?*local.sql_memory_budget = null,
    scratch_owner: ?*std.heap.ArenaAllocator = null,
    block: Block,
    hashed: bool,
    leases: std.ArrayList(metadata.Owned(Block)) = .empty,
    directory: ?metadata.Owned(Directory) = null,
    wires: std.ArrayList([]u8) = .empty,
    wire_allocator: ?A = null,
    pub fn deinit(self: *@This()) void {
        for (self.leases.items) |owned| owned.release();
        self.leases.deinit(self.a);
        for (self.wires.items) |bytes| self.wire_allocator.?.free(bytes);
        self.wires.deinit(self.a);
        if (self.directory) |owned| owned.release();
        if (self.scratch_owner) |owner| {
            owner.deinit();
            self.backing.?.destroy(owner);
        }
        if (self.budget) |budget| self.backing.?.destroy(budget);
        self.* = undefined;
    }
    pub fn verify(self: Loaded, domain: [32]u8, row: usize, physical: [16]u8, expected: [32]u8) !void {
        if (self.block.keys.len != @as(usize, if (self.hashed) 2 else 1) or row >= self.block.count()) return error.InvalidNativeLakeRowIndex;
        const key = try self.block.keyCell(row, 0);
        if (key.sql_null or key.value != .string) return error.InvalidNativeLakeRowIndex;
        const actual = if (self.hashed) blk: {
            if (key.value.string.len != 32) return error.InvalidNativeLakeRowIndex;
            const coordinate = try self.block.keyCell(row, 1);
            if (coordinate.sql_null or coordinate.value != .string or !std.mem.eql(u8, coordinate.value.string, &physical)) return error.InvalidNativeLakeRowIndex;
            break :blk key.value.string[0..32].*;
        } else blk: {
            const tuple = key.value.string;
            if (tuple.len < 16 or !std.mem.eql(u8, tuple[tuple.len - 16 ..], &physical)) return error.InvalidNativeLakeRowIndex;
            break :blk witness(domain, tuple);
        };
        if (!std.mem.eql(u8, &actual, &expected)) return error.InvalidNativeLakeRowIndex;
    }
};
fn interpretation(buffer: []u8, section: Section) ![]const u8 {
    return std.fmt.bufPrint(buffer, "cover-stripe-v1:{d}:{d}:{s}", .{ section.offset, section.bytes, std.fmt.bytesToHex(section.digest, .lower) });
}
/// The returned projected block uses SQL-null placeholders for omitted columns.
/// Consumers must request every projection/predicate dependency before loading.
pub fn load(backing: A, store: stores.ArtifactStore, ref: Ref, striped: bool, columns: []const usize, width: usize, token: Token, cached: ?artifacts.CachedRead, limit: usize, reads: *u64, prepared: ?Prepared) !Loaded {
    const budget = try backing.create(local.sql_memory_budget);
    budget.* = .{ .backing = backing, .limit = limit };
    errdefer backing.destroy(budget);
    const arena = try backing.create(std.heap.ArenaAllocator);
    arena.* = .init(budget.allocator());
    errdefer {
        arena.deinit();
        backing.destroy(arena);
    }
    var loaded = loadInner(arena.allocator(), store, ref, striped, columns, width, token, cached, limit, reads, prepared) catch |err| {
        if (err == error.OutOfMemory and budget.isLocallyExhausted()) return error.SqlMemoryLimitExceeded;
        return err;
    };
    loaded.backing = backing;
    loaded.budget = budget;
    loaded.scratch_owner = arena;
    return loaded;
}
fn loadInner(a: A, store: stores.ArtifactStore, ref: Ref, striped: bool, columns: []const usize, width: usize, token: Token, cached: ?artifacts.CachedRead, limit: usize, reads: *u64, prepared: ?Prepared) !Loaded {
    try token.check();
    if (width > 1024) return error.InvalidNativeLakeRowIndex;
    for (columns) |column| if (column >= width) return error.InvalidNativeLakeRowIndex;
    try stores.chargeReadBudget(reads, ref.byte_len);
    if (!striped) {
        if (cached) |cache| {
            const owned = try metadata.acquireColumnBlockBounded(cache, store, ref, token, limit);
            errdefer owned.release();
            if (owned.value.values.len != width) return error.InvalidNativeLakeRowIndex;
            var leases: std.ArrayList(metadata.Owned(Block)) = .empty;
            try leases.append(a, owned);
            return .{ .a = a, .block = owned.value.*, .hashed = false, .leases = leases };
        }
        const block = try local.sql_spill.decodeColumnarBlockInArena(a, try artifacts.readArtifact(a, store, ref, token, null), artifacts.max_block_bytes);
        if (block.values.len != width) return error.InvalidNativeLakeRowIndex;
        return .{ .a = a, .block = block, .hashed = false };
    }
    var result: Loaded = .{ .a = a, .block = undefined, .hashed = true, .wire_allocator = local.sql_memory_budget.reclaimingAllocator(a) };
    errdefer result.deinit();
    const directory = if (prepared) |known| verified: {
        if (known.reference.byte_len != ref.byte_len or !std.mem.eql(u8, known.reference.artifact_id, ref.artifact_id) or !std.mem.eql(u8, known.reference.checksum, ref.checksum)) return error.ArtifactIntegrityMismatch;
        const value = known.directory orelse return error.InvalidNativeLakeRowIndex;
        try value.validate();
        break :verified value;
    } else if (cached) |cache| blk: {
        const Loader = struct {
            fn decode(out: A, source: stores.ArtifactStore, reference: local.serverless_manifest_artifact_ref.ArtifactRef, cancel: Token, caching: artifacts.CachedRead) !Directory {
                return parse(out, .{ .artifact_id = reference.artifact_id, .checksum = reference.checksum, .byte_len = reference.byte_len }, try artifacts.readArtifact(out, source, .{ .artifact_id = reference.artifact_id, .checksum = reference.checksum, .byte_len = reference.byte_len }, cancel, caching));
            }
        };
        const owned = try metadata.acquire(Directory, cache, store, .{ .kind = .ordered_row_index, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, Loader.decode);
        result.directory = owned;
        break :blk owned.value.*;
    } else try readDirectory(a, store, ref, token);
    if (directory.sections.len != width + 1) return error.InvalidNativeLakeRowIndex;
    const Column = @typeInfo(@FieldType(Block, "values")).pointer.child;
    const values = try a.alloc(Column, width);
    const nulls = try a.alloc(u8, (@as(usize, directory.rows) + 3) / 4);
    @memset(nulls, 0x55);
    for (values) |*value| value.* = .{ .flags = nulls, .values = .empty };
    var ordinals: []const u64 = &.{};
    var identity_keys: []const Column = &.{};
    var live: usize = if (result.directory) |owned| owned.lease.item.budget.live else 0;
    var selected: std.ArrayList(usize) = .empty;
    for (directory.sections, 0..) |_, index| {
        if (index == 0 or std.mem.indexOfScalar(usize, columns, index - 1) != null) try selected.append(a, index);
    }
    const reference: local.serverless_manifest_artifact_ref.ArtifactRef = .{ .kind = .ordered_row_index, .artifact_id = directory.pack.artifact_id, .checksum = directory.pack.checksum, .byte_len = directory.pack.byte_len };
    const Charge = struct {
        fn charge(raw: *anyopaque, bytes: u64) !void {
            try stores.chargeReadBudget(@ptrCast(@alignCast(raw)), bytes);
        }
    };
    var begin: usize = 0;
    while (begin < selected.items.len) {
        const end = @min(selected.items.len, begin + ranges.max_batch);
        var units: [ranges.max_batch]ranges.Unit = undefined;
        var cold: [ranges.max_batch]?usize = @splat(null);
        var warm: [ranges.max_batch]?metadata.Owned(Block) = @splat(null);
        defer for (&warm) |*slot| if (slot.*) |held| held.release();
        var count: usize = 0;
        var admitted: u64 = 0;
        for (selected.items[begin..end], 0..) |index, position| {
            const section = directory.sections[index];
            try token.check();
            try stores.chargeReadBudget(reads, section.bytes);
            admitted += section.bytes;
            if (cached) |cache| {
                var buffer: [160]u8 = undefined;
                if (try metadata.lookupRequiredInterpretedBounded(Block, cache, reference, token, try interpretation(&buffer, section), limit)) |held| {
                    warm[position] = held;
                    live = std.math.add(usize, live, held.lease.item.budget.live) catch return error.SqlMemoryLimitExceeded;
                    if (live > limit) return error.SqlMemoryLimitExceeded;
                    continue;
                }
            }
            cold[position] = count;
            units[count] = .{ .object = directory.pack, .offset = section.offset, .bytes = section.bytes, .digest = section.digest };
            count += 1;
        }
        // Exact adjacency avoids reading omitted columns or charging gaps. Raw
        // transport storage is reclaimed after this wave; cache units stay stripes.
        var wire = try ranges.load(result.wire_allocator.?, store, units[0..count], token, cached, .{ .ptr = &admitted, .charge = Charge.charge }, .{ .max_range = artifacts.max_block_bytes });
        defer wire.deinit();
        for (selected.items[begin..end], 0..) |index, position| {
            const section = directory.sections[index];
            const bytes = if (cold[position]) |slot| wire.values[slot] else null;
            const Loader = struct {
                const Arguments = struct { stripe: Section, encoded: ?[]const u8 };
                fn decode(out: A, source: stores.ArtifactStore, ref_: local.serverless_manifest_artifact_ref.ArtifactRef, cancel: Token, caching: artifacts.CachedRead, arguments: Arguments) !Block {
                    const data = if (arguments.encoded) |encoded| try out.dupe(u8, encoded) else try artifacts.readColumnRange(out, source, .{ .artifact_id = ref_.artifact_id, .checksum = ref_.checksum, .byte_len = ref_.byte_len }, arguments.stripe.offset, arguments.stripe.bytes, arguments.stripe.digest, cancel, caching);
                    return local.sql_spill.decodeColumnarBlockInArena(out, data, artifacts.max_block_bytes);
                }
            };
            const block = if (cached) |cache| blk: {
                var buffer: [160]u8 = undefined;
                const retained = warm[position] != null;
                const owned = warm[position] orelse try metadata.acquireRequiredInterpretedBounded(Block, cache, store, reference, token, try interpretation(&buffer, section), Loader.Arguments{ .stripe = section, .encoded = bytes }, Loader.decode, limit);
                warm[position] = null;
                var transfer = false;
                defer if (!transfer) owned.release();
                if (!retained) live = std.math.add(usize, live, owned.lease.item.budget.live) catch return error.SqlMemoryLimitExceeded;
                if (live > limit) return error.SqlMemoryLimitExceeded;
                try result.leases.append(a, owned);
                transfer = true;
                break :blk owned.value.*;
            } else blk: {
                const slot = cold[position].?;
                try result.wires.append(a, wire.values[slot].?);
                wire.values[slot] = null;
                break :blk try local.sql_spill.decodeColumnarBlockInArena(a, bytes.?, artifacts.max_block_bytes);
            };
            if (block.count() != directory.rows) return error.InvalidNativeLakeRowIndex;
            if (index == 0) {
                if (block.values.len != 0 or block.keys.len != 2) return error.InvalidNativeLakeRowIndex;
                ordinals = block.ordinals;
                identity_keys = block.keys;
            } else {
                if (block.values.len != 1 or block.keys.len != 0 or !std.mem.eql(u64, block.ordinals, ordinals)) return error.InvalidNativeLakeRowIndex;
                values[index - 1] = block.values[0];
            }
        }
        begin = end;
    }
    result.block = .{ .values = values, .keys = identity_keys, .ordinals = ordinals };
    return result;
}

test "external lake bounded window profiles covering stripes prune wide columns preserve identity and bound allocation faults" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("cover-stripes");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const scope: stores.UploadScope = .{ .domain = @splat(7), .attempt = @splat(3) };
    store.upload_scope = scope;
    var inputs = std.heap.ArenaAllocator.init(a);
    defer inputs.deinit();
    const ia = inputs.allocator();
    const count = 16;
    const payload = try ia.alloc(u8, 131000);
    @memset(payload, 'w');
    var rows: [count]local.sql_operators.Row = undefined;
    var keys: [count][1]local.sql_scalar.Datum = undefined;
    var values: [count][3]local.sql_scalar.Datum = undefined;
    for (&rows, &keys, &values, 0..) |*row, *key, *value, i| {
        const id = try std.fmt.allocPrint(ia, "row-with-physical-{d:0>16}", .{i});
        key[0] = local.sql_scalar.Datum.fromJson(.{ .string = id });
        value.* = .{ local.sql_scalar.Datum.fromJson(.{ .integer = @intCast(i) }), local.sql_scalar.Datum.fromJson(.{ .integer = 9007199254740993 }), local.sql_scalar.Datum.fromJson(.{ .string = try std.fmt.allocPrint(ia, "{s}{d}", .{ payload, i }) }) };
        row.* = .{ .keys = key, .values = value, .ordinal = i };
    }
    var writes: u64 = 0;
    const ref = try publish(a, store, scope, &rows, .none, &writes);
    defer a.free(ref.artifact_id);
    const Probe = struct {
        var active: *@This() = undefined;
        original: *const stores.ArtifactStore.VTable,
        pack: []const u8,
        calls: usize = 0,
        max_length: usize = 0,
        fn get(raw: *anyopaque, alloc: A, id: []const u8, offset: u64, length: usize, cancel: Token) ![]u8 {
            const self = active;
            if (std.mem.eql(u8, id, self.pack)) {
                self.calls += 1;
                self.max_length = @max(self.max_length, length);
            }
            return if (self.original.get_range_alloc_with_cancellation) |read| read(raw, alloc, id, offset, length, cancel) else self.original.get_range_alloc(raw, alloc, id, offset, length);
        }
    };
    const proof = try readDirectory(ia, store, ref, .none);
    var probe: Probe = .{ .original = store.vtable, .pack = proof.pack.artifact_id };
    Probe.active = &probe;
    var traced = store.vtable.*;
    traced.get_range_alloc_with_cancellation = Probe.get;
    store.vtable = &traced;
    const Load = struct {
        fn run(backing: A, backend: stores.ArtifactStore, reference: Ref) !void {
            var reads: u64 = 64 * 1024;
            var loaded = try load(backing, backend, reference, true, &.{ 0, 1 }, 3, .none, null, 256 * 1024, &reads, null);
            defer loaded.deinit();
            try std.testing.expect(64 * 1024 - reads < 16 * 1024);
            for (0..count) |i| {
                try std.testing.expectEqual(@as(i64, @intCast(i)), (try loaded.block.cell(i, 0)).value.integer);
                try std.testing.expectEqual(@as(i64, 9007199254740993), (try loaded.block.cell(i, 1)).value.integer);
                try std.testing.expect((try loaded.block.cell(i, 2)).sql_null);
                var key: [64]u8 = undefined;
                const id = try std.fmt.bufPrint(&key, "row-with-physical-{d:0>16}", .{i});
                try loaded.verify(@splat(7), i, id[id.len - 16 ..][0..16].*, witness(@splat(7), id));
                try std.testing.expectError(error.InvalidNativeLakeRowIndex, loaded.verify(@splat(7), i, @splat(0), witness(@splat(7), id)));
                try std.testing.expectError(error.InvalidNativeLakeRowIndex, loaded.verify(@splat(7), i, id[id.len - 16 ..][0..16].*, @splat(0)));
            }
        }
    };
    try Load.run(a, store, ref);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expect(probe.max_length < 16 * 1024);
    std.debug.print("COVER_RANGE requests_before=3 requests_after={d} selected_max_range={d}\n", .{ probe.calls, probe.max_length });
    {
        var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
        defer cache.deinit();
        cache.max_entries = 0;
        cache.physical.max_entries = 0;
        const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(9), .context = .{ .io = std.testing.io } };
        var allowance: u64 = 64 * 1024;
        var first = try load(a, store, ref, true, &.{ 0, 1 }, 3, .none, cached, 256 * 1024, &allowance, null);
        first.deinit();
        try std.testing.expectEqual(@as(usize, 0), cache.entries.count());
        try std.testing.expectEqual(@as(usize, 0), cache.physical.entries.count());
        const before = probe.calls;
        var second = try load(a, store, ref, true, &.{ 0, 1 }, 3, .none, cached, 256 * 1024, &allowance, null);
        defer second.deinit();
        try std.testing.expectEqual(before, probe.calls);
        try std.testing.expectEqual(@as(i64, 9007199254740993), (try second.block.cell(0, 1)).value.integer);
        std.debug.print("COVER_RANGE decoded_warm_raw_cache_empty_requests=0\n", .{});
    }
    try std.testing.checkAllAllocationFailures(a, Load.run, .{ store, ref });
    var reads: u64 = 4 * 1024 * 1024;
    if (load(a, store, ref, true, &.{ 0, 1, 2 }, 3, .none, null, 256 * 1024, &reads, null)) |value| {
        var unexpected = value;
        unexpected.deinit();
        return error.TestUnexpectedResult;
    } else |err| try std.testing.expectEqual(error.SqlMemoryLimitExceeded, err);
    var wide_reads: u64 = 4 * 1024 * 1024;
    var wide = try load(a, store, ref, true, &.{2}, 3, .none, null, 4 * 1024 * 1024, &wide_reads, null);
    defer wide.deinit();
    try std.testing.expectEqual(@as(usize, 131001), (try wide.block.cell(0, 2)).value.string.len);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const parsed = try readDirectory(scratch.allocator(), store, ref, .none);
    var exact: u64 = 4 * 1024 * 1024;
    const prepared = try prepare(scratch.allocator(), store, ref, true, &.{0}, 3, .none, null, &exact);
    try std.testing.expect(prepared.bytes * 100 < parsed.pack.byte_len);
    var mismatched = prepared;
    mismatched.reference.byte_len += 1;
    var bad_reads: u64 = 4 * 1024 * 1024;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, load(a, store, ref, true, &.{0}, 3, .none, null, 256 * 1024, &bad_reads, mismatched));
    var corrupt = parsed;
    const bad = try scratch.allocator().dupe(Section, parsed.sections);
    bad[0].offset = 1;
    corrupt.sections = bad;
    try std.testing.expectError(error.InvalidNativeLakeRowIndex, corrupt.validate());
    std.debug.print("COVER_COLUMNS rows={d} pack_bytes={d} selected_bytes={d}\n", .{ count, parsed.pack.byte_len, prepared.bytes });
}
