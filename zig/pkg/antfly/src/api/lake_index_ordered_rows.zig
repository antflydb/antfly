// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Native tuple keys in immutable seekable pages. The native publication is
//! the visibility boundary; neither a cache nor an object-store HEAD is one.
const std = @import("std");
const local = @import("antfly_local_sources");
const tuples = local.storage_db_relational_index_keys;
const tree = @import("../serverless/graph_segment/page_tree.zig");
const page_store = @import("../serverless/graph_segment/page_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Ref = local.serverless_manifest_artifact_ref.ArtifactRef;
const rows = local.storage_rowsource_types;
pub const metadata_version: u16 = 4;
pub const max_root_bytes = 4 * 1024 * 1024;
pub const Root = struct {
    version: u16 = metadata_version,
    tuple_encoding: u32 = tuples.encoding_version,
    fingerprint: [32]u8,
    domain: [32]u8,
    page: ?tree.Ref,
    reverse: ?tree.Ref = null,
    source: []const u8,
    snapshot: []const u8,
    files: []const []const u8,
    cover: []const []const u8 = &.{},
    file_fingerprints: []const [32]u8 = &.{},
    pub fn validate(self: Root) !void {
        if (self.version != metadata_version or self.tuple_encoding != tuples.encoding_version or std.mem.allEqual(u8, &self.domain, 0) or std.mem.allEqual(u8, &self.fingerprint, 0) or self.source.len == 0 or self.snapshot.len == 0) return error.InvalidNativeLakeRowIndex;
        if (self.file_fingerprints.len != 0 and self.file_fingerprints.len != self.files.len) return error.InvalidNativeLakeRowIndex;
        if (self.files.len > 16384) return error.InvalidNativeLakeRowIndex;
        if (self.page) |page| try page.validate();
        if (self.reverse) |page| try page.validate();
        if (self.reverse != null and (self.page == null or self.reverse.?.records != self.page.?.records)) return error.InvalidNativeLakeRowIndex;
        var names: std.StringHashMapUnmanaged(void) = .empty;
        defer names.deinit(std.heap.page_allocator);
        for (self.files) |file| {
            if (file.len == 0) return error.InvalidNativeLakeRowIndex;
            if ((try names.getOrPut(std.heap.page_allocator, file)).found_existing) return error.InvalidNativeLakeRowIndex;
        }
        names.clearRetainingCapacity();
        for (self.cover) |column| {
            if (column.len == 0) return error.InvalidNativeLakeRowIndex;
            if ((try names.getOrPut(std.heap.page_allocator, column)).found_existing) return error.InvalidNativeLakeRowIndex;
        }
    }
};

/// Stream a sorted native spill run into bounded immutable B+tree pages.
/// Per-row keys include the physical coordinate, so equal logical tuples have
/// a stable, unique tie and never collapse distinct source rows.
pub fn publish(a: A, result_alloc: A, store: *stores.ArtifactStore, sort: *local.sql_spill.Sort, name: []const u8, fingerprint: [32]u8, inventory: local.serverless_external_source_types.Inventory, cover: []const []const u8, cancellation: Cancellation) !Ref {
    return publishIncremental(a, result_alloc, store, sort, name, fingerprint, inventory, cover, cancellation, .{});
}
pub const Delta = struct {
    previous: ?Root = null,
    keep: []const bool = &.{},
    files: []const []const u8 = &.{},
    fingerprints: []const [32]u8 = &.{},
};
pub fn publishIncremental(a: A, result_alloc: A, store: *stores.ArtifactStore, sort: *local.sql_spill.Sort, name: []const u8, fingerprint: [32]u8, inventory: local.serverless_external_source_types.Inventory, cover: []const []const u8, cancellation: Cancellation, delta: Delta) !Ref {
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var read_bytes: u64 = 256 * 1024 * 1024;
    var write_bytes: u64 = 512 * 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &read_bytes, .remaining_write_bytes = &write_bytes };
    var reverse_sort = local.sql_spill.Sort.init(a, sort.manager, &.{.{}}, 512 * 1024);
    defer reverse_sort.deinit();
    const Sorted = struct {
        sort: *local.sql_spill.Sort,
        store: *stores.ArtifactStore,
        scope: stores.UploadScope,
        cancellation: Cancellation,
        write_bytes: *u64,
        scratch: std.heap.ArenaAllocator,
        pending: []const local.sql_operators.Row = &.{},
        position: usize = 0,
        block: ?[52]u8 = null,
        value: [70]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.position == self.pending.len) {
                _ = self.scratch.reset(.retain_capacity);
                const scratch_alloc = self.scratch.allocator();
                var batch: std.ArrayList(local.sql_operators.Row) = .empty;
                var retained_bytes: usize = 0;
                while (batch.items.len < 256 and retained_bytes < 512 * 1024) {
                    const row = try self.sort.next(scratch_alloc) orelse break;
                    for (row.values) |value| retained_bytes +|= try local.sql_operators.datumBytes(value);
                    for (row.keys) |key| retained_bytes +|= try local.sql_operators.datumBytes(key);
                    try batch.append(scratch_alloc, row);
                }
                if (batch.items.len == 0) return null;
                self.pending = batch.items;
                self.position = 0;
                self.block = null;
                if (batch.items[0].values.len != 0) {
                    const payload = try local.sql_spill.encodeColumnarBlockAlloc(scratch_alloc, batch.items, artifacts.max_block_bytes);
                    if (payload.len > self.write_bytes.*) return error.GraphPageWriteBudgetExceeded;
                    self.write_bytes.* -= payload.len;
                    var upload = self.store.*;
                    upload.allocator = scratch_alloc;
                    const ref = try upload.putScoped(self.scope, payload, self.cancellation);
                    var encoded: [52]u8 = undefined;
                    @memcpy(encoded[0..32], &try stores.sha256DigestFromChecksum(ref.checksum));
                    @memcpy(encoded[32..48], &self.scope.attempt);
                    std.mem.writeInt(u32, encoded[48..52], @intCast(payload.len), .big);
                    self.block = encoded;
                }
            }
            const row = self.pending[self.position];
            const ordinal = self.position;
            self.position += 1;
            if (row.keys.len != 1 or row.keys[0].sql_null or row.keys[0].value != .string) return error.InvalidNativeLakeRowIndex;
            const key = row.keys[0].value.string;
            if (key.len < 16) return error.InvalidNativeLakeRowIndex;
            if (self.block) |block| {
                @memcpy(self.value[0..16], key[key.len - 16 ..]);
                std.mem.writeInt(u16, self.value[16..18], @intCast(ordinal), .big);
                @memcpy(self.value[18..70], &block);
                return .{ .key = key, .value = &self.value };
            }
            return .{ .key = key, .value = key[key.len - 16 ..] };
        }
    };
    var sorted: Sorted = .{ .sort = sort, .store = store, .scope = scope, .cancellation = cancellation, .write_bytes = &write_bytes, .scratch = .init(a) };
    defer sorted.scratch.deinit();
    const Merge = struct {
        sorted: *Sorted,
        cursor: ?tree.Cursor = null,
        keep: []const bool,
        old: ?tree.Cursor.Record = null,
        fresh: ?tree.Cursor.Record = null,
        old_done: bool = false,
        fresh_done: bool = false,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            while (self.old == null and !self.old_done) {
                const record = if (self.cursor) |*cursor| try cursor.next() else null;
                if (record == null) {
                    self.old_done = true;
                    break;
                }
                const row = record.?;
                if ((row.value.len != 16 and row.value.len != 70) or row.key.len < 16 or !std.mem.eql(u8, row.value[0..16], row.key[row.key.len - 16 ..])) return error.InvalidNativeLakeRowIndex;
                const ordinal = std.mem.readInt(u32, row.value[0..4], .big);
                if (ordinal >= self.keep.len) return error.InvalidNativeLakeRowIndex;
                if (self.keep[ordinal]) self.old = row;
            }
            if (self.fresh == null and !self.fresh_done) {
                self.fresh = try self.sorted.next();
                self.fresh_done = self.fresh == null;
            }
            if (self.old) |old| {
                const order = if (self.fresh) |fresh| std.mem.order(u8, old.key, fresh.key) else .lt;
                if (order == .eq) return error.InvalidNativeLakeRowIndex;
                if (order == .lt) {
                    self.old = null;
                    return old;
                }
            }
            const fresh = self.fresh;
            self.fresh = null;
            return fresh;
        }
    };
    var merge: Merge = .{ .sorted = &sorted, .keep = delta.keep };
    if (delta.previous) |previous| {
        if (!std.mem.eql(u8, &previous.domain, &scope.domain) or !std.mem.eql(u8, &previous.fingerprint, &fingerprint) or delta.keep.len != previous.files.len) return error.InvalidNativeLakeRowIndex;
        merge.cursor = try tree.Cursor.init(a, pages.store(), previous.page, "", null);
    }
    defer if (merge.cursor) |*cursor| cursor.deinit();
    var page: ?tree.Ref = null;
    var reverse: ?tree.Ref = null;
    // Authenticated subtree counts make the cost estimate proportional to
    // changed files, without walking every old row before choosing a strategy.
    const has_delta = if (delta.previous) |previous| blk: {
        if (previous.reverse == null) break :blk false;
        var removed: u64 = 0;
        for (delta.keep, 0..) |keep_file, slot| {
            if (keep_file) continue;
            var lower: [4]u8 = undefined;
            var upper: [4]u8 = undefined;
            std.mem.writeInt(u32, &lower, @intCast(slot), .big);
            std.mem.writeInt(u32, &upper, @intCast(slot + 1), .big);
            removed +|= try tree.countRange(a, pages.store(), previous.reverse, &lower, &upper);
        }
        break :blk preferCopyOnWrite(if (previous.page) |root| root.records else 0, sort.total, removed);
    } else false;
    if (has_delta) {
        const previous = delta.previous.?;
        page = previous.page;
        reverse = previous.reverse;
        // Walk only the reverse ranges belonging to replaced/removed files.
        // Every cursor stays on the retained prior root while copy-on-write
        // mutations publish a new candidate, never a partially visible index.
        for (delta.keep, 0..) |keep_file, slot| {
            if (keep_file) continue;
            var prefix: [4]u8 = undefined;
            std.mem.writeInt(u32, &prefix, @intCast(slot), .big);
            var upper: [4]u8 = undefined;
            std.mem.writeInt(u32, &upper, @intCast(slot + 1), .big);
            var cursor = try tree.Cursor.init(a, pages.store(), previous.reverse, &prefix, &upper);
            defer cursor.deinit();
            var batch_arena = std.heap.ArenaAllocator.init(a);
            defer batch_arena.deinit();
            while (true) {
                _ = batch_arena.reset(.retain_capacity);
                const ba = batch_arena.allocator();
                var forward_changes: std.ArrayList(tree.Mutation) = .empty;
                var reverse_changes: std.ArrayList(tree.Mutation) = .empty;
                var bytes: usize = 0;
                while (forward_changes.items.len < 256 and bytes < 512 * 1024) {
                    const record = try cursor.next() orelse break;
                    if (record.key.len < 32 or !std.mem.eql(u8, record.key[0..16], record.key[record.key.len - 16 ..]) or record.value.len != 0) return error.InvalidNativeLakeRowIndex;
                    const key = try ba.dupe(u8, record.key);
                    try forward_changes.append(ba, .{ .key = key[16..], .value = null });
                    try reverse_changes.append(ba, .{ .key = key, .value = null });
                    bytes += key.len;
                }
                if (forward_changes.items.len == 0) break;
                page = try applyChanges(a, pages.store(), page, forward_changes.items);
                reverse = try applyChanges(a, pages.store(), reverse, reverse_changes.items);
            }
        }
        var batch_arena = std.heap.ArenaAllocator.init(a);
        defer batch_arena.deinit();
        while (true) {
            _ = batch_arena.reset(.retain_capacity);
            const ba = batch_arena.allocator();
            var forward_changes: std.ArrayList(tree.Mutation) = .empty;
            var reverse_changes: std.ArrayList(tree.Mutation) = .empty;
            var bytes: usize = 0;
            while (forward_changes.items.len < 256 and bytes < 512 * 1024) {
                const record = try sorted.next() orelse break;
                const key = try ba.dupe(u8, record.key);
                const value = try ba.dupe(u8, record.value);
                try forward_changes.append(ba, .{ .key = key, .value = value });
                try reverse_changes.append(ba, .{ .key = try std.mem.concat(ba, u8, &.{ key[key.len - 16 ..], key }), .value = "" });
                bytes += key.len * 2 + value.len;
            }
            if (forward_changes.items.len == 0) break;
            page = try applyChanges(a, pages.store(), page, forward_changes.items);
            reverse = try applyChanges(a, pages.store(), reverse, reverse_changes.items);
        }
    } else {
        // Initial and legacy roots get a streaming reverse tree once.
        const Collect = struct {
            merge: *Merge,
            reverse_sort: *local.sql_spill.Sort,
            arena: std.heap.ArenaAllocator,
            ordinal: u64 = 0,
            pub fn next(self: *@This()) !?tree.Cursor.Record {
                const record = try self.merge.next() orelse return null;
                _ = self.arena.reset(.retain_capacity);
                const key = try std.mem.concat(self.arena.allocator(), u8, &.{ record.key[record.key.len - 16 ..], record.key });
                try self.reverse_sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = key })}, .values = &.{}, .ordinal = self.ordinal });
                self.ordinal += 1;
                return record;
            }
        };
        var collect: Collect = .{ .merge = &merge, .reverse_sort = &reverse_sort, .arena = .init(a) };
        defer collect.arena.deinit();
        page = try tree.buildSorted(a, pages.store(), &collect);
        var reverse_sorted: Sorted = .{ .sort = &reverse_sort, .store = store, .scope = scope, .cancellation = cancellation, .write_bytes = &write_bytes, .scratch = .init(a) };
        defer reverse_sorted.scratch.deinit();
        const ReverseSource = struct {
            sorted: *Sorted,
            pub fn next(self: *@This()) !?tree.Cursor.Record {
                const record = try self.sorted.next() orelse return null;
                return .{ .key = record.key, .value = "" };
            }
        };
        var reverse_source: ReverseSource = .{ .sorted = &reverse_sorted };
        reverse = try tree.buildSorted(a, pages.store(), &reverse_source);
    }
    const files = try a.alloc([]const u8, inventory.files.len);
    defer a.free(files);
    for (files, inventory.files) |*file, entry| file.* = entry.file_id;
    const root: Root = .{ .fingerprint = fingerprint, .domain = scope.domain, .page = page, .reverse = reverse, .source = inventory.source_id, .snapshot = inventory.snapshot_id, .files = if (delta.files.len != 0) delta.files else files, .file_fingerprints = delta.fingerprints, .cover = cover };
    try root.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, root, .{});
    defer a.free(bytes);
    if (bytes.len > max_root_bytes) return error.LakeSidecarBuildLimitExceeded;
    var upload = store.*;
    upload.allocator = result_alloc;
    var ref = try upload.putWithCancellation(bytes, cancellation);
    errdefer ref.deinit(result_alloc);
    return .{ .kind = .ordered_row_index, .name = try result_alloc.dupe(u8, name), .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len, .metadata_version = metadata_version };
}

fn applyChanges(a: A, store: tree.Store, root: ?tree.Ref, changes: []tree.Mutation) !?tree.Ref {
    std.mem.sort(tree.Mutation, changes, {}, struct {
        fn less(_: void, left: tree.Mutation, right: tree.Mutation) bool {
            return std.mem.order(u8, left.key, right.key) == .lt;
        }
    }.less);
    return tree.apply(a, store, root, changes);
}

pub fn loadRoot(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Root {
    if (ref.kind != .ordered_row_index or ref.metadata_version != metadata_version or ref.byte_len > max_root_bytes) return error.InvalidNativeLakeRowIndex;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, cache);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    try root.validate();
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeRowIndex;
    if (!std.mem.eql(u8, &scope.domain, &root.domain)) return error.InvalidNativeLakeRowIndex;
    return root;
}

/// Caller retains the leased root and store capability through cursor close.
/// A batch is owned by its caller, including all physical identity strings.
pub const Reader = struct {
    physical_cursor: ?tree.Cursor = null,
    root: Root,
    pages: page_store.PageStore,
    cursor: ?tree.Cursor = null,
    remaining_reads: u64 = 256 * 1024 * 1024,
    remaining_writes: u64 = 0,
    exhausted: bool = false,
    cached: ?artifacts.CachedRead = null,
    pub fn init(self: *Reader, a: A, store: *stores.ArtifactStore, root: Root, fingerprint: [32]u8, lower: []const u8, upper: ?[]const u8, cancellation: Cancellation) !void {
        return self.initCached(a, store, root, fingerprint, lower, upper, cancellation, null);
    }
    pub fn initCached(self: *Reader, a: A, store: *stores.ArtifactStore, root: Root, fingerprint: [32]u8, lower: []const u8, upper: ?[]const u8, cancellation: Cancellation, cached: ?artifacts.CachedRead) !void {
        try root.validate();
        if (!std.mem.eql(u8, &root.fingerprint, &fingerprint)) return error.ExternalLakeIndexUnavailable;
        self.* = .{ .root = root, .pages = undefined, .cached = cached };
        self.pages = .{ .domain = root.domain, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &self.remaining_reads, .remaining_write_bytes = &self.remaining_writes };
        if (cached != null) self.pages.read_cache = .{ .ptr = self, .read = readPage };
        self.cursor = try tree.Cursor.init(a, self.pages.store(), root.page, lower, upper);
    }
    /// Enclosed immutable subtrees carry exact counts without leaf reads.
    pub fn countRange(self: *Reader, a: A, lower: []const u8, upper: ?[]const u8) !u64 {
        return tree.countRange(a, self.pages.store(), self.root.page, lower, upper);
    }
    fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: Ref, offset: u64, len: usize, _: [32]u8, cancellation: Cancellation, budget: *u64) ![]u8 {
        const self: *Reader = @ptrCast(@alignCast(raw));
        if (offset != 0 or len != ref.byte_len) return error.InvalidNativeLakeRowIndex;
        try stores.chargeReadBudget(budget, ref.byte_len);
        return artifacts.readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, self.cached);
    }
    pub fn deinit(self: *Reader) void {
        if (self.cursor) |*cursor| cursor.deinit();
        self.cursor = null;
        if (self.physical_cursor) |*cursor| cursor.deinit();
        self.physical_cursor = null;
    }
    /// Reverse-tree order is physical file/group/row order. Test the stored
    /// tuple against the exact seek bounds without opening Parquet columns.
    pub fn nextPhysical(self: *Reader, a: A, max_rows: usize, lower: []const u8, upper: ?[]const u8) ![]const rows.RowRef {
        if (max_rows == 0 or max_rows > 65536) return error.InvalidNativeLakeRowIndex;
        if (self.physical_cursor == null) self.physical_cursor = try tree.Cursor.init(self.cursor.?.alloc, self.pages.store(), self.root.reverse orelse return error.InvalidNativeLakeRowIndex, "", null);
        var refs: std.ArrayList(rows.RowRef) = .empty;
        errdefer refs.deinit(a);
        while (refs.items.len < max_rows) {
            const record = try self.physical_cursor.?.next() orelse break;
            if (record.key.len < 32) return error.InvalidNativeLakeRowIndex;
            const key = record.key[16..];
            if (!std.mem.eql(u8, record.key[0..16], key[key.len - 16 ..])) return error.InvalidNativeLakeRowIndex;
            if (std.mem.order(u8, key, lower) == .lt) continue;
            if (upper) |end| if (std.mem.order(u8, key, end) != .lt) continue;
            try refs.append(a, try self.decode(.{ .key = key, .value = record.key[0..16] }));
        }
        return refs.toOwnedSlice(a);
    }
    pub const Entry = struct { key: []const u8, ref: rows.RowRef, cover: ?Cover = null };
    pub const Cover = struct { block: artifacts.ChunkRef, row: u16 };
    pub fn nextEntries(self: *Reader, a: A, max_rows: usize) ![]const Entry {
        if (max_rows == 0 or max_rows > 65536) return error.InvalidNativeLakeRowIndex;
        var entries: std.ArrayList(Entry) = .empty;
        errdefer entries.deinit(a);
        while (!self.exhausted and entries.items.len < max_rows) {
            const record = try self.cursor.?.next() orelse {
                self.exhausted = true;
                break;
            };
            try entries.append(a, .{ .key = try a.dupe(u8, record.key), .ref = try self.decode(record), .cover = try coverReference(a, self.root.domain, record.value) });
        }
        return entries.toOwnedSlice(a);
    }
    fn decode(self: *Reader, record: tree.Cursor.Record) !rows.RowRef {
        if ((record.value.len != 16 and record.value.len != 70) or record.key.len < 16 or !std.mem.eql(u8, record.value[0..16], record.key[record.key.len - 16 ..])) return error.InvalidNativeLakeRowIndex;
        const physical: *const [16]u8 = @ptrCast(record.value.ptr);
        const file = std.mem.readInt(u32, physical[0..4], .big);
        if (file >= self.root.files.len) return error.InvalidNativeLakeRowIndex;
        return .{ .external = .{ .source_id = self.root.source, .snapshot_id = self.root.snapshot, .file_id = self.root.files[file], .row_group_ordinal = std.mem.readInt(u32, physical[4..8], .big), .row_ordinal = std.mem.readInt(u64, physical[8..16], .big) } };
    }
    pub fn next(self: *Reader, a: A, max_rows: usize) ![]const rows.RowRef {
        if (max_rows == 0 or max_rows > 65536) return error.InvalidNativeLakeRowIndex;
        var refs: std.ArrayList(rows.RowRef) = .empty;
        errdefer refs.deinit(a);
        while (!self.exhausted and refs.items.len < max_rows) {
            const record = try self.cursor.?.next() orelse {
                self.exhausted = true;
                break;
            };
            try refs.append(a, try self.decode(record));
        }
        // Root strings already belong to the reader's arena. The consumer
        // retains that owner throughout candidate hydration.
        return refs.toOwnedSlice(a);
    }
};

pub fn coordinate(file: u32, ref: rows.ExternalRowRef) [16]u8 {
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u32, bytes[0..4], file, .big);
    std.mem.writeInt(u32, bytes[4..8], ref.row_group_ordinal, .big);
    std.mem.writeInt(u64, bytes[8..16], ref.row_ordinal, .big);
    return bytes;
}

/// Cover blocks are native typed columns, never per-row JSON documents.
pub fn coverReference(a: A, domain: [32]u8, value: []const u8) !?Reader.Cover {
    if (value.len == 16) return null;
    if (value.len != 70) return error.InvalidNativeLakeRowIndex;
    const row = std.mem.readInt(u16, value[16..18], .big);
    const bytes = std.mem.readInt(u32, value[66..70], .big);
    if (row >= 256 or bytes == 0 or bytes > artifacts.max_block_bytes) return error.InvalidNativeLakeRowIndex;
    const scope: stores.UploadScope = .{ .domain = domain, .attempt = value[50..66].* };
    const digest = std.fmt.bytesToHex(value[18..50].*, .lower);
    const id = try a.dupe(u8, &try scope.artifactId(&digest));
    return .{ .block = .{ .artifact_id = id, .checksum = id[7..71], .byte_len = bytes }, .row = row };
}

test "external lake ordered deltas retain untouched pages and delete only one file range" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("ordered-cow-deltas");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(5), .attempt = @splat(1) };
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check };
    defer manager.deinit();
    const Make = struct {
        fn add(sort: *local.sql_spill.Sort, n: u32, file: u32, row: u64) !void {
            var key: [148]u8 = @splat('x');
            std.mem.writeInt(u32, key[0..4], n, .big);
            @memcpy(key[132..148], &coordinate(file, .{ .source_id = "lake", .snapshot_id = "snapshot", .file_id = "file", .row_group_ordinal = 0, .row_ordinal = row }));
            try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &key })}, .values = &.{}, .ordinal = n });
        }
    };
    const files: []const local.serverless_external_source_types.FileEntry = &.{
        .{ .file_id = @constCast("a"), .object_uri = @constCast("file://a"), .byte_len = 1, .row_count = 2048, .row_groups = &.{} },
        .{ .file_id = @constCast("b"), .object_uri = @constCast("file://b"), .byte_len = 1, .row_count = 1, .row_groups = &.{} },
    };
    var inventory: local.serverless_external_source_types.Inventory = .{ .format = .parquet, .source_id = @constCast("lake"), .source_uri = @constCast("file://lake"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = @constCast(files[0..1]) };
    var first = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 64 * 1024);
    defer first.deinit();
    for (0..2048) |n| try Make.add(&first, @intCast(n), 0, n);
    const ref = try publish(a, ca, &store, &first, "ordered", @splat(3), inventory, &.{}, .none);
    const root = try loadRoot(ca, store, ref, .none, null);
    // Physical reverse traversal applies tuple bounds without hydration.
    var predicate_reader: Reader = undefined;
    var lower_key: [4]u8 = undefined;
    var upper_key: [4]u8 = undefined;
    std.mem.writeInt(u32, &lower_key, 100, .big);
    std.mem.writeInt(u32, &upper_key, 103, .big);
    try predicate_reader.init(a, &store, root, @splat(3), &lower_key, &upper_key, .none);
    defer predicate_reader.deinit();
    const physical_matches = try predicate_reader.nextPhysical(ca, 1024, &lower_key, &upper_key);
    try std.testing.expectEqual(@as(usize, 3), physical_matches.len);
    for (physical_matches, 100..) |row, expected_row| try std.testing.expectEqual(@as(u64, @intCast(expected_row)), row.external.row_ordinal);
    try std.testing.expectEqual(@as(usize, 0), (try predicate_reader.nextPhysical(ca, 1024, &lower_key, &upper_key)).len);
    const Pages = struct {
        refs: std.AutoHashMapUnmanaged([32]u8, void) = .empty,
        alloc: A,
        pub fn skip(_: *@This(), _: tree.Ref) !bool {
            return false;
        }
        pub fn visit(self: *@This(), page: tree.Ref) !void {
            try self.refs.put(self.alloc, page.digest, {});
        }
    };
    var before: Pages = .{ .alloc = a };
    defer before.refs.deinit(a);
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 0;
    var pages: page_store.PageStore = .{ .domain = root.domain, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    try tree.walkPostOrder(a, pages.store(), root.page.?, &before, false);
    try std.testing.expect(before.refs.count() > 3);
    inventory.files = @constCast(files);
    store.upload_scope.?.attempt = @splat(2);
    var append = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 64 * 1024);
    defer append.deinit();
    try Make.add(&append, 5000, 1, 0);
    const appended = try publishIncremental(a, ca, &store, &append, "ordered", @splat(3), inventory, &.{}, .none, .{ .previous = root, .keep = &.{true}, .files = &.{ "a", "b" }, .fingerprints = &.{ @splat(1), @splat(2) } });
    const next = try loadRoot(ca, store, appended, .none, null);
    try std.testing.expectEqual(@as(u64, 2049), next.page.?.records);
    try std.testing.expectEqual(next.page.?.records, next.reverse.?.records);
    var after: Pages = .{ .alloc = a };
    defer after.refs.deinit(a);
    try tree.walkPostOrder(a, pages.store(), next.page.?, &after, false);
    var retained: usize = 0;
    var iterator = after.refs.keyIterator();
    while (iterator.next()) |key| retained += @intFromBool(before.refs.contains(key.*));
    try std.testing.expect(retained > 2);
    store.upload_scope.?.attempt = @splat(3);
    var replacement = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 64 * 1024);
    defer replacement.deinit();
    try Make.add(&replacement, 9000, 0, 0);
    const replaced = try publishIncremental(a, ca, &store, &replacement, "ordered", @splat(3), inventory, &.{}, .none, .{ .previous = next, .keep = &.{ false, true }, .files = &.{ "a", "b" }, .fingerprints = &.{ @splat(3), @splat(2) } });
    const final = try loadRoot(ca, store, replaced, .none, null);
    try std.testing.expectEqual(@as(u64, 2), final.page.?.records);
    try std.testing.expectEqual(final.page.?.records, final.reverse.?.records);
    var reader: Reader = undefined;
    try reader.init(a, &store, final, @splat(3), "", null, .none);
    defer reader.deinit();
    const entries = try reader.nextEntries(ca, 10);
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings("b", entries[0].ref.external.file_id);
    try std.testing.expectEqualStrings("a", entries[1].ref.external.file_id);
    var original: Reader = undefined;
    try original.init(a, &store, root, @splat(3), "", null, .none);
    defer original.deinit();
    try std.testing.expectEqual(@as(usize, 10), (try original.next(ca, 10)).len);
}

// Small deltas share untouched pages. Large changes use one bounded sorted
// merge rather than repeatedly rewriting the same paths in 256-row batches.
fn preferCopyOnWrite(records: u64, inserted: u64, removed: u64) bool {
    return inserted +| removed < @max(@as(u64, 1024), records / 8);
}

test "external lake ordered update strategy accounts for inserted and removed rows" {
    try std.testing.expect(preferCopyOnWrite(100_000, 50, 50));
    try std.testing.expect(!preferCopyOnWrite(100_000, 1, 20_000));
    try std.testing.expect(!preferCopyOnWrite(100_000, 20_000, 0));
    try std.testing.expect(preferCopyOnWrite(2, 2, 2));
}
