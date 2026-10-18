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
const tie_directory = @import("lake_index_tie_directory.zig");
const Ref = local.serverless_manifest_artifact_ref.ArtifactRef;
const rows = local.storage_rowsource_types;
pub const metadata_version: u16 = 9;
pub const file_dictionary = @import("lake_index_ordered_files.zig");
// Distinct checkpoint metadata lets older workers safely discard derived runs
// instead of mistaking a partial forest for a complete serving index.
pub const checkpoint_metadata_version: u16 = 10;
pub fn supportsBuildMetadata(version: u16) bool {
    return version == 5 or version == 6 or version == 7 or version == 8 or version == metadata_version or version == checkpoint_metadata_version;
}
pub const predicate_blocks = @import("lake_index_predicate_blocks.zig");
pub const max_root_bytes = 4 * 1024 * 1024;
pub const Root = struct {
    pub const Run = struct { page: tree.Ref, level: u8 };
    pub const FileSlot = struct { file: []const u8, slot: u32 };
    /// Checkpoint-only binary merge forest. Serving roots always clear it.
    building: bool = false,
    build_level: u8 = 0,
    build_runs: []const Run = &.{},
    /// Authenticated artifact identity; populated by loadRoot, never persisted.
    retained_id: []const u8 = "",
    version: u16 = metadata_version,
    tuple_encoding: u32 = tuples.encoding_version,
    fingerprint: [32]u8,
    domain: [32]u8,
    page: ?tree.Ref,
    reverse: ?tree.Ref = null,
    ties: ?tree.Ref = null,
    tie_version: u8 = 0,
    /// Derived once by loadRoot in the bounded decoded-metadata cache.
    public_digests: []const [32]u8 = &.{},
    public_slots: []const u32 = &.{},
    file_slots: []const FileSlot = &.{},
    predicates: ?tree.Ref = null,
    source: []const u8,
    snapshot: []const u8,
    files: []const []const u8 = &.{},
    file_dictionary: ?file_dictionary.Roots = null,
    /// Build-only borrowed publication directory; its references persist in the dictionary.
    public_navigation: ?file_dictionary.Public = null,
    cover: []const []const u8 = &.{},
    file_fingerprints: []const [32]u8 = &.{},
    pub fn jsonStringify(self: Root, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Root).@"struct".field_names) |field| {
            if (comptime !std.mem.eql(u8, field, "public_navigation") and !std.mem.eql(u8, field, "retained_id") and !std.mem.eql(u8, field, "public_digests") and !std.mem.eql(u8, field, "public_slots") and !std.mem.eql(u8, field, "file_slots")) {
                const checkpoint_field = comptime std.mem.eql(u8, field, "building") or std.mem.eql(u8, field, "build_level") or std.mem.eql(u8, field, "build_runs");
                const dictionary_field = comptime std.mem.eql(u8, field, "files") or std.mem.eql(u8, field, "file_fingerprints");
                if ((!checkpoint_field or self.building) and (!dictionary_field or self.file_dictionary == null)) {
                    try stream.objectField(field);
                    try stream.write(@field(self, field));
                }
            }
        }
        try stream.endObject();
    }
    pub fn fileCount(self: Root) usize {
        return if (self.file_dictionary) |dictionary| dictionary.count else self.files.len;
    }
    pub fn validate(self: Root) !void {
        if (!(if (self.building) (self.version == 6 or self.version == 8 or self.version == checkpoint_metadata_version) else (self.version == 5 or self.version == 7 or self.version == metadata_version)) or self.tuple_encoding != tuples.encoding_version or std.mem.allEqual(u8, &self.domain, 0) or std.mem.allEqual(u8, &self.fingerprint, 0) or self.source.len == 0 or self.snapshot.len == 0) return error.InvalidNativeLakeRowIndex;
        if (self.file_fingerprints.len != 0 and self.file_fingerprints.len != self.files.len) return error.InvalidNativeLakeRowIndex;
        if (self.files.len > std.math.maxInt(u32) or (self.version <= 6 and self.files.len > 16384)) return error.InvalidNativeLakeRowIndex;
        if (self.file_dictionary) |dictionary| {
            if (self.version < 7) return error.InvalidNativeLakeRowIndex;
            try file_dictionary.validate(dictionary);
            if (dictionary.public_names and self.version < 9) return error.InvalidNativeLakeRowIndex;
            if (self.files.len != 0 and self.files.len != dictionary.count) return error.InvalidNativeLakeRowIndex;
        }
        if (self.build_runs.len > 16 or self.build_level > 16 or (!self.building and (self.build_runs.len != 0 or self.build_level != 0))) return error.InvalidNativeLakeRowIndex;
        var levels: u32 = if (self.page != null) @as(u32, 1) << @as(u5, @intCast(self.build_level)) else 0;
        for (self.build_runs) |run| {
            if (run.level > 16 or levels & (@as(u32, 1) << @as(u5, @intCast(run.level))) != 0) return error.InvalidNativeLakeRowIndex;
            levels |= @as(u32, 1) << @as(u5, @intCast(run.level));
            try run.page.validate();
        }
        if (self.page) |page| try page.validate();
        if (self.reverse) |page| try page.validate();
        if (self.predicates) |page| try page.validate();
        if (self.tie_version > 1 or (self.tie_version == 0 and self.ties != null) or (self.tie_version == 1 and (self.page != null) != (self.ties != null))) return error.InvalidNativeLakeRowIndex;
        if (self.ties) |page| try page.validate();
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
    public_navigation: ?file_dictionary.Public = null,
    run_only: bool = false,
    // Dictionary ownership is independent of a data run's merge ancestry.
    dictionary_previous: ?Root = null,
    dictionary_slots: ?[]const u32 = null,
    previous: ?Root = null,
    keep: []const bool = &.{},
    /// Explicit retirements avoid walking the complete slot mask per checkpoint.
    retired_slots: ?[]const u32 = null,
    files: []const []const u8 = &.{},
    fingerprints: []const [32]u8 = &.{},
    reverse_built: bool = false,
    built_reverse: ?tree.Ref = null,
};
const Retirements = struct {
    keep: []const bool,
    slots: ?[]const u32,
    position: usize = 0,
    previous: ?u32 = null,
    fn next(self: *@This()) !?u32 {
        if (self.slots) |slots| {
            if (self.position == slots.len) return null;
            const slot = slots[self.position];
            if (slot >= self.keep.len or self.keep[slot] or (self.previous != null and slot <= self.previous.?)) return error.InvalidNativeLakeRowIndex;
            self.position += 1;
            self.previous = slot;
            return slot;
        }
        while (self.position < self.keep.len) {
            const slot = self.position;
            self.position += 1;
            if (!self.keep[slot]) return @intCast(slot);
        }
        return null;
    }
};
/// Initial scans arrive in file-slot/row coordinates. Build the reverse tree
/// directly in that order instead of sorting the tuple stream a second time.
/// The address stays stable while the push builder borrows its page store.
pub const ReverseBuilder = struct {
    a: A,
    pages: page_store.PageStore,
    reads: u64 = 0,
    writes: u64 = 512 * 1024 * 1024,
    builder: tree.SortedBuilder,
    pub fn create(a: A, store: *stores.ArtifactStore, cancellation: Cancellation) !*ReverseBuilder {
        const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
        const self = try a.create(ReverseBuilder);
        self.* = .{ .a = a, .pages = undefined, .builder = undefined };
        self.pages = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &self.reads, .remaining_write_bytes = &self.writes, .streaming = true };
        var storage = self.pages.store();
        storage.reduce = @import("lake_index_predicate_ranges.zig").reduce;
        self.builder = .{ .alloc = a, .store = storage };
        return self;
    }
    pub fn add(self: *ReverseBuilder, key: []const u8) !void {
        if (key.len < 16) return error.InvalidNativeLakeRowIndex;
        const bytes = try std.mem.concat(self.a, u8, &.{ key[key.len - 16 ..], key });
        defer self.a.free(bytes);
        try self.builder.add(bytes, "");
    }
    pub fn finish(self: *ReverseBuilder) !?tree.Ref {
        return self.builder.finish();
    }
    pub fn deinit(self: *ReverseBuilder) void {
        self.builder.deinit();
        self.a.destroy(self);
    }
};

pub fn publishIncremental(a: A, result_alloc: A, store: *stores.ArtifactStore, sort: *local.sql_spill.Sort, name: []const u8, fingerprint: [32]u8, inventory: local.serverless_external_source_types.Inventory, cover: []const []const u8, cancellation: Cancellation, delta: Delta) !Ref {
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var read_bytes: u64 = 256 * 1024 * 1024;
    var write_bytes: u64 = 512 * 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &read_bytes, .remaining_write_bytes = &write_bytes, .streaming = true, .io = sort.manager.io };
    var reverse_storage = pages.store();
    reverse_storage.reduce = @import("lake_index_predicate_ranges.zig").reduce;
    var predicates = predicate_blocks.Builder.init(a, sort.manager);
    defer predicates.deinit();
    var reverse_sort = local.sql_spill.Sort.init(a, sort.manager, &.{.{}}, 512 * 1024);
    defer reverse_sort.deinit();
    reverse_sort.shared_runs = true;
    reverse_sort.encoded_bytes = true;
    var tie_builder = tie_directory.Builder.init(a, sort.manager);
    defer tie_builder.deinit();
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
                    self.write_bytes.* = @max(self.write_bytes.*, artifacts.max_block_bytes);
                    if (payload.len > self.write_bytes.*) return error.GraphPageWriteBudgetExceeded;
                    self.write_bytes.* -= payload.len;
                    var upload = self.store.*;
                    upload.allocator = scratch_alloc;
                    const ref = try upload.putScoped(self.scope, payload, self.cancellation);
                    defer if (ref.provider_version) |version| upload.allocator.free(version);
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
        cursor: ?RunSource = null,
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
        if (!std.mem.eql(u8, &previous.domain, &scope.domain) or !std.mem.eql(u8, &previous.fingerprint, &fingerprint) or delta.keep.len != previous.fileCount()) return error.InvalidNativeLakeRowIndex;
        merge.cursor = try RunSource.init(a, pages.store(), previous, sort.manager);
    }
    defer if (merge.cursor) |*cursor| cursor.deinit();
    var page: ?tree.Ref = null;
    var reverse: ?tree.Ref = null;
    var ties: ?tree.Ref = null;
    // Authenticated subtree counts make the cost estimate proportional to
    // changed files, without walking every old row before choosing a strategy.
    const has_delta = if (delta.previous) |previous| blk: {
        if (previous.building or delta.run_only or previous.tie_version != 1 or previous.reverse == null or (previous.page != null and previous.predicates == null)) break :blk false;
        var removed: u64 = 0;
        var retirements: Retirements = .{ .keep = delta.keep, .slots = delta.retired_slots };
        while (try retirements.next()) |slot| {
            var lower: [4]u8 = undefined;
            var upper: [4]u8 = undefined;
            std.mem.writeInt(u32, &lower, @intCast(slot), .big);
            std.mem.writeInt(u32, &upper, @intCast(slot + 1), .big);
            removed +|= try tree.countRange(a, pages.store(), previous.reverse, &lower, &upper);
        }
        break :blk try preferPageCopyOnWrite(a, pages.store(), previous.page, sort, removed);
    } else false;
    if (has_delta) {
        const previous = delta.previous.?;
        page = previous.page;
        reverse = previous.reverse;
        ties = previous.ties;
        // Walk only the reverse ranges belonging to replaced/removed files.
        // Every cursor stays on the retained prior root while copy-on-write
        // mutations publish a new candidate, never a partially visible index.
        var retirements: Retirements = .{ .keep = delta.keep, .slots = delta.retired_slots };
        while (try retirements.next()) |slot| {
            var prefix: [4]u8 = undefined;
            std.mem.writeInt(u32, &prefix, @intCast(slot), .big);
            var upper: [4]u8 = undefined;
            std.mem.writeInt(u32, &upper, @intCast(slot + 1), .big);
            var cursor = try tree.Cursor.init(a, pages.store(), previous.reverse, &prefix, &upper);
            defer cursor.deinit();
            var batch_arena = std.heap.ArenaAllocator.init(a);
            defer batch_arena.deinit();
            while (true) {
                try sort.manager.check();
                _ = batch_arena.reset(.retain_capacity);
                const ba = batch_arena.allocator();
                var forward_changes: std.ArrayList(tree.Mutation) = .empty;
                var reverse_changes: std.ArrayList(tree.Mutation) = .empty;
                var bytes: usize = 0;
                while (forward_changes.items.len < 256 and bytes < 512 * 1024) {
                    const record = try cursor.next() orelse break;
                    if (record.key.len < 32 or !std.mem.eql(u8, record.key[0..16], record.key[record.key.len - 16 ..]) or record.value.len != 0) return error.InvalidNativeLakeRowIndex;
                    const key = try ba.dupe(u8, record.key);
                    try predicates.remove(key[16..]);
                    try forward_changes.append(ba, .{ .key = key[16..], .value = null });
                    try reverse_changes.append(ba, .{ .key = key, .value = null });
                    bytes += key.len;
                }
                if (forward_changes.items.len == 0) break;
                ties = try tie_directory.apply(a, pages.store(), ties, forward_changes.items);
                page = try applyChanges(a, pages.store(), page, forward_changes.items);
                reverse = try applyChanges(a, reverse_storage, reverse, reverse_changes.items);
            }
        }
        var batch_arena = std.heap.ArenaAllocator.init(a);
        defer batch_arena.deinit();
        while (true) {
            try sort.manager.check();
            _ = batch_arena.reset(.retain_capacity);
            const ba = batch_arena.allocator();
            var forward_changes: std.ArrayList(tree.Mutation) = .empty;
            var reverse_changes: std.ArrayList(tree.Mutation) = .empty;
            var bytes: usize = 0;
            while (forward_changes.items.len < 256 and bytes < 512 * 1024) {
                const record = try sorted.next() orelse break;
                try predicates.add(record.key);
                const key = try ba.dupe(u8, record.key);
                const value = try ba.dupe(u8, record.value);
                try forward_changes.append(ba, .{ .key = key, .value = value });
                try reverse_changes.append(ba, .{ .key = try std.mem.concat(ba, u8, &.{ key[key.len - 16 ..], key }), .value = "" });
                bytes += key.len * 2 + value.len;
            }
            if (forward_changes.items.len == 0) break;
            ties = try tie_directory.apply(a, pages.store(), ties, forward_changes.items);
            page = try applyChanges(a, pages.store(), page, forward_changes.items);
            reverse = try applyChanges(a, reverse_storage, reverse, reverse_changes.items);
        }
    } else {
        // Initial and legacy roots get a streaming reverse tree once.
        const Collect = struct {
            merge: *Merge,
            reverse_sort: ?*local.sql_spill.Sort,
            predicates: *predicate_blocks.Builder,
            ties: *tie_directory.Builder,
            arena: std.heap.ArenaAllocator,
            ordinal: u64 = 0,
            run_only: bool = false,
            pub fn next(self: *@This()) !?tree.Cursor.Record {
                const record = try self.merge.next() orelse return null;
                if (!self.run_only) {
                    try self.predicates.add(record.key);
                    try self.ties.add(record.key);
                }
                _ = self.arena.reset(.retain_capacity);
                if (self.reverse_sort) |sort_| {
                    const key = try std.mem.concat(self.arena.allocator(), u8, &.{ record.key[record.key.len - 16 ..], record.key });
                    try sort_.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = key })}, .values = &.{}, .ordinal = self.ordinal });
                }
                self.ordinal += 1;
                return record;
            }
        };
        // Masking the row suffix and grouping the tuple prefix preserve the
        // forward stream's order. Avoid sorting these derived streams again.
        predicates.sort.presorted = true;
        tie_builder.sort.presorted = true;
        var collect: Collect = .{ .merge = &merge, .reverse_sort = if (delta.reverse_built or delta.run_only) null else &reverse_sort, .predicates = &predicates, .ties = &tie_builder, .arena = .init(a), .run_only = delta.run_only };
        defer collect.arena.deinit();
        page = try tree.buildSortedWithOptions(a, pages.store(), &collect, .{ .target_bytes = tree.bulk_target_page_bytes });
        ties = if (delta.run_only) null else try tie_builder.finish(pages.store());
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
        reverse = if (delta.run_only) null else if (delta.reverse_built) delta.built_reverse else try tree.buildSortedWithOptions(a, reverse_storage, &reverse_source, .{ .target_bytes = tree.bulk_target_page_bytes });
        if (!delta.run_only and delta.reverse_built and (page != null) != (reverse != null)) return error.InvalidNativeLakeRowIndex;
    }
    const predicate_root = if (delta.run_only) null else try predicates.publish(pages.store(), if (has_delta) delta.previous.?.predicates else null);
    const files: [][]const u8 = if (delta.files.len == 0) try a.alloc([]const u8, inventory.files.len) else @constCast(&.{});
    defer a.free(files);
    for (files, if (delta.files.len == 0) inventory.files else &.{}) |*file, entry| file.* = entry.file_id;
    var dictionary: ?file_dictionary.Roots = null;
    if (delta.dictionary_slots) |slots| {
        const previous = delta.dictionary_previous orelse delta.previous orelse return error.InvalidNativeLakeRowIndex;
        dictionary = try file_dictionary.updateFingerprints(a, pages.store(), previous.file_dictionary orelse return error.InvalidNativeLakeRowIndex, delta.files, delta.fingerprints, slots);
    }
    const root: Root = .{ .fingerprint = fingerprint, .domain = scope.domain, .page = page, .reverse = reverse, .ties = ties, .tie_version = if (delta.run_only) 0 else 1, .building = delta.run_only, .predicates = predicate_root, .source = inventory.source_id, .snapshot = inventory.snapshot_id, .files = if (delta.files.len != 0) delta.files else files, .file_fingerprints = delta.fingerprints, .public_navigation = delta.public_navigation, .cover = cover };
    return persistRoot(a, result_alloc, store, root, name, cancellation, delta.dictionary_previous orelse delta.previous, !delta.run_only, sort.manager, dictionary);
}
fn persistRoot(a: A, result_alloc: A, store: *stores.ArtifactStore, root: Root, name: []const u8, cancellation: Cancellation, previous_root: ?Root, register_closure: bool, manager: ?*local.sql_spill.Manager, ready_dictionary: ?file_dictionary.Roots) !Ref {
    var persisted = root;
    persisted.version = if (root.building) checkpoint_metadata_version else metadata_version;
    if (ready_dictionary) |ready| {
        persisted.file_dictionary = ready;
    } else if (root.files.len != 0 or root.file_dictionary == null) {
        var reads: u64 = 256 * 1024 * 1024;
        var writes: u64 = 512 * 1024 * 1024;
        const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
        var dictionary_pages: page_store.PageStore = .{ .domain = root.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
        const prior: ?file_dictionary.Previous = if (previous_root) |old| if (old.file_dictionary) |dictionary| .{ .roots = dictionary, .files = old.files, .fingerprints = old.file_fingerprints, .same_snapshot = std.mem.eql(u8, old.source, root.source) and std.mem.eql(u8, old.snapshot, root.snapshot) } else null else null;
        persisted.file_dictionary = try file_dictionary.publishWithOptions(a, dictionary_pages.store(), root.source, root.snapshot, root.files, root.file_fingerprints, prior, .{ .shared = root.public_navigation, .manager = manager });
    }
    // Published dictionaries authenticate their own slot/name census. Do not
    // revalidate an archive-sized borrowed array at every per-file checkpoint.
    persisted.files = &.{};
    persisted.file_fingerprints = &.{};
    try persisted.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, persisted, .{});
    defer a.free(bytes);
    if (bytes.len > max_root_bytes) return error.LakeSidecarBuildLimitExceeded;
    var upload = store.*;
    upload.allocator = result_alloc;
    var ref = try upload.putWithCancellation(bytes, cancellation);
    // The durable map owns its selector copy; ArtifactRef transfers only the
    // public artifact identity, not this temporary provider response.
    const provider_version = ref.provider_version;
    ref.provider_version = null;
    defer if (provider_version) |version| result_alloc.free(version);
    errdefer ref.deinit(result_alloc);
    // Only authenticated previous closures may be inherited. Advisory backend
    // failures retain the full audit path, while cancellation remains fatal.
    if (register_closure) store.inheritClosure(ref.artifact_id, if (previous_root) |previous| (if (previous.retained_id.len != 0) previous.retained_id else null) else null, cancellation) catch {
        try cancellation.check();
    };
    return .{ .kind = .ordered_row_index, .name = try result_alloc.dupe(u8, name), .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len, .metadata_version = persisted.version };
}

/// Borrow only one head per durable run; advance a selected cursor on the next
/// call so returned key/cover bytes remain pinned through page construction.
const RunSource = struct {
    manager: *local.sql_spill.Manager,
    cursors: [17]?tree.Cursor = @splat(null),
    heads: [17]?tree.Cursor.Record = @splat(null),
    pending: ?usize = null,
    fn init(a: A, store: tree.Store, root: Root, manager: *local.sql_spill.Manager) !RunSource {
        try manager.check();
        var self: RunSource = .{ .manager = manager };
        errdefer self.deinit();
        if (root.page) |page| self.cursors[0] = try tree.Cursor.init(a, store, page, "", null);
        for (root.build_runs, 1..) |run, index| {
            try manager.check();
            self.cursors[index] = try tree.Cursor.init(a, store, run.page, "", null);
        }
        for (&self.cursors, &self.heads) |*cursor, *head| if (cursor.*) |*open| {
            head.* = try open.next();
        };
        return self;
    }
    pub fn next(self: *RunSource) !?tree.Cursor.Record {
        try self.manager.check();
        if (self.pending) |index| self.heads[index] = try self.cursors[index].?.next();
        self.pending = null;
        var selected: ?usize = null;
        for (self.heads, 0..) |head, index| if (head) |record| {
            if (selected == null or std.mem.order(u8, record.key, self.heads[selected.?].?.key) == .lt) selected = index;
        };
        const index = selected orelse return null;
        self.pending = index;
        return self.heads[index];
    }
    fn deinit(self: *RunSource) void {
        for (&self.cursors) |*cursor| if (cursor.*) |*open| {
            open.deinit();
        };
    }
};
/// Each file first becomes an authenticated durable sorted run. Equal levels
/// compact once, keeping at most log2(files) roots and avoiding prefix rebuilds.
/// Final publication consumes this forest through the normal streaming builder.
pub fn publishRunCheckpoint(a: A, out: A, store: *stores.ArtifactStore, sort: *local.sql_spill.Sort, name: []const u8, fingerprint: [32]u8, inventory: local.serverless_external_source_types.Inventory, cover: []const []const u8, cancellation: Cancellation, delta: Delta) !Ref {
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    if (delta.previous) |previous| {
        if (!previous.building or delta.keep.len != previous.fileCount() or (previous.file_fingerprints.len != 0 and previous.file_fingerprints.len != previous.files.len) or !std.mem.eql(u8, &previous.fingerprint, &fingerprint) or !std.mem.eql(u8, &previous.domain, &scope.domain)) return error.InvalidNativeLakeRowIndex;
        // A changed immutable identity retires old coordinates through the
        // ordinary filtered merge, never appends a conflicting run.
        var reads: u64 = 16 * 1024 * 1024;
        var writes: u64 = 0;
        var pages: page_store.PageStore = .{ .domain = scope.domain, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
        var slot_arena = std.heap.ArenaAllocator.init(a);
        defer slot_arena.deinit();
        const slots = delta.dictionary_slots;
        const n = if (slots) |selected| selected.len else delta.keep.len;
        for (0..n) |i| {
            const slot = if (slots) |selected| selected[i] else i;
            if (slot >= delta.keep.len) return error.InvalidNativeLakeRowIndex;
            if (delta.keep[slot]) continue;
            _ = slot_arena.reset(.retain_capacity);
            const digest = if (previous.file_dictionary) |dictionary|
                (try file_dictionary.file(slot_arena.allocator(), pages.store(), dictionary, @intCast(slot))).fingerprint
            else
                previous.file_fingerprints[slot];
            if (!std.mem.allEqual(u8, &digest, 0)) return publishIncremental(a, out, store, sort, name, fingerprint, inventory, cover, cancellation, delta);
        }
    }
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const ca = scratch.allocator();
    const fresh_ref = try publishIncremental(a, ca, store, sort, name, fingerprint, inventory, cover, cancellation, .{ .dictionary_previous = delta.previous, .dictionary_slots = delta.dictionary_slots, .run_only = delta.previous != null, .public_navigation = delta.public_navigation, .files = delta.files, .fingerprints = delta.fingerprints, .reverse_built = delta.reverse_built, .built_reverse = delta.built_reverse });
    var fresh = try loadRootLazy(ca, store.*, fresh_ref, cancellation, null);
    var runs: std.ArrayList(Root.Run) = .empty;
    var incoming: ?tree.Ref = fresh.page;
    var level: u8 = 0;
    if (delta.previous) |previous| {
        if (previous.page) |page| try runs.append(ca, .{ .page = page, .level = previous.build_level });
        try runs.appendSlice(ca, previous.build_runs);
    }
    var reads: u64 = 256 * 1024 * 1024;
    var writes: u64 = 512 * 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    while (incoming) |page| {
        const match = for (runs.items, 0..) |run, index| {
            if (run.level == level) break index;
        } else null;
        const index = match orelse {
            try runs.append(ca, .{ .page = page, .level = level });
            break;
        };
        const prior = runs.swapRemove(index);
        var source = try RunSource.init(a, pages.store(), .{ .domain = scope.domain, .fingerprint = fingerprint, .source = inventory.source_id, .snapshot = inventory.snapshot_id, .files = delta.files, .page = page, .build_runs = &.{.{ .page = prior.page, .level = prior.level }} }, sort.manager);
        defer source.deinit();
        incoming = try tree.buildSorted(a, pages.store(), &source);
        level = std.math.add(u8, level, 1) catch return error.LakeSidecarBuildLimitExceeded;
        if (level > 16) return error.LakeSidecarBuildLimitExceeded;
    }
    fresh.page = if (runs.items.len == 0) null else runs.items[0].page;
    fresh.build_level = if (runs.items.len == 0) 0 else runs.items[0].level;
    fresh.build_runs = if (runs.items.len == 0) &.{} else runs.items[1..];
    fresh.building = true;
    fresh.public_navigation = delta.public_navigation;
    if (delta.previous != null) {
        fresh.reverse = null;
        fresh.ties = null;
        fresh.predicates = null;
        fresh.tie_version = 0;
    }
    return persistRoot(a, out, store, fresh, name, cancellation, delta.previous orelse fresh, true, sort.manager, fresh.file_dictionary);
}

/// A single-file checkpoint already owns all serving trees. Seal its metadata
/// directly; multi-run forests require one final streaming assembly.
pub fn finalizeRunCheckpoint(a: A, out: A, store: *stores.ArtifactStore, sort: *local.sql_spill.Sort, name: []const u8, fingerprint: [32]u8, inventory: local.serverless_external_source_types.Inventory, cover: []const []const u8, cancellation: Cancellation, delta: Delta) !Ref {
    if (delta.previous) |previous| {
        if (previous.building and previous.build_runs.len == 0 and previous.tie_version == 1 and sort.total == 0 and std.mem.allEqual(bool, delta.keep, true)) {
            var root = previous;
            root.building = false;
            root.public_navigation = delta.public_navigation;
            root.build_level = 0;
            return persistRoot(a, out, store, root, name, cancellation, previous, true, sort.manager, previous.file_dictionary);
        }
    }
    return publishIncremental(a, out, store, sort, name, fingerprint, inventory, cover, cancellation, delta);
}

fn applyChanges(a: A, store: tree.Store, root: ?tree.Ref, changes: []tree.Mutation) !?tree.Ref {
    std.mem.sort(tree.Mutation, changes, {}, struct {
        fn less(_: void, left: tree.Mutation, right: tree.Mutation) bool {
            return std.mem.order(u8, left.key, right.key) == .lt;
        }
    }.less);
    return tree.apply(a, store, root, changes);
}

pub fn loadRootLazy(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Root {
    if (ref.kind != .ordered_row_index or !supportsBuildMetadata(ref.metadata_version) or ref.byte_len > max_root_bytes) return error.InvalidNativeLakeRowIndex;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, cache);
    defer a.free(bytes);
    var root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    try root.validate();
    if (root.version != ref.metadata_version) return error.InvalidNativeLakeRowIndex;
    if (root.file_dictionary == null) try deriveFiles(a, &root);
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeRowIndex;
    if (!std.mem.eql(u8, &scope.domain, &root.domain)) return error.InvalidNativeLakeRowIndex;
    root.retained_id = try a.dupe(u8, ref.artifact_id);
    return root;
}
/// Full slot state is a maintenance operation, never a serving prerequisite.
pub fn loadRoot(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Root {
    var root = try loadRootLazy(a, store, ref, cancellation, cache);
    if (root.file_dictionary) |dictionary| {
        var reads: u64 = 256 * 1024 * 1024;
        var writes: u64 = 0;
        var backend = store;
        var pages: page_store.PageStore = .{ .domain = root.domain, .artifacts = &backend, .cancellation = cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
        const slots = try file_dictionary.materialize(a, pages.store(), dictionary);
        root.files = slots.files;
        root.file_fingerprints = slots.fingerprints;
        try deriveFiles(a, &root);
        try root.validate();
    }
    return root;
}
fn deriveFiles(a: A, root: *Root) !void {
    const digests = try a.alloc([32]u8, root.files.len);
    for (root.files, digests) |file, *digest| digest.* = local.storage_rowsource_identity.fileDigest(root.source, root.snapshot, file);
    root.public_digests = digests;
    const public_slots = try a.alloc(u32, root.files.len);
    for (public_slots, 0..) |*slot, i| slot.* = @intCast(i);
    std.mem.sort(u32, public_slots, digests, struct {
        fn less(values: []const [32]u8, x: u32, y: u32) bool {
            return std.mem.order(u8, &values[x], &values[y]) == .lt;
        }
    }.less);
    root.public_slots = public_slots;
    const slots = try a.alloc(Root.FileSlot, root.files.len);
    for (root.files, slots, 0..) |file, *entry, slot| entry.* = .{ .file = file, .slot = @intCast(slot) };
    std.mem.sort(Root.FileSlot, slots, {}, struct {
        fn less(_: void, left: Root.FileSlot, right: Root.FileSlot) bool {
            return std.mem.order(u8, left.file, right.file) == .lt;
        }
    }.less);
    root.file_slots = slots;
}

/// Prove the complete retained dependency graph before trusting file fingerprints.
/// Copy-on-write updates can keep pages without reading them, and covering data
/// lives outside the tree. Bound individual reads and the deduplication cache,
/// not the total index size. Lease renewal and cancellation bound execution.
pub fn validateReuse(a: A, backend: stores.ArtifactStore, root: Root, context: local.serverless_query_lake_read_context.Context, cancellation: Cancellation) !void {
    var store = backend;
    const certified = root.retained_id.len != 0 and (store.retainedClosure(root.retained_id, cancellation) catch false);
    try context.ensureActive();
    try cancellation.check();
    if (certified) return;
    return validateReuseWithCache(a, backend, root, context, cancellation, 4096);
}
fn validateReuseWithCache(a: A, backend: stores.ArtifactStore, root: Root, context: local.serverless_query_lake_read_context.Context, cancellation: Cancellation, cache_entries: usize) !void {
    if (cache_entries == 0) return error.InvalidNativeLakeRowIndex;
    const Validator = struct {
        a: A,
        store: stores.ArtifactStore,
        domain: [32]u8,
        context: local.serverless_query_lake_read_context.Context,
        cancellation: Cancellation,
        covers: std.AutoHashMapUnmanaged([52]u8, void) = .empty,
        witnesses: std.AutoHashMapUnmanaged(stores.UploadScope, stores.RetentionWitness) = .empty,
        certifiable: bool = true,
        check_covers: bool = false,
        cache_entries: usize,
        remaining_reads: u64 = tree.max_page_bytes,
        fn retain(v: *@This(), scope: stores.UploadScope) !void {
            if (!v.certifiable or v.witnesses.contains(scope)) return;
            if (v.witnesses.count() == 4096) {
                v.certifiable = false;
                return;
            }
            const proof = v.store.retentionWitness(scope, v.cancellation) catch {
                v.certifiable = false;
                try v.check();
                return;
            };
            const witness = proof orelse {
                v.certifiable = false;
                return;
            };
            try v.witnesses.put(v.a, scope, witness);
        }
        fn retainArtifact(v: *@This(), id: []const u8) !void {
            if (try stores.uploadScopeFromArtifactId(id)) |scope| try v.retain(scope) else v.certifiable = false;
        }
        fn certify(v: *@This(), id: []const u8) !void {
            if (!v.certifiable or id.len == 0) return;
            try v.check();
            const witnesses = try v.a.alloc(stores.RetentionWitness, v.witnesses.count());
            defer v.a.free(witnesses);
            var it = v.witnesses.valueIterator();
            for (witnesses) |*entry| entry.* = it.next().?.*;
            v.store.rememberClosure(id, witnesses, v.cancellation) catch {
                try v.check();
            };
        }
        fn check(v: *@This()) !void {
            try v.context.ensureActive();
            try v.cancellation.check();
        }
        pub fn skip(v: *@This(), ref: tree.Ref) !bool {
            try v.check();
            try v.retain(.{ .domain = v.domain, .attempt = ref.attempt });
            // Each authenticated page is at most max_page_bytes and child
            // heights strictly decrease. No archive-wide read quota is needed.
            v.remaining_reads = tree.max_page_bytes;
            return false;
        }
        pub fn visit(_: *@This(), _: tree.Ref) !void {}
        pub fn visitRecord(v: *@This(), _: tree.Ref, _: []const u8, value: []const u8) !void {
            if (!v.check_covers) return;
            try v.context.ensureActive();
            try v.cancellation.check();
            const cover = try coverReference(v.a, v.domain, value) orelse return;
            defer v.a.free(cover.block.artifact_id);
            try v.retainArtifact(cover.block.artifact_id);
            const identity = value[18..70].*;
            if (v.covers.contains(identity)) return;
            try v.check();
            var metadata = try v.store.statWithCancellationUsingAllocator(v.a, cover.block.artifact_id, v.cancellation);
            defer metadata.deinit(v.a);
            if (metadata.byte_len != cover.block.byte_len or !std.mem.eql(u8, metadata.artifact_id, cover.block.artifact_id) or !std.mem.eql(u8, metadata.checksum, cover.block.checksum)) return error.ArtifactIntegrityMismatch;
            if (v.covers.count() == v.cache_entries) v.covers.clearRetainingCapacity();
            try v.covers.put(v.a, identity, {});
        }
        fn pages(v: *@This(), ref: ?tree.Ref, check_covers: bool) !void {
            v.check_covers = check_covers;
            var writes: u64 = 0;
            var pages_store: page_store.PageStore = .{ .domain = v.domain, .artifacts = &v.store, .cancellation = v.cancellation, .remaining_read_bytes = &v.remaining_reads, .remaining_write_bytes = &writes };
            if (ref) |page| try tree.walkPostOrder(v.a, pages_store.store(), page, v, false);
        }
    };
    var validator: Validator = .{ .a = a, .store = backend, .domain = root.domain, .context = context, .cancellation = cancellation, .cache_entries = cache_entries };
    defer validator.covers.deinit(a);
    defer validator.witnesses.deinit(a);
    if (root.retained_id.len != 0) try validator.retainArtifact(root.retained_id);
    try validator.pages(root.page, true);
    for (root.build_runs) |run| try validator.pages(run.page, true);
    try validator.pages(root.reverse, false);
    try validator.pages(root.ties, false);
    try validator.pages(root.predicates, false);
    if (root.file_dictionary) |dictionary| {
        try validator.pages(dictionary.slots, false);
        try validator.pages(dictionary.names, false);
        try validator.pages(dictionary.public, false);
    }
    try validator.certify(root.retained_id);
}

/// Caller retains the leased root and store capability through cursor close.
/// A batch is owned by its caller, including all physical identity strings.
/// Authenticated immutable metadata owned by a decoded-cache lease. Readers
/// reuse structural validation, then check the request-specific fingerprint.
pub const VerifiedRoot = struct {
    value: Root,
    pub fn load(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation, cache: ?artifacts.CachedRead) !VerifiedRoot {
        const root = try loadRootLazy(a, store, ref, cancellation, cache);
        if (root.building) return error.InvalidNativeLakeRowIndex;
        return .{ .value = root };
    }
};

pub const Reader = struct {
    physical_cursor: ?tree.Cursor = null,
    membership_arena: ?std.heap.ArenaAllocator = null,
    membership_reads: u64 = 8 * 1024 * 1024,
    range_reads: u64 = 256 * 1024 * 1024,
    range_pages: page_store.PageStore = undefined,
    range_cache: ?tree.Cache = null,
    dictionary_cache: ?tree.Cache = null,
    // A small decoded-name memo avoids reparsing the same slot for every row.
    // Output strings are copied into the caller, independently of eviction.
    file_names: [32]?struct { slot: u32, name: []u8, age: u64 } = @splat(null),
    file_name_tick: u64 = 0,
    file_name_misses: usize = 0,
    predicate_cursor: ?tree.Cursor = null,
    root: Root,
    pages: page_store.PageStore,
    cursor: ?tree.Cursor = null,
    remaining_reads: u64 = 256 * 1024 * 1024,
    remaining_writes: u64 = 0,
    exhausted: bool = false,
    preserve_path: bool = false,
    window_upper: ?[]const u8 = null,
    cached: ?artifacts.CachedRead = null,
    pub fn init(self: *Reader, a: A, store: *stores.ArtifactStore, root: Root, fingerprint: [32]u8, lower: []const u8, upper: ?[]const u8, cancellation: Cancellation) !void {
        return self.initCached(a, store, root, fingerprint, lower, upper, cancellation, null);
    }
    pub fn initCached(self: *Reader, a: A, store: *stores.ArtifactStore, root: Root, fingerprint: [32]u8, lower: []const u8, upper: ?[]const u8, cancellation: Cancellation, cached: ?artifacts.CachedRead) !void {
        try root.validate();
        return self.initVerified(a, store, .{ .value = root }, fingerprint, lower, upper, cancellation, cached);
    }
    pub fn initVerified(self: *Reader, a: A, store: *stores.ArtifactStore, verified: VerifiedRoot, fingerprint: [32]u8, lower: []const u8, upper: ?[]const u8, cancellation: Cancellation, cached: ?artifacts.CachedRead) !void {
        const root = verified.value;
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
        for (&self.file_names) |*entry| if (entry.*) |held| {
            self.cursor.?.alloc.free(held.name);
            entry.* = null;
        };
        if (self.dictionary_cache) |*cache| cache.deinit();
        self.dictionary_cache = null;
        if (self.range_cache) |*cache| cache.deinit();
        self.range_cache = null;
        if (self.membership_arena) |*arena| arena.deinit();
        self.membership_arena = null;
        if (self.cursor) |*cursor| cursor.deinit();
        self.cursor = null;
        if (self.physical_cursor) |*cursor| cursor.deinit();
        self.physical_cursor = null;
        if (self.predicate_cursor) |*cursor| cursor.deinit();
        self.predicate_cursor = null;
    }
    pub fn dictionaryStore(self: *Reader) tree.Store {
        if (self.dictionary_cache == null) self.dictionary_cache = .{ .alloc = self.cursor.?.alloc, .underlying = self.pages.store(), .max_bytes = 1024 * 1024 };
        return self.dictionary_cache.?.store();
    }
    pub fn fileAtAlloc(self: *Reader, a: A, slot: u32) ![]const u8 {
        if (slot >= self.root.fileCount()) return error.InvalidNativeLakeRowIndex;
        if (self.root.files.len != 0) return self.root.files[slot];
        self.file_name_tick +%= 1;
        var target: usize = 0;
        for (&self.file_names, 0..) |*entry, i| {
            if (entry.*) |*held| {
                if (held.slot == slot) {
                    held.age = self.file_name_tick;
                    return a.dupe(u8, held.name);
                }
                if (self.file_names[target] != null and held.age < self.file_names[target].?.age) target = i;
            } else target = i;
        }
        self.file_name_misses += 1;
        var scratch = std.heap.ArenaAllocator.init(self.cursor.?.alloc);
        defer scratch.deinit();
        const record = try file_dictionary.file(scratch.allocator(), self.dictionaryStore(), self.root.file_dictionary orelse return error.InvalidNativeLakeRowIndex, slot);
        if (record.file.len <= 4096) {
            const owned = try self.cursor.?.alloc.dupe(u8, record.file);
            if (self.file_names[target]) |held| self.cursor.?.alloc.free(held.name);
            self.file_names[target] = .{ .slot = slot, .name = owned, .age = self.file_name_tick };
        }
        return a.dupe(u8, record.file);
    }
    pub fn fileSlot(self: *Reader, a: A, name: []const u8) !u32 {
        if (self.root.files.len == 0) if (self.root.file_dictionary) |dictionary| return (try file_dictionary.named(a, self.dictionaryStore(), dictionary, name)) orelse error.ExternalLakeSnapshotMismatch;
        var lo: usize = 0;
        var hi = self.root.file_slots.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, self.root.file_slots[mid].file, name) == .lt) lo = mid + 1 else hi = mid;
        }
        if (lo < self.root.file_slots.len and std.mem.eql(u8, self.root.file_slots[lo].file, name)) return self.root.file_slots[lo].slot;
        if (self.root.file_slots.len == 0) for (self.root.files, 0..) |file, i| if (std.mem.eql(u8, file, name)) {
            return @intCast(i);
        };
        return error.ExternalLakeSnapshotMismatch;
    }
    pub fn publicRecordSlot(self: *Reader, a: A, record: tree.Cursor.Record) !u32 {
        const dictionary = self.root.file_dictionary orelse return error.InvalidNativeLakeRowIndex;
        if (record.key.len != 32) return error.InvalidNativeLakeRowIndex;
        if (dictionary.public_names) return self.fileSlot(a, record.value);
        if (record.value.len != 4) return error.InvalidNativeLakeRowIndex;
        const slot = std.mem.readInt(u32, record.value[0..4], .big);
        if (slot >= dictionary.count) return error.InvalidNativeLakeRowIndex;
        return slot;
    }
    pub fn publicSlot(self: *Reader, a: A, digest: [32]u8) !?u32 {
        if (self.root.file_dictionary) |dictionary| return file_dictionary.publicSlot(a, self.dictionaryStore(), dictionary, digest);
        var lo: usize = 0;
        var hi = self.root.public_slots.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const slot = self.root.public_slots[mid];
            switch (std.mem.order(u8, &self.root.public_digests[slot], &digest)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return slot,
            }
        }
        return null;
    }
    pub fn identityVersion(self: *const Reader) u8 {
        return if (self.root.file_dictionary) |dictionary| dictionary.public_identity_version else 1;
    }
    pub fn publicDigest(self: *Reader, a: A, slot: u32) ![32]u8 {
        if (self.root.public_digests.len != 0) {
            if (slot >= self.root.public_digests.len) return error.InvalidNativeLakeRowIndex;
            return self.root.public_digests[slot];
        }
        const file = try self.fileAtAlloc(a, slot);
        return local.storage_rowsource_identity.navigationDigest(self.identityVersion(), self.root.source, self.root.snapshot, file);
    }
    pub fn nextPredicateBlocks(self: *Reader, a: A, lower: []const u8, upper: ?[]const u8) ![]const predicate_blocks.Block {
        if (self.exhausted) return &.{};
        if (self.predicate_cursor == null) self.predicate_cursor = try tree.Cursor.init(self.cursor.?.alloc, self.pages.store(), self.root.predicates, lower, upper);
        var blocks: std.ArrayList(predicate_blocks.Block) = .empty;
        while (blocks.items.len < 16) {
            const record = try self.predicate_cursor.?.next() orelse break;
            if (record.key.len < 16) return error.InvalidNativeLakeRowIndex;
            const physical = record.key[record.key.len - 16 ..];
            const file = std.mem.readInt(u32, physical[0..4], .big);
            const base = std.mem.readInt(u64, physical[8..16], .big);
            if (file >= self.root.fileCount() or base & ((1 << predicate_blocks.shift) - 1) != 0) return error.InvalidNativeLakeRowIndex;
            try blocks.append(a, .{ .file = try self.fileAtAlloc(a, file), .group = std.mem.readInt(u32, physical[4..8], .big), .base = base, .selection = try predicate_blocks.decode(a, record.value) });
        }
        return blocks.toOwnedSlice(a);
    }
    pub fn canProbeMembership(self: *const Reader) bool {
        const reverse = self.root.reverse orelse return false;
        // A lower bound beyond a leaf can visit its next sibling path.
        return self.membership_reads >= 2 * (@as(u64, reverse.height) + 1) * tree.max_page_bytes;
    }
    /// Probe one physical coordinate in the authenticated reverse tree. Scratch
    /// retains only its maximum root-to-leaf path across repeated probes.
    pub fn containsPhysical(self: *Reader, ref: rows.RowRef, lower: []const u8, upper: ?[]const u8) !bool {
        if (ref != .external) return error.InvalidNativeLakeRowIndex;
        const row = ref.external;
        if (!std.mem.eql(u8, row.source_id, self.root.source) or !std.mem.eql(u8, row.snapshot_id, self.root.snapshot)) return error.ExternalLakeSnapshotMismatch;
        var lookup_scratch = std.heap.ArenaAllocator.init(self.cursor.?.alloc);
        defer lookup_scratch.deinit();
        const slot = try self.fileSlot(lookup_scratch.allocator(), row.file_id);
        const physical = coordinate(slot, row);
        if (self.membership_arena == null) self.membership_arena = .init(self.cursor.?.alloc);
        _ = self.membership_arena.?.reset(.retain_capacity);
        var pages = self.pages;
        pages.remaining_read_bytes = &self.membership_reads;
        var cursor = try tree.Cursor.init(self.membership_arena.?.allocator(), pages.store(), self.root.reverse orelse return error.InvalidNativeLakeRowIndex, &physical, null);
        defer cursor.deinit();
        const record = try cursor.next() orelse return false;
        if (!std.mem.startsWith(u8, record.key, &physical)) return false;
        if (record.key.len < 32 or !std.mem.eql(u8, record.key[record.key.len - 16 ..], &physical)) return error.InvalidNativeLakeRowIndex;
        const key = record.key[16..];
        return std.mem.order(u8, key, lower) != .lt and (upper == null or std.mem.order(u8, key, upper.?) == .lt);
    }
    pub fn intersectsPhysical(self: *Reader, first: rows.RowRef, last: rows.RowRef, lower: []const u8, upper: ?[]const u8) !?bool {
        if (first != .external or last != .external or !std.mem.eql(u8, first.external.file_id, last.external.file_id)) return error.InvalidNativeLakeRowIndex;
        const row = first.external;
        if (!std.mem.eql(u8, row.source_id, self.root.source) or !std.mem.eql(u8, row.snapshot_id, self.root.snapshot) or
            !std.mem.eql(u8, last.external.source_id, self.root.source) or !std.mem.eql(u8, last.external.snapshot_id, self.root.snapshot)) return error.ExternalLakeSnapshotMismatch;
        if (self.root.reverse == null or self.range_reads == 0) return null;
        var lookup_scratch = std.heap.ArenaAllocator.init(self.cursor.?.alloc);
        defer lookup_scratch.deinit();
        const selected = try self.fileSlot(lookup_scratch.allocator(), row.file_id);
        if (self.membership_arena == null) self.membership_arena = .init(self.cursor.?.alloc);
        _ = self.membership_arena.?.reset(.retain_capacity);
        if (self.range_cache == null) {
            self.range_pages = self.pages;
            self.range_pages.remaining_read_bytes = &self.range_reads;
            self.range_cache = .{ .alloc = self.cursor.?.alloc, .underlying = self.range_pages.store(), .max_bytes = 1024 * 1024 };
        }
        return @import("lake_index_predicate_ranges.zig").intersects(self.membership_arena.?.allocator(), self.range_cache.?.store(), self.root.reverse, coordinate(selected, row), coordinate(selected, last.external), lower, upper);
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
            try refs.append(a, try self.decode(a, .{ .key = key, .value = record.key[0..16] }));
        }
        return refs.toOwnedSlice(a);
    }
    pub const Entry = struct { key: []const u8, ref: rows.RowRef, cover: ?Cover = null };
    pub const Cover = struct { block: artifacts.ChunkRef, row: u16 };
    fn nextRecord(self: *Reader) !?tree.Cursor.Record {
        return if (self.preserve_path) self.cursor.?.nextWindow(self.window_upper) else self.cursor.?.next();
    }
    pub fn nextEntries(self: *Reader, a: A, max_rows: usize) ![]const Entry {
        if (max_rows == 0 or max_rows > 65536) return error.InvalidNativeLakeRowIndex;
        var entries: std.ArrayList(Entry) = .empty;
        errdefer entries.deinit(a);
        while (!self.exhausted and entries.items.len < max_rows) {
            const record = try self.nextRecord() orelse {
                self.exhausted = true;
                break;
            };
            try entries.append(a, .{ .key = try a.dupe(u8, record.key), .ref = try self.decode(a, record), .cover = try coverReference(a, self.root.domain, record.value) });
        }
        return entries.toOwnedSlice(a);
    }
    pub fn decode(self: *Reader, a: A, record: tree.Cursor.Record) !rows.RowRef {
        if ((record.value.len != 16 and record.value.len != 70) or record.key.len < 16 or !std.mem.eql(u8, record.value[0..16], record.key[record.key.len - 16 ..])) return error.InvalidNativeLakeRowIndex;
        const physical: *const [16]u8 = @ptrCast(record.value.ptr);
        const file = std.mem.readInt(u32, physical[0..4], .big);
        if (file >= self.root.fileCount()) return error.InvalidNativeLakeRowIndex;
        return .{ .external = .{ .source_id = self.root.source, .snapshot_id = self.root.snapshot, .identity_version = self.identityVersion(), .file_id = try self.fileAtAlloc(a, file), .row_group_ordinal = std.mem.readInt(u32, physical[4..8], .big), .row_ordinal = std.mem.readInt(u64, physical[8..16], .big) } };
    }
    pub fn next(self: *Reader, a: A, max_rows: usize) ![]const rows.RowRef {
        if (max_rows == 0 or max_rows > 65536) return error.InvalidNativeLakeRowIndex;
        var refs: std.ArrayList(rows.RowRef) = .empty;
        errdefer refs.deinit(a);
        while (!self.exhausted and refs.items.len < max_rows) {
            const record = try self.nextRecord() orelse {
                self.exhausted = true;
                break;
            };
            try refs.append(a, try self.decode(a, record));
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
        .{ .file_id = @constCast("a"), .object_uri = @constCast("file://a"), .byte_len = 1, .row_count = 16384, .row_groups = &.{} },
        .{ .file_id = @constCast("b"), .object_uri = @constCast("file://b"), .byte_len = 1, .row_count = 1, .row_groups = &.{} },
    };
    var inventory: local.serverless_external_source_types.Inventory = .{ .format = .parquet, .source_id = @constCast("lake"), .source_uri = @constCast("file://lake"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = @constCast(files[0..1]) };
    var first = local.sql_spill.Sort.init(a, &manager, &.{.{}}, tree.bulk_target_page_bytes * 4);
    defer first.deinit();
    for (0..16384) |n| try Make.add(&first, @intCast(n), 0, n);
    const ref = try publish(a, ca, &store, &first, "ordered", @splat(3), inventory, &.{}, .none);
    const root = try loadRoot(ca, store, ref, .none, null);
    {
        const lazy = try loadRootLazy(ca, store, ref, .none, null);
        try std.testing.expectEqual(@as(usize, 0), lazy.files.len);
        try std.testing.expectEqual(@as(usize, 1), lazy.fileCount());
        var selective: Reader = undefined;
        try selective.init(a, &store, lazy, @splat(3), "", null, .none);
        defer selective.deinit();
        const first_rows = try selective.next(ca, 2);
        try std.testing.expectEqual(@as(usize, 2), first_rows.len);
        for (first_rows) |row| try std.testing.expectEqualStrings("a", row.external.file_id);
        try std.testing.expectEqual(@as(usize, 1), selective.file_name_misses);
    }
    // Physical reverse traversal applies tuple bounds without hydration.
    var predicate_reader: Reader = undefined;
    var lower_key: [4]u8 = undefined;
    var upper_key: [4]u8 = undefined;
    std.mem.writeInt(u32, &lower_key, 100, .big);
    std.mem.writeInt(u32, &upper_key, 103, .big);
    try predicate_reader.init(a, &store, root, @splat(3), &lower_key, &upper_key, .none);
    defer predicate_reader.deinit();
    const Probe = struct {
        fn row(n: u64) rows.RowRef {
            return .{ .external = .{ .source_id = "lake", .snapshot_id = "snapshot", .file_id = "a", .row_group_ordinal = 0, .row_ordinal = n } };
        }
    };
    for ([_]u64{ 102, 99, 100, 103, 101, 16384 }) |n| try std.testing.expectEqual(n >= 100 and n < 103, try predicate_reader.containsPhysical(Probe.row(n), &lower_key, &upper_key));
    var wrong = Probe.row(100);
    wrong.external.snapshot_id = "changed";
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, predicate_reader.containsPhysical(wrong, &lower_key, &upper_key));
    var rejected: Reader = undefined;
    try std.testing.expectError(error.ExternalLakeIndexUnavailable, rejected.initVerified(a, &store, .{ .value = root }, @splat(9), &lower_key, &upper_key, .none, null));
    const materialization_reads = predicate_reader.remaining_reads;
    try std.testing.expect(try predicate_reader.containsPhysical(Probe.row(101), &lower_key, &upper_key));
    try std.testing.expectEqual(materialization_reads, predicate_reader.remaining_reads);
    try std.testing.expectEqual(@as(?bool, true), try predicate_reader.intersectsPhysical(Probe.row(90), Probe.row(110), &lower_key, &upper_key));
    try std.testing.expectEqual(@as(?bool, false), try predicate_reader.intersectsPhysical(Probe.row(500), Probe.row(16000), &lower_key, &upper_key));
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, predicate_reader.intersectsPhysical(Probe.row(90), wrong, &lower_key, &upper_key));
    predicate_reader.membership_reads = 0;
    predicate_reader.range_reads = 0;
    try std.testing.expect((try predicate_reader.intersectsPhysical(Probe.row(90), Probe.row(110), &lower_key, &upper_key)) == null);
    try std.testing.expect(!predicate_reader.canProbeMembership());
    const physical_matches = try predicate_reader.nextPhysical(ca, 1024, &lower_key, &upper_key);
    try std.testing.expectEqual(@as(usize, 3), physical_matches.len);
    for (physical_matches, 100..) |row, expected_row| try std.testing.expectEqual(@as(u64, @intCast(expected_row)), row.external.row_ordinal);
    try std.testing.expectEqual(@as(usize, 0), (try predicate_reader.nextPhysical(ca, 1024, &lower_key, &upper_key)).len);
    const compressed_matches = try predicate_reader.nextPredicateBlocks(ca, &lower_key, &upper_key);
    try std.testing.expectEqual(@as(usize, 3), compressed_matches.len);
    for (compressed_matches, 100..) |block, expected_row| {
        try std.testing.expectEqual(@as(u64, 0), block.base);
        try std.testing.expectEqual(@as(u32, @intCast(expected_row)), block.selection.interval.lower);
        try std.testing.expectEqual(@as(u32, 1), block.selection.interval.count);
    }
    try std.testing.expectEqual(@as(usize, 0), (try predicate_reader.nextPredicateBlocks(ca, &lower_key, &upper_key)).len);
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
    try Make.add(&append, 20000, 1, 0);
    const appended = try publishIncremental(a, ca, &store, &append, "ordered", @splat(3), inventory, &.{}, .none, .{ .previous = root, .keep = &.{true}, .files = &.{ "a", "b" }, .fingerprints = &.{ @splat(1), @splat(2) } });
    const next = try loadRoot(ca, store, appended, .none, null);
    try std.testing.expectEqual(@as(u64, 16385), next.page.?.records);
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
    try Make.add(&replacement, 30000, 0, 0);
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
    var final_predicates: Reader = undefined;
    try final_predicates.init(a, &store, final, @splat(3), "", null, .none);
    defer final_predicates.deinit();
    const final_blocks = try final_predicates.nextPredicateBlocks(ca, "", null);
    try std.testing.expectEqual(@as(usize, 2), final_blocks.len);
    try std.testing.expectEqualStrings("b", final_blocks[0].file);
    try std.testing.expectEqualStrings("a", final_blocks[1].file);
    for (final_blocks) |block| try std.testing.expectEqual(@as(u32, 1), block.selection.interval.count);
    var original: Reader = undefined;
    try original.init(a, &store, root, @splat(3), "", null, .none);
    defer original.deinit();
    try std.testing.expectEqual(@as(usize, 10), (try original.next(ca, 10)).len);
}

/// Count page bytes in each mutation batch, including ancestor rewrites.
/// Ordered seeks keep planning memory bounded by one path. Widely scattered
/// inserts cross over earlier than clustered inserts of the same cardinality.
fn preferPageCopyOnWrite(a: A, store: tree.Store, root: ?tree.Ref, sort: *local.sql_spill.Sort, removed: u64) !bool {
    const page = root orelse return true;
    if (page.records < 1024) return preferCopyOnWrite(page.records, sort.total, removed);
    var sample = try tree.Cursor.init(a, store, page, "", null);
    defer sample.deinit();
    var sample_bytes: u64 = 0;
    var samples: u64 = 0;
    while (samples < 64) {
        const record = try sample.next() orelse break;
        sample_bytes +|= record.key.len + record.value.len + 9;
        samples += 1;
    }
    const rebuild_bytes = if (page.height == 0) page.bytes else (page.records *| (sample_bytes / @max(1, samples))) *| 5 / 4;
    const threshold = @max(@as(u64, tree.target_page_bytes), rebuild_bytes / 2);
    // Reverse-file order does not imply tuple locality, so deletions use a
    // conservative path estimate instead of an optimistic row fraction.
    var touched: u64 = removed *| (@as(u64, page.height) + 1) *| tree.target_page_bytes;
    if (touched >= threshold) return false;
    var cursor = try tree.Cursor.init(a, store, page, "", null);
    defer cursor.deinit();
    var last: [tree.max_height + 1]?tree.Ref = @splat(null);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    try sort.rewind();
    var count: usize = 0;
    while (true) {
        _ = scratch.reset(.retain_capacity);
        const row = try sort.next(scratch.allocator()) orelse break;
        if (count % 256 == 0) @memset(&last, null);
        try cursor.seekForward(row.keys[0].value.string);
        for (cursor.frames[0..cursor.depth], 0..) |frame, depth| {
            const ref = frame.page.ref;
            if (last[depth] == null or !tree.Ref.eql(last[depth].?, ref)) touched +|= ref.bytes;
            last[depth] = ref;
        }
        count += 1;
        if (touched >= threshold) break;
    }
    try sort.rewind();
    return touched < threshold;
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

test "external lake public ordering seeks complete ties across files and directions" {
    const public = @import("lake_index_public_order.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("public-ordered-ties");
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
    const files: []const local.serverless_external_source_types.FileEntry = &.{
        .{ .file_id = @constCast("a"), .object_uri = @constCast("file://a"), .byte_len = 1, .row_count = 10, .row_groups = &.{} },
        .{ .file_id = @constCast("b"), .object_uri = @constCast("file://b"), .byte_len = 1, .row_count = 10, .row_groups = &.{} },
        .{ .file_id = @constCast("c"), .object_uri = @constCast("file://c"), .byte_len = 1, .row_count = 10, .row_groups = &.{} },
    };
    const inventory: local.serverless_external_source_types.Inventory = .{ .format = .parquet, .source_id = @constCast("lake"), .source_uri = @constCast("file://lake"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = @constCast(files) };
    const Expected = struct { key: [4]u8, id: []const u8, ref: rows.RowRef };
    for ([_]bool{ false, true }) |primary_desc| {
        var sort = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 64 * 1024);
        defer sort.deinit();
        const expected = try ca.alloc(Expected, 30);
        var position: usize = 0;
        for (0..2) |primary| for (files, 0..) |file, slot| for (0..5) |row| {
            var key: [20]u8 = undefined;
            std.mem.writeInt(u32, key[0..4], @intCast(if (primary_desc) 1 - primary else primary), .big);
            const ref: rows.RowRef = .{ .external = .{ .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .file_id = file.file_id, .row_group_ordinal = @intCast(row % 2), .row_ordinal = primary * 100 + row } };
            @memcpy(key[4..20], &coordinate(@intCast(slot), ref.external));
            try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &key })}, .values = &.{}, .ordinal = position });
            expected[position] = .{ .key = key[0..4].*, .ref = ref, .id = try local.storage_rowsource_identity.allocId(ca, ref) };
            position += 1;
        };
        const artifact = try publish(a, ca, &store, &sort, "ordered", @splat(3), inventory, &.{}, .none);
        const root = try loadRoot(ca, store, artifact, .none, null);
        try std.testing.expectEqual(@as(u64, 6), root.ties.?.records);
        var reader: Reader = undefined;
        try reader.init(a, &store, root, @splat(3), "", null, .none);
        defer reader.deinit();
        const Order = struct {
            descending: bool,
            fn less(self: @This(), x: Expected, y: Expected) bool {
                const relation = std.mem.order(u8, &x.key, &y.key);
                return if (relation != .eq) relation == .lt else std.mem.order(u8, x.id, y.id) == (if (self.descending) std.math.Order.gt else .lt);
            }
        };
        for ([_]bool{ false, true }) |id_desc| {
            std.mem.sort(Expected, expected, Order{ .descending = id_desc }, Order.less);
            var all = try public.Cursor.init(a, &reader, "", null, null, null, id_desc, false);
            defer all.deinit();
            var rank: usize = 0;
            while (true) {
                const page = try all.next(ca, 3);
                if (page.len == 0) break;
                for (page) |ref| {
                    try std.testing.expectEqualStrings(expected[rank].id, try local.storage_rowsource_identity.allocId(ca, ref));
                    rank += 1;
                }
            }
            try std.testing.expectEqual(expected.len, rank);
            for ([_]usize{ 0, 7, 14, 15, 29 }) |boundary| for ([_]bool{ false, true }) |before| {
                var cursor = try public.Cursor.init(a, &reader, "", null, &expected[boundary].key, expected[boundary].id, id_desc, before);
                defer cursor.deinit();
                var seen: usize = 0;
                while (true) {
                    const page = try cursor.next(ca, 2);
                    if (page.len == 0) break;
                    for (page) |ref| {
                        const ordinal = if (before) boundary - seen - 1 else boundary + seen + 1;
                        try std.testing.expectEqualStrings(expected[ordinal].id, try local.storage_rowsource_identity.allocId(ca, ref));
                        seen += 1;
                    }
                }
                try std.testing.expectEqual(if (before) boundary else expected.len - boundary - 1, seen);
            };
        }
    }
}

test "external lake warm tie pagination reuses scoped file order with bounded page reads" {
    const public = @import("lake_index_public_order.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("cached-public-ties");
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
    const files = try ca.alloc(local.serverless_external_source_types.FileEntry, 32);
    for (files, 0..) |*file, i| file.* = .{ .file_id = try std.fmt.allocPrint(ca, "file-{d}", .{i}), .object_uri = @constCast("file://fixture"), .byte_len = 1, .row_count = 2, .row_groups = &.{} };
    const inventory: local.serverless_external_source_types.Inventory = .{ .format = .parquet, .source_id = @constCast("lake"), .source_uri = @constCast("file://lake"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = files };
    var sort = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 512 * 1024);
    defer sort.deinit();
    const expected = try ca.alloc([]const u8, 64);
    const prefix: [2048]u8 = @splat(1);
    for (files, 0..) |file, slot| for (0..2) |row| {
        const ref: rows.RowRef = .{ .external = .{ .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .file_id = file.file_id, .row_group_ordinal = 0, .row_ordinal = row } };
        var key: [2064]u8 = undefined;
        @memcpy(key[0..2048], &prefix);
        @memcpy(key[2048..], &coordinate(@intCast(slot), ref.external));
        const position = slot * 2 + row;
        try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &key })}, .values = &.{}, .ordinal = position });
        expected[position] = try local.storage_rowsource_identity.allocId(ca, ref);
    };
    std.mem.sort([]const u8, expected, {}, struct {
        fn less(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.order(u8, x, y) == .lt;
        }
    }.less);
    const artifact = try publish(a, ca, &store, &sort, "ordered", @splat(3), inventory, &.{}, .none);
    const root = try loadRoot(ca, store, artifact, .none, null);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    cache.decoded.max_entries = 1;
    var cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{ .io = std.testing.io } };
    var reader: Reader = undefined;
    try reader.initCached(a, &store, root, @splat(3), "", null, .none, cached);
    defer reader.deinit();
    const cold_start = reader.remaining_reads;
    var first = try public.Cursor.init(a, &reader, "", null, null, null, false, false);
    defer first.deinit();
    const first_page = try first.next(ca, 3);
    for (first_page, expected[0..3]) |ref, id| try std.testing.expectEqualStrings(id, try local.storage_rowsource_identity.allocId(ca, ref));
    const cold_reads = cold_start - reader.remaining_reads;
    const hits_before = cache.decoded.hits;
    const warm_start = reader.remaining_reads;
    {
        var next = try public.Cursor.init(a, &reader, "", null, &prefix, expected[2], false, false);
        defer next.deinit();
        const page = try next.next(ca, 3);
        for (page, expected[3..6]) |ref, id| try std.testing.expectEqualStrings(id, try local.storage_rowsource_identity.allocId(ca, ref));
    }
    try std.testing.expect(cache.decoded.hits > hits_before);
    try std.testing.expect(warm_start - reader.remaining_reads < cold_reads);
    // A new authorization scope gets its own entry. Evicting the old entry
    // cannot invalidate files/rows already borrowed by an active cursor.
    cached.scope = @splat(2);
    reader.cached = cached;
    const hits_scoped = cache.decoded.hits;
    {
        var scoped = try public.Cursor.init(a, &reader, "", null, null, null, true, false);
        defer scoped.deinit();
        const page = try scoped.next(ca, 3);
        for (page, 0..) |ref, i| try std.testing.expectEqualStrings(expected[expected.len - 1 - i], try local.storage_rowsource_identity.allocId(ca, ref));
    }
    try std.testing.expectEqual(hits_scoped, cache.decoded.hits);
    // Identical tuple/count trees can belong to different stable slot histories.
    // Keep the inventory valid and supply the second index's independent layout.
    const other_names = try ca.alloc([]const u8, files.len);
    for (other_names, 0..) |*name, i| name.* = files[files.len - i - 1].file_id;
    try sort.rewind();
    const other_artifact = try publishIncremental(a, ca, &store, &sort, "other", @splat(3), inventory, &.{}, .none, .{ .files = other_names });
    const other_root = try loadRoot(ca, store, other_artifact, .none, null);
    try std.testing.expectEqualDeep(root.ties, other_root.ties);
    var other_reader: Reader = undefined;
    try other_reader.initCached(a, &store, other_root, @splat(3), "", null, .none, cached);
    defer other_reader.deinit();
    for ([_]bool{ false, true }) |descending| for ([_]bool{ false, true }) |before| {
        var other = try public.Cursor.init(a, &other_reader, "", null, &prefix, expected[31], descending, before);
        defer other.deinit();
        const page = try other.next(ca, 3);
        for (page, 0..) |ref, i| {
            const rank = if (descending != before) 30 - i else 32 + i;
            try std.testing.expectEqualStrings(expected[rank], try local.storage_rowsource_identity.allocId(ca, ref));
        }
    };
    const retained = try first.next(ca, 3);
    for (retained, expected[3..6]) |ref, id| try std.testing.expectEqualStrings(id, try local.storage_rowsource_identity.allocId(ca, ref));
    for ([_]bool{ false, true }) |descending| for ([_]bool{ false, true }) |before| {
        var cursor = try public.Cursor.init(a, &reader, "", null, &prefix, expected[31], descending, before);
        defer cursor.deinit();
        const page = try cursor.next(ca, 3);
        try std.testing.expectEqual(@as(usize, 3), page.len);
        for (page, 0..) |ref, i| {
            const rank = if (descending != before) 30 - i else 32 + i;
            try std.testing.expectEqualStrings(expected[rank], try local.storage_rowsource_identity.allocId(ca, ref));
        }
    };
}

test "external lake warm tie pagination distinct tuples retain sequential reads" {
    const public = @import("lake_index_public_order.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("review-distinct-public-ties");
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
    const count = 5000;
    const files = try ca.alloc(local.serverless_external_source_types.FileEntry, 128);
    for (files, 0..) |*file, i| file.* = .{ .file_id = if (i == 0) @constCast("file") else try std.fmt.allocPrint(ca, "empty-{d}", .{i}), .object_uri = @constCast("file://fixture"), .byte_len = 1, .row_count = if (i == 0) count else 0, .row_groups = &.{} };
    const inventory: local.serverless_external_source_types.Inventory = .{ .format = .parquet, .source_id = @constCast("lake"), .source_uri = @constCast("file://lake"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = files };
    var sort = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 512 * 1024);
    defer sort.deinit();
    for (0..count) |row| {
        const ref: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .file_id = "file", .row_group_ordinal = 0, .row_ordinal = row } };
        var key: [24]u8 = undefined;
        std.mem.writeInt(u64, key[0..8], row, .big);
        @memcpy(key[8..], &coordinate(0, ref.external));
        try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &key })}, .values = &.{}, .ordinal = row });
    }
    const artifact = try publish(a, ca, &store, &sort, "ordered", @splat(3), inventory, &.{}, .none);
    const root = try loadRoot(ca, store, artifact, .none, null);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: @import("lake_index_aggregate_artifact.zig").CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{ .io = std.testing.io } };
    for ([_]bool{ false, true }) |reverse| {
        var reader: Reader = undefined;
        try reader.initCached(a, &store, root, @splat(3), "", null, .none, cached);
        defer reader.deinit();
        var cursor = try public.Cursor.init(a, &reader, "", null, null, null, false, reverse);
        defer cursor.deinit();
        const before_reads = reader.remaining_reads;
        const page = try cursor.next(ca, count);
        try std.testing.expectEqual(@as(usize, count), page.len);
        try std.testing.expectEqual(@as(usize, 0), cursor.group_counts);
        try std.testing.expect(before_reads - reader.remaining_reads < 4 * 1024 * 1024);
        for (page, 0..) |ref, i| try std.testing.expectEqual(@as(u64, if (reverse) count - i - 1 else i), ref.external.row_ordinal);
    }
}

test "external lake ordered reuse streams beyond a page budget with bounded cover cache" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("ordered-reuse-streaming");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(5), .attempt = @splat(1) };
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var sort = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 8 * 1024 * 1024);
    defer sort.deinit();
    const key = try ca.alloc(u8, 96 * 1024);
    @memset(key, 'x');
    const payload = try ca.alloc(u8, 64 * 1024);
    @memset(payload, 'v');
    const count = 20;
    for (0..count) |n| {
        std.mem.writeInt(u32, key[0..4], @intCast(n), .big);
        @memcpy(key[key.len - 16 ..], &coordinate(0, .{ .source_id = "lake", .snapshot_id = "snapshot", .file_id = "a", .row_group_ordinal = 0, .row_ordinal = n }));
        payload[0] = @intCast(n);
        try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = key })}, .values = &.{local.sql_scalar.Datum.fromJson(.{ .string = payload })}, .ordinal = n });
    }
    const files: []const local.serverless_external_source_types.FileEntry = &.{.{ .file_id = @constCast("a"), .object_uri = @constCast("file://a"), .byte_len = 1, .row_count = count, .row_groups = &.{} }};
    const inventory: local.serverless_external_source_types.Inventory = .{ .format = .parquet, .source_id = @constCast("lake"), .source_uri = @constCast("file://lake"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = @constCast(files) };
    const ref = try publish(a, ca, &store, &sort, "ordered", @splat(3), inventory, &.{"value"}, .none);
    const root = try loadRoot(ca, store, ref, .none, null);
    try std.testing.expect(count * key.len > tree.max_page_bytes);
    try std.testing.expect(root.page.?.height > 0);
    // A one-entry cache must evict cover identities, without limiting the
    // number of blocks or total pages that a healthy root can retain.
    try validateReuseWithCache(a, store, root, .{}, .none, 1);
    const Deadline = struct {
        remaining: usize = 10,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.remaining == 0) return error.DeadlineExceeded;
            self.remaining -= 1;
        }
    };
    var deadline: Deadline = .{};
    try std.testing.expectError(error.DeadlineExceeded, validateReuseWithCache(a, store, root, .{ .checkpoint = .{ .ptr = &deadline, .check = Deadline.check } }, .none, 1));
    var reader: Reader = undefined;
    try reader.init(a, &store, root, root.fingerprint, "", null, .none);
    defer reader.deinit();
    const entries = try reader.nextEntries(ca, count);
    try std.testing.expectEqual(@as(usize, count), entries.len);
    try std.testing.expect(!std.mem.eql(u8, entries[0].cover.?.block.artifact_id, entries[count - 1].cover.?.block.artifact_id));
    try store.delete(entries[count - 1].cover.?.block.artifact_id);
    try std.testing.expectError(error.FileNotFound, validateReuseWithCache(a, store, root, .{}, .none, 1));
}

test "external lake ordered durable run checkpoints compact resume and certify final closure" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("ordered-run-checkpoints");
    defer directory.cleanup();
    const Fs = @import("../serverless/artifacts/fs_store.zig").FsStore;
    var fs = try Fs.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(4), .attempt = @splat(2) };
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check };
    defer manager.deinit();
    const count = 70; // More checkpoints than the advisory pending-proof bound.
    const files = try ca.alloc(local.serverless_external_source_types.FileEntry, count);
    const names = try ca.alloc([]const u8, count);
    const hashes = try ca.alloc([32]u8, count);
    const keep = try ca.alloc(bool, count);
    @memset(hashes, @splat(0));
    @memset(keep, true);
    for (files, names, 0..) |*file, *name, index| {
        name.* = try std.fmt.allocPrint(ca, "file-{d}", .{index});
        file.* = .{ .file_id = @constCast(name.*), .object_uri = @constCast("file://data"), .byte_len = 1, .row_count = 4, .row_groups = &.{} };
    }
    const inventory: local.serverless_external_source_types.Inventory = .{ .format = .parquet, .source_id = @constCast("lake"), .source_uri = @constCast("file://lake"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = files };
    var previous: ?Root = null;
    var final_ref: Ref = undefined;
    for (0..count) |file| {
        var sort = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 16 * 1024);
        defer sort.deinit();
        sort.encoded_bytes = true;
        for (0..4) |row| {
            var key: [24]u8 = undefined;
            std.mem.writeInt(u64, key[0..8], row * count + file, .big);
            @memcpy(key[8..24], &coordinate(@intCast(file), .{ .source_id = "lake", .snapshot_id = "snapshot", .file_id = names[file], .row_group_ordinal = 0, .row_ordinal = row }));
            try sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = &key })}, .values = &.{local.sql_scalar.Datum.json(.{ .integer = @intCast(row * count + file) })}, .ordinal = row });
        }
        hashes[file] = @splat(1);
        keep[file] = false;
        final_ref = try publishRunCheckpoint(a, ca, &store, &sort, "ordered", @splat(3), inventory, &.{"v"}, .none, .{ .previous = previous, .keep = if (previous == null) &.{} else keep, .files = names, .fingerprints = hashes, .dictionary_slots = if (previous != null) &.{@intCast(file)} else null });
        keep[file] = true;
        previous = try loadRootLazy(ca, store, final_ref, .none, null);
        try std.testing.expectEqual(@as(usize, 0), previous.?.files.len);
        try std.testing.expect(previous.?.building);
        try std.testing.expect(previous.?.build_runs.len <= 6);
        var records: u64 = if (previous.?.page) |page| page.records else 0;
        for (previous.?.build_runs) |run| {
            try std.testing.expectEqual(@as(u64, 4) << @as(u6, @intCast(run.level)), run.page.records);
            records += run.page.records;
        }
        try std.testing.expectEqual(@as(u64, (file + 1) * 4), records);
        if (file == 36) {
            var reopened = try Fs.init(a, directory.path());
            defer reopened.deinit();
            const loaded = try loadRoot(ca, reopened.artifactStore(), final_ref, .none, null);
            try validateReuse(a, reopened.artifactStore(), loaded, .{}, .none);
            try std.testing.expectError(error.InvalidNativeLakeRowIndex, VerifiedRoot.load(ca, reopened.artifactStore(), final_ref, .none, null));
        }
    }
    var empty = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 16 * 1024);
    defer empty.deinit();
    final_ref = try publishIncremental(a, ca, &store, &empty, "ordered", @splat(3), inventory, &.{"v"}, .none, .{ .previous = previous, .keep = keep, .files = names, .fingerprints = hashes, .dictionary_slots = &.{} });
    const root = try loadRoot(ca, store, final_ref, .none, null);
    try std.testing.expect(!root.building);
    try std.testing.expectEqual(@as(usize, 0), root.build_runs.len);
    try std.testing.expectEqual(@as(u64, count * 4), root.page.?.records);
    try std.testing.expectEqual(root.page.?.records, root.reverse.?.records);
    try store.sealClosures(.none);
    try std.testing.expect(try store.retainedClosure(final_ref.artifact_id, .none));
    var reader: Reader = undefined;
    try reader.init(a, &store, root, root.fingerprint, "", null, .none);
    defer reader.deinit();
    const entries = try reader.nextEntries(ca, count * 4);
    for (entries, 0..) |entry, ordinal| {
        try std.testing.expectEqual(@as(u64, ordinal), std.mem.readInt(u64, entry.key[0..8], .big));
        try std.testing.expectEqualStrings(names[ordinal % count], entry.ref.external.file_id);
        try std.testing.expectEqual(@as(u64, ordinal / count), entry.ref.external.row_ordinal);
        try std.testing.expect(entry.cover != null);
    }
    // The certificate survives backend reopening, then fails closed after an
    // old covering block is lost. A warm audit performs only its active check.
    var reopened = try Fs.init(a, directory.path());
    defer reopened.deinit();
    var fresh = reopened.artifactStore();
    const Deadline = struct {
        remaining: usize = 1,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.remaining == 0) return error.DeadlineExceeded;
            self.remaining -= 1;
        }
    };
    var deadline: Deadline = .{};
    try validateReuse(a, fresh, root, .{ .checkpoint = .{ .ptr = &deadline, .check = Deadline.check } }, .none);
    try fresh.delete(entries[0].cover.?.block.artifact_id);
    try std.testing.expect(!try fresh.retainedClosure(final_ref.artifact_id, .none));
    try std.testing.expectError(error.FileNotFound, validateReuse(a, fresh, root, .{}, .none));
}

test "external lake ordered copy strategy distinguishes clustered from scattered page fanout" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("ordered-page-cost");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(4), .attempt = @splat(1) };
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 64 * 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = @splat(4), .attempt = @splat(1), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const Source = struct {
        index: u64 = 0,
        key: [24]u8 = @splat(0),
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.index == 5000) return null;
            std.mem.writeInt(u64, self.key[0..8], self.index * 2, .big);
            self.index += 1;
            return .{ .key = &self.key, .value = "" };
        }
    };
    var source: Source = .{};
    const root = try tree.buildSorted(a, pages.store(), &source);
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check };
    defer manager.deinit();
    for ([_]bool{ false, true }) |scattered| {
        var sort = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 64 * 1024);
        defer sort.deinit();
        sort.encoded_bytes = true;
        for (0..20) |n| {
            var key: [24]u8 = @splat(0);
            std.mem.writeInt(u64, key[0..8], 1 + n * (if (scattered) @as(u64, 499) else 2), .big);
            try sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = &key })}, .values = &.{}, .ordinal = n });
        }
        try std.testing.expectEqual(!scattered, try preferPageCopyOnWrite(a, pages.store(), root, &sort, 0));
        // Admission never consumes the run needed by the selected writer.
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var count: usize = 0;
        while (true) {
            _ = scratch.reset(.retain_capacity);
            _ = try sort.next(scratch.allocator()) orelse break;
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 20), count);
    }
}

test "external lake reverse construction streams beyond cumulative write admission" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("reverse-streaming-admission");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(2) };
    const builder = try ReverseBuilder.create(a, &store, .none);
    defer builder.deinit();
    builder.writes = 64 * 1024;
    for (0..6000) |row| {
        var key: [24]u8 = undefined;
        std.mem.writeInt(u64, key[0..8], 6000 - row, .big);
        @memcpy(key[8..24], &coordinate(0, .{ .source_id = "s", .snapshot_id = "v", .file_id = "f", .row_group_ordinal = 0, .row_ordinal = row }));
        try builder.add(&key);
    }
    const root = (try builder.finish()).?;
    try std.testing.expectEqual(@as(u64, 6000), root.records);
    var cursor = try tree.Cursor.init(a, builder.pages.store(), root, "", null);
    defer cursor.deinit();
    var row: u64 = 0;
    while (try cursor.next()) |record| : (row += 1) {
        try std.testing.expectEqual(row, std.mem.readInt(u64, record.key[8..16], .big));
        try std.testing.expectEqual(6000 - row, std.mem.readInt(u64, record.key[16..24], .big));
    }
    try std.testing.expectEqual(@as(u64, 6000), row);
}

test "external lake metadata navigation ordered root publication releases provider selectors" {
    const a = std.testing.allocator;
    var provider = @import("../serverless/artifacts/cloud_generations_test_provider.zig").Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    var object = try @import("../serverless/artifacts/object_store.zig").ObjectStore.initWithClientOptions(a, client, "bucket", "native", .{});
    defer object.deinit();
    var store = object.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    const root: Root = .{ .fingerprint = @splat(3), .domain = @splat(6), .source = "source", .snapshot = "snapshot", .page = null };
    const published = try persistRoot(a, a, &store, root, "ordered", .none, null, false, null, null);
    defer {
        a.free(published.name);
        a.free(published.artifact_id);
        a.free(published.checksum);
    }
    try std.testing.expectEqual(metadata_version, published.metadata_version);
    try std.testing.expect(provider.serial > 0);
}

test "external lake metadata navigation wide public ties seek bounded prefixes across sparse files and tuple boundaries" {
    const public = @import("lake_index_public_order.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("bounded-public-ties");
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
    const files = try ca.alloc(local.serverless_external_source_types.FileEntry, 512);
    for (files, 0..) |*file, i| file.* = .{ .file_id = try std.fmt.allocPrint(ca, "file-{d}", .{i}), .object_uri = @constCast("file://fixture"), .byte_len = 1, .row_count = 2, .row_groups = &.{} };
    const inventory: local.serverless_external_source_types.Inventory = .{ .format = .parquet, .source_id = @constCast("lake"), .source_uri = @constCast("file://lake"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = files };
    var sort = local.sql_spill.Sort.init(a, &manager, &.{.{}}, 512 * 1024);
    defer sort.deinit();
    const Expected = struct { group: u8, id: []const u8 };
    var expected: std.ArrayList(Expected) = .empty;
    for (0..2) |group| for (files, 0..) |file, slot| {
        if (group == 0 and slot % 3 == 0) continue;
        const ref: rows.RowRef = .{ .external = .{ .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .file_id = file.file_id, .row_group_ordinal = 0, .row_ordinal = group } };
        var key: [17]u8 = undefined;
        key[0] = @intCast(group);
        @memcpy(key[1..], &coordinate(@intCast(slot), ref.external));
        try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &key })}, .values = &.{}, .ordinal = expected.items.len });
        try expected.append(ca, .{ .group = @intCast(group), .id = try local.storage_rowsource_identity.allocId(ca, ref) });
    };
    // Build the same digest-to-name directory shared by production publication.
    var directory_reads: u64 = 16 * 1024 * 1024;
    var directory_writes: u64 = 16 * 1024 * 1024;
    var directory_pages: page_store.PageStore = .{ .domain = store.upload_scope.?.domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &directory_reads, .remaining_write_bytes = &directory_writes };
    var public_builder: tree.SortedBuilder = .{ .alloc = a, .store = directory_pages.store() };
    defer public_builder.deinit();
    const file_order = try local.serverless_query_lake_serving.ServingSource.canonicalFileOrder(ca, inventory);
    for (file_order) |slot| {
        const name = files[slot].file_id;
        try public_builder.add(&local.storage_rowsource_identity.fileDigest(inventory.source_id, inventory.snapshot_id, name), name);
    }
    const shared = try public_builder.finish();
    const artifact = try publishIncremental(a, ca, &store, &sort, "ordered", @splat(3), inventory, &.{}, .none, .{ .public_navigation = .{ .root = shared, .count = @intCast(files.len), .source = inventory.source_id, .snapshot = inventory.snapshot_id } });
    const root = try loadRootLazy(ca, store, artifact, .none, null);
    try std.testing.expectEqual(@as(usize, 0), root.files.len);
    var reader: Reader = undefined;
    try reader.init(a, &store, root, @splat(3), "", null, .none);
    defer reader.deinit();
    const Order = struct {
        descending: bool,
        fn less(self: @This(), x: Expected, y: Expected) bool {
            return if (x.group != y.group) x.group < y.group else std.mem.order(u8, x.id, y.id) == (if (self.descending) std.math.Order.gt else .lt);
        }
    };
    for ([_]bool{ false, true }) |descending| {
        std.mem.sort(Expected, expected.items, Order{ .descending = descending }, Order.less);
        var first = try public.Cursor.init(a, &reader, "", null, null, null, descending, false);
        defer first.deinit();
        const misses = reader.file_name_misses;
        const page = try first.next(ca, 3);
        try std.testing.expectEqual(@as(usize, 3), page.len);
        for (page, expected.items[0..3]) |ref, item| try std.testing.expectEqualStrings(item.id, try local.storage_rowsource_identity.allocId(ca, ref));
        try std.testing.expectEqual(@as(usize, 0), first.files.len);
        try std.testing.expect(first.public_probes < 16);
        try std.testing.expect(reader.file_name_misses - misses <= 3);
        const split = expected.items.len - files.len;
        for ([_]usize{ 0, 170, split - 1, split, expected.items.len - 1 }) |boundary| for ([_]bool{ false, true }) |before| {
            const item = expected.items[boundary];
            var cursor = try public.Cursor.init(a, &reader, "", null, &.{item.group}, item.id, descending, before);
            defer cursor.deinit();
            const next = try cursor.next(ca, 5);
            const remaining = if (before) boundary else expected.items.len - boundary - 1;
            try std.testing.expectEqual(@min(remaining, 5), next.len);
            for (next, 0..) |ref, i| {
                const rank = if (before) boundary - i - 1 else boundary + i + 1;
                try std.testing.expectEqualStrings(expected.items[rank].id, try local.storage_rowsource_identity.allocId(ca, ref));
            }
        };
    }
}

test "external lake sparse wide public ties spill participants without archive probes" {
    const public = @import("lake_index_public_order.zig");
    const a = std.testing.allocator;
    // The large independent fixture uses an explicitly accounted allocator;
    // per-allocation debug stack unwinding would dominate its spill merges.
    var budget: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = 128 * 1024 * 1024 };
    defer std.debug.assert(budget.live == 0);
    const ba = budget.allocator();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("sparse-wide-public-ties");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(ba, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(5), .attempt = @splat(1) };
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = ba, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check };
    defer manager.deinit();
    const participants = 4097;
    const files = try ca.alloc(local.serverless_external_source_types.FileEntry, participants * 16 + 1);
    for (files, 0..) |*file, i| file.* = .{ .file_id = try std.fmt.allocPrint(ca, "file-{d:0>6}", .{i}), .object_uri = @constCast("file://fixture"), .byte_len = 1, .row_count = 1, .row_groups = &.{} };
    const inventory: local.serverless_external_source_types.Inventory = .{ .format = .parquet, .source_id = @constCast("lake"), .source_uri = @constCast("file://lake"), .snapshot_id = @constCast("snapshot"), .schema_fingerprint = @constCast("schema"), .files = files };
    var sort = local.sql_spill.Sort.init(ba, &manager, &.{.{}}, 512 * 1024);
    defer sort.deinit();
    const expected = try ca.alloc([]const u8, participants);
    for (files[0..participants], 0..) |file, slot| {
        const ref: rows.RowRef = .{ .external = .{ .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .file_id = file.file_id, .row_group_ordinal = 0, .row_ordinal = 0 } };
        var key: [17]u8 = undefined;
        key[0] = 1;
        key[1..17].* = coordinate(@intCast(slot), ref.external);
        try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &key })}, .values = &.{}, .ordinal = slot });
        expected[slot] = try local.storage_rowsource_identity.allocId(ca, ref);
    }
    files[0].row_count = 2;
    const tail_ref: rows.RowRef = .{ .external = .{ .source_id = inventory.source_id, .snapshot_id = inventory.snapshot_id, .file_id = files[0].file_id, .row_group_ordinal = 0, .row_ordinal = 1 } };
    var tail_key: [17]u8 = undefined;
    tail_key[0] = 2;
    tail_key[1..17].* = coordinate(0, tail_ref.external);
    try sort.add(.{ .keys = &.{local.sql_scalar.Datum.fromJson(.{ .string = &tail_key })}, .values = &.{}, .ordinal = participants });
    const tail_id = try local.storage_rowsource_identity.allocId(ca, tail_ref);
    std.mem.sort([]const u8, expected, {}, struct {
        fn less(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.less);
    const artifact = try publish(ba, ca, &store, &sort, "ordered", @splat(3), inventory, &.{}, .none);
    const root = try loadRootLazy(ca, store, artifact, .none, null);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(ba);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{ .io = std.testing.io } };
    for ([_]bool{ false, true }) |descending| {
        var reader: Reader = undefined;
        try reader.initCached(ba, &store, root, @splat(3), "", null, .none, cached);
        defer reader.deinit();
        var cursor = try public.Cursor.init(ba, &reader, "", null, null, null, descending, false);
        defer cursor.deinit();
        var rank: usize = 0;
        while (true) {
            const page = try cursor.next(ca, 8);
            if (page.len == 0) break;
            for (page) |ref| {
                const id = if (rank == expected.len) tail_id else expected[if (descending) expected.len - rank - 1 else rank];
                try std.testing.expectEqualStrings(id, try local.storage_rowsource_identity.allocId(ca, ref));
                rank += 1;
            }
        }
        try std.testing.expectEqual(expected.len + 1, rank);
        try std.testing.expectEqual(@as(usize, 1), cursor.participant_sorts);
        try std.testing.expect(cursor.public_probes > 0);
        try std.testing.expect(cursor.public_probes <= participants * 4);
        try std.testing.expect(cursor.participant_spill_files_peak > 0);
        {
            var whole_reader: Reader = undefined;
            try whole_reader.initCached(ba, &store, root, @splat(3), "", null, .none, cached);
            defer whole_reader.deinit();
            var whole = try public.Cursor.init(ba, &whole_reader, "", null, null, null, descending, false);
            defer whole.deinit();
            const page = try whole.next(ca, 65536);
            try std.testing.expectEqual(expected.len + 1, page.len);
            for (page, 0..) |ref, i| {
                const id = if (i == expected.len) tail_id else expected[if (descending) expected.len - i - 1 else i];
                try std.testing.expectEqualStrings(id, try local.storage_rowsource_identity.allocId(ca, ref));
            }
            try std.testing.expectEqual(@as(usize, 0), whole.public_probes);
            try std.testing.expectEqual(@as(usize, 1), whole.participant_sorts);
        }
        {
            var fallback_reader: Reader = undefined;
            try fallback_reader.initCached(ba, &store, root, @splat(3), "", null, .none, cached);
            defer fallback_reader.deinit();
            fallback_reader.remaining_reads = 8 * 1024 * 1024;
            var fallback = try public.Cursor.init(ba, &fallback_reader, "", null, null, null, descending, false);
            defer fallback.deinit();
            fallback.participant_spill_limit = 1;
            const page = try fallback.next(ca, 3);
            try std.testing.expectEqual(@as(usize, 3), page.len);
            for (page, 0..) |ref, i| {
                const position = if (descending) expected.len - i - 1 else i;
                try std.testing.expectEqualStrings(expected[position], try local.storage_rowsource_identity.allocId(ca, ref));
            }
            try std.testing.expectEqual(@as(usize, 0), fallback.participant_sorts);
            try std.testing.expect(fallback.public_probes > 0);
            try std.testing.expect(fallback.promotion_disabled);
        }
        {
            var stream_reader: Reader = undefined;
            try stream_reader.initCached(ba, &store, root, @splat(3), "", null, .none, cached);
            defer stream_reader.deinit();
            var fallback = try public.Cursor.init(ba, &stream_reader, "", null, null, null, descending, false);
            defer fallback.deinit();
            fallback.participant_spill_limit = 1;
            var seen: usize = 0;
            for (0..32) |_| {
                const page = try fallback.next(ca, 8);
                try std.testing.expectEqual(@as(usize, 8), page.len);
                for (page) |ref| {
                    const position = if (descending) expected.len - seen - 1 else seen;
                    try std.testing.expectEqualStrings(expected[position], try local.storage_rowsource_identity.allocId(ca, ref));
                    seen += 1;
                }
            }
            try std.testing.expectEqual(@as(usize, 256), seen);
            try std.testing.expectEqual(@as(usize, 0), fallback.participant_sorts);
            try std.testing.expect(fallback.promotion_disabled);
            try std.testing.expect(fallback.public_probes >= fallback.promotion_probe_limit);
        }
        for ([_]bool{ false, true }) |before| {
            var resume_reader: Reader = undefined;
            try resume_reader.initCached(ba, &store, root, @splat(3), "", null, .none, cached);
            defer resume_reader.deinit();
            var resumed = try public.Cursor.init(ba, &resume_reader, "", null, &.{1}, expected[2048], descending, before);
            defer resumed.deinit();
            const page = try resumed.next(ca, 5);
            try std.testing.expectEqual(@as(usize, 5), page.len);
            for (page, 0..) |ref, i| {
                const position = if (descending != before) 2047 - i else 2049 + i;
                try std.testing.expectEqualStrings(expected[position], try local.storage_rowsource_identity.allocId(ca, ref));
            }
            try std.testing.expectEqual(@as(usize, 0), resumed.participant_sorts);
            try std.testing.expect(resumed.public_probes > 0 and resumed.public_probes < 512);
        }
    }
}

test "external lake ordered retirement planning visits explicit changed slots only" {
    const a = std.testing.allocator;
    const keep = try a.alloc(bool, 1_000_000);
    defer a.free(keep);
    @memset(keep, true);
    keep[999_999] = false;
    var changed: Retirements = .{ .keep = keep, .slots = &.{999_999} };
    try std.testing.expectEqual(@as(?u32, 999_999), try changed.next());
    try std.testing.expectEqual(@as(?u32, null), try changed.next());
    try std.testing.expectEqual(@as(usize, 1), changed.position);
    var unchanged: Retirements = .{ .keep = keep, .slots = &.{} };
    try std.testing.expectEqual(@as(?u32, null), try unchanged.next());
    try std.testing.expectEqual(@as(usize, 0), unchanged.position);
    var legacy: Retirements = .{ .keep = keep, .slots = null };
    try std.testing.expectEqual(@as(?u32, 999_999), try legacy.next());
    try std.testing.expectEqual(@as(?u32, null), try legacy.next());
    var invalid: Retirements = .{ .keep = keep, .slots = &.{0} };
    try std.testing.expectError(error.InvalidNativeLakeRowIndex, invalid.next());
}
