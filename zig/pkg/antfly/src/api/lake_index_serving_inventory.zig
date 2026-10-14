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

//! Stable immutable-manifest keys make publication proportional to changed
//! files. Compact scoring routes exclude section navigation and physical rows;
//! those remain in independently admitted, authenticated file manifests.
const std = @import("std");
const A = std.mem.Allocator;
const corpus = @import("lake_index_native_text.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages_mod = @import("../serverless/graph_segment/page_store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const Ref = corpus.statistics.Ref;
const Chunk = artifacts.ChunkRef;
const chunk_bytes = 16 * 1024;
const FileRouting = struct { file: @import("lake_index_native_state.zig").File, segments: []const corpus.SegmentRouting, physical: ?Chunk = null };
pub const Roots = struct { full: ?Ref, routes: ?Ref, segments: ?Ref = null };

pub fn compactRoutes(a: A, group: corpus.FileGroup) ![]const corpus.SegmentRouting {
    if (group.admissions.len != group.segments.len or group.summaries.len != group.segments.len) return error.InvalidNativeLakeTextCorpus;
    const segments = try a.alloc(corpus.SegmentRouting, group.segments.len);
    for (segments, group.segments, group.summaries, group.admissions, 0..) |*out, ref, summary, metadata, i| {
        const fields = try a.alloc(corpus.SegmentRouting.Field, metadata.fields.len);
        for (fields, metadata.fields) |*field, source| {
            var mask: u16 = 0;
            for (source.sections) |section| mask |= @as(u16, 1) << @as(u4, @intCast(@backingInt(section.kind)));
            field.* = .{ .name = source.name, .stats = source.stats, .section_mask = mask };
        }
        out.* = .{ .ref = ref, .summary = summary, .bytes = metadata.bytes, .doc_count = metadata.doc_count, .fields = fields, .descriptor = if (group.descriptors.len == group.segments.len) group.descriptors[i] else null };
    }
    return segments;
}
fn encode(a: A, group: corpus.FileGroup, compact: bool) ![]u8 {
    if (!compact) return std.json.Stringify.valueAlloc(a, group, .{});
    var file = group.file;
    file.docs = &.{};
    file.count = 0;
    return std.json.Stringify.valueAlloc(a, FileRouting{ .file = file, .segments = try compactRoutes(a, group), .physical = group.physical_manifest }, .{});
}
fn key(ref: Chunk, offset: usize) ![36]u8 {
    var result: [36]u8 = undefined;
    result[0..32].* = try stores.sha256DigestFromChecksum(ref.checksum);
    std.mem.writeInt(u32, result[32..36], @intCast(offset), .big);
    return result;
}
fn identity(ref: Chunk) ![32]u8 {
    return stores.sha256DigestFromChecksum(ref.checksum);
}
fn build(a: A, store: tree.Store, groups: []const corpus.FileGroup, refs: []const Chunk, compact: bool) !?Ref {
    const Order = struct { index: usize, digest: [32]u8 };
    const order = try a.alloc(Order, refs.len);
    defer a.free(order);
    for (order, refs, 0..) |*entry, ref, index| entry.* = .{ .index = index, .digest = try identity(ref) };
    std.mem.sort(Order, order, {}, struct {
        fn less(_: void, left: Order, right: Order) bool {
            return std.mem.order(u8, &left.digest, &right.digest) == .lt;
        }
    }.less);
    const Source = struct {
        groups: []const corpus.FileGroup,
        refs: []const Chunk,
        order: []const Order,
        compact: bool,
        index: usize = 0,
        offset: usize = 0,
        bytes: ?[]u8 = null,
        arena: std.heap.ArenaAllocator,
        key_bytes: [36]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.bytes) |bytes| if (self.offset == bytes.len) {
                _ = self.arena.reset(.free_all);
                self.bytes = null;
                self.offset = 0;
                self.index += 1;
            };
            if (self.index == self.order.len) return null;
            const index = self.order[self.index].index;
            if (self.bytes == null) self.bytes = try encode(self.arena.allocator(), self.groups[index], self.compact);
            const bytes = self.bytes.?;
            if (bytes.len == 0 or bytes.len > corpus.max_root_bytes) return error.NativeLakeTextCorpusTooLarge;
            self.key_bytes = try key(self.refs[index], self.offset);
            const end = @min(bytes.len, self.offset + chunk_bytes);
            const value = bytes[self.offset..end];
            self.offset = end;
            return .{ .key = &self.key_bytes, .value = value };
        }
    };
    var source: Source = .{ .groups = groups, .refs = refs, .order = order, .compact = compact, .arena = .init(a) };
    defer source.arena.deinit();
    return tree.buildSorted(a, store, &source);
}
fn mutate(a: A, store: tree.Store, root: ?Ref, ref: Chunk, bytes: ?[]const u8, previous_len: usize) !?Ref {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temporary = arena.allocator();
    var mutations: std.ArrayList(tree.Mutation) = .empty;
    const length = if (bytes) |value| value.len else previous_len;
    if (length == 0 or length > corpus.max_root_bytes) return error.NativeLakeTextCorpusTooLarge;
    var offset: usize = 0;
    while (offset < length) : (offset += chunk_bytes) {
        const k = try temporary.create([36]u8);
        k.* = try key(ref, offset);
        try mutations.append(temporary, .{ .key = k, .value = if (bytes) |value| value[offset..@min(length, offset + chunk_bytes)] else null });
    }
    return tree.apply(a, store, root, mutations.items);
}
pub fn publish(a: A, store: *stores.ArtifactStore, groups: []const corpus.FileGroup, refs: []const Chunk, prior: ?corpus.Root, cancellation: Cancellation) !Roots {
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var reads: u64 = 128 * 1024 * 1024;
    var writes: u64 = 128 * 1024 * 1024;
    var pages: pages_mod.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    return update(a, pages.store(), groups, refs, prior, cancellation);
}
fn update(a: A, storage: tree.Store, groups: []const corpus.FileGroup, refs: []const Chunk, prior: ?corpus.Root, cancellation: Cancellation) !Roots {
    const old = prior orelse return .{ .full = try build(a, storage, groups, refs, false), .routes = try build(a, storage, groups, refs, true), .segments = try segmentFiles(a, storage, groups, refs) };
    if (old.inventory_version < 2 or old.manifests.len != old.file_groups.len) return .{ .full = try build(a, storage, groups, refs, false), .routes = try build(a, storage, groups, refs, true), .segments = try segmentFiles(a, storage, groups, refs) };
    var before: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer before.deinit(a);
    var after: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer after.deinit(a);
    for (old.manifests) |ref| try before.put(a, try identity(ref), {});
    for (refs) |ref| try after.put(a, try identity(ref), {});
    var result: Roots = .{ .full = old.serving_inventory, .routes = old.serving_routes, .segments = old.segment_files };
    const rebuild_segments = old.segment_files == null;
    if (rebuild_segments) result.segments = try segmentFiles(a, storage, groups, refs);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (old.manifests, old.file_groups) |ref, group| {
        try cancellation.check();
        if (after.contains(try identity(ref))) continue;
        if (!rebuild_segments) result.segments = try mutateSegmentFiles(a, storage, result.segments, group, null);
        result.full = try mutate(a, storage, result.full, ref, null, @intCast(ref.byte_len));
        _ = scratch.reset(.free_all);
        const encoded = try encode(scratch.allocator(), group, true);
        result.routes = try mutate(a, storage, result.routes, ref, null, encoded.len);
    }
    for (refs, groups) |ref, group| {
        try cancellation.check();
        if (before.contains(try identity(ref))) continue;
        _ = scratch.reset(.free_all);
        if (!rebuild_segments) result.segments = try mutateSegmentFiles(a, storage, result.segments, group, try identity(ref));
        result.full = try mutate(a, storage, result.full, ref, try encode(scratch.allocator(), group, false), 0);
        _ = scratch.reset(.free_all);
        result.routes = try mutate(a, storage, result.routes, ref, try encode(scratch.allocator(), group, true), 0);
    }
    return result;
}

fn segmentFiles(a: A, storage: tree.Store, groups: []const corpus.FileGroup, refs: []const Chunk) !?Ref {
    const Record = struct { key: [32]u8, file: [32]u8 };
    var records: std.ArrayList(Record) = .empty;
    defer records.deinit(a);
    for (groups, refs) |group, ref| for (group.segments) |segment| try records.append(a, .{ .key = corpus.term_directory.identity(segment.artifact_id), .file = try identity(ref) });
    std.mem.sort(Record, records.items, {}, struct {
        fn less(_: void, l: Record, r: Record) bool {
            return std.mem.order(u8, &l.key, &r.key) == .lt;
        }
    }.less);
    const Source = struct {
        records: []const Record,
        index: usize = 0,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.index == self.records.len) return null;
            const record = &self.records[self.index];
            self.index += 1;
            return .{ .key = &record.key, .value = &record.file };
        }
    };
    var source: Source = .{ .records = records.items };
    return tree.buildSorted(a, storage, &source);
}
fn mutateSegmentFiles(a: A, storage: tree.Store, root: ?Ref, group: corpus.FileGroup, digest: ?[32]u8) !?Ref {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const mutations = try scratch.alloc(tree.Mutation, group.segments.len);
    for (mutations, group.segments) |*mutation, segment| {
        const key_ = try scratch.create([32]u8);
        key_.* = corpus.term_directory.identity(segment.artifact_id);
        const value: ?[]const u8 = if (digest) |file| blk: {
            const owned = try scratch.create([32]u8);
            owned.* = file;
            break :blk owned;
        } else null;
        mutation.* = .{ .key = key_, .value = value };
    }
    std.mem.sort(tree.Mutation, mutations, {}, struct {
        fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
            return std.mem.order(u8, l.key, r.key) == .lt;
        }
    }.less);
    return tree.apply(a, storage, root, mutations);
}

pub fn loadSelected(a: A, scratch: A, directory: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, cancellation: Cancellation, selection: []const [32]u8) !corpus.Root {
    var wanted: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer wanted.deinit(scratch);
    for (selection) |id| try wanted.put(scratch, id, {});
    var refs: std.ArrayList(Chunk) = .empty;
    for (directory.manifests) |ref| if (wanted.contains(try identity(ref))) {
        try refs.append(a, ref);
    };
    if (refs.items.len != wanted.count()) return error.InvalidNativeLakeTextCorpus;
    var subset = directory;
    subset.manifests = refs.items;
    subset.partial_inventory = true;
    return load(a, scratch, subset, store, cached, cancellation, true);
}

pub fn load(a: A, scratch: A, directory: corpus.Root, store_value: stores.ArtifactStore, cached: artifacts.CachedRead, cancellation: Cancellation, compact: bool) !corpus.Root {
    const Read = struct {
        cached: artifacts.CachedRead,
        fn page(raw: *anyopaque, alloc: A, store: *stores.ArtifactStore, ref: @import("antfly_local_sources").serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, _: [32]u8, token: Cancellation, budget: *u64) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.cached.context.ensureActive();
            if (offset != 0 or length != ref.byte_len) return error.InvalidNativeLakeTextCorpus;
            try stores.chargeReadBudget(budget, length);
            return artifacts.readArtifact(alloc, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached);
        }
    };
    try cached.context.ensureActive();
    try cancellation.check();
    var read: Read = .{ .cached = cached };
    var store = store_value;
    var reads: u64 = 128 * 1024 * 1024;
    var writes: u64 = 0;
    var pages: pages_mod.PageStore = .{ .domain = directory.domain, .artifacts = &store, .cancellation = cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .read_cache = .{ .ptr = &read, .read = Read.page } };
    var indices: std.AutoHashMapUnmanaged([32]u8, usize) = .empty;
    defer indices.deinit(scratch);
    for (directory.manifests, 0..) |ref, index| {
        const entry = try indices.getOrPut(scratch, try identity(ref));
        if (entry.found_existing) return error.InvalidNativeLakeTextCorpus;
        entry.value_ptr.* = index;
    }
    const groups = try a.alloc(corpus.FileGroup, directory.manifests.len);
    const routing = try a.alloc([]const corpus.SegmentRouting, if (compact) groups.len else 0);
    var seen = try std.DynamicBitSetUnmanaged.initEmpty(scratch, groups.len);
    defer seen.deinit(scratch);
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(scratch);
    // Selected partitions seek directly to their immutable digest prefix.
    const passes = if (directory.partial_inventory) groups.len else 1;
    for (0..passes) |pass| {
        var start_key: [36]u8 = @splat(0);
        if (directory.partial_inventory) start_key = try key(directory.manifests[pass], 0);
        var cursor = try tree.Cursor.init(scratch, pages.store(), if (compact) directory.serving_routes else directory.serving_inventory, if (directory.partial_inventory) &start_key else "", null);
        defer cursor.deinit();
        bytes.clearRetainingCapacity();
        var digest: ?[32]u8 = null;
        while (try cursor.next()) |record| {
            try cached.context.ensureActive();
            try cancellation.check();
            if (record.key.len != 36 or record.value.len == 0 or record.value.len > chunk_bytes) return error.InvalidNativeLakeTextCorpus;
            const current: [32]u8 = record.key[0..32].*;
            if (directory.partial_inventory and !std.mem.eql(u8, &current, start_key[0..32])) break;
            if (digest != null and !std.mem.eql(u8, &digest.?, &current)) {
                try append(a, directory, groups, routing, &seen, indices, digest.?, bytes.items, compact);
                bytes.clearRetainingCapacity();
            }
            digest = current;
            if (std.mem.readInt(u32, record.key[32..36], .big) != bytes.items.len or record.value.len > corpus.max_root_bytes -| bytes.items.len) return error.InvalidNativeLakeTextCorpus;
            try bytes.appendSlice(scratch, record.value);
        }
        if (digest) |id| try append(a, directory, groups, routing, &seen, indices, id, bytes.items, compact);
    }
    if (seen.count() != groups.len) return error.InvalidNativeLakeTextCorpus;
    var segments: std.ArrayList(artifacts.ChunkRef) = .empty;
    for (groups) |group| try segments.appendSlice(a, group.segments);
    if (segments.items.len > corpus.max_segments) return error.InvalidNativeLakeTextCorpus;
    var root = directory;
    root.file_groups = groups;
    root.segments = segments.items;
    root.routing = routing;
    if (!compact) try root.validate();
    return root;
}
fn append(a: A, directory: corpus.Root, groups: []corpus.FileGroup, routes: [][]const corpus.SegmentRouting, seen: *std.DynamicBitSetUnmanaged, indices: std.AutoHashMapUnmanaged([32]u8, usize), digest: [32]u8, bytes: []const u8, compact: bool) !void {
    const index = indices.get(digest) orelse return error.InvalidNativeLakeTextCorpus;
    if (seen.isSet(index)) return error.InvalidNativeLakeTextCorpus;
    const ref = directory.manifests[index];
    if (!compact) {
        var actual: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &actual, .{});
        if (ref.byte_len != bytes.len) return error.InvalidNativeLakeTextCorpus;
        if (!std.mem.eql(u8, &digest, &actual)) return error.ArtifactIntegrityMismatch;
        groups[index] = try std.json.parseFromSliceLeaky(corpus.FileGroup, a, bytes, .{ .allocate = .alloc_always });
    } else {
        const route = try std.json.parseFromSliceLeaky(FileRouting, a, bytes, .{ .allocate = .alloc_always });
        const refs = try a.alloc(Chunk, route.segments.len);
        const summaries = try a.alloc(?Ref, route.segments.len);
        for (route.segments, refs, summaries) |segment, *target, *summary| {
            if (segment.fields.len > std.math.maxInt(u16) or segment.bytes == 0 or segment.bytes > 32 * 1024 * 1024) return error.InvalidNativeLakeTextCorpus;
            target.* = segment.ref;
            summary.* = segment.summary;
            for (segment.fields, 0..) |field, i| {
                if (field.name.len > std.math.maxInt(u16)) return error.InvalidNativeLakeTextCorpus;
                if (i != 0 and std.mem.order(u8, segment.fields[i - 1].name, field.name) != .lt) return error.InvalidNativeLakeTextCorpus;
                if (field.stats) |stats| if (stats.doc_count > segment.doc_count) return error.InvalidNativeLakeTextCorpus;
            }
        }
        groups[index] = .{ .file = route.file, .segments = refs, .summaries = summaries, .physical_manifest = route.physical };
        routes[index] = route.segments;
        try groups[index].validate(directory.domain);
    }
    seen.set(index);
}

test "external lake serving inventory COW retains old readers and touches only changed paths" {
    const a = std.testing.allocator;
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const groups = try ca.alloc(corpus.FileGroup, 2048);
    const refs = try ca.alloc(Chunk, groups.len);
    for (groups, refs, 0..) |*group, *ref, index| {
        const name = try std.fmt.allocPrint(ca, "file-{d:0>8}-{s}", .{ index, @as([256]u8, @splat('x')) });
        group.* = .{ .file = .{ .id = name, .digest = @splat(1) }, .segments = &.{} };
        const bytes = try encode(ca, group.*, false);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        const checksum = try ca.dupe(u8, &std.fmt.bytesToHex(&digest, .lower));
        ref.* = .{ .artifact_id = "", .checksum = checksum, .byte_len = bytes.len };
    }
    const first = try update(a, memory.store(), groups, refs, null, .none);
    const initial_writes = memory.writes;
    const old_groups = try ca.dupe(corpus.FileGroup, groups);
    const old_refs = try ca.dupe(Chunk, refs);
    var prior: corpus.Root = .{ .domain = @splat(1), .binding = undefined, .config_json = "{}", .inventory_version = 2, .serving_inventory = first.full, .serving_routes = first.routes, .file_groups = old_groups, .manifests = old_refs };
    groups[400].file.digest = @splat(2);
    const bytes = try encode(ca, groups[400], false);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    refs[400].checksum = try ca.dupe(u8, &std.fmt.bytesToHex(&digest, .lower));
    refs[400].byte_len = bytes.len;
    const second = try update(a, memory.store(), groups, refs, prior, .none);
    std.debug.print("Serving inventory COW: initial_pages={d} changed_pages={d}\n", .{ initial_writes, memory.writes - initial_writes });
    try std.testing.expect(initial_writes > 20);
    try std.testing.expect(memory.writes - initial_writes < initial_writes / 4);
    const old_key = try key(old_refs[400], 0);
    const new_key = try key(refs[400], 0);
    var old_cursor = try tree.Cursor.init(a, memory.store(), first.full, &old_key, null);
    defer old_cursor.deinit();
    try std.testing.expectEqualSlices(u8, &old_key, (try old_cursor.next()).?.key);
    var new_cursor = try tree.Cursor.init(a, memory.store(), second.full, &new_key, null);
    defer new_cursor.deinit();
    try std.testing.expectEqualSlices(u8, &new_key, (try new_cursor.next()).?.key);
    prior.serving_inventory = second.full;
    prior.serving_routes = second.routes;
    prior.file_groups = groups;
    prior.manifests = refs;
    const before_noop = memory.writes;
    const noop = try update(a, memory.store(), groups, refs, prior, .none);
    try std.testing.expectEqualDeep(second, noop);
    try std.testing.expectEqual(before_noop, memory.writes);
}

test "external lake selected compact inventory seeks file partitions and retains global totals" {
    const local = @import("antfly_local_sources");
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("selected-serving-inventory");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(6), .attempt = @splat(4) };
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 64 * 1024 * 1024;
    var pages: pages_mod.PageStore = .{ .domain = @splat(6), .attempt = @splat(4), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const groups = try ca.alloc(corpus.FileGroup, 64);
    const refs = try ca.alloc(Chunk, groups.len);
    for (groups, refs, 0..) |*group, *ref, i| {
        group.* = .{ .file = .{ .id = try std.fmt.allocPrint(ca, "file-{d}-{s}", .{ i, @as([512]u8, @splat('x')) }), .digest = @splat(1) }, .segments = &.{} };
        const encoded = try encode(ca, group.*, false);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
        ref.* = .{ .artifact_id = "", .checksum = try ca.dupe(u8, &std.fmt.bytesToHex(&digest, .lower)), .byte_len = encoded.len };
    }
    const roots = try update(a, pages.store(), groups, refs, null, .none);
    const root: corpus.Root = .{ .domain = @splat(6), .binding = undefined, .config_json = "{}", .inventory_version = 2, .serving_inventory = roots.full, .serving_routes = roots.routes, .manifests = refs, .collection = .{ .documents = 1234, .fields = &.{.{ .name = "body", .total_length = 9876 }} } };
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(3), .context = .{} };
    const selected = try loadSelected(ca, a, root, store, cached, .none, &.{ try identity(refs[50]), try identity(refs[1]) });
    try std.testing.expect(selected.partial_inventory);
    try std.testing.expectEqual(@as(usize, 2), selected.file_groups.len);
    try std.testing.expectEqualStrings(groups[1].file.id, selected.file_groups[0].file.id);
    try std.testing.expectEqualStrings(groups[50].file.id, selected.file_groups[1].file.id);
    try std.testing.expectEqual(@as(u32, 1234), selected.collection.?.documents);
    try std.testing.expectEqual(@as(u64, 9876), selected.collection.?.fields[0].total_length);
    const empty = try loadSelected(ca, a, root, store, cached, .none, &.{});
    try std.testing.expect(empty.partial_inventory and empty.file_groups.len == 0 and empty.segments.len == 0);
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, loadSelected(ca, a, root, store, cached, .none, &.{@splat(9)}));
}

/// Recovery visits one compact file route at a time. It never opens a combined
/// maintenance manifest or retains an archive's detailed admission metadata.
pub fn walkRoutes(a: A, directory: corpus.Root, store_value: stores.ArtifactStore, cancellation: Cancellation, visitor: anytype) !void {
    var store = store_value;
    var reads: u64 = 128 * 1024 * 1024;
    var writes: u64 = 0;
    var pages: pages_mod.PageStore = .{ .domain = directory.domain, .artifacts = &store, .cancellation = cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var cursor = try tree.Cursor.init(a, pages.store(), directory.serving_routes, "", null);
    defer cursor.deinit();
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    var digest: ?[32]u8 = null;
    var files: usize = 0;
    while (try cursor.next()) |record| {
        try visitor.check();
        if (record.key.len != 36 or record.value.len == 0 or record.value.len > chunk_bytes) return error.InvalidNativeLakeTextCorpus;
        const current: [32]u8 = record.key[0..32].*;
        if (digest != null and !std.mem.eql(u8, &digest.?, &current)) {
            try visitRoute(a, bytes.items, visitor);
            files += 1;
            bytes.clearRetainingCapacity();
        }
        digest = current;
        if (std.mem.readInt(u32, record.key[32..36], .big) != bytes.items.len or record.value.len > corpus.max_root_bytes -| bytes.items.len) return error.InvalidNativeLakeTextCorpus;
        try bytes.appendSlice(a, record.value);
    }
    if (digest != null) {
        try visitRoute(a, bytes.items, visitor);
        files += 1;
    }
    if (files != directory.file_count) return error.InvalidNativeLakeTextCorpus;
}
fn visitRoute(a: A, bytes: []const u8, visitor: anytype) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const route = try std.json.parseFromSliceLeaky(FileRouting, arena.allocator(), bytes, .{ .allocate = .alloc_always });
    try visitor.route(route);
}
