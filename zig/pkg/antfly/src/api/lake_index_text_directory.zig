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

//! Paged file identities and independently admitted native segment descriptors.
//! Maintenance walks compact file state, then opens only changed manifests.
const std = @import("std");
const local = @import("antfly_local_sources");
const corpus = @import("lake_index_native_text.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages_mod = @import("../serverless/graph_segment/page_store.zig");
const state = @import("lake_index_native_state.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Id = [32]u8;
const Chunk = artifacts.ChunkRef;
const chunk_bytes = 16 * 1024;
pub const Roots = struct {
    files: ?tree.Ref = null,
    segments: ?tree.Ref = null,
    pub fn validate(self: @This()) !void {
        if (self.files) |ref| try ref.validate();
        if (self.segments) |ref| try ref.validate();
    }
};
pub const Interval = struct { lower: u32, count: u32 };
pub const FileRecord = struct { file: state.File, manifest: Chunk };
const SegmentRecord = struct {
    file: state.File,
    manifest: Chunk,
    ordinal: u32,
    interval: Interval,
    route: corpus.SegmentRouting,
    rows: []const corpus.physical.Block,
};
pub fn validateRef(ref: Chunk, domain: Id) !void {
    if (ref.byte_len == 0 or ref.byte_len > corpus.max_root_bytes) return error.InvalidNativeLakeTextCorpus;
    try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
    if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeTextCorpus;
}
fn fileState(file: state.File) state.File {
    var result = file;
    result.docs = &.{};
    result.count = 0;
    return result;
}
fn put(a: A, store: *stores.ArtifactStore, value: anytype, token: Cancellation) !Chunk {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    if (bytes.len > corpus.max_root_bytes) return error.NativeLakeTextCorpusTooLarge;
    var upload = store.*;
    upload.allocator = a;
    const ref = try upload.putWithCancellation(bytes, token);
    return .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
}
/// Descriptor payloads contain one segment's admission, and physical metadata
/// contains no admissions. Neither hydration nor binding decodes a whole file.
pub fn prepare(a: A, store: *stores.ArtifactStore, group: *corpus.FileGroup, token: Cancellation) !void {
    const descriptors = try a.alloc(Chunk, group.segments.len);
    for (descriptors, group.segments, group.admissions, group.summaries) |*ref, segment, admission, summary| {
        try token.check();
        ref.* = try put(a, store, corpus.FileGroup{ .file = fileState(group.file), .segments = &.{segment}, .admissions = &.{admission}, .summaries = &.{summary} }, token);
    }
    group.descriptors = descriptors;
    group.physical_manifest = try put(a, store, corpus.FileGroup{ .file = fileState(group.file), .segments = &.{}, .rows = group.rows }, token);
}
const Reader = struct {
    store: stores.ArtifactStore,
    cached: ?artifacts.CachedRead,
    token: Cancellation,
    reads: u64 = 128 * 1024 * 1024,
    writes: u64 = 0,
    external_budget: ?*u64 = null,
    pages: pages_mod.PageStore = undefined,
    fn init(self: *@This(), domain: Id) void {
        self.pages = .{ .domain = domain, .artifacts = &self.store, .cancellation = self.token, .remaining_read_bytes = self.external_budget orelse &self.reads, .remaining_write_bytes = &self.writes, .read_cache = if (self.cached != null) .{ .ptr = self, .read = readPage } else null };
    }
    fn check(self: *@This()) !void {
        try self.token.check();
        if (self.cached) |cached| try cached.context.ensureActive();
    }
    fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, _: Id, token: Cancellation, budget: *u64) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.check();
        if (offset != 0 or length != ref.byte_len) return error.InvalidNativeLakeTextCorpus;
        try stores.chargeReadBudget(budget, length);
        return artifacts.readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached);
    }
};
fn key(id: Id, offset: usize) [36]u8 {
    var result: [36]u8 = undefined;
    result[0..32].* = id;
    std.mem.writeInt(u32, result[32..36], @intCast(offset), .big);
    return result;
}
fn mutation(a: A, list: *std.ArrayList(tree.Mutation), id: Id, value: ?[]const u8, previous_length: usize) !void {
    const length = if (value) |bytes| bytes.len else previous_length;
    if (length == 0 or length > corpus.max_root_bytes) return error.InvalidNativeLakeTextCorpus;
    var offset: usize = 0;
    while (offset < length) : (offset += chunk_bytes) {
        const k = try a.create([36]u8);
        k.* = key(id, offset);
        try list.append(a, .{ .key = k, .value = if (value) |bytes| bytes[offset..@min(length, offset + chunk_bytes)] else null });
    }
}
fn apply(a: A, storage: tree.Store, root: ?tree.Ref, changes: []tree.Mutation) !?tree.Ref {
    var unique: std.StringHashMapUnmanaged(?[]const u8) = .empty;
    defer unique.deinit(a);
    // Last mutation wins: replacement can shrink a chunked record and must
    // delete its old tail as well as replace its first pages.
    for (changes) |change| try unique.put(a, change.key, change.value);
    const sorted = try a.alloc(tree.Mutation, unique.count());
    defer a.free(sorted);
    var it = unique.iterator();
    for (sorted) |*change| {
        const entry = it.next().?;
        change.* = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
    }
    std.mem.sort(tree.Mutation, sorted, {}, struct {
        fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
            return std.mem.order(u8, l.key, r.key) == .lt;
        }
    }.less);
    return tree.apply(a, storage, root, sorted);
}
fn segmentRecords(a: A, group: corpus.FileGroup, manifest: Chunk) ![]const SegmentRecord {
    const routes = try @import("lake_index_serving_inventory.zig").compactRoutes(a, group);
    const records = try a.alloc(SegmentRecord, routes.len);
    var lower: u32 = 0;
    for (records, routes, 0..) |*record, route, ordinal| {
        const upper = try std.math.add(u32, lower, route.doc_count);
        var rows: std.ArrayList(corpus.physical.Block) = .empty;
        for (group.rows) |block| if (block.base < upper and @as(u64, block.base) + block.count > lower) try rows.append(a, block);
        record.* = .{ .file = fileState(group.file), .manifest = manifest, .ordinal = @intCast(ordinal), .interval = .{ .lower = lower, .count = route.doc_count }, .route = route, .rows = rows.items };
        lower = upper;
    }
    if (lower != try corpus.physical.validate(group.rows)) return error.InvalidNativeLakeTextCorpus;
    return records;
}
/// Callers pass only changed/removed files for v3 seeds. Old roots are upgraded
/// once with a full publication. All trees use immutable copy-on-write paths.
pub fn publish(a: A, store: *stores.ArtifactStore, groups: []const corpus.FileGroup, refs: []const Chunk, removed: []const corpus.FileGroup, removed_refs: []const Chunk, old: Roots, token: Cancellation) !Roots {
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var reads: u64 = 128 * 1024 * 1024;
    var writes: u64 = 128 * 1024 * 1024;
    var pages: pages_mod.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    return update(a, pages.store(), groups, refs, removed, removed_refs, old, token);
}
fn update(a: A, storage: tree.Store, groups: []const corpus.FileGroup, refs: []const Chunk, removed: []const corpus.FileGroup, removed_refs: []const Chunk, old: Roots, token: Cancellation) !Roots {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var files: std.ArrayList(tree.Mutation) = .empty;
    var segments: std.ArrayList(tree.Mutation) = .empty;
    for (removed, removed_refs) |group, ref| {
        try token.check();
        const file = try std.json.Stringify.valueAlloc(ca, FileRecord{ .file = fileState(group.file), .manifest = ref }, .{});
        const id = corpus.term_directory.identity(group.file.id);
        try mutation(ca, &files, id, null, file.len);
        for (try segmentRecords(ca, group, ref)) |record| {
            const encoded = try std.json.Stringify.valueAlloc(ca, record, .{});
            try mutation(ca, &segments, corpus.term_directory.identity(record.route.ref.artifact_id), null, encoded.len);
        }
    }
    for (groups, refs) |group, ref| {
        try token.check();
        const file = try std.json.Stringify.valueAlloc(ca, FileRecord{ .file = fileState(group.file), .manifest = ref }, .{});
        try mutation(ca, &files, corpus.term_directory.identity(group.file.id), file, 0);
        for (try segmentRecords(ca, group, ref)) |record| try mutation(ca, &segments, corpus.term_directory.identity(record.route.ref.artifact_id), try std.json.Stringify.valueAlloc(ca, record, .{}), 0);
    }
    return .{ .files = try apply(a, storage, old.files, files.items), .segments = try apply(a, storage, old.segments, segments.items) };
}
fn get(a: A, storage: tree.Store, root: ?tree.Ref, id: Id) ![]const u8 {
    const start = key(id, 0);
    var cursor = try tree.Cursor.init(a, storage, root, &start, null);
    defer cursor.deinit();
    var bytes: std.ArrayList(u8) = .empty;
    errdefer bytes.deinit(a);
    while (try cursor.next()) |record| {
        if (record.key.len != 36) return error.InvalidNativeLakeTextCorpus;
        if (!std.mem.eql(u8, record.key[0..32], &id)) break;
        if (std.mem.readInt(u32, record.key[32..36], .big) != bytes.items.len or record.value.len == 0 or record.value.len > chunk_bytes or record.value.len > corpus.max_root_bytes -| bytes.items.len) return error.InvalidNativeLakeTextCorpus;
        try bytes.appendSlice(a, record.value);
    }
    if (bytes.items.len == 0) return error.InvalidNativeLakeTextCorpus;
    return bytes.toOwnedSlice(a);
}
fn appendFile(a: A, result: *std.ArrayList(FileRecord), id: Id, bytes: []const u8, domain: Id) !void {
    const file = try std.json.parseFromSliceLeaky(FileRecord, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, &id, &corpus.term_directory.identity(file.file.id))) return error.InvalidNativeLakeTextCorpus;
    try validateRef(file.manifest, domain);
    try state.validate(&.{file.file}, domain);
    if (result.items.len >= 16384) return error.InvalidNativeLakeTextCorpus;
    try result.append(a, file);
}
pub fn fileRecords(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: ?artifacts.CachedRead, token: Cancellation) ![]const FileRecord {
    var budget: u64 = 128 * 1024 * 1024;
    return fileRecordsBudget(a, root, store, cached, token, &budget);
}
pub fn fileRecordsBudget(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: ?artifacts.CachedRead, token: Cancellation, budget: *u64) ![]const FileRecord {
    var read: Reader = .{ .store = store, .cached = cached, .token = token, .external_budget = budget };
    read.init(root.domain);
    try read.check();
    var cursor = try tree.Cursor.init(a, read.pages.store(), root.directories.files, "", null);
    defer cursor.deinit();
    var result: std.ArrayList(FileRecord) = .empty;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    var id: ?Id = null;
    while (try cursor.next()) |record| {
        try read.check();
        if (record.key.len != 36) return error.InvalidNativeLakeTextCorpus;
        const current: Id = record.key[0..32].*;
        if (id) |previous| if (!std.mem.eql(u8, &previous, &current)) {
            try appendFile(a, &result, previous, bytes.items, root.domain);
            bytes.clearRetainingCapacity();
        };
        id = current;
        if (std.mem.readInt(u32, record.key[32..36], .big) != bytes.items.len or record.value.len == 0 or record.value.len > chunk_bytes or record.value.len > corpus.max_root_bytes -| bytes.items.len) return error.InvalidNativeLakeTextCorpus;
        try bytes.appendSlice(a, record.value);
    }
    if (id) |last| try appendFile(a, &result, last, bytes.items, root.domain);
    if (result.items.len != root.file_count) return error.InvalidNativeLakeTextCorpus;
    return result.items;
}
pub fn lookupFile(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: ?artifacts.CachedRead, token: Cancellation, file_id: []const u8) !FileRecord {
    var read: Reader = .{ .store = store, .cached = cached, .token = token };
    read.init(root.domain);
    try read.check();
    const bytes = try get(a, read.pages.store(), root.directories.files, corpus.term_directory.identity(file_id));
    defer a.free(bytes);
    const record = try std.json.parseFromSliceLeaky(FileRecord, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, record.file.id, file_id)) return error.InvalidNativeLakeTextCorpus;
    try validateRef(record.manifest, root.domain);
    try state.validate(&.{record.file}, root.domain);
    return record;
}
pub fn withManifests(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: ?artifacts.CachedRead, token: Cancellation) !corpus.Root {
    if (root.inventory_version != 3 or root.manifests.len != 0) return root;
    const records = try fileRecords(a, root, store, cached, token);
    const refs = try a.alloc(Chunk, records.len);
    for (records, refs) |record, *ref| ref.* = record.manifest;
    var expanded = root;
    expanded.manifests = refs;
    return expanded;
}
fn validateSegment(record: SegmentRecord, domain: Id) !void {
    try state.validate(&.{record.file}, domain);
    try validateRef(record.manifest, domain);
    try validateRef(record.route.ref, domain);
    try validateRef(record.route.descriptor orelse return error.InvalidNativeLakeTextCorpus, domain);
    if (record.route.summary) |summary| try summary.validate();
    if (record.interval.count == 0 or record.interval.count != record.route.doc_count or record.route.bytes == 0 or record.route.bytes > 32 * 1024 * 1024 or record.route.fields.len > std.math.maxInt(u16)) return error.InvalidNativeLakeTextCorpus;
    const upper = try std.math.add(u32, record.interval.lower, record.interval.count);
    for (record.route.fields, 0..) |field, i| {
        if (field.name.len > std.math.maxInt(u16) or (i != 0 and std.mem.order(u8, record.route.fields[i - 1].name, field.name) != .lt)) return error.InvalidNativeLakeTextCorpus;
        if (field.stats) |stats| if (stats.doc_count > record.route.doc_count) return error.InvalidNativeLakeTextCorpus;
    }
    var covered = record.interval.lower;
    for (record.rows, 0..) |block, i| {
        var independent = block;
        independent.base = 0;
        _ = try corpus.physical.validate(&.{independent});
        if (block.bitmap) |ref| try validateRef(ref, domain);
        const end = try std.math.add(u32, block.base, block.count);
        if (block.base > covered or end <= covered or block.base >= upper) return error.InvalidNativeLakeTextCorpus;
        if (i != 0) {
            const previous = record.rows[i - 1];
            if (block.base != @as(u64, previous.base) + previous.count or block.group < previous.group or (block.group == previous.group and block.high <= previous.high)) return error.InvalidNativeLakeTextCorpus;
        }
        covered = end;
    }
    if (covered < upper) return error.InvalidNativeLakeTextCorpus;
}
pub fn loadSelected(a: A, scratch: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Cancellation, selection: []const Id) !corpus.Root {
    var read: Reader = .{ .store = store, .cached = cached, .token = token };
    read.init(root.domain);
    try read.check();
    const records = try a.alloc(SegmentRecord, selection.len);
    for (records, selection) |*record, id| {
        try read.check();
        const bytes = try get(scratch, read.pages.store(), root.directories.segments, id);
        defer scratch.free(bytes);
        record.* = try std.json.parseFromSliceLeaky(SegmentRecord, a, bytes, .{ .allocate = .alloc_always });
        if (!std.mem.eql(u8, &id, &corpus.term_directory.identity(record.route.ref.artifact_id))) return error.InvalidNativeLakeTextCorpus;
        try validateSegment(record.*, root.domain);
    }
    std.mem.sort(SegmentRecord, records, {}, struct {
        fn less(_: void, l: SegmentRecord, r: SegmentRecord) bool {
            // Match the complete paged file catalog's immutable order. Subset
            // ordinals must preserve the full corpus' score-tie ordering.
            const left = corpus.term_directory.identity(l.file.id);
            const right = corpus.term_directory.identity(r.file.id);
            const order = std.mem.order(u8, &left, &right);
            return order == .lt or (order == .eq and l.ordinal < r.ordinal);
        }
    }.less);
    var groups: std.ArrayList(corpus.FileGroup) = .empty;
    var manifests: std.ArrayList(Chunk) = .empty;
    var routes: std.ArrayList([]const corpus.SegmentRouting) = .empty;
    var segments: std.ArrayList(Chunk) = .empty;
    var first: usize = 0;
    while (first < records.len) {
        var end = first + 1;
        while (end < records.len and std.mem.eql(u8, records[first].file.id, records[end].file.id)) : (end += 1) {}
        const selected = records[first..end];
        const refs = try a.alloc(Chunk, selected.len);
        const summaries = try a.alloc(?tree.Ref, selected.len);
        const routing = try a.alloc(corpus.SegmentRouting, selected.len);
        const intervals = try a.alloc(Interval, selected.len);
        var blocks: std.ArrayList(corpus.physical.Block) = .empty;
        for (selected, refs, summaries, routing, intervals, 0..) |record, *ref, *summary, *route, *interval, i| {
            if (i != 0 and (selected[i - 1].ordinal >= record.ordinal or @as(u64, intervals[i - 1].lower) + intervals[i - 1].count > record.interval.lower)) return error.InvalidNativeLakeTextCorpus;
            if (!std.mem.eql(u8, &selected[0].file.digest, &record.file.digest) or !std.mem.eql(u8, selected[0].manifest.artifact_id, record.manifest.artifact_id)) return error.InvalidNativeLakeTextCorpus;
            ref.* = record.route.ref;
            summary.* = record.route.summary;
            route.* = record.route;
            interval.* = record.interval;
            for (record.rows) |block| {
                if (blocks.items.len != 0 and blocks.items[blocks.items.len - 1].base == block.base) continue;
                try blocks.append(a, block);
            }
        }
        try groups.append(a, .{ .file = selected[0].file, .segments = refs, .summaries = summaries, .rows = blocks.items, .selected = intervals });
        try manifests.append(a, selected[0].manifest);
        try routes.append(a, routing);
        try segments.appendSlice(a, refs);
        first = end;
    }
    var view = root;
    view.partial_inventory = true;
    view.file_groups = groups.items;
    view.manifests = manifests.items;
    view.routing = routes.items;
    view.segments = segments.items;
    return view;
}

test "external lake exact segment admission skips missing unrelated descriptors and preserves row gaps" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("paged-native-segment-routing");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    const segments = try ca.alloc(Chunk, 9);
    const admissions = try ca.alloc(local.segment.SegmentReader.Admission, segments.len);
    const summaries = try ca.alloc(?tree.Ref, segments.len);
    for (segments, admissions, summaries, 0..) |*ref, *admission, *summary, i| {
        const bytes = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, &.{.{ .key = try std.fmt.allocPrint(ca, "row-{d}", .{i}), .value = if (i == 0 or i == 8) "{\"body\":\"quasar\"}" else "{\"body\":\"common\"}" }}, .{}, null)).?;
        defer a.free(bytes);
        ref.* = try @import("lake_index_seekable_text.zig").publish(a, ca, &store, bytes, .none);
        var reader = try local.segment.SegmentReader.init(a, bytes);
        defer reader.deinit();
        admission.* = try reader.admission(ca);
        summary.* = null;
    }
    var group: corpus.FileGroup = .{ .file = .{ .id = "one-large-file", .digest = @splat(1) }, .segments = segments, .admissions = admissions, .summaries = summaries, .rows = &.{.{ .group = 0, .high = 0, .base = 0, .lower = 10, .count = 9 }} };
    try prepare(ca, &store, &group, .none);
    const manifest = try put(ca, &store, group, .none);
    const roots = try publish(a, &store, &.{group}, &.{manifest}, &.{}, &.{}, .{}, .none);
    const root: corpus.Root = .{ .domain = store.upload_scope.?.domain, .binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} }, .config_json = "{\"field\":\"body\"}", .seekable = true, .inventory_version = 3, .file_count = 1, .directories = roots, .collection = .{ .documents = 9, .fields = &.{.{ .name = "body", .total_length = 9 }} } };
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{ .io = std.testing.io } };
    var expired = cached;
    expired.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, loadSelected(ca, a, root, store, expired, .none, &.{}));
    try std.testing.expectError(error.DeadlineExceeded, fileRecords(ca, root, store, expired, .none));
    try std.testing.expectError(error.DeadlineExceeded, lookupFile(ca, root, store, expired, .none, group.file.id));
    // A precise seek needs neither a file catalog nor any maintenance manifest.
    // Removing unselected payloads makes an accidental file-wide admission fail.
    try store.delete(manifest.artifact_id);
    try store.delete(group.descriptors[4].artifact_id);
    try store.delete(segments[4].artifact_id);
    var selective_root = root;
    selective_root.directories.files = null;
    const selected = try loadSelected(ca, a, selective_root, store, cached, .none, &.{ corpus.term_directory.identity(segments[8].artifact_id), corpus.term_directory.identity(segments[0].artifact_id) });
    try std.testing.expectEqual(@as(usize, 2), selected.segments.len);
    try std.testing.expectEqual(@as(usize, 1), selected.file_groups.len);
    try std.testing.expectEqual(@as(u32, 8), selected.file_groups[0].selected[1].lower);
    var writer = try corpus.loadWriter(a, store, selected, .none, cached, null);
    defer writer.deinit();
    const snapshot = writer.acquireSnapshot();
    defer snapshot.release();
    const identities = try @import("lake_index_text_predicate.zig").Identities.init(ca, selected, snapshot);
    var rows: @import("lake_index_text_predicate.zig").LiveRowsCache = .{};
    defer rows.deinit(a);
    for (0..9) |i| {
        const physical: local.storage_rowsource_types.RowRef = .{ .external = .{ .source_id = "source", .snapshot_id = "snapshot", .file_id = group.file.id, .row_group_ordinal = 0, .row_ordinal = 10 + i } };
        const ordinal_value = try identities.ordinal(a, &rows, store, cached, physical);
        if (i != 0 and i != 8) {
            try std.testing.expect(ordinal_value == null);
        } else {
            try std.testing.expectEqual(@as(?u32, if (i == 0) 0 else 1), ordinal_value);
            const restored = try identities.physicalRow(a, &rows, store, cached, ordinal_value.?, "source", "snapshot");
            try std.testing.expectEqualDeep(physical, restored);
        }
    }
    // Details really are independent immutable descriptors, one segment each.
    const detail = try corpus.loadFileManifest(ca, store, group.descriptors[8], root.domain, .none, null);
    try std.testing.expectEqual(@as(usize, 1), detail.admissions.len);
    try std.testing.expectEqualStrings(segments[8].artifact_id, detail.segments[0].artifact_id);
}

test "external lake paged file catalog updates and looks up only bounded tree paths" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    const groups = try ca.alloc(corpus.FileGroup, 2048);
    const refs = try ca.alloc(Chunk, groups.len);
    for (groups, refs, 0..) |*group, *ref, i| {
        group.* = .{ .file = .{ .id = try std.fmt.allocPrint(ca, "file-{d}-{s}", .{ i, @as([512]u8, @splat('x')) }), .digest = @splat(1) }, .segments = &.{} };
        ref.* = .{ .artifact_id = "unopened maintenance manifest", .checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .byte_len = 128 };
    }
    const initial = try update(a, memory.store(), groups, refs, &.{}, &.{}, .{}, .none);
    const initial_writes = memory.writes;
    const old = groups[1000];
    groups[1000].file.digest = @splat(2);
    const before_reads = memory.reads;
    const changed = try update(a, memory.store(), groups[1000..1001], refs[1000..1001], &.{old}, refs[1000..1001], initial, .none);
    try std.testing.expect(initial_writes > 32);
    try std.testing.expect(memory.writes - initial_writes < 8);
    try std.testing.expect(memory.reads - before_reads < 12);
    const lookup_reads = memory.reads;
    const id = corpus.term_directory.identity(old.file.id);
    const old_bytes = try get(ca, memory.store(), initial.files, id);
    const new_bytes = try get(ca, memory.store(), changed.files, id);
    const before = try std.json.parseFromSliceLeaky(FileRecord, ca, old_bytes, .{});
    const after = try std.json.parseFromSliceLeaky(FileRecord, ca, new_bytes, .{});
    try std.testing.expectEqualSlices(u8, &old.file.digest, &before.file.digest);
    try std.testing.expectEqualSlices(u8, &groups[1000].file.digest, &after.file.digest);
    try std.testing.expect(memory.reads - lookup_reads < 12);
    std.debug.print("Paged file catalog: initial_pages={d} changed_pages={d} update_reads={d} two_lookup_reads={d}\n", .{ initial_writes, memory.writes - initial_writes, lookup_reads - before_reads, memory.reads - lookup_reads });
}
