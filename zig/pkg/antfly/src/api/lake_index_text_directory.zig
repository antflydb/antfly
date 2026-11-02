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
    routing: ?tree.Ref = null,
    ordered: ?tree.Ref = null,
    ordered_format: u8 = 0,
    pub fn validate(self: @This()) !void {
        if (self.ordered_format > 2) return error.InvalidNativeLakeTextCorpus;
        if (self.files) |ref| try ref.validate();
        if (self.segments) |ref| try ref.validate();
        if (self.routing) |ref| try ref.validate();
        if (self.ordered) |ref| try ref.validate();
    }
};
pub const RouteFilter = struct { ptr: *anyopaque, keep: *const fn (*anyopaque, corpus.SegmentRouting) anyerror!bool };
pub const Interval = struct { lower: u32, count: u32 };
pub const FileRecord = struct { file: state.File, manifest: Chunk };
pub const SegmentRecord = struct {
    sources: []const corpus.source_spans.Span = &.{},
    source_pages: []const Chunk = &.{},
    source_count: u32 = 0,
    source_view: ?corpus.source_spans.View = null,
    source_paged: ?corpus.source_spans.Paged = null,
    members: []const corpus.Member = &.{},
    file: state.File,
    manifest: Chunk,
    physical: ?Chunk = null,
    ordinal: u32,
    interval: Interval,
    route: corpus.SegmentRouting,
    rows: []const corpus.physical.Block,
    pub fn jsonStringify(self: SegmentRecord, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(SegmentRecord).@"struct".field_names) |field| {
            if (comptime !std.mem.eql(u8, field, "source_view") and !std.mem.eql(u8, field, "source_paged")) {
                try stream.objectField(field);
                try stream.write(@field(self, field));
            }
        }
        try stream.endObject();
    }
};
fn parseSegment(a: A, bytes: []const u8, options: std.json.ParseOptions) !SegmentRecord {
    var record = try std.json.parseFromSliceLeaky(SegmentRecord, a, bytes, options);
    if (record.members.len != 0) {
        if (record.sources.len != 0) return error.InvalidNativeLakeTextCorpus;
        const scope = (try stores.uploadScopeFromArtifactId(record.manifest.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
        record.sources = try corpus.source_spans.fromMembers(a, record.members, scope.domain);
        // v10 retained a physical wrapper for merged members. Actual physical
        // lookups resolve the member manifests, independently of that wrapper.
        record.physical = null;
        record.members = &.{};
    }
    return record;
}
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
    defer if (ref.provider_version) |version| upload.allocator.free(version);
    return .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
}
/// Descriptor payloads contain one segment's admission, and physical metadata
/// contains no admissions. Neither hydration nor binding decodes a whole file.
pub fn prepare(a: A, store: *stores.ArtifactStore, group: *corpus.FileGroup, token: Cancellation) !void {
    if (group.sources.len != 0 and group.source_pages.len == 0) {
        const prepared = try corpus.source_spans.Prepared.init(a, group.sources);
        defer prepared.deinit(a);
        if (prepared.pages.len != 0) {
            group.source_pages = try prepared.publish(a, store, token);
            group.source_count = @intCast(group.sources.len);
        }
    }
    if (group.sources.len == 0) {
        group.physical_manifest = try put(a, store, corpus.FileGroup{ .file = fileState(group.file), .segments = &.{}, .rows = group.rows, .opening_packs = group.opening_packs }, token);
        if (group.parts.len != 0) {
            for (group.parts) |part| for (@constCast(part)) |*span| {
                span.physical = group.physical_manifest.?;
            };
        }
    }
    // Persist bounds with the same authenticated descriptor/route graph.
    // Missing or oversized bounds decline pruning rather than ordering data.
    const bounds = if (group.sort_bounds.len == group.segments.len) group.sort_bounds else fresh: {
        const result = try a.alloc(?@import("lake_index_sort_bounds.zig").Bound, group.segments.len);
        for (result, group.segments, group.admissions) |*bound, ref, admission| {
            bound.* = null;
            const sorted = for (admission.fields) |field| {
                if (for (field.sections) |section| {
                    if (section.kind == .index_sort_bounds) break true;
                } else false) break true;
            } else false;
            if (!sorted) continue;
            var data = try @import("lake_index_seekable_text.zig").loadPublication(a, .{ .store = store.*, .cache = null, .context = .{}, .cancellation = token }, ref);
            defer data.deinit(a);
            var reader = try local.segment.SegmentReader.initAuthenticated(a, data.source(), admission);
            defer reader.deinit();
            bound.* = try @import("lake_index_sort_bounds.zig").Bound.fromReader(a, &reader);
        }
        break :fresh result;
    };
    group.sort_bounds = bounds;
    const descriptors = try a.alloc(Chunk, group.segments.len);
    for (descriptors, group.segments, group.admissions, group.summaries, 0..) |*ref, segment, admission, summary, ordinal| {
        try token.check();
        const sources = if (group.parts.len != 0) group.parts[ordinal] else if (group.source_pages.len == 0) group.sources else &.{};
        ref.* = try put(a, store, corpus.FileGroup{ .file = fileState(group.file), .sources = sources, .masked = group.masked or group.parts.len != 0, .source_pages = group.source_pages, .source_count = group.source_count, .segments = &.{segment}, .admissions = &.{admission}, .summaries = &.{summary}, .sort_bounds = bounds[ordinal..][0..1] }, token);
    }
    group.descriptors = descriptors;
}
const Reader = struct {
    store: stores.ArtifactStore,
    cached: ?artifacts.CachedRead,
    token: Cancellation,
    reads: u64 = 128 * 1024 * 1024,
    writes: u64 = 0,
    external_budget: ?*u64 = null,
    publication: bool = false,
    pages: pages_mod.PageStore = undefined,
    fn init(self: *@This(), domain: Id) void {
        self.pages = .{ .domain = domain, .artifacts = &self.store, .cancellation = self.token, .remaining_read_bytes = self.external_budget orelse &self.reads, .remaining_write_bytes = &self.writes, .read_cache = if (self.cached != null) .{ .ptr = self, .read = readPage } else null };
        if (self.publication) self.pages.admitPublication();
    }
    fn check(self: *@This()) !void {
        try self.token.check();
        if (self.cached) |cached| try cached.context.ensureActive();
    }
    fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, digest: Id, token: Cancellation, budget: *u64) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.check();
        try stores.chargeReadBudget(budget, length);
        return artifacts.readPageRange(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, offset, length, digest, token, self.cached);
    }
};
fn key(id: Id, offset: usize) [36]u8 {
    var result: [36]u8 = undefined;
    result[0..32].* = id;
    std.mem.writeInt(u32, result[32..36], @intCast(offset), .big);
    return result;
}
const Segments = struct {
    group: corpus.FileGroup,
    manifest: Chunk,
    routes: []const corpus.SegmentRouting,
    ordinal: usize = 0,
    lower: u32 = 0,
    first_block: usize = 0,
    blocks_examined: usize = 0,
    fn init(a: A, group: corpus.FileGroup, manifest: Chunk) !Segments {
        if (group.sources.len == 0) _ = try corpus.physical.validate(group.rows);
        return .{ .group = group, .manifest = manifest, .routes = try @import("lake_index_serving_inventory.zig").compactRoutes(a, group) };
    }
    fn next(self: *Segments) !?SegmentRecord {
        if (self.ordinal == self.routes.len) {
            if (self.group.sources.len == 0 and self.group.parts.len == 0 and self.lower != try corpus.physical.validate(self.group.rows)) return error.InvalidNativeLakeTextCorpus;
            return null;
        }
        const route = self.routes[self.ordinal];
        const upper = try std.math.add(u32, self.lower, route.doc_count);
        while (self.first_block < self.group.rows.len) {
            self.blocks_examined += 1;
            const block = self.group.rows[self.first_block];
            if (@as(u64, block.base) + block.count > self.lower) break;
            self.first_block += 1;
        }
        var end = self.first_block;
        while (end < self.group.rows.len and self.group.rows[end].base < upper) : (end += 1) self.blocks_examined += 1;
        const has_parts = self.group.parts.len != 0;
        const sources = if (has_parts) self.group.parts[self.ordinal] else if (self.group.source_pages.len == 0) self.group.sources else &.{};
        const result: SegmentRecord = .{ .sources = sources, .source_pages = self.group.source_pages, .source_count = self.group.source_count, .file = fileState(self.group.file), .manifest = self.manifest, .physical = if (has_parts) null else self.group.physical_manifest, .ordinal = @intCast(self.ordinal), .interval = .{ .lower = self.lower, .count = route.doc_count }, .route = route, .rows = if (has_parts) &.{} else self.group.rows[self.first_block..end] };
        self.lower = upper;
        self.ordinal += 1;
        return result;
    }
};
/// Metadata construction streams one record at a time into bounded sorts and
/// private local runs. Only the final tree is durable; replacement tails and
/// last-write ordering remain explicit.
pub const Mutations = @import("lake_index_page_mutations.zig").Mutations;
/// Address-stable, operation-owned streaming directory publication. Each decoded
/// group is copied into bounded sorts before the caller reuses its read arena.
/// Statistics and directory consumers can therefore share one manifest read.
pub const Publisher = struct {
    token: Cancellation,
    reads: u64 = 0,
    writes: u64 = 0,
    pages: pages_mod.PageStore = undefined,
    manager: local.sql_spill.Manager = undefined,
    files: Mutations = undefined,
    segments: Mutations = undefined,
    routing: Mutations = undefined,
    ordered: Mutations = undefined,
    metadata: MetadataBuilder = undefined,
    pub fn init(self: *Publisher, a: A, store: *stores.ArtifactStore, token: Cancellation, io: std.Io) !void {
        const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
        self.* = .{ .token = token };
        self.pages = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &self.reads, .remaining_write_bytes = &self.writes, .streaming = true };
        self.pages.admitPublication();
        self.initReducers(a, self.pages.store(), io);
    }
    fn initReducers(self: *Publisher, a: A, storage: tree.Store, io: std.Io) void {
        self.manager = .{ .alloc = a, .io = io, .context = self, .checkpoint = check };
        self.files = Mutations.init(a, &self.manager, storage);
        self.segments = Mutations.init(a, &self.manager, storage);
        self.routing = Mutations.init(a, &self.manager, storage);
        self.ordered = Mutations.init(a, &self.manager, storage);
        self.ordered.reduction = @import("lake_index_sort_directory.zig").reduce;
        self.metadata = .{ .files = &self.files, .segments = &self.segments, .routing = &self.routing, .ordered = &self.ordered, .token = self.token, .group_arena = .init(a), .record_arena = .init(a) };
    }
    fn check(raw: *anyopaque) !void {
        try @as(*Publisher, @ptrCast(@alignCast(raw))).token.check();
    }
    pub fn add(self: *Publisher, group: corpus.FileGroup, ref: Chunk, deleting: bool) !void {
        try self.metadata.add(group, ref, deleting);
    }
    /// Final additions supersede removals regardless of lazy discovery order.
    /// This affects private mutation runs only; persisted directory values keep
    /// their existing format and the prior roots are updated exactly once.
    pub fn preferAdditions(self: *Publisher) void {
        inline for (.{ "files", "segments", "routing", "ordered" }) |name| {
            @field(self, name).addition_first = true;
        }
    }
    pub fn finish(self: *Publisher, old: Roots) !Roots {
        const files = try self.files.finish(old.files);
        const segments = try self.segments.finish(old.segments);
        const routing = try self.routing.finish(old.routing);
        const ordered = if (old.ordered_format != 2 and old.segments != null) rebuild: {
            // One metadata-only migration preserves native payload generations.
            var fresh = Mutations.init(self.metadata.group_arena.child_allocator, &self.manager, self.segments.destination);
            defer fresh.deinit();
            fresh.reduction = @import("lake_index_sort_directory.zig").reduce;
            var cursor = try tree.Cursor.init(self.metadata.group_arena.child_allocator, self.segments.destination, segments, "", null);
            defer cursor.deinit();
            var region = std.heap.ArenaAllocator.init(self.metadata.group_arena.child_allocator);
            defer region.deinit();
            while (try cursor.next()) |entry| {
                try self.token.check();
                _ = region.reset(.retain_capacity);
                const a = region.allocator();
                if (entry.key.len != 36) return error.InvalidNativeLakeTextCorpus;
                if (std.mem.readInt(u32, entry.key[32..36], .big) != 0) continue;
                const value = try get(a, self.segments.destination, segments, entry.key[0..32].*);
                const record = try parseSegment(a, value, .{ .allocate = .alloc_always });
                try @import("lake_index_sort_directory.zig").mutate(a, &fresh, record.route, segmentPlanningBytes(record), false);
            }
            break :rebuild try fresh.finish(null);
        } else try self.ordered.finish(old.ordered);
        return .{ .files = files, .segments = segments, .routing = routing, .ordered = ordered, .ordered_format = 2 };
    }
    pub fn deinit(self: *Publisher) void {
        self.metadata.deinit();
        self.files.deinit();
        self.segments.deinit();
        self.routing.deinit();
        self.ordered.deinit();
        self.manager.deinit();
    }
};
pub fn publish(a: A, store: *stores.ArtifactStore, groups: []const corpus.FileGroup, refs: []const Chunk, removed: []const corpus.FileGroup, removed_refs: []const Chunk, old: Roots, token: Cancellation, io: std.Io) !Roots {
    var publisher: Publisher = undefined;
    try publisher.init(a, store, token, io);
    defer publisher.deinit();
    for ([_]bool{ true, false }) |deleting| {
        for (if (deleting) removed else groups, if (deleting) removed_refs else refs) |group, ref| try publisher.add(group, ref, deleting);
    }
    return publisher.finish(old);
}
/// Checkpoints need only compact file identities, never segment/routing trees.
/// Changes are paged and applied to the previous immutable delta inventory.
pub fn publishFileRecords(a: A, store: *stores.ArtifactStore, records: []const FileRecord, removed: []const FileRecord, prior: ?tree.Ref, token: Cancellation, io: std.Io) !?tree.Ref {
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: pages_mod.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    pages.admitPublication();
    var check = token;
    const Check = struct {
        fn call(raw: *anyopaque) !void {
            try @as(*Cancellation, @ptrCast(@alignCast(raw))).check();
        }
    };
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = io, .context = &check, .checkpoint = Check.call };
    defer manager.deinit();
    var mutations = Mutations.init(a, &manager, pages.store());
    defer mutations.deinit();
    for ([_]bool{ true, false }) |deleting| for (if (deleting) removed else records) |record| {
        try token.check();
        const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
        defer a.free(bytes);
        // GC admits a compact file record as one authenticated leaf value.
        if (bytes.len > chunk_bytes) return error.InvalidNativeLakeTextCorpus;
        try mutations.add(corpus.term_directory.identity(record.file.id), if (deleting) null else bytes, bytes.len);
    };
    return mutations.finish(prior);
}
fn update(a: A, storage: tree.Store, groups: []const corpus.FileGroup, refs: []const Chunk, removed: []const corpus.FileGroup, removed_refs: []const Chunk, old: Roots, token: Cancellation, io: std.Io) !Roots {
    var publisher: Publisher = .{ .token = token };
    publisher.initReducers(a, storage, io);
    defer publisher.deinit();
    for ([_]bool{ true, false }) |deleting| {
        for (if (deleting) removed else groups, if (deleting) removed_refs else refs) |group, ref| try publisher.add(group, ref, deleting);
    }
    return publisher.finish(old);
}
const MetadataBuilder = struct {
    files: *Mutations,
    segments: *Mutations,
    routing: *Mutations,
    ordered: *Mutations,
    token: Cancellation,
    group_arena: std.heap.ArenaAllocator,
    record_arena: std.heap.ArenaAllocator,
    fn deinit(self: *@This()) void {
        self.group_arena.deinit();
        self.record_arena.deinit();
    }
    fn add(self: *@This(), group: corpus.FileGroup, ref: Chunk, deleting: bool) !void {
        try self.token.check();
        _ = self.group_arena.reset(.retain_capacity);
        _ = self.record_arena.reset(.retain_capacity);
        var ca = self.record_arena.allocator();
        const file = try std.json.Stringify.valueAlloc(ca, FileRecord{ .file = fileState(group.file), .manifest = ref }, .{});
        // New file records are independently decodable GC units. Segment
        // descriptors retain their separate chunked encoding for large schemas.
        if (file.len > chunk_bytes) return error.NativeLakeTextCorpusTooLarge;
        try self.files.add(corpus.term_directory.identity(group.file.id), if (deleting) null else file, file.len);
        var records = try Segments.init(self.group_arena.allocator(), group, ref);
        while (try records.next()) |record| {
            try self.token.check();
            _ = self.record_arena.reset(.retain_capacity);
            ca = self.record_arena.allocator();
            try @import("lake_index_sort_directory.zig").mutate(ca, self.ordered, record.route, segmentPlanningBytes(record), deleting);
            const encoded = try std.json.Stringify.valueAlloc(ca, record, .{});
            try self.segments.add(corpus.term_directory.identity(record.route.ref.artifact_id), if (deleting) null else encoded, encoded.len);
            // Ordered compact routing has no physical blocks or admissions.
            // Its authenticated descriptor references admit those only on use.
            var compact = record;
            compact.rows = &.{};
            const route = try std.json.Stringify.valueAlloc(ca, compact, .{});
            var route_key: [36]u8 = undefined;
            route_key[0..32].* = corpus.term_directory.identity(group.file.id);
            std.mem.writeInt(u32, route_key[32..36], record.ordinal, .big);
            try self.routing.addPrefix(&route_key, if (deleting) null else route, route.len);
        }
    }
};

/// Maintenance consumes one durable file manifest at a time. No decoded
/// admissions or physical directories survive the next file.
pub fn publishManifests(a: A, store: *stores.ArtifactStore, refs: []const Chunk, removed: []const Chunk, old: Roots, domain: Id, token: Cancellation, io: std.Io) !Roots {
    var publisher: Publisher = undefined;
    try publisher.init(a, store, token, io);
    defer publisher.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var manifests: corpus.ManifestReader = .{ .a = a, .store = store.*, .domain = domain, .token = token };
    defer manifests.deinit();
    for ([_]bool{ true, false }) |deleting| {
        const inputs = if (deleting) removed else refs;
        for (inputs, 0..) |ref, index| {
            try token.check();
            _ = arena.reset(.free_all);
            const group = try manifests.load(arena.allocator(), ref);
            if (index + 1 < inputs.len) manifests.prefetch(inputs[index + 1]) else if (deleting and refs.len != 0) manifests.prefetch(refs[0]);
            try publisher.add(group, ref, deleting);
        }
    }
    return publisher.finish(old);
}

fn get(a: A, storage: tree.Store, root: ?tree.Ref, id: Id) ![]const u8 {
    const start = key(id, 0);
    var upper = id;
    var position: usize = upper.len;
    while (position != 0) {
        position -= 1;
        upper[position] +%= 1;
        if (upper[position] != 0) break;
    }
    const end: ?[]const u8 = if (position == 0 and upper[0] == 0) null else &upper;
    var cursor = try tree.Cursor.init(a, storage, root, &start, end);
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
fn segmentPlanningBytes(record: SegmentRecord) u64 {
    var metadata = corpus.source_spans.metadataBytes(record.sources, record.source_pages) +| record.manifest.byte_len;
    if (record.route.descriptor) |descriptor| metadata +|= descriptor.byte_len;
    return record.route.bytes +| @max(metadata *| 4, @as(u64, @max(record.source_count, record.sources.len)) *| (@sizeOf(corpus.source_spans.Range) * 12));
}
/// Authenticate a selected leaf's size without admitting its inventory.
pub fn segmentBytes(a: A, storage: tree.Store, root: corpus.Root, id: Id) !u64 {
    return (try selectedPlanningBytes(a, storage, root, id, null)).?;
}
fn selectedPlanningBytes(a: A, storage: tree.Store, root: corpus.Root, id: Id, filter: ?RouteFilter) !?u64 {
    const bytes = try get(a, storage, root.directories.segments, id);
    defer a.free(bytes);
    const record = try parseSegment(a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, &id, &corpus.term_directory.identity(record.route.ref.artifact_id))) return error.InvalidNativeLakeTextCorpus;
    try validateRouting(record, root.domain);
    if (filter) |value| if (!try value.keep(value.ptr, record.route)) return null;
    return segmentPlanningBytes(record);
}

pub fn decodeFileRecord(a: A, id: Id, bytes: []const u8, domain: Id) !FileRecord {
    const file = try std.json.parseFromSliceLeaky(FileRecord, a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, &id, &corpus.term_directory.identity(file.file.id))) return error.InvalidNativeLakeTextCorpus;
    try validateRef(file.manifest, domain);
    try state.validate(&.{file.file}, domain);
    return file;
}
fn appendFile(a: A, result: *std.ArrayList(FileRecord), id: Id, bytes: []const u8, domain: Id) !void {
    if (result.items.len >= corpus.max_segments) return error.InvalidNativeLakeTextCorpus;
    try result.append(a, try decodeFileRecord(a, id, bytes, domain));
}
pub fn fileRecords(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: ?artifacts.CachedRead, token: Cancellation) ![]const FileRecord {
    var budget: u64 = 128 * 1024 * 1024;
    return fileRecordsBudget(a, root, store, cached, token, &budget);
}
pub fn fileRecordsPublication(a: A, root: corpus.Root, store: stores.ArtifactStore, token: Cancellation) ![]const FileRecord {
    var budget: u64 = 128 * 1024 * 1024;
    return fileRecordsInner(a, root, store, null, token, &budget, true);
}
pub fn fileRecordsBudget(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: ?artifacts.CachedRead, token: Cancellation, budget: *u64) ![]const FileRecord {
    return fileRecordsInner(a, root, store, cached, token, budget, false);
}
fn fileRecordsInner(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: ?artifacts.CachedRead, token: Cancellation, budget: *u64, publication: bool) ![]const FileRecord {
    var read: Reader = .{ .store = store, .cached = cached, .token = token, .external_budget = budget, .publication = publication };
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
    return lookupFileBudget(a, root, store, cached, token, file_id, null);
}
pub fn lookupFileBudget(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: ?artifacts.CachedRead, token: Cancellation, file_id: []const u8, budget: ?*u64) !FileRecord {
    var read: Reader = .{ .store = store, .cached = cached, .token = token, .external_budget = budget };
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
    try validateRouting(record, domain);
    try validatePhysical(record, domain, try std.math.add(u32, record.interval.lower, record.interval.count));
}
fn validateRouting(record: SegmentRecord, domain: Id) !void {
    try state.validate(&.{record.file}, domain);
    try validateRef(record.manifest, domain);
    if (record.physical) |physical_ref| try validateRef(physical_ref, domain);
    try validateRef(record.route.ref, domain);
    try validateRef(record.route.descriptor orelse return error.InvalidNativeLakeTextCorpus, domain);
    if (record.route.summary) |summary| try summary.validate();
    if (record.interval.count == 0 or record.interval.count != record.route.doc_count or record.route.bytes == 0 or record.route.bytes > 32 * 1024 * 1024 or record.route.fields.len > std.math.maxInt(u16)) return error.InvalidNativeLakeTextCorpus;
    _ = try std.math.add(u32, record.interval.lower, record.interval.count);
    for (record.route.fields, 0..) |field, i| {
        if (field.name.len > std.math.maxInt(u16) or (i != 0 and std.mem.order(u8, record.route.fields[i - 1].name, field.name) != .lt)) return error.InvalidNativeLakeTextCorpus;
        if (field.stats) |stats| if (stats.doc_count > record.route.doc_count) return error.InvalidNativeLakeTextCorpus;
    }
}
fn validatePhysical(record: SegmentRecord, domain: Id, upper: u32) !void {
    if (record.source_view) |view| {
        if (record.sources.len != 0 or view.ranges.len != record.source_count or record.source_pages.len == 0 or record.ordinal != 0 or record.interval.lower != 0 or record.rows.len != 0 or record.physical != null) return error.InvalidNativeLakeTextCorpus;
        for (record.source_pages) |page| try validateRef(page, domain);
        if (record.route.live.len != 0) {
            if (record.route.live.len != view.ranges.len) return error.InvalidNativeLakeTextCorpus;
            for (record.route.live, view.ranges) |live, r| if (live.lower != r.pack_lower or live.count != r.count) return error.InvalidNativeLakeTextCorpus;
        }
        return view.validateMode(record.interval.count, domain, record.route.live.len != 0);
    }
    if (record.source_pages.len != 0) {
        if (record.source_count == 0 or record.source_count > corpus.source_spans.max_spans or record.source_pages.len > corpus.source_spans.max_spans + 128 or record.ordinal != 0 or record.interval.lower != 0 or record.rows.len != 0 or record.physical != null) return error.InvalidNativeLakeTextCorpus;
        for (record.source_pages) |page| try validateRef(page, domain);
        if (record.sources.len == 0) return;
        if (record.sources.len != record.source_count) return error.InvalidNativeLakeTextCorpus;
    }
    if (record.sources.len != 0) {
        if (record.rows.len != 0 or record.physical != null) return error.InvalidNativeLakeTextCorpus;
        if (record.route.live.len == 0 and (record.ordinal != 0 or record.interval.lower != 0)) return error.InvalidNativeLakeTextCorpus;
        if (record.route.live.len != 0) {
            if (record.route.live.len != record.sources.len) return error.InvalidNativeLakeTextCorpus;
            for (record.route.live, record.sources) |live, span| if (live.lower != span.pack_lower or live.count != span.count) return error.InvalidNativeLakeTextCorpus;
        }
        return corpus.source_spans.validateMode(record.sources, record.interval.count, domain, record.route.live.len != 0);
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
/// Load one authenticated serving descriptor under publication I/O admission.
/// Source routing already selected this pack; no file inventory is materialized.
pub fn loadSegmentPublication(a: A, root: corpus.Root, store: stores.ArtifactStore, token: Cancellation, id: Id, remaining: *u64) !corpus.FileGroup {
    var read: Reader = .{ .store = store, .cached = null, .token = token, .publication = true, .external_budget = remaining };
    read.init(root.domain);
    try read.check();
    const bytes = try get(a, read.pages.store(), root.directories.segments, id);
    defer a.free(bytes);
    const record = try parseSegment(a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, &id, &corpus.term_directory.identity(record.route.ref.artifact_id))) return error.InvalidNativeLakeTextCorpus;
    try validateSegment(record, root.domain);
    const group = try corpus.loadFileManifestPublicationBudget(a, store, record.route.descriptor.?, root.domain, token, remaining);
    if (group.segments.len != 1 or group.admissions.len != 1 or !std.mem.eql(u8, group.segments[0].artifact_id, record.route.ref.artifact_id) or !std.mem.eql(u8, group.segments[0].checksum, record.route.ref.checksum) or group.segments[0].byte_len != record.route.ref.byte_len or group.admissions[0].bytes != record.route.bytes or group.admissions[0].doc_count != record.route.doc_count) return error.InvalidNativeLakeTextCorpus;
    return group;
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
        record.* = try parseSegment(a, bytes, .{ .allocate = .alloc_always });
        if (!std.mem.eql(u8, &id, &corpus.term_directory.identity(record.route.ref.artifact_id))) return error.InvalidNativeLakeTextCorpus;
        try validateSegment(record.*, root.domain);
    }
    try hydrateSources(a, root, store, cached, token, records, &read.reads);
    return assemble(a, root, records, true);
}
/// Compact routing is already in immutable file/segment order. Decode pages
/// incrementally and retain only scoring navigation; physical row metadata is
/// loaded later through the bounded LiveRowsCache. No archive-wide re-sort.
fn loadRouting(a: A, scratch: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Cancellation) !corpus.Root {
    var read: Reader = .{ .store = store, .cached = cached, .token = token };
    read.init(root.domain);
    read.pages.streaming = true;
    var cursor = try tree.Cursor.init(scratch, read.pages.store(), root.directories.routing, "", null);
    defer cursor.deinit();
    var records: std.ArrayList(SegmentRecord) = .empty;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(scratch);
    var previous: ?[36]u8 = null;
    while (try cursor.next()) |entry| {
        try read.check();
        if (entry.key.len != 40) return error.InvalidNativeLakeTextCorpus;
        const current: [36]u8 = entry.key[0..36].*;
        if (previous) |old| if (!std.mem.eql(u8, &old, &current)) {
            try appendRouting(a, &records, root.domain, old, bytes.items);
            bytes.clearRetainingCapacity();
        };
        previous = current;
        if (std.mem.readInt(u32, entry.key[36..40], .big) != bytes.items.len or entry.value.len == 0 or entry.value.len > chunk_bytes or entry.value.len > corpus.max_root_bytes -| bytes.items.len) return error.InvalidNativeLakeTextCorpus;
        try bytes.appendSlice(scratch, entry.value);
    }
    if (previous) |last| try appendRouting(a, &records, root.domain, last, bytes.items);
    if (root.segment_count != 0 and records.items.len != root.segment_count) return error.InvalidNativeLakeTextCorpus;
    try hydrateSources(a, root, store, cached, token, records.items, &read.reads);
    return assembleOrdered(a, root, records.items, false);
}
fn appendRouting(a: A, records: *std.ArrayList(SegmentRecord), domain: Id, expected: [36]u8, bytes: []const u8) !void {
    if (records.items.len >= corpus.max_segments) return error.InvalidNativeLakeTextCorpus;
    const record = try parseSegment(a, bytes, .{ .allocate = .alloc_always });
    const file_id = corpus.term_directory.identity(record.file.id);
    if (!std.mem.eql(u8, expected[0..32], &file_id) or std.mem.readInt(u32, expected[32..36], .big) != record.ordinal or record.rows.len != 0 or (record.physical == null and record.sources.len == 0 and record.source_pages.len == 0)) return error.InvalidNativeLakeTextCorpus;
    try validateRouting(record, domain);
    try records.append(a, record);
}
/// Broad serving admission streams compact ordered routing when present;
/// historical directories use canonical records. Selective admission continues
/// to seek exact identities directly without reading the compact routing tree.
pub fn loadAll(a: A, scratch: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Cancellation) !corpus.Root {
    // Internal callers may carry an expanded catalog. Its references must
    // agree with the authenticated canonical catalog; ordinary wire roots
    // have no flat references and avoid this additional lookup entirely.
    if (root.manifests.len != 0) {
        var arena = std.heap.ArenaAllocator.init(scratch);
        defer arena.deinit();
        const files = try fileRecords(arena.allocator(), root, store, cached, token);
        if (files.len != root.manifests.len) return error.InvalidNativeLakeTextCorpus;
        for (files, root.manifests) |file, ref| {
            if (file.manifest.byte_len != ref.byte_len or !std.mem.eql(u8, file.manifest.artifact_id, ref.artifact_id) or !std.mem.eql(u8, file.manifest.checksum, ref.checksum)) return error.InvalidNativeLakeTextCorpus;
        }
    }
    if (root.directories.routing != null) return loadRouting(a, scratch, root, store, cached, token);
    var read: Reader = .{ .store = store, .cached = cached, .token = token };
    read.init(root.domain);
    var cursor = try tree.Cursor.init(scratch, read.pages.store(), root.directories.segments, "", null);
    defer cursor.deinit();
    var records: std.ArrayList(SegmentRecord) = .empty;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(scratch);
    var id: ?Id = null;
    while (try cursor.next()) |record| {
        try read.check();
        if (record.key.len != 36) return error.InvalidNativeLakeTextCorpus;
        const current: Id = record.key[0..32].*;
        if (id) |previous| if (!std.mem.eql(u8, &previous, &current)) {
            try appendSegment(a, &records, root.domain, previous, bytes.items);
            bytes.clearRetainingCapacity();
        };
        id = current;
        if (std.mem.readInt(u32, record.key[32..36], .big) != bytes.items.len or record.value.len == 0 or record.value.len > chunk_bytes or record.value.len > corpus.max_root_bytes -| bytes.items.len) return error.InvalidNativeLakeTextCorpus;
        try bytes.appendSlice(scratch, record.value);
    }
    if (id) |last| try appendSegment(a, &records, root.domain, last, bytes.items);
    try hydrateSources(a, root, store, cached, token, records.items, &read.reads);
    return assemble(a, root, records.items, false);
}
/// Stream the authenticated retained serving closure with one record decoded.
/// Maintenance manifests are deliberately not required by the serving audit.
pub fn walkSegments(a: A, root: corpus.Root, store: stores.ArtifactStore, token: Cancellation, visitor: anytype) !void {
    return walkSegmentsInner(a, root, store, token, visitor, false);
}
pub fn walkSegmentsPublication(a: A, root: corpus.Root, store: stores.ArtifactStore, token: Cancellation, visitor: anytype) !void {
    return walkSegmentsInner(a, root, store, token, visitor, true);
}
fn walkSegmentsInner(a: A, root: corpus.Root, store: stores.ArtifactStore, token: Cancellation, visitor: anytype, publication: bool) !void {
    var read: Reader = .{ .store = store, .cached = null, .token = token, .publication = publication };
    read.init(root.domain);
    var cursor = try tree.Cursor.init(a, read.pages.store(), root.directories.segments, "", null);
    defer cursor.deinit();
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    var id: ?Id = null;
    while (try cursor.next()) |record| {
        try visitor.check();
        if (record.key.len != 36) return error.InvalidNativeLakeTextCorpus;
        const current: Id = record.key[0..32].*;
        if (id) |previous| if (!std.mem.eql(u8, &previous, &current)) {
            try visitSegment(a, root.domain, previous, bytes.items, visitor);
            bytes.clearRetainingCapacity();
        };
        id = current;
        if (std.mem.readInt(u32, record.key[32..36], .big) != bytes.items.len or record.value.len == 0 or record.value.len > chunk_bytes or record.value.len > corpus.max_root_bytes -| bytes.items.len) return error.InvalidNativeLakeTextCorpus;
        try bytes.appendSlice(a, record.value);
    }
    if (id) |last| try visitSegment(a, root.domain, last, bytes.items, visitor);
}
fn visitSegment(a: A, domain: Id, id: Id, bytes: []const u8, visitor: anytype) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const record = try parseSegment(arena.allocator(), bytes, .{});
    if (!std.mem.eql(u8, &id, &corpus.term_directory.identity(record.route.ref.artifact_id))) return error.InvalidNativeLakeTextCorpus;
    try validateSegment(record, domain);
    try visitor.record(record);
}
fn appendSegment(a: A, records: *std.ArrayList(SegmentRecord), domain: Id, id: Id, bytes: []const u8) !void {
    const record = try parseSegment(a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, &id, &corpus.term_directory.identity(record.route.ref.artifact_id))) return error.InvalidNativeLakeTextCorpus;
    try validateSegment(record, domain);
    try records.append(a, record);
}
fn hydrateSources(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Cancellation, records: []SegmentRecord, reads: *u64) !void {
    for (records) |*record| if (record.source_pages.len != 0) {
        if (cached.lazy_lineage and record.route.live.len == 0) record.source_paged = try corpus.source_spans.Paged.open(a, store, record.source_pages, record.source_count, root.domain, token, cached, reads);
        if (record.source_paged != null) continue;
        record.source_view = try corpus.source_spans.loadView(a, store, record.source_pages, record.source_count, root.domain, token, cached, reads, false);
        try validateSegment(record.*, root.domain);
    };
}
fn assemble(a: A, root: corpus.Root, records: []SegmentRecord, partial: bool) !corpus.Root {
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
    return assembleOrdered(a, root, records, partial);
}
fn assembleOrdered(a: A, root: corpus.Root, records: []const SegmentRecord, partial: bool) !corpus.Root {
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
            if (!partial and (record.ordinal != i or record.interval.lower != (if (i == 0) @as(u64, 0) else @as(u64, intervals[i - 1].lower) + intervals[i - 1].count))) return error.InvalidNativeLakeTextCorpus;
            if (selected[0].physical) |physical_ref| if (record.physical == null or !std.mem.eql(u8, physical_ref.artifact_id, record.physical.?.artifact_id)) return error.InvalidNativeLakeTextCorpus;
            ref.* = record.route.ref;
            summary.* = record.route.summary;
            route.* = record.route;
            interval.* = record.interval;
            for (record.rows) |block| {
                if (blocks.items.len != 0 and blocks.items[blocks.items.len - 1].base == block.base) continue;
                try blocks.append(a, block);
            }
        }
        try groups.append(a, .{ .masked = selected[0].route.live.len != 0, .sources = selected[0].sources, .source_pages = selected[0].source_pages, .source_count = selected[0].source_count, .source_view = selected[0].source_view, .source_paged = selected[0].source_paged, .file = selected[0].file, .segments = refs, .summaries = summaries, .rows = blocks.items, .physical_manifest = selected[0].physical, .selected = if (partial or blocks.items.len != 0) intervals else &.{} });
        try manifests.append(a, selected[0].manifest);
        try routes.append(a, routing);
        try segments.appendSlice(a, refs);
        first = end;
    }
    var view = root;
    view.partial_inventory = partial;
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
    var max_segment_bytes: u64 = 0;
    for (segments, admissions, summaries, 0..) |*ref, *admission, *summary, i| {
        const bytes = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, &.{.{ .key = try std.fmt.allocPrint(ca, "row-{d}", .{i}), .value = if (i == 0 or i == 8) "{\"body\":\"quasar\"}" else "{\"body\":\"common\"}" }}, .{}, null)).?;
        defer a.free(bytes);
        max_segment_bytes = @max(max_segment_bytes, bytes.len);
        ref.* = try @import("lake_index_seekable_text.zig").publish(a, ca, &store, bytes, .none);
        var reader = try local.segment.SegmentReader.init(a, bytes);
        defer reader.deinit();
        admission.* = try reader.admission(ca);
        summary.* = null;
    }
    var group: corpus.FileGroup = .{ .file = .{ .id = "one-large-file", .digest = @splat(1) }, .segments = segments, .admissions = admissions, .summaries = summaries, .rows = &.{.{ .group = 0, .high = 0, .base = 0, .lower = 10, .count = 9 }} };
    try prepare(ca, &store, &group, .none);
    const manifest = try put(ca, &store, group, .none);
    const roots = try publish(a, &store, &.{group}, &.{manifest}, &.{}, &.{}, .{}, .none, std.testing.io);
    const streamed = try publishManifests(a, &store, &.{manifest}, &.{}, .{}, store.upload_scope.?.domain, .none, std.testing.io);
    var historical = roots;
    historical.ordered_format = 0;
    const migrated = try publishManifests(a, &store, &.{}, &.{}, historical, store.upload_scope.?.domain, .none, std.testing.io);
    try std.testing.expectEqual(@as(u8, 2), migrated.ordered_format);
    try std.testing.expect(migrated.segments.?.eql(roots.segments.?));
    try std.testing.expectEqual(@as(u64, 18), migrated.ordered.?.records);

    try std.testing.expect(streamed.files.?.eql(roots.files.?));
    try std.testing.expect(streamed.segments.?.eql(roots.segments.?));
    try std.testing.expect(streamed.routing.?.eql(roots.routing.?));
    const root: corpus.Root = .{ .domain = store.upload_scope.?.domain, .binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} }, .config_json = "{\"field\":\"body\"}", .seekable = true, .inventory_version = 3, .file_count = 1, .directories = roots, .collection = .{ .documents = 9, .fields = &.{.{ .name = "body", .total_length = 9 }} } };
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{ .io = std.testing.io } };
    var expired = cached;
    expired.context.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, loadSelected(ca, a, root, store, expired, .none, &.{}));
    try std.testing.expectError(error.DeadlineExceeded, fileRecords(ca, root, store, expired, .none));
    try std.testing.expectError(error.DeadlineExceeded, lookupFile(ca, root, store, expired, .none, group.file.id));
    // Windowing never needs canonical descriptors or physical row manifests.
    var window_root = root;
    window_root.segment_count = 9;
    const navigation = @import("lake_index_navigation_warming.zig");
    const warmed = try navigation.warmRoots(store, cached, &.{.{ .domain = root.domain, .ref = roots.routing.? }}, .none);
    try std.testing.expect(warmed > 0 and warmed <= navigation.max_pages);
    try std.testing.expectError(error.DeadlineExceeded, navigation.warmRoots(store, expired, &.{.{ .domain = root.domain, .ref = roots.routing.? }}, .none));
    var windows = try RoutingCursor.create(a, window_root, store, cached, .none);
    defer windows.deinit();
    var window_ids: [2]Id = undefined;
    const subset = [_]Id{ corpus.term_directory.identity(segments[0].artifact_id), corpus.term_directory.identity(segments[8].artifact_id) };
    // Prune seven routing leaves before descriptors, payloads or lineage are
    // admitted. Gaps retain original full-corpus ordinals between pulls.
    const Filter = struct {
        ids: []const Id,
        fn keep(raw: *anyopaque, route: corpus.SegmentRouting) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const id = corpus.term_directory.identity(route.ref.artifact_id);
            for (self.ids) |expected| if (std.mem.eql(u8, &id, &expected)) return true;
            return false;
        }
    };
    var filter: Filter = .{ .ids = &subset };
    const pruned = try RoutingCursor.create(a, window_root, store, cached, .none);
    defer pruned.deinit();
    pruned.filter = .{ .ptr = &filter, .keep = Filter.keep };
    for (subset, [_]u64{ 0, 8 }) |id, ordinal| {
        const window = (try pruned.next(&window_ids)).?;
        try std.testing.expectEqual(@as(usize, 1), window.identities.len);
        try std.testing.expectEqual(id, window.identities[0]);
        try std.testing.expectEqual(ordinal, window.doc_offset);
    }
    try std.testing.expect(try pruned.next(&window_ids) == null);
    var all_ids: [9]Id = undefined;
    for (segments, &all_ids) |ref, *id| id.* = corpus.term_directory.identity(ref.artifact_id);
    const filtered_selection = try SelectedCursor.create(a, window_root, store, cached, .none, &all_ids);
    defer filtered_selection.deinit();
    filtered_selection.filter = .{ .ptr = &filter, .keep = Filter.keep };
    var selected_kept: usize = 0;
    while (try filtered_selection.nextBounded(&window_ids, max_segment_bytes)) |window| selected_kept += window.identities.len;
    try std.testing.expectEqual(@as(usize, 2), selected_kept);
    try std.testing.expectEqual(@as(u32, 7), filtered_selection.pruned);
    try std.testing.expectEqual(@as(u32, 7), pruned.pruned);
    try std.testing.expectEqual(@as(u64, 9), pruned.documents);
    const selected_windows = try SelectedCursor.create(a, window_root, store, cached, .none, &subset);
    defer selected_windows.deinit();
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, selected_windows.nextBounded(&window_ids, 0));
    for (subset) |id| {
        const selected_window = (try selected_windows.nextBounded(&window_ids, max_segment_bytes)).?;
        try std.testing.expectEqual(@as(usize, 1), selected_window.identities.len);
        try std.testing.expectEqual(id, selected_window.identities[0]);
    }
    try std.testing.expect(try selected_windows.nextBounded(&window_ids, max_segment_bytes) == null);
    var visited: usize = 0;
    while (try windows.next(&window_ids)) |window| {
        try std.testing.expectEqual(@as(u64, visited), window.doc_offset);
        for (window.identities) |id| {
            try std.testing.expectEqualDeep(corpus.term_directory.identity(segments[visited].artifact_id), id);
            visited += 1;
        }
    }
    try std.testing.expectEqual(segments.len, visited);
    try std.testing.expect(try windows.next(&window_ids) == null);
    var byte_windows = try RoutingCursor.create(a, window_root, store, cached, .none);
    defer byte_windows.deinit();
    visited = 0;
    // Two segments exceed the byte target. The overflow record must survive
    // the next pull, including at the end of the authenticated routing tree.
    while (try byte_windows.nextBounded(&window_ids, max_segment_bytes)) |window| {
        try std.testing.expectEqual(@as(usize, 1), window.identities.len);
        try std.testing.expectEqual(@as(u64, visited), window.doc_offset);
        try std.testing.expectEqualDeep(corpus.term_directory.identity(segments[visited].artifact_id), window.identities[0]);
        visited += 1;
    }
    try std.testing.expectEqual(segments.len, visited);
    var invalid_root = window_root;
    invalid_root.collection.?.documents += 1;
    var invalid_cursor = try RoutingCursor.create(a, invalid_root, store, cached, .none);
    defer invalid_cursor.deinit();
    for (0..4) |_| _ = try invalid_cursor.next(&window_ids);
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, invalid_cursor.next(&window_ids));
    // Best-first batches use the same authenticated byte envelope and carry
    // an overflowing leaf without opening descriptors or dropping coverage.
    {
        var reads: u64 = 8 * 1024 * 1024;
        var writes: u64 = 8 * 1024 * 1024;
        var pages: pages_mod.PageStore = .{ .domain = root.domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
        const term = try corpus.statistics.keyAlloc(ca, "body", "common");
        const prefix = try corpus.term_directory.prefixAlloc(ca, term);
        var ordered: [9]Id = undefined;
        for (&ordered, segments) |*id, ref| id.* = corpus.term_directory.identity(ref.artifact_id);
        std.mem.sort(Id, &ordered, {}, struct {
            fn less(_: void, l: Id, r: Id) bool {
                return std.mem.order(u8, &l, &r) == .lt;
            }
        }.less);
        const Source = struct {
            prefix: []const u8,
            ids: []const Id,
            position: usize = 0,
            key_: [256]u8 = undefined,
            value: [corpus.statistics.Summary.max_bytes]u8 = undefined,
            pub fn next(self: *@This()) !?tree.Cursor.Record {
                if (self.position == self.ids.len) return null;
                @memcpy(self.key_[0..self.prefix.len], self.prefix);
                @memcpy(self.key_[self.prefix.len..][0..32], &self.ids[self.position]);
                self.position += 1;
                const summary: corpus.statistics.Summary = .{ .frequency = 1, .max_frequency = 1, .min_norm = 1 };
                return .{ .key = self.key_[0 .. self.prefix.len + 32], .value = summary.encode(&self.value) };
            }
        };
        var source: Source = .{ .prefix = prefix, .ids = &ordered };
        var reduced = pages.store();
        reduced.reduce = corpus.term_directory.reduce;
        var frontier_root = window_root;
        frontier_root.term_directory = .{ .version = 2, .leaves = try tree.buildSorted(a, reduced, &source) };
        var frequency: [8]u8 = undefined;
        std.mem.writeInt(u64, &frequency, 9, .big);
        frontier_root.term_statistics = try tree.apply(a, pages.store(), null, &.{.{ .key = term, .value = &frequency }});
        for ([_]u64{ max_segment_bytes, 32 * 1024 * 1024 }) |target| {
            const frontier = (try @import("lake_index_text_frontier.zig").create(a, frontier_root, store, cached, .none, .{ .term = .{ .field = "body", .term = "common" } }, .{}, .{})).?;
            defer frontier.deinit();
            var output: [3]Id = undefined;
            var seen: usize = 0;
            while (try frontier.nextWindow(&output, null, target)) |batch| {
                try std.testing.expectEqual(@min(if (target == max_segment_bytes) @as(usize, 1) else output.len, ordered.len - seen), batch.len);
                for (batch) |id| {
                    try std.testing.expectEqual(ordered[seen], id);
                    seen += 1;
                }
            }
            try std.testing.expectEqual(ordered.len, seen);
            try std.testing.expectEqual(@as(u64, 256 * 1024 * 1024) - frontier.reads + (8 * 1024 * 1024 - frontier.prefetch_reads), frontier.charged_bytes);
        }
    }
    // Broad admission reads only the compact routing tree. Physical metadata
    // and admissions stay behind authenticated references until consumed.
    var routing_only = root;
    routing_only.directories.segments = null;
    const full = try loadAll(ca, a, routing_only, store, cached, .none);
    try std.testing.expectEqual(segments.len, full.segments.len);
    try std.testing.expectEqual(@as(usize, 0), full.file_groups[0].rows.len);
    try std.testing.expectEqual(@as(usize, 0), full.file_groups[0].selected.len);
    var full_writer = try corpus.loadWriter(a, store, full, .none, cached, null);
    defer full_writer.deinit();
    const full_snapshot = full_writer.acquireSnapshot();
    defer full_snapshot.release();
    const full_ids = try @import("lake_index_text_predicate.zig").Identities.init(ca, full, full_snapshot);
    try store.delete(group.physical_manifest.?.artifact_id);
    var missing_rows: @import("lake_index_text_predicate.zig").LiveRowsCache = .{ .backing = a };
    defer missing_rows.deinit(a);
    try std.testing.expectError(error.FileNotFound, full_ids.physicalRow(a, &missing_rows, store, cached, 8, "source", "snapshot"));
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
    var rows: @import("lake_index_text_predicate.zig").LiveRowsCache = .{ .backing = a };
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
    const initial = try update(a, memory.store(), groups, refs, &.{}, &.{}, .{}, .none, std.testing.io);
    const initial_writes = memory.writes;
    const old = groups[1000];
    groups[1000].file.digest = @splat(2);
    const before_reads = memory.reads;
    const changed = try update(a, memory.store(), groups[1000..1001], refs[1000..1001], &.{old}, refs[1000..1001], initial, .none, std.testing.io);
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

test "external lake segment span routing advances linearly across fragmented physical blocks" {
    const a = std.testing.allocator;
    const blocks = try a.alloc(corpus.physical.Block, 1000);
    defer a.free(blocks);
    for (blocks, 0..) |*block, i| block.* = .{ .group = 0, .high = i, .base = @intCast(i * 13), .lower = 5, .count = 13 };
    const routes = try a.alloc(corpus.SegmentRouting, 1300);
    defer a.free(routes);
    const chunk: Chunk = .{ .artifact_id = "unused", .checksum = "unused", .byte_len = 1 };
    for (routes) |*route| route.* = .{ .ref = chunk, .summary = null, .bytes = 1, .doc_count = 10, .fields = &.{} };
    var records: Segments = .{ .group = .{ .file = .{ .id = "fragmented", .digest = @splat(1) }, .segments = &.{}, .rows = blocks }, .manifest = chunk, .routes = routes };
    var count: usize = 0;
    while (try records.next()) |record| : (count += 1) {
        try std.testing.expectEqual(@as(u32, @intCast(count * 10)), record.interval.lower);
        var expected: usize = 0;
        for (blocks) |block| if (block.base < record.interval.lower + 10 and @as(u64, block.base) + block.count > record.interval.lower) {
            try std.testing.expectEqualDeep(block, record.rows[expected]);
            expected += 1;
        };
        try std.testing.expectEqual(expected, record.rows.len);
    }
    try std.testing.expectEqual(routes.len, count);
    try std.testing.expect(records.blocks_examined < 6 * (blocks.len + routes.len));
}

/// Streaming compact navigation for broad queries. Only the authenticated tree
/// path, one fragmented routing record and the caller's segment window survive
/// a pull. The ordinal base preserves full-corpus score ties across windows.
pub const RoutingCursor = struct {
    a: A,
    scratch: std.heap.ArenaAllocator,
    reader: Reader,
    cursor: tree.Cursor = undefined,
    open: bool = false,
    previous: ?[36]u8 = null,
    bytes: std.ArrayList(u8) = .empty,
    root: corpus.Root,
    emitted: u32 = 0,
    documents: u64 = 0,
    last_file: ?Id = null,
    last_digest: Id = undefined,
    last_manifest: Id = undefined,
    last_physical: Id = undefined,
    last_ordinal: u32 = 0,
    last_upper: u32 = 0,
    done: bool = false,
    last_segment_bytes: u64 = 0,
    last_segment_docs: u32 = 0,
    pending: ?struct { identity: Id, bytes: u64, documents: u32 } = null,
    filter: ?RouteFilter = null,
    included: bool = true,
    pruned: u32 = 0,
    pub const Window = struct { identities: []const Id, doc_offset: u64 };
    pub fn create(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Cancellation) !*RoutingCursor {
        if (root.inventory_version != 3 or root.directories.routing == null or root.collection == null) return error.InvalidNativeLakeTextCorpus;
        const self = try a.create(RoutingCursor);
        self.* = .{ .a = a, .scratch = .init(a), .reader = .{ .store = store, .cached = cached, .token = token }, .root = root };
        errdefer self.deinit();
        self.reader.init(root.domain);
        self.reader.pages.streaming = true;
        self.cursor = try tree.Cursor.init(a, self.reader.pages.store(), root.directories.routing, "", null);
        self.open = true;
        return self;
    }
    pub fn deinit(self: *RoutingCursor) void {
        if (self.open) self.cursor.deinit();
        self.bytes.deinit(self.a);
        self.scratch.deinit();
        self.a.destroy(self);
    }
    fn emit(self: *RoutingCursor, key_: [36]u8) !Id {
        _ = self.scratch.reset(.retain_capacity);
        const record = try parseSegment(self.scratch.allocator(), self.bytes.items, .{ .allocate = .alloc_always });
        const file = corpus.term_directory.identity(record.file.id);
        if (!std.mem.eql(u8, key_[0..32], &file) or std.mem.readInt(u32, key_[32..36], .big) != record.ordinal) return error.InvalidNativeLakeTextCorpus;
        if (record.rows.len != 0 or (record.physical == null and record.sources.len == 0 and record.source_pages.len == 0)) return error.InvalidNativeLakeTextCorpus;
        try validateRouting(record, self.root.domain);
        const manifest_id = corpus.term_directory.identity(record.manifest.artifact_id);
        const physical_id = corpus.term_directory.identity((record.physical orelse record.manifest).artifact_id);
        const same_file = if (self.last_file) |last| std.mem.eql(u8, &file, &last) else false;
        if (record.ordinal != (if (same_file) std.math.add(u32, self.last_ordinal, 1) catch return error.InvalidNativeLakeTextCorpus else @as(u32, 0)) or
            record.interval.lower != (if (same_file) self.last_upper else @as(u32, 0))) return error.InvalidNativeLakeTextCorpus;
        if (same_file and (!std.mem.eql(u8, &record.file.digest, &self.last_digest) or
            !std.mem.eql(u8, &manifest_id, &self.last_manifest) or !std.mem.eql(u8, &physical_id, &self.last_physical))) return error.InvalidNativeLakeTextCorpus;
        self.last_file = file;
        self.last_digest = record.file.digest;
        self.last_manifest = manifest_id;
        self.last_physical = physical_id;
        self.last_ordinal = record.ordinal;
        self.last_upper = try std.math.add(u32, record.interval.lower, record.interval.count);
        self.last_segment_bytes = segmentPlanningBytes(record);
        self.last_segment_docs = record.route.doc_count;
        self.emitted = std.math.add(u32, self.emitted, 1) catch return error.InvalidNativeLakeTextCorpus;
        self.documents = std.math.add(u64, self.documents, record.route.doc_count) catch return error.InvalidNativeLakeTextCorpus;
        if (self.emitted > self.root.segment_count) return error.InvalidNativeLakeTextCorpus;
        self.included = if (self.filter) |filter| try filter.keep(filter.ptr, record.route) else true;
        if (!self.included) self.pruned += 1;
        return corpus.term_directory.identity(record.route.ref.artifact_id);
    }
    pub fn next(self: *RoutingCursor, output: []Id) !?Window {
        return self.nextBounded(output, std.math.maxInt(u64));
    }
    /// Limit encoded admission as well as segment count. One indivisible
    /// segment can exceed the target; it is always admitted alone. A carried
    /// record owns only its identity/counts, never a decoded file manifest.
    pub fn nextBounded(self: *RoutingCursor, output: []Id, max_bytes: u64) !?Window {
        if (output.len == 0 or max_bytes == 0) return error.InvalidNativeLakeTextCorpus;
        try self.reader.check();
        var offset = self.documents;
        var count: usize = 0;
        var bytes: u64 = 0;
        var gap = false;
        if (self.pending) |held| {
            offset -= held.documents;
            output[0] = held.identity;
            count = 1;
            bytes = held.bytes;
            self.pending = null;
        }
        if (self.done) return if (count == 0) null else .{ .identities = output[0..count], .doc_offset = offset };
        if (count == output.len or bytes >= max_bytes) return .{ .identities = output[0..count], .doc_offset = offset };
        while (try self.cursor.next()) |entry| {
            if (entry.key.len != 40) return error.InvalidNativeLakeTextCorpus;
            const current: [36]u8 = entry.key[0..36].*;
            if (self.previous) |previous| if (!std.mem.eql(u8, &previous, &current)) {
                const identity = try self.emit(previous);
                if (!self.included) {
                    gap = count != 0;
                } else if (count != 0 and self.last_segment_bytes > max_bytes -| bytes) {
                    self.pending = .{ .identity = identity, .bytes = self.last_segment_bytes, .documents = self.last_segment_docs };
                } else {
                    if (count == 0) offset = self.documents - self.last_segment_docs;
                    output[count] = identity;
                    count += 1;
                    bytes +|= self.last_segment_bytes;
                }
                self.bytes.clearRetainingCapacity();
            };
            self.previous = current;
            if (std.mem.readInt(u32, entry.key[36..40], .big) != self.bytes.items.len or entry.value.len == 0 or entry.value.len > chunk_bytes or entry.value.len > corpus.max_root_bytes -| self.bytes.items.len) return error.InvalidNativeLakeTextCorpus;
            try self.bytes.appendSlice(self.a, entry.value);
            if (gap or self.pending != null or count == output.len or bytes >= max_bytes) return .{ .identities = output[0..count], .doc_offset = offset };
        }
        if (self.previous) |last| {
            const identity = try self.emit(last);
            self.previous = null;
            if (self.included and count != 0 and self.last_segment_bytes > max_bytes -| bytes) {
                self.pending = .{ .identity = identity, .bytes = self.last_segment_bytes, .documents = self.last_segment_docs };
            } else if (self.included) {
                if (count == 0) offset = self.documents - self.last_segment_docs;
                output[count] = identity;
                count += 1;
            }
        }
        if (self.emitted != self.root.segment_count or self.documents != self.root.collection.?.documents) return error.InvalidNativeLakeTextCorpus;
        self.done = true;
        return if (count == 0) null else .{ .identities = output[0..count], .doc_offset = offset };
    }
};

/// Logical position stays independent of admission/traversal order. This compact
/// pair has the same order as a full immutable routing walk, without summing all
/// earlier files' documents or admitting their inventories.
pub const Position = struct { file: Id, rank: u32, recent: bool = false };

/// A proved subset can span many windows. Never widen it to the full archive
/// or admit every selected segment simply because it is a small percentage.
pub const SelectedCursor = struct {
    a: A,
    budget: local.sql_memory_budget,
    scratch: std.heap.ArenaAllocator = undefined,
    reader: Reader,
    root: corpus.Root,
    identities: []const Id,
    position: usize = 0,
    pending_bytes: ?u64 = null,
    filter: ?RouteFilter = null,
    pruned: u32 = 0,
    pub fn create(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: ?artifacts.CachedRead, token: Cancellation, identities: []const Id) !*SelectedCursor {
        const self = try a.create(SelectedCursor);
        self.* = .{ .a = a, .budget = .{ .backing = a, .limit = 8 * 1024 * 1024 }, .reader = .{ .store = store, .cached = cached, .token = token }, .root = root, .identities = identities };
        self.scratch = .init(self.budget.allocator());
        self.reader.init(root.domain);
        return self;
    }
    pub fn deinit(self: *SelectedCursor) void {
        const a = self.a;
        self.scratch.deinit();
        a.destroy(self);
    }
    pub fn nextBounded(self: *SelectedCursor, output: []Id, max_bytes: u64) !?RoutingCursor.Window {
        if (output.len == 0 or max_bytes == 0) return error.InvalidNativeLakeTextCorpus;
        try self.reader.check();
        var count: usize = 0;
        var bytes: u64 = 0;
        while (count < output.len and self.position < self.identities.len) {
            try self.reader.check();
            const id = self.identities[self.position];
            const size = self.pending_bytes orelse blk: {
                _ = self.scratch.reset(.retain_capacity);
                break :blk (try selectedPlanningBytes(self.scratch.allocator(), self.reader.pages.store(), self.root, id, self.filter)) orelse {
                    self.position += 1;
                    self.pruned += 1;
                    continue;
                };
            };
            if (count != 0 and size > max_bytes -| bytes) {
                self.pending_bytes = size;
                break;
            }
            self.pending_bytes = null;
            output[count] = id;
            count += 1;
            self.position += 1;
            bytes +|= size;
            if (bytes >= max_bytes) break;
        }
        return if (count == 0) null else .{ .identities = output[0..count], .doc_offset = 0 };
    }
};

test "external lake serving metadata additions override late deletions within and across spill runs" {
    const a = std.testing.allocator;
    var temporary = try local.common_test_directory.TestDirectory.init("serving-directory-addition-priority");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(7), 1, std.testing.io);
    var publisher: Publisher = undefined;
    try publisher.init(a, &store, .none, std.testing.io);
    defer publisher.deinit();
    publisher.preferAdditions();
    const id = corpus.term_directory.identity("replacement");
    const removed = corpus.term_directory.identity("removed");
    try publisher.files.add(id, "replacement", 11);
    try publisher.files.add(id, null, 11);
    try publisher.files.add(id, null, 11);
    try publisher.files.add(removed, null, 3);
    try std.testing.expectEqual(@as(u64, 0), publisher.writes);
    try std.testing.expectEqual(@as(u64, 0), publisher.reads);
    try std.testing.expectEqual(@as(usize, 0), publisher.manager.files);
    try publisher.files.add(id, "newest", 6);
    try publisher.files.add(id, null, 6);
    const root = (try publisher.files.finish(null)).?;
    try std.testing.expectEqual(@as(u64, 1), root.records);
    var cursor = try tree.Cursor.init(a, publisher.pages.store(), root, "", null);
    defer cursor.deinit();
    try std.testing.expectEqualStrings("newest", (try cursor.next()).?.value);
    try std.testing.expect(try cursor.next() == null);
}

test "external lake private metadata streams one bounded reduction under a live spill quota" {
    const a = std.testing.allocator;
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var context: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = Check.call, .max_bytes = 4 * 1024 * 1024, .async_writes = false };
    defer manager.deinit();
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    {
        var mutations = Mutations.init(a, &manager, memory.store());
        defer mutations.deinit();
        mutations.sort.memory_bytes = 16 * 1024;
        const value: [128]u8 = @splat(42);
        for (0..4096) |i| {
            var id: Id = @splat(0);
            std.mem.writeInt(u64, id[24..32], i, .big);
            try mutations.add(id, &value, 0);
        }
        const root = (try mutations.finish(null)).?;
        var cursor = try tree.Cursor.init(a, memory.store(), root, "", null);
        defer cursor.deinit();
        var count: usize = 0;
        while (try cursor.next()) |record| : (count += 1) {
            try std.testing.expectEqual(count, std.mem.readInt(u64, record.key[24..32], .big));
            try std.testing.expectEqualSlices(u8, &value, record.value);
        }
        try std.testing.expectEqual(@as(usize, 4096), count);
        // Cumulative merge writes exceed the live quota. Consumed inputs
        // are recycled in shared extents instead of accumulating on disk.
        try std.testing.expect(manager.written_bytes > manager.max_bytes);
        try std.testing.expect(manager.peak_bytes <= manager.max_bytes);
        try std.testing.expect(manager.peak_files < 8);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}

/// A bounded fan-in over complete physical-layout cohorts. Incompatible runs
/// are consumed exactly; compatible cohorts keep independent monotone cutoffs.
const CohortCursor = struct {
    const directory = @import("lake_index_sort_directory.zig");
    const Stream = struct {
        owner: *CohortCursor,
        fields_arena: std.heap.ArenaAllocator,
        fields: []const local.segment.SegmentIndexSortField = &.{},
        lower: [33]u8 = undefined,
        upper: [33]u8 = undefined,
        cursor: ?tree.Cursor = null,
        arena: std.heap.ArenaAllocator,
        head: ?directory.Record = null,
        compatible: bool = false,
        done: bool = false,
    };
    a: A,
    budget: local.sql_memory_budget,
    reader: Reader,
    streams: [directory.max_cohorts]Stream = undefined,
    count: usize = 0,
    order: local.storage_db_query_search_exec.SearchHitOrder,
    selected: ?[]const Id,
    scratch: std.heap.ArenaAllocator,
    pruned: bool = false,
    cursor_pruned: u64 = 0,
    cursor_pruned_subtrees: u64 = 0,
    fn create(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Cancellation, order: local.storage_db_query_search_exec.SearchHitOrder, selected: ?[]const Id) !?*CohortCursor {
        const ref = root.directories.ordered orelse return null;
        if (ref.records != @as(u64, root.segment_count) * 2) return null;
        if (selected) |ids| {
            if (ids.len * 4 < root.segment_count) return null;
            for (ids, 0..) |id, i| if (i != 0 and std.mem.order(u8, &ids[i - 1], &id) != .lt) return null;
        }
        const self = try a.create(CohortCursor);
        self.* = .{ .a = a, .budget = .{ .backing = local.sql_memory_budget.reclaimingAllocator(a), .limit = 8 * 1024 * 1024 }, .reader = .{ .store = store, .cached = cached, .token = token }, .order = order, .selected = selected, .scratch = .init(a) };
        const admitted = self.admit(root, ref) catch |err| {
            const optional = err == error.OutOfMemory and self.budget.isLocallyExhausted();
            self.deinit();
            if (optional) return null;
            return err;
        };
        if (!admitted) {
            self.deinit();
            return null;
        }
        return self;
    }
    fn admit(self: *CohortCursor, root: corpus.Root, ref: tree.Ref) !bool {
        self.reader.init(root.domain);
        const alloc = self.budget.allocator();
        // Discover cohort prefixes with tree seeks; no aggregate grows with
        // layout diversity. The bounded fan-in may decline without affecting publication.
        var cohorts: [directory.max_cohorts]directory.Cohort = undefined;
        var cohort_count: usize = 0;
        var discovery = try tree.Cursor.init(alloc, self.reader.pages.store(), ref, "", null);
        defer discovery.deinit();
        var records: u64 = 0;
        while (try discovery.next()) |entry| {
            if (entry.key.len < 65) return error.InvalidNativeLakeTextCorpus;
            if (cohort_count == cohorts.len) return false;
            const shape = entry.key[0..32].*;
            var upper = shape;
            var pos: usize = upper.len;
            var carry = true;
            while (pos != 0) {
                pos -= 1;
                upper[pos] +%= 1;
                if (upper[pos] != 0) {
                    carry = false;
                    break;
                }
            }
            const count = try tree.countRange(alloc, self.reader.pages.store(), ref, &shape, if (carry) null else &upper);
            if (count == 0 or count % 2 != 0) return error.InvalidNativeLakeTextCorpus;
            cohorts[cohort_count] = .{ .shape = shape, .records = count };
            cohort_count += 1;
            records = try std.math.add(u64, records, count);
            if (carry) break;
            try discovery.seekForward(&upper);
        }
        if (records != ref.records) return error.InvalidNativeLakeTextCorpus;
        for (cohorts[0..cohort_count]) |cohort| {
            const stream = &self.streams[self.count];
            stream.* = .{ .owner = self, .arena = .init(alloc), .fields_arena = .init(alloc) };
            self.count += 1;
            @memcpy(stream.lower[0..32], &cohort.shape);
            @memcpy(stream.upper[0..32], &cohort.shape);
            stream.lower[32] = 0;
            stream.upper[32] = 1;
            var sample = try tree.Cursor.init(alloc, self.reader.pages.store(), ref, &stream.lower, &stream.upper);
            defer sample.deinit();
            const entry = (try sample.next()) orelse return error.InvalidNativeLakeTextCorpus;
            const record = try directory.decode(stream.arena.allocator(), entry.value);
            if (record.bound) |bound| {
                const fa = stream.fields_arena.allocator();
                const encoded = try std.json.Stringify.valueAlloc(fa, bound.fields, .{});
                stream.fields = try std.json.parseFromSliceLeaky([]const local.segment.SegmentIndexSortField, fa, encoded, .{ .allocate = .alloc_always });
            }
            var reverse = false;
            if (record.bound) |bound| if (try bound.reversal(stream.arena.allocator(), self.order)) |reversal| {
                stream.compatible = !std.mem.allEqual(u8, &cohort.shape, 0);
                reverse = reversal != (self.order.effective.req.search_before.len != 0);
            };
            stream.lower[32] = @intFromBool(reverse);
            stream.upper[32] = stream.lower[32] + 1;
            stream.cursor = if (reverse) try tree.Cursor.initReverse(alloc, self.reader.pages.store(), ref, &stream.lower, &stream.upper) else try tree.Cursor.init(alloc, self.reader.pages.store(), ref, &stream.lower, &stream.upper);
            stream.cursor.?.pruning = .{ .ptr = stream, .keep = keepSubtree };
            try self.pull(stream);
        }
        return true;
    }
    fn keepSubtree(raw: *anyopaque, bytes: []const u8) !bool {
        const stream: *Stream = @ptrCast(@alignCast(raw));
        if (!stream.compatible) return true;
        const witness = (try directory.RangeSummary.read(bytes)) orelse return true;
        if (!witness.complete or !witness.uniform or witness.reverse != (stream.lower[32] != 0) or !std.mem.eql(u8, &witness.shape, stream.lower[0..32])) return true;
        var region = std.heap.ArenaAllocator.init(stream.owner.budget.allocator());
        defer region.deinit();
        const tuple = try @import("lake_ordering_keys.zig").decodeIntoArena(region.allocator(), witness.tuple);
        const keep = try stream.owner.order.nativeTupleMayReachCursor(stream.fields, tuple.keys);
        if (!keep) {
            stream.owner.cursor_pruned += witness.records;
            stream.owner.cursor_pruned_subtrees += 1;
            stream.owner.pruned = true;
        }
        return keep;
    }
    fn pull(self: *CohortCursor, stream: *Stream) !void {
        stream.head = null;
        if (stream.done) return;
        try self.reader.check();
        _ = stream.arena.reset(.retain_capacity);
        while (true) {
            const entry = (try stream.cursor.?.next()) orelse {
                stream.done = true;
                return;
            };
            const record = try directory.decode(stream.arena.allocator(), entry.value);
            if (entry.key.len < 65 or !std.mem.eql(u8, entry.key[0..32], stream.lower[0..32]) or !std.mem.eql(u8, entry.key[entry.key.len - 32 ..], &record.id) or record.planning_bytes == 0) return error.InvalidNativeLakeTextCorpus;
            if (stream.compatible) if (record.bound) |bound| {
                const far = if (stream.lower[32] != 0) bound.first else bound.last;
                const tuple = try stream.arena.allocator().alloc(local.segment.SegmentIndexSortBoundValue, far.len);
                for (tuple, far) |*tuple_key, value| tuple_key.* = try value.native();
                if (!try self.order.nativeTupleMayReachCursor(bound.fields, tuple)) {
                    self.cursor_pruned += 1;
                    self.pruned = true;
                    _ = stream.arena.reset(.retain_capacity);
                    continue;
                }
            };
            stream.head = record;
            return;
        }
    }
    fn best(self: *CohortCursor) !?usize {
        var result: ?usize = null;
        for (self.streams[0..self.count], 0..) |stream, i| if (stream.head) |head| {
            // Unknown layouts cannot be stopped by a cohort endpoint proof.
            if (!stream.compatible) return i;
            if (result == null) {
                result = i;
                continue;
            }
            _ = self.scratch.reset(.retain_capacity);
            if (try head.bound.?.compareBest(self.streams[result.?].head.?.bound.?, self.scratch.allocator(), self.order) == .lt) result = i;
        };
        return result;
    }
    fn selectedId(self: *CohortCursor, id: Id) bool {
        const ids = self.selected orelse return true;
        var low: usize = 0;
        var high = ids.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (std.mem.order(u8, &ids[mid], &id) == .lt) low = mid + 1 else high = mid;
        }
        return low < ids.len and std.mem.eql(u8, &ids[low], &id);
    }
    fn next(self: *CohortCursor, output: []Id, max_bytes: u64, boundary: []const std.json.Value) !?RoutingCursor.Window {
        if (output.len == 0 or max_bytes == 0) return error.InvalidNativeLakeTextCorpus;
        var count: usize = 0;
        var bytes: u64 = 0;
        while (count < output.len and bytes < max_bytes) {
            const i = (try self.best()) orelse break;
            const stream = &self.streams[i];
            const record = stream.head.?;
            _ = self.scratch.reset(.retain_capacity);
            if (record.bound) |bound| if (!try bound.mayCompete(self.scratch.allocator(), self.order, boundary)) {
                self.pruned = true;
                if (stream.compatible) {
                    stream.head = null;
                    stream.done = true;
                } else try self.pull(stream);
                continue;
            };
            if (!self.selectedId(record.id)) {
                try self.pull(stream);
                continue;
            }
            if (count != 0 and record.planning_bytes > max_bytes -| bytes) break;
            output[count] = record.id;
            count += 1;
            bytes +|= record.planning_bytes;
            try self.pull(stream);
        }
        return if (count == 0) null else .{ .identities = output[0..count], .doc_offset = 0 };
    }
    fn deinit(self: *CohortCursor) void {
        for (self.streams[0..self.count]) |*stream| {
            if (stream.cursor) |*cursor| cursor.deinit();
            stream.arena.deinit();
            stream.fields_arena.deinit();
        }
        self.scratch.deinit();
        self.a.destroy(self);
    }
};

/// Complete ordered endpoint cover. The page-tree order itself permits stopping
/// without fetching later routing records, descriptors or lineage pages.
pub const SortCursor = struct {
    a: A,
    reader: Reader,
    scratch: std.heap.ArenaAllocator,
    root: corpus.Root,
    order: local.storage_db_query_search_exec.SearchHitOrder,
    cursor: ?tree.Cursor = null,
    selected: ?[]const Id,
    lower: [1]u8 = .{0},
    upper: [1]u8 = .{1},
    pending: ?struct { id: Id, bytes: u64 } = null,
    done: bool = false,
    pruned: bool = false,
    cohorts: ?*CohortCursor = null,
    pub fn create(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Cancellation, order: local.storage_db_query_search_exec.SearchHitOrder, selected: ?[]const Id) !?*SortCursor {
        if (root.directories.ordered_format != 0) {
            const groups = (try CohortCursor.create(a, root, store, cached, token, order, selected)) orelse return null;
            errdefer groups.deinit();
            const self = try a.create(SortCursor);
            self.* = .{ .a = a, .reader = .{ .store = store, .cached = cached, .token = token }, .scratch = .init(a), .root = root, .order = order, .selected = selected, .cohorts = groups };
            return self;
        }
        const ref = root.directories.ordered orelse return null;
        if (ref.records != @as(u64, root.segment_count) * 2) return null;
        // A sparse selection should seek its own routing facts, not walk an
        // unrelated global prefix. Broad selections can reuse ordered pages.
        if (selected) |ids| {
            if (ids.len * 4 < root.segment_count) return null;
            for (ids, 0..) |id, i| if (i != 0 and std.mem.order(u8, &ids[i - 1], &id) != .lt) return null;
        }
        const self = try a.create(SortCursor);
        self.* = .{ .a = a, .reader = .{ .store = store, .cached = cached, .token = token }, .scratch = .init(a), .root = root, .order = order, .selected = selected };
        var owned = true;
        defer if (owned) self.deinit();
        self.reader.init(root.domain);
        var page = try tree.readNavigationPage(a, self.reader.pages.store(), ref);
        defer page.deinit(a);
        if (page.aggregate.len != 32 or std.mem.allEqual(u8, page.aggregate, 0)) return null;
        var first = try tree.Cursor.init(a, self.reader.pages.store(), ref, "", null);
        defer first.deinit();
        const sample = (try first.next()) orelse return error.InvalidNativeLakeTextCorpus;
        const record = try @import("lake_index_sort_directory.zig").decodeLegacy(self.scratch.allocator(), sample.value);
        const bound = record.bound orelse return null;
        const reversal = (try bound.reversal(self.scratch.allocator(), order)) orelse return null;
        const reverse = reversal != (order.effective.req.search_before.len != 0);
        self.lower[0] = @intFromBool(reverse);
        self.upper[0] = self.lower[0] + 1;
        self.cursor = if (reverse) try tree.Cursor.initReverse(a, self.reader.pages.store(), ref, &self.lower, &self.upper) else try tree.Cursor.init(a, self.reader.pages.store(), ref, &self.lower, &self.upper);
        owned = false;
        return self;
    }
    pub fn deinit(self: *SortCursor) void {
        if (self.cohorts) |value| value.deinit();
        if (self.cursor) |*cursor| cursor.deinit();
        self.scratch.deinit();
        self.a.destroy(self);
    }
    pub fn next(self: *SortCursor, output: []Id, max_bytes: u64, boundary: []const std.json.Value) !?RoutingCursor.Window {
        if (self.cohorts) |value| {
            defer self.pruned = value.pruned;
            return value.next(output, max_bytes, boundary);
        }
        if (self.done and self.pending == null) return null;
        if (output.len == 0 or max_bytes == 0) return error.InvalidNativeLakeTextCorpus;
        var count: usize = 0;
        var bytes: u64 = 0;
        if (self.pending) |held| {
            output[0] = held.id;
            count = 1;
            bytes = held.bytes;
            self.pending = null;
        }
        while (!self.done and count < output.len and bytes < max_bytes) {
            const entry = (try self.cursor.?.next()) orelse {
                self.done = true;
                break;
            };
            try self.reader.check();
            _ = self.scratch.reset(.retain_capacity);
            const a = self.scratch.allocator();
            const record = try @import("lake_index_sort_directory.zig").decodeLegacy(a, entry.value);
            if (entry.key.len < 33 or !std.mem.eql(u8, entry.key[entry.key.len - 32 ..], &record.id)) return error.InvalidNativeLakeTextCorpus;
            const bound = record.bound orelse return error.InvalidNativeLakeTextCorpus;
            if (!try bound.mayCompete(a, self.order, boundary)) {
                self.pruned = true;
                self.done = true;
                break;
            }
            if (self.selected) |ids| {
                var lower: usize = 0;
                var upper = ids.len;
                while (lower < upper) {
                    const middle = lower + (upper - lower) / 2;
                    if (std.mem.order(u8, &ids[middle], &record.id) == .lt) lower = middle + 1 else upper = middle;
                }
                if (lower == ids.len or !std.mem.eql(u8, &ids[lower], &record.id)) continue;
            }
            if (record.planning_bytes == 0) return error.InvalidNativeLakeTextCorpus;
            if (count != 0 and record.planning_bytes > max_bytes -| bytes) {
                self.pending = .{ .id = record.id, .bytes = record.planning_bytes };
                break;
            }
            output[count] = record.id;
            count += 1;
            bytes +|= record.planning_bytes;
        }
        return if (count == 0) null else .{ .identities = output[0..count], .doc_offset = 0 };
    }
};

test "external lake global frontier sort directory visits best ranges and stops before dominated leaves" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("best-sort-directory");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(51), 1, std.testing.io);
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: pages_mod.PageStore = .{ .domain = store.upload_scope.?.domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    pages.admitPublication();
    const Check = struct {
        fn run(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.run };
    defer manager.deinit();
    var changes = Mutations.init(a, &manager, pages.store());
    defer changes.deinit();
    changes.reduction = @import("lake_index_sort_directory.zig").reduce;
    var ids: [512]Id = undefined;
    const Bound = @import("lake_index_sort_bounds.zig").Bound;
    for (&ids, 0..) |*id, i| {
        var upload_store = store;
        upload_store.allocator = out;
        const uploaded = try upload_store.put(try std.fmt.allocPrint(out, "run-{d}", .{i}));
        const ref: Chunk = .{ .artifact_id = uploaded.artifact_id, .checksum = uploaded.checksum, .byte_len = uploaded.byte_len };
        id.* = corpus.term_directory.identity(ref.artifact_id);
        const values = try out.dupe(Bound.Value, &.{ .{ .kind = .i64_val, .text = try std.fmt.allocPrint(out, "{d}", .{i}) }, .{ .kind = .id, .text = "private" } });
        const bound: Bound = .{ .fields = &.{ .{ .field = "rank" }, .{ .field = "_id" } }, .first = values, .last = values };
        try @import("lake_index_sort_directory.zig").mutate(out, &changes, .{ .ref = ref, .summary = null, .bytes = 1, .doc_count = 1, .fields = &.{}, .sort_bound = bound }, 1, false);
    }
    const ref = (try changes.finish(null)).?;
    var root: corpus.Root = .{ .domain = pages.domain, .binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "cut", .schema_fingerprint = "schema", .index_config_hash = "text", .column_bindings = &.{} }, .config_json = "{}", .segment_count = ids.len, .directories = .{ .ordered = ref, .ordered_format = 1 } };
    var parsed = try local.schema_mod.parseValidatedTableSchema(out,
        \\{"default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"rank":{"type":"integer","x-antfly-field":{"type":"number","sortable":true,"missing_null_policy":"nulls_ordered"}}}}}}}
    );
    defer parsed.deinit(out);
    const schema = try local.schema_mod.deriveRuntimeTableSchema(out, parsed);
    var serving_cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer serving_cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &serving_cache, .scope = @splat(7), .context = .{ .io = std.testing.io } };
    for ([_]bool{ false, true }) |descending| for ([_]bool{ false, true }) |before| {
        var order = try local.storage_db_query_search_exec.SearchHitOrder.init(out, .{ .order_by = &.{.{ .field = "rank", .desc = descending }}, .search_before = if (before) &.{ .{ .integer = 256 }, .{ .string = "public" } } else &.{} }, schema);
        defer order.deinit(out);
        const cursor = (try SortCursor.create(a, root, store, cached, .none, order, null)).?;
        defer cursor.deinit();
        var output: [1]Id = undefined;
        const first = (try cursor.next(&output, 32 * 1024 * 1024, &.{})).?;
        const reverse = descending != before;
        try std.testing.expectEqual(ids[if (before) 256 else if (reverse) 511 else 0], first.identities[0]);
        const cutoff = [_]std.json.Value{ .{ .integer = if (before) 256 else if (reverse) 511 else 0 }, .{ .string = "public" } };
        try std.testing.expect(try cursor.next(&output, 32 * 1024 * 1024, &cutoff) == null);
        try std.testing.expect(cursor.pruned);
        if (before) {
            try std.testing.expect(cursor.cohorts.?.cursor_pruned >= 255);
            try std.testing.expect(cursor.cohorts.?.cursor_pruned_subtrees > 0);
            std.debug.print("CURSOR_RANGES excluded_records={d} excluded_subtrees={d}\n", .{ cursor.cohorts.?.cursor_pruned, cursor.cohorts.?.cursor_pruned_subtrees });
        }
        const batched = (try SortCursor.create(a, root, store, cached, .none, order, null)).?;
        defer batched.deinit();
        var batch: [3]Id = undefined;
        const first_batch = (try batched.next(&batch, 2, &.{})).?;
        try std.testing.expectEqual(@as(usize, 2), first_batch.identities.len);
        try std.testing.expectEqual(ids[if (before) 256 else if (reverse) 511 else 0], first_batch.identities[0]);
        try std.testing.expectEqual(ids[if (before) (if (reverse) 255 else 257) else if (reverse) 510 else 1], first_batch.identities[1]);
        const second_batch = (try batched.next(&batch, 1, &.{})).?;
        try std.testing.expectEqual(@as(usize, 1), second_batch.identities.len);
        try std.testing.expectEqual(ids[if (before) (if (reverse) 254 else 258) else if (reverse) 509 else 2], second_batch.identities[0]);
    };

    // An interval that starts before the cursor can still contain later rows.
    // Keep it even while excluding whole pages of earlier singleton ranges.
    {
        var overlap = Mutations.init(a, &manager, pages.store());
        defer overlap.deinit();
        overlap.reduction = @import("lake_index_sort_directory.zig").reduce;
        var backend = store;
        backend.allocator = out;
        const uploaded = try backend.put("overlapping-range");
        const overlap_ref: Chunk = .{ .artifact_id = uploaded.artifact_id, .checksum = uploaded.checksum, .byte_len = uploaded.byte_len };
        const low = try out.dupe(Bound.Value, &.{ .{ .kind = .i64_val, .text = "0" }, .{ .kind = .id, .text = "first" } });
        const high = try out.dupe(Bound.Value, &.{ .{ .kind = .i64_val, .text = "1000" }, .{ .kind = .id, .text = "last" } });
        try @import("lake_index_sort_directory.zig").mutate(out, &overlap, .{ .ref = overlap_ref, .summary = null, .bytes = 1, .doc_count = 2, .fields = &.{}, .sort_bound = .{ .fields = &.{ .{ .field = "rank" }, .{ .field = "_id" } }, .first = low, .last = high } }, 1, false);
        var overlap_root = root;
        overlap_root.directories.ordered = try overlap.finish(root.directories.ordered);
        overlap_root.segment_count += 1;
        const boundary = [_]std.json.Value{ .{ .integer = 600 }, .{ .string = "public" } };
        for ([_]bool{ false, true }) |before| {
            var overlap_order = try local.storage_db_query_search_exec.SearchHitOrder.init(out, .{ .order_by = &.{.{ .field = "rank", .desc = before }}, .search_after = if (before) &.{} else &boundary, .search_before = if (before) &boundary else &.{} }, schema);
            defer overlap_order.deinit(out);
            const cursor = (try SortCursor.create(a, overlap_root, store, cached, .none, overlap_order, null)).?;
            defer cursor.deinit();
            var output: [1]Id = undefined;
            try std.testing.expectEqual(corpus.term_directory.identity(overlap_ref.artifact_id), (try cursor.next(&output, 1, &.{})).?.identities[0]);
            try std.testing.expect(try cursor.next(&output, 1, &.{}) == null);
            try std.testing.expect(cursor.pruned);
            try std.testing.expect(cursor.cohorts.?.cursor_pruned_subtrees > 0);
        }
    }

    var extra = Mutations.init(a, &manager, pages.store());
    defer extra.deinit();
    extra.reduction = @import("lake_index_sort_directory.zig").reduce;
    var additions: [3]Id = undefined;
    for (&additions, 0..) |*id, i| {
        var backend = store;
        backend.allocator = out;
        const uploaded = try backend.put(try std.fmt.allocPrint(out, "mixed-cohort-{d}", .{i}));
        const item: Chunk = .{ .artifact_id = uploaded.artifact_id, .checksum = uploaded.checksum, .byte_len = uploaded.byte_len };
        id.* = corpus.term_directory.identity(item.artifact_id);
        const values = try out.dupe(Bound.Value, &.{ .{ .kind = if (i == 1) .null_value else .i64_val, .text = if (i == 1) "" else "1000" }, .{ .kind = .id, .text = "private" } });
        const fields = try out.dupe(local.segment.SegmentIndexSortField, &.{ .{ .field = "rank", .desc = i == 0, .nulls_complete = true }, .{ .field = "_id" } });
        const bound: ?Bound = if (i == 2) null else .{ .fields = fields, .first = values, .last = values };
        try @import("lake_index_sort_directory.zig").mutate(out, &extra, .{ .ref = item, .summary = null, .bytes = 1, .doc_count = 1, .fields = &.{}, .sort_bound = bound }, 1, false);
    }
    root.directories.ordered = try extra.finish(root.directories.ordered);
    root.segment_count += 3;
    for ([_]bool{ false, true }) |reverse| {
        var mixed_order = try local.storage_db_query_search_exec.SearchHitOrder.init(out, .{ .order_by = &.{.{ .field = "rank", .desc = reverse }} }, schema);
        defer mixed_order.deinit(out);
        const cursor = (try SortCursor.create(a, root, store, cached, .none, mixed_order, null)).?;
        defer cursor.deinit();
        var output: [1]Id = undefined;
        try std.testing.expectEqual(additions[2], (try cursor.next(&output, 1, &.{})).?.identities[0]);
        try std.testing.expectEqual(additions[if (reverse) 0 else 1], (try cursor.next(&output, 1, &.{})).?.identities[0]);
        const cutoff = [_]std.json.Value{ if (reverse) .{ .integer = 1000 } else .null, .{ .string = "public" } };
        try std.testing.expect(try cursor.next(&output, 1, &cutoff) == null);
        try std.testing.expect(cursor.pruned);
    }
    root.segment_count += 1;
    var order = try local.storage_db_query_search_exec.SearchHitOrder.init(out, .{ .order_by = &.{.{ .field = "rank" }} }, schema);
    defer order.deinit(out);
    try std.testing.expect(try SortCursor.create(a, root, store, cached, .none, order, null) == null);
}

test "external lake global frontier publication admits forty layout cohorts with bounded aggregates" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    var marker: u8 = 0;
    const Check = struct {
        fn run(_: *anyopaque) !void {}
    };
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.run };
    defer manager.deinit();
    var changes = Mutations.init(a, &manager, memory.store());
    defer changes.deinit();
    const directory = @import("lake_index_sort_directory.zig");
    changes.reduction = directory.reduce;
    for (0..40) |i| {
        const fields = try out.dupe(local.segment.SegmentIndexSortField, &.{ .{ .field = try std.fmt.allocPrint(out, "rank{d}", .{i}) }, .{ .field = "_id" } });
        const values = try out.dupe(@import("lake_index_sort_bounds.zig").Bound.Value, &.{ .{ .kind = .i64_val, .text = "1" }, .{ .kind = .id, .text = "doc" } });
        try directory.mutate(out, &changes, .{ .ref = .{ .artifact_id = try std.fmt.allocPrint(out, "segment{d}", .{i}), .checksum = "checksum", .byte_len = 1 }, .summary = null, .bytes = 1, .doc_count = 1, .fields = &.{}, .sort_bound = .{ .fields = fields, .first = values, .last = values } }, 1, false);
    }
    const root = (try changes.finish(null)).?;
    var page = try tree.readPage(a, memory.store(), root);
    defer page.deinit(a);
    try std.testing.expectEqual(@as(u64, 80), root.records);
    try std.testing.expect(page.aggregate.len <= tree.max_aggregate_bytes);
    const summary = (try directory.RangeSummary.read(page.aggregate)).?;
    try std.testing.expectEqual(@as(u64, 80), summary.records);
    try std.testing.expect(!summary.uniform and !summary.complete);
    std.debug.print("COHORT_PUBLICATION cohorts=40 summary_bytes={d} hard_limit={d}\n", .{ page.aggregate.len, tree.max_aggregate_bytes });
}
