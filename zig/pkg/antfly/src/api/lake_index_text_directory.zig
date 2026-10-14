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
    routing: ?tree.Ref = null,
    pub fn validate(self: @This()) !void {
        if (self.files) |ref| try ref.validate();
        if (self.segments) |ref| try ref.validate();
        if (self.routing) |ref| try ref.validate();
    }
};
pub const Interval = struct { lower: u32, count: u32 };
pub const FileRecord = struct { file: state.File, manifest: Chunk };
pub const SegmentRecord = struct {
    sources: []const corpus.source_spans.Span = &.{},
    source_pages: []const Chunk = &.{},
    source_count: u32 = 0,
    members: []const corpus.Member = &.{},
    file: state.File,
    manifest: Chunk,
    physical: ?Chunk = null,
    ordinal: u32,
    interval: Interval,
    route: corpus.SegmentRouting,
    rows: []const corpus.physical.Block,
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
    if (group.sources.len > corpus.source_spans.page_spans and group.source_pages.len == 0) {
        group.source_pages = try corpus.source_spans.publish(a, store, group.sources, token);
        group.source_count = @intCast(group.sources.len);
    }
    const descriptors = try a.alloc(Chunk, group.segments.len);
    for (descriptors, group.segments, group.admissions, group.summaries) |*ref, segment, admission, summary| {
        try token.check();
        ref.* = try put(a, store, corpus.FileGroup{ .file = fileState(group.file), .sources = if (group.source_pages.len == 0) group.sources else &.{}, .source_pages = group.source_pages, .source_count = group.source_count, .segments = &.{segment}, .admissions = &.{admission}, .summaries = &.{summary} }, token);
    }
    group.descriptors = descriptors;
    if (group.sources.len != 0) return;
    group.physical_manifest = try put(a, store, corpus.FileGroup{ .file = fileState(group.file), .segments = &.{}, .rows = group.rows, .opening_packs = group.opening_packs }, token);
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
            if (self.group.sources.len == 0 and self.lower != try corpus.physical.validate(self.group.rows)) return error.InvalidNativeLakeTextCorpus;
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
        const result: SegmentRecord = .{ .sources = if (self.group.source_pages.len == 0) self.group.sources else &.{}, .source_pages = self.group.source_pages, .source_count = self.group.source_count, .file = fileState(self.group.file), .manifest = self.manifest, .physical = self.group.physical_manifest, .ordinal = @intCast(self.ordinal), .interval = .{ .lower = self.lower, .count = route.doc_count }, .route = route, .rows = self.group.rows[self.first_block..end] };
        self.lower = upper;
        self.ordinal += 1;
        return result;
    }
};
/// Metadata construction streams one record at a time into bounded sorts and
/// durable runs. Replacement tails and last-write ordering remain explicit.
pub const Mutations = struct {
    a: A,
    sort: local.sql_spill.Sort,
    forest: @import("lake_index_delta_runs.zig").Forest,
    ordinal: u64 = 0,
    pub fn init(a: A, manager: *local.sql_spill.Manager, storage: tree.Store) Mutations {
        var sort = local.sql_spill.Sort.init(a, manager, &.{.{}}, 1024 * 1024);
        sort.shared_runs = true;
        sort.encoded_bytes = true;
        return .{ .a = a, .sort = sort, .forest = .{ .a = a, .store = storage, .policy = .latest } };
    }
    pub fn deinit(self: *Mutations) void {
        self.sort.deinit();
    }
    fn add(self: *Mutations, id: Id, value: ?[]const u8, previous_length: usize) !void {
        return self.addPrefix(&id, value, previous_length);
    }
    pub fn addPrefix(self: *Mutations, prefix: []const u8, value: ?[]const u8, previous_length: usize) !void {
        const length = if (value) |bytes| bytes.len else previous_length;
        if (length == 0 or length > corpus.max_root_bytes) return error.InvalidNativeLakeTextCorpus;
        var offset: usize = 0;
        while (offset < length) : (offset += chunk_bytes) {
            var buffer: [68]u8 = undefined;
            if (prefix.len > 64) return error.InvalidNativeLakeTextCorpus;
            @memcpy(buffer[0..prefix.len], prefix);
            std.mem.writeInt(u32, buffer[prefix.len..][0..4], @intCast(offset), .big);
            const k = buffer[0 .. prefix.len + 4];
            try self.sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = k })}, .values = &.{local.sql_scalar.Datum.json(if (value) |bytes| .{ .string = bytes[offset..@min(length, offset + chunk_bytes)] } else .null)}, .ordinal = self.ordinal });
            self.ordinal += 1;
            if (self.sort.total >= 1024) try self.flush();
        }
    }
    fn flush(self: *Mutations) !void {
        if (self.sort.total == 0) return;
        try self.sort.finish();
        const Source = struct {
            sort: *local.sql_spill.Sort,
            scratch: std.heap.ArenaAllocator,
            owned: std.heap.ArenaAllocator,
            pending: ?local.sql_operators.Row = null,
            pub fn next(self_: *@This()) !?tree.Cursor.Record {
                _ = self_.owned.reset(.retain_capacity);
                const a = self_.owned.allocator();
                if (self_.pending == null) self_.pending = try self_.sort.next(self_.scratch.allocator());
                const row = self_.pending orelse return null;
                const k = try a.dupe(u8, row.keys[0].value.string);
                var encoded: []const u8 = "";
                while (self_.pending) |next_| {
                    if (!std.mem.eql(u8, k, next_.keys[0].value.string)) break;
                    const value = next_.values[0];
                    encoded = if (value.sql_null or value.value == .null) &.{0} else try std.mem.concat(a, u8, &.{ &.{1}, value.value.string });
                    _ = self_.scratch.reset(.retain_capacity);
                    self_.pending = try self_.sort.next(self_.scratch.allocator());
                }
                return .{ .key = k, .value = encoded };
            }
        };
        var source: Source = .{ .sort = &self.sort, .scratch = .init(self.a), .owned = .init(self.a) };
        defer source.scratch.deinit();
        defer source.owned.deinit();
        try self.forest.add(&source);
        const manager = self.sort.manager;
        self.sort.deinit();
        self.sort = local.sql_spill.Sort.init(self.a, manager, &.{.{}}, 1024 * 1024);
        self.sort.shared_runs = true;
        self.sort.encoded_bytes = true;
    }
    pub fn finish(self: *Mutations, prior: ?tree.Ref) !?tree.Ref {
        try self.flush();
        const changes = try self.forest.finish() orelse return prior;
        const storage = self.forest.store;
        var delta = try tree.Cursor.init(self.a, storage, changes, "", null);
        defer delta.deinit();
        if (prior != null and changes.records < @max(4096, prior.?.records / 4)) {
            var cache: tree.Cache = .{ .alloc = self.a, .underlying = storage };
            defer cache.deinit();
            var arena = std.heap.ArenaAllocator.init(self.a);
            defer arena.deinit();
            var root = prior;
            var done = false;
            while (!done) {
                _ = arena.reset(.retain_capacity);
                const a = arena.allocator();
                var batch: std.ArrayList(tree.Mutation) = .empty;
                var bytes: usize = 0;
                while (batch.items.len < 256 and bytes < 512 * 1024) {
                    const record = try delta.next() orelse {
                        done = true;
                        break;
                    };
                    const mutation_ = try decode(record);
                    try batch.append(a, .{ .key = try a.dupe(u8, mutation_.key), .value = if (mutation_.value) |value| try a.dupe(u8, value) else null });
                    bytes += record.key.len + record.value.len;
                }
                if (batch.items.len != 0) root = try tree.apply(self.a, cache.store(), root, batch.items);
            }
            return root;
        }
        var old = try tree.Cursor.init(self.a, storage, prior, "", null);
        defer old.deinit();
        const Merge = struct {
            old: *tree.Cursor,
            delta: *tree.Cursor,
            left: ?tree.Cursor.Record = null,
            right: ?tree.Cursor.Record = null,
            advance_left: bool = true,
            advance_right: bool = true,
            pub fn next(self_: *@This()) !?tree.Cursor.Record {
                while (true) {
                    if (self_.advance_left) self_.left = try self_.old.next();
                    if (self_.advance_right) self_.right = try self_.delta.next();
                    self_.advance_left = false;
                    self_.advance_right = false;
                    if (self_.left == null and self_.right == null) return null;
                    const order: std.math.Order = if (self_.left == null) .gt else if (self_.right == null) .lt else std.mem.order(u8, self_.left.?.key, self_.right.?.key);
                    self_.advance_left = order != .gt;
                    self_.advance_right = order != .lt;
                    if (order == .lt) return self_.left;
                    const mutation_ = try decode(self_.right.?);
                    if (mutation_.value) |value| return .{ .key = mutation_.key, .value = value };
                }
            }
        };
        var source: Merge = .{ .old = &old, .delta = &delta };
        return tree.buildSorted(self.a, storage, &source);
    }
    fn decode(record: tree.Cursor.Record) !tree.Mutation {
        if (record.value.len == 0 or record.value[0] > 1 or (record.value[0] == 0 and record.value.len != 1)) return error.InvalidNativeLakeTextCorpus;
        return .{ .key = record.key, .value = if (record.value[0] == 0) null else record.value[1..] };
    }
};
pub fn publish(a: A, store: *stores.ArtifactStore, groups: []const corpus.FileGroup, refs: []const Chunk, removed: []const corpus.FileGroup, removed_refs: []const Chunk, old: Roots, token: Cancellation, io: std.Io) !Roots {
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: pages_mod.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    return update(a, pages.store(), groups, refs, removed, removed_refs, old, token, io);
}
/// Checkpoints need only compact file identities, never segment/routing trees.
/// Changes are paged and applied to the previous immutable delta inventory.
pub fn publishFileRecords(a: A, store: *stores.ArtifactStore, records: []const FileRecord, removed: []const FileRecord, prior: ?tree.Ref, token: Cancellation, io: std.Io) !?tree.Ref {
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: pages_mod.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
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
    var check = token;
    const Check = struct {
        fn call(raw: *anyopaque) !void {
            const cancellation: *Cancellation = @ptrCast(@alignCast(raw));
            try cancellation.check();
        }
    };
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = io, .context = &check, .checkpoint = Check.call };
    defer manager.deinit();
    var files = Mutations.init(a, &manager, storage);
    defer files.deinit();
    var segments = Mutations.init(a, &manager, storage);
    defer segments.deinit();
    var routing = Mutations.init(a, &manager, storage);
    defer routing.deinit();
    var builder: MetadataBuilder = .{ .files = &files, .segments = &segments, .routing = &routing, .token = token, .group_arena = .init(a), .record_arena = .init(a) };
    defer builder.deinit();
    for ([_]bool{ true, false }) |deleting| {
        for (if (deleting) removed else groups, if (deleting) removed_refs else refs) |group, ref| try builder.add(group, ref, deleting);
    }
    return .{ .files = try files.finish(old.files), .segments = try segments.finish(old.segments), .routing = try routing.finish(old.routing) };
}
const MetadataBuilder = struct {
    files: *Mutations,
    segments: *Mutations,
    routing: *Mutations,
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
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: pages_mod.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    var check = token;
    const Check = struct {
        fn call(raw: *anyopaque) !void {
            try @as(*Cancellation, @ptrCast(@alignCast(raw))).check();
        }
    };
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = io, .context = &check, .checkpoint = Check.call };
    defer manager.deinit();
    var files = Mutations.init(a, &manager, pages.store());
    defer files.deinit();
    var segments = Mutations.init(a, &manager, pages.store());
    defer segments.deinit();
    var routing = Mutations.init(a, &manager, pages.store());
    defer routing.deinit();
    var builder: MetadataBuilder = .{ .files = &files, .segments = &segments, .routing = &routing, .token = token, .group_arena = .init(a), .record_arena = .init(a) };
    defer builder.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for ([_]bool{ true, false }) |deleting| {
        for (if (deleting) removed else refs) |ref| {
            try token.check();
            _ = arena.reset(.free_all);
            const group = try corpus.loadFileManifest(arena.allocator(), store.*, ref, domain, token, null);
            try builder.add(group, ref, deleting);
        }
    }
    return .{ .files = try files.finish(old.files), .segments = try segments.finish(old.segments), .routing = try routing.finish(old.routing) };
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
    return corpus.source_spans.planningBytes(record.route.bytes, metadata);
}
/// Authenticate a selected leaf's size without admitting its inventory.
pub fn segmentBytes(a: A, storage: tree.Store, root: corpus.Root, id: Id) !u64 {
    const bytes = try get(a, storage, root.directories.segments, id);
    defer a.free(bytes);
    const record = try parseSegment(a, bytes, .{ .allocate = .alloc_always });
    if (!std.mem.eql(u8, &id, &corpus.term_directory.identity(record.route.ref.artifact_id))) return error.InvalidNativeLakeTextCorpus;
    try validateRouting(record, root.domain);
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
    if (record.source_pages.len != 0) {
        if (record.source_count == 0 or record.source_count > corpus.source_spans.max_spans or record.source_pages.len > record.source_count or record.ordinal != 0 or record.interval.lower != 0 or record.rows.len != 0 or record.physical != null) return error.InvalidNativeLakeTextCorpus;
        for (record.source_pages) |page| try validateRef(page, domain);
        if (record.sources.len == 0) return;
        if (record.sources.len != record.source_count) return error.InvalidNativeLakeTextCorpus;
    }
    if (record.sources.len != 0) {
        if (record.ordinal != 0 or record.interval.lower != 0 or record.rows.len != 0 or record.physical != null) return error.InvalidNativeLakeTextCorpus;
        return corpus.source_spans.validate(record.sources, record.interval.count, domain);
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
    var read: Reader = .{ .store = store, .cached = null, .token = token };
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
        record.sources = try corpus.source_spans.load(a, store, record.source_pages, record.source_count, root.domain, token, cached, reads);
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
        try groups.append(a, .{ .sources = selected[0].sources, .source_pages = selected[0].source_pages, .source_count = selected[0].source_count, .file = selected[0].file, .segments = refs, .summaries = summaries, .rows = blocks.items, .physical_manifest = selected[0].physical, .selected = if (partial or blocks.items.len != 0) intervals else &.{} });
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
            try std.testing.expectEqual(@as(u64, 256 * 1024 * 1024) - frontier.reads, frontier.charged_bytes);
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
    var missing_rows: @import("lake_index_text_predicate.zig").LiveRowsCache = .{};
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
                if (count != 0 and self.last_segment_bytes > max_bytes -| bytes) {
                    self.pending = .{ .identity = identity, .bytes = self.last_segment_bytes, .documents = self.last_segment_docs };
                } else {
                    output[count] = identity;
                    count += 1;
                    bytes +|= self.last_segment_bytes;
                }
                self.bytes.clearRetainingCapacity();
            };
            self.previous = current;
            if (std.mem.readInt(u32, entry.key[36..40], .big) != self.bytes.items.len or entry.value.len == 0 or entry.value.len > chunk_bytes or entry.value.len > corpus.max_root_bytes -| self.bytes.items.len) return error.InvalidNativeLakeTextCorpus;
            try self.bytes.appendSlice(self.a, entry.value);
            if (self.pending != null or count == output.len or bytes >= max_bytes) return .{ .identities = output[0..count], .doc_offset = offset };
        }
        if (self.previous) |last| {
            const identity = try self.emit(last);
            self.previous = null;
            if (count != 0 and self.last_segment_bytes > max_bytes -| bytes) {
                self.pending = .{ .identity = identity, .bytes = self.last_segment_bytes, .documents = self.last_segment_docs };
            } else {
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
                break :blk try segmentBytes(self.scratch.allocator(), self.reader.pages.store(), self.root, id);
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
