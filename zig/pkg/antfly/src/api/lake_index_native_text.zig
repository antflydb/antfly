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

//! Immutable native text corpora. Segment construction uses the same field
//! projection, analyzers, positions, typed values and WAND blocks as local text
//! indexes; the root authenticates every segment in one global scoring corpus.
const std = @import("std");
const local = @import("antfly_local_sources");
const state = @import("lake_index_native_state.zig");
const mapper = local.storage_db_document_mapper;
const rebuild = @import("../serverless/build/lake_rebuild.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const Declared = local.serverless_segment_sidecar_manifest.DeclaredArtifact;
const Ref = local.serverless_manifest_artifact_ref.ArtifactRef;
const RowProvider = @import("lake_index_row_source.zig").Provider;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
pub const statistics = @import("lake_index_text_statistics.zig");
pub const term_directory = @import("lake_index_term_directory.zig");
// Includes v8 ordered producer identities and stored projection coverage.
// Never reuse a v8 source-free segment as a covered stored-text sidecar.
// V11 authenticates independent serving packs and original-source spans.
// Protocol 42 fences paged source spans and larger byte-targeted serving packs.
pub const metadata_version: u16 = 11;
pub fn supportsBuildMetadata(version: u16) bool {
    return version == 9 or version == 10 or version == metadata_version;
}
pub const max_root_bytes = 4 * 1024 * 1024;
pub const max_segments = 262144;
/// Source duplication is an explicit per-index policy. The complete config
/// participates in the recipe and binding hash, fencing old artifacts.
pub fn storesSource(config: std.json.Value) !bool {
    if (config != .object) return error.InvalidNativeLakeTextCorpus;
    const value = config.object.get("store_source") orelse return false;
    if (value != .bool) return error.InvalidNativeLakeTextCorpus;
    return value.bool;
}
/// Older builders may have accepted unknown config fields while still
/// omitting source. A distinct authenticated recipe attests actual production,
/// without changing the native format or invalidating source-free artifacts.
pub fn sourceRecipe(table: local.common_topology_records.TableRecord, config: []const u8, store_source: bool) [32]u8 {
    const original = state.recipe(table, config);
    if (!store_source) return original;
    var hash = std.crypto.hash.Blake3.init(.{});
    // v2 stores the complete declared relational row even when `field`
    // selects only one searchable text field. v1 retained only that field.
    hash.update("native-lake-text-stored-source-v2");
    hash.update(&original);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

test "external lake stored source recipe attests production without changing source free identity" {
    const table: local.common_topology_records.TableRecord = .{ .table_id = 7, .name = "lake", .schema_json = "schema" };
    const config = "{\"store_source\":true}";
    const old = state.recipe(table, config);
    try std.testing.expectEqual(old, sourceRecipe(table, config, false));
    try std.testing.expect(!std.mem.eql(u8, &old, &sourceRecipe(table, config, true)));
}
pub const source_spans = @import("lake_index_text_spans.zig");
pub const physical = @import("lake_index_physical_ordinals.zig");
pub const text_directory = @import("lake_index_text_directory.zig");
pub const SegmentRouting = struct {
    pub const Field = local.segment.SegmentReader.RoutingField;
    ref: artifacts.ChunkRef,
    summary: ?statistics.Ref,
    bytes: u64,
    doc_count: u32,
    fields: []const Field,
    descriptor: ?artifacts.ChunkRef = null,
};
pub const Member = struct {
    file: state.File,
    physical: artifacts.ChunkRef,
    source_lower: u32,
    lower: u32,
    count: u32,
};
pub fn validateMembers(members: []const Member, domain: [32]u8, lower: u32, count: u32) !void {
    var next = lower;
    for (members) |member| {
        if (member.lower != next or member.count == 0) return error.InvalidNativeLakeTextCorpus;
        try state.validate(&.{member.file}, domain);
        try text_directory.validateRef(member.physical, domain);
        _ = try std.math.add(u32, member.source_lower, member.count);
        next = try std.math.add(u32, next, member.count);
    }
    if (next != try std.math.add(u32, lower, count)) return error.InvalidNativeLakeTextCorpus;
}
pub const FileGroup = struct {
    file: state.File,
    sources: []const source_spans.Span = &.{},
    source_pages: []const artifacts.ChunkRef = &.{},
    source_count: u32 = 0,
    /// Historical v10 compaction coordinates, normalized before serving.
    members: []const Member = &.{},
    segments: []const artifacts.ChunkRef,
    rows: []const physical.Block = &.{},
    summaries: []const ?statistics.Ref = &.{},
    opening_packs: []const @import("lake_index_text_opening_pack.zig").Pack = &.{},
    admissions: []const local.segment.SegmentReader.Admission = &.{},
    descriptors: []const artifacts.ChunkRef = &.{},
    physical_manifest: ?artifacts.ChunkRef = null,
    /// Query-only intervals map selected segment ordinals to original file ranks.
    selected: []const text_directory.Interval = &.{},
    pub fn validate(self: FileGroup, domain: [32]u8) !void {
        if (self.source_pages.len != 0) {
            if (self.source_count == 0 or self.source_count > source_spans.max_spans or self.sources.len != self.source_count) return error.InvalidNativeLakeTextCorpus;
            for (self.source_pages) |page| try text_directory.validateRef(page, domain);
        }
        if (self.sources.len != 0) {
            if (self.segments.len != 1) return error.InvalidNativeLakeTextCorpus;
            const last = self.sources[self.sources.len - 1];
            try source_spans.validate(self.sources, if (self.admissions.len != 0) self.admissions[0].doc_count else try std.math.add(u32, last.pack_lower, last.count), domain);
        }
        if (self.segments.len > max_segments) return error.InvalidNativeLakeTextCorpus;
        if (self.opening_packs.len > 65536) return error.InvalidNativeLakeTextCorpus;
        for (self.opening_packs) |pack| try pack.validate(domain);
        if (self.summaries.len != 0 and self.summaries.len != self.segments.len) return error.InvalidNativeLakeTextCorpus;
        if (self.admissions.len != 0 and self.admissions.len != self.segments.len) return error.InvalidNativeLakeTextCorpus;
        if (self.descriptors.len != 0 and self.descriptors.len != self.segments.len) return error.InvalidNativeLakeTextCorpus;
        for (self.descriptors) |ref| try text_directory.validateRef(ref, domain);
        if (self.physical_manifest) |ref| try text_directory.validateRef(ref, domain);
        for (self.admissions) |metadata| try metadata.validate();
        for (self.summaries) |summary| if (summary) |ref| try ref.validate();
        try state.validate(&.{self.file}, domain);
        _ = try physical.validate(self.rows);
        for (self.rows) |block| if (block.bitmap) |ref| {
            const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
            if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeTextCorpus;
        };
        for (self.segments) |segment| {
            if (segment.byte_len == 0 or segment.byte_len > 32 * 1024 * 1024) return error.InvalidNativeLakeTextCorpus;
            try stores.validateSha256ArtifactIdentity(segment.artifact_id, segment.checksum);
            const scope = (try stores.uploadScopeFromArtifactId(segment.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
            if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeTextCorpus;
        }
    }
};
pub const Collection = struct {
    pub const Field = struct { name: []const u8, total_length: u64 };
    documents: u32,
    fields: []const Field,
    fn validate(self: Collection) !void {
        for (self.fields, 0..) |field, i| {
            if (field.name.len == 0 or (i != 0 and std.mem.order(u8, self.fields[i - 1].name, field.name) != .lt)) return error.InvalidNativeLakeTextCorpus;
        }
    }
};
/// Collection state retains one counter per field, independently of the
/// number of files/segments whose durable manifests have been consumed.
const CollectionBuilder = struct {
    a: A,
    documents: u32 = 0,
    segments: u32 = 0,
    fields: std.StringHashMapUnmanaged(u64) = .empty,
    fn deinit(self: *@This()) void {
        var keys = self.fields.keyIterator();
        while (keys.next()) |key| self.a.free(key.*);
        self.fields.deinit(self.a);
    }
    fn field(self: *@This(), name: []const u8, length: u64, subtract: bool) !void {
        if (self.fields.getPtr(name)) |value| {
            value.* = if (subtract) try std.math.sub(u64, value.*, length) else try std.math.add(u64, value.*, length);
        } else {
            if (subtract and length != 0) return error.InvalidNativeLakeTextCorpus;
            const owned = try self.a.dupe(u8, name);
            errdefer self.a.free(owned);
            try self.fields.put(self.a, owned, length);
        }
    }
    fn add(self: *@This(), group: FileGroup, subtract: bool) !void {
        const count: u32 = @intCast(group.segments.len);
        self.segments = if (subtract) try std.math.sub(u32, self.segments, count) else try std.math.add(u32, self.segments, count);
        if (self.segments > max_segments) return error.NativeLakeTextCorpusTooLarge;
        for (group.admissions) |segment| {
            self.documents = if (subtract) try std.math.sub(u32, self.documents, segment.doc_count) else try std.math.add(u32, self.documents, segment.doc_count);
            for (segment.fields) |f| if (f.stats) |stats| try self.field(f.name, stats.total_field_len, subtract);
        }
    }
    fn finish(self: *@This(), out: A) !Collection {
        const fields = try out.alloc(Collection.Field, self.fields.count());
        var it = self.fields.iterator();
        for (fields) |*f| {
            const entry = it.next().?;
            f.* = .{ .name = try out.dupe(u8, entry.key_ptr.*), .total_length = entry.value_ptr.* };
        }
        std.mem.sort(Collection.Field, fields, {}, struct {
            fn less(_: void, l: Collection.Field, r: Collection.Field) bool {
                return std.mem.order(u8, l.name, r.name) == .lt;
            }
        }.less);
        return .{ .documents = self.documents, .fields = fields };
    }
};
pub const Root = struct {
    pub const Tail = struct { ref: artifacts.ChunkRef, tier: u8, bytes: u64, spans: u32 };

    version: u16 = metadata_version,
    /// Independent serving packs; this root retains per-file maintenance statistics.
    serving: ?artifacts.ChunkRef = null,
    compacted: ?artifacts.ChunkRef = null,
    /// Resumable delta: the published maintenance/serving baseline stays rooted.
    checkpoint_base: ?artifacts.ChunkRef = null,
    checkpoint_removed: []const []const u8 = &.{},
    checkpoint_files: ?statistics.Ref = null,
    checkpoint_file_count: u32 = 0,
    checkpoint_removed_files: ?statistics.Ref = null,
    checkpoint_removed_count: u32 = 0,
    serving_bytes: u64 = 0,
    serving_metadata_bytes: ?u64 = null,
    pack_ownership: ?statistics.Ref = null,
    source_layout_version: u8 = 0,
    /// Durable proof that inventory-only identities include the entire row set.
    inventory_delta_version: u8 = 0,
    source_routes_version: u8 = 0,
    source_routes: ?statistics.Ref = null,
    tie_bounds: ?statistics.Ref = null,
    open_packs: []const artifacts.ChunkRef = &.{},
    open_tails: []const Tail = &.{},
    seekable: bool = false,
    /// Stored JSON contains exactly the binding projection, never an implicit
    /// full source document. Older publications require a format refresh.
    stored_projection: bool = false,
    domain: [32]u8,
    binding: local.serverless_segment_source_binding.Binding,
    config_json: []const u8,
    segments: []const artifacts.ChunkRef = &.{},
    recipe: [32]u8 = @splat(0),
    file_groups: []const FileGroup = &.{},
    manifests: []const artifacts.ChunkRef = &.{},
    /// Authenticated paged serving inventory; file manifests remain maintenance units.
    serving_inventory: ?statistics.Ref = null,
    inventory_version: u8 = 1,
    file_count: u32 = 0,
    segment_count: u32 = 0,
    directories: text_directory.Roots = .{},
    serving_routes: ?statistics.Ref = null,
    segment_files: ?statistics.Ref = null,
    collection: ?Collection = null,
    /// Query-selected serving view only; never persisted.
    partial_inventory: bool = false,
    /// Expanded serving view only; never accepted in a persisted directory.
    routing: []const []const SegmentRouting = &.{},
    admission_version: u8 = 0,
    statistics_version: u8 = 0,
    term_statistics: ?statistics.Ref = null,
    /// Non-serving resumable checkpoint: signed DF runs relative to the base.
    /// Final publication folds these once and never exposes pending deltas.
    statistics_deltas: []const @import("lake_index_delta_runs.zig").Run = &.{},
    term_directory: term_directory.Roots = .{},
    pub fn jsonStringify(self: Root, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        inline for (@typeInfo(Root).@"struct".field_names) |field| {
            if (comptime std.mem.eql(u8, field, "statistics_deltas")) {
                if (self.statistics_deltas.len != 0) {
                    try stream.objectField(field);
                    try stream.write(@field(self, field));
                }
            } else {
                try stream.objectField(field);
                try stream.write(@field(self, field));
            }
        }
        try stream.endObject();
    }
    pub fn validate(self: Root) !void {
        return self.validateAlloc(std.heap.page_allocator);
    }
    fn validateAlloc(self: Root, a: A) !void {
        if (self.version != metadata_version or self.segments.len > max_segments or std.mem.allEqual(u8, &self.domain, 0) or self.config_json.len > 256 * 1024) return error.InvalidNativeLakeTextCorpus;
        if (self.serving) |ref| try text_directory.validateRef(ref, self.domain);
        if (self.compacted) |ref| try text_directory.validateRef(ref, self.domain);
        if (self.checkpoint_base) |ref| {
            try text_directory.validateRef(ref, self.domain);
            if (self.serving != null or self.inventory_version != 1 or self.statistics_version != 1) return error.InvalidNativeLakeTextCorpus;
        }
        if (self.checkpoint_files) |ref| try ref.validate();
        if (self.checkpoint_removed_files) |ref| try ref.validate();
        if ((self.checkpoint_files == null) != (self.checkpoint_file_count == 0) or (self.checkpoint_removed_files == null) != (self.checkpoint_removed_count == 0) or self.checkpoint_file_count > max_segments or self.checkpoint_removed_count > max_segments) return error.InvalidNativeLakeTextCorpus;
        if (self.checkpoint_files != null or self.checkpoint_removed_files != null) {
            if (self.serving != null or self.compacted != null or self.inventory_version != 1 or self.statistics_version != 1 or self.manifests.len != 0 or self.file_groups.len != 0 or self.segments.len != 0 or self.checkpoint_removed.len != 0) return error.InvalidNativeLakeTextCorpus;
            if (self.checkpoint_removed_files != null and self.checkpoint_base == null) return error.InvalidNativeLakeTextCorpus;
        }
        if (self.serving != null and self.compacted != null) return error.InvalidNativeLakeTextCorpus;
        if (self.checkpoint_removed.len > 16384 or (self.checkpoint_removed.len != 0 and self.checkpoint_base == null)) return error.InvalidNativeLakeTextCorpus;
        if (self.pack_ownership) |ref| try ref.validate();
        if (self.source_layout_version > 1) return error.InvalidNativeLakeTextCorpus;
        if (self.inventory_delta_version > 2 or (self.inventory_delta_version != 0 and self.inventory_version != 3)) return error.InvalidNativeLakeTextCorpus;
        if (self.tie_bounds) |ref| {
            try ref.validate();
            if (ref.records != self.segment_count) return error.InvalidNativeLakeTextCorpus;
        }
        if (self.source_routes_version > 1 or (self.source_routes != null and self.source_routes_version != 1)) return error.InvalidNativeLakeTextCorpus;
        if (self.source_routes) |ref| try ref.validate();
        if (self.open_tails.len != 0) {
            if (self.open_tails.len != self.open_packs.len) return error.InvalidNativeLakeTextCorpus;
            for (self.open_tails, self.open_packs) |tail, ref| {
                if (tail.tier > 3 or tail.spans == 0 or tail.spans > source_spans.max_spans or tail.bytes == 0 or !std.mem.eql(u8, tail.ref.artifact_id, ref.artifact_id)) return error.InvalidNativeLakeTextCorpus;
                try text_directory.validateRef(tail.ref, self.domain);
            }
        }
        if (self.open_packs.len > 28) return error.InvalidNativeLakeTextCorpus;
        for (self.open_packs) |ref| try text_directory.validateRef(ref, self.domain);
        if (self.admission_version > 1) return error.InvalidNativeLakeTextCorpus;
        if (self.term_directory.version < 1 or self.term_directory.version > 2) return error.InvalidNativeLakeTextCorpus;
        if (self.statistics_version > 2 or (self.statistics_version == 0 and self.term_statistics != null)) return error.InvalidNativeLakeTextCorpus;
        if (self.serving_inventory) |ref| try ref.validate();
        if (self.serving_routes) |ref| try ref.validate();
        if (self.segment_files) |ref| try ref.validate();
        if (self.collection) |collection| try collection.validate();
        try self.directories.validate();
        if (self.file_count > max_segments or self.segment_count > max_segments) return error.InvalidNativeLakeTextCorpus;
        if (self.inventory_version < 1 or self.inventory_version > 3) return error.InvalidNativeLakeTextCorpus;
        if (self.term_statistics) |ref| try ref.validate();
        if (self.statistics_deltas.len != 0) {
            if (self.statistics_version != 1 or self.inventory_version != 1 or self.statistics_deltas.len > 32) return error.InvalidNativeLakeTextCorpus;
            var levels: u32 = 0;
            for (self.statistics_deltas) |run| {
                if (run.level >= 32 or levels & (@as(u32, 1) << @as(u5, @intCast(run.level))) != 0) return error.InvalidNativeLakeTextCorpus;
                levels |= @as(u32, 1) << @as(u5, @intCast(run.level));
                try run.page.validate();
            }
        }
        if (self.statistics_version < 2 and (self.term_directory.branches != null or self.term_directory.leaves != null)) return error.InvalidNativeLakeTextCorpus;
        if ((self.term_directory.branches == null) != (self.term_directory.leaves == null)) return error.InvalidNativeLakeTextCorpus;
        if (self.term_directory.branches) |ref| try ref.validate();
        if (self.term_directory.leaves) |ref| try ref.validate();
        try self.binding.validate();
        if (self.binding.sidecar_kind != .text) return error.InvalidNativeLakeTextCorpus;
        if (self.file_groups.len > 16384) return error.InvalidNativeLakeTextCorpus;
        if (self.manifests.len > 16384) return error.InvalidNativeLakeTextCorpus;
        for (self.manifests) |ref| {
            try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
            const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
            if (ref.byte_len == 0 or ref.byte_len > max_root_bytes or !std.mem.eql(u8, &scope.domain, &self.domain)) return error.InvalidNativeLakeTextCorpus;
        }
        var segment_position: usize = 0;
        for (self.file_groups) |group| {
            try group.validate(self.domain);
            if (self.admission_version == 1 and group.admissions.len != group.segments.len) return error.InvalidNativeLakeTextCorpus;
            if (self.statistics_version >= 1 and group.summaries.len != group.segments.len) return error.InvalidNativeLakeTextCorpus;
            for (group.segments) |segment| {
                if (segment_position >= self.segments.len or (!std.mem.eql(u8, segment.artifact_id, self.segments[segment_position].artifact_id) or !std.mem.eql(u8, segment.checksum, self.segments[segment_position].checksum) or segment.byte_len != self.segments[segment_position].byte_len)) return error.InvalidNativeLakeTextCorpus;
                segment_position += 1;
            }
        }
        if (self.file_groups.len != 0 and segment_position != self.segments.len) return error.InvalidNativeLakeTextCorpus;
        var identities: std.StringHashMapUnmanaged(void) = .empty;
        defer identities.deinit(a);
        for (self.segments) |segment| {
            if (segment.byte_len == 0 or segment.byte_len > 32 * 1024 * 1024) return error.InvalidNativeLakeTextCorpus;
            try stores.validateSha256ArtifactIdentity(segment.artifact_id, segment.checksum);
            const scope = (try stores.uploadScopeFromArtifactId(segment.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
            if (!std.mem.eql(u8, &scope.domain, &self.domain) or (try identities.getOrPut(a, segment.artifact_id)).found_existing) return error.InvalidNativeLakeTextCorpus;
        }
    }
};
/// Read only the authenticated root directory; callers own child admission.
pub fn loadRootDirectory(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Root {
    if (ref.kind != .text_segment or !supportsBuildMetadata(ref.metadata_version) or ref.byte_len > max_root_bytes) return error.InvalidNativeLakeTextCorpus;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, cache);
    defer a.free(bytes);
    return decodeRootDirectory(a, ref, bytes);
}
/// Legacy compacted pointers did not carry a version. Authenticate their body
/// before selecting a supported decoder; advertised root versions stay strict.
pub fn loadRootDirectoryAuto(a: A, store: stores.ArtifactStore, ref: artifacts.ChunkRef, cancellation: Cancellation, cache: ?artifacts.CachedRead) !struct { root: Root, metadata_version: u16 } {
    if (ref.byte_len == 0 or ref.byte_len > max_root_bytes) return error.InvalidNativeLakeTextCorpus;
    const bytes = try artifacts.readArtifact(a, store, ref, cancellation, cache);
    defer a.free(bytes);
    const header = try std.json.parseFromSliceLeaky(struct { version: u16 }, a, bytes, .{ .ignore_unknown_fields = true });
    if (!supportsBuildMetadata(header.version)) return error.InvalidNativeLakeTextCorpus;
    return .{ .root = try decodeRootDirectory(a, .{ .kind = .text_segment, .metadata_version = header.version, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, bytes), .metadata_version = header.version };
}
fn decodeRootDirectory(a: A, ref: Ref, bytes: []const u8) !Root {
    var root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    if (root.version != ref.metadata_version) return error.InvalidNativeLakeTextCorpus;
    // V11 file records are single-page GC units; authenticate their population
    // before a paged mark traversal can authorize any deletion.
    if (ref.metadata_version >= 11 and root.inventory_version == 3 and (if (root.directories.files) |files| files.records else 0) != root.file_count) return error.InvalidNativeLakeTextCorpus;
    if (root.checkpoint_files) |files| if (files.records != root.checkpoint_file_count) return error.InvalidNativeLakeTextCorpus;
    if (root.checkpoint_removed_files) |files| if (files.records != root.checkpoint_removed_count) return error.InvalidNativeLakeTextCorpus;
    // Historical roots remain usable by recovery/retained readers. Reconciliation
    // only advertises the new identity and upgrades protocol-33 publications.
    root.version = metadata_version;
    try root.validate();
    if (root.inventory_version == 3 and (root.manifests.len != 0 or root.file_groups.len != 0 or root.segments.len != 0 or (root.file_count != 0 and root.directories.files == null))) return error.InvalidNativeLakeTextCorpus;
    if (root.routing.len != 0 or root.partial_inventory) return error.InvalidNativeLakeTextCorpus;
    if (root.manifests.len != 0 and (root.segments.len != 0 or root.file_groups.len != 0)) return error.InvalidNativeLakeTextCorpus;
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
    if (!std.mem.eql(u8, &root.domain, &scope.domain)) return error.InvalidNativeLakeTextCorpus;
    return root;
}
/// Directory-only metadata has a distinct cache type from expanded roots.
pub const Directory = struct { root: Root };
pub fn loadDirectory(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Directory {
    const started = @import("antfly_platform").time.monotonicNs();
    defer if (cache) |cached| if (cached.metrics) |metrics| metrics.record(.text_directory, started);
    return .{ .root = try servingRoot(a, store, try loadRootDirectory(a, store, ref, cancellation, cache), cancellation, cache) };
}
pub fn servingRoot(a: A, store: stores.ArtifactStore, outer: Root, token: Cancellation, cache: ?artifacts.CachedRead) !Root {
    const ref = outer.serving orelse outer.compacted orelse return outer;
    const root = if (outer.serving != null) try loadRootDirectory(a, store, .{ .kind = .text_segment, .metadata_version = metadata_version, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, cache) else (try loadRootDirectoryAuto(a, store, ref, token, cache)).root;
    if (root.serving != null or root.compacted != null or !rebuild.bindingsEqual(root.binding, outer.binding) or !std.mem.eql(u8, &root.recipe, &outer.recipe) or !std.mem.eql(u8, &root.domain, &outer.domain) or root.collection == null or outer.collection == null or root.collection.?.documents != outer.collection.?.documents or root.collection.?.fields.len != outer.collection.?.fields.len or root.stored_projection != outer.stored_projection or !std.mem.eql(u8, root.config_json, outer.config_json)) return error.InvalidNativeLakeTextCorpus;
    return root;
}
/// One independently admitted immutable file manifest. Share validation with
/// serving while allowing GC to checkpoint between file jobs.
pub fn loadFileManifest(a: A, store: stores.ArtifactStore, ref: artifacts.ChunkRef, domain: [32]u8, cancellation: Cancellation, cache: ?artifacts.CachedRead) !FileGroup {
    if (ref.byte_len == 0 or ref.byte_len > max_root_bytes) return error.InvalidNativeLakeTextCorpus;
    return loadFileManifestBudget(a, store, ref, domain, cancellation, cache, null);
}
pub fn loadFileManifestBudget(a: A, store: stores.ArtifactStore, ref: artifacts.ChunkRef, domain: [32]u8, cancellation: Cancellation, cache: ?artifacts.CachedRead, remaining: ?*u64) !FileGroup {
    if (ref.byte_len == 0 or ref.byte_len > max_root_bytes) return error.InvalidNativeLakeTextCorpus;
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
    if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeTextCorpus;
    const bytes = try artifacts.readArtifact(a, store, ref, cancellation, cache);
    defer a.free(bytes);
    var group = try std.json.parseFromSliceLeaky(FileGroup, a, bytes, .{ .allocate = .alloc_always });
    if (group.selected.len != 0) return error.InvalidNativeLakeTextCorpus;
    if (group.members.len != 0) {
        if (group.sources.len != 0 or group.segments.len != 1) return error.InvalidNativeLakeTextCorpus;
        group.sources = try source_spans.fromMembers(a, group.members, domain);
        group.physical_manifest = null;
    }
    if (group.source_pages.len != 0) {
        if (group.sources.len != 0) return error.InvalidNativeLakeTextCorpus;
        group.sources = try source_spans.load(a, store, group.source_pages, group.source_count, domain, cancellation, cache, remaining);
    }
    try group.validate(domain);
    return group;
}
/// Cold corpus admission borrows independently cached manifests. Leases end
/// after the corpus has copied its metadata; warm corpora need only a directory.
pub const Inventory = struct {
    root: Root,
    manifests: []?@import("lake_index_decoded_metadata.zig").Owned(FileGroup),
    pub fn deinit(self: Inventory) void {
        for (self.manifests) |manifest| if (manifest) |owned| owned.release();
    }
};
pub fn loadCachedFile(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation, cache: ?artifacts.CachedRead) !FileGroup {
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
    return loadFileManifest(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, scope.domain, cancellation, cache);
}
pub fn admitInventory(a: A, directory: Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, cancellation: Cancellation) !Inventory {
    return admitInventoryBounded(a, std.heap.page_allocator, directory, store, cached, cancellation);
}
pub fn admitInventoryBounded(a: A, scratch_backing: A, directory_value: Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, cancellation: Cancellation) !Inventory {
    try cached.context.ensureActive();
    try cancellation.check();
    if (directory_value.statistics_deltas.len != 0) return error.InvalidNativeLakeTextCorpus;
    if (directory_value.inventory_version == 3 and directory_value.file_groups.len == 0) return admitServingInventory(a, scratch_backing, directory_value, store, cached, cancellation);
    const directory = try text_directory.withManifests(a, directory_value, store, cached, cancellation);
    if (directory.inventory_version >= 2 and directory.serving_inventory != null and directory.file_groups.len == 0) return .{ .root = try @import("lake_index_serving_inventory.zig").load(a, scratch_backing, directory, store, cached, cancellation, false), .manifests = &.{} };
    if (directory.serving_inventory != null and directory.file_groups.len == 0) return .{ .root = try readServingInventory(a, scratch_backing, directory, store, cached, cancellation), .manifests = &.{} };
    const Owned = @import("lake_index_decoded_metadata.zig").Owned(FileGroup);
    const manifests = try a.alloc(?Owned, directory.manifests.len);
    @memset(manifests, null);
    var inventory: Inventory = .{ .root = directory, .manifests = manifests };
    errdefer inventory.deinit();
    if (manifests.len == 0 or directory.file_groups.len != 0) {
        try directory.validateAlloc(scratch_backing);
        return inventory;
    }
    const Work = struct {
        directory: Root,
        store: stores.ArtifactStore,
        cached: artifacts.CachedRead,
        cancellation: Cancellation,
        manifests: []?Owned,
        next: std.atomic.Value(usize) = .init(0),
        bytes: std.atomic.Value(usize) = .init(0),
        fn run(self: *@This()) anyerror!void {
            while (true) {
                try self.cancellation.check();
                try self.cached.context.ensureActive();
                const index = self.next.fetchAdd(1, .monotonic);
                if (index >= self.manifests.len) return;
                const manifest = self.directory.manifests[index];
                const owned = try @import("lake_index_decoded_metadata.zig").acquire(FileGroup, self.cached, self.store, .{ .kind = .text_segment, .metadata_version = metadata_version, .artifact_id = manifest.artifact_id, .checksum = manifest.checksum, .byte_len = manifest.byte_len }, self.cancellation, loadCachedFile);
                errdefer owned.release();
                try owned.value.validate(self.directory.domain);
                // Bound actual decoded capacity, independently of active decode
                // admission. No arbitrary aggregate wire-byte cutoff is needed.
                const bytes = owned.lease.item.budget.live;
                var retained = self.bytes.load(.monotonic);
                while (true) {
                    if (bytes > 64 * 1024 * 1024 -| retained) return error.NativeLakeTextCorpusTooLarge;
                    retained = self.bytes.cmpxchgWeak(retained, retained + bytes, .monotonic, .monotonic) orelse break;
                }
                self.manifests[index] = owned;
            }
        }
    };
    var work: Work = .{ .directory = directory, .store = store, .cached = cached, .cancellation = cancellation, .manifests = manifests };
    const scheduler = local.sql_parallel_scheduler;
    var tasks: [3]?scheduler.Task(anyerror!void) = @splat(null);
    // Join before output leases or borrowed request data can be released.
    defer if (cached.context.io) |io| for (&tasks) |*slot| if (slot.*) |*task| {
        task.cancel(io) catch {};
    };
    if (cached.context.io) |io| for (tasks[0..@min(3, manifests.len - 1)]) |*slot| {
        slot.* = scheduler.global().submitIo(io, 8 * 1024 * 1024, Work.run, .{&work});
    };
    try work.run();
    if (cached.context.io) |io| for (&tasks) |*slot| if (slot.*) |*task| {
        var joined = task.*;
        slot.* = null;
        try joined.await(io);
    };
    var count: usize = 0;
    for (manifests) |manifest| {
        const group = manifest.?.value;
        if (group.segments.len > max_segments -| count) return error.InvalidNativeLakeTextCorpus;
        count += group.segments.len;
    }
    const groups = try a.alloc(FileGroup, manifests.len);
    const segments = try a.alloc(artifacts.ChunkRef, count);
    var offset: usize = 0;
    for (manifests, groups) |manifest, *group| {
        group.* = manifest.?.value.*;
        @memcpy(segments[offset..][0..group.segments.len], group.segments);
        offset += group.segments.len;
    }
    inventory.root.file_groups = groups;
    inventory.root.segments = segments;
    try inventory.root.validateAlloc(scratch_backing);
    return inventory;
}

pub fn admitSelectedServingInventory(a: A, scratch_backing: A, directory: Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, cancellation: Cancellation, files: []const [32]u8) !Inventory {
    if (directory.inventory_version == 3) return .{ .root = try text_directory.loadSelected(a, scratch_backing, directory, store, cached, cancellation, files), .manifests = &.{} };
    return .{ .root = try @import("lake_index_serving_inventory.zig").loadSelected(a, scratch_backing, directory, store, cached, cancellation, files), .manifests = &.{} };
}
pub fn admitServingInventory(a: A, scratch_backing: A, directory_value: Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, cancellation: Cancellation) !Inventory {
    try cached.context.ensureActive();
    try cancellation.check();
    if (directory_value.routing.len != 0) return .{ .root = directory_value, .manifests = &.{} };
    if (directory_value.inventory_version == 3 and directory_value.file_groups.len == 0) return .{ .root = try text_directory.loadAll(a, scratch_backing, directory_value, store, cached, cancellation), .manifests = &.{} };
    const directory = directory_value;
    if (directory.routing.len != 0) return .{ .root = directory, .manifests = &.{} };
    if (directory.inventory_version >= 2 and directory.serving_routes != null and directory.file_groups.len == 0) return .{ .root = try @import("lake_index_serving_inventory.zig").load(a, scratch_backing, directory, store, cached, cancellation, true), .manifests = &.{} };
    return admitInventoryBounded(a, scratch_backing, directory, store, cached, cancellation);
}
// Serving records are bounded chunks of one file descriptor, ordered by file
// and byte offset. This preserves the existing validated descriptor contract
// while packing many small files into ordinary 32 KiB authenticated pages.
// Only one encoded descriptor is retained while streaming in either direction.
const inventory_chunk_bytes = 16 * 1024;
const inventory_tree = @import("../serverless/graph_segment/page_tree.zig");
const inventory_pages = @import("../serverless/graph_segment/page_store.zig");
fn readServingInventory(a: A, scratch_backing: A, directory: Root, store_value: stores.ArtifactStore, cached: artifacts.CachedRead, cancellation: Cancellation) !Root {
    const Read = struct {
        cached: artifacts.CachedRead,
        fn page(raw: *anyopaque, alloc: A, store: *stores.ArtifactStore, ref: Ref, offset: u64, length: usize, _: [32]u8, token: Cancellation, budget: *u64) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.cached.context.ensureActive();
            if (offset != 0 or length != ref.byte_len) return error.InvalidNativeLakeTextCorpus;
            try stores.chargeReadBudget(budget, length);
            return artifacts.readArtifact(alloc, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached);
        }
    };
    var reader: Read = .{ .cached = cached };
    var store = store_value;
    var reads: u64 = 128 * 1024 * 1024;
    var writes: u64 = 0;
    var pages: inventory_pages.PageStore = .{ .domain = directory.domain, .artifacts = &store, .cancellation = cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .read_cache = .{ .ptr = &reader, .read = Read.page } };
    // Cursor pages and encoded descriptor scratch are released immediately;
    // only the parsed serving descriptors enter the admission allocator.
    var scratch_budget: local.sql_memory_budget = .{ .backing = scratch_backing, .limit = 8 * 1024 * 1024 };
    const scratch = scratch_budget.allocator();
    var cursor = try inventory_tree.Cursor.init(scratch, pages.store(), directory.serving_inventory, "", null);
    defer cursor.deinit();
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(scratch);
    var groups: std.ArrayList(FileGroup) = .empty;
    var segments: std.ArrayList(artifacts.ChunkRef) = .empty;
    while (try cursor.next()) |record| {
        try cached.context.ensureActive();
        try cancellation.check();
        if (record.key.len != 8 or record.value.len == 0 or record.value.len > inventory_chunk_bytes) return error.InvalidNativeLakeTextCorpus;
        const file = std.mem.readInt(u32, record.key[0..4], .big);
        const offset = std.mem.readInt(u32, record.key[4..8], .big);
        if (offset == 0 and bytes.items.len != 0) {
            try appendServingGroup(a, directory, bytes.items, &groups, &segments, cancellation);
            bytes.clearRetainingCapacity();
        }
        if (file != groups.items.len or offset != bytes.items.len or record.value.len > max_root_bytes -| bytes.items.len) return error.InvalidNativeLakeTextCorpus;
        try bytes.appendSlice(scratch, record.value);
    }
    if (bytes.items.len != 0) try appendServingGroup(a, directory, bytes.items, &groups, &segments, cancellation);
    if (groups.items.len != directory.manifests.len) return error.InvalidNativeLakeTextCorpus;
    var root = directory;
    root.file_groups = groups.items;
    root.segments = segments.items;
    try root.validateAlloc(scratch_backing);
    return root;
}
fn appendServingGroup(a: A, directory: Root, bytes: []const u8, groups: *std.ArrayList(FileGroup), segments: *std.ArrayList(artifacts.ChunkRef), cancellation: Cancellation) !void {
    if (groups.items.len >= directory.manifests.len or groups.items.len >= 16384) return error.InvalidNativeLakeTextCorpus;
    const manifest = directory.manifests[groups.items.len];
    if (manifest.byte_len != bytes.len) return error.InvalidNativeLakeTextCorpus;
    // Serving and maintenance/GC must describe exactly the same children.
    try stores.validatePayloadSha256WithCancellation(bytes, manifest.checksum, cancellation);
    const group = try std.json.parseFromSliceLeaky(FileGroup, a, bytes, .{ .allocate = .alloc_always });
    if (group.segments.len > max_segments -| segments.items.len) return error.InvalidNativeLakeTextCorpus;
    try groups.append(a, group);
    try segments.appendSlice(a, group.segments);
}
pub fn loadRoot(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Root {
    return loadRootView(a, store, ref, cancellation, cache, false);
}
pub fn loadServing(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Root {
    return loadRootView(a, store, ref, cancellation, cache, true);
}
fn loadRootView(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation, cache: ?artifacts.CachedRead, serving: bool) !Root {
    // Legacy/maintenance callers own the returned plain descriptors in their
    // allocator. Bound parse allocations (including unreclaimed arena scratch)
    // without imposing an unrelated sum of encoded artifact lengths.
    var budget: local.sql_memory_budget = .{ .backing = a, .limit = 64 * 1024 * 1024, .monotonic = true };
    const bounded = budget.allocator();
    var root = try loadRootDirectory(bounded, store, ref, cancellation, cache);
    if (serving) root = try servingRoot(bounded, store, root, cancellation, cache);
    // Explicit materializing maintenance callers retain the legacy plain-view
    // contract. Build/GC paths use loadRootDirectory and keep checkpoint pages.
    if (root.checkpoint_files != null or root.checkpoint_removed_files != null) {
        const records = try checkpointDeltaFiles(bounded, bounded, root, store, cancellation);
        const refs = try bounded.alloc(artifacts.ChunkRef, records.len);
        for (records, refs) |record, *manifest| manifest.* = record.manifest;
        root.manifests = refs;
        if (root.checkpoint_removed_files != null) {
            const removed = try checkpointRecords(bounded, root, root.checkpoint_removed_files, root.checkpoint_removed_count, store, cancellation);
            const ids = try bounded.alloc([]const u8, removed.len);
            for (removed, ids) |record, *id| id.* = record.file.id;
            root.checkpoint_removed = ids;
        }
        root.checkpoint_files = null;
        root.checkpoint_file_count = 0;
        root.checkpoint_removed_files = null;
        root.checkpoint_removed_count = 0;
    }
    root = try text_directory.withManifests(bounded, root, store, cache, cancellation);
    if (root.manifests.len != 0) {
        var groups: std.ArrayList(FileGroup) = .empty;
        var segments: std.ArrayList(artifacts.ChunkRef) = .empty;
        for (root.manifests) |manifest| {
            try cancellation.check();
            const group = try loadFileManifest(bounded, store, manifest, root.domain, cancellation, cache);
            if (group.segments.len > max_segments -| segments.items.len) return error.InvalidNativeLakeTextCorpus;
            try groups.append(bounded, group);
            try segments.appendSlice(bounded, group.segments);
        }
        root.file_groups = groups.items;
        root.segments = segments.items;
        try root.validate();
    }
    return root;
}
/// The serving cache supplies mapped, pinned native segment bytes. A bounded
/// heap loader is available to tests and hosts without a filesystem cache.
/// Ownership transfers into one snapshot publication, so global statistics
/// are constructed once rather than once for every appended segment.
pub const SegmentLoader = struct {
    ptr: *anyopaque,
    load: *const fn (*anyopaque, A, artifacts.ChunkRef, Cancellation) anyerror!local.index.SegmentData,
};
pub const CachedSegments = struct {
    store: stores.ArtifactStore,
    cache: artifacts.CachedRead,
    seekable: bool = false,
    query_owned: bool = false,
    resource_manager: ?*local.storage_resource_manager.ResourceManager = null,
    pub fn loader(self: *CachedSegments) SegmentLoader {
        return .{ .ptr = self, .load = load };
    }
    const OwnedLease = struct {
        a: A,
        lease: local.serverless_query_lake_serving_cache.Cache.ImmutableLease,
        fn adviseRandom(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.lease == .mapped) self.lease.mapped.adviseRandom();
        }
        fn discardCleanPages(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.lease == .mapped) self.lease.mapped.discardCleanPages();
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const a = self.a;
            self.lease.deinit();
            a.destroy(self);
        }
    };
    fn load(raw: *anyopaque, a: A, ref: artifacts.ChunkRef, cancellation: Cancellation) !local.index.SegmentData {
        const self: *CachedSegments = @ptrCast(@alignCast(raw));
        try cancellation.check();
        if (self.seekable) {
            const read: @import("lake_index_seekable_text.zig").Read = .{ .store = self.store, .cache = self.cache, .context = self.cache.context, .cancellation = cancellation, .resource_manager = self.resource_manager };
            return if (self.query_owned) @import("lake_index_seekable_text.zig").loadQueryScoped(a, read, ref) else @import("lake_index_seekable_text.zig").load(a, read, ref);
        }
        const Provider = struct {
            store: stores.ArtifactStore,
            ref: artifacts.ChunkRef,
            cancellation: Cancellation,
            fn load(ptr: *anyopaque, alloc: A) ![]u8 {
                const provider: *@This() = @ptrCast(@alignCast(ptr));
                return provider.store.getVerifiedAllocWithCancellationUsingAllocator(alloc, provider.ref.artifact_id, provider.ref.byte_len, provider.ref.checksum, provider.cancellation);
            }
        };
        var provider: Provider = .{ .store = self.store, .ref = ref, .cancellation = cancellation };
        const owner = try a.create(OwnedLease);
        errdefer a.destroy(owner);
        owner.* = .{ .a = a, .lease = try self.cache.cache.readImmutableLease(a, self.cache.scope, ref.artifact_id, std.math.cast(usize, ref.byte_len) orelse return error.InvalidNativeLakeTextCorpus, try stores.sha256DigestFromChecksum(ref.checksum), self.cache.context, .{ .ptr = &provider, .load = Provider.load }) };
        errdefer owner.lease.deinit();
        try cancellation.check();
        try self.cache.context.ensureActive();
        return .{ .owned_view = .{ .bytes = owner.lease.bytes(), .owner = owner, .release = OwnedLease.release, .file_backed = owner.lease == .mapped, .advise_random = OwnedLease.adviseRandom, .discard_clean_pages = OwnedLease.discardCleanPages } };
    }
};
pub fn loadWriter(a: A, store: stores.ArtifactStore, root: Root, cancellation: Cancellation, cache: ?artifacts.CachedRead, loader: ?SegmentLoader) !local.index.IndexWriter {
    if (root.statistics_deltas.len != 0) return error.InvalidNativeLakeTextCorpus;
    try root.validate();
    var writer = try local.index.IndexWriter.init(a);
    errdefer writer.deinit();
    const replacements = try a.alloc(local.index.ReplacementSegmentData, root.segments.len);
    defer a.free(replacements);
    var loaded: usize = 0;
    errdefer for (replacements[0..loaded]) |*replacement| replacement.data.deinit(a);
    var read_bytes: u64 = 512 * 1024 * 1024;
    for (root.segments, replacements, 0..) |segment, *replacement, ordinal| {
        try cancellation.check();
        if (loader == null and !root.seekable) try stores.chargeReadBudget(&read_bytes, segment.byte_len);
        const data = if (loader) |mapped| try mapped.load(mapped.ptr, a, segment, cancellation) else if (root.seekable) try @import("lake_index_seekable_text.zig").load(a, .{ .store = store, .cache = cache, .context = if (cache) |cached| cached.context else .{}, .cancellation = cancellation }, segment) else local.index.SegmentData.fromOwnedHeap(try artifacts.readArtifact(a, store, segment, cancellation, cache));
        replacement.* = .{ .id = ordinal + 1, .data = data };
        loaded += 1;
    }
    try cancellation.check();
    if (replacements.len != 0) try writer.replaceSegmentsManyData(&.{}, replacements);
    for (replacements) |replacement| {
        if (replacement.data == .native and replacement.data.native == .ranges) {
            const range = replacement.data.native.ranges;
            if (range.seal_read_context) |seal| seal(range.ptr);
        }
    }
    return writer;
}
pub fn build(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, base: local.serverless_manifest_base_source.BaseSourceDescriptor, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: Cancellation, reusable: []const Declared) ![]const Declared {
    return buildIncremental(a, out, table, source, base, store, provider, cancellation, reusable, &.{});
}
pub fn buildIncremental(a: A, out: A, table: local.common_topology_records.TableRecord, source: *local.serverless_query_lake_serving.ServingSource, base: local.serverless_manifest_base_source.BaseSourceDescriptor, store: *stores.ArtifactStore, provider: *@import("lake_index_row_source.zig").Provider, cancellation: Cancellation, reusable: []const Declared, candidates: []const Declared) ![]const Declared {
    var desired = try rebuild.desiredArtifactsFromResolvedExternalSourceAlloc(a, base, source.inventory, .{ .table_name = table.name, .schema_json = table.schema_json, .indexes_json = table.indexes_json });
    defer desired.deinit(a);
    var schema = try local.schema_mod.parseValidatedTableSchema(a, table.schema_json);
    defer schema.deinit(a);
    const runtime = try local.schema_mod.deriveRuntimeTableSchema(a, schema);
    defer local.storage_schema.freeSchema(a, runtime);
    var declarations: std.ArrayList(Declared) = .empty;
    errdefer declarations.deinit(out);
    for (desired.artifacts) |want| {
        if (want.kind != .text_segment) continue;
        const spec = want.build_spec.?.text;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const ca = arena.allocator();
        const config = try std.json.parseFromSliceLeaky(std.json.Value, ca, spec.config_json, .{});
        const store_source = try storesSource(config);
        const selected_field: ?[]const u8 = if (config.object.get("field")) |field| if (field == .string) field.string else null else null;
        var binding = want.binding;
        if (selected_field == null or store_source) {
            const paths = try ca.alloc([]const u8, runtime.relational_columns.len);
            for (paths, runtime.relational_columns) |*path, column| path.* = column.name;
            binding.column_bindings = paths;
        }
        binding.index_config_hash = try std.fmt.allocPrint(ca, "native-text-corpus-v11:{s}", .{want.binding.index_config_hash});
        const recipe = sourceRecipe(table, spec.config_json, store_source);
        const prior = for (reusable) |declaration| {
            if (declaration.artifact.kind == .text_segment and declaration.artifact.metadata_version == metadata_version and std.mem.eql(u8, declaration.name, want.name) and rebuild.bindingsEqual(declaration.binding, binding)) break declaration;
        } else null;
        var exact_seed: ?Root = null;
        var seed_artifact: ?[]const u8 = null;
        var seed_ref: ?artifacts.ChunkRef = null;
        if (prior) |declaration| {
            const directory_root = try loadRootDirectory(ca, store.*, declaration.artifact, cancellation, null);
            const root = if ((directory_root.inventory_version == 3 and directory_root.serving_routes == null) or directory_root.manifests.len != 0 or directory_root.checkpoint_files != null) directory_root else try loadRoot(ca, store.*, declaration.artifact, cancellation, null);
            if (root.source_layout_version == 1 and root.source_routes_version == 1 and root.serving != null and root.inventory_version == 3 and root.admission_version == 1 and root.statistics_version == 2 and root.term_directory.version == 2 and root.collection != null and root.serving_inventory == null and root.serving_routes == null and root.segment_files == null and std.mem.eql(u8, &root.recipe, &recipe)) {
                try validateBuildReuse(a, store.*, root, declaration.artifact, provider, cancellation);
                try declarations.append(out, declaration);
                continue;
            }
            if (root.seekable and root.statistics_version >= 1 and std.mem.eql(u8, &root.recipe, &recipe) and std.mem.eql(u8, &root.domain, &store.upload_scope.?.domain)) {
                try validateBuildReuse(a, store.*, root, declaration.artifact, provider, cancellation);
                seed_artifact = declaration.artifact.artifact_id;
                seed_ref = .{ .artifact_id = declaration.artifact.artifact_id, .checksum = declaration.artifact.checksum, .byte_len = declaration.artifact.byte_len };
                exact_seed = root;
            }
        }
        var seed: ?Root = if (exact_seed) |root| root else for (candidates) |declaration| {
            if (declaration.artifact.kind != .text_segment or declaration.artifact.metadata_version != metadata_version or !std.mem.eql(u8, declaration.name, want.name)) continue;
            var old = declaration.binding;
            old.snapshot_id = binding.snapshot_id;
            if (!rebuild.bindingsEqual(old, binding)) continue;
            const directory_root = try loadRootDirectory(ca, store.*, declaration.artifact, cancellation, null);
            const root = if ((directory_root.inventory_version == 3 and directory_root.serving_routes == null) or directory_root.manifests.len != 0 or directory_root.checkpoint_files != null) directory_root else try loadRoot(ca, store.*, declaration.artifact, cancellation, null);
            if (!root.seekable or root.statistics_version < 1 or !std.mem.eql(u8, &root.recipe, &recipe) or !std.mem.eql(u8, &root.domain, &store.upload_scope.?.domain)) continue;
            try validateBuildReuse(a, store.*, root, declaration.artifact, provider, cancellation);
            seed_artifact = declaration.artifact.artifact_id;
            seed_ref = .{ .artifact_id = declaration.artifact.artifact_id, .checksum = declaration.artifact.checksum, .byte_len = declaration.artifact.byte_len };
            break root;
        } else null;
        if (provider.resume_work) |work| if (work.file) |ordinal| {
            if ((if (seed) |old| old.file_count else 0) != ordinal) return error.InvalidNativeLakeFileState;
            if (seed != null and !work.trusts(store.*, candidates[0].artifact)) return error.InvalidNativeLakeFileState;
        };
        const paged_seed = if (seed) |root| root.inventory_version == 3 and root.serving_routes == null else false;
        const manifest_seed = if (seed) |root| !paged_seed and (root.manifests.len != 0 or root.checkpoint_base != null or root.checkpoint_files != null) and root.file_groups.len == 0 else false;
        const reference_seed = paged_seed or manifest_seed;
        var previous_groups: std.StringHashMapUnmanaged(FileGroup) = .empty;
        defer previous_groups.deinit(a);
        var previous_refs: std.StringHashMapUnmanaged(text_directory.FileRecord) = .empty;
        defer previous_refs.deinit(a);
        const baseline_ref: ?artifacts.ChunkRef = if (seed) |old| old.checkpoint_base orelse (if (old.serving != null) seed_ref else null) else null;
        const baseline: ?Root = if (seed) |old| if (old.checkpoint_base != null) try checkpointBase(ca, store.*, old, cancellation) else if (old.serving != null) old else null else null;
        // An authenticated prior inventory replaces a full native file-catalog
        // census for ordinary append/replacement publication. Recovery and delete
        // migrations retain the complete reconciliation path.
        const native_delta = if (paged_seed and baseline != null and seed.?.checkpoint_base == null) eligibleDelta(provider, baseline.?) else null;
        const inventory_delta = if (native_delta == null and paged_seed and baseline != null and seed.?.checkpoint_base == null) eligibleInventory(provider, baseline.?) else null;
        const appending = if (provider.resume_work) |work| work.file != null else false;
        var prepared_plan: ?state.Plan = if (appending) try state.Plan.initPaged(a, ca, provider, &.{}, if (baseline) |old| old.directories.files else null, baseline != null, false) else if (native_delta) |delta| try state.Plan.fromDelta(a, ca, delta) else if (inventory_delta) |inventory| try state.Plan.fromInventory(a, ca, provider, inventory) else null;
        errdefer if (prepared_plan) |*value| value.deinit();
        const baseline_records = if (appending) &.{} else if (native_delta) |delta| try changedDeltaRecords(ca, store.*, baseline.?, delta, cancellation) else if (inventory_delta) |inventory|
            try changedInventoryRecords(a, ca, store.*, baseline.?, provider, inventory, &prepared_plan.?, cancellation, 128 * 1024 * 1024)
        else if (baseline) |old| try text_directory.fileRecords(ca, old, store.*, null, cancellation) else &.{};
        const records = if (paged_seed) (if (baseline != null) baseline_records else try text_directory.fileRecords(ca, seed.?, store.*, null, cancellation)) else if (manifest_seed) try checkpointFiles(a, ca, seed.?, baseline_records, store.*, cancellation) else &.{};
        var checkpoint_removed: std.ArrayList([]const u8) = .empty;
        var baseline_by_id: std.StringHashMapUnmanaged(text_directory.FileRecord) = .empty;
        defer baseline_by_id.deinit(a);
        for (baseline_records) |record| try baseline_by_id.put(a, record.file.id, record);
        const previous = try ca.alloc(state.File, if (reference_seed) records.len else if (seed) |root| root.file_groups.len else 0);
        if (reference_seed) {
            for (previous, records) |*file, record| {
                file.* = record.file;
                try previous_refs.put(a, record.file.id, record);
            }
        } else if (seed) |root| for (previous, root.file_groups) |*file, group| {
            file.* = group.file;
            try previous_groups.put(a, group.file.id, group);
        };
        var plan = prepared_plan orelse try state.Plan.init(a, ca, provider, previous);
        prepared_plan = null;
        defer plan.deinit();
        for (baseline_records) |record| if (!plan.by_id.contains(record.file.id)) try checkpoint_removed.append(ca, record.file.id);
        if (paged_seed) {
            var removed_refs: std.ArrayList(artifacts.ChunkRef) = .empty;
            for (records) |record| if (!plan.unchanged(record.file)) try removed_refs.append(ca, record.manifest);
            // Keep only durable references. Removed/replaced admissions are
            // decoded in a file arena when their contributions are subtracted.
            seed.?.file_groups = &.{};
            seed.?.manifests = removed_refs.items;
        }
        const analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(a, spec.config_json, runtime);
        defer local.introducer.freeTextAnalysisConfig(a, analysis);
        var batch_arena = std.heap.ArenaAllocator.init(a);
        defer batch_arena.deinit();
        var builder = mapper.TextProjectionBatchBuilder.initWithSelectedField(batch_arena.allocator(), analysis, runtime, null, selected_field);
        var file_arena = std.heap.ArenaAllocator.init(a);
        defer file_arena.deinit();
        var input_bytes: usize = 0;
        var file_manifests: std.AutoHashMapUnmanaged(usize, artifacts.ChunkRef) = .empty;
        defer file_manifests.deinit(a);
        var checkpoint_files: ?statistics.Ref = if (seed) |old| old.checkpoint_files else null;
        var checkpoint_ids: std.StringHashMapUnmanaged(void) = .empty;
        defer checkpoint_ids.deinit(a);
        var pending_files: std.ArrayList(text_directory.FileRecord) = .empty;
        defer pending_files.deinit(a);
        if (manifest_seed) {
            const delta = try checkpointDeltaFiles(a, ca, seed.?, store.*, cancellation);
            var kept: std.ArrayList(text_directory.FileRecord) = .empty;
            defer kept.deinit(a);
            var removed_delta: std.ArrayList(text_directory.FileRecord) = .empty;
            defer removed_delta.deinit(a);
            for (delta) |record| {
                if (plan.by_id.contains(record.file.id)) {
                    try checkpoint_ids.put(a, record.file.id, {});
                    if (checkpoint_files == null) try kept.append(a, record);
                } else if (checkpoint_files != null) try removed_delta.append(a, record);
            }
            // Removed pending files are subtracted from DF below; the next
            // atomic save must remove them from the delta inventory as well.
            checkpoint_files = try text_directory.publishFileRecords(a, store, kept.items, removed_delta.items, checkpoint_files, cancellation, provider.context.io.?);
        }
        var removed_records: std.ArrayList(text_directory.FileRecord) = .empty;
        defer removed_records.deinit(a);
        for (checkpoint_removed.items) |id| try removed_records.append(a, baseline_by_id.get(id).?);
        const checkpoint_removed_files = if (removed_records.items.len != 0) try text_directory.publishFileRecords(a, store, removed_records.items, &.{}, null, cancellation, provider.context.io.?) else null;
        var checkpoints_saved: usize = 0;
        var checkpoint_time = @import("antfly_platform").time.monotonicNs();
        for (records) |record| if (plan.by_id.get(record.file.id)) |index| {
            try file_manifests.put(a, index, record.manifest);
        };
        var df_checkpoint: StatisticsCheckpoint = .{ .provider = provider, .cancellation = cancellation };
        var df_manager: local.sql_spill.Manager = .{ .alloc = a, .io = provider.context.io orelse return error.UnsupportedSqlExecution, .context = &df_checkpoint, .checkpoint = StatisticsCheckpoint.check };
        defer df_manager.deinit();
        var df_reads: u64 = 0;
        var df_writes: u64 = 0;
        const df_scope = store.upload_scope.?;
        var df_pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = df_scope.domain, .attempt = df_scope.attempt, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &df_reads, .remaining_write_bytes = &df_writes, .streaming = true };
        var df = statistics.Builder.init(a, &df_manager);
        defer df.deinit();
        df.enableDurable(df_pages.store());
        if (seed) |old| try df.durable.?.restore(old.statistics_deltas);
        var global_statistics: ?statistics.Ref = if (seed) |old| old.term_statistics else null;
        // Checkpoints retain the base plus signed durable contributions. Fold
        // global DF only once, including when recovering a partial refresh.
        if (seed) |old| for (old.file_groups) |group| {
            if (plan.by_id.get(group.file.id)) |index| {
                try file_manifests.put(a, index, try publishFileManifest(a, ca, store, group, cancellation));
            } else {
                for (group.summaries) |ref| try df.add(df_pages.store(), ref, -1);
            }
        };
        if (reference_seed) for (records) |record| {
            if (plan.by_id.contains(record.file.id)) continue;
            _ = file_arena.reset(.free_all);
            const old = try loadRecordedFile(file_arena.allocator(), store.*, record, seed.?.domain, cancellation);
            for (old.summaries) |ref| try df.add(df_pages.store(), ref, -1);
        };
        var planned_files = plan.scanIterator(paged_seed or (baseline != null and reference_seed));
        // Paged seeds need only changed files; legacy imports stream their full state once.
        while (try planned_files.next()) |planned| {
            const file = planned.file;
            const file_ordinal = planned.ordinal;
            const changed = plan.isChanged(file_ordinal);
            _ = file_arena.reset(.free_all);
            const fa = file_arena.allocator();
            var file_group: FileGroup = .{ .file = file, .segments = &.{} };
            const group = &file_group;
            var segments: std.ArrayList(artifacts.ChunkRef) = .empty;
            var summaries: std.ArrayList(?statistics.Ref) = .empty;
            var admissions: std.ArrayList(local.segment.SegmentReader.Admission) = .empty;
            if (!changed) {
                if (paged_seed or (baseline != null and reference_seed)) continue;
                const previous_group = if (manifest_seed) try loadRecordedFile(fa, store.*, previous_refs.get(file.id) orelse return error.InvalidNativeLakeTextCorpus, seed.?.domain, cancellation) else previous_groups.get(file.id) orelse return error.InvalidNativeLakeTextCorpus;
                group.segments = previous_group.segments;
                group.rows = previous_group.rows;
                group.summaries = previous_group.summaries;
                group.opening_packs = previous_group.opening_packs;
                group.admissions = previous_group.admissions;
                group.descriptors = previous_group.descriptors;
                group.physical_manifest = previous_group.physical_manifest;
                if (group.admissions.len == 0 and group.segments.len != 0) {
                    const upgraded = try fa.alloc(local.segment.SegmentReader.Admission, group.segments.len);
                    for (group.segments, upgraded) |ref, *metadata| {
                        var data = try @import("lake_index_seekable_text.zig").load(a, .{ .store = store.*, .cache = null, .context = provider.context, .cancellation = cancellation }, ref);
                        defer data.deinit(a);
                        var reader = try data.initReader(a);
                        defer reader.deinit();
                        metadata.* = try reader.admission(fa);
                    }
                    group.admissions = upgraded;
                }
                if (group.physical_manifest == null or group.descriptors.len != group.segments.len) try text_directory.prepare(fa, store, group, cancellation);
                if (group.summaries.len != group.segments.len) return error.InvalidNativeLakeTextCorpus;
                try file_manifests.put(a, file_ordinal, publishFileManifest(a, ca, store, group.*, cancellation) catch |err| return buildFailure(err, "file_manifest", file_ordinal, segments.items.len));
                continue;
            }
            var opening: @import("lake_index_text_opening_pack.zig").Collector = .{ .a = a, .out = fa, .store = store, .cancellation = cancellation };
            defer opening.deinit();
            var compactor: Compactor = .{ .a = a, .out = fa, .store = store, .segments = &segments, .summaries = &summaries, .admissions = &admissions, .cancellation = cancellation, .file_ordinal = file_ordinal, .opening = &opening, .io = provider.context.io };
            defer compactor.deinit();
            var ordinals = physical.Builder.init(a, fa, store, cancellation);
            defer ordinals.deinit();
            var previous_id: ?[96]u8 = null;
            var input = provider.*;
            input.only_file = file_ordinal;
            input.only_files = null;
            const rows = input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, binding, cancellation) catch |err| return buildFailure(err, "open_rows", file_ordinal, segments.items.len);
            defer rows.deinit(a);
            while (rows.next(a) catch |err| return buildFailure(err, "read_rows", file_ordinal, segments.items.len)) |batch| {
                for (0..batch.rowCount()) |row| {
                    const ref = batch.rowRef(row);
                    try provider.context.ensureActive();
                    try cancellation.check();
                    const page: local.sql_catalog.ColumnPage = .{ .batch = batch, .selection = &.{row} };
                    const ba = batch_arena.allocator();
                    var root: std.json.Value = .{ .object = .empty };
                    // Projection owns every source byte before the row source
                    // releases its page; no JSON serialization of source rows.
                    for (binding.column_bindings) |path| {
                        const cell = try page.cell(ba, 0, path);
                        const value = try local.api_json_helpers.cloneJsonValue(ba, cell.value);
                        try putPath(ba, &root, path, value);
                        if (value == .string) input_bytes +|= value.string.len;
                    }
                    const id = try plan.privateKey(ba, ref);
                    if (previous_id) |last_id| if (std.mem.order(u8, &last_id, id) != .lt) return error.InvalidNativeLakeTextCorpus;
                    previous_id = id[0..96].*;
                    input_bytes +|= id.len + 128;
                    const before = builder.batch().docs.len;
                    const stored = if (store_source) try std.json.Stringify.valueAlloc(ba, root, .{}) else "";
                    input_bytes +|= stored.len;
                    try builder.appendSourceDoc(.{ .key = id, .root = root, .stored_data = stored, .typed_source = if (store_source) root else null });
                    if (builder.batch().docs.len != before) {
                        if (ref != .external) return error.InvalidNativeLakeTextCorpus;
                        ordinals.add(ref.external.row_group_ordinal, ref.external.row_ordinal) catch |err| return buildFailure(err, "ordinal_pages", file_ordinal, segments.items.len);
                    }
                    if (builder.batch().docs.len >= 65536 or input_bytes >= 8 * 1024 * 1024 or batch_arena.queryCapacity() >= 24 * 1024 * 1024) {
                        try flush(&compactor, builder.batch(), analysis, store_source);
                        _ = batch_arena.reset(.free_all);
                        builder = mapper.TextProjectionBatchBuilder.initWithSelectedField(batch_arena.allocator(), analysis, runtime, null, selected_field);
                        input_bytes = 0;
                    }
                }
            }
            try flush(&compactor, builder.batch(), analysis, store_source);
            try compactor.finish();
            try opening.flush();
            group.opening_packs = opening.refs.items;
            ordinals.flush() catch |err| return buildFailure(err, "ordinal_pages", file_ordinal, segments.items.len);
            group.rows = ordinals.blocks.items;
            group.segments = segments.items;
            group.summaries = summaries.items;
            group.admissions = admissions.items;
            _ = batch_arena.reset(.free_all);
            builder = mapper.TextProjectionBatchBuilder.initWithSelectedField(batch_arena.allocator(), analysis, runtime, null, selected_field);
            input_bytes = 0;
            if (group.physical_manifest == null or group.descriptors.len != group.segments.len) try text_directory.prepare(fa, store, group, cancellation);
            try group.validate(store.upload_scope.?.domain);
            try file_manifests.put(a, file_ordinal, publishFileManifest(a, ca, store, group.*, cancellation) catch |err| return buildFailure(err, "file_manifest", file_ordinal, segments.items.len));
            if (previous_refs.get(file.id)) |record| {
                const old = try loadRecordedFile(fa, store.*, record, seed.?.domain, cancellation);
                for (old.summaries) |summary| try df.add(df_pages.store(), summary, -1);
            } else if (previous_groups.get(file.id)) |old| for (old.summaries) |ref| try df.add(df_pages.store(), ref, -1);
            for (group.summaries) |ref| try df.add(df_pages.store(), ref, 1);
            try df.flush();
            // Atomically checkpoint the completed file inventory and its DF
            // state. Save the first file, then at most 64 files or five seconds
            // of completed work; metadata traffic grows with the delta batch.
            if (provider.progress != null) {
                try pending_files.append(a, .{ .file = .{ .id = file.id, .digest = file.digest, .key_width = file.key_width }, .manifest = file_manifests.get(file_ordinal).? });
                const now = @import("antfly_platform").time.monotonicNs();
                if (checkpoints_saved == 0 or pending_files.items.len >= 64 or now -| checkpoint_time >= 5 * std.time.ns_per_s) {
                    var checkpoint_arena = std.heap.ArenaAllocator.init(a);
                    defer checkpoint_arena.deinit();
                    checkpoint_files = try text_directory.publishFileRecords(a, store, pending_files.items, &.{}, checkpoint_files, cancellation, provider.context.io.?);
                    for (pending_files.items) |record| try checkpoint_ids.put(a, record.file.id, {});
                    const partial: Root = .{ .checkpoint_base = baseline_ref, .checkpoint_files = checkpoint_files, .checkpoint_file_count = checkpoint_ids.count(), .checkpoint_removed_files = checkpoint_removed_files, .checkpoint_removed_count = @intCast(removed_records.items.len), .statistics_version = 1, .term_statistics = global_statistics, .statistics_deltas = try df.durable.?.snapshot(checkpoint_arena.allocator()), .seekable = true, .stored_projection = store_source, .domain = store.upload_scope.?.domain, .binding = binding, .config_json = spec.config_json, .recipe = recipe };
                    try provider.checkpoint(try publishRoot(a, checkpoint_arena.allocator(), store, want.name, partial, cancellation));
                    pending_files.clearRetainingCapacity();
                    checkpoints_saved += 1;
                    checkpoint_time = now;
                }
            }
        }
        global_statistics = try df.finish(df_pages.store(), global_statistics);
        // Global DF touches only changed file contributions. The spill manager
        // bounds disk independently of the sort's 4 MiB resident working set.
        var checkpoint: StatisticsCheckpoint = .{ .provider = provider, .cancellation = cancellation };
        var spill_manager: local.sql_spill.Manager = .{ .alloc = a, .io = provider.context.io orelse return error.UnsupportedSqlExecution, .context = &checkpoint, .checkpoint = StatisticsCheckpoint.check };
        defer spill_manager.deinit();
        var directory_builder = term_directory.Builder.init(a, &spill_manager);
        defer directory_builder.deinit();
        var removed_files: std.ArrayList(text_directory.FileRecord) = .empty;
        var added_files: std.ArrayList(text_directory.FileRecord) = .empty;
        for (baseline_records) |record| {
            const index = plan.by_id.get(record.file.id);
            if (index != null and std.mem.eql(u8, &record.file.digest, &(try plan.stateAt(index.?)).digest)) continue;
            try removed_files.append(ca, record);
        }
        var additions = plan.iterator(native_delta != null or inventory_delta != null);
        while (try additions.next()) |item| {
            const file = item.file;
            const slot = file_manifests.get(item.ordinal);
            if (baseline_by_id.get(file.id)) |old| if (std.mem.eql(u8, &old.file.digest, &file.digest)) continue;
            try added_files.append(ca, .{ .file = file, .manifest = slot orelse return error.InvalidNativeLakeTextCorpus });
        }
        const prior_directory: term_directory.Roots = if (baseline) |old| (if (old.term_directory.version == 2) old.term_directory else .{}) else .{};
        const incremental_directory = prior_directory.leaves != null;
        var stats_reads: u64 = 512 * 1024 * 1024;
        var stats_writes: u64 = 512 * 1024 * 1024;
        const scope = store.upload_scope.?;
        var stats_pages: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = &stats_reads, .remaining_write_bytes = &stats_writes, .streaming = true };
        directory_builder.enableDurable(stats_pages.store());
        // A disabled optional directory has no complete base to update. Paged
        // seeds hold only changed maintenance groups, so stream unchanged
        // contributions from the canonical serving directory when bootstrapping.
        if (baseline != null and !incremental_directory) {
            const Bootstrap = struct {
                a: A,
                pages: *@import("../serverless/graph_segment/page_store.zig").PageStore,
                plan: *const state.Plan,
                builder: *term_directory.Builder,
                provider: *@import("lake_index_row_source.zig").Provider,
                cancellation: Cancellation,
                pub fn check(self: *@This()) !void {
                    try self.provider.context.ensureActive();
                    try self.cancellation.check();
                }
                pub fn record(self: *@This(), entry: anytype) !void {
                    if (!self.plan.unchanged(entry.file)) return;
                    try addStatisticsContribution(self.a, self.pages.store(), entry.route.summary, term_directory.identity(entry.route.ref.artifact_id), 1, null, self.builder);
                }
            };
            var bootstrap: Bootstrap = .{ .a = a, .pages = &stats_pages, .plan = &plan, .builder = &directory_builder, .provider = provider, .cancellation = cancellation };
            try text_directory.walkSegments(a, baseline.?, store.*, cancellation, &bootstrap);
        }
        var totals: CollectionBuilder = .{ .a = a };
        defer totals.deinit();
        var removed_refs: std.ArrayList(artifacts.ChunkRef) = .empty;
        if (baseline) |old| {
            totals.documents = old.collection.?.documents;
            totals.segments = old.segment_count;
            for (old.collection.?.fields) |field| try totals.field(field.name, field.total_length, false);
            for (removed_files.items) |record| {
                try removed_refs.append(ca, record.manifest);
                _ = file_arena.reset(.free_all);
                const group = try loadFileManifest(file_arena.allocator(), store.*, record.manifest, old.domain, cancellation, null);
                try totals.add(group, true);
                if (incremental_directory) for (group.summaries, group.segments) |summary, segment| {
                    try addStatisticsContribution(a, stats_pages.store(), summary, term_directory.identity(segment.artifact_id), -1, null, &directory_builder);
                };
            }
        }
        var current_refs: std.ArrayList(artifacts.ChunkRef) = .empty;
        for (added_files.items) |record| {
            try current_refs.append(ca, record.manifest);
            _ = file_arena.reset(.free_all);
            const group = try loadFileManifest(file_arena.allocator(), store.*, record.manifest, store.upload_scope.?.domain, cancellation, null);
            for (group.summaries, group.segments) |summary, segment| {
                try addStatisticsContribution(a, stats_pages.store(), summary, term_directory.identity(segment.artifact_id), 1, null, &directory_builder);
            }
            try totals.add(group, false);
        }
        const directory = try directory_builder.finish(stats_pages.store(), prior_directory);
        const directories = try text_directory.publishManifests(a, store, current_refs.items, removed_refs.items, if (baseline) |old| old.directories else .{}, store.upload_scope.?.domain, cancellation, provider.context.io orelse return error.UnsupportedSqlExecution);
        var root: Root = .{ .admission_version = 1, .statistics_version = 2, .term_statistics = global_statistics, .term_directory = directory, .seekable = true, .stored_projection = store_source, .domain = store.upload_scope.?.domain, .binding = binding, .config_json = spec.config_json, .recipe = recipe, .directories = directories, .inventory_version = 3, .file_count = @intCast(plan.count()), .segment_count = totals.segments, .collection = try totals.finish(ca) };
        root.source_layout_version = 1;
        root.source_routes_version = 1;
        root.inventory_delta_version = if (provider.native_delta != null) 2 else if (source.inventory.deleted_row_groups.len == 0 and (if (source.scanner.iceberg_delete_plan) |deletes| deletes.files.len == 0 else true)) 1 else 0;
        root.serving = try @import("lake_index_serving_compaction.zig").publishDelta(a, ca, store, root, baseline, .{ .removed = removed_files.items, .added = added_files.items }, &plan, provider.context, cancellation);
        try root.validate();
        const bytes = try std.json.Stringify.valueAlloc(ca, root, .{});
        if (bytes.len > max_root_bytes) return error.NativeLakeTextCorpusTooLarge;
        var upload = store.*;
        upload.allocator = ca;
        const uploaded = try upload.putWithCancellation(bytes, cancellation);
        defer if (uploaded.provider_version) |version| upload.allocator.free(version);
        store.inheritClosure(uploaded.artifact_id, seed_artifact, cancellation) catch {
            try provider.context.ensureActive();
            try cancellation.check();
        };
        const owned = try std.json.Stringify.valueAlloc(ca, Declared{ .name = want.name, .binding = binding, .artifact = .{ .name = want.name, .kind = .text_segment, .metadata_version = metadata_version, .artifact_id = uploaded.artifact_id, .checksum = uploaded.checksum, .byte_len = uploaded.byte_len } }, .{});
        try declarations.append(out, try std.json.parseFromSliceLeaky(Declared, out, owned, .{ .allocate = .alloc_always }));
    }
    return declarations.toOwnedSlice(out);
}
fn eligibleDelta(provider: *RowProvider, baseline: Root) ?@import("lake_source_navigation.zig").Delta {
    const delta = provider.native_delta orelse return null;
    const snapshot = delta.prior_snapshot orelse return null;
    if (baseline.inventory_delta_version == 0 or baseline.file_count != delta.prior_count or
        !std.mem.eql(u8, snapshot, baseline.binding.snapshot_id) or
        !std.mem.eql(u8, delta.source, baseline.binding.source_id) or
        !std.mem.eql(u8, delta.schema, baseline.binding.schema_fingerprint)) return null;
    return delta;
}
fn changedDeltaRecords(a: A, store: stores.ArtifactStore, baseline: Root, delta: @import("lake_source_navigation.zig").Delta, token: Cancellation) ![]const text_directory.FileRecord {
    var remaining: u64 = 128 * 1024 * 1024;
    if (delta.prior_changed.len > 64 and delta.prior_changed.len * 4 >= baseline.file_count)
        return text_directory.fileRecordsBudget(a, baseline, store, null, token, &remaining);
    const result = try a.alloc(text_directory.FileRecord, delta.prior_changed.len);
    for (delta.prior_changed, result) |old, *record| {
        record.* = try text_directory.lookupFileBudget(a, baseline, store, null, token, old.id, &remaining);
        if (!std.mem.eql(u8, &record.file.digest, &old.digest)) return error.InvalidNativeLakeFileState;
    }
    return result;
}
fn eligibleInventory(provider: *RowProvider, baseline: Root) ?local.serverless_external_source_types.Inventory {
    const prior = provider.previous_inventory orelse return null;
    if (baseline.inventory_delta_version != 1 or prior.files.len != baseline.file_count or
        prior.format != provider.source.inventory.format or
        !std.mem.eql(u8, prior.source_id, provider.source.inventory.source_id) or
        !std.mem.eql(u8, prior.schema_fingerprint, provider.source.inventory.schema_fingerprint) or
        prior.deleted_row_groups.len != 0 or provider.source.inventory.deleted_row_groups.len != 0 or
        !std.mem.eql(u8, prior.snapshot_id, baseline.binding.snapshot_id) or
        !std.mem.eql(u8, prior.source_id, baseline.binding.source_id) or
        !std.mem.eql(u8, prior.schema_fingerprint, baseline.binding.schema_fingerprint)) return null;
    if (provider.source.scanner.iceberg_delete_plan) |plan| if (plan.files.len != 0) return null;
    // Unqualified legacy inventories cannot prove the private identities used
    // by their publication. They migrate through ordinary reconciliation.
    for (prior.files) |file| if (file.etag.len == 0 and file.version_id.len == 0) return null;
    return prior;
}
fn changedInventoryRecords(a: A, out: A, store: stores.ArtifactStore, baseline: Root, provider: *RowProvider, prior: local.serverless_external_source_types.Inventory, plan: *const state.Plan, token: Cancellation, max_read_bytes: u64) ![]const text_directory.FileRecord {
    var remaining = max_read_bytes;
    var changed: usize = 0;
    for (prior.files) |file| {
        const current = plan.by_id.get(file.file_id);
        if (current == null or plan.isChanged(current.?)) changed += 1;
    }
    // Broad replacement is cheaper as one bounded sequential catalog walk.
    if (changed > 64 and changed * 4 >= prior.files.len)
        return text_directory.fileRecordsBudget(out, baseline, store, null, token, &remaining);
    var records: std.ArrayList(text_directory.FileRecord) = .empty;
    for (prior.files) |file| {
        try provider.context.ensureActive();
        try token.check();
        const current = plan.by_id.get(file.file_id);
        if (current != null and !plan.isChanged(current.?)) continue;
        const record = try text_directory.lookupFileBudget(out, baseline, store, null, token, file.file_id, &remaining);
        const expected = try state.inventoryIdentity(a, prior, file, local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint());
        if (!std.mem.eql(u8, &record.file.digest, &expected)) return error.InvalidNativeLakeFileState;
        try records.append(out, record);
    }
    return records.items;
}
// Reusing manifests must prove their retained leaves still exist. HEAD checks
// bound memory and avoid downloading the archive; normal serving continues to
// authenticate posting ranges. Metadata trees are read and authenticated in
// bounded pages, including descendants not touched by incremental updates.
fn validateBuildReuse(a: A, store: stores.ArtifactStore, root: Root, ref: local.serverless_manifest_artifact_ref.ArtifactRef, provider: *@import("lake_index_row_source.zig").Provider, token: Cancellation) !void {
    try provider.context.ensureActive();
    try token.check();
    if (provider.resume_work) |work| if (work.trusts(store, ref)) return;
    return validateReuse(a, store, root, ref.artifact_id, provider.context, token);
}
fn validateReuse(backing_a: A, backend: stores.ArtifactStore, root: Root, root_id: []const u8, context: local.serverless_query_lake_read_context.Context, cancellation: Cancellation) anyerror!void {
    var retained_backend = backend;
    const certified = retained_backend.retainedClosure(root_id, cancellation) catch false;
    try context.ensureActive();
    try cancellation.check();
    if (certified) return;
    if (root.checkpoint_base) |base_ref| {
        var base_arena = std.heap.ArenaAllocator.init(backing_a);
        defer base_arena.deinit();
        const held = try checkpointBase(base_arena.allocator(), backend, root, cancellation);
        try validateReuse(backing_a, backend, held, base_ref.artifact_id, context, cancellation);
    }
    const tree = @import("../serverless/graph_segment/page_tree.zig");
    var locked: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = backing_a };
    const a = locked.allocator();
    const Validator = struct {
        const Pending = struct { task: local.sql_parallel_scheduler.Task(anyerror!void), ref: artifacts.ChunkRef };
        pending: [8]?Pending = @splat(null),
        cursor: usize = 0,
        a: A,
        store: stores.ArtifactStore,
        context: local.serverless_query_lake_read_context.Context,
        cancellation: Cancellation,
        domain: [32]u8,
        remaining_reads: u64 = tree.max_page_bytes,
        witnesses: std.AutoHashMapUnmanaged(stores.UploadScope, stores.RetentionWitness) = .empty,
        certifiable: bool = true,
        maintenance_only: bool = false,
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
            if (!v.certifiable) return;
            const witnesses = try v.a.alloc(stores.RetentionWitness, v.witnesses.count());
            defer v.a.free(witnesses);
            var it = v.witnesses.valueIterator();
            for (witnesses) |*entry| entry.* = it.next().?.*;
            v.store.rememberClosure(id, witnesses, v.cancellation) catch {
                try v.check();
            };
        }
        pub fn check(v: *@This()) !void {
            try v.context.ensureActive();
            try v.cancellation.check();
        }
        fn auditLeaf(a_: A, backend_: stores.ArtifactStore, ref: artifacts.ChunkRef, context_: local.serverless_query_lake_read_context.Context, token: Cancellation) anyerror!void {
            try context_.ensureActive();
            try token.check();
            var budget: local.sql_memory_budget = .{ .backing = a_, .limit = 64 * 1024 };
            const scratch = budget.allocator();
            var store_ = backend_;
            var metadata = try store_.statWithCancellationUsingAllocator(scratch, ref.artifact_id, token);
            defer metadata.deinit(scratch);
            if (metadata.byte_len != ref.byte_len or !std.mem.eql(u8, metadata.artifact_id, ref.artifact_id) or !std.mem.eql(u8, metadata.checksum, ref.checksum)) return error.ArtifactIntegrityMismatch;
        }
        fn finish(v: *@This(), index: usize, cancel: bool) !void {
            if (v.pending[index]) |entry| {
                var owned = entry;
                v.pending[index] = null;
                defer v.a.free(owned.ref.artifact_id);
                defer v.a.free(owned.ref.checksum);
                if (cancel) return owned.task.cancel(v.context.io.?);
                return owned.task.await(v.context.io.?);
            }
        }
        fn drain(v: *@This(), cancel: bool) void {
            for (0..v.pending.len) |i| v.finish(i, cancel) catch {};
        }
        fn leaf(v: *@This(), ref: artifacts.ChunkRef) !void {
            try v.check();
            try v.retainArtifact(ref.artifact_id);
            const io = v.context.io orelse return auditLeaf(v.a, v.store, ref, v.context, v.cancellation);
            const index = v.cursor;
            v.cursor = (index + 1) % v.pending.len;
            try v.finish(index, false);
            const id = try v.a.dupe(u8, ref.artifact_id);
            const checksum = v.a.dupe(u8, ref.checksum) catch |err| {
                v.a.free(id);
                return err;
            };
            const retained: artifacts.ChunkRef = .{ .artifact_id = id, .checksum = checksum, .byte_len = ref.byte_len };
            if (local.sql_parallel_scheduler.global().submitIoTransient(io, 64 * 1024, auditLeaf, .{ v.a, v.store, retained, v.context, v.cancellation })) |task| {
                v.pending[index] = .{ .task = task, .ref = retained };
            } else {
                defer v.a.free(checksum);
                defer v.a.free(id);
                try auditLeaf(v.a, v.store, retained, v.context, v.cancellation);
            }
        }
        pub fn skip(v: *@This(), ref: tree.Ref) !bool {
            try v.check();
            try v.retain(.{ .domain = v.domain, .attempt = ref.attempt });
            v.remaining_reads = tree.max_page_bytes;
            return false;
        }
        pub fn visit(_: *@This(), _: tree.Ref) !void {}
        fn pages(v: *@This(), ref: ?tree.Ref) !void {
            var writes: u64 = 0;
            var pages_store: @import("../serverless/graph_segment/page_store.zig").PageStore = .{ .domain = v.domain, .artifacts = &v.store, .cancellation = v.cancellation, .remaining_read_bytes = &v.remaining_reads, .remaining_write_bytes = &writes };
            if (ref) |page| try tree.walkPostOrder(v.a, pages_store.store(), page, v, false);
        }
        fn segment(v: *@This(), ref: artifacts.ChunkRef, seekable: bool) !void {
            try v.retainArtifact(ref.artifact_id);
            if (!seekable) return v.leaf(ref);
            try v.check();
            var scratch = std.heap.ArenaAllocator.init(v.a);
            defer scratch.deinit();
            const directory = try @import("lake_index_seekable_text.zig").loadDirectory(scratch.allocator(), .{ .store = v.store, .cache = null, .context = v.context, .cancellation = v.cancellation }, ref);
            if (directory.metadata_bundle) |bundle| try v.leaf(bundle);
            for ([_][]const @import("lake_index_seekable_text.zig").Piece{ directory.metadata, directory.blocks }) |pieces| {
                var prior: ?artifacts.ChunkRef = null;
                for (pieces) |piece| {
                    const retained = piece.retainedArtifact();
                    if (prior) |old| if (std.mem.eql(u8, old.artifact_id, retained.artifact_id)) continue;
                    try v.leaf(retained);
                    prior = retained;
                }
            }
        }
        pub fn record(v: *@This(), entry: anytype) !void {
            try v.check();
            for (entry.rows) |block| if (block.bitmap) |bitmap| try v.leaf(bitmap);
            if (entry.physical) |physical_ref| try v.leaf(physical_ref);
            try v.pages(entry.route.summary);
            if (v.maintenance_only) return;
            for (entry.source_pages) |page| try v.leaf(page);
            const spans = if (entry.source_pages.len != 0 and entry.sources.len == 0) try source_spans.load(v.a, v.store, entry.source_pages, entry.source_count, v.domain, v.cancellation, null, null) else entry.sources;
            for (spans) |span| try v.leaf(span.physical);
            try v.leaf(entry.route.descriptor orelse return error.InvalidNativeLakeTextCorpus);
            try v.pages(entry.route.summary);
            try v.segment(entry.route.ref, true);
        }
        pub fn route(v: *@This(), file: anytype) !void {
            try state.validate(&.{file.file}, v.domain);
            var scratch = std.heap.ArenaAllocator.init(v.a);
            defer scratch.deinit();
            const physical_ref = file.physical orelse return error.InvalidNativeLakeTextCorpus;
            try v.retainArtifact(physical_ref.artifact_id);
            const physical_group = try loadFileManifest(scratch.allocator(), v.store, physical_ref, v.domain, v.cancellation, null);
            if (!std.mem.eql(u8, physical_group.file.id, file.file.id) or !std.mem.eql(u8, &physical_group.file.digest, &file.file.digest)) return error.InvalidNativeLakeTextCorpus;
            for (physical_group.opening_packs) |pack| try v.leaf(pack.artifact);
            for (physical_group.rows) |block| if (block.bitmap) |bitmap| try v.leaf(bitmap);
            for (file.segments) |entry| {
                try text_directory.validateRef(entry.ref, v.domain);
                const descriptor = entry.descriptor orelse return error.InvalidNativeLakeTextCorpus;
                try text_directory.validateRef(descriptor, v.domain);
                try v.leaf(descriptor);
                try v.pages(entry.summary);
                try v.segment(entry.ref, true);
            }
        }
    };
    var validator: Validator = .{ .a = a, .store = backend, .context = context, .cancellation = cancellation, .domain = root.domain, .maintenance_only = root.serving != null };
    defer validator.drain(true);
    defer validator.witnesses.deinit(a);
    try validator.retainArtifact(root_id);
    if (root.serving) |ref| {
        var arena_ = std.heap.ArenaAllocator.init(a);
        defer arena_.deinit();
        const serving_root = try servingRoot(arena_.allocator(), backend, root, cancellation, null);
        try validateReuse(a, backend, serving_root, ref.artifact_id, context, cancellation);
    }
    try validator.pages(root.checkpoint_files);
    try validator.pages(root.checkpoint_removed_files);
    try validator.pages(root.pack_ownership);
    try validator.pages(root.source_routes);
    try validator.pages(root.tie_bounds);
    try validator.pages(root.term_statistics);
    for (root.statistics_deltas) |run| try validator.pages(run.page);
    try validator.pages(root.term_directory.leaves);
    try validator.pages(root.term_directory.branches);
    if (root.inventory_version == 3) {
        try validator.pages(root.directories.files);
        try validator.pages(root.directories.segments);
        try validator.pages(root.directories.routing);
        try validator.pages(root.serving_inventory);
        try validator.pages(root.segment_files);
        try validator.pages(root.serving_routes);
        try text_directory.walkSegments(a, root, backend, cancellation, &validator);
        if (root.serving_routes != null) try @import("lake_index_serving_inventory.zig").walkRoutes(a, root, backend, cancellation, &validator);
        for (0..validator.pending.len) |i| try validator.finish(i, false);
        try validator.certify(root_id);
        return;
    }
    if (root.file_groups.len == 0 and (root.manifests.len != 0 or root.checkpoint_files != null)) {
        var records_arena = std.heap.ArenaAllocator.init(a);
        defer records_arena.deinit();
        const checkpoint_records = try checkpointDeltaFiles(a, records_arena.allocator(), root, backend, cancellation);
        var file_arena = std.heap.ArenaAllocator.init(a);
        defer file_arena.deinit();
        for (checkpoint_records) |record| {
            const ref = record.manifest;
            _ = file_arena.reset(.free_all);
            try validator.leaf(ref);
            const group = try loadFileManifest(file_arena.allocator(), backend, ref, root.domain, cancellation, null);
            for (group.opening_packs) |pack| try validator.leaf(pack.artifact);
            for (group.summaries) |summary| try validator.pages(summary);
            for (group.rows) |block| if (block.bitmap) |bitmap| try validator.leaf(bitmap);
            for (group.descriptors) |descriptor| try validator.leaf(descriptor);
            if (group.physical_manifest) |physical_ref| try validator.leaf(physical_ref);
            for (group.segments) |segment| try validator.segment(segment, root.seekable);
        }
    }
    for (root.file_groups) |group| {
        for (group.opening_packs) |pack| try validator.leaf(pack.artifact);
        for (group.summaries) |summary| try validator.pages(summary);
        for (group.rows) |block| if (block.bitmap) |bitmap| try validator.leaf(bitmap);
    }
    for (root.segments) |ref| try validator.segment(ref, root.seekable);
    for (0..validator.pending.len) |i| try validator.finish(i, false);
    try validator.certify(root_id);
}

fn loadRecordedFile(a: A, store: stores.ArtifactStore, record: text_directory.FileRecord, domain: [32]u8, cancellation: Cancellation) !FileGroup {
    const group = try loadFileManifest(a, store, record.manifest, domain, cancellation, null);
    if (!std.mem.eql(u8, group.file.id, record.file.id) or !std.mem.eql(u8, &group.file.digest, &record.file.digest)) return error.InvalidNativeLakeTextCorpus;
    return group;
}

pub fn checkpointBase(a: A, store: stores.ArtifactStore, root: Root, cancellation: Cancellation) !Root {
    const ref = root.checkpoint_base orelse return error.InvalidNativeLakeTextCorpus;
    const base = try loadRootDirectory(a, store, .{ .kind = .text_segment, .metadata_version = metadata_version, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, null);
    if (base.checkpoint_base != null or base.serving == null or !std.mem.eql(u8, &base.domain, &root.domain) or !std.mem.eql(u8, &base.recipe, &root.recipe)) return error.InvalidNativeLakeTextCorpus;
    return base;
}
fn checkpointFiles(a: A, out: A, root: Root, records: []const text_directory.FileRecord, store: stores.ArtifactStore, cancellation: Cancellation) ![]const text_directory.FileRecord {
    const delta = try checkpointDeltaFiles(a, out, root, store, cancellation);
    if (root.checkpoint_base == null) return delta;
    var merged: std.StringHashMapUnmanaged(text_directory.FileRecord) = .empty;
    defer merged.deinit(a);
    var removed: std.StringHashMapUnmanaged(void) = .empty;
    defer removed.deinit(a);
    for (root.checkpoint_removed) |id| try removed.put(a, id, {});
    if (root.checkpoint_removed_files != null) for (try checkpointRecords(out, root, root.checkpoint_removed_files, root.checkpoint_removed_count, store, cancellation)) |record| try removed.put(a, record.file.id, {});
    for (records) |record| if (!removed.contains(record.file.id)) {
        try merged.put(a, record.file.id, record);
    };
    for (delta) |record| try merged.put(a, record.file.id, record);
    const result = try out.alloc(text_directory.FileRecord, merged.count());
    var it = merged.valueIterator();
    for (result) |*record| record.* = it.next().?.*;
    return result;
}
fn checkpointRecords(a: A, root: Root, ref: ?statistics.Ref, count: u32, store: stores.ArtifactStore, token: Cancellation) ![]const text_directory.FileRecord {
    var directory = root;
    directory.directories = .{ .files = ref };
    directory.file_count = count;
    return text_directory.fileRecords(a, directory, store, null, token);
}
pub fn checkpointDeltaFiles(a: A, out: A, root: Root, store: stores.ArtifactStore, cancellation: Cancellation) ![]const text_directory.FileRecord {
    if (root.checkpoint_files != null) return checkpointRecords(out, root, root.checkpoint_files, root.checkpoint_file_count, store, cancellation);
    const records = try out.alloc(text_directory.FileRecord, root.manifests.len);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for (root.manifests, records) |ref, *record| {
        try cancellation.check();
        _ = arena.reset(.free_all);
        const group = try loadFileManifest(arena.allocator(), store, ref, root.domain, cancellation, null);
        record.* = .{ .file = .{ .id = try out.dupe(u8, group.file.id), .digest = group.file.digest, .key_width = group.file.key_width }, .manifest = ref };
    }
    return records;
}

fn presentManifests(a: A, slots: []const ?artifacts.ChunkRef) ![]const artifacts.ChunkRef {
    var refs: std.ArrayList(artifacts.ChunkRef) = .empty;
    for (slots) |slot| if (slot) |ref| try refs.append(a, ref);
    return refs.toOwnedSlice(a);
}
pub fn publishFileManifest(a: A, out: A, store: *stores.ArtifactStore, group: FileGroup, cancellation: Cancellation) !artifacts.ChunkRef {
    var wire = group;
    if (wire.source_pages.len != 0) wire.sources = &.{};
    const bytes = try std.json.Stringify.valueAlloc(a, wire, .{});
    defer a.free(bytes);
    if (bytes.len > max_root_bytes) return error.NativeLakeTextCorpusTooLarge;
    var upload = store.*;
    upload.allocator = out;
    const ref = try upload.putWithCancellation(bytes, cancellation);
    defer if (ref.provider_version) |version| upload.allocator.free(version);
    return .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
}
fn publishRoot(a: A, out: A, store: *stores.ArtifactStore, name: []const u8, root: Root, cancellation: Cancellation) !Declared {
    try root.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, root, .{});
    defer a.free(bytes);
    if (bytes.len > max_root_bytes) return error.NativeLakeTextCorpusTooLarge;
    var upload = store.*;
    upload.allocator = a;
    var ref = try upload.putWithCancellation(bytes, cancellation);
    defer ref.deinit(a);
    const declaration: Declared = .{ .name = name, .binding = root.binding, .artifact = .{ .name = name, .kind = .text_segment, .metadata_version = metadata_version, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len } };
    const encoded = try std.json.Stringify.valueAlloc(a, declaration, .{});
    defer a.free(encoded);
    return std.json.parseFromSliceLeaky(Declared, out, encoded, .{ .allocate = .alloc_always });
}
/// Consolidate only adjacent fresh runs from one immutable file. Eight-run
/// pressure triggers one bounded merge; small files and inherited historical
/// runs retain their original bytes. Concatenation preserves physical ranks,
/// producer identity order and score ties without a cross-file coordinate map.
const Compactor = struct {
    const fan_in = 8;
    const target_bytes = 16 * 1024 * 1024;
    a: A,
    out: A,
    store: *stores.ArtifactStore,
    segments: *std.ArrayList(artifacts.ChunkRef),
    summaries: *std.ArrayList(?statistics.Ref),
    admissions: *std.ArrayList(local.segment.SegmentReader.Admission),
    cancellation: Cancellation,
    file_ordinal: usize,
    opening: ?*@import("lake_index_text_opening_pack.zig").Collector = null,
    io: ?std.Io = null,
    workspace_bytes: usize = 64 * 1024 * 1024,
    pending: [fan_in]?[]u8 = @splat(null),
    count: usize = 0,
    bytes: usize = 0,
    fn deinit(self: *Compactor) void {
        for (self.pending[0..self.count]) |value| self.a.free(value.?);
        self.count = 0;
        self.bytes = 0;
    }
    fn add(self: *Compactor, bytes: []const u8) !void {
        try self.cancellation.check();
        if (self.count != 0 and bytes.len > target_bytes -| self.bytes) try self.finish();
        if (bytes.len > target_bytes) return self.publish(bytes);
        self.pending[self.count] = try self.a.dupe(u8, bytes);
        self.count += 1;
        self.bytes += bytes.len;
        if (self.count == fan_in) try self.finish();
    }
    fn finish(self: *Compactor) !void {
        if (self.count == 0) return;
        defer self.deinit();
        try self.cancellation.check();
        if (self.count == fan_in) {
            // Optional acceleration cannot exhaust the publication allocator.
            // A pathological dictionary/source shape falls back to the exact
            // original runs, rather than failing ingestion or raising limits.
            var budget: local.sql_memory_budget = .{ .backing = self.a, .limit = @min(self.workspace_bytes, local.sql_memory_budget.headroom(self.a) orelse self.workspace_bytes) };
            const a = budget.allocator();
            const merged = self.merge(a) catch |err| switch (err) {
                error.OutOfMemory => null,
                else => return err,
            };
            if (merged) |bytes| {
                defer a.free(bytes);
                // A merge must save an artifact and respect the serving cap.
                if (bytes.len <= 32 * 1024 * 1024 and bytes.len <= self.bytes + 64 * 1024) {
                    try self.cancellation.check();
                    try self.publish(bytes);
                    return;
                }
            }
        }
        for (self.pending[0..self.count]) |bytes| try self.publish(bytes.?);
    }
    fn merge(self: *Compactor, a: A) ![]u8 {
        var readers: [fan_in]local.segment.SegmentReader = undefined;
        var opened: usize = 0;
        defer for (readers[0..opened]) |*reader| reader.deinit();
        var inputs: [fan_in]local.segment.MergeInput = undefined;
        for (self.pending[0..self.count], &readers, &inputs) |bytes, *reader, *input| {
            reader.* = try local.segment.SegmentReader.init(a, bytes.?);
            opened += 1;
            input.* = .{ .reader = reader };
        }
        return local.segment.mergeSegmentInputs(a, &inputs);
    }
    fn publish(self: *Compactor, bytes: []const u8) !void {
        const a = self.a;
        const out = self.out;
        const cancellation = self.cancellation;
        const store = self.store;
        const segments = self.segments;
        try cancellation.check();
        if (segments.items.len == max_segments or bytes.len > 32 * 1024 * 1024) return error.NativeLakeTextCorpusTooLarge;
        var upload = store.*;
        upload.allocator = out;
        const ref = @import("lake_index_seekable_text.zig").publishWithOpeningCollector(a, out, &upload, bytes, cancellation, self.opening) catch |err| return buildFailure(err, "segment_packs", self.file_ordinal, segments.items.len);
        var reader = try local.segment.SegmentReader.init(a, bytes);
        defer reader.deinit();
        try self.admissions.append(out, try reader.admission(out));
        const summary = statistics.publishSegmentWithIo(a, store, bytes, cancellation, self.io) catch |err| return buildFailure(err, "segment_statistics", self.file_ordinal, segments.items.len);
        try self.summaries.append(out, summary);
        try segments.append(out, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len });
    }
};
fn flush(compactor: *Compactor, batch: mapper.TextProjectionBatch, analysis: local.introducer.TextAnalysisConfig, store_source: bool) !void {
    try compactor.cancellation.check();
    for (batch.docs, 0..) |doc, i| {
        if (i != 0 and std.mem.order(u8, batch.docs[i - 1].id, doc.id) != .lt) return error.InvalidNativeLakeTextCorpus;
    }
    const encoded = mapper.buildTextSegmentsFromProjectionBatch(compactor.a, batch, analysis, .{ .target_segment_bytes = 8 * 1024 * 1024, .target_build_memory_bytes = 32 * 1024 * 1024, .store_document_source = store_source }) catch |err| return buildFailure(err, "encode_segments", compactor.file_ordinal, compactor.segments.items.len);
    defer mapper.freeTextSegments(compactor.a, encoded);
    for (encoded) |bytes| try compactor.add(bytes);
}
fn putPath(a: A, root: *std.json.Value, path: []const u8, value: std.json.Value) !void {
    if (std.mem.indexOfScalar(u8, path, '.')) |dot| {
        const key = path[0..dot];
        if (!root.object.contains(key)) try root.object.put(a, key, .{ .object = .empty });
        const child = root.object.getPtr(key).?;
        if (child.* != .object) return error.InvalidNativeLakeTextCorpus;
        try putPath(a, child, path[dot + 1 ..], value);
    } else try root.object.put(a, path, value);
}

const StatisticsCheckpoint = struct {
    provider: *@import("lake_index_row_source.zig").Provider,
    cancellation: Cancellation,
    fn check(raw: *anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.provider.context.ensureActive();
        try self.cancellation.check();
    }
};

/// Feed both indexes from a single authenticated summary cursor, including
/// version-1 upgrades where only the new directory needs unchanged metadata.
fn addStatisticsContribution(a: A, store: @import("../serverless/graph_segment/page_tree.zig").Store, root: ?statistics.Ref, segment: [32]u8, sign: i64, global: ?*statistics.Builder, directory: ?*term_directory.Builder) !void {
    const tree = @import("../serverless/graph_segment/page_tree.zig");
    var cursor = try tree.Cursor.init(a, store, root, "", null);
    defer cursor.deinit();
    while (try cursor.next()) |record| {
        if (global) |builder| try builder.addRecord(record, sign);
        if (directory) |builder| try builder.addRecord(record, segment, sign);
    }
}

fn buildFailure(err: anyerror, stage: []const u8, file_ordinal: usize, completed_segments: usize) anyerror {
    if (err != error.Canceled and err != error.Cancelled) std.log.warn("native lake text construction deferred stage={s} file_index={d} completed_segments={d} err={s}", .{ stage, file_ordinal, completed_segments, @errorName(err) });
    return err;
}

test "external lake text statistics wire upgrade fences new roots and admits historical v9 recovery" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("text-wire-upgrade");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    const legacy: Root = .{ .version = 9, .domain = store.upload_scope.?.domain, .binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} }, .config_json = "{}", .seekable = true, .statistics_version = 1 };
    // Reproduce a v9 root without extension fields introduced by this branch.
    var wire = try std.json.parseFromSliceLeaky(std.json.Value, ca, try std.json.Stringify.valueAlloc(ca, legacy, .{}), .{});
    for ([_][]const u8{ "term_directory", "directories", "collection", "file_count", "segment_count", "serving_routes", "segment_files", "partial_inventory", "routing" }) |field| _ = wire.object.swapRemove(field);
    const bytes = try std.json.Stringify.valueAlloc(ca, wire, .{});
    var upload = try store.putWithCancellation(bytes, .none);
    defer upload.deinit(a);
    var ref: Ref = .{ .kind = .text_segment, .metadata_version = 9, .artifact_id = upload.artifact_id, .checksum = upload.checksum, .byte_len = upload.byte_len };
    const recovered = try loadRootDirectory(ca, store, ref, .none, null);
    try std.testing.expectEqual(metadata_version, recovered.version);
    // Metadata identity is checked against the authenticated body before any
    // normalization. Neither future formats nor a falsely advertised v10 pass.
    ref.metadata_version = metadata_version;
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, loadRootDirectory(ca, store, ref, .none, null));
    ref.metadata_version = metadata_version + 1;
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, loadRootDirectory(ca, store, ref, .none, null));
    try std.testing.expect(metadata_version > 9 and local.metadata_lake_index_catalog.native_reader_protocol > 33);
}

test "external lake text compaction preserves all physical keys in input order" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("native-text-compaction");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    for ([_]usize{ 64 * 1024 * 1024, 1 }) |workspace| {
        var segments: std.ArrayList(artifacts.ChunkRef) = .empty;
        var summaries: std.ArrayList(?statistics.Ref) = .empty;
        var admissions: std.ArrayList(local.segment.SegmentReader.Admission) = .empty;
        var compactor: Compactor = .{ .a = a, .out = ca, .store = &store, .segments = &segments, .summaries = &summaries, .admissions = &admissions, .cancellation = .none, .file_ordinal = 0, .workspace_bytes = workspace };
        defer compactor.deinit();
        // Eight tiny runs become one; the next current-data run remains separate.
        const file_id: [64]u8 = @splat('0');
        for (0..9) |i| {
            const key = try std.fmt.allocPrint(ca, "lake1:{s}:00000000:{x:0>16}", .{ file_id, i + 10 });
            const bytes = (try mapper.buildTextSegmentFromDocuments(a, &.{.{ .key = key, .value = "{\"body\":\"quasar\"}" }}, .{}, null)).?;
            defer a.free(bytes);
            try compactor.add(bytes);
        }
        try compactor.finish();
        try std.testing.expectEqual(@as(usize, if (workspace == 1) 9 else 2), segments.items.len);
        try std.testing.expectEqual(@as(u32, if (workspace == 1) 1 else 8), admissions.items[0].doc_count);
        try std.testing.expectEqual(@as(u32, 1), admissions.items[1].doc_count);
        for (segments.items, 0..) |segment, i| {
            var data = try @import("lake_index_seekable_text.zig").load(a, .{ .store = store, .cache = null, .context = .{ .io = std.testing.io }, .cancellation = .none }, segment);
            defer data.deinit(a);
            var reader = try data.initReader(a);
            defer reader.deinit();
            for (0..reader.doc_count) |doc| {
                const key = (try reader.storedIdAlloc(a, @intCast(doc))).?;
                defer a.free(key);
                const expected = try std.fmt.allocPrint(a, "lake1:{s}:00000000:{x:0>16}", .{ file_id, (if (workspace == 1) i else if (i == 0) doc else 8) + 10 });
                defer a.free(expected);
                try std.testing.expectEqualStrings(expected, key);
            }
        }
    }
}

test "external lake global frontier checkpoints page sixteen thousand files and retain old cuts" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("paged-native-checkpoint-scale");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(9), 1, std.testing.io);
    const manifest_id = try store.upload_scope.?.artifactId(&std.fmt.bytesToHex(@as([32]u8, @splat(4)), .lower));
    const manifest: artifacts.ChunkRef = .{ .artifact_id = &manifest_id, .checksum = &std.fmt.bytesToHex(@as([32]u8, @splat(4)), .lower), .byte_len = 1 };
    const records = try ca.alloc(text_directory.FileRecord, 16384);
    for (records, 0..) |*record, i| record.* = .{ .file = .{ .id = try std.fmt.allocPrint(ca, "file-{d:0>5}", .{i}), .digest = @splat(1) }, .manifest = manifest };
    const files = try text_directory.publishFileRecords(a, &store, records, &.{}, null, .none, std.testing.io);
    const root: Root = .{ .domain = store.upload_scope.?.domain, .binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} }, .config_json = "{}", .seekable = true, .statistics_version = 1, .checkpoint_files = files, .checkpoint_file_count = @intCast(records.len) };
    try root.validate();
    const encoded = try std.json.Stringify.valueAlloc(ca, root, .{});
    try std.testing.expect(encoded.len < 16 * 1024);
    const recovered = try checkpointDeltaFiles(a, ca, root, store, .none);
    try std.testing.expectEqual(records.len, recovered.len);
    var replacement = records[7];
    replacement.file.digest = @splat(2);
    const next = try text_directory.publishFileRecords(a, &store, &.{replacement}, records[0..3], files, .none, std.testing.io);
    var resumed = root;
    resumed.checkpoint_files = next;
    resumed.checkpoint_file_count -= 3;
    const updated = try checkpointDeltaFiles(a, ca, resumed, store, .none);
    try std.testing.expectEqual(records.len - 3, updated.len);
    var found = false;
    for (updated) |record| if (std.mem.eql(u8, record.file.id, replacement.file.id)) {
        found = true;
        try std.testing.expectEqual(replacement.file.digest, record.file.digest);
    };
    try std.testing.expect(found);
    // The old durable cut remains independently readable after COW mutation.
    try std.testing.expectEqual(records.len, (try checkpointDeltaFiles(a, ca, root, store, .none)).len);
}

test "external lake bounded WAL inventory deltas seek changed records within a bounded planning budget" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const external = local.serverless_external_source_types;
    var directory = try local.common_test_directory.TestDirectory.init("inventory-delta-planning");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    var manifest = try store.put("immutable manifest reference");
    defer manifest.deinit(a);
    const prior_files = try ca.alloc(external.FileEntry, 4096);
    for (prior_files, 0..) |*file, i| file.* = .{ .file_id = try std.fmt.allocPrint(ca, "file-{d}", .{i}), .object_uri = try std.fmt.allocPrint(ca, "file:///source-{d}.parquet", .{i}), .etag = @constCast("old"), .byte_len = 128, .row_count = 0, .row_groups = &.{} };
    const prior: external.Inventory = .{ .format = .parquet, .source_id = @constCast("source"), .source_uri = @constCast("file:///source"), .snapshot_id = @constCast("before"), .schema_fingerprint = @constCast("schema"), .files = prior_files };
    const records = try ca.alloc(text_directory.FileRecord, prior_files.len);
    const states = try ca.alloc(state.File, prior_files.len);
    for (prior_files, records, states) |file, *record, *owned| {
        owned.* = .{ .id = file.file_id, .digest = try state.inventoryIdentity(ca, prior, file, local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint()) };
        record.* = .{ .file = owned.*, .manifest = .{ .artifact_id = manifest.artifact_id, .checksum = manifest.checksum, .byte_len = manifest.byte_len } };
    }
    const files = try text_directory.publishFileRecords(a, &store, records, &.{}, null, .none, std.testing.io);
    const baseline: Root = .{ .domain = store.upload_scope.?.domain, .binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "before", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} }, .config_json = "{}", .inventory_version = 3, .inventory_delta_version = 1, .file_count = @intCast(records.len), .directories = .{ .files = files } };
    var current = prior;
    current.snapshot_id = @constCast("after");
    current.files = try ca.dupe(external.FileEntry, prior_files);
    current.files[0].file_id = @constCast("appended");
    current.files[0].object_uri = @constCast("file:///appended.parquet");
    current.files[1].etag = @constCast("replacement");
    // This planner consumes only immutable inventory fields; it never opens a
    // source reader. Keep unused reader capabilities inaccessible in the test.
    var source: local.serverless_query_lake_serving.ServingSource = undefined;
    source.inventory = current;
    source.verified_files = null;
    source.prepared_deletes = null;
    source.scanner.iceberg_delete_plan = null;
    var provider: RowProvider = .{ .source = &source, .previous_inventory = prior, .context = .{ .io = std.testing.io } };
    try std.testing.expect(eligibleInventory(&provider, baseline) != null);
    var delta = try state.Plan.fromInventory(a, ca, &provider, prior);
    defer delta.deinit();
    var full = try state.Plan.init(a, ca, &provider, states);
    defer full.deinit();
    for (delta.files, full.files, delta.changed, full.changed, 0..) |left, right, changed, expected, i| {
        try std.testing.expectEqual(left.digest, right.digest);
        try std.testing.expectEqual(expected, changed);
        try std.testing.expectEqual(i < 2, changed);
    }
    const changed = try changedInventoryRecords(a, ca, store, baseline, &provider, prior, &delta, .none, 256 * 1024);
    try std.testing.expectEqual(@as(usize, 2), changed.len);
    var full_budget: u64 = 256 * 1024;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, text_directory.fileRecordsBudget(ca, baseline, store, null, .none, &full_budget));
    var different = baseline;
    different.inventory_delta_version = 0;
    try std.testing.expect(eligibleInventory(&provider, different) == null);
    different = baseline;
    different.binding.snapshot_id = "unrelated";
    try std.testing.expect(eligibleInventory(&provider, different) == null);
    prior_files[0].etag = &.{};
    try std.testing.expect(eligibleInventory(&provider, baseline) == null);
    // Coverage contributes exact selectors, while this snapshot owns sequence
    // and partition metadata even if the evidence came from a prior inventory.
    current.files[1].data_sequence_number = 9;
    var verified_file = current.files[1];
    verified_file.etag = @constCast("qualified");
    verified_file.data_sequence_number = 7;
    var verified: std.StringHashMapUnmanaged(*const external.FileEntry) = .empty;
    defer verified.deinit(a);
    try verified.put(a, verified_file.file_id, &verified_file);
    source.verified_files = &verified;
    var expected_file = current.files[1];
    expected_file.etag = verified_file.etag;
    try std.testing.expectEqual(try state.inventoryIdentity(a, current, expected_file, local.serverless_query_lake_prepared_deletes.Prepared.emptyFileFingerprint()), try state.identity(a, &provider, current.files[1]));
}
