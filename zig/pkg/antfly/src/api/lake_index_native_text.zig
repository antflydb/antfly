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
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
pub const metadata_version: u16 = 3;
pub const max_root_bytes = 4 * 1024 * 1024;
pub const max_segments = 8192;
pub const FileGroup = struct { file: state.File, segments: []const artifacts.ChunkRef };
pub const Root = struct {
    version: u16 = metadata_version,
    seekable: bool = false,
    domain: [32]u8,
    binding: local.serverless_segment_source_binding.Binding,
    config_json: []const u8,
    segments: []const artifacts.ChunkRef,
    recipe: [32]u8 = @splat(0),
    file_groups: []const FileGroup = &.{},
    pub fn validate(self: Root) !void {
        if (self.version != metadata_version or self.segments.len > max_segments or std.mem.allEqual(u8, &self.domain, 0) or self.config_json.len > 256 * 1024) return error.InvalidNativeLakeTextCorpus;
        try self.binding.validate();
        if (self.binding.sidecar_kind != .text) return error.InvalidNativeLakeTextCorpus;
        if (self.file_groups.len > 16384) return error.InvalidNativeLakeTextCorpus;
        var segment_position: usize = 0;
        for (self.file_groups) |group| {
            try state.validate(&.{group.file}, self.domain);
            for (group.segments) |segment| {
                if (segment_position >= self.segments.len or (!std.mem.eql(u8, segment.artifact_id, self.segments[segment_position].artifact_id) or !std.mem.eql(u8, segment.checksum, self.segments[segment_position].checksum) or segment.byte_len != self.segments[segment_position].byte_len)) return error.InvalidNativeLakeTextCorpus;
                segment_position += 1;
            }
        }
        if (self.file_groups.len != 0 and segment_position != self.segments.len) return error.InvalidNativeLakeTextCorpus;
        var identities: std.StringHashMapUnmanaged(void) = .empty;
        defer identities.deinit(std.heap.page_allocator);
        for (self.segments) |segment| {
            if (segment.byte_len == 0 or segment.byte_len > 32 * 1024 * 1024) return error.InvalidNativeLakeTextCorpus;
            try stores.validateSha256ArtifactIdentity(segment.artifact_id, segment.checksum);
            const scope = (try stores.uploadScopeFromArtifactId(segment.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
            if (!std.mem.eql(u8, &scope.domain, &self.domain) or (try identities.getOrPut(std.heap.page_allocator, segment.artifact_id)).found_existing) return error.InvalidNativeLakeTextCorpus;
        }
    }
};
pub fn loadRoot(a: A, store: stores.ArtifactStore, ref: Ref, cancellation: Cancellation, cache: ?artifacts.CachedRead) !Root {
    if (ref.kind != .text_segment or ref.metadata_version != metadata_version or ref.byte_len > max_root_bytes) return error.InvalidNativeLakeTextCorpus;
    const bytes = try artifacts.readArtifact(a, store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, cancellation, cache);
    defer a.free(bytes);
    const root = try std.json.parseFromSliceLeaky(Root, a, bytes, .{ .allocate = .alloc_always });
    try root.validate();
    const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
    if (!std.mem.eql(u8, &root.domain, &scope.domain)) return error.InvalidNativeLakeTextCorpus;
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
            const read: @import("lake_index_seekable_text.zig").Read = .{ .store = self.store, .cache = self.cache, .context = self.cache.context, .cancellation = cancellation };
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
        try stores.chargeReadBudget(&read_bytes, segment.byte_len);
        const data = if (loader) |mapped| try mapped.load(mapped.ptr, a, segment, cancellation) else if (root.seekable) try @import("lake_index_seekable_text.zig").load(a, .{ .store = store, .cache = cache, .context = if (cache) |cached| cached.context else .{}, .cancellation = cancellation }, segment) else local.index.SegmentData.fromOwnedHeap(try artifacts.readArtifact(a, store, segment, cancellation, cache));
        replacement.* = .{ .id = ordinal + 1, .data = data };
        loaded += 1;
    }
    try cancellation.check();
    if (replacements.len != 0) try writer.replaceSegmentsManyData(&.{}, replacements);
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
        const selected_field: ?[]const u8 = if (config.object.get("field")) |field| if (field == .string) field.string else null else null;
        var binding = want.binding;
        if (selected_field == null) {
            const paths = try ca.alloc([]const u8, runtime.relational_columns.len);
            for (paths, runtime.relational_columns) |*path, column| path.* = column.name;
            binding.column_bindings = paths;
        }
        binding.index_config_hash = try std.fmt.allocPrint(ca, "native-text-corpus-v3:{s}", .{want.binding.index_config_hash});
        const recipe = state.recipe(table, spec.config_json);
        const prior = for (reusable) |declaration| {
            if (declaration.artifact.kind == .text_segment and declaration.artifact.metadata_version == metadata_version and std.mem.eql(u8, declaration.name, want.name) and rebuild.bindingsEqual(declaration.binding, binding)) break declaration;
        } else null;
        if (prior) |declaration| {
            const root = try loadRoot(ca, store.*, declaration.artifact, cancellation, null);
            if (std.mem.eql(u8, &root.recipe, &recipe)) {
                try declarations.append(out, declaration);
                continue;
            }
        }
        const seed: ?Root = for (candidates) |declaration| {
            if (declaration.artifact.kind != .text_segment or declaration.artifact.metadata_version != metadata_version or !std.mem.eql(u8, declaration.name, want.name)) continue;
            var old = declaration.binding;
            old.snapshot_id = binding.snapshot_id;
            if (!rebuild.bindingsEqual(old, binding)) continue;
            const root = try loadRoot(ca, store.*, declaration.artifact, cancellation, null);
            if (!std.mem.eql(u8, &root.recipe, &recipe) or !std.mem.eql(u8, &root.domain, &store.upload_scope.?.domain)) continue;
            break root;
        } else null;
        var previous_groups: std.StringHashMapUnmanaged(FileGroup) = .empty;
        defer previous_groups.deinit(a);
        if (seed) |root| for (root.file_groups) |group| try previous_groups.put(a, group.file.id, group);
        const previous = try ca.alloc(state.File, if (seed) |root| root.file_groups.len else 0);
        if (seed) |root| for (previous, root.file_groups) |*file, group| {
            file.* = group.file;
        };
        var plan = try state.Plan.init(a, ca, provider, previous);
        defer plan.deinit();
        const groups = try ca.alloc(FileGroup, plan.files.len);
        const analysis = try local.storage_db_catalog_index_manager.parseTextAnalysisForIndexConfig(a, spec.config_json, runtime);
        defer local.introducer.freeTextAnalysisConfig(a, analysis);
        var batch_arena = std.heap.ArenaAllocator.init(a);
        defer batch_arena.deinit();
        var builder = mapper.TextProjectionBatchBuilder.initWithSelectedField(batch_arena.allocator(), analysis, runtime, null, selected_field);
        var segments: std.ArrayList(artifacts.ChunkRef) = .empty;
        var input_bytes: usize = 0;
        var output_bytes: usize = 0;
        for (groups, plan.files, plan.changed, 0..) |*group, file, changed, file_ordinal| {
            group.* = .{ .file = file, .segments = &.{} };
            if (!changed) {
                const previous_group = previous_groups.get(file.id) orelse return error.InvalidNativeLakeTextCorpus;
                group.segments = previous_group.segments;
                try segments.appendSlice(ca, group.segments);
                continue;
            }
            const first_segment = segments.items.len;
            var input = provider.*;
            input.only_file = file_ordinal;
            input.only_files = null;
            const rows = try input.provider().open_with_cancellation_fn.?(input.provider().ptr, a, binding, cancellation);
            defer rows.deinit(a);
            while (try rows.next(a)) |batch| {
                for (batch.row_refs, 0..) |ref, row| {
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
                    input_bytes +|= id.len + 128;
                    try builder.appendSourceDoc(.{ .key = id, .root = root, .stored_data = "", .typed_source = null });
                    if (builder.batch().docs.len >= 1024 or input_bytes >= 2 * 1024 * 1024) {
                        try flush(a, ca, store, builder.batch(), analysis, &segments, &output_bytes, cancellation);
                        _ = batch_arena.reset(.retain_capacity);
                        builder = mapper.TextProjectionBatchBuilder.initWithSelectedField(batch_arena.allocator(), analysis, runtime, null, selected_field);
                        input_bytes = 0;
                    }
                }
            }
            try flush(a, ca, store, builder.batch(), analysis, &segments, &output_bytes, cancellation);
            group.segments = try ca.dupe(artifacts.ChunkRef, segments.items[first_segment..]);
            _ = batch_arena.reset(.retain_capacity);
            builder = mapper.TextProjectionBatchBuilder.initWithSelectedField(batch_arena.allocator(), analysis, runtime, null, selected_field);
            input_bytes = 0;
        }
        const root: Root = .{ .seekable = true, .domain = store.upload_scope.?.domain, .binding = binding, .config_json = spec.config_json, .segments = segments.items, .recipe = recipe, .file_groups = groups };
        try root.validate();
        const bytes = try std.json.Stringify.valueAlloc(ca, root, .{});
        if (bytes.len > max_root_bytes) return error.NativeLakeTextCorpusTooLarge;
        var upload = store.*;
        upload.allocator = ca;
        const uploaded = try upload.putWithCancellation(bytes, cancellation);
        const owned = try std.json.Stringify.valueAlloc(ca, Declared{ .name = want.name, .binding = binding, .artifact = .{ .name = want.name, .kind = .text_segment, .metadata_version = metadata_version, .artifact_id = uploaded.artifact_id, .checksum = uploaded.checksum, .byte_len = uploaded.byte_len } }, .{});
        try declarations.append(out, try std.json.parseFromSliceLeaky(Declared, out, owned, .{ .allocate = .alloc_always }));
    }
    return declarations.toOwnedSlice(out);
}
fn flush(a: A, out: A, store: *stores.ArtifactStore, batch: mapper.TextProjectionBatch, analysis: local.introducer.TextAnalysisConfig, segments: *std.ArrayList(artifacts.ChunkRef), output_bytes: *usize, cancellation: Cancellation) !void {
    try cancellation.check();
    const encoded = try mapper.buildTextSegmentsFromProjectionBatch(a, batch, analysis, .{ .target_segment_bytes = 8 * 1024 * 1024, .target_build_memory_bytes = 32 * 1024 * 1024, .store_document_source = false });
    defer mapper.freeTextSegments(a, encoded);
    for (encoded) |bytes| {
        try cancellation.check();
        if (segments.items.len == max_segments or bytes.len > 32 * 1024 * 1024 or bytes.len > 512 * 1024 * 1024 -| output_bytes.*) return error.NativeLakeTextCorpusTooLarge;
        output_bytes.* += bytes.len;
        var upload = store.*;
        upload.allocator = out;
        const ref = try @import("lake_index_seekable_text.zig").publish(a, out, &upload, bytes, cancellation);
        try segments.append(out, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len });
    }
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
