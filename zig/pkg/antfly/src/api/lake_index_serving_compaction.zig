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

//! Bounded cross-file serving packs, independent of canonical file maintenance.
//! Unchanged closed packs remain byte-identical. Small open tails and packs
//! touched by source replacement/removal are the only historical payload reads.
const std = @import("std");
const local = @import("antfly_local_sources");
const corpus = @import("lake_index_native_text.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const state = @import("lake_index_native_state.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages_mod = @import("../serverless/graph_segment/page_store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Chunk = artifacts.ChunkRef;
const Span = corpus.source_spans.Span;
const Input = struct { ref: Chunk, admission: local.segment.SegmentReader.Admission, sources: []const Span, original_docs: u32, summary: ?corpus.statistics.Ref };
const Builder = struct {
    a: A,
    out: A,
    store: *stores.ArtifactStore,
    context: local.serverless_query_lake_read_context.Context,
    token: Cancellation,
    pages: *pages_mod.PageStore,
    terms: *corpus.term_directory.Builder,
    ownership: *corpus.text_directory.Mutations,
    source_routes: *corpus.text_directory.Mutations,
    open_packs: std.ArrayList(Chunk) = .empty,
    added: std.ArrayList(Chunk) = .empty,
    removed: std.ArrayList(Chunk) = .empty,
    // Four eight-way tiers fill tiny-file packs by bytes. Source spans live in
    // independent pages; no merge opens more than eight posting inputs.
    pending: [4][8]Input = undefined,
    count: [4]usize = @splat(0),
    bytes: [4]u64 = @splat(0),
    spans: [4]usize = @splat(0),
    open_counts: [4]usize = @splat(0),
    pending_arena: [4]std.heap.ArenaAllocator,
    metadata_bytes: [4]usize = @splat(0),
    pack_count: u32 = 0,
    serving_bytes: u64 = 0,
    fn check(self: *@This()) !void {
        try self.context.ensureActive();
        try self.token.check();
    }
    fn contribution(self: *@This(), group: corpus.FileGroup, sign: i64) !void {
        for (group.summaries, group.segments) |summary, segment| try self.terms.add(self.pages.store(), summary, corpus.term_directory.identity(segment.artifact_id), sign);
    }
    fn owner(self: *@This(), group: corpus.FileGroup, manifest: Chunk, deleting: bool) !void {
        try @import("lake_index_source_routes.zig").mutate(self.a, self.source_routes, group, deleting);
        const a = self.pending_arena[0].child_allocator;
        const bytes = try std.json.Stringify.valueAlloc(a, manifest, .{});
        defer a.free(bytes);
        for (group.sources) |span| {
            var key: [64]u8 = undefined;
            key[0..32].* = corpus.term_directory.identity(span.file.id);
            key[32..64].* = corpus.term_directory.identity(group.file.id);
            try self.ownership.addPrefix(&key, if (deleting) null else bytes, bytes.len);
        }
    }
    fn add(self: *@This(), input: Input) !void {
        var level: usize = 0;
        var threshold: usize = 8;
        while (level < 3 and input.sources.len >= threshold) : (level += 1) threshold *= 8;
        return self.addAt(level, input);
    }
    fn addAt(self: *@This(), level: usize, input: Input) !void {
        try self.check();
        var metadata: usize = 0;
        for (input.sources) |span| metadata += span.file.id.len + 512;
        if (self.count[level] != 0 and (self.bytes[level] + input.admission.bytes > 16 * 1024 * 1024 or self.spans[level] + input.sources.len > corpus.source_spans.max_spans or self.metadata_bytes[level] + metadata > 1024 * 1024)) try self.flush(level);
        const a = self.pending_arena[level].allocator();
        const bytes = try std.json.Stringify.valueAlloc(a, input, .{});
        self.pending[level][self.count[level]] = try std.json.parseFromSliceLeaky(Input, a, bytes, .{ .allocate = .alloc_always });
        self.count[level] += 1;
        self.bytes[level] += input.admission.bytes;
        self.spans[level] += input.sources.len;
        self.metadata_bytes[level] += metadata;
        if (self.count[level] == 8 or self.spans[level] == corpus.source_spans.max_spans) try self.flush(level);
    }
    fn flush(self: *@This(), level: usize) anyerror!void {
        if (self.count[level] == 0) return;
        try self.emit(self.pending[level][0..self.count[level]], level);
        self.count[level] = 0;
        self.bytes[level] = 0;
        self.spans[level] = 0;
        self.metadata_bytes[level] = 0;
        _ = self.pending_arena[level].reset(.free_all);
    }
    fn emit(self: *@This(), inputs: []const Input, level: usize) anyerror!void {
        try self.check();
        var memory: local.sql_memory_budget = .{ .backing = self.a, .limit = 64 * 1024 * 1024 };
        const a = memory.allocator();
        var arena = std.heap.ArenaAllocator.init(a);
        var arena_live = true;
        defer if (arena_live) arena.deinit();
        const out = arena.allocator();
        var sources: std.ArrayList(Span) = .empty;
        var count: u32 = 0;
        for (inputs) |input| for (input.sources) |span_| {
            var span = span_;
            span.pack_lower = count;
            count = try std.math.add(u32, count, span.count);
            try sources.append(out, span);
        };
        if (count == 0) return;
        const reuse = inputs.len == 1 and count == inputs[0].original_docs;
        var group: corpus.FileGroup = undefined;
        if (reuse) {
            const input = inputs[0];
            group = .{ .file = undefined, .sources = sources.items, .segments = try out.dupe(Chunk, &.{input.ref}), .admissions = try out.dupe(local.segment.SegmentReader.Admission, &.{input.admission}), .summaries = try out.dupe(?corpus.statistics.Ref, &.{input.summary}) };
        } else {
            const encoded = self.merge(a, inputs) catch |err| switch (err) {
                error.OutOfMemory => {
                    if (inputs.len == 1) return err;
                    const mid = inputs.len / 2;
                    try self.emit(inputs[0..mid], level);
                    try self.emit(inputs[mid..], level);
                    return;
                },
                else => return err,
            };
            defer a.free(encoded);
            if (encoded.len > 32 * 1024 * 1024) {
                if (inputs.len == 1) return error.NativeLakeTextCorpusTooLarge;
                const mid = inputs.len / 2;
                try self.emit(inputs[0..mid], level);
                try self.emit(inputs[mid..], level);
                return;
            }
            const ref = try @import("lake_index_seekable_text.zig").publish(a, out, self.store, encoded, self.token);
            var reader = try local.segment.SegmentReader.init(a, encoded);
            defer reader.deinit();
            const admission = try reader.admission(out);
            if (admission.doc_count != count) return error.InvalidNativeLakeTextCorpus;
            group = .{ .file = undefined, .sources = sources.items, .segments = try out.dupe(Chunk, &.{ref}), .admissions = try out.dupe(local.segment.SegmentReader.Admission, &.{admission}), .summaries = try out.dupe(?corpus.statistics.Ref, &.{try corpus.statistics.publishSegmentWithIo(a, self.store, encoded, self.token, self.context.io)}) };
        }
        if (level < 3 and group.sources.len >= std.math.pow(usize, 8, level + 1) and group.sources.len < corpus.source_spans.max_spans and group.admissions[0].bytes < 2 * 1024 * 1024) {
            // Only final packs own directory/ownership/statistics entries.
            // Intermediate immutable segments remain ordinary attempt orphans.
            var carry = std.heap.ArenaAllocator.init(self.a);
            defer carry.deinit();
            const encoded = try std.json.Stringify.valueAlloc(carry.allocator(), Input{ .ref = group.segments[0], .admission = group.admissions[0], .sources = group.sources, .original_docs = count, .summary = group.summaries[0] }, .{});
            const input = try std.json.parseFromSliceLeaky(Input, carry.allocator(), encoded, .{ .allocate = .alloc_always });
            // A second-tier flush must not overlap the first merge workspace.
            arena.deinit();
            arena_live = false;
            try self.addAt(level + 1, input);
            return;
        }
        const digest = corpus.term_directory.identity(group.segments[0].artifact_id);
        const id = try std.fmt.allocPrint(out, "pack-{x}", .{&digest});
        group.file = .{ .id = id, .digest = digest };
        try corpus.text_directory.prepare(out, self.store, &group, self.token);
        const manifest = try put(out, self.out, self.store, group, self.token);
        try self.added.append(self.out, manifest);
        try self.owner(group, manifest, false);
        var tier: usize = 0;
        var threshold: usize = 8;
        while (tier < 3 and group.sources.len >= threshold) : (tier += 1) threshold *= 8;
        var source_metadata: usize = 0;
        for (group.sources) |span| source_metadata += span.file.id.len + 512;
        if (group.sources.len < corpus.source_spans.max_spans and source_metadata < 512 * 1024 and group.admissions[0].bytes < 2 * 1024 * 1024 and self.open_counts[tier] < 7) {
            try self.open_packs.append(self.out, manifest);
            self.open_counts[tier] += 1;
        }
        try self.contribution(group, 1);
        self.pack_count = try std.math.add(u32, self.pack_count, 1);
        self.serving_bytes = try std.math.add(u64, self.serving_bytes, group.admissions[0].bytes);
    }
    fn merge(self: *@This(), a: A, inputs: []const Input) ![]u8 {
        var data: [8]local.index.SegmentData = undefined;
        var readers: [8]local.segment.SegmentReader = undefined;
        var deleted: [8]local.encoding_roaring.RoaringBitmap = undefined;
        var merged: [8]local.segment.MergeInput = undefined;
        var opened: usize = 0;
        defer for (0..opened) |i| {
            deleted[i].deinit();
            readers[i].deinit();
            data[i].deinit(a);
        };
        for (inputs, 0..) |input, i| {
            try self.check();
            data[i] = try @import("lake_index_seekable_text.zig").load(a, .{ .store = self.store.*, .cache = null, .context = self.context, .cancellation = self.token }, input.ref);
            readers[i] = data[i].initReader(a) catch |err| {
                data[i].deinit(a);
                return err;
            };
            deleted[i] = .init(a);
            opened += 1;
            var previous: u32 = 0;
            for (input.sources) |span| {
                if (span.pack_lower < previous or @as(u64, span.pack_lower) + span.count > input.original_docs) return error.InvalidNativeLakeTextCorpus;
                try deleted[i].addRange(previous, span.pack_lower);
                previous = span.pack_lower + span.count;
            }
            try deleted[i].addRange(previous, input.original_docs);
            merged[i] = .{ .reader = &readers[i], .deleted = deleted[i] };
        }
        return local.segment.mergeSegmentInputs(a, merged[0..inputs.len]);
    }
};
fn put(a: A, out: A, store: *stores.ArtifactStore, value: anytype, token: Cancellation) !Chunk {
    var wire = value;
    if (@TypeOf(value) == corpus.FileGroup) if (wire.source_pages.len != 0) {
        wire.sources = &.{};
    };
    const bytes = try std.json.Stringify.valueAlloc(a, wire, .{});
    defer a.free(bytes);
    if (bytes.len > corpus.max_root_bytes) return error.NativeLakeTextCorpusTooLarge;
    var writer = store.*;
    writer.allocator = out;
    const ref = try writer.putWithCancellation(bytes, token);
    defer if (ref.provider_version) |version| writer.allocator.free(version);
    return .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
}
pub const Delta = struct { removed: []const corpus.text_directory.FileRecord, added: []const corpus.text_directory.FileRecord };
pub fn publish(a: A, out: A, store: *stores.ArtifactStore, current: corpus.Root, previous: ?corpus.Root, plan: *const state.Plan, context: local.serverless_query_lake_read_context.Context, token: Cancellation) !Chunk {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const base: ?corpus.Root = if (previous) |old| if (old.checkpoint_base != null) try corpus.checkpointBase(ca, store.*, old, token) else old else null;
    const removed = if (base) |old| try corpus.text_directory.fileRecords(ca, old, store.*, null, token) else &.{};
    const files = try corpus.text_directory.fileRecords(ca, current, store.*, null, token);
    var added: std.ArrayList(corpus.text_directory.FileRecord) = .empty;
    var previous_files: std.StringHashMapUnmanaged(state.File) = .empty;
    defer previous_files.deinit(a);
    for (removed) |record| try previous_files.put(a, record.file.id, record.file);
    for (files) |record| {
        if (previous_files.get(record.file.id)) |old| if (std.mem.eql(u8, &old.digest, &record.file.digest)) continue;
        try added.append(ca, record);
    }
    return publishDelta(a, out, store, current, base, .{ .removed = removed, .added = added.items }, plan, context, token);
}
pub fn publishDelta(a: A, out: A, store: *stores.ArtifactStore, current: corpus.Root, previous: ?corpus.Root, delta: Delta, plan: *const state.Plan, context: local.serverless_query_lake_read_context.Context, token: Cancellation) !Chunk {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var reads: u64 = 512 * 1024 * 1024;
    var writes: u64 = 512 * 1024 * 1024;
    const scope = store.upload_scope.?;
    var pages: pages_mod.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    var cancellation = token;
    const Check = struct {
        fn call(raw: *anyopaque) !void {
            try @as(*Cancellation, @ptrCast(@alignCast(raw))).check();
        }
    };
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = context.io orelse return error.UnsupportedSqlExecution, .context = &cancellation, .checkpoint = Check.call };
    defer manager.deinit();
    var terms = corpus.term_directory.Builder.init(a, &manager);
    defer terms.deinit();
    terms.enableDurable(pages.store());
    var ownership = corpus.text_directory.Mutations.init(a, &manager, pages.store());
    defer ownership.deinit();
    var source_routes = corpus.text_directory.Mutations.init(a, &manager, pages.store());
    defer source_routes.deinit();
    var builder: Builder = .{ .a = a, .out = ca, .store = store, .context = context, .token = token, .pages = &pages, .terms = &terms, .ownership = &ownership, .source_routes = &source_routes, .pending_arena = .{ .init(a), .init(a), .init(a), .init(a) } };
    defer for (&builder.pending_arena) |*pending| pending.deinit();
    var prior: ?corpus.Root = null;
    if (previous) |old| if (old.serving != null) {
        prior = try corpus.servingRoot(ca, store.*, old, token, null);
    };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    if (prior) |old| {
        if (old.source_routes_version == 0) {
            for (try corpus.text_directory.fileRecords(ca, old, store.*, null, token)) |record| {
                _ = scratch.reset(.free_all);
                const group = try corpus.loadFileManifest(scratch.allocator(), store.*, record.manifest, old.domain, token, null);
                try @import("lake_index_source_routes.zig").mutate(a, &source_routes, group, false);
            }
        }
        builder.pack_count = old.segment_count;
        builder.serving_bytes = old.serving_bytes;
        // Paged ownership lookup touches only changed/removed source prefixes.
        var affected: std.StringHashMapUnmanaged(Chunk) = .empty;
        defer affected.deinit(a);
        for (delta.removed) |record| {
            if (plan.unchanged(record.file)) continue;
            const prefix = corpus.term_directory.identity(record.file.id);
            var cursor = try tree.Cursor.init(a, pages.store(), old.pack_ownership, &prefix, null);
            defer cursor.deinit();
            while (try cursor.next()) |entry| {
                if (entry.key.len != 68 or !std.mem.eql(u8, entry.key[0..32], &prefix)) break;
                if (std.mem.readInt(u32, entry.key[64..68], .big) != 0) return error.InvalidNativeLakeTextCorpus;
                const ref = try std.json.parseFromSliceLeaky(Chunk, ca, entry.value, .{ .allocate = .alloc_always });
                try affected.put(a, ref.artifact_id, ref);
            }
        }
        for (old.open_packs) |ref| try affected.put(a, ref.artifact_id, ref);
        var it = affected.valueIterator();
        while (it.next()) |ref| {
            try builder.check();
            _ = scratch.reset(.free_all);
            const sa = scratch.allocator();
            const group = try corpus.loadFileManifest(sa, store.*, ref.*, current.domain, token, null);
            if (group.segments.len != 1 or group.sources.len == 0) return error.InvalidNativeLakeTextCorpus;
            var sources: std.ArrayList(Span) = .empty;
            for (group.sources) |span| if (plan.by_id.get(span.file.id)) |ordinal| {
                if (!std.mem.eql(u8, &span.file.digest, &plan.files[ordinal].digest)) continue;
                try sources.append(sa, span);
            };
            try builder.removed.append(ca, ref.*);
            try builder.contribution(group, -1);
            try builder.owner(group, ref.*, true);
            builder.pack_count = try std.math.sub(u32, builder.pack_count, 1);
            builder.serving_bytes = try std.math.sub(u64, builder.serving_bytes, group.admissions[0].bytes);
            if (sources.items.len != 0) try builder.add(.{ .ref = group.segments[0], .admission = group.admissions[0], .sources = sources.items, .original_docs = group.admissions[0].doc_count, .summary = group.summaries[0] });
        }
    }
    for (delta.added) |record| {
        try builder.check();
        _ = scratch.reset(.free_all);
        const sa = scratch.allocator();
        const group = try corpus.loadFileManifest(sa, store.*, record.manifest, current.domain, token, null);
        const physical = group.physical_manifest orelse return error.InvalidNativeLakeTextCorpus;
        const rows: u32 = @intCast(try corpus.physical.validate(group.rows));
        var lower: u32 = 0;
        for (group.segments, group.admissions, group.summaries) |segment, admission, summary| {
            const span: Span = .{ .file = group.file, .physical = physical, .file_rows = rows, .file_lower = lower, .pack_lower = 0, .count = admission.doc_count };
            try builder.add(.{ .ref = segment, .admission = admission, .sources = &.{span}, .original_docs = admission.doc_count, .summary = summary });
            lower = try std.math.add(u32, lower, admission.doc_count);
        }
        if (lower != rows) return error.InvalidNativeLakeTextCorpus;
    }
    for (0..4) |level| try builder.flush(level);
    var root = current;
    root.serving = null;
    root.directories = try corpus.text_directory.publishManifests(a, store, builder.added.items, builder.removed.items, if (prior) |old| old.directories else .{}, current.domain, token, context.io.?);
    root.term_directory = try terms.finish(pages.store(), if (prior) |old| old.term_directory else .{});
    root.source_layout_version = 1;
    root.source_routes_version = 1;
    root.source_routes = try source_routes.finish(if (prior) |old| old.source_routes else null);
    root.pack_ownership = try ownership.finish(if (prior) |old| old.pack_ownership else null);
    root.open_packs = builder.open_packs.items;
    root.file_count = builder.pack_count;
    root.segment_count = builder.pack_count;
    root.serving_bytes = builder.serving_bytes;
    try root.validate();
    return put(a, out, store, root, token);
}

test "external lake global frontier cross file packs preserve coordinates ties restart and incremental ownership" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("cross-file-serving-packs");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@import("lake_index_publication.zig").uploadDomain(7, @splat(5)), 1, std.testing.io);
    const Helper = struct {
        fn file(a_: A, store_: *stores.ArtifactStore, index: usize, digest: u8) !Chunk {
            const id = try std.fmt.allocPrint(a_, "source-{d}", .{index});
            const bytes = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a_, &.{
                .{ .key = try std.fmt.allocPrint(a_, "{s}-a", .{id}), .value = "{\"body\":\"quasar\"}" },
                .{ .key = try std.fmt.allocPrint(a_, "{s}-b", .{id}), .value = "{\"body\":\"quasar\"}" },
            }, .{}, null)).?;
            defer a_.free(bytes);
            const ref = try @import("lake_index_seekable_text.zig").publish(a_, a_, store_, bytes, .none);
            var reader = try local.segment.SegmentReader.init(a_, bytes);
            defer reader.deinit();
            var group: corpus.FileGroup = .{ .file = .{ .id = id, .digest = @splat(digest) }, .segments = &.{ref}, .admissions = &.{try reader.admission(a_)}, .summaries = &.{try corpus.statistics.publishSegmentWithIo(a_, store_, bytes, .none, std.testing.io)}, .rows = &.{.{ .group = 0, .high = 0, .base = 0, .lower = 10, .count = 2 }} };
            try corpus.text_directory.prepare(a_, store_, &group, .none);
            return put(a_, a_, store_, group, .none);
        }
        fn root(a_: A, store_: *stores.ArtifactStore, refs: []const Chunk) !corpus.Root {
            return .{ .domain = store_.upload_scope.?.domain, .binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} }, .config_json = "{}", .statistics_version = 2, .seekable = true, .inventory_version = 3, .file_count = @intCast(refs.len), .segment_count = @intCast(refs.len), .collection = .{ .documents = @intCast(refs.len * 2), .fields = try a_.dupe(corpus.Collection.Field, &.{.{ .name = "body", .total_length = refs.len * 2 }}) }, .directories = try corpus.text_directory.publishManifests(a_, store_, refs, &.{}, .{}, store_.upload_scope.?.domain, .none, std.testing.io) };
        }
        fn plan(a_: A, root_: corpus.Root, store_: stores.ArtifactStore, changed: ?[]const u8) !state.Plan {
            const files = try corpus.text_directory.fileRecords(a_, root_, store_, null, .none);
            var result: state.Plan = .{ .a = a_, .files = try a_.alloc(state.File, files.len), .changed = try a_.alloc(bool, files.len) };
            for (files, result.files, result.changed, 0..) |record, *file_, *change, i| {
                file_.* = record.file;
                change.* = if (changed) |id| std.mem.eql(u8, id, file_.id) else true;
                try result.by_id.put(a_, file_.id, i);
            }
            return result;
        }
        fn reopen(a_: A, store_: *stores.ArtifactStore, root_: corpus.Root) !corpus.Root {
            const ref = try put(a_, a_, store_, root_, .none);
            return corpus.loadServing(a_, store_.*, .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, .none, null);
        }
        fn verify(a_: A, store_: stores.ArtifactStore, root_: corpus.Root, expected: usize) !void {
            var writer = try corpus.loadWriter(a_, store_, root_, .none, null, null);
            defer writer.deinit();
            const snapshot = writer.snapshot();
            const identities = try @import("lake_index_text_predicate.zig").Identities.init(a_, root_, snapshot);
            try std.testing.expect(identities.tie_rank != null);
            var cache = local.serverless_query_lake_serving_cache.Cache.init(a_);
            defer cache.deinit();
            const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = .{ .io = std.testing.io } };
            var route_store = store_;
            var route_reads: u64 = 16 * 1024 * 1024;
            var route_writes: u64 = 0;
            var route_pages: pages_mod.PageStore = .{ .domain = root_.domain, .artifacts = &route_store, .remaining_read_bytes = &route_reads, .remaining_write_bytes = &route_writes };
            var route_cache: tree.Cache = .{ .alloc = a_, .underlying = route_pages.store() };
            defer route_cache.deinit();
            var physical: @import("lake_index_text_predicate.zig").LiveRowsCache = .{};
            defer physical.deinit(a_);
            for (0..expected) |i| {
                const row = try identities.physicalRow(a_, &physical, store_, cached, @intCast(i), "source", "snapshot");
                try std.testing.expect(row.external.row_ordinal == 10 or row.external.row_ordinal == 11);
                const route = (try @import("lake_index_source_routes.zig").lookup(a_, route_cache.store(), root_, store_, cached, .none, &physical, row)).?;
                var ordinal: usize = 0;
                while (identities.offsets[ordinal + 1] <= i) ordinal += 1;
                try std.testing.expectEqual(corpus.term_directory.identity(root_.file_groups[ordinal].segments[0].artifact_id), route);
                try std.testing.expectEqual(@as(?u32, @intCast(i)), try identities.ordinal(a_, &physical, store_, cached, row));
            }
            var actual = try local.search_search.execute(a_, snapshot, .{ .query = .{ .term = .{ .field = "body", .term = "quasar" } }, .k = 3, .include_stored = false, .tie_rank = identities.tie_rank });
            defer actual.deinit();
            try std.testing.expectEqual(@as(usize, 3), actual.hits.len);
            var ranked = try a_.alloc(local.search_scorer.ScoredHit, expected);
            defer a_.free(ranked);
            for (ranked, 0..) |*hit, i| hit.* = .{ .doc_id = @intCast(i), .score = actual.hits[0].score };
            local.search_scorer.sortScoredHitsWithRank(ranked, identities.tie_rank);
            for (actual.hits, ranked[0..3]) |hit, expected_hit| try std.testing.expectEqual(expected_hit.doc_id, hit.doc_id);
            var next = try local.search_search.execute(a_, snapshot, .{ .query = .{ .term = .{ .field = "body", .term = "quasar" } }, .k = 3, .include_stored = false, .tie_rank = identities.tie_rank, .search_after = .{ .doc_id = actual.hits[2].doc_id, .score = actual.hits[2].score } });
            defer next.deinit();
            for (next.hits, ranked[3..6]) |hit, expected_hit| try std.testing.expectEqual(expected_hit.doc_id, hit.doc_id);
        }
    };
    const refs = try ca.alloc(Chunk, 129);
    for (refs[0..128], 0..) |*ref, i| ref.* = try Helper.file(ca, &store, i, @intCast(i + 1));
    var current = try Helper.root(ca, &store, refs[0..128]);
    var first_plan = try Helper.plan(ca, current, store, null);
    defer first_plan.deinit();
    current.serving = try publish(a, ca, &store, current, null, &first_plan, .{ .io = std.testing.io }, .none);
    const first = try corpus.servingRoot(ca, store, current, .none, null);
    try std.testing.expectEqual(@as(u32, 1), first.segment_count);
    try std.testing.expectEqual(@as(usize, 1), first.open_packs.len);
    const first_packs = try corpus.text_directory.fileRecords(ca, first, store, null, .none);
    for (first_packs) |pack| {
        const group = try corpus.loadFileManifest(ca, store, pack.manifest, current.domain, .none, null);
        try std.testing.expectEqual(@as(usize, 128), group.sources.len);
        try std.testing.expectEqual(@as(usize, 2), group.source_pages.len);
    }
    try Helper.verify(ca, store, try Helper.reopen(ca, &store, current), 256);
    // A serving publication retains canonical maintenance metadata, without
    // retaining a second full copy of every original posting payload.
    const root_ref = try put(ca, ca, &store, current, .none);
    var collector: @import("lake_index_gc.zig").Collector = .{ .a = a, .table = 7, .authority = undefined, .store = store, .identity = @splat(5), .context = .{ .io = std.testing.io }, .token = @splat(1), .audit_only = true, .remaining_reads = 64 * 1024 * 1024 };
    defer {
        var keys = collector.marked.keyIterator();
        while (keys.next()) |key_| a.free(key_.*);
        collector.marked.deinit(a);
    }
    try collector.markArtifact(ca, .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = root_ref.artifact_id, .checksum = root_ref.checksum, .byte_len = root_ref.byte_len });
    for (first_packs) |pack| {
        const retained_pack = try corpus.loadFileManifest(ca, store, pack.manifest, current.domain, .none, null);
        for (retained_pack.source_pages) |page| try std.testing.expect(collector.marked.contains(page.artifact_id));
    }
    for (refs[0..128]) |ref| {
        const group = try corpus.loadFileManifest(ca, store, ref, current.domain, .none, null);
        try std.testing.expect(collector.marked.contains(group.physical_manifest.?.artifact_id));
        for (group.segments) |segment| {
            try std.testing.expect(!collector.marked.contains(segment.artifact_id));
            try store.delete(segment.artifact_id);
        }
    }
    try Helper.verify(ca, store, try Helper.reopen(ca, &store, current), 256);
    refs[128] = try Helper.file(ca, &store, 128, 129);
    const checkpoint: corpus.Root = .{ .checkpoint_base = root_ref, .statistics_version = 1, .seekable = true, .domain = current.domain, .binding = current.binding, .config_json = current.config_json, .recipe = current.recipe, .manifests = refs[128..129] };
    const checkpoint_ref = try put(ca, ca, &store, checkpoint, .none);
    const recovered = try corpus.loadRootDirectory(ca, store, .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = checkpoint_ref.artifact_id, .checksum = checkpoint_ref.checksum, .byte_len = checkpoint_ref.byte_len }, .none, null);
    try collector.markArtifact(ca, .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = checkpoint_ref.artifact_id, .checksum = checkpoint_ref.checksum, .byte_len = checkpoint_ref.byte_len });
    try std.testing.expect(collector.marked.contains(root_ref.artifact_id));
    var appended = try Helper.root(ca, &store, refs);
    var append_plan = try Helper.plan(ca, appended, store, "source-128");
    defer append_plan.deinit();
    // The appended file is already complete in the recovered checkpoint.
    @memset(append_plan.changed, false);
    appended.serving = try publish(a, ca, &store, appended, recovered, &append_plan, .{ .io = std.testing.io }, .none);
    // Delta publication must not reopen either canonical file inventory.
    // Make those inventories unavailable while retaining serving ownership.
    var delta_base = current;
    delta_base.directories.files = null;
    var delta_current = appended;
    delta_current.directories.files = null;
    const added: corpus.text_directory.FileRecord = .{ .file = append_plan.files[128], .manifest = refs[128] };
    appended.serving = try publishDelta(a, ca, &store, delta_current, delta_base, .{ .removed = &.{}, .added = &.{added} }, &append_plan, .{ .io = std.testing.io }, .none);
    const appended_root = try corpus.servingRoot(ca, store, appended, .none, null);
    try std.testing.expectEqual(@as(u32, 2), appended_root.segment_count);
    const appended_packs = try corpus.text_directory.fileRecords(ca, appended_root, store, null, .none);
    for (first_packs) |pack| {
        var found = false;
        for (appended_packs) |present| {
            if (std.mem.eql(u8, pack.manifest.artifact_id, present.manifest.artifact_id)) found = true;
        }
        try std.testing.expect(found);
    }
    refs[0] = try Helper.file(ca, &store, 0, 99);
    var replaced = try Helper.root(ca, &store, refs);
    var replace_plan = try Helper.plan(ca, replaced, store, "source-0");
    defer replace_plan.deinit();
    replaced.serving = try publish(a, ca, &store, replaced, appended, &replace_plan, .{ .io = std.testing.io }, .none);
    try Helper.verify(ca, store, try Helper.reopen(ca, &store, replaced), 258);
    // The old publication still resolves the replaced source's original rows.
    try Helper.verify(ca, store, try Helper.reopen(ca, &store, current), 256);
    var removed = try Helper.root(ca, &store, refs[1..]);
    var remove_plan = try Helper.plan(ca, removed, store, "missing");
    defer remove_plan.deinit();
    removed.serving = try publish(a, ca, &store, removed, replaced, &remove_plan, .{ .io = std.testing.io }, .none);
    try Helper.verify(ca, store, try Helper.reopen(ca, &store, removed), 256);
}
