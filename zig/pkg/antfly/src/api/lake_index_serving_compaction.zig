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
    open_packs: std.ArrayList(Chunk) = .empty,
    added: std.ArrayList(Chunk) = .empty,
    removed: std.ArrayList(Chunk) = .empty,
    pending: [8]Input = undefined,
    count: usize = 0,
    bytes: u64 = 0,
    spans: usize = 0,
    pending_arena: std.heap.ArenaAllocator,
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
        const a = self.pending_arena.child_allocator;
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
        try self.check();
        if (self.count != 0 and (self.bytes + input.admission.bytes > 16 * 1024 * 1024 or self.spans + input.sources.len > 64)) try self.flush();
        const a = self.pending_arena.allocator();
        const bytes = try std.json.Stringify.valueAlloc(a, input, .{});
        self.pending[self.count] = try std.json.parseFromSliceLeaky(Input, a, bytes, .{ .allocate = .alloc_always });
        self.count += 1;
        self.bytes += input.admission.bytes;
        self.spans += input.sources.len;
        if (self.count == 8) try self.flush();
    }
    fn flush(self: *@This()) !void {
        if (self.count == 0) return;
        try self.emit(self.pending[0..self.count]);
        self.count = 0;
        self.bytes = 0;
        self.spans = 0;
        _ = self.pending_arena.reset(.free_all);
    }
    fn emit(self: *@This(), inputs: []const Input) anyerror!void {
        try self.check();
        var memory: local.sql_memory_budget = .{ .backing = self.a, .limit = 64 * 1024 * 1024 };
        const a = memory.allocator();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
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
            group = .{ .file = undefined, .sources = sources.items, .segments = &.{input.ref}, .admissions = &.{input.admission}, .summaries = &.{input.summary} };
        } else {
            const encoded = self.merge(a, inputs) catch |err| switch (err) {
                error.OutOfMemory => {
                    if (inputs.len == 1) return err;
                    const mid = inputs.len / 2;
                    try self.emit(inputs[0..mid]);
                    try self.emit(inputs[mid..]);
                    return;
                },
                else => return err,
            };
            defer a.free(encoded);
            if (encoded.len > 32 * 1024 * 1024) {
                if (inputs.len == 1) return error.NativeLakeTextCorpusTooLarge;
                const mid = inputs.len / 2;
                try self.emit(inputs[0..mid]);
                try self.emit(inputs[mid..]);
                return;
            }
            const ref = try @import("lake_index_seekable_text.zig").publish(a, out, self.store, encoded, self.token);
            var reader = try local.segment.SegmentReader.init(a, encoded);
            defer reader.deinit();
            const admission = try reader.admission(out);
            if (admission.doc_count != count) return error.InvalidNativeLakeTextCorpus;
            group = .{ .file = undefined, .sources = sources.items, .segments = &.{ref}, .admissions = &.{admission}, .summaries = &.{try corpus.statistics.publishSegmentWithIo(a, self.store, encoded, self.token, self.context.io)} };
        }
        const digest = corpus.term_directory.identity(group.segments[0].artifact_id);
        const id = try std.fmt.allocPrint(out, "pack-{x}", .{&digest});
        group.file = .{ .id = id, .digest = digest };
        try corpus.text_directory.prepare(out, self.store, &group, self.token);
        const manifest = try put(out, self.out, self.store, group, self.token);
        try self.added.append(self.out, manifest);
        try self.owner(group, manifest, false);
        if (group.sources.len < 8 and group.admissions[0].bytes < 2 * 1024 * 1024 and self.open_packs.items.len < 7) try self.open_packs.append(self.out, manifest);
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
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    if (bytes.len > corpus.max_root_bytes) return error.NativeLakeTextCorpusTooLarge;
    var writer = store.*;
    writer.allocator = out;
    const ref = try writer.putWithCancellation(bytes, token);
    return .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
}
pub fn publish(a: A, out: A, store: *stores.ArtifactStore, current: corpus.Root, previous: ?corpus.Root, plan: *const state.Plan, context: local.serverless_query_lake_read_context.Context, token: Cancellation) !Chunk {
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
    var builder: Builder = .{ .a = a, .out = ca, .store = store, .context = context, .token = token, .pages = &pages, .terms = &terms, .ownership = &ownership, .pending_arena = .init(a) };
    defer builder.pending_arena.deinit();
    var prior: ?corpus.Root = null;
    if (previous) |old| if (old.serving != null) {
        prior = try corpus.servingRoot(ca, store.*, old, token, null);
    };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    if (prior) |old| {
        builder.pack_count = old.segment_count;
        builder.serving_bytes = old.serving_bytes;
        // Paged ownership lookup touches only changed/removed source prefixes.
        var affected: std.StringHashMapUnmanaged(Chunk) = .empty;
        defer affected.deinit(a);
        const original = try corpus.text_directory.fileRecords(ca, previous.?, store.*, null, token);
        for (original) |record| {
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
            for (group.sources) |span| if (plan.unchanged(span.file)) {
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
    const files = try corpus.text_directory.fileRecords(ca, current, store.*, null, token);
    for (files) |record| {
        if (prior != null and plan.unchanged(record.file)) continue;
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
    try builder.flush();
    var root = current;
    root.serving = null;
    root.directories = try corpus.text_directory.publishManifests(a, store, builder.added.items, builder.removed.items, if (prior) |old| old.directories else .{}, current.domain, token, context.io.?);
    root.term_directory = try terms.finish(pages.store(), if (prior) |old| old.term_directory else .{});
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
    store.upload_scope = try stores.UploadScope.forPublication(@splat(5), 1, std.testing.io);
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
            return .{ .domain = store_.upload_scope.?.domain, .binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} }, .config_json = "{}", .seekable = true, .inventory_version = 3, .file_count = @intCast(refs.len), .segment_count = @intCast(refs.len), .collection = .{ .documents = @intCast(refs.len * 2), .fields = try a_.dupe(corpus.Collection.Field, &.{.{ .name = "body", .total_length = refs.len * 2 }}) }, .directories = try corpus.text_directory.publishManifests(a_, store_, refs, &.{}, .{}, store_.upload_scope.?.domain, .none, std.testing.io) };
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
            return corpus.loadRoot(a_, store_.*, .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, .none, null);
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
            var physical: @import("lake_index_text_predicate.zig").LiveRowsCache = .{};
            defer physical.deinit(a_);
            for (0..expected) |i| {
                const row = try identities.physicalRow(a_, &physical, store_, cached, @intCast(i), "source", "snapshot");
                try std.testing.expect(row.external.row_ordinal == 10 or row.external.row_ordinal == 11);
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
    const refs = try ca.alloc(Chunk, 17);
    for (refs[0..16], 0..) |*ref, i| ref.* = try Helper.file(ca, &store, i, @intCast(i + 1));
    var current = try Helper.root(ca, &store, refs[0..16]);
    var first_plan = try Helper.plan(ca, current, store, null);
    defer first_plan.deinit();
    current.serving = try publish(a, ca, &store, current, null, &first_plan, .{ .io = std.testing.io }, .none);
    const first = try corpus.servingRoot(ca, store, current, .none, null);
    try std.testing.expectEqual(@as(u32, 2), first.segment_count);
    const first_packs = try corpus.text_directory.fileRecords(ca, first, store, null, .none);
    try Helper.verify(ca, store, try Helper.reopen(ca, &store, current), 32);
    refs[16] = try Helper.file(ca, &store, 16, 17);
    var appended = try Helper.root(ca, &store, refs);
    var append_plan = try Helper.plan(ca, appended, store, "source-16");
    defer append_plan.deinit();
    appended.serving = try publish(a, ca, &store, appended, current, &append_plan, .{ .io = std.testing.io }, .none);
    const appended_root = try corpus.servingRoot(ca, store, appended, .none, null);
    try std.testing.expectEqual(@as(u32, 3), appended_root.segment_count);
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
    try Helper.verify(ca, store, try Helper.reopen(ca, &store, replaced), 34);
    // The old publication still resolves the replaced source's original rows.
    try Helper.verify(ca, store, try Helper.reopen(ca, &store, current), 32);
    var removed = try Helper.root(ca, &store, refs[1..]);
    var remove_plan = try Helper.plan(ca, removed, store, "missing");
    defer remove_plan.deinit();
    removed.serving = try publish(a, ca, &store, removed, replaced, &remove_plan, .{ .io = std.testing.io }, .none);
    try Helper.verify(ca, store, try Helper.reopen(ca, &store, removed), 32);
}
