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
const LocalSegment = @import("lake_index_segment_spill.zig").LocalSegment;

const Input = struct {
    ref: ?Chunk,
    staged: ?*LocalSegment = null,
    admission: local.segment.SegmentReader.Admission,
    sources: []const Span,
    original_docs: u32,
    summary: ?corpus.statistics.Ref,
    const Wire = struct { ref: ?Chunk, admission: local.segment.SegmentReader.Admission, sources: []const Span, original_docs: u32, summary: ?corpus.statistics.Ref };
    fn copy(self: Input, a: A) !Input {
        const bytes = try std.json.Stringify.valueAlloc(a, Wire{ .ref = self.ref, .admission = self.admission, .sources = self.sources, .original_docs = self.original_docs, .summary = self.summary }, .{});
        const wire = try std.json.parseFromSliceLeaky(Wire, a, bytes, .{ .allocate = .alloc_always });
        return .{ .ref = wire.ref, .staged = self.staged, .admission = wire.admission, .sources = wire.sources, .original_docs = wire.original_docs, .summary = wire.summary };
    }
};
const Builder = struct {
    a: A,
    out: A,
    store: *stores.ArtifactStore,
    context: local.serverless_query_lake_read_context.Context,
    token: Cancellation,
    pages: *pages_mod.PageStore,
    terms: *corpus.term_directory.Builder,
    collect_term_deltas: bool = true,
    ownership: *corpus.text_directory.Mutations,
    source_routes: *corpus.text_directory.Mutations,
    ties: *corpus.text_directory.Mutations,
    metadata: *corpus.text_directory.Publisher,
    staging: *local.sql_spill.Manager,
    open_packs: std.ArrayList(Chunk) = .empty,
    open_tails: std.ArrayList(corpus.Root.Tail) = .empty,
    candidates: [4]std.ArrayList(corpus.Root.Tail) = @splat(.empty),
    activated: [4]bool = @splat(false),
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
    serving_metadata_bytes: ?u64 = 0,
    fn check(self: *@This()) !void {
        try self.context.ensureActive();
        try self.token.check();
    }
    fn contribution(self: *@This(), group: corpus.FileGroup, sign: i64) !void {
        if (!self.collect_term_deltas) return;
        for (group.summaries, group.segments) |summary, segment| try self.terms.add(self.pages.store(), summary, corpus.term_directory.identity(segment.artifact_id), sign);
    }
    fn owner(self: *@This(), group: corpus.FileGroup, manifest: Chunk, deleting: bool) !void {
        try self.metadata.add(group, manifest, deleting);
        try @import("lake_index_source_routes.zig").mutate(self.a, self.source_routes, group, deleting);
        try @import("lake_index_text_ties.zig").mutate(self.a, self.ties, group, deleting);
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
    fn activate(self: *@This(), level: usize) anyerror!void {
        if (self.activated[level]) return;
        self.activated[level] = true;
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        for (self.candidates[level].items) |tail| {
            try self.check();
            _ = arena.reset(.free_all);
            const group = try corpus.loadFileManifestPublication(arena.allocator(), self.store.*, tail.ref, self.pages.domain, self.token);
            if (group.segments.len != 1 or group.sources.len != tail.spans or group.admissions[0].bytes != tail.bytes) return error.InvalidNativeLakeTextCorpus;
            try self.contribution(group, -1);
            try self.owner(group, tail.ref, true);
            self.pack_count = try std.math.sub(u32, self.pack_count, 1);
            self.serving_bytes = try std.math.sub(u64, self.serving_bytes, tail.bytes);
            if (self.serving_metadata_bytes) |bytes| self.serving_metadata_bytes = try std.math.sub(u64, bytes, metadataBytes(group, tail.ref));
            try self.addAt(level, .{ .ref = group.segments[0], .admission = group.admissions[0], .sources = group.sources, .original_docs = group.admissions[0].doc_count, .summary = group.summaries[0] });
        }
    }
    fn addAt(self: *@This(), level: usize, input: Input) anyerror!void {
        try self.check();
        try self.activate(level);
        var metadata: usize = 0;
        for (input.sources) |span| metadata += span.file.id.len + 512;
        if (self.count[level] != 0 and (self.bytes[level] + input.admission.bytes > 16 * 1024 * 1024 or self.spans[level] + input.sources.len > corpus.source_spans.max_spans or self.metadata_bytes[level] + metadata > 1024 * 1024)) try self.flush(level);
        const a = self.pending_arena[level].allocator();
        self.pending[level][self.count[level]] = try input.copy(a);
        if (input.staged) |staged| staged.retain();
        self.count[level] += 1;
        self.bytes[level] += input.admission.bytes;
        self.spans[level] += input.sources.len;
        self.metadata_bytes[level] += metadata;
        if (self.count[level] == 8 or self.spans[level] == corpus.source_spans.max_spans) try self.flush(level);
    }
    fn flush(self: *@This(), level: usize) anyerror!void {
        if (self.count[level] == 0) return;
        try self.emit(self.pending[level][0..self.count[level]], level);
        for (self.pending[level][0..self.count[level]]) |input| if (input.staged) |staged| staged.release();
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
        if (reuse and inputs[0].staged == null) {
            const input = inputs[0];
            group = .{ .file = undefined, .sources = sources.items, .segments = try out.dupe(Chunk, &.{input.ref.?}), .admissions = try out.dupe(local.segment.SegmentReader.Admission, &.{input.admission}), .summaries = try out.dupe(?corpus.statistics.Ref, &.{input.summary}) };
        } else {
            const staged = (if (reuse and inputs[0].staged != null) blk: {
                const retained = inputs[0].staged.?;
                retained.retain();
                break :blk retained;
            } else self.merge(a, inputs)) catch |err| switch (err) {
                error.OutOfMemory, error.NativeLakeTextCorpusTooLarge => {
                    if (inputs.len == 1) return err;
                    arena.deinit();
                    arena_live = false;
                    const mid = inputs.len / 2;
                    try self.emit(inputs[0..mid], level);
                    try self.emit(inputs[mid..], level);
                    return;
                },
                else => return err,
            };
            defer staged.release();
            var reader = try local.segment.SegmentReader.initSource(a, staged.source());
            var reader_live = true;
            defer if (reader_live) reader.deinit();
            const admission = try reader.admission(out);
            if (admission.doc_count != count) return error.InvalidNativeLakeTextCorpus;
            if (level < 3 and sources.items.len >= std.math.pow(usize, 8, level + 1) and sources.items.len < corpus.source_spans.max_spans and staged.file.size < 2 * 1024 * 1024) {
                // Intermediate levels stay private and local. Only final packs
                // and their statistics become durable publication artifacts.
                var carry = std.heap.ArenaAllocator.init(self.a);
                defer carry.deinit();
                const input = try (Input{ .ref = null, .staged = staged, .admission = admission, .sources = sources.items, .original_docs = count, .summary = null }).copy(carry.allocator());
                // Drop this merge's output and reader before the next tier can
                // open its own workspace. The staged file owns the payload.
                reader.deinit();
                reader_live = false;
                arena.deinit();
                arena_live = false;
                try self.addAt(level + 1, input);
                return;
            }
            const ref = try @import("lake_index_seekable_text.zig").publishSource(a, out, self.store, staged.source(), self.token);
            group = .{ .file = undefined, .sources = sources.items, .segments = try out.dupe(Chunk, &.{ref}), .admissions = try out.dupe(local.segment.SegmentReader.Admission, &.{admission}), .summaries = try out.dupe(?corpus.statistics.Ref, &.{try corpus.statistics.publishReaderWithIo(a, self.store, &reader, self.token, self.context.io)}) };
        }
        if (level < 3 and group.sources.len >= std.math.pow(usize, 8, level + 1) and group.sources.len < corpus.source_spans.max_spans and group.admissions[0].bytes < 2 * 1024 * 1024) {
            // Only final packs own directory/ownership/statistics entries.
            // Reused remote inputs already have a durable identity.
            var carry = std.heap.ArenaAllocator.init(self.a);
            defer carry.deinit();
            const input = try (Input{ .ref = group.segments[0], .admission = group.admissions[0], .sources = group.sources, .original_docs = count, .summary = group.summaries[0] }).copy(carry.allocator());
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
        const manifest = try put(out, out, self.store, group, self.token);
        try self.owner(group, manifest, false);
        var tier: usize = 0;
        var threshold: usize = 8;
        while (tier < 3 and group.sources.len >= threshold) : (tier += 1) threshold *= 8;
        var source_metadata: usize = 0;
        for (group.sources) |span| source_metadata += span.file.id.len + 512;
        if (group.sources.len < corpus.source_spans.max_spans and source_metadata < 512 * 1024 and group.admissions[0].bytes < 2 * 1024 * 1024 and self.open_counts[tier] < 7) {
            // Only the bounded open-tail inventory retains this reference;
            // directory/ownership reducers have already copied their records.
            const retained: Chunk = .{ .artifact_id = try self.out.dupe(u8, manifest.artifact_id), .checksum = try self.out.dupe(u8, manifest.checksum), .byte_len = manifest.byte_len };
            try self.open_packs.append(self.out, retained);
            try self.open_tails.append(self.out, .{ .ref = retained, .tier = @intCast(tier), .bytes = group.admissions[0].bytes, .spans = @intCast(group.sources.len) });
            self.open_counts[tier] += 1;
        }
        try self.contribution(group, 1);
        self.pack_count = try std.math.add(u32, self.pack_count, 1);
        self.serving_bytes = try std.math.add(u64, self.serving_bytes, group.admissions[0].bytes);
        if (self.serving_metadata_bytes) |bytes| self.serving_metadata_bytes = try std.math.add(u64, bytes, metadataBytes(group, manifest));
    }
    fn merge(self: *@This(), a: A, inputs: []const Input) !*LocalSegment {
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
            data[i] = if (input.staged) |staged| .{ .native = staged.source() } else try @import("lake_index_seekable_text.zig").loadPublication(a, .{ .store = self.store.*, .cache = null, .context = self.context, .cancellation = self.token }, input.ref.?);
            readers[i] = local.segment.SegmentReader.initAuthenticated(a, data[i].source(), input.admission) catch |err| {
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
        const staged = try LocalSegment.empty(self.a, self.staging, self.store);
        errdefer staged.release();
        var sink = staged.sink();
        try local.segment.writeMergedSegmentToSink(a, &sink, merged[0..inputs.len]);
        try staged.seal();
        return staged;
    }
};
fn metadataBytes(group: corpus.FileGroup, manifest: Chunk) u64 {
    var bytes = corpus.source_spans.metadataBytes(group.sources, group.source_pages) +| manifest.byte_len;
    for (group.descriptors) |descriptor| bytes +|= descriptor.byte_len;
    return bytes;
}
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
pub const Delta = struct { removed: []const corpus.text_directory.FileRecord, added: []const corpus.text_directory.FileRecord, replay: ?*corpus.ManifestReplay = null };
pub fn publish(a: A, out: A, store: *stores.ArtifactStore, current: corpus.Root, previous: ?corpus.Root, plan: *const state.Plan, context: local.serverless_query_lake_read_context.Context, token: Cancellation) !Chunk {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const base: ?corpus.Root = if (previous) |old| if (old.checkpoint_base != null) try corpus.checkpointBasePublication(ca, store.*, old, token) else old else null;
    const removed = if (base) |old| try corpus.text_directory.fileRecordsPublication(ca, old, store.*, token) else &.{};
    const files = try corpus.text_directory.fileRecordsPublication(ca, current, store.*, token);
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
    pages.admitPublication();
    var cancellation = token;
    const Check = struct {
        fn call(raw: *anyopaque) !void {
            try @as(*Cancellation, @ptrCast(@alignCast(raw))).check();
        }
    };
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = context.io orelse return error.UnsupportedSqlExecution, .context = &cancellation, .checkpoint = Check.call };
    defer manager.deinit();
    var staging: local.sql_spill.Manager = .{ .alloc = a, .io = manager.io, .context = &cancellation, .checkpoint = Check.call, .max_bytes = 128 * 1024 * 1024, .async_writes = false };
    defer staging.deinit();
    var terms = corpus.term_directory.Builder.init(a, &manager);
    defer terms.deinit();
    terms.enableDurable(pages.store());
    var ownership = corpus.text_directory.Mutations.init(a, &manager, pages.store());
    ownership.private_pages.phase = store.io_phase;
    defer ownership.deinit();
    var source_routes = corpus.text_directory.Mutations.init(a, &manager, pages.store());
    source_routes.private_pages.phase = store.io_phase;
    defer source_routes.deinit();
    var ties = corpus.text_directory.Mutations.init(a, &manager, pages.store());
    ties.private_pages.phase = store.io_phase;
    ties.reduction = @import("lake_index_text_ties.zig").reduce;
    defer ties.deinit();
    var metadata: corpus.text_directory.Publisher = undefined;
    try metadata.init(a, store, token, context.io.?);
    defer metadata.deinit();
    metadata.preferAdditions();
    var builder: Builder = .{ .a = a, .out = ca, .store = store, .context = context, .token = token, .pages = &pages, .terms = &terms, .ownership = &ownership, .source_routes = &source_routes, .ties = &ties, .metadata = &metadata, .staging = &staging, .pending_arena = .{ .init(a), .init(a), .init(a), .init(a) } };
    defer {
        for (0..4) |level| {
            for (builder.pending[level][0..builder.count[level]]) |input| if (input.staged) |staged| staged.release();
            builder.pending_arena[level].deinit();
        }
    }
    var prior: ?corpus.Root = null;
    if (previous) |old| if (old.serving != null) {
        prior = try corpus.servingRootPublication(ca, store.*, old, token);
    };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    if (prior) |old| {
        builder.collect_term_deltas = old.term_directory.leaves != null;
        if (old.source_routes_version == 0 or old.tie_bounds == null) {
            for (try corpus.text_directory.fileRecordsPublication(ca, old, store.*, token)) |record| {
                _ = scratch.reset(.free_all);
                const group = try corpus.loadFileManifestPublication(scratch.allocator(), store.*, record.manifest, old.domain, token);
                if (old.source_routes_version == 0) try @import("lake_index_source_routes.zig").mutate(a, &source_routes, group, false);
                if (old.tie_bounds == null) try @import("lake_index_text_ties.zig").mutate(a, &ties, group, false);
            }
        }
        builder.pack_count = old.segment_count;
        builder.serving_bytes = old.serving_bytes;
        builder.serving_metadata_bytes = old.serving_metadata_bytes;
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
        if (old.open_tails.len == old.open_packs.len) {
            // Only a new or changed input in the same tier activates its tails.
            // Unrelated tiers keep manifests, source pages and ownership intact.
            for (old.open_tails) |tail| if (!affected.contains(tail.ref.artifact_id)) {
                try builder.candidates[tail.tier].append(ca, tail);
            };
        } else for (old.open_packs) |ref| try affected.put(a, ref.artifact_id, ref);
        var it = affected.valueIterator();
        while (it.next()) |ref| {
            try builder.check();
            _ = scratch.reset(.free_all);
            const sa = scratch.allocator();
            const group = try corpus.loadFileManifestPublication(sa, store.*, ref.*, current.domain, token);
            if (group.segments.len != 1 or group.sources.len == 0) return error.InvalidNativeLakeTextCorpus;
            var sources: std.ArrayList(Span) = .empty;
            for (group.sources) |span| if (plan.by_id.get(span.file.id)) |ordinal| {
                if (!std.mem.eql(u8, &span.file.digest, &(try plan.stateAt(ordinal)).digest)) continue;
                try sources.append(sa, span);
            };
            try builder.contribution(group, -1);
            try builder.owner(group, ref.*, true);
            builder.pack_count = try std.math.sub(u32, builder.pack_count, 1);
            builder.serving_bytes = try std.math.sub(u64, builder.serving_bytes, group.admissions[0].bytes);
            if (builder.serving_metadata_bytes) |bytes| builder.serving_metadata_bytes = try std.math.sub(u64, bytes, metadataBytes(group, ref.*));
            if (sources.items.len != 0) try builder.add(.{ .ref = group.segments[0], .admission = group.admissions[0], .sources = sources.items, .original_docs = group.admissions[0].doc_count, .summary = group.summaries[0] });
        }
    }
    var manifests: corpus.ManifestReader = .{ .a = a, .store = store.*, .domain = current.domain, .token = token };
    defer manifests.deinit();
    for (delta.added, 0..) |record, index| {
        try builder.check();
        _ = scratch.reset(.free_all);
        const sa = scratch.allocator();
        const group = if (delta.replay) |replay| try replay.load(&scratch, &manifests, record.manifest, current.domain) else try manifests.load(sa, record.manifest);
        if (index + 1 < delta.added.len and (if (delta.replay) |replay| !try replay.contains(delta.added[index + 1].manifest) else true)) manifests.prefetch(delta.added[index + 1].manifest);
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
    std.debug.assert(staging.live_bytes == 0 and staging.files == 0);
    for (0..4) |level| if (!builder.activated[level]) for (builder.candidates[level].items) |tail| {
        try builder.open_packs.append(ca, tail.ref);
        try builder.open_tails.append(ca, tail);
    };
    var root = current;
    root.serving = null;
    // Every group has already been authenticated or constructed locally. The
    // reducers copy it before its arena resets; no archive-sized handoff and no
    // GET of a just-written manifest or source-span page is required.
    root.directories = try metadata.finish(if (prior) |old| old.directories else .{});
    root.file_count = builder.pack_count;
    root.segment_count = builder.pack_count;
    if (!builder.collect_term_deltas) {
        // A disabled directory supplies no complete base for signed deltas.
        // Bootstrap positive contributions from the final serving inventory,
        // including untouched packs; postings and source files stay unopened.
        const Bootstrap = struct {
            pages: *pages_mod.PageStore,
            terms: *corpus.term_directory.Builder,
            context: local.serverless_query_lake_read_context.Context,
            token: Cancellation,
            pub fn check(self: *@This()) !void {
                try self.context.ensureActive();
                try self.token.check();
            }
            pub fn record(self: *@This(), entry: anytype) !void {
                try self.check();
                try self.terms.add(self.pages.store(), entry.route.summary, corpus.term_directory.identity(entry.route.ref.artifact_id), 1);
            }
        };
        var bootstrap: Bootstrap = .{ .pages = &pages, .terms = &terms, .context = context, .token = token };
        try corpus.text_directory.walkSegmentsPublication(a, root, store.*, token, &bootstrap);
    }
    root.tie_bounds = try ties.finish(if (prior) |old| old.tie_bounds else null);
    root.term_directory = try terms.finish(pages.store(), if (prior) |old| old.term_directory else .{});
    root.source_layout_version = 1;
    root.source_routes_version = 1;
    root.source_routes = try source_routes.finish(if (prior) |old| old.source_routes else null);
    root.pack_ownership = try ownership.finish(if (prior) |old| old.pack_ownership else null);
    root.open_packs = builder.open_packs.items;
    root.open_tails = builder.open_tails.items;
    root.serving_bytes = builder.serving_bytes;
    root.serving_metadata_bytes = builder.serving_metadata_bytes;
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
            var metadata_bytes: u64 = 0;
            for (try corpus.text_directory.fileRecords(a_, root_, store_, null, .none)) |pack| {
                const group = try corpus.loadFileManifest(a_, store_, pack.manifest, root_.domain, .none, null);
                metadata_bytes += metadataBytes(group, pack.manifest);
            }
            try std.testing.expectEqual(metadata_bytes, root_.serving_metadata_bytes.?);
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
            var physical: @import("lake_index_text_predicate.zig").LiveRowsCache = .{ .backing = a_ };
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
    const IoProbe = struct {
        var active: *@This() = undefined;
        original: stores.ArtifactStore,
        execution: local.sql_parallel_scheduler.Execution,
        pending: ?local.sql_parallel_scheduler.Execution.Io = null,
        peak: usize = 0,
        parked_reads: std.atomic.Value(usize) = .init(0),
        read_count: std.atomic.Value(usize) = .init(0),
        published_segments: std.atomic.Value(usize) = .init(0),
        read_ids: [4096][32]u8 = undefined,
        fn read(_: *anyopaque, alloc: A, id: []const u8, maximum: usize, token: Cancellation) ![]u8 {
            const self = active;
            const index = self.read_count.fetchAdd(1, .monotonic);
            try std.testing.expect(index < self.read_ids.len);
            self.read_ids[index] = corpus.term_directory.identity(id);
            const bytes = try self.original.getAllocWithCancellationUsingAllocator(alloc, id, token);
            errdefer alloc.free(bytes);
            if (bytes.len > maximum) return error.ArtifactTooLarge;
            return bytes;
        }
        fn put(_: *anyopaque, alloc: A, scope: stores.UploadScope, bytes: []const u8, token: Cancellation) !stores.ArtifactMetadata {
            const self = active;
            if (bytes.len != 0 and bytes[0] == '{' and std.mem.indexOf(u8, bytes, "\"blocks\":") != null)
                _ = self.published_segments.fetchAdd(1, .monotonic);
            var backend = self.original;
            backend.allocator = alloc;
            return backend.putScoped(scope, bytes, token);
        }
        fn begin(raw: *anyopaque, count: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.peak = @max(self.peak, count);
            self.pending = try self.execution.beginIo(std.testing.io, count);
        }
        fn finish(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var pending = self.pending.?;
            self.pending = null;
            try pending.finish();
        }
        fn abort(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.pending) |*pending| pending.abort();
            self.pending = null;
        }
        fn range(_: *anyopaque, alloc: A, id: []const u8, offset: u64, count: usize, token: Cancellation) ![]u8 {
            const self = active;
            try std.testing.expect(!self.execution.active);
            if (!self.execution.active) {
                _ = self.parked_reads.fetchAdd(1, .monotonic);
                var other: local.sql_parallel_scheduler.Execution = .{ .scheduler = self.execution.scheduler, .bytes = 1024 };
                try other.enter(std.testing.io);
                other.pause();
            }
            return self.original.getRangeAllocWithCancellationUsingAllocator(alloc, id, offset, count, token);
        }
    };
    var scheduler: local.sql_parallel_scheduler.Scheduler = .{ .max_workers = 1, .max_io_workers = 3 };
    var probe: IoProbe = .{ .original = store, .execution = .{ .scheduler = &scheduler, .bytes = 1024 } };
    IoProbe.active = &probe;
    try probe.execution.enter(std.testing.io);
    defer probe.execution.pause();
    const capability: stores.IoPhase = .{ .io = std.testing.io, .ptr = &probe, .scheduler = &scheduler, .max_parallel = 3, .begin = IoProbe.begin, .finish = IoProbe.finish, .abort = IoProbe.abort };
    var vtable = store.vtable.*;
    vtable.get_range_alloc_with_cancellation = IoProbe.range;
    vtable.get_bounded_alloc_with_cancellation = IoProbe.read;
    vtable.put_scoped = IoProbe.put;
    var publication_store = store;
    publication_store.io_phase = &capability;
    publication_store.vtable = &vtable;
    var replay: corpus.ManifestReplay = undefined;
    replay.init(a, std.testing.io, .none);
    defer replay.deinit();
    const canonical = try corpus.text_directory.fileRecords(ca, current, store, null, .none);
    for (canonical) |record| {
        var decoded = std.heap.ArenaAllocator.init(a);
        defer decoded.deinit();
        try replay.append(a, record.manifest, try corpus.loadFileManifest(decoded.allocator(), store, record.manifest, current.domain, .none, null));
    }
    current.serving = try publishDelta(a, ca, &publication_store, current, null, .{ .added = canonical, .removed = &.{}, .replay = &replay }, &first_plan, .{ .io = std.testing.io }, .none);
    try std.testing.expectEqual(@as(usize, 1), probe.published_segments.load(.acquire));
    try std.testing.expectEqual(canonical.len, replay.hits);
    std.debug.print("local compaction: canonical_manifests={} canonical_gets=0 published_segments=1\n", .{canonical.len});
    for (canonical) |record| {
        const id = corpus.term_directory.identity(record.manifest.artifact_id);
        for (probe.read_ids[0..probe.read_count.load(.acquire)]) |read_id| try std.testing.expect(!std.mem.eql(u8, &id, &read_id));
    }
    // Optional replay can run out of allocation headroom at read time. The
    // same publication must fall back to canonical reads and retain its exact
    // root identity after retiring partial cache/decode state.
    {
        replay.offset = 0;
        replay.consumed = 0;
        replay.manager.allocator().free(replay.file.?.read_buffer);
        replay.file.?.read_buffer = &.{};
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        replay.manager.alloc = failing.allocator();
        defer replay.manager.alloc = a;
        const before = probe.read_count.load(.acquire);
        const retried = try publishDelta(a, ca, &publication_store, current, null, .{ .added = canonical, .removed = &.{}, .replay = &replay }, &first_plan, .{ .io = std.testing.io }, .none);
        try std.testing.expectEqualStrings(current.serving.?.artifact_id, retried.artifact_id);
        try std.testing.expect(replay.full and replay.file == null);
        var canonical_reads: usize = 0;
        for (probe.read_ids[before..probe.read_count.load(.acquire)]) |read_id| {
            for (canonical) |record| {
                if (std.mem.eql(u8, &read_id, &corpus.term_directory.identity(record.manifest.artifact_id))) canonical_reads += 1;
            }
        }
        try std.testing.expectEqual(canonical.len, canonical_reads);
    }
    try std.testing.expect(probe.execution.active);
    try std.testing.expect(probe.parked_reads.load(.acquire) > 0);
    try std.testing.expect(probe.peak > 1 and probe.peak <= 3);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
    probe.execution.pause();
    const first = try corpus.servingRoot(ca, store, current, .none, null);
    try std.testing.expectEqual(@as(u32, 1), first.segment_count);
    try std.testing.expectEqual(@as(u64, 1), first.tie_bounds.?.records);
    try std.testing.expectEqual(@as(usize, 1), first.open_packs.len);
    const first_packs = try corpus.text_directory.fileRecords(ca, first, store, null, .none);
    for (first_packs) |pack| {
        // Locally emitted metadata feeds directory/tie reducers directly. A
        // fresh serving manifest must never be read back during publication.
        const identity = corpus.term_directory.identity(pack.manifest.artifact_id);
        for (probe.read_ids[0..probe.read_count.load(.acquire)]) |read_id| try std.testing.expect(!std.mem.eql(u8, &identity, &read_id));
        const group = try corpus.loadFileManifest(ca, store, pack.manifest, current.domain, .none, null);
        try std.testing.expectEqual(@as(usize, 128), group.sources.len);
        try std.testing.expectEqual(@as(usize, 2), group.source_pages.len);
        const metadata_bytes = metadataBytes(group, pack.manifest);
        try std.testing.expect(metadata_bytes > 0);
        try std.testing.expectEqual(metadata_bytes, first.serving_metadata_bytes.?);
        const segment_id = corpus.term_directory.identity(group.segments[0].artifact_id);
        const selected = try corpus.text_directory.SelectedCursor.create(a, first, store, null, .none, &.{ segment_id, segment_id });
        defer selected.deinit();
        const planning_bytes = try corpus.text_directory.segmentBytes(ca, selected.reader.pages.store(), first, segment_id);
        try std.testing.expectEqual(corpus.source_spans.planningBytes(first.serving_bytes, metadata_bytes), planning_bytes);
        try std.testing.expect(planning_bytes > first.serving_bytes);
        // Metadata alone must prevent two native-small packs sharing a window.
        const target = @max(planning_bytes, first.serving_bytes * 2);
        var window: [2][32]u8 = undefined;
        try std.testing.expectEqual(@as(usize, 1), (try selected.nextBounded(&window, target)).?.identities.len);
        try std.testing.expectEqual(@as(usize, 1), (try selected.nextBounded(&window, target)).?.identities.len);
        try std.testing.expect((try selected.nextBounded(&window, target)) == null);
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
    try std.testing.expect(collector.marked.contains(&try pages_mod.PageStore.identity(first.domain, first.tie_bounds.?)));
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
    const old_scope = store.upload_scope.?;
    const tail_bytes = try store.getAlloc(first_packs[0].manifest.artifact_id);
    defer store.allocator.free(tail_bytes);
    try store.delete(first_packs[0].manifest.artifact_id);
    store.upload_scope = try stores.UploadScope.forPublication(current.domain, 2, std.testing.io);
    appended.serving = try publish(a, ca, &store, appended, recovered, &append_plan, .{ .io = std.testing.io }, .none);
    // Delta publication must not reopen either canonical file inventory.
    // Make those inventories unavailable while retaining serving ownership.
    var delta_base = current;
    delta_base.directories.files = null;
    var delta_current = appended;
    delta_current.directories.files = null;
    const added: corpus.text_directory.FileRecord = .{ .file = append_plan.files[128], .manifest = refs[128] };
    appended.serving = try publishDelta(a, ca, &store, delta_current, delta_base, .{ .removed = &.{}, .added = &.{added} }, &append_plan, .{ .io = std.testing.io }, .none);
    const new_scope = store.upload_scope.?;
    store.upload_scope = old_scope;
    var restored_tail = try store.put(tail_bytes);
    defer restored_tail.deinit(store.allocator);
    try std.testing.expectEqualStrings(first_packs[0].manifest.artifact_id, restored_tail.artifact_id);
    store.upload_scope = new_scope;
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
    // Recover a complete optional directory while leaving the historical pack
    // untouched. Delta-only positive entries would miss its quasar candidates;
    // signed deletions against an absent base must never reach sorted build.
    var disabled_serving = appended_root;
    disabled_serving.term_directory = .{ .version = 2 };
    // Legacy navigation bootstraps from old groups before signed changes, so
    // untouched packs survive without reloading any freshly emitted manifest.
    disabled_serving.tie_bounds = null;
    disabled_serving.source_routes_version = 0;
    disabled_serving.source_routes = null;
    var disabled_base = appended;
    disabled_base.serving = try put(ca, ca, &store, disabled_serving, .none);
    const original_tail = refs[128];
    refs[128] = try Helper.file(ca, &store, 128, 200);
    var bootstrapped = try Helper.root(ca, &store, refs);
    var bootstrap_plan = try Helper.plan(ca, bootstrapped, store, "source-128");
    defer bootstrap_plan.deinit();
    bootstrapped.serving = try publish(a, ca, &store, bootstrapped, disabled_base, &bootstrap_plan, .{ .io = std.testing.io }, .none);
    const complete = try Helper.reopen(ca, &store, bootstrapped);
    try Helper.verify(ca, store, complete, 258);
    try std.testing.expectEqual(@as(u64, complete.segment_count), complete.tie_bounds.?.records);
    var expected_terms: u64 = 0;
    for (try corpus.text_directory.fileRecords(ca, complete, store, null, .none)) |pack| {
        const group_ = try corpus.loadFileManifest(ca, store, pack.manifest, complete.domain, .none, null);
        for (group_.summaries) |summary| if (summary) |ref| {
            expected_terms += ref.records;
        };
    }
    try std.testing.expectEqual(expected_terms, complete.term_directory.leaves.?.records);
    refs[128] = original_tail;
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

test "external lake global frontier local compaction staging releases every file and allocation on failure" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: A, store: *stores.ArtifactStore) !void {
            var context: u8 = 0;
            var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = check, .max_bytes = 8192, .async_writes = false };
            defer manager.deinit();
            const payload: [4097]u8 = @splat(42);
            {
                const staged = try LocalSegment.create(a, &manager, store, &payload);
                defer staged.release();
                staged.retain();
                staged.release();
                const bytes = try staged.materialize(a);
                defer a.free(bytes);
                try std.testing.expectEqualSlices(u8, &payload, bytes);
            }
            try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
            try std.testing.expectEqual(@as(usize, 0), manager.files);
        }
    };
    const a = std.testing.allocator;
    var temporary = try local.common_test_directory.TestDirectory.init("local-compaction-staging-faults");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{&store});
}

test "external lake global frontier streamed output exceeds resident quota and supports patches checksums" {
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var temporary = try local.common_test_directory.TestDirectory.init("streamed-compaction-sink");
    defer temporary.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, temporary.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    var context: u8 = 0;
    var memory: local.sql_memory_budget = .{ .backing = a, .limit = 128 * 1024 };
    var manager: local.sql_spill.Manager = .{ .alloc = memory.allocator(), .io = std.testing.io, .context = &context, .checkpoint = Check.call, .max_bytes = 8 * 1024 * 1024, .async_writes = false };
    defer manager.deinit();
    const payload = try a.alloc(u8, 4 * 1024 * 1024);
    defer a.free(payload);
    @memset(payload, 42);
    {
        const staged = try LocalSegment.empty(memory.allocator(), &manager, &store);
        defer staged.release();
        var sink = staged.sink();
        try sink.appendNTimes(42, payload.len);
        try sink.writeAt(13, "patch");
        @memcpy(payload[13..18], "patch");
        try std.testing.expectEqual(@import("antfly_hash").Crc32.hash(payload), try sink.crc32Prefix(payload.len));
        try staged.seal();
        try std.testing.expectEqual(@as(u64, payload.len), staged.file.size);
        try std.testing.expect(sink.residentBytes() < memory.limit);
    }
    try std.testing.expectEqual(@as(usize, 0), memory.live);
    try std.testing.expect(!memory.exhausted);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.files);

    // Streaming preserves the exact native format, including patched tables,
    // page integrity, and posting sections.
    const encoded = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a, &.{
        .{ .key = "one", .value = "{\"body\":\"quasar alpha\"}" },
        .{ .key = "two", .value = "{\"body\":\"quasar beta\"}" },
    }, .{}, null)).?;
    defer a.free(encoded);
    var reader = try local.segment.SegmentReader.init(a, encoded);
    defer reader.deinit();
    const inputs = &.{local.segment.MergeInput{ .reader = &reader }};
    const expected = try local.segment.mergeSegmentInputs(a, inputs);
    defer a.free(expected);
    const staged = try LocalSegment.empty(a, &manager, &store);
    defer staged.release();
    var sink = staged.sink();
    try local.segment.writeMergedSegmentToSink(a, &sink, inputs);
    try staged.seal();
    const actual = try staged.materialize(a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(u8, expected, actual);
}
