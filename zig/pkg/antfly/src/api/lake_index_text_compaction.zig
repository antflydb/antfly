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

//! Frozen v10 producer for historical-format compatibility qualification.
//! Production publication uses lake_index_serving_compaction and v11 spans.
const std = @import("std");
const local = @import("antfly_local_sources");
const corpus = @import("lake_index_native_text.zig");
const directory = @import("lake_index_text_directory.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const pages_mod = @import("../serverless/graph_segment/page_store.zig");
const seekable = @import("lake_index_seekable_text.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Ref = artifacts.ChunkRef;
const fan_in = 8;
const target_bytes = 16 * 1024 * 1024;

fn build(a: A, out: A, store: *stores.ArtifactStore, canonical: corpus.Root, prior: ?corpus.Root, token: Cancellation, context: local.serverless_query_lake_read_context.Context) !?Ref {
    try context.ensureActive();
    const io = context.io orelse return error.UnsupportedSqlExecution;
    if (canonical.file_count < 2) return null;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const files = try directory.fileRecords(ca, canonical, store.*, null, token);
    var old: ?corpus.Root = null;
    if (prior) |parent| if (parent.compacted) |ref| {
        old = try corpus.loadRootDirectory(ca, store.*, .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, null);
        if (old.?.compacted != null) return error.InvalidNativeLakeTextCorpus;
    };
    const previous = if (old) |root| try directory.fileRecords(ca, root, store.*, null, token) else &.{};
    const kept = try ca.alloc(bool, previous.len);
    @memset(kept, false);
    var by_file: std.StringHashMapUnmanaged(usize) = .empty;
    for (previous, 0..) |record, index| try by_file.put(ca, record.file.id, index);
    var reads: u64 = 0;
    var writes: u64 = 0;
    const scope = store.upload_scope orelse return error.InvalidArtifactUploadScope;
    var pages: pages_mod.PageStore = .{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    const Checkpoint = struct { token: Cancellation, context: local.serverless_query_lake_read_context.Context };
    var checkpoint: Checkpoint = .{ .token = token, .context = context };
    const Check = struct {
        fn call(raw: *anyopaque) !void {
            const check: *Checkpoint = @ptrCast(@alignCast(raw));
            try check.context.ensureActive();
            try check.token.check();
        }
    };
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = io, .context = &checkpoint, .checkpoint = Check.call };
    defer manager.deinit();
    // An optional directory can be absent after a bounded-admission or key
    // limit fallback. Without a complete base, rebuild current contributions
    // instead of subtracting old segments or omitting retained ones.
    const prior_terms: corpus.term_directory.Roots = if (old) |v| (if (v.term_directory.version == 2) v.term_directory else .{}) else .{};
    const incremental_terms = prior_terms.leaves != null;
    var terms = corpus.term_directory.Builder.init(a, &manager);
    defer terms.deinit();
    terms.enableDurable(pages.store());
    var workspace: local.sql_memory_budget = .{ .backing = a, .limit = 64 * 1024 * 1024 };
    var pending = std.heap.ArenaAllocator.init(workspace.allocator());
    defer pending.deinit();
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var builder: Builder = .{ .a = a, .out = ca, .store = store, .token = token, .io = io, .context = context, .pages = &pages, .terms = &terms, .incremental_terms = incremental_terms, .previous = previous, .by_file = &by_file, .kept = kept, .pending = &pending };
    var index: usize = 0;
    while (index < files.len) {
        try context.ensureActive();
        try token.check();
        _ = scratch.reset(.free_all);
        if (by_file.get(files[index].file.id)) |old_index| {
            const ref = previous[old_index].manifest;
            const group = try corpus.loadFileManifest(scratch.allocator(), store.*, ref, canonical.domain, token, null);
            const count = reusable(group, files[index..]);
            if (count != 0) {
                try builder.flush();
                try builder.emit(group, ref);
                index += count;
                continue;
            }
        }
        const group = try corpus.loadFileManifest(scratch.allocator(), store.*, files[index].manifest, canonical.domain, token, null);
        if (!std.mem.eql(u8, group.file.id, files[index].file.id) or !std.mem.eql(u8, &group.file.digest, &files[index].file.digest) or group.members.len != 0) return error.InvalidNativeLakeTextCorpus;
        const eligible = group.segments.len == 1 and group.admissions.len == 1 and group.physical_manifest != null and group.admissions[0].doc_count != 0 and group.admissions[0].bytes <= target_bytes;
        if (!eligible) {
            try builder.flush();
            _ = scratch.reset(.free_all);
            const original = try corpus.loadFileManifest(scratch.allocator(), store.*, files[index].manifest, canonical.domain, token, null);
            try builder.emit(original, files[index].manifest);
            _ = pending.reset(.free_all);
        } else {
            if (builder.count != 0 and group.admissions[0].bytes > target_bytes -| builder.bytes) {
                try builder.flush();
                const reloaded = try corpus.loadFileManifest(pending.allocator(), store.*, files[index].manifest, canonical.domain, token, null);
                try builder.add(reloaded, files[index].manifest);
            } else {
                const copied = corpus.loadFileManifest(pending.allocator(), store.*, files[index].manifest, canonical.domain, token, null) catch |err| switch (err) {
                    error.OutOfMemory => {
                        try builder.flush();
                        try builder.emit(group, files[index].manifest);
                        index += 1;
                        continue;
                    },
                    else => return err,
                };
                try builder.add(copied, files[index].manifest);
            }
        }
        index += 1;
    }
    try builder.flush();
    if (!builder.compacted) return null;
    var removed: std.ArrayList(Ref) = .empty;
    for (previous, kept) |record, retained| if (!retained) {
        try removed.append(ca, record.manifest);
        _ = scratch.reset(.free_all);
        const group = try corpus.loadFileManifest(scratch.allocator(), store.*, record.manifest, canonical.domain, token, null);
        if (incremental_terms) for (group.segments, group.summaries) |segment, summary| try terms.add(pages.store(), summary, corpus.term_directory.identity(segment.artifact_id), -1);
    };
    var root = canonical;
    root.compacted = null;
    root.file_count = @intCast(builder.files);
    root.segment_count = builder.segments;
    root.term_directory = try terms.finish(pages.store(), prior_terms);
    root.directories = try directory.publishManifests(a, store, builder.changed.items, removed.items, if (old) |v| v.directories else .{}, canonical.domain, token, io);
    try root.validate();
    const bytes = try std.json.Stringify.valueAlloc(ca, root, .{});
    var upload = store.*;
    upload.allocator = out;
    const ref = try upload.putWithCancellation(bytes, token);
    defer if (ref.provider_version) |version| upload.allocator.free(version);
    return .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
}
fn reusable(group: corpus.FileGroup, files: []const directory.FileRecord) usize {
    if (group.members.len == 0) return if (files.len != 0 and std.mem.eql(u8, group.file.id, files[0].file.id) and std.mem.eql(u8, &group.file.digest, &files[0].file.digest) and group.segments.len != 1) 1 else 0;
    if (files.len < group.members.len) return 0;
    for (group.members, files[0..group.members.len]) |member, file| if (!std.mem.eql(u8, member.file.id, file.file.id) or !std.mem.eql(u8, &member.file.digest, &file.file.digest)) return 0;
    return group.members.len;
}
const Builder = struct {
    a: A,
    out: A,
    store: *stores.ArtifactStore,
    token: Cancellation,
    io: std.Io,
    context: local.serverless_query_lake_read_context.Context,
    workspace_bytes: usize = 64 * 1024 * 1024,
    pages: *pages_mod.PageStore,
    terms: *corpus.term_directory.Builder,
    incremental_terms: bool = false,
    previous: []const directory.FileRecord,
    by_file: *std.StringHashMapUnmanaged(usize),
    kept: []bool,
    pending: *std.heap.ArenaAllocator,
    groups: [fan_in]corpus.FileGroup = undefined,
    refs: [fan_in]Ref = undefined,
    count: usize = 0,
    bytes: usize = 0,
    changed: std.ArrayList(Ref) = .empty,
    files: usize = 0,
    segments: u32 = 0,
    compacted: bool = false,
    fn emit(self: *Builder, group: corpus.FileGroup, ref: Ref) !void {
        self.files += 1;
        self.segments = try std.math.add(u32, self.segments, @intCast(group.segments.len));
        self.compacted = self.compacted or group.members.len != 0;
        const retained = if (self.by_file.get(group.file.id)) |index| blk: {
            if (!std.mem.eql(u8, self.previous[index].manifest.artifact_id, ref.artifact_id)) break :blk false;
            self.kept[index] = true;
            break :blk true;
        } else false;
        if (!retained) try self.changed.append(self.out, .{ .artifact_id = try self.out.dupe(u8, ref.artifact_id), .checksum = try self.out.dupe(u8, ref.checksum), .byte_len = ref.byte_len });
        if (!retained or !self.incremental_terms) for (group.segments, group.summaries) |segment, summary| try self.terms.add(self.pages.store(), summary, corpus.term_directory.identity(segment.artifact_id), 1);
    }
    fn add(self: *Builder, group: corpus.FileGroup, ref: Ref) !void {
        self.groups[self.count] = group;
        self.refs[self.count] = ref;
        self.count += 1;
        self.bytes += @intCast(group.admissions[0].bytes);
        if (self.count == fan_in) try self.flush();
    }
    fn flush(self: *Builder) !void {
        if (self.count == 0) return;
        defer {
            self.count = 0;
            self.bytes = 0;
            _ = self.pending.reset(.free_all);
        }
        try self.context.ensureActive();
        try self.token.check();
        if (self.count >= 2) {
            const merged = self.merge() catch |err| switch (err) {
                error.OutOfMemory => null,
                else => return err,
            };
            if (merged) |result| {
                try self.emit(result.group, result.ref);
                return;
            }
        }
        for (self.groups[0..self.count], self.refs[0..self.count]) |group, ref| try self.emit(group, ref);
    }
    fn merge(self: *Builder) !?struct { group: corpus.FileGroup, ref: Ref } {
        var budget: local.sql_memory_budget = .{ .backing = self.a, .limit = @min(self.workspace_bytes, local.sql_memory_budget.headroom(self.a) orelse self.workspace_bytes) };
        const a = budget.allocator();
        var data: [fan_in]local.index.SegmentData = undefined;
        var loaded: usize = 0;
        defer for (data[0..loaded]) |*source| source.deinit(a);
        var readers: [fan_in]local.segment.SegmentReader = undefined;
        var opened: usize = 0;
        defer for (readers[0..opened]) |*reader| reader.deinit();
        var inputs: [fan_in]local.segment.MergeInput = undefined;
        for (self.groups[0..self.count], data[0..self.count], readers[0..self.count], inputs[0..self.count]) |group, *source, *reader, *input| {
            try self.token.check();
            source.* = try seekable.load(a, .{ .store = self.store.*, .cache = null, .context = self.context, .cancellation = self.token }, group.segments[0]);
            loaded += 1;
            reader.* = try source.initReader(a);
            opened += 1;
            input.* = .{ .reader = reader };
        }
        const bytes = try local.segment.mergeSegmentInputs(a, inputs[0..self.count]);
        defer a.free(bytes);
        if (bytes.len > 32 * 1024 * 1024 or bytes.len > self.bytes + 64 * 1024) return null;
        const ca = self.pending.allocator();
        var opening: @import("lake_index_text_opening_pack.zig").Collector = .{ .a = a, .out = ca, .store = self.store, .cancellation = self.token };
        defer opening.deinit();
        const ref = try seekable.publishWithOpeningCollector(a, ca, self.store, bytes, self.token, &opening);
        try opening.flush();
        var reader = try local.segment.SegmentReader.init(a, bytes);
        defer reader.deinit();
        const admission = try reader.admission(ca);
        const summary = try corpus.statistics.publishSegmentWithIo(a, self.store, bytes, self.token, self.io);
        const members = try ca.alloc(corpus.Member, self.count);
        var count: u32 = 0;
        for (members, self.groups[0..self.count]) |*member, source| {
            member.* = .{ .file = source.file, .physical = source.physical_manifest.?, .source_lower = 0, .lower = count, .count = source.admissions[0].doc_count };
            count = try std.math.add(u32, count, member.count);
        }
        if (count != admission.doc_count) return error.InvalidNativeLakeTextCorpus;
        var group: corpus.FileGroup = .{ .file = self.groups[0].file, .segments = &.{.{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }}, .members = members, .summaries = &.{summary}, .admissions = &.{admission}, .opening_packs = opening.refs.items };
        try directory.prepare(ca, self.store, &group, self.token);
        const manifest = try corpus.publishFileManifest(a, ca, self.store, group, self.token);
        return .{ .group = try corpus.loadFileManifest(ca, self.store.*, manifest, self.store.upload_scope.?.domain, self.token, null), .ref = manifest };
    }
};

test "external lake cross file compaction preserves physical holes score ties reuse and old generations" {
    const a = std.testing.allocator;
    var directory_ = try local.common_test_directory.TestDirectory.init("cross-file-compaction");
    defer directory_.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory_.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const Fixture = struct {
        fn expanded(a_: A, ca_: A, store_: *stores.ArtifactStore, root: corpus.Root, cache: artifacts.CachedRead) !corpus.Root {
            const bytes = try std.json.Stringify.valueAlloc(a_, root, .{});
            defer a_.free(bytes);
            var ref = try store_.putWithCancellation(bytes, .none);
            defer ref.deinit(a_);
            return corpus.loadRoot(ca_, store_.*, .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, .none, cache);
        }
        fn group(a_: A, ca_: A, store_: *stores.ArtifactStore, file: @import("lake_index_native_state.zig").File, body: []const u8) !corpus.FileGroup {
            var rows = corpus.physical.Builder.init(a_, ca_, store_, .none);
            defer rows.deinit();
            try rows.add(0, 5);
            try rows.add(0, 9);
            try rows.flush();
            var docs: [2]local.storage_db_document_mapper.MapperDoc = undefined;
            for (&docs, [_]u64{ 5, 9 }) |*doc, row| doc.* = .{ .key = try std.fmt.allocPrint(ca_, "lake1:{s}:00000000:{x:0>16}", .{ std.fmt.bytesToHex(&file.digest, .lower), row }), .value = body };
            const bytes = (try local.storage_db_document_mapper.buildTextSegmentFromDocuments(a_, &docs, .{}, null)).?;
            defer a_.free(bytes);
            const ref = try seekable.publish(a_, ca_, store_, bytes, .none);
            var reader = try local.segment.SegmentReader.init(a_, bytes);
            defer reader.deinit();
            const segments = try ca_.alloc(Ref, 1);
            segments[0] = .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len };
            const admissions = try ca_.alloc(local.segment.SegmentReader.Admission, 1);
            admissions[0] = try reader.admission(ca_);
            const summaries = try ca_.alloc(?corpus.statistics.Ref, 1);
            summaries[0] = try corpus.statistics.publishSegmentWithIo(a_, store_, bytes, .none, std.testing.io);
            var result: corpus.FileGroup = .{ .file = file, .segments = segments, .admissions = admissions, .summaries = summaries, .rows = rows.blocks.items };
            try directory.prepare(ca_, store_, &result, .none);
            return result;
        }
    };
    const groups = try ca.alloc(corpus.FileGroup, 16);
    const refs = try ca.alloc(Ref, groups.len);
    for (groups, refs, 0..) |*group, *ref, i| {
        group.* = try Fixture.group(a, ca, &store, .{ .id = try std.fmt.allocPrint(ca, "file-{d}", .{i}), .digest = @splat(@intCast(i + 1)) }, "{\"body\":\"quasar\"}");
        ref.* = try corpus.publishFileManifest(a, ca, &store, group.*, .none);
    }
    var canonical: corpus.Root = .{ .domain = store.upload_scope.?.domain, .binding = .{ .sidecar_kind = .text, .source_kind = .external_parquet, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"body"} }, .config_json = "{\"field\":\"body\"}", .inventory_version = 3, .seekable = true, .admission_version = 1, .statistics_version = 2, .file_count = 16, .segment_count = 16, .collection = .{ .documents = 32, .fields = &.{.{ .name = "body", .total_length = 32 }} }, .directories = try directory.publish(a, &store, groups, refs, &.{}, &.{}, .{}, .none, std.testing.io) };
    const context: local.serverless_query_lake_read_context.Context = .{ .io = std.testing.io };
    // Optional merging must preserve the original runs if workspace is denied.
    var pending = std.heap.ArenaAllocator.init(a);
    defer pending.deinit();
    var by_file: std.StringHashMapUnmanaged(usize) = .empty;
    defer by_file.deinit(a);
    var remaining_reads: u64 = 0;
    var remaining_writes: u64 = 0;
    var pages: pages_mod.PageStore = .{ .domain = store.upload_scope.?.domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &remaining_reads, .remaining_write_bytes = &remaining_writes, .streaming = true };
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &remaining_reads, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check };
    defer manager.deinit();
    var terms = corpus.term_directory.Builder.init(a, &manager);
    defer terms.deinit();
    terms.enableDurable(pages.store());
    var denied: Builder = .{ .a = a, .out = ca, .store = &store, .token = .none, .io = std.testing.io, .context = context, .workspace_bytes = 1, .pages = &pages, .terms = &terms, .previous = &.{}, .by_file = &by_file, .kept = &.{}, .pending = &pending };
    try denied.add(groups[0], refs[0]);
    try denied.add(groups[1], refs[1]);
    try denied.flush();
    try std.testing.expectEqual(@as(u32, 2), denied.segments);
    try std.testing.expect(!denied.compacted);
    const compact = (try build(a, ca, &store, canonical, null, .none, context)).?;
    var prior = canonical;
    prior.compacted = compact;
    const serving = try corpus.loadRootDirectory(ca, store, .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = compact.artifact_id, .checksum = compact.checksum, .byte_len = compact.byte_len }, .none, null);
    try std.testing.expectEqual(@as(u32, 2), serving.segment_count);
    try std.testing.expectEqual(@as(u32, 2), serving.file_count);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(1), .context = context };
    const original = try Fixture.expanded(a, ca, &store, canonical, cached);
    const expanded = try Fixture.expanded(a, ca, &store, serving, cached);
    var baseline = try corpus.loadWriter(a, store, original, .none, cached, null);
    defer baseline.deinit();
    var merged = try corpus.loadWriter(a, store, expanded, .none, cached, null);
    defer merged.deinit();
    const identities = try @import("lake_index_text_predicate.zig").Identities.init(ca, expanded, merged.snapshot());
    var live: @import("lake_index_text_predicate.zig").LiveRowsCache = .{ .backing = a };
    defer live.deinit(a);
    for (0..32) |i| {
        const old_key = (try baseline.snapshot().storedIdScoped(a, @intCast(i))).?;
        defer a.free(old_key);
        const new_key = (try merged.snapshot().storedIdScoped(a, @intCast(i))).?;
        defer a.free(new_key);
        try std.testing.expectEqualStrings(old_key, new_key);
        const physical = try identities.physicalRow(a, &live, store, cached, @intCast(i), "source", "snapshot");
        try std.testing.expectEqual(@as(?u32, @intCast(i)), try identities.ordinal(a, &live, store, cached, physical));
        var deleted = physical;
        deleted.external.row_ordinal = 6;
        try std.testing.expect((try identities.ordinal(a, &live, store, cached, deleted)) == null);
    }
    const old_hits = try baseline.snapshot().search(a, "body", &.{"quasar"}, 32);
    defer a.free(old_hits.hits);
    const new_hits = try merged.snapshot().search(a, "body", &.{"quasar"}, 32);
    defer a.free(new_hits.hits);
    try std.testing.expectEqual(old_hits.total_count, new_hits.total_count);
    try std.testing.expectEqualSlices(local.search_scorer.ScoredHit, old_hits.hits, new_hits.hits);
    const reused = (try build(a, ca, &store, canonical, prior, .none, context)).?;
    try std.testing.expectEqualStrings(compact.artifact_id, reused.artifact_id);
    const old_units = try directory.fileRecords(ca, serving, store, null, .none);
    const changed_index = for (groups, 0..) |group, i| {
        if (std.mem.eql(u8, group.file.id, old_units[0].file.id)) break i;
    } else return error.TestUnexpectedResult;
    var file = groups[changed_index].file;
    file.digest = @splat(42);
    const changed = try Fixture.group(a, ca, &store, file, "{\"body\":\"galaxy\"}");
    const changed_ref = try corpus.publishFileManifest(a, ca, &store, changed, .none);
    canonical.directories = try directory.publish(a, &store, &.{changed}, &.{changed_ref}, &.{groups[changed_index]}, &.{refs[changed_index]}, canonical.directories, .none, std.testing.io);
    const updated = (try build(a, ca, &store, canonical, prior, .none, context)).?;
    const updated_root = try corpus.loadRootDirectory(ca, store, .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = updated.artifact_id, .checksum = updated.checksum, .byte_len = updated.byte_len }, .none, null);
    const updated_units = try directory.fileRecords(ca, updated_root, store, null, .none);
    try std.testing.expectEqualStrings(old_units[1].manifest.artifact_id, updated_units[1].manifest.artifact_id);
    try std.testing.expect(!std.mem.eql(u8, old_units[0].manifest.artifact_id, updated_units[0].manifest.artifact_id));
    const latest = try Fixture.expanded(a, ca, &store, updated_root, cached);
    var latest_writer = try corpus.loadWriter(a, store, latest, .none, cached, null);
    defer latest_writer.deinit();
    const changed_hits = try latest_writer.snapshot().search(a, "body", &.{"quasar"}, 32);
    defer a.free(changed_hits.hits);
    try std.testing.expectEqual(@as(u32, 30), changed_hits.total_count);
    const retained_hits = try merged.snapshot().search(a, "body", &.{"quasar"}, 32);
    defer a.free(retained_hits.hits);
    try std.testing.expectEqual(@as(u32, 32), retained_hits.total_count);
    // Removing the first member must not shift every subsequent merge unit.
    var updated_parent = canonical;
    updated_parent.compacted = updated;
    canonical.directories = try directory.publish(a, &store, &.{}, &.{}, &.{changed}, &.{changed_ref}, canonical.directories, .none, std.testing.io);
    canonical.file_count = 15;
    canonical.segment_count = 15;
    canonical.collection = .{ .documents = 30, .fields = &.{.{ .name = "body", .total_length = 30 }} };
    const removed = (try build(a, ca, &store, canonical, updated_parent, .none, context)).?;
    const removed_root = try corpus.loadRootDirectory(ca, store, .{ .kind = .text_segment, .metadata_version = corpus.metadata_version, .artifact_id = removed.artifact_id, .checksum = removed.checksum, .byte_len = removed.byte_len }, .none, null);
    const removed_units = try directory.fileRecords(ca, removed_root, store, null, .none);
    try std.testing.expectEqualStrings(old_units[1].manifest.artifact_id, removed_units[1].manifest.artifact_id);
    var deadline = context;
    deadline.deadline_ns = 1;
    try std.testing.expectError(error.DeadlineExceeded, build(a, ca, &store, canonical, updated_parent, .none, deadline));
}
