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

//! Durable replacement of one document's text/sparse postings. A checkpoint
//! contains immutable roots plus an authenticated input byte offset, allowing
//! one accepted WAL record to span invocations without partial publication.
const std = @import("std");
const A = std.mem.Allocator;
const input = @import("publication_input.zig");
const tree = @import("../graph_segment/page_tree.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const manifest = @import("../manifest/mod.zig");
const text = @import("../text_segment/paged.zig");
const common = @import("../paged_projection.zig");
const builder = @import("builder.zig");
const materializer = @import("../query/materializer.zig");
const Feature = struct { term: []const u8, weight: f32 };
pub const Job = struct {
    kind: enum { text, sparse },
    id: []const u8,
    text_root: ?text.Root = null,
    sparse_root: ?common.Root = null,
    old_terms: input.Input = .{},
    new_terms: input.Input = .{},
    document: input.Input = .{},
    new_document: bool,
    phase: enum { remove, add, row, done } = .remove,
    offset: u64 = 0,

    pub fn prepareText(a: A, m: A, pages: *PageStore, prior: ?manifest.ArtifactRef, id: []const u8, normalized: ?[]const u8, spec: @import("antfly_local_sources").api_full_text_indexes.FullTextIndexSpec) !Job {
        const source_name = try builder.textSourceNameAlloc(m, spec);
        const root = if (prior) |ref| (try text.loadRoot(m, pages, ref)).value else text.Root{
            .domain = pages.domain,
            .index_name = try m.dupe(u8, spec.name),
            .source_name = source_name,
            .config_json = try m.dupe(u8, spec.config_json),
        };
        // Source names are selected by the existing native projection helper.
        if (!std.mem.eql(u8, root.index_name, spec.name) or !std.mem.eql(u8, root.source_name, source_name) or !std.mem.eql(u8, root.config_json, spec.config_json)) return error.TextIndexDefinitionChanged;
        var cache = tree.Cache{ .alloc = a, .underlying = pages.store() };
        defer cache.deinit();
        const old = try tree.lookupMany(a, cache.store(), root.docs, &.{id});
        defer {
            for (old) |row| if (row) |entry| entry.deinit(a);
            a.free(old);
        }
        const before = if (old[0]) |row| try input.Input.create(a, cache.store(), row.value) else input.Input{};
        const after = if (normalized) |bytes| try input.Input.create(a, cache.store(), bytes) else input.Input{};
        return .{ .kind = .text, .id = try m.dupe(u8, id), .text_root = root, .old_terms = before, .new_terms = after, .document = after, .new_document = normalized != null };
    }

    pub fn prepareSparse(a: A, m: A, pages: *PageStore, prior: ?manifest.ArtifactRef, id: []const u8, delta: ?@import("../sparse_segment/types.zig").Segment, name: []const u8, fingerprint: [32]u8) !Job {
        const root = if (prior) |ref| (try common.load(m, pages, ref)).value else common.Root{ .kind = .sparse_segment, .domain = pages.domain, .index_name = try m.dupe(u8, name), .source_fingerprint = fingerprint };
        if (root.kind != .sparse_segment or !std.mem.eql(u8, root.index_name, name) or !std.mem.eql(u8, &root.source_fingerprint, &fingerprint)) return error.SparseIndexDefinitionChanged;
        var scratch_arena = std.heap.ArenaAllocator.init(a);
        defer scratch_arena.deinit();
        const scratch = scratch_arena.allocator();
        var cache = tree.Cache{ .alloc = a, .underlying = pages.store() };
        defer cache.deinit();
        var old_bytes = std.ArrayList(u8).empty;
        const old = try tree.lookupMany(a, cache.store(), root.docs, &.{id});
        defer {
            for (old) |row| if (row) |entry| entry.deinit(a);
            a.free(old);
        }
        if (old[0]) |row| {
            const features = try std.json.parseFromSlice([]Feature, scratch, row.value, .{});
            for (features.value) |feature| try input.appendFrame(scratch, &old_bytes, feature.term, "");
        }
        var new_bytes = std.ArrayList(u8).empty;
        var features = std.ArrayList(Feature).empty;
        if (delta) |segment| {
            if (segment.docs.len != 1 or !std.mem.eql(u8, segment.docs[0].doc_id, id)) return error.InvalidPublicationDocument;
            for (segment.terms) |term| {
                var weights = std.ArrayList(u8).empty;
                for (term.postings) |posting| {
                    if (posting.doc_index != 0 or !std.math.isFinite(posting.weight)) return error.InvalidSparseWeight;
                    try features.append(scratch, .{ .term = term.term, .weight = posting.weight });
                    var value: [4]u8 = undefined;
                    std.mem.writeInt(u32, &value, @bitCast(posting.weight), .little);
                    try weights.appendSlice(scratch, &value);
                }
                if (weights.items.len != 0) try input.appendFrame(scratch, &new_bytes, term.term, weights.items);
            }
        }
        const document = if (features.items.len != 0) try std.json.Stringify.valueAlloc(scratch, features.items, .{}) else "";
        return .{ .kind = .sparse, .id = try m.dupe(u8, id), .sparse_root = root, .old_terms = try input.Input.create(a, cache.store(), old_bytes.items), .new_terms = try input.Input.create(a, cache.store(), new_bytes.items), .document = try input.Input.create(a, cache.store(), document), .new_document = features.items.len != 0 };
    }

    pub fn validate(self: Job, domain: [32]u8) !void {
        if (self.id.len == 0 or self.id.len > @import("document_facts.zig").max_document_id_bytes) return error.InvalidPublicationDocument;
        try self.old_terms.validate();
        try self.new_terms.validate();
        try self.document.validate();
        const root_domain = switch (self.kind) {
            .text => (self.text_root orelse return error.InvalidPublicationCheckpoint).domain,
            .sparse => (self.sparse_root orelse return error.InvalidPublicationCheckpoint).domain,
        };
        if (!std.mem.eql(u8, &domain, &root_domain)) return error.GraphPageDomainMismatch;
        if (self.offset > (if (self.phase == .remove) self.old_terms.bytes else self.new_terms.bytes)) return error.InvalidPublicationCheckpoint;
    }
    pub fn step(self: *Job, a: A, pages: *PageStore, maximum: usize) !?manifest.ArtifactRef {
        if (maximum == 0 or maximum > 256 or self.phase == .done) return error.InvalidPublicationBudget;
        try self.validate(pages.domain);
        var scratch_arena = std.heap.ArenaAllocator.init(a);
        defer scratch_arena.deinit();
        const scratch = scratch_arena.allocator();
        var physical = pages.*;
        physical.prefix_keys = true;
        var writes = tree.BufferedWrites{ .alloc = a, .underlying = physical.store() };
        defer writes.deinit();
        var cache = tree.Cache{ .alloc = a, .underlying = writes.store() };
        defer cache.deinit();
        const store = cache.store();
        const docs = if (self.kind == .text) &self.text_root.?.docs else &self.sparse_root.?.docs;
        const postings = if (self.kind == .text) &self.text_root.?.postings else &self.sparse_root.?.postings;
        if (self.phase == .row) {
            const value = if (self.new_document) try self.document.readAll(scratch, store) else null;
            docs.* = try tree.apply(a, store, docs.*, &.{.{ .key = self.id, .value = value }});
            try writes.flush();
            self.phase = .done;
            return if (self.kind == .text) try text.publishRoot(a, pages, self.text_root.?) else try common.publish(a, pages, self.sparse_root.?);
        }
        const source = if (self.phase == .remove) self.old_terms else self.new_terms;
        var cursor = try input.Cursor.init(a, store, source, self.offset);
        defer cursor.deinit();
        var changes = std.StringHashMapUnmanaged(?[]const u8).empty;
        var key_bytes: usize = 0;
        for (0..maximum) |_| {
            try pages.cancellation.check();
            const item: input.Frame = if (self.kind == .text) blk: {
                const term = (try cursor.token(scratch)) orelse break;
                break :blk .{ .term = term, .value = @as([]const u8, "") };
            } else (try cursor.frame(scratch)) orelse break;
            const key = try common.postingKey(scratch, item.term, self.id);
            if (key.len > tree.max_key_bytes) return error.GraphPageRecordTooLarge;
            key_bytes += key.len;
            if (self.phase == .remove) try changes.put(scratch, key, null) else if (self.kind == .sparse) {
                if (item.value.len == 0 or item.value.len % 4 != 0) return error.InvalidPagedProjectionPosting;
                try changes.put(scratch, key, item.value);
            } else {
                const slot = try changes.getOrPut(scratch, key);
                if (!slot.found_existing) {
                    slot.value_ptr.* = try scratch.alloc(u8, 4);
                    @memset(@constCast(slot.value_ptr.*.?), 0);
                }
                const bytes = @constCast(slot.value_ptr.*.?);
                std.mem.writeInt(u32, bytes[0..4], try std.math.add(u32, std.mem.readInt(u32, bytes[0..4], .little), 1), .little);
            }
            if (key_bytes >= 64 * 1024) break;
        }
        if (self.phase == .add and self.kind == .text and changes.count() != 0) {
            const keys = try scratch.alloc([]const u8, changes.count());
            var key_it = changes.keyIterator();
            var i: usize = 0;
            while (key_it.next()) |key| : (i += 1) keys[i] = key.*;
            const prior = try tree.lookupMany(a, store, postings.*, keys);
            defer {
                for (prior) |row| if (row) |entry| entry.deinit(a);
                a.free(prior);
            }
            for (keys, prior) |key, row| if (row) |entry| {
                if (entry.value.len != 4) return error.InvalidPagedTextPosting;
                const value = @constCast(changes.get(key).?.?);
                std.mem.writeInt(u32, value[0..4], try std.math.add(u32, std.mem.readInt(u32, value[0..4], .little), std.mem.readInt(u32, entry.value[0..4], .little)), .little);
            };
        }
        postings.* = try common.applyMap(a, scratch, store, postings.*, changes);
        try writes.flush();
        self.offset = cursor.offset;
        if (self.offset == source.bytes) {
            self.phase = if (self.phase == .remove) .add else .row;
            self.offset = 0;
        }
        return null;
    }
};

test "serverless publication postings resume within one record under fresh upload scopes" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var object_store = try @import("../artifacts/object_store.zig").ObjectStore.initWithClient(a, memory.client(), "database", "artifacts");
    var artifacts = object_store.artifactStore();
    defer artifacts.deinit();
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = reads;
    const domain = PageStore.namespaceDomain("docs");
    artifacts.upload_scope = try @import("../artifacts/store.zig").UploadScope.forPublication(domain, 1, std.testing.io);
    var pages = PageStore{ .domain = domain, .attempt = artifacts.upload_scope.?.attempt, .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var metadata_arena = std.heap.ArenaAllocator.init(a);
    defer metadata_arena.deinit();
    const m = metadata_arena.allocator();
    const content = "alpha alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa quebec romeo sierra tango uniform victor whiskey xray yankee zulu alpha";
    const body = try std.json.Stringify.valueAlloc(a, .{ .text = content }, .{});
    defer a.free(body);
    const doc = materializer.Document{ .doc_id = @constCast("one"), .body = body, .last_lsn = 1, .last_timestamp_ns = 1 };
    const spec = @import("antfly_local_sources").api_full_text_indexes.FullTextIndexSpec{ .name = @constCast("fts"), .config_json = @constCast("{}") };
    var segment = try builder.allocTextSegmentAlloc(a, &.{doc}, spec, null);
    defer segment.deinit(a);
    const expected = try text.publishAlloc(a, &pages, null, segment, &.{});
    defer builder.freeArtifactRef(a, expected);
    var job = try Job.prepareText(a, m, &pages, null, "one", content, spec);
    var completed: ?manifest.ArtifactRef = null;
    for (0..32) |i| {
        const checkpoint = try std.json.Stringify.valueAlloc(a, job, .{});
        defer a.free(checkpoint);
        const parsed = try std.json.parseFromSlice(Job, m, checkpoint, .{ .allocate = .alloc_always });
        job = parsed.value;
        artifacts.upload_scope = try @import("../artifacts/store.zig").UploadScope.forPublication(domain, i + 2, std.testing.io);
        pages.attempt = artifacts.upload_scope.?.attempt;
        if (try job.step(a, &pages, 3)) |ref| {
            completed = ref;
            break;
        }
    }
    const actual = completed orelse return error.PublicationDidNotConverge;
    defer builder.freeArtifactRef(a, actual);
    var left = try text.loadRoot(a, &pages, expected);
    defer left.deinit();
    var right = try text.loadRoot(a, &pages, actual);
    defer right.deinit();
    var l = try tree.Cursor.init(a, pages.store(), left.value.postings, "", null);
    defer l.deinit();
    var rr = try tree.Cursor.init(a, pages.store(), right.value.postings, "", null);
    defer rr.deinit();
    while (try l.next()) |row| {
        const other = (try rr.next()) orelse return error.MissingPosting;
        try std.testing.expectEqualStrings(row.key, other.key);
        try std.testing.expectEqualSlices(u8, row.value, other.value);
    }
    try std.testing.expectEqual(null, try rr.next());
}
