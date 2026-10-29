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

//! Immutable point-read preparation. This plan conveys no authority. Fresh
//! admission and the normal live session always precede result selection.
const std = @import("std");
const A = std.mem.Allocator;
const facts = @import("../build/document_facts.zig");
const tree = @import("../graph_segment/page_tree.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const ArtifactRef = @import("../manifest/mod.zig").ArtifactRef;
const Live = @import("live_session.zig").Session;

pub const Plan = struct {
    format: u8 = 1,
    domain: [32]u8,
    source: ArtifactRef,

    pub fn fromSession(session: *Live) ?Plan {
        const published = session.published orelse return null;
        for (published.manifest.artifacts) |ref| if (ref.kind == .document_facts) return .{
            .domain = PageStore.namespaceDomain(published.manifest.namespace),
            .source = ref,
        };
        return null;
    }

    pub fn prepare(self: Plan, a: A, namespace: []const u8, id: []const u8, pages: *PageStore) !Prepared {
        if (self.format != 1 or !std.mem.eql(u8, &self.domain, &PageStore.namespaceDomain(namespace)) or
            self.source.kind != .document_facts or self.source.byte_len > facts.Root.encoded_bytes or
            self.source.artifact_id.len > 256 or self.source.checksum.len != 64) return error.InvalidNativeReadPlan;
        const root = try facts.loadRoot(a, pages, self.source);
        if (!std.mem.eql(u8, &root.domain, &self.domain)) return error.InvalidNativeReadPlan;
        var cache = tree.Cache{ .alloc = a, .underlying = pages.store() };
        defer cache.deinit();
        var cursor = try tree.Cursor.init(a, cache.store(), root.page, id, null);
        defer cursor.deinit();
        const record = (try cursor.next()) orelse return .{ .id = try a.dupe(u8, id), .root = root, .body = null, .fact = null };
        if (!std.mem.eql(u8, record.key, id)) return .{ .id = try a.dupe(u8, id), .root = root, .body = null, .fact = null };
        const fact = try facts.decodeRecord(record);
        if (fact.last_lsn > root.wal_end_lsn) return error.InvalidDocumentFact;
        // Speculation must not duplicate a large response in the small point lane.
        if (fact.body.bytes > 128 * 1024) return .{ .id = try a.dupe(u8, id), .root = root, .body = null, .fact = fact, .materialized = false };
        const body = try facts.readRecordBodyAlloc(a, pages, record);
        errdefer a.free(body);
        return .{ .id = try a.dupe(u8, id), .root = root, .body = body, .fact = fact };
    }
};

pub const Prepared = struct {
    id: []u8,
    fact: ?facts.Fact,
    root: facts.Root,
    body: ?[]u8,
    materialized: bool = true,
    pub fn deinit(self: *Prepared, a: A) void {
        a.free(self.id);
        if (self.body) |body| a.free(body);
    }
    /// Native root equality accepts prepared membership/body bytes only after
    /// fresh authority, pin/floor admission and a fresh WAL capture. The fresh
    /// overlay still wins over the prepared published row or its absence.
    pub fn lookupAlloc(self: Prepared, live: *Live, id: []const u8) !?[]u8 {
        if (!std.mem.eql(u8, self.id, id)) return error.InvalidNativeReadPlan;
        if (!self.materialized) return live.lookupAlloc(id);
        const reader = live.reader orelse return live.lookupAlloc(id);
        const Base = struct {
            prepared: Prepared,
            reader: *@import("document_facts_reader.zig").Reader,
            pub fn lookupAlloc(base: @This(), a: A, key: []const u8) !?[]u8 {
                // The live overlay gets first refusal. An acknowledged recent
                // update/delete need not fetch or validate an obsolete row.
                if (base.prepared.root.eql(base.reader.root)) return if (base.prepared.body) |body| try a.dupe(u8, body) else null;
                return base.reader.lookupBodyWithHintAlloc(key, if (base.prepared.fact) |fact| .{ .fact = fact, .body = base.prepared.body.? } else null);
            }
        };
        return live.view.lookupAlloc(id, Base{ .prepared = self, .reader = reader });
    }
};

/// An optional immutable working set, never an authority or a live session.
/// Entries own all their bytes and are consumed once. Eviction is a cache miss;
/// the caller must perform the ordinary fresh read in that case.
pub const PreparedCache = struct {
    pub const max_entries = 4;
    pub const max_bytes = 256 * 1024;
    const Entry = struct { token: [16]u8, domain: [32]u8, prepared: Prepared };
    entries: [max_entries]?Entry = @splat(null),
    next: usize = 0,
    bytes: usize = 0,

    fn size(value: Prepared) usize {
        return @sizeOf(Entry) + value.id.len + if (value.body) |body| body.len else @as(usize, 0);
    }
    fn remove(self: *PreparedCache, a: A, index: usize) void {
        if (self.entries[index]) |*entry| {
            self.bytes -= size(entry.prepared);
            entry.prepared.deinit(a);
            self.entries[index] = null;
        }
    }
    pub fn deinit(self: *PreparedCache, a: A) void {
        for (0..max_entries) |i| self.remove(a, i);
    }
    pub fn put(self: *PreparedCache, a: A, namespace: []const u8, token: [16]u8, value: Prepared) !bool {
        if (!value.materialized or value.id.len > 1024 or size(value) > max_bytes) return false;
        // Duplicate tickets cannot select a different, older entry.
        for (self.entries, 0..) |entry, i| if (entry) |item| {
            if (std.mem.eql(u8, &item.token, &token)) self.remove(a, i);
        };
        self.remove(a, self.next);
        while (self.bytes + size(value) > max_bytes) {
            self.next = (self.next + 1) % max_entries;
            self.remove(a, self.next);
        }
        var copy = value;
        copy.id = try a.dupe(u8, value.id);
        errdefer a.free(copy.id);
        copy.body = if (value.body) |body| try a.dupe(u8, body) else null;
        self.entries[self.next] = .{ .token = token, .domain = PageStore.namespaceDomain(namespace), .prepared = copy };
        self.bytes += size(copy);
        self.next = (self.next + 1) % max_entries;
        return true;
    }
    pub fn take(self: *PreparedCache, namespace: []const u8, id: []const u8, token: [16]u8) ?Prepared {
        for (&self.entries) |*slot| if (slot.*) |entry| {
            if (!std.mem.eql(u8, &entry.token, &token) or !std.mem.eql(u8, &entry.domain, &PageStore.namespaceDomain(namespace)) or !std.mem.eql(u8, entry.prepared.id, id)) continue;
            slot.* = null;
            self.bytes -= size(entry.prepared);
            return entry.prepared;
        };
        return null;
    }
};

test "serverless native read plan authenticates bounded immutable inputs and binds namespace and document" {
    const a = std.testing.allocator;
    var memory = @import("objectstore").MemoryClient.init(a);
    defer memory.deinit();
    var object_store = try @import("../artifacts/object_store.zig").ObjectStore.initWithClient(a, memory.client(), "database", "artifacts");
    var artifacts = object_store.artifactStore();
    defer artifacts.deinit();
    const domain = PageStore.namespaceDomain("docs");
    artifacts.upload_scope = try @import("../artifacts/store.zig").UploadScope.forPublication(domain, 1, std.testing.io);
    var reads: u64 = 4 * 1024 * 1024;
    var writes: u64 = reads;
    var pages = PageStore{ .domain = domain, .attempt = artifacts.upload_scope.?.attempt, .artifacts = &artifacts, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const doc = @import("materializer.zig").Document{ .doc_id = @constCast("one"), .body = @constCast("{\"text\":\"prepared\"}"), .last_lsn = 1, .last_timestamp_ns = 1 };
    const ref = try @import("../build/document_facts_builder.zig").publishAlloc(a, &pages, null, &.{doc}, null, .{}, "{}", 1);
    defer @import("../build/builder.zig").freeArtifactRef(a, ref);
    writes = 0;
    const plan = Plan{ .domain = domain, .source = ref };
    var prepared = try plan.prepare(a, "docs", "one", &pages);
    defer prepared.deinit(a);
    try std.testing.expectEqualStrings("one", prepared.id);
    try std.testing.expectEqualStrings(doc.body, prepared.body.?);
    try std.testing.expectEqual(@as(u64, 1), prepared.fact.?.last_lsn);
    try std.testing.expectError(error.InvalidNativeReadPlan, plan.prepare(a, "other", "one", &pages));
    var missing = try plan.prepare(a, "docs", "missing", &pages);
    defer missing.deinit(a);
    try std.testing.expectEqual(null, missing.body);
    try std.testing.expectEqual(null, missing.fact);
    try std.testing.expectEqual(@as(u64, 0), writes);
}

test "serverless native read plan cache owns bytes and bounds tickets by namespace and document" {
    const a = std.testing.allocator;
    var cache: PreparedCache = .{};
    defer cache.deinit(a);
    const token: [16]u8 = @splat(1);
    var id = [_]u8{'a'};
    var body = [_]u8{'b'};
    const value: Prepared = .{ .id = &id, .fact = null, .root = .{ .domain = PageStore.namespaceDomain("docs"), .policy_fingerprint = @splat(0) }, .body = &body };
    try std.testing.expect(try cache.put(a, "docs", token, value));
    body[0] = 'c';
    try std.testing.expectEqual(null, cache.take("other", "a", token));
    try std.testing.expectEqual(null, cache.take("docs", "b", token));
    var taken = cache.take("docs", "a", token).?;
    defer taken.deinit(a);
    try std.testing.expectEqualStrings("b", taken.body.?);
    try std.testing.expectEqual(null, cache.take("docs", "a", token));
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
    const large = try a.alloc(u8, 128 * 1024);
    defer a.free(large);
    var bounded = value;
    bounded.body = large;
    for (0..10) |i| {
        try std.testing.expect(try cache.put(a, "docs", @splat(@intCast(i)), bounded));
        try std.testing.expect(cache.bytes <= PreparedCache.max_bytes);
    }
    try std.testing.expectEqual(null, cache.take("docs", "a", @splat(0)));
    cache.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), cache.bytes);
}
