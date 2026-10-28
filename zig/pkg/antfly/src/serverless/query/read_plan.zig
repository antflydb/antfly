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
        if (!self.root.eql(reader.root)) {
            const fresh = try reader.lookup(id);
            if ((fresh == null) != (self.fact == null)) return live.lookupAlloc(id);
            if (fresh) |value| if (value.last_lsn != self.fact.?.last_lsn or value.body.bytes != self.fact.?.body.bytes or
                !std.mem.eql(u8, &value.body.digest, &self.fact.?.body.digest)) return live.lookupAlloc(id);
        }
        const Base = struct {
            body: ?[]const u8,
            pub fn lookupAlloc(base: @This(), a: A, _: []const u8) !?[]u8 {
                return if (base.body) |body| try a.dupe(u8, body) else null;
            }
        };
        return live.view.lookupAlloc(id, Base{ .body = self.body });
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
