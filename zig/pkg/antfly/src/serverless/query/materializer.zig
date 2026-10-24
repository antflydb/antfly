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

const std = @import("std");
const Allocator = std.mem.Allocator;
const facts = @import("../build/document_facts.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const api_types = @import("../api/types.zig");

pub const Bodies = struct {
    pages: ?*PageStore = null,
    artifacts: *@import("../artifacts/store.zig").ArtifactStore,
    namespace: []const u8,
    domain: ?[32]u8 = null,
    cancellation: @import("antfly_cancellation").CancellationToken = .none,
    max_read_bytes: u64 = 512 * 1024 * 1024,
};

pub const Mutation = struct {
    lsn: u64,
    timestamp_ns: u64,
    kind: api_types.MutationKind,
    doc_id: []const u8,
    body: ?[]const u8 = null,
};

pub const Document = struct {
    doc_id: []u8,
    body: []u8,
    /// Physical checkpoint payload, retained for compact fact publication.
    stored_body: ?[]u8 = null,
    last_lsn: u64,
    last_timestamp_ns: u64,

    pub fn deinit(self: *Document, alloc: Allocator) void {
        alloc.free(self.doc_id);
        alloc.free(self.body);
        if (self.stored_body) |bytes| alloc.free(bytes);
        self.* = undefined;
    }
};

const Slot = struct {
    doc_id: []u8,
    body: ?[]u8 = null,
    stored_body: ?[]u8 = null,
    deleted: bool = false,
    last_lsn: u64 = 0,
    last_timestamp_ns: u64 = 0,

    pub fn deinit(self: *Slot, alloc: Allocator) void {
        alloc.free(self.doc_id);
        if (self.body) |body| alloc.free(body);
        if (self.stored_body) |bytes| alloc.free(bytes);
        self.* = undefined;
    }
};

pub fn materializeAlloc(alloc: Allocator, mutations: []const Mutation) ![]Document {
    return try materializeOverBaseAlloc(alloc, &.{}, mutations);
}

pub fn materializeOverBaseAlloc(alloc: Allocator, base_docs: []const Document, mutations: []const Mutation) ![]Document {
    return materializeWithBodiesAlloc(alloc, base_docs, mutations, null);
}
pub fn materializeWithBodiesAlloc(alloc: Allocator, base_docs: []const Document, mutations: []const Mutation, bodies: ?Bodies) ![]Document {
    var read_budget: u64 = if (bodies) |store| store.max_read_bytes else 0;
    var write_budget: u64 = 0;

    var slots = std.ArrayListUnmanaged(Slot).empty;
    defer {
        for (slots.items) |*slot| slot.deinit(alloc);
        slots.deinit(alloc);
    }

    var index_by_doc = std.StringHashMapUnmanaged(usize).empty;
    defer index_by_doc.deinit(alloc);

    for (base_docs) |doc| {
        // Reserve both containers before transferring string ownership. A
        // failed map growth must never leave a slot owning freed strings.
        try slots.ensureUnusedCapacity(alloc, 1);
        try index_by_doc.ensureUnusedCapacity(alloc, 1);
        const doc_id = try alloc.dupe(u8, doc.doc_id);
        errdefer alloc.free(doc_id);
        const body = try alloc.dupe(u8, doc.body);
        errdefer alloc.free(body);
        const stored_body = if (doc.stored_body) |bytes| try alloc.dupe(u8, bytes) else null;
        errdefer if (stored_body) |bytes| alloc.free(bytes);
        slots.appendAssumeCapacity(.{
            .stored_body = stored_body,
            .doc_id = doc_id,
            .body = body,
            .deleted = false,
            .last_lsn = doc.last_lsn,
            .last_timestamp_ns = doc.last_timestamp_ns,
        });
        const slot_index = slots.items.len - 1;
        index_by_doc.putAssumeCapacity(slots.items[slot_index].doc_id, slot_index);
    }

    for (mutations) |mutation| {
        const existing_index = index_by_doc.get(mutation.doc_id);
        const idx = existing_index orelse blk: {
            try slots.ensureUnusedCapacity(alloc, 1);
            try index_by_doc.ensureUnusedCapacity(alloc, 1);
            const doc_id = try alloc.dupe(u8, mutation.doc_id);
            slots.appendAssumeCapacity(.{ .doc_id = doc_id });
            const slot_index = slots.items.len - 1;
            index_by_doc.putAssumeCapacity(slots.items[slot_index].doc_id, slot_index);
            break :blk slot_index;
        };

        var slot = &slots.items[idx];
        slot.last_lsn = mutation.lsn;
        slot.last_timestamp_ns = mutation.timestamp_ns;
        switch (mutation.kind) {
            .upsert, .checkpoint => {
                const raw = mutation.body orelse "";
                if (mutation.kind == .upsert and facts.isCheckpointBody(raw)) return error.InvalidDocumentBody;
                const next_body = if (mutation.kind == .checkpoint) checkpoint: {
                    const store = bodies orelse return error.DocumentBodyNotFound;
                    var pages = PageStore{ .domain = store.domain orelse PageStore.namespaceDomain(store.namespace), .artifacts = store.artifacts, .cancellation = store.cancellation, .remaining_read_bytes = &read_budget, .remaining_write_bytes = &write_budget };
                    break :checkpoint try facts.resolveCheckpointBodyAlloc(alloc, store.pages orelse &pages, raw);
                } else try alloc.dupe(u8, raw);
                errdefer alloc.free(next_body);
                const stored_body = if (mutation.kind == .checkpoint) try alloc.dupe(u8, raw) else null;
                if (slot.stored_body) |bytes| alloc.free(bytes);
                slot.stored_body = stored_body;
                if (slot.body) |body| alloc.free(body);
                slot.body = next_body;
                slot.deleted = false;
            },
            .delete => {
                if (slot.stored_body) |bytes| alloc.free(bytes);
                slot.stored_body = null;
                if (slot.body) |body| {
                    alloc.free(body);
                    slot.body = null;
                }
                slot.deleted = true;
            },
        }
    }

    var live_count: usize = 0;
    for (slots.items) |slot| {
        if (!slot.deleted and slot.body != null) live_count += 1;
    }

    const docs = try alloc.alloc(Document, live_count);
    errdefer alloc.free(docs);

    var out_idx: usize = 0;
    for (slots.items) |*slot| {
        if (slot.deleted or slot.body == null) continue;
        docs[out_idx] = .{
            .doc_id = slot.doc_id,
            .body = slot.body.?,
            .stored_body = slot.stored_body,
            .last_lsn = slot.last_lsn,
            .last_timestamp_ns = slot.last_timestamp_ns,
        };
        // The completed view owns these buffers; avoid cloning the entire
        // namespace a second time at the materialization boundary.
        slot.doc_id = &.{};
        slot.body = null;
        slot.stored_body = null;
        out_idx += 1;
    }

    std.mem.sort(Document, docs, {}, lessDocument);
    return docs;
}

pub fn freeDocuments(alloc: Allocator, docs: []Document) void {
    for (docs) |*doc| doc.deinit(alloc);
    alloc.free(docs);
}

fn lessDocument(_: void, lhs: Document, rhs: Document) bool {
    return std.mem.order(u8, lhs.doc_id, rhs.doc_id) == .lt;
}

test "serverless materializer transfers output ownership and unwinds every allocation failure" {
    const Check = struct {
        fn run(alloc: Allocator) !void {
            const base = [_]Document{.{ .doc_id = @constCast("a"), .body = @constCast("old"), .last_lsn = 1, .last_timestamp_ns = 1 }};
            const mutations = [_]Mutation{
                .{ .lsn = 2, .timestamp_ns = 2, .kind = .upsert, .doc_id = "a", .body = "replacement" },
                .{ .lsn = 3, .timestamp_ns = 3, .kind = .upsert, .doc_id = "b", .body = "temporary" },
                .{ .lsn = 4, .timestamp_ns = 4, .kind = .delete, .doc_id = "b" },
                .{ .lsn = 5, .timestamp_ns = 5, .kind = .upsert, .doc_id = "c", .body = "new" },
            };
            const docs = try materializeOverBaseAlloc(alloc, &base, &mutations);
            defer freeDocuments(alloc, docs);
            try std.testing.expectEqual(@as(usize, 2), docs.len);
            try std.testing.expectEqualStrings("replacement", docs[0].body);
            try std.testing.expectEqualStrings("c", docs[1].doc_id);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "materializer applies upserts and deletes into document state" {
    const alloc = std.testing.allocator;
    const mutations = [_]Mutation{
        .{ .lsn = 1, .timestamp_ns = 10, .kind = .upsert, .doc_id = "doc-b", .body = "one" },
        .{ .lsn = 2, .timestamp_ns = 20, .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
        .{ .lsn = 3, .timestamp_ns = 30, .kind = .delete, .doc_id = "doc-b" },
        .{ .lsn = 4, .timestamp_ns = 40, .kind = .upsert, .doc_id = "doc-c", .body = "gamma" },
    };

    const docs = try materializeAlloc(alloc, &mutations);
    defer freeDocuments(alloc, docs);

    try std.testing.expectEqual(@as(usize, 2), docs.len);
    try std.testing.expectEqualStrings("doc-a", docs[0].doc_id);
    try std.testing.expectEqualStrings("alpha", docs[0].body);
    try std.testing.expectEqualStrings("doc-c", docs[1].doc_id);
    try std.testing.expectEqualStrings("gamma", docs[1].body);
}

test "materializer overlays mutations onto existing document state" {
    const alloc = std.testing.allocator;
    var base_docs = [_]Document{
        .{
            .doc_id = try alloc.dupe(u8, "doc-a"),
            .body = try alloc.dupe(u8, "alpha"),
            .last_lsn = 2,
            .last_timestamp_ns = 20,
        },
        .{
            .doc_id = try alloc.dupe(u8, "doc-b"),
            .body = try alloc.dupe(u8, "beta"),
            .last_lsn = 3,
            .last_timestamp_ns = 30,
        },
    };
    defer for (&base_docs) |*doc| doc.deinit(alloc);

    const mutations = [_]Mutation{
        .{ .lsn = 4, .timestamp_ns = 40, .kind = .delete, .doc_id = "doc-a" },
        .{ .lsn = 5, .timestamp_ns = 50, .kind = .upsert, .doc_id = "doc-c", .body = "gamma" },
    };

    const docs = try materializeOverBaseAlloc(alloc, &base_docs, &mutations);
    defer freeDocuments(alloc, docs);

    try std.testing.expectEqual(@as(usize, 2), docs.len);
    try std.testing.expectEqualStrings("doc-b", docs[0].doc_id);
    try std.testing.expectEqualStrings("doc-c", docs[1].doc_id);
}
