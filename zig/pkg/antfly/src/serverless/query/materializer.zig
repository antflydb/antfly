// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const Allocator = std.mem.Allocator;
const api_types = @import("../api/types.zig");

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
    last_lsn: u64,
    last_timestamp_ns: u64,

    pub fn deinit(self: *Document, alloc: Allocator) void {
        alloc.free(self.doc_id);
        alloc.free(self.body);
        self.* = undefined;
    }
};

const Slot = struct {
    doc_id: []u8,
    body: ?[]u8 = null,
    deleted: bool = false,
    last_lsn: u64 = 0,
    last_timestamp_ns: u64 = 0,

    fn deinit(self: *Slot, alloc: Allocator) void {
        alloc.free(self.doc_id);
        if (self.body) |body| alloc.free(body);
        self.* = undefined;
    }
};

pub fn materializeAlloc(alloc: Allocator, mutations: []const Mutation) ![]Document {
    return try materializeOverBaseAlloc(alloc, &.{}, mutations);
}

pub fn materializeOverBaseAlloc(alloc: Allocator, base_docs: []const Document, mutations: []const Mutation) ![]Document {
    var slots = std.ArrayListUnmanaged(Slot).empty;
    defer {
        for (slots.items) |*slot| slot.deinit(alloc);
        slots.deinit(alloc);
    }

    var index_by_doc = std.StringHashMapUnmanaged(usize).empty;
    defer index_by_doc.deinit(alloc);

    for (base_docs) |doc| {
        const doc_id = try alloc.dupe(u8, doc.doc_id);
        errdefer alloc.free(doc_id);
        const body = try alloc.dupe(u8, doc.body);
        errdefer alloc.free(body);
        try slots.append(alloc, .{
            .doc_id = doc_id,
            .body = body,
            .deleted = false,
            .last_lsn = doc.last_lsn,
            .last_timestamp_ns = doc.last_timestamp_ns,
        });
        const slot_index = slots.items.len - 1;
        try index_by_doc.put(alloc, slots.items[slot_index].doc_id, slot_index);
    }

    for (mutations) |mutation| {
        const existing_index = index_by_doc.get(mutation.doc_id);
        const idx = existing_index orelse blk: {
            const doc_id = try alloc.dupe(u8, mutation.doc_id);
            errdefer alloc.free(doc_id);
            try slots.append(alloc, .{ .doc_id = doc_id });
            const slot_index = slots.items.len - 1;
            try index_by_doc.put(alloc, slots.items[slot_index].doc_id, slot_index);
            break :blk slot_index;
        };

        var slot = &slots.items[idx];
        slot.last_lsn = mutation.lsn;
        slot.last_timestamp_ns = mutation.timestamp_ns;
        switch (mutation.kind) {
            .upsert => {
                if (slot.body) |body| alloc.free(body);
                slot.body = try alloc.dupe(u8, mutation.body orelse "");
                slot.deleted = false;
            },
            .delete => {
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
    for (slots.items) |slot| {
        if (slot.deleted or slot.body == null) continue;
        docs[out_idx] = .{
            .doc_id = try alloc.dupe(u8, slot.doc_id),
            .body = try alloc.dupe(u8, slot.body.?),
            .last_lsn = slot.last_lsn,
            .last_timestamp_ns = slot.last_timestamp_ns,
        };
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
