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

//! Fixed-ordinal source cohorts below a native immutable family publication.
//! Navigation borrows the family's actual reader lease. Descriptors identify
//! sources; callers must acquire the selected leaf's own native lease before
//! releasing the family, and before opening any primary source read.
const std = @import("std");
const graph = @import("immutable_graph.zig");
const publication = @import("index_publication.zig");
const maps = @import("index_source_map.zig");
const fields = @import("index_field_generation.zig");
const Digest = publication.Digest;
pub const cohort_capacity = maps.page_capacity;
const maximum_levels = 5; // ceil(log64(ceil(maxInt(u32) / 64)))
const layout_bytes = 80;
const leaf_bytes = 76;
pub const maximum_layout_bytes = 148;
pub const FieldInventory = struct { ordinal: Digest, count: u32 };

pub const Layout = struct {
    segment: u64,
    documents: u32,
    physical: Digest,
    configuration: Digest,
    /// Retry provenance, not read authority. Lives only with this active
    /// immutable generation; ordinary mutation clears it rather than retaining
    /// an unbounded historical transfer ledger.
    transfer: ?Digest = null,
    field_inventory: ?FieldInventory = null,
    fn encode(self: Layout) ![layout_bytes]u8 {
        if (self.segment == 0 or self.documents == 0) return error.InvalidIndexSourceFamily;
        if (self.field_inventory) |inventory| if (inventory.count > fields.maximum_fields or std.mem.allEqual(u8, &inventory.ordinal, 0)) return error.InvalidIndexSourceFamily;
        var raw: [layout_bytes]u8 = undefined;
        @memcpy(raw[0..4], "ISF1");
        std.mem.writeInt(u64, raw[4..12], self.segment, .little);
        std.mem.writeInt(u32, raw[12..16], self.documents, .little);
        @memcpy(raw[16..48], &self.physical);
        @memcpy(raw[48..80], &self.configuration);
        return raw;
    }
    pub fn metadata(self: Layout, buffer: *[maximum_layout_bytes]u8) ![]const u8 {
        const base = try self.encode();
        @memcpy(buffer[0..layout_bytes], &base);
        if (self.field_inventory) |inventory| {
            @memcpy(buffer[0..4], "ISF3");
            @memcpy(buffer[80..112], &inventory.ordinal);
            std.mem.writeInt(u32, buffer[112..116], inventory.count, .little);
            @memset(buffer[116..148], 0);
            if (self.transfer) |id| {
                if (std.mem.allEqual(u8, &id, 0)) return error.InvalidIndexSourceFamily;
                @memcpy(buffer[116..148], &id);
            }
            return buffer;
        }
        if (self.transfer) |transfer_id| {
            if (std.mem.allEqual(u8, &transfer_id, 0)) return error.InvalidIndexSourceFamily;
            @memcpy(buffer[0..4], "ISF2");
            @memcpy(buffer[layout_bytes..][0..32], &transfer_id);
            return buffer[0 .. layout_bytes + 32];
        }
        return buffer[0..layout_bytes];
    }
    fn decode(raw: []const u8) !Layout {
        const with_fields = raw.len == maximum_layout_bytes and std.mem.eql(u8, raw[0..4], "ISF3");
        const extended = raw.len == layout_bytes + 32 and std.mem.eql(u8, raw[0..4], "ISF2");
        if (!with_fields and !extended and (raw.len != layout_bytes or !std.mem.eql(u8, raw[0..4], "ISF1"))) return error.InvalidIndexSourceFamily;
        const value: Layout = .{ .segment = std.mem.readInt(u64, raw[4..12], .little), .documents = std.mem.readInt(u32, raw[12..16], .little), .physical = raw[16..48].*, .configuration = raw[48..80].*, .transfer = if (extended) raw[80..112].* else if (with_fields and !std.mem.allEqual(u8, raw[116..148], 0)) raw[116..148].* else null, .field_inventory = if (with_fields) .{ .ordinal = raw[80..112].*, .count = std.mem.readInt(u32, raw[112..116], .little) } else null };
        _ = try value.encode();
        if (value.transfer) |transfer_id| if (std.mem.allEqual(u8, &transfer_id, 0)) return error.InvalidIndexSourceFamily;
        return value;
    }
    pub fn count(self: Layout, first: u32) !u32 {
        if (first >= self.documents or first % cohort_capacity != 0) return error.InvalidIndexSourceFamily;
        return @min(cohort_capacity, self.documents - first);
    }
    /// Domain-separated layout/range identity. The native publisher establishes
    /// the retained physical/configuration identity once, not once per cohort.
    pub fn identity(self: Layout, first: u32) !Digest {
        const raw = try self.encode();
        var range: [8]u8 = undefined;
        std.mem.writeInt(u32, range[0..4], first, .little);
        std.mem.writeInt(u32, range[4..8], try self.count(first), .little);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        if (self.field_inventory) |inventory| {
            // Exact source ownership follows stable ordinals, independently of
            // changing field/posting generations or compiler configuration.
            hash.update("antfly/index-source-cohort/v2\x00");
            hash.update(&inventory.ordinal);
            hash.update(raw[12..16]);
        } else {
            hash.update("antfly/index-source-cohort/v1\x00");
            hash.update(&raw);
        }
        hash.update(&range);
        return hash.finalResult();
    }
    fn cohorts(self: Layout) u32 {
        return (self.documents - 1) / cohort_capacity + 1;
    }
};

pub const Leaf = struct {
    publication_id: Digest,
    root: Digest,
    first: u32,
    count: u32,
    fn encode(self: Leaf) ![leaf_bytes]u8 {
        if (self.count == 0 or self.count > cohort_capacity or self.first % cohort_capacity != 0 or std.mem.allEqual(u8, &self.publication_id, 0)) return error.InvalidIndexSourceFamily;
        var raw: [leaf_bytes]u8 = undefined;
        @memcpy(raw[0..4], "ISL1");
        @memcpy(raw[4..36], &self.publication_id);
        @memcpy(raw[36..68], &self.root);
        std.mem.writeInt(u32, raw[68..72], self.first, .little);
        std.mem.writeInt(u32, raw[72..76], self.count, .little);
        return raw;
    }
    pub fn decode(raw: []const u8) !Leaf {
        if (raw.len != leaf_bytes or !std.mem.eql(u8, raw[0..4], "ISL1")) return error.InvalidIndexSourceFamily;
        const leaf: Leaf = .{ .publication_id = raw[4..36].*, .root = raw[36..68].*, .first = std.mem.readInt(u32, raw[68..72], .little), .count = std.mem.readInt(u32, raw[72..76], .little) };
        _ = try leaf.encode();
        return leaf;
    }
};

/// Validate a freshly published leaf against its authenticated source map before
/// it can become a graph child. Caller stages receipt, map and graph atomically.
pub fn stageLeaf(writer: anytype, layout: Layout, decision: publication.Decision, first: u32, limits: graph.Limits) !Digest {
    errdefer writer.poison();
    try decision.requireRole(.source_leaf);
    try decision.require(decision.binding, .published);
    const summary = try maps.summary(writer, decision.binding, decision.root);
    if (summary.documents != try layout.count(first) or !std.mem.eql(u8, &summary.identity, &try layout.identity(first))) return error.InvalidIndexSourceFamily;
    const leaf: Leaf = .{ .publication_id = decision.binding.publication, .root = decision.root, .first = first, .count = summary.documents };
    const node = try graph.stage(writer, decision.binding.owner, .{ .kind = .leaf, .payload = &try leaf.encode() }, limits);
    try graph.bindResource(writer, decision.binding.owner, decision.binding.publication, node, limits);
    return node;
}

pub fn slot(segment: u64) Digest {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, segment, .little);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/index-source-family-slot/v1\x00");
    hash.update(&raw);
    return hash.finalResult();
}

/// Streaming bulk construction: constant scratch independent of corpus size,
/// with one direct edge per child and no historical-family parent chains.
pub const Builder = struct {
    owner: Digest,
    layout: Layout,
    next: u64 = 0,
    buckets: [maximum_levels + 1][graph.fanout]Digest = undefined,
    counts: [maximum_levels + 1]u8 = @splat(0),
    finished: bool = false,
    field_root: ?Digest = null,
    pub fn init(owner: Digest, layout: Layout) !Builder {
        if (layout.field_inventory != null) return error.InvalidIndexSourceFamily;
        _ = try layout.encode();
        return .{ .owner = owner, .layout = layout };
    }
    pub fn initWithFields(owner: Digest, layout: Layout, field_root: Digest) !Builder {
        if (layout.field_inventory == null) return error.InvalidIndexSourceFamily;
        _ = try layout.encode();
        return .{ .owner = owner, .layout = layout, .field_root = field_root };
    }
    fn push(self: *Builder, writer: anytype, initial: Digest, initial_level: usize, limits: graph.Limits) !void {
        var id = initial;
        var level = initial_level;
        while (true) : (level += 1) {
            if (level >= self.buckets.len) return error.InvalidIndexSourceFamily;
            self.buckets[level][self.counts[level]] = id;
            self.counts[level] += 1;
            if (self.counts[level] < graph.fanout) return;
            id = try graph.stage(writer, self.owner, .{ .kind = .branch, .level = @intCast(level + 1), .children = &self.buckets[level] }, limits);
            self.counts[level] = 0;
        }
    }
    pub fn append(self: *Builder, writer: anytype, decision: publication.Decision, limits: graph.Limits) !void {
        errdefer writer.poison();
        if (self.finished or self.next >= self.layout.documents or !std.mem.eql(u8, &self.owner, &decision.binding.owner)) return error.InvalidIndexSourceFamily;
        const first: u32 = @intCast(self.next);
        const id = try stageLeaf(writer, self.layout, decision, first, limits);
        try self.push(writer, id, 0, limits);
        self.next += try self.layout.count(first);
    }
    pub fn finish(self: *Builder, writer: anytype, limits: graph.Limits) !Digest {
        errdefer writer.poison();
        if (self.finished or self.next != self.layout.documents) return error.InvalidIndexSourceFamily;
        if (self.field_root) |root| {
            const directory = try fields.checkPublication(writer, self.owner, root);
            const inventory = self.layout.field_inventory orelse return error.InvalidIndexSourceFamily;
            if (!std.mem.eql(u8, &root, &self.layout.physical) or directory.documents != self.layout.documents or directory.fields != inventory.count or directory.stored == null or !std.mem.eql(u8, &directory.ordinal_inventory, &inventory.ordinal) or !std.mem.eql(u8, &directory.configuration, &self.layout.configuration)) return error.InvalidIndexSourceFamily;
        }
        for (0..self.buckets.len) |level| {
            const n = self.counts[level];
            if (n == 0) continue;
            var higher = false;
            for (self.counts[level + 1 ..]) |other| higher = higher or other != 0;
            if (!higher and n == 1) {
                const directory = self.buckets[level][0];
                var metadata: [maximum_layout_bytes]u8 = undefined;
                const children = [_]Digest{ directory, self.field_root orelse @splat(0) };
                const root = try graph.stage(writer, self.owner, .{ .kind = .family, .children = children[0..if (self.field_root != null) @as(usize, 2) else 1], .payload = try self.layout.metadata(&metadata) }, limits);
                self.finished = true;
                return root;
            }
            const branch = try graph.stage(writer, self.owner, .{ .kind = .branch, .level = @intCast(level + 1), .children = self.buckets[level][0..n] }, limits);
            self.counts[level] = 0;
            try self.push(writer, branch, level + 1, limits);
        }
        return error.InvalidIndexSourceFamily;
    }
};

pub const Selection = struct {
    layout: Layout,
    leaf: Leaf,
    /// Path includes family->directory and ends at the selected leaf. It is
    /// suitable for graph.replacePath while this actual family lease is held.
    path: [maximum_levels + 1]u8,
    path_length: u8,
};

pub fn select(reader: anytype, held: *const graph.HeldNode, document: u32) !Selection {
    const layout = try Layout.decode(try held.metadata());
    if (document >= layout.documents or held.node.kind() != .family or held.node.childCount() != if (layout.field_inventory != null) @as(usize, 2) else 1) return error.InvalidIndexSourceFamily;
    var result: Selection = .{ .layout = layout, .leaf = undefined, .path = undefined, .path_length = 1 };
    result.path[0] = 0;
    var node = try held.child(reader, 0);
    var capacity: u64 = 1;
    var level: u8 = 0;
    while (capacity < layout.cohorts()) : (level += 1) capacity *= graph.fanout;
    var offset: u64 = document / cohort_capacity;
    var remaining: u64 = layout.cohorts();
    while (level != 0) : (level -= 1) {
        if (node.node.kind() != .branch or node.node.level() != level) return error.InvalidIndexSourceFamily;
        capacity /= graph.fanout;
        const children = (remaining - 1) / capacity + 1;
        if (children != node.node.childCount()) return error.InvalidIndexSourceFamily;
        const ordinal: usize = @intCast(offset / capacity);
        result.path[result.path_length] = @intCast(ordinal);
        result.path_length += 1;
        node = try node.child(reader, ordinal);
        offset %= capacity;
        remaining = @min(capacity, remaining - ordinal * capacity);
    }
    if (node.node.kind() != .leaf) return error.InvalidIndexSourceFamily;
    result.leaf = try Leaf.decode(try node.metadata());
    const first = document / cohort_capacity * cohort_capacity;
    if (result.leaf.first != first or result.leaf.count != try layout.count(first)) return error.InvalidIndexSourceFamily;
    return result;
}
