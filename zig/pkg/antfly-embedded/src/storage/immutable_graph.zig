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

//! Durable immutable child-edge ownership for index families. Digests identify
//! metadata, never authorize primary/source reads. The native family publisher
//! owns the surrounding transaction and its actual publication reader cells.
const std = @import("std");
const publication = @import("index_publication.zig");
pub const Digest = publication.Digest;
const nodes = "family_node_v1:";
const counts = "family_edges_v1:";
const pending = "family_collect_v1:";
const roots = "family_root_v1:";
const retired = "family_retired_v1:";
const used = "family_root_publication_v1:";
const budgets = "family_budget_v1:";
const initialized = "family_initialized_v1:";
const bookmarks = "family_collect_after_v1:";
const resources = "family_resource_v1:";
pub const fanout = 64;
pub const maximum_payload_bytes = 4096;
pub const maximum_node_bytes = 8 + fanout * 32 + maximum_payload_bytes;
pub const Limits = struct { nodes: u64 = 1024 * 1024, bytes: u64 = 128 * 1024 * 1024 };
const unlimited: Limits = .{ .nodes = std.math.maxInt(u64), .bytes = std.math.maxInt(u64) };
// Reserve retirement representation at admission, including at the byte limit.
const root_charge: u64 = @as(u64, @max(roots.len, retired.len)) + 64 + 108;
const use_charge: u64 = used.len + 64 + 64;
/// Retained resource roots use IGN2 and the previously invalid kind zero.
/// Their native reader drain is checked before releasing descendant edges.
pub const Kind = enum(u8) { retained = 0, branch = 1, leaf = 2, family = 3 };
fn prefixKey(comptime prefix: []const u8, owner: Digest) [prefix.len + 32]u8 {
    var out: [prefix.len + 32]u8 = undefined;
    @memcpy(out[0..prefix.len], prefix);
    @memcpy(out[prefix.len..], &owner);
    return out;
}
fn key(comptime prefix: []const u8, owner: Digest, id: Digest) [prefix.len + 64]u8 {
    var out: [prefix.len + 64]u8 = undefined;
    @memcpy(out[0 .. prefix.len + 32], &prefixKey(prefix, owner));
    @memcpy(out[prefix.len + 32 ..], &id);
    return out;
}
fn digest(domain: []const u8, bytes: []const u8) Digest {
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    h.update(domain);
    h.update(bytes);
    return h.finalResult();
}
fn maybe(reader: anytype, name: []const u8) !?[]const u8 {
    return reader.get(name) catch |err| if (err == error.NotFound) return null else return err;
}
fn erase(writer: anytype, name: []const u8) !void {
    writer.delete(name) catch |err| if (err != error.NotFound) return err;
}
pub const Budget = struct {
    nodes: u64 = 0,
    bytes: u64 = 0,
    pub fn load(reader: anytype, owner: Digest) !Budget {
        const name = prefixKey(budgets, owner);
        const raw = (try maybe(reader, &name)) orelse return error.ImmutableGraphUninitialized;
        if (raw.len != 52 or !std.mem.eql(u8, raw[0..4], "IGB1") or !std.mem.eql(u8, raw[20..], &digest(&name, raw[0..20]))) return error.ImmutableGraphCorrupt;
        return .{ .nodes = std.mem.readInt(u64, raw[4..12], .little), .bytes = std.mem.readInt(u64, raw[12..20], .little) };
    }
    fn save(self: Budget, writer: anytype, owner: Digest, limits: Limits) !void {
        if (self.nodes > limits.nodes or self.bytes > limits.bytes) return error.ImmutableGraphFull;
        const name = prefixKey(budgets, owner);
        var raw: [52]u8 = undefined;
        @memcpy(raw[0..4], "IGB1");
        std.mem.writeInt(u64, raw[4..12], self.nodes, .little);
        std.mem.writeInt(u64, raw[12..20], self.bytes, .little);
        @memcpy(raw[20..], &digest(&name, raw[0..20]));
        try writer.put(&name, &raw);
    }
};
pub fn initialize(writer: anytype, owner: Digest) !void {
    errdefer writer.poison();
    if (std.mem.allEqual(u8, &owner, 0)) return error.InvalidIndexPublication;
    const witness = prefixKey(initialized, owner);
    if (try maybe(writer, &prefixKey(budgets, owner)) != null) {
        _ = try Budget.load(writer, owner);
        if (try maybe(writer, &witness) == null) return error.ImmutableGraphCorrupt;
        return;
    }
    if (try maybe(writer, &witness) != null) return error.ImmutableGraphCorrupt;
    inline for (.{ nodes, counts, pending, roots, retired, used, bookmarks, resources }) |prefix| {
        if (try writer.hasPrefix(&prefixKey(prefix, owner))) return error.ImmutableGraphCorrupt;
    }
    try (Budget{}).save(writer, owner, .{});
    try writer.put(&witness, "1");
}
pub const Node = struct {
    kind: Kind,
    /// Branch levels strictly descend to leaves. Family roots are always
    /// level 7 and cannot appear beneath another node or form parent chains.
    level: u8 = 0,
    children: []const Digest = &.{},
    payload: []const u8 = &.{},
    fn encode(self: Node, scratch: *[maximum_node_bytes]u8) ![]const u8 {
        if (self.children.len > fanout or self.payload.len > maximum_payload_bytes or
            (self.kind == .leaf and (self.children.len != 0 or self.level != 0)) or
            (self.kind != .leaf and self.children.len == 0) or
            ((self.kind == .branch or self.kind == .retained) and (self.level == 0 or self.level >= 7)) or
            (self.kind == .family and self.level != 0 and self.level != 7)) return error.InvalidImmutableNode;
        const out = scratch[0 .. 8 + self.children.len * 32 + self.payload.len];
        @memcpy(out[0..4], if (self.kind == .retained) "IGN2" else "IGN1");
        out[4] = @backingInt(self.kind) | (if (self.kind == .family) @as(u8, 7) else self.level) << 2;
        out[5] = @intCast(self.children.len);
        std.mem.writeInt(u16, out[6..8], @intCast(self.payload.len), .little);
        for (self.children, 0..) |child, i| @memcpy(out[8 + i * 32 ..][0..32], &child);
        @memcpy(out[8 + self.children.len * 32 ..], self.payload);
        return out;
    }
};
/// Owns one bounded decode, independent of later borrowed store reads.
pub const OwnedNode = struct {
    raw: [maximum_node_bytes]u8 = undefined,
    length: usize,
    pub fn kind(self: *const OwnedNode) Kind {
        return @fromBackingInt(@intCast(self.raw[4] & 3));
    }
    pub fn level(self: *const OwnedNode) u8 {
        return self.raw[4] >> 2;
    }
    pub fn childCount(self: *const OwnedNode) usize {
        return self.raw[5];
    }
    pub fn child(self: *const OwnedNode, index: usize) Digest {
        std.debug.assert(index < self.childCount());
        return self.raw[8 + index * 32 ..][0..32].*;
    }
    pub fn payload(self: *const OwnedNode) []const u8 {
        return self.raw[8 + self.childCount() * 32 .. self.length];
    }
};
pub fn loadNode(reader: anytype, owner: Digest, id: Digest) !OwnedNode {
    const raw = (try maybe(reader, &key(nodes, owner, id))) orelse return error.ImmutableGraphCorrupt;
    if (raw.len < 8 or raw.len > maximum_node_bytes) return error.ImmutableGraphCorrupt;
    const retained_root = std.mem.eql(u8, raw[0..4], "IGN2");
    if ((!retained_root and !std.mem.eql(u8, raw[0..4], "IGN1")) or
        (retained_root != (raw[4] & 3 == 0)) or raw[4] >> 2 > 7 or raw[5] > fanout or
        std.mem.readInt(u16, raw[6..8], .little) > maximum_payload_bytes or
        raw.len != 8 + @as(usize, raw[5]) * 32 + std.mem.readInt(u16, raw[6..8], .little) or
        (raw[4] & 3 == 2 and (raw[5] != 0 or raw[4] >> 2 != 0)) or
        (raw[4] & 3 != 2 and raw[5] == 0) or
        ((raw[4] & 3 == 1 or retained_root) and (raw[4] >> 2 == 0 or raw[4] >> 2 >= 7)) or
        (raw[4] & 3 == 3 and raw[4] >> 2 != 7) or
        !std.mem.eql(u8, &id, &digest(&prefixKey(nodes, owner), raw))) return error.ImmutableGraphCorrupt;
    var out: OwnedNode = .{ .length = raw.len };
    @memcpy(out.raw[0..raw.len], raw);
    return out;
}
fn readerSeal() void {}
/// Authenticated metadata navigation borrowing an actual family reader lease.
/// No constructor takes a caller-supplied root digest. The lease object and its
/// owner remain live through navigation; selecting a source leaf must acquire
/// that leaf's own native lease before releasing this parent. Metadata alone
/// never authorizes reading its external field/source resources.
pub const HeldNode = struct {
    id: Digest,
    node: OwnedNode,
    binding: publication.Binding,
    lease: *const publication.SharedReaders.Lease,
    seal_fn: *const fn () void,
    pub fn jsonStringify(_: HeldNode, _: anytype) error{WriteFailed}!void {
        return error.WriteFailed;
    }
    pub fn jsonParse(_: std.mem.Allocator, _: anytype, _: std.json.ParseOptions) error{UnexpectedToken}!HeldNode {
        return error.UnexpectedToken;
    }
    pub fn jsonParseFromValue(_: std.mem.Allocator, _: std.json.Value, _: std.json.ParseOptions) error{UnexpectedToken}!HeldNode {
        return error.UnexpectedToken;
    }
    fn require(self: *const HeldNode) !void {
        if (self.seal_fn != readerSeal) return error.IndexPublicationFenceMismatch;
        if (!std.meta.eql(try self.lease.binding(), self.binding)) return error.IndexPublicationFenceMismatch;
        if (self.node.length < 8 or self.node.length > maximum_node_bytes or
            !std.mem.eql(u8, &self.id, &digest(&prefixKey(nodes, self.binding.owner), self.node.raw[0..self.node.length]))) return error.ImmutableGraphCorrupt;
    }
    pub fn child(self: *const HeldNode, reader: anytype, ordinal: usize) !HeldNode {
        try self.require();
        if (ordinal >= self.node.childCount()) return error.InvalidImmutableNode;
        const id = self.node.child(ordinal);
        const node = try loadNode(reader, self.binding.owner, id);
        if (node.level() >= self.node.level()) return error.InvalidImmutableNode;
        return .{ .id = id, .node = node, .binding = self.binding, .lease = self.lease, .seal_fn = readerSeal };
    }
    pub fn metadata(self: *const HeldNode) ![]const u8 {
        try self.require();
        return self.node.payload();
    }
};
pub fn openHeld(reader: anytype, lease: *const publication.SharedReaders.Lease) !HeldNode {
    const binding = try lease.binding();
    const decision = try publication.heldReaderDecision(reader, binding, &lease.inner);
    try decision.requireRole(.family);
    const node = try loadNode(reader, binding.owner, decision.root);
    if (node.kind() != .family) return error.InvalidImmutableNode;
    return .{ .id = decision.root, .node = node, .binding = binding, .lease = lease, .seal_fn = readerSeal };
}

/// Physical field readers retain their own resource root. Pinning a logical
/// family here would keep superseded source claims alive for the lifetime of
/// unchanged postings, even after a source-only family refresh.
pub fn openHeldFields(reader: anytype, lease: *const publication.SharedReaders.Lease) !HeldNode {
    const binding = try lease.binding();
    const decision = try publication.heldReaderDecision(reader, binding, &lease.inner);
    try decision.requireRole(.fields);
    const node = try loadNode(reader, binding.owner, decision.root);
    if (node.kind() != .retained) return error.InvalidImmutableNode;
    return .{ .id = decision.root, .node = node, .binding = binding, .lease = lease, .seal_fn = readerSeal };
}
/// Rebuild only one authenticated ancestor path, preserving every other child
/// by its direct immutable identity. The caller publishes the result with a
/// fresh family receipt and root CAS in this same index transaction. A stale
/// CAS must abort that transaction, including newly staged nodes/edges.
pub const PathReplacement = struct { path: []const u8, replacement: Digest };
pub const maximum_path_replacements = 64;

pub fn replacePath(writer: anytype, root: *const HeldNode, path: []const u8, replacement: Digest, limits: Limits) !Digest {
    return replacePaths(writer, root, &.{.{ .path = path, .replacement = replacement }}, limits);
}

/// Atomically stage a bounded set of disjoint, lexicographically sorted paths
/// beneath an actual held root. Shared ancestors are read and copied once,
/// rather than producing intermediate family roots and orphan directory nodes.
/// Empty and identity batches perform no writes. Errors poison the enclosing
/// transaction; callers must publish the receipt and root CAS in that transaction.
pub fn replacePaths(writer: anytype, root: *const HeldNode, replacements: []const PathReplacement, limits: Limits) !Digest {
    return replacePathsWithMetadata(writer, root, replacements, null, limits);
}

/// Replace authenticated descendants and root metadata in the same path-copy
/// operation. Useful when a mutation must invalidate generation-local retry
/// provenance without producing an intermediate orphan family.
pub fn replacePathsWithMetadata(writer: anytype, root: *const HeldNode, replacements: []const PathReplacement, metadata: ?[]const u8, limits: Limits) !Digest {
    errdefer writer.poison();
    try root.require();
    if ((root.node.kind() != .family and root.node.kind() != .retained) or replacements.len > maximum_path_replacements) return error.InvalidImmutableNode;
    for (replacements, 0..) |item, i| {
        if (item.path.len == 0 or item.path.len > 7) return error.InvalidImmutableNode;
        if (i != 0) {
            const previous = replacements[i - 1].path;
            if (std.mem.order(u8, previous, item.path) != .lt or std.mem.startsWith(u8, item.path, previous)) return error.InvalidImmutableNode;
        }
    }
    return replaceDescendants(writer, root, replacements, 0, metadata, limits);
}

fn replaceDescendants(writer: anytype, cursor: *const HeldNode, replacements: []const PathReplacement, depth: usize, metadata: ?[]const u8, limits: Limits) anyerror!Digest {
    const payload = if (depth == 0) metadata orelse cursor.node.payload() else cursor.node.payload();
    if (replacements.len == 0 and std.mem.eql(u8, payload, cursor.node.payload())) return cursor.id;
    if (replacements.len != 0 and replacements[0].path.len == depth) {
        if (replacements.len != 1) return error.InvalidImmutableNode;
        return replacements[0].replacement;
    }
    var children: [fanout]Digest = undefined;
    for (children[0..cursor.node.childCount()], 0..) |*child, i| child.* = cursor.node.child(i);
    var changed = !std.mem.eql(u8, payload, cursor.node.payload());
    var first: usize = 0;
    while (first < replacements.len) {
        const ordinal = replacements[first].path[depth];
        var end = first + 1;
        while (end < replacements.len and replacements[end].path[depth] == ordinal) : (end += 1) {}
        const child = try cursor.child(writer, ordinal);
        const next = try replaceDescendants(writer, &child, replacements[first..end], depth + 1, null, limits);
        children[ordinal] = next;
        changed = changed or !std.mem.eql(u8, &next, &child.id);
        first = end;
    }
    if (!changed) return cursor.id;
    return stage(writer, cursor.binding.owner, .{
        .kind = cursor.node.kind(),
        .level = cursor.node.level(),
        .children = children[0..cursor.node.childCount()],
        .payload = payload,
    }, limits);
}
pub fn referenceCount(reader: anytype, owner: Digest, id: Digest) !u64 {
    const name = key(counts, owner, id);
    const raw = (try maybe(reader, &name)) orelse return error.ImmutableGraphCorrupt;
    if (raw.len != 40 or !std.mem.eql(u8, raw[8..], &digest(&name, raw[0..8]))) return error.ImmutableGraphCorrupt;
    return std.mem.readInt(u64, raw[0..8], .little);
}
/// A resource publication belongs to exactly one immutable leaf. Its reverse
/// edge is charged until external reclamation completes; receipt collection
/// and direct retirement consult it even after the leaf loses its last parent.
pub fn resourceNode(reader: anytype, owner: Digest, resource: Digest) !?Digest {
    const name = key(resources, owner, resource);
    const raw = (try maybe(reader, &name)) orelse return null;
    if (raw.len != 64 or !std.mem.eql(u8, raw[32..], &digest(&name, raw[0..32]))) return error.ImmutableGraphCorrupt;
    return raw[0..32].*;
}
pub fn bindResource(writer: anytype, owner: Digest, resource: Digest, node: Digest, limits: Limits) !void {
    errdefer writer.poison();
    if (try resourceNode(writer, owner, resource)) |existing| {
        if (!std.mem.eql(u8, &existing, &node)) return error.IndexPublicationConflict;
        return;
    }
    const kind = (try loadNode(writer, owner, node)).kind();
    if (kind != .leaf and kind != .retained) return error.InvalidImmutableNode;
    var budget = try Budget.load(writer, owner);
    budget.bytes = try std.math.add(u64, budget.bytes, resources.len + 128);
    try budget.save(writer, owner, limits);
    const name = key(resources, owner, resource);
    var raw: [64]u8 = undefined;
    @memcpy(raw[0..32], &node);
    @memcpy(raw[32..], &digest(&name, &node));
    try writer.put(&name, &raw);
}
/// Called by the leaf completion probe in the collector's transaction, after
/// native resource/claim cleanup has completed outside that transaction.
pub fn unbindResource(writer: anytype, owner: Digest, resource: Digest, node: Digest) !void {
    errdefer writer.poison();
    const existing = (try resourceNode(writer, owner, resource)) orelse return error.ImmutableGraphCorrupt;
    if (!std.mem.eql(u8, &existing, &node) or try referenceCount(writer, owner, node) != 0) return error.IndexPublicationStillActive;
    var budget = try Budget.load(writer, owner);
    budget.bytes = std.math.sub(u64, budget.bytes, resources.len + 128) catch return error.ImmutableGraphCorrupt;
    try budget.save(writer, owner, unlimited);
    try writer.delete(&key(resources, owner, resource));
}
fn saveCount(writer: anytype, owner: Digest, id: Digest, value: u64) !void {
    const name = key(counts, owner, id);
    var raw: [40]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], value, .little);
    @memcpy(raw[8..], &digest(&name, raw[0..8]));
    try writer.put(&name, &raw);
}
fn retain(writer: anytype, owner: Digest, id: Digest) !void {
    const before = try referenceCount(writer, owner, id);
    try saveCount(writer, owner, id, try std.math.add(u64, before, 1));
    if (before == 0) try erase(writer, &key(pending, owner, id));
}
fn release(writer: anytype, owner: Digest, id: Digest) !void {
    const before = try referenceCount(writer, owner, id);
    if (before == 0) return error.ImmutableGraphCorrupt;
    try saveCount(writer, owner, id, before - 1);
    if (before == 1) try writer.put(&key(pending, owner, id), "1");
}
fn nodeCharge(length: usize) u64 {
    // Reserve queue space for the complete node lifetime, even while retained.
    return length + nodes.len + 64 + counts.len + 64 + 40 + pending.len + 64 + 1;
}
/// Staging never retains its caller. A new node retains each direct child once;
/// reusing an existing node never walks or recharges that node's descendants.
pub fn stage(writer: anytype, owner: Digest, node: Node, limits: Limits) !Digest {
    errdefer writer.poison();
    var scratch: [maximum_node_bytes]u8 = undefined;
    const raw = try node.encode(&scratch);
    const id = digest(&prefixKey(nodes, owner), raw);
    if (try maybe(writer, &key(nodes, owner, id))) |existing| {
        if (!std.mem.eql(u8, raw, existing)) return error.ImmutableGraphCorrupt;
        _ = try referenceCount(writer, owner, id);
        return id;
    }
    var budget = try Budget.load(writer, owner);
    budget.nodes = try std.math.add(u64, budget.nodes, 1);
    budget.bytes = try std.math.add(u64, budget.bytes, nodeCharge(raw.len));
    try budget.save(writer, owner, limits);
    for (node.children) |child| {
        if (std.mem.eql(u8, &child, &id)) return error.InvalidImmutableNode;
        const loaded = try loadNode(writer, owner, child);
        if (loaded.level() >= raw[4] >> 2) return error.InvalidImmutableNode;
        try retain(writer, owner, child);
    }
    try writer.put(&key(nodes, owner, id), raw);
    try saveCount(writer, owner, id, 0);
    try writer.put(&key(pending, owner, id), "1");
    return id;
}
pub const Root = struct {
    generation: u64,
    publication: Digest,
    node: Digest,
    fn encode(self: Root, name: []const u8) [108]u8 {
        var raw: [108]u8 = undefined;
        @memcpy(raw[0..4], "IGR1");
        std.mem.writeInt(u64, raw[4..12], self.generation, .little);
        @memcpy(raw[12..44], &self.publication);
        @memcpy(raw[44..76], &self.node);
        @memcpy(raw[76..], &digest(name, raw[0..76]));
        return raw;
    }
    fn decode(name: []const u8, raw: []const u8) !Root {
        if (raw.len != 108 or !std.mem.eql(u8, raw[0..4], "IGR1") or
            !std.mem.eql(u8, raw[76..], &digest(name, raw[0..76])) or std.mem.readInt(u64, raw[4..12], .little) == 0 or
            std.mem.allEqual(u8, raw[12..44], 0)) return error.ImmutableGraphCorrupt;
        return .{ .generation = std.mem.readInt(u64, raw[4..12], .little), .publication = raw[12..44].*, .node = raw[44..76].* };
    }
};
pub fn loadRoot(reader: anytype, owner: Digest, slot: Digest) !?Root {
    const name = key(roots, owner, slot);
    return try Root.decode(&name, (try maybe(reader, &name)) orelse return null);
}
/// Publication receipt and family root are staged in the same index transaction.
/// Old roots keep their incoming edge until native retirement AND reader drain.
pub fn compareAndSwap(writer: anytype, owner: Digest, slot: Digest, expected: ?Root, publication_id: Digest, node: Digest, limits: Limits) !bool {
    errdefer writer.poison();
    const actual = try loadRoot(writer, owner, slot);
    if (!std.meta.eql(actual, expected)) return false;
    if (actual) |old| if (std.mem.eql(u8, &old.publication, &publication_id)) return error.IndexPublicationConflict;
    const binding: publication.Binding = .{ .owner = owner, .publication = publication_id };
    const receipt = (try publication.load(writer, binding)) orelse return error.IndexPublicationMissing;
    if (receipt.role != .family or receipt.state != .published or !std.mem.eql(u8, &receipt.root, &node)) return error.IndexPublicationFenceMismatch;
    if ((try loadNode(writer, owner, node)).kind() != .family) return error.InvalidImmutableNode;
    var budget = try Budget.load(writer, owner);
    const usage_key = key(used, owner, publication_id);
    if (try maybe(writer, &usage_key) != null) return error.IndexPublicationConflict;
    budget.bytes = try std.math.add(u64, budget.bytes, root_charge + use_charge);
    try budget.save(writer, owner, limits);
    try retain(writer, owner, node);
    if (actual) |old| {
        const name = key(retired, owner, old.publication);
        if (try maybe(writer, &name) != null) return error.ImmutableGraphCorrupt;
        try writer.put(&name, &old.encode(&name));
    }
    const name = key(roots, owner, slot);
    const next: Root = .{ .generation = if (actual) |old| try std.math.add(u64, old.generation, 1) else 1, .publication = publication_id, .node = node };
    try writer.put(&name, &next.encode(&name));
    var usage: [64]u8 = undefined;
    @memcpy(usage[0..32], &slot);
    @memcpy(usage[32..], &digest(&usage_key, &slot));
    try writer.put(&usage_key, &usage);
    return true;
}
pub fn removeRoot(writer: anytype, owner: Digest, slot: Digest, expected: Root) !bool {
    errdefer writer.poison();
    if (!std.meta.eql(try loadRoot(writer, owner, slot), @as(?Root, expected))) return false;
    _ = try Budget.load(writer, owner);
    const name = key(retired, owner, expected.publication);
    if (try maybe(writer, &name) != null) return error.ImmutableGraphCorrupt;
    try writer.put(&name, &expected.encode(&name));
    try writer.delete(&key(roots, owner, slot));
    return true;
}
pub fn releaseRetired(writer: anytype, owner: Digest, decision: publication.Decision, drained: publication.ReaderPins.Drained) !bool {
    errdefer writer.poison();
    try decision.requireRole(.family);
    try decision.require(.{ .owner = owner, .publication = decision.binding.publication }, .retired);
    try publication.ReaderPins.requireDrained(drained, decision.binding);
    const name = key(retired, owner, decision.binding.publication);
    const root = try Root.decode(&name, (try maybe(writer, &name)) orelse return false);
    if (!std.mem.eql(u8, &root.node, &decision.root)) return error.IndexPublicationFenceMismatch;
    const usage_key = key(used, owner, root.publication);
    const usage = (try maybe(writer, &usage_key)) orelse return error.ImmutableGraphCorrupt;
    if (usage.len != 64 or !std.mem.eql(u8, usage[32..], &digest(&usage_key, usage[0..32]))) return error.ImmutableGraphCorrupt;
    try release(writer, owner, root.node);
    try writer.delete(&name);
    try writer.delete(&usage_key);
    var budget = try Budget.load(writer, owner);
    budget.bytes = std.math.sub(u64, budget.bytes, root_charge + use_charge) catch return error.ImmutableGraphCorrupt;
    try budget.save(writer, owner, unlimited);
    return true;
}
/// Bounded archive enumeration. Own every decode before closing the cursor;
/// callers may then retire reader cells and mutate the graph in this txn.
pub fn retiredWindow(reader: anytype, owner: Digest, after: ?Digest, out: []Root) !usize {
    if (out.len == 0 or out.len > 128) return error.InvalidArgument;
    var cursor = try reader.openCursor();
    defer cursor.close();
    const prefix = prefixKey(retired, owner);
    var entry = try cursor.seekAtOrAfter(if (after) |id| &key(retired, owner, id) else &prefix);
    if (after) |id| if (entry) |item| {
        if (std.mem.eql(u8, item.key, &key(retired, owner, id))) entry = try cursor.next();
    };
    var count: usize = 0;
    while (entry) |item| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, &prefix)) break;
        if (item.key.len != prefix.len + 32) return error.ImmutableGraphCorrupt;
        const root = try Root.decode(item.key, item.value);
        if (!std.mem.eql(u8, &root.publication, item.key[prefix.len..])) return error.ImmutableGraphCorrupt;
        out[count] = root;
        count += 1;
        if (count == out.len) break;
    }
    return count;
}
/// Receipt GC must retain the native retirement decision until this root's
/// actual reader drain releases its graph edge. This is a retention check,
/// never a primary/source read grant.
pub fn ownsPublication(reader: anytype, owner: Digest, id: Digest) !bool {
    const name = key(used, owner, id);
    if (try maybe(reader, &name)) |raw| {
        if (raw.len != 64 or !std.mem.eql(u8, raw[32..], &digest(&name, raw[0..32]))) return error.ImmutableGraphCorrupt;
        return true;
    }
    return try maybe(reader, &key(retired, owner, id)) != null;
}
pub const Progress = struct { scanned: usize = 0, reclaimed: usize = 0, blocked: usize = 0 };
fn reclaimNode(writer: anytype, owner: Digest, id: Digest, node: *const OwnedNode) !void {
    for (0..node.childCount()) |i| try release(writer, owner, node.child(i));
    try writer.delete(&key(nodes, owner, id));
    try writer.delete(&key(counts, owner, id));
    try writer.delete(&key(pending, owner, id));
    var budget = try Budget.load(writer, owner);
    budget.nodes = std.math.sub(u64, budget.nodes, 1) catch return error.ImmutableGraphCorrupt;
    budget.bytes = std.math.sub(u64, budget.bytes, nodeCharge(node.length)) catch return error.ImmutableGraphCorrupt;
    try budget.save(writer, owner, unlimited);
}
/// Finish an external resource probe without advancing the discovery bookmark
/// again. Rechecks the zero edge count and leaf kind in the deletion txn.
pub fn collectLeaf(writer: anytype, owner: Digest, id: Digest, leaf_retirement: anytype) !bool {
    errdefer writer.poison();
    if (try maybe(writer, &key(nodes, owner, id)) == null) return false;
    if (try referenceCount(writer, owner, id) != 0) return false;
    const node = try loadNode(writer, owner, id);
    if (node.kind() != .leaf and node.kind() != .retained) return error.InvalidImmutableNode;
    if (!try leaf_retirement.retire(owner, id, node.payload())) return false;
    try reclaimNode(writer, owner, id, &node);
    return true;
}
/// One page of candidates, then no live cursor during mutation. The durable
/// bookmark rotates past blocked leaves, and collection never recurses. A leaf
/// retire adapter is an idempotent completion probe: native resource/claim
/// retirement finishes outside this index transaction before it returns true.
/// Returning false keeps that leaf and its charge durable.
pub fn collect(writer: anytype, owner: Digest, requested: usize, leaf_retirement: anytype) !Progress {
    errdefer writer.poison();
    const limit = @min(requested, 128);
    if (limit == 0) return .{};
    _ = try Budget.load(writer, owner);
    const bookmark = prefixKey(bookmarks, owner);
    var after: ?Digest = null;
    if (try maybe(writer, &bookmark)) |raw| {
        if (raw.len != 64 or !std.mem.eql(u8, raw[32..], &digest(&bookmark, raw[0..32]))) return error.ImmutableGraphCorrupt;
        after = raw[0..32].*;
    }
    var candidates: [128]Digest = undefined;
    var found: usize = 0;
    {
        var cursor = try writer.openCursor();
        defer cursor.close();
        cursor.setKeysOnly();
        const prefix = prefixKey(pending, owner);
        var entry = if (after) |id| try cursor.seekAtOrAfter(&key(pending, owner, id)) else try cursor.seekAtOrAfter(&prefix);
        if (after) |id| if (entry) |item| {
            if (std.mem.eql(u8, item.key, &key(pending, owner, id))) entry = try cursor.next();
        };
        while (entry) |item| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, item.key, &prefix)) break;
            if (item.key.len != prefix.len + 32) return error.ImmutableGraphCorrupt;
            candidates[found] = item.key[prefix.len..][0..32].*;
            found += 1;
            if (found == limit) break;
        }
    }
    if (found == 0) {
        try erase(writer, &bookmark);
        return .{};
    }
    var result: Progress = .{ .scanned = found };
    for (candidates[0..found]) |id| {
        if (try referenceCount(writer, owner, id) != 0) {
            try erase(writer, &key(pending, owner, id));
            continue;
        }
        const node = try loadNode(writer, owner, id);
        if ((node.kind() == .leaf or node.kind() == .retained) and !try leaf_retirement.retire(owner, id, node.payload())) {
            result.blocked += 1;
            continue;
        }
        try reclaimNode(writer, owner, id, &node);
        result.reclaimed += 1;
    }
    var raw: [64]u8 = undefined;
    @memcpy(raw[0..32], &candidates[found - 1]);
    @memcpy(raw[32..], &digest(&bookmark, raw[0..32]));
    try writer.put(&bookmark, &raw);
    return result;
}
