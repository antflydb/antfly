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

//! Convert final visible graph-record changes into authority replacements.
//! Callers must resolve source precedence and validate the physical write set
//! before using this adapter.
const std = @import("std");
const authority = @import("../../graph/dag_authority.zig");
const edges = @import("graph_edge_types.zig");
const keys = @import("../internal_keys.zig");
const A = std.mem.Allocator;
const Replacement = authority.Contributions.Replacement;

/// Result strings and arrays belong to the caller's preparation arena.
pub fn collect(
    alloc: A,
    index: []const u8,
    edge_type: []const u8,
    writes: []const edges.GraphEdgeWrite,
    deletes: []const edges.GraphEdgeDelete,
) ![]const Replacement {
    var result: std.ArrayList(Replacement) = .empty;
    errdefer {
        for (result.items) |replacement| alloc.free(replacement.targets);
        result.deinit(alloc);
    }
    for (writes) |edge| {
        if (!matches(edge, index, edge_type)) continue;
        try result.ensureUnusedCapacity(alloc, 1);
        const targets = try alloc.alloc([]const u8, 1);
        targets[0] = edge.target;
        result.appendAssumeCapacity(.{ .owner = owner(edge), .source = edge.source, .targets = targets });
    }
    for (deletes) |edge| {
        if (!matches(edge, index, edge_type)) continue;
        try result.append(alloc, .{ .owner = owner(edge), .source = edge.source, .targets = &.{} });
    }
    std.mem.sort(Replacement, result.items, {}, lessThan);
    for (result.items, 0..) |replacement, i| {
        if (i != 0 and !lessThan({}, result.items[i - 1], replacement)) return error.InvalidDagMutation;
    }
    return result.toOwnedSlice(alloc);
}

pub const ScanRequest = struct {
    namespace: [24]u8,
    fence: authority.SourceFence,
    index_name: []const u8,
    edge_type: []const u8,
    generation: u64,
    after: ?[]const u8 = null,
};

pub const ScanPage = struct {
    next: ?[]const u8,
    exhausted: bool,
    replacements: []const Replacement,
};

pub const ScanRoot = struct {
    namespace: [24]u8,
    index_name: []const u8,
    edge_type: []const u8,
    generation: u64,
};

pub const ScanDomain = struct {
    id: authority.Digest,
    roots: []const ScanRoot,
};

pub const ScanManifest = struct {
    domains: []const ScanDomain,
    authority_namespace: [24]u8,
    fence: authority.SourceFence,

    /// Borrow the immutable source list in physical traversal order. The
    /// digest excludes page-local cursors and binds the catalog generations.
    pub fn init(
        domains: []const ScanDomain,
        authority_namespace: [24]u8,
        activation: authority.Digest,
    ) !ScanManifest {
        if (domains.len == 0) return error.InvalidDagMutation;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/dag-activation/source-manifest/v1");
        hash.update(&activation);
        hash.update(&authority_namespace);
        hashNumber(&hash, domains.len);
        for (domains, 0..) |domain, index| {
            if (index != 0 and std.mem.order(u8, &domains[index - 1].id, &domain.id) != .lt)
                return error.InvalidDagMutation;
            if (domain.roots.len == 0 or domain.roots.len > std.math.maxInt(u32))
                return error.InvalidDagMutation;
            hash.update(&domain.id);
            hashNumber(&hash, domain.roots.len);
            for (domain.roots) |root| {
                if (root.index_name.len == 0 or root.edge_type.len == 0 or root.generation == 0)
                    return error.InvalidDagMutation;
                hash.update(&root.namespace);
                hashNumber(&hash, root.generation);
                for ([_][]const u8{ root.index_name, root.edge_type }) |name| {
                    hashNumber(&hash, name.len);
                    hash.update(name);
                }
            }
        }
        return .{
            .domains = domains,
            .authority_namespace = authority_namespace,
            .fence = .{ .activation = activation, .snapshot = hash.finalResult() },
        };
    }
};

fn hashNumber(hash: *std.crypto.hash.sha2.Sha256, number: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, number, .big);
    hash.update(&bytes);
}

/// Advance a single bounded page on the immutable root list for this fence.
/// The port reads the pinned source and commits the authority command in order.
/// Read allocations belong to the caller's scratch arena. A completed-root
/// marker is committed even for an empty root, so resume needs no local state.
pub fn advanceScan(
    alloc: A,
    port: anytype,
    manifest: ScanManifest,
    authority_command: authority.SourceFenceCommand,
    checkpoint: authority.Contributions.ScanCheckpoint,
) !bool {
    var command = authority_command;
    const scan = command.scan orelse return error.InvalidDagMutation;
    if (command.action != .scan or scan.page != null) return error.InvalidDagMutation;
    if (!std.meta.eql(manifest.fence, command.fence) or
        !std.mem.eql(u8, &manifest.authority_namespace, &command.namespace)) return error.GraphConstraintRevisionChanged;
    const roots = for (manifest.domains) |domain| {
        if (std.mem.eql(u8, &domain.id, &scan.domain)) break domain.roots;
    } else return error.InvalidDagMutation;
    if (scan.owner_epoch != checkpoint.owner_epoch) return error.GraphConstraintOwnerChanged;
    var root_index: u32 = 0;
    var physical_after: ?[]const u8 = null;
    if (checkpoint.after) |after| {
        if (after.len < 5) return error.InvalidDagAuthority;
        root_index = std.mem.readInt(u32, after[0..4], .big);
        if (root_index >= roots.len) return error.InvalidDagAuthority;
        switch (after[4]) {
            0 => {
                if (after.len == 5) return error.InvalidDagAuthority;
                physical_after = after[5..];
            },
            1 => {
                if (after.len != 5) return error.InvalidDagAuthority;
                root_index += 1;
            },
            else => return error.InvalidDagAuthority,
        }
    }
    if (root_index == roots.len) return true;

    const root = roots[root_index];
    const source: ScanRequest = .{
        .namespace = root.namespace,
        .fence = manifest.fence,
        .index_name = root.index_name,
        .edge_type = root.edge_type,
        .generation = root.generation,
        .after = physical_after,
    };
    const page = try port.readSource(alloc, source);
    if (page.next) |next| {
        if (next.len == 0 or (physical_after != null and std.mem.order(u8, physical_after.?, next) != .lt))
            return error.InvalidDagMutation;
    } else if (!page.exhausted or page.replacements.len != 0) return error.InvalidDagMutation;

    var prefix: [5]u8 = undefined;
    std.mem.writeInt(u32, prefix[0..4], root_index, .big);
    prefix[4] = @intFromBool(page.exhausted);
    const next = try std.mem.concat(alloc, u8, &.{ &prefix, if (page.exhausted) "" else page.next.? });
    defer alloc.free(next);
    command.scan.?.page = .{
        .after = checkpoint.after,
        .next = next,
        .replacements = page.replacements,
    };
    try port.writePage(command);
    return page.exhausted and root_index + 1 == roots.len;
}

/// Read accepted physical graph artifacts from a single fenced root. The
/// caller supplies the currently admitted catalog generation and owns the arena.
/// The cursor advances over skipped rows as well as matching relationships.
pub fn scanAccepted(alloc: A, txn: anytype, request: ScanRequest, start: []const u8, end: []const u8) !ScanPage {
    if (request.index_name.len == 0 or request.edge_type.len == 0 or request.generation == 0)
        return error.InvalidDagMutation;
    const current = (try authority.SourceFence.read(txn)) orelse return error.GraphConstraintNotReady;
    if (!std.meta.eql(current, request.fence)) return error.GraphConstraintAttemptConflict;

    const lower = try keys.documentRangeLowerAlloc(alloc, start);
    const upper = if (end.len != 0)
        try keys.documentRangeLowerAlloc(alloc, end)
    else
        (try keys.documentRangeUpperAlloc(alloc, "")) orelse return error.InvalidDagAuthority;
    if (request.after) |after| {
        if (std.mem.order(u8, after, lower) == .lt or std.mem.order(u8, after, upper) != .lt)
            return error.InvalidDagMutation;
    }

    const max_rows = 256;
    const max_bytes = 512 * 1024;
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    cursor.setUpperBound(upper);
    var entry = try cursor.seekAtOrAfter(request.after orelse lower);
    if (entry) |row| {
        if (request.after) |after| if (std.mem.eql(u8, after, row.key)) {
            entry = try cursor.next();
        };
    }

    var result: std.ArrayList(Replacement) = .empty;
    var next: ?[]const u8 = null;
    var visited: usize = 0;
    var bytes: usize = 0;
    while (entry) |row| : (entry = try cursor.next()) {
        if (std.mem.order(u8, row.key, upper) != .lt) {
            entry = null;
            break;
        }
        const extent = row.key.len +| (if (keys.isGraphEdgeArtifactKey(row.key)) row.value.len else 0);
        if (visited == max_rows or (visited != 0 and extent > max_bytes - bytes)) break;
        if (extent > max_bytes) return error.GraphConstraintCapacity;
        visited += 1;
        bytes += extent;
        if (next) |previous| alloc.free(previous);
        next = try alloc.dupe(u8, row.key);
        if (try readAccepted(alloc, request, row.key, row.value)) |replacement|
            try result.append(alloc, replacement);
    }
    std.mem.sort(Replacement, result.items, {}, lessThan);
    return .{ .next = next, .exhausted = entry == null, .replacements = try result.toOwnedSlice(alloc) };
}

fn readAccepted(alloc: A, request: ScanRequest, key: []const u8, value: []const u8) !?Replacement {
    if (!keys.matchesGraphEdgeIndexName(key, request.index_name)) return null;
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const parsed = (try keys.parseGraphEdgeArtifactKeyAlloc(scratch.allocator(), key)) orelse return error.InvalidDagAuthority;
    if (!std.mem.eql(u8, parsed.edge_type, request.edge_type)) return null;
    const codec = @import("enrichment/artifact_codec.zig");
    const record = try codec.decodeGraphEdgeBorrowed(value);
    if (record.generation == 0 or codec.isPortableUnboundGraphEdge(value)) return error.GraphConstraintNotReady;
    if (record.generation != request.generation) return null;
    const identity: edges.GraphEdgeWrite = .{
        .index_name = parsed.index_name,
        .edge_type = parsed.edge_type,
        .source = parsed.source_node orelse parsed.doc_key,
        .target = parsed.target_doc_key,
        .owner = parsed.doc_key,
        .edge_id = parsed.edge_id,
    };
    const source = try alloc.dupe(u8, identity.source);
    const targets = try alloc.alloc([]const u8, 1);
    targets[0] = try alloc.dupe(u8, identity.target);
    return .{ .owner = owner(identity), .source = source, .targets = targets };
}

fn matches(edge: anytype, index: []const u8, edge_type: []const u8) bool {
    return std.mem.eql(u8, index, edge.index_name) and std.mem.eql(u8, edge_type, edge.edge_type);
}

fn owner(edge: anytype) authority.Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/dag-visible-relationship/v1");
    // A named relationship can have several different endpoint rows over its
    // lifetime. Each row has its own membership, so retiring the old endpoint
    // and installing the new one are distinct records in the final delta.
    for ([_][]const u8{ edge.producingDocument(), edge.target, edge.edge_id }) |part| {
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, part.len, .little);
        hash.update(&size);
        hash.update(part);
    }
    return hash.finalResult();
}

fn lessThan(_: void, left: Replacement, right: Replacement) bool {
    return switch (std.mem.order(u8, &left.owner, &right.owner)) {
        .lt => true,
        .gt => false,
        .eq => std.mem.order(u8, left.source, right.source) == .lt,
    };
}

test "DAG final graph effects retain relationship identity across publication and retirement" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const writes: []const edges.GraphEdgeWrite = &.{
        .{ .index_name = "g", .edge_type = "R", .source = "a", .target = "b", .owner = "first" },
        .{ .index_name = "g", .edge_type = "R", .source = "a", .target = "b", .owner_document = "second", .edge_id = "x" },
        .{ .index_name = "g", .edge_type = "S", .source = "b", .target = "a" },
    };
    const added = try collect(alloc, "g", "R", writes, &.{});
    try std.testing.expectEqual(@as(usize, 2), added.len);
    try std.testing.expect(!std.mem.eql(u8, &added[0].owner, &added[1].owner));
    const delete: edges.GraphEdgeDelete = .{ .index_name = "g", .edge_type = "R", .source = "a", .target = "b", .owner = "first" };
    const removed = try collect(alloc, "g", "R", &.{}, &.{delete});
    try std.testing.expectEqual(@as(usize, 1), removed.len);
    try std.testing.expectEqualDeep(owner(writes[0]), removed[0].owner);
    try std.testing.expectEqual(@as(usize, 0), removed[0].targets.len);
    try std.testing.expectError(error.InvalidDagMutation, collect(alloc, "g", "R", writes, &.{delete}));
}
