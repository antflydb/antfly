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

//! Shared private contract for driving a fenced DAG activation scan.
const std = @import("std");
pub const authority = @import("../../graph/dag_authority.zig");
const A = std.mem.Allocator;
const Replacement = authority.Contributions.Replacement;

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

    pub fn beginScanCommand(self: ScanManifest, id: authority.Digest, owner_epoch: u64) !authority.SourceFenceCommand {
        const roots = try self.domainRoots(id);
        return .{
            .namespace = self.authority_namespace,
            .fence = self.fence,
            .action = .scan,
            .scan = .{ .domain = id, .owner_epoch = owner_epoch, .root_count = @intCast(roots.len) },
        };
    }

    fn domainRoots(self: ScanManifest, id: authority.Digest) ![]const ScanRoot {
        for (self.domains) |domain| {
            if (std.mem.eql(u8, &domain.id, &id)) return domain.roots;
        }
        return error.InvalidDagMutation;
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
    const roots = try manifest.domainRoots(scan.domain);
    if (!std.meta.eql(checkpoint.fence, manifest.fence) or
        !std.mem.eql(u8, &checkpoint.domain, &scan.domain) or
        checkpoint.root_count != @as(u32, @intCast(roots.len))) return error.GraphConstraintRevisionChanged;
    if (scan.owner_epoch != checkpoint.owner_epoch) return error.GraphConstraintOwnerChanged;
    var root_index: u32 = 0;
    var physical_after: ?[]const u8 = null;
    if (checkpoint.after) |after| {
        const position = authority.ScanPosition.decode(after) catch return error.InvalidDagAuthority;
        if (position.root >= roots.len) return error.InvalidDagAuthority;
        root_index = position.root;
        physical_after = position.after;
        if (position.after == null) root_index += 1;
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
    command.scan.?.root_count = null;
    try port.writePage(command);
    return page.exhausted and root_index + 1 == roots.len;
}
