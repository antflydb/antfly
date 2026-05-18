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
const law = @import("law.zig");
const tensor = @import("tensor.zig");
const token = @import("token.zig");

pub const Edge = struct {
    from: []const u8,
    to: []const u8,
    provenance: []const u8,
};

pub const PathResult = struct {
    node: []u8,
    provenance: []u8,
    depth: u32,

    pub fn deinit(self: *PathResult, alloc: Allocator) void {
        alloc.free(self.node);
        alloc.free(self.provenance);
        self.* = undefined;
    }
};

pub fn deinitPathResults(alloc: Allocator, results: []PathResult) void {
    for (results) |*result| result.deinit(alloc);
    if (results.len > 0) alloc.free(results);
}

pub fn provenanceTokenAlloc(alloc: Allocator, labels: []const []const u8) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, labels);
}

pub fn provenanceLabelsAlloc(alloc: Allocator, provenance: []const u8) ![][]u8 {
    return try token.decodeTupleAlloc(alloc, provenance);
}

pub const BoundedReachabilityOptions = struct {
    target_nodes: []const []const u8 = &.{},
};

pub const ExecutionOptions = struct {
    semiring_enabled: bool = false,
    deduplicate: bool = true,
    max_depth: u32 = 3,
    max_results: u32 = 0,
    min_weight: f64 = 0,
    max_weight: f64 = 0,
    min_hops: bool = true,
};

pub const ExecutionRejectReason = enum {
    disabled,
    non_deduplicated,
    zero_depth,
    weighted_mode,
};

pub const ExecutionProof = union(enum) {
    proven,
    rejected: ExecutionRejectReason,

    pub fn safe(self: @This()) bool {
        return switch (self) {
            .proven => true,
            .rejected => false,
        };
    }
};

pub fn executionProof(options: ExecutionOptions) ExecutionProof {
    if (!options.semiring_enabled) return .{ .rejected = .disabled };
    if (!options.deduplicate) return .{ .rejected = .non_deduplicated };
    if (options.max_depth == 0) return .{ .rejected = .zero_depth };
    if (!options.min_hops) return .{ .rejected = .weighted_mode };
    return .proven;
}

const Pending = struct {
    provenance: []u8,
    depth: u32,

    fn deinit(self: *Pending, alloc: Allocator) void {
        alloc.free(self.provenance);
        self.* = undefined;
    }
};

const PendingMap = std.StringHashMapUnmanaged(Pending);

pub fn boundedReachabilityAlloc(
    alloc: Allocator,
    start_node: []const u8,
    edges: []const Edge,
    max_depth: u32,
) ![]PathResult {
    return try boundedReachabilityWithOptionsAlloc(alloc, start_node, edges, max_depth, .{});
}

pub fn boundedReachabilityWithOptionsAlloc(
    alloc: Allocator,
    start_node: []const u8,
    edges: []const Edge,
    max_depth: u32,
    options: BoundedReachabilityOptions,
) ![]PathResult {
    if (max_depth == 0) return try alloc.alloc(PathResult, 0);

    var results = PendingMap.empty;
    defer deinitPendingMap(alloc, &results);

    var frontier = PendingMap.empty;
    const identity = try law.identityAlloc(alloc, .provenance_semiring);
    defer alloc.free(identity);
    try mergePending(alloc, &frontier, start_node, identity, 0);

    var depth: u32 = 0;
    while (depth < max_depth) : (depth += 1) {
        var next = PendingMap.empty;
        var it = frontier.iterator();
        while (it.next()) |entry| {
            const from = entry.key_ptr.*;
            for (edges) |edge| {
                if (!std.mem.eql(u8, edge.from, from)) continue;
                const product = (try law.multiplyAlloc(alloc, .provenance_semiring, entry.value_ptr.provenance, edge.provenance)) orelse try law.identityAlloc(alloc, .provenance_semiring);
                defer alloc.free(product);
                try mergePending(alloc, &results, edge.to, product, depth + 1);
                try mergePending(alloc, &next, edge.to, product, depth + 1);
            }
        }
        deinitPendingMap(alloc, &frontier);
        frontier = next;
    }
    deinitPendingMap(alloc, &frontier);

    var out = std.ArrayListUnmanaged(PathResult).empty;
    errdefer {
        for (out.items) |*result| result.deinit(alloc);
        out.deinit(alloc);
    }
    var result_it = results.iterator();
    while (result_it.next()) |entry| {
        if (!targetNodeAllowed(options.target_nodes, entry.key_ptr.*)) continue;
        try out.append(alloc, .{
            .node = try alloc.dupe(u8, entry.key_ptr.*),
            .provenance = try alloc.dupe(u8, entry.value_ptr.provenance),
            .depth = entry.value_ptr.depth,
        });
    }
    std.mem.sort(PathResult, out.items, {}, lessPathResult);
    return try out.toOwnedSlice(alloc);
}

fn mergePending(alloc: Allocator, map: *PendingMap, node: []const u8, provenance: []const u8, depth: u32) !void {
    const key = try alloc.dupe(u8, node);
    var key_owned = true;
    errdefer if (key_owned) alloc.free(key);
    const gop = try map.getOrPut(alloc, key);
    if (gop.found_existing) {
        key_owned = false;
        alloc.free(key);
        const next = try tensor.mergeOneSlotValuesAlloc(alloc, .provenance_semiring, gop.value_ptr.provenance, provenance);
        alloc.free(gop.value_ptr.provenance);
        gop.value_ptr.provenance = next;
        gop.value_ptr.depth = @min(gop.value_ptr.depth, depth);
        return;
    }
    gop.key_ptr.* = key;
    gop.value_ptr.* = .{
        .provenance = try alloc.dupe(u8, provenance),
        .depth = depth,
    };
    key_owned = false;
}

fn deinitPendingMap(alloc: Allocator, map: *PendingMap) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        alloc.free(entry.key_ptr.*);
        entry.value_ptr.deinit(alloc);
    }
    map.deinit(alloc);
    map.* = .empty;
}

fn lessPathResult(_: void, lhs: PathResult, rhs: PathResult) bool {
    if (lhs.depth == rhs.depth) return std.mem.order(u8, lhs.node, rhs.node) == .lt;
    return lhs.depth < rhs.depth;
}

fn targetNodeAllowed(target_nodes: []const []const u8, node: []const u8) bool {
    if (target_nodes.len == 0) return true;
    for (target_nodes) |target_node| {
        if (std.mem.eql(u8, target_node, node)) return true;
    }
    return false;
}

test "bounded provenance path query composes edge facts with semiring multiplication" {
    const alloc = std.testing.allocator;
    const order_customer = try provenanceTokenAlloc(alloc, &.{"edge:order-customer"});
    defer alloc.free(order_customer);
    const customer_region = try provenanceTokenAlloc(alloc, &.{"edge:customer-region"});
    defer alloc.free(customer_region);
    const order_region = try provenanceTokenAlloc(alloc, &.{"edge:order-region"});
    defer alloc.free(order_region);
    const edges = [_]Edge{
        .{ .from = "order:o1", .to = "customer:c1", .provenance = order_customer },
        .{ .from = "customer:c1", .to = "region:r1", .provenance = customer_region },
        .{ .from = "order:o1", .to = "region:r2", .provenance = order_region },
    };

    const results = try boundedReachabilityAlloc(alloc, "order:o1", edges[0..], 2);
    defer deinitPathResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 3), results.len);
    try std.testing.expectEqualStrings("customer:c1", results[0].node);
    try std.testing.expectEqual(@as(u32, 1), results[0].depth);
    try std.testing.expectEqualStrings("region:r2", results[1].node);
    try std.testing.expectEqual(@as(u32, 1), results[1].depth);
    try std.testing.expectEqualStrings("region:r1", results[2].node);
    try std.testing.expectEqual(@as(u32, 2), results[2].depth);

    const provenance = try token.decodeTupleAlloc(alloc, results[2].provenance);
    defer {
        for (provenance) |part| alloc.free(part);
        alloc.free(provenance);
    }
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("edge:customer-region", provenance[0]);
    try std.testing.expectEqualStrings("edge:order-customer", provenance[1]);
}

test "bounded provenance execution proof gates exact semiring shapes" {
    try std.testing.expectEqual(ExecutionProof.proven, executionProof(.{ .semiring_enabled = true }));
    try std.testing.expectEqual(ExecutionProof{ .rejected = .disabled }, executionProof(.{}));
    try std.testing.expectEqual(ExecutionProof{ .rejected = .non_deduplicated }, executionProof(.{ .semiring_enabled = true, .deduplicate = false }));
    try std.testing.expectEqual(ExecutionProof{ .rejected = .zero_depth }, executionProof(.{ .semiring_enabled = true, .max_depth = 0 }));
    try std.testing.expectEqual(ExecutionProof.proven, executionProof(.{ .semiring_enabled = true, .min_weight = 0.5 }));
    try std.testing.expectEqual(ExecutionProof.proven, executionProof(.{ .semiring_enabled = true, .max_weight = 2.0 }));
    try std.testing.expectEqual(ExecutionProof.proven, executionProof(.{ .semiring_enabled = true, .max_results = 10 }));
    try std.testing.expectEqual(ExecutionProof{ .rejected = .weighted_mode }, executionProof(.{ .semiring_enabled = true, .min_hops = false }));
}

test "bounded provenance path query merges alternate paths by destination" {
    const alloc = std.testing.allocator;
    const left = try provenanceTokenAlloc(alloc, &.{"edge:left"});
    defer alloc.free(left);
    const right = try provenanceTokenAlloc(alloc, &.{"edge:right"});
    defer alloc.free(right);
    const edges = [_]Edge{
        .{ .from = "a", .to = "b", .provenance = left },
        .{ .from = "a", .to = "b", .provenance = right },
    };

    const results = try boundedReachabilityAlloc(alloc, "a", edges[0..], 1);
    defer deinitPathResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("b", results[0].node);
    const provenance = try token.decodeTupleAlloc(alloc, results[0].provenance);
    defer {
        for (provenance) |part| alloc.free(part);
        alloc.free(provenance);
    }
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("edge:left", provenance[0]);
    try std.testing.expectEqualStrings("edge:right", provenance[1]);
}

test "bounded provenance path query can constrain reachable targets" {
    const alloc = std.testing.allocator;
    const ab = try provenanceTokenAlloc(alloc, &.{"edge:ab"});
    defer alloc.free(ab);
    const bc = try provenanceTokenAlloc(alloc, &.{"edge:bc"});
    defer alloc.free(bc);
    const ad = try provenanceTokenAlloc(alloc, &.{"edge:ad"});
    defer alloc.free(ad);
    const edges = [_]Edge{
        .{ .from = "a", .to = "b", .provenance = ab },
        .{ .from = "b", .to = "c", .provenance = bc },
        .{ .from = "a", .to = "d", .provenance = ad },
    };

    const targets: []const []const u8 = &.{"c"};
    const results = try boundedReachabilityWithOptionsAlloc(alloc, "a", edges[0..], 2, .{ .target_nodes = targets });
    defer deinitPathResults(alloc, results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("c", results[0].node);
    try std.testing.expectEqual(@as(u32, 2), results[0].depth);

    const provenance = try token.decodeTupleAlloc(alloc, results[0].provenance);
    defer {
        for (provenance) |part| alloc.free(part);
        alloc.free(provenance);
    }
    try std.testing.expectEqual(@as(usize, 2), provenance.len);
    try std.testing.expectEqualStrings("edge:ab", provenance[0]);
    try std.testing.expectEqualStrings("edge:bc", provenance[1]);
}
