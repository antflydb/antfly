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
const raft_catalog = @import("../raft/catalog.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const store_observer = @import("store_observer.zig");
const table_manager = @import("table_manager.zig");

pub const PlacementPlanner = struct {
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) PlacementPlanner {
        return .{ .alloc = alloc };
    }

    pub fn planLocalIntents(
        self: *const PlacementPlanner,
        manager: *table_manager.TableManager,
        local_node_id: u64,
        candidate_node_ids: []const u64,
    ) ![]raft_reconciler.PlacementIntent {
        const all = try self.planAllIntents(manager, candidate_node_ids);
        defer self.freeIntents(self.alloc, all);

        var out = std.ArrayListUnmanaged(raft_reconciler.PlacementIntent).empty;
        errdefer {
            for (out.items) |intent| freeIntent(self.alloc, intent);
            out.deinit(self.alloc);
        }
        for (all) |intent| {
            if (intent.record.local_node_id != local_node_id) continue;
            try out.append(self.alloc, .{
                .record = intent.record,
                .store_id = intent.store_id,
                .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try self.alloc.dupe(u64, intent.peer_node_ids),
            });
        }
        return try out.toOwnedSlice(self.alloc);
    }

    pub fn planAllIntents(
        self: *const PlacementPlanner,
        manager: *table_manager.TableManager,
        candidate_node_ids: []const u64,
    ) ![]raft_reconciler.PlacementIntent {
        return try self.planAllIntentsWithCurrentAndDomains(manager, candidate_node_ids, &.{}, &.{});
    }

    pub fn planAllIntentsWithCurrent(
        self: *const PlacementPlanner,
        manager: *table_manager.TableManager,
        candidate_node_ids: []const u64,
        current_intents: []const raft_reconciler.PlacementIntent,
    ) ![]raft_reconciler.PlacementIntent {
        return try self.planAllIntentsWithCurrentAndDomains(manager, candidate_node_ids, current_intents, &.{});
    }

    pub fn planAllIntentsWithCurrentAndDomains(
        self: *const PlacementPlanner,
        manager: *table_manager.TableManager,
        candidate_node_ids: []const u64,
        current_intents: []const raft_reconciler.PlacementIntent,
        candidate_domains: []const CandidateDomain,
    ) ![]raft_reconciler.PlacementIntent {
        if (candidate_node_ids.len == 0) return error.MissingCandidateNodes;

        const tables = try manager.listTables(self.alloc);
        defer manager.freeTables(self.alloc, tables);
        const ranges = try manager.listRanges(self.alloc);
        defer manager.freeRanges(self.alloc, ranges);
        std.mem.sort(table_manager.RangeRecord, ranges, current_intents, struct {
            fn lessThan(current: []const raft_reconciler.PlacementIntent, a: table_manager.RangeRecord, b: table_manager.RangeRecord) bool {
                const a_has_current = findCurrentIntent(current, a.group_id, null) != null;
                const b_has_current = findCurrentIntent(current, b.group_id, null) != null;
                if (a_has_current != b_has_current) return a_has_current;
                return a.group_id < b.group_id;
            }
        }.lessThan);

        var out = std.ArrayListUnmanaged(raft_reconciler.PlacementIntent).empty;
        errdefer {
            for (out.items) |intent| freeIntent(self.alloc, intent);
            out.deinit(self.alloc);
        }

        var load_by_node = std.AutoHashMapUnmanaged(u64, usize).empty;
        defer load_by_node.deinit(self.alloc);
        var pair_by_nodes = std.AutoHashMapUnmanaged(u128, usize).empty;
        defer pair_by_nodes.deinit(self.alloc);

        for (ranges) |range| {
            const table = findTable(tables, range.table_id) orelse return error.UnknownTable;
            const replica_count = @min(@as(usize, table.desired_replica_count), countEligibleCandidates(candidate_node_ids, candidate_domains, table.placement_role));
            if (replica_count == 0) continue;
            const has_current_group = findCurrentIntent(current_intents, range.group_id, null) != null;

            var selected = std.ArrayListUnmanaged(u64).empty;
            defer selected.deinit(self.alloc);
            const preserved = try collectCurrentPeers(
                self.alloc,
                current_intents,
                range.group_id,
                candidate_node_ids,
                candidate_domains,
                table.placement_role,
            );
            defer self.alloc.free(preserved);
            for (preserved) |node_id| {
                if (selected.items.len >= replica_count) break;
                try selected.append(self.alloc, node_id);
            }

            const start = @as(usize, @intCast(range.group_id % candidate_node_ids.len));
            const ordered = try orderCandidates(self.alloc, candidate_node_ids, candidate_domains, start, &load_by_node);
            defer self.alloc.free(ordered);
            while (selected.items.len < replica_count) {
                const node_id = chooseNextCandidate(ordered, selected.items, &pair_by_nodes, candidate_domains, table.placement_role) orelse break;
                if (containsNode(selected.items, node_id)) break;
                try selected.append(self.alloc, node_id);
            }

            const peers = try self.alloc.dupe(u64, selected.items);
            defer self.alloc.free(peers);
            for (peers) |node_id| {
                const entry = try load_by_node.getOrPut(self.alloc, node_id);
                if (!entry.found_existing) entry.value_ptr.* = 0;
                entry.value_ptr.* += 1;
            }
            for (peers, 0..) |left, i| {
                for (peers[i + 1 ..]) |right| {
                    const entry = try pair_by_nodes.getOrPut(self.alloc, pairKey(left, right));
                    if (!entry.found_existing) entry.value_ptr.* = 0;
                    entry.value_ptr.* += 1;
                }
            }

            for (peers, 0..) |node_id, replica_index| {
                const existing_intent = findCurrentIntent(current_intents, range.group_id, node_id);
                const bootstrap_mode: raft_catalog.ReplicaBootstrapMode = if (existing_intent) |existing|
                    existing.record.bootstrap_mode
                else if (!has_current_group)
                    .empty
                else
                    .persisted;
                try out.append(self.alloc, .{
                    .record = .{
                        .group_id = range.group_id,
                        .replica_id = @as(u64, @intCast(replica_index + 1)),
                        .local_node_id = node_id,
                        .bootstrap_mode = bootstrap_mode,
                    },
                    .store_id = chooseStoreIdForNode(current_intents, candidate_domains, range.group_id, node_id),
                    .peer_node_ids = if (peers.len == 0) &.{} else try self.alloc.dupe(u64, peers),
                });
            }
        }

        return try out.toOwnedSlice(self.alloc);
    }

    pub fn freeIntents(_: *const PlacementPlanner, alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
        for (intents) |intent| freeIntent(alloc, intent);
        alloc.free(intents);
    }
};

pub const CandidateDomain = struct {
    node_id: u64,
    store_id: u64 = 0,
    role: []const u8,
    failure_domain: []const u8,
    priority: u8 = 0,
    status_tag: store_observer.PlacementStatusTag = .preferred,
    available_bytes: u64 = 0,
    lease_pressure: u32 = 0,
    read_load: u32 = 0,
    write_load: u32 = 0,
    retain_current: bool = true,
};

fn findTable(records: []const table_manager.TableRecord, table_id: u64) ?table_manager.TableRecord {
    for (records) |record| {
        if (record.table_id == table_id) return record;
    }
    return null;
}

fn chooseStoreIdForNode(
    current_intents: []const raft_reconciler.PlacementIntent,
    candidate_domains: []const CandidateDomain,
    group_id: u64,
    node_id: u64,
) u64 {
    if (findCurrentIntent(current_intents, group_id, node_id)) |existing| {
        if (existing.store_id != 0 and nodeHasStoreCandidate(candidate_domains, node_id, existing.store_id)) {
            return existing.store_id;
        }
    }
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id and candidate.store_id != 0) return candidate.store_id;
    }
    return 0;
}

fn nodeHasStoreCandidate(candidate_domains: []const CandidateDomain, node_id: u64, store_id: u64) bool {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id and candidate.store_id == store_id) return true;
    }
    return false;
}

fn chooseNextCandidate(
    ordered: []const u64,
    selected: []const u64,
    pair_by_nodes: *const std.AutoHashMapUnmanaged(u128, usize),
    candidate_domains: []const CandidateDomain,
    placement_role: []const u8,
) ?u64 {
    var best_node: ?u64 = null;
    var best_domain_score: usize = std.math.maxInt(usize);
    var best_pair_score: usize = std.math.maxInt(usize);
    var best_order_index: usize = std.math.maxInt(usize);
    for (ordered, 0..) |node_id, order_index| {
        if (containsNode(selected, node_id)) continue;
        if (!candidateRoleMatches(candidate_domains, node_id, placement_role)) continue;
        const domain_score = domainScore(selected, node_id, candidate_domains);
        const pair_score = pairScore(selected, node_id, pair_by_nodes);
        if (best_node == null or
            domain_score < best_domain_score or
            (domain_score == best_domain_score and pair_score < best_pair_score) or
            (domain_score == best_domain_score and pair_score == best_pair_score and order_index < best_order_index))
        {
            best_node = node_id;
            best_domain_score = domain_score;
            best_pair_score = pair_score;
            best_order_index = order_index;
        }
    }
    return best_node;
}

fn pairScore(
    selected: []const u64,
    candidate_node_id: u64,
    pair_by_nodes: *const std.AutoHashMapUnmanaged(u128, usize),
) usize {
    var total: usize = 0;
    for (selected) |existing| {
        total += pair_by_nodes.get(pairKey(existing, candidate_node_id)) orelse 0;
    }
    return total;
}

fn domainScore(
    selected: []const u64,
    candidate_node_id: u64,
    candidate_domains: []const CandidateDomain,
) usize {
    const candidate_domain = findFailureDomain(candidate_domains, candidate_node_id);
    if (candidate_domain.len == 0) return 0;

    var total: usize = 0;
    for (selected) |existing| {
        if (std.mem.eql(u8, candidate_domain, findFailureDomain(candidate_domains, existing))) total += 1;
    }
    return total;
}

fn findFailureDomain(candidate_domains: []const CandidateDomain, node_id: u64) []const u8 {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.failure_domain;
    }
    return "";
}

fn countEligibleCandidates(
    candidate_node_ids: []const u64,
    candidate_domains: []const CandidateDomain,
    placement_role: []const u8,
) usize {
    var count: usize = 0;
    for (candidate_node_ids) |node_id| {
        if (candidateRoleMatches(candidate_domains, node_id, placement_role)) count += 1;
    }
    return count;
}

fn candidateRoleMatches(candidate_domains: []const CandidateDomain, node_id: u64, placement_role: []const u8) bool {
    if (placement_role.len == 0) return true;
    for (candidate_domains) |candidate| {
        if (candidate.node_id != node_id) continue;
        return table_manager.placementRoleCompatible(placement_role, candidate.role);
    }
    return table_manager.placementRoleCompatible(placement_role, "data");
}

fn pairKey(a: u64, b: u64) u128 {
    const lo = @min(a, b);
    const hi = @max(a, b);
    return (@as(u128, hi) << 64) | @as(u128, lo);
}

fn orderCandidates(
    alloc: std.mem.Allocator,
    candidate_node_ids: []const u64,
    candidate_domains: []const CandidateDomain,
    start: usize,
    load_by_node: *const std.AutoHashMapUnmanaged(u64, usize),
) ![]u64 {
    const Candidate = struct {
        node_id: u64,
        load: usize,
        priority: u8,
        available_bytes: u64,
        lease_pressure: u32,
        load_pressure: u32,
        rotated_index: usize,
    };

    const ranked = try alloc.alloc(Candidate, candidate_node_ids.len);
    defer alloc.free(ranked);
    for (candidate_node_ids, 0..) |node_id, i| {
        ranked[i] = .{
            .node_id = node_id,
            .load = load_byNode(load_by_node, node_id),
            .priority = candidatePriority(candidate_domains, node_id),
            .available_bytes = candidateAvailableBytes(candidate_domains, node_id),
            .lease_pressure = candidateLeasePressure(candidate_domains, node_id),
            .load_pressure = candidateLoadPressure(candidate_domains, node_id),
            .rotated_index = if (candidate_node_ids.len == 0) 0 else (i + candidate_node_ids.len - start) % candidate_node_ids.len,
        };
    }
    std.mem.sort(Candidate, ranked, {}, struct {
        fn lessThan(_: void, a: Candidate, b: Candidate) bool {
            if (a.load != b.load) return a.load < b.load;
            if (a.priority != b.priority) return a.priority < b.priority;
            if (a.lease_pressure != b.lease_pressure) return a.lease_pressure < b.lease_pressure;
            if (a.load_pressure != b.load_pressure) return a.load_pressure < b.load_pressure;
            if (a.available_bytes != b.available_bytes) return a.available_bytes > b.available_bytes;
            if (a.rotated_index != b.rotated_index) return a.rotated_index < b.rotated_index;
            return a.node_id < b.node_id;
        }
    }.lessThan);

    const out = try alloc.alloc(u64, candidate_node_ids.len);
    for (ranked, 0..) |candidate, i| out[i] = candidate.node_id;
    return out;
}

fn collectCurrentPeers(
    alloc: std.mem.Allocator,
    current_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    candidate_node_ids: []const u64,
    candidate_domains: []const CandidateDomain,
    placement_role: []const u8,
) ![]u64 {
    const ExistingPeer = struct {
        node_id: u64,
        replica_id: u64,
    };

    var peers = std.ArrayListUnmanaged(ExistingPeer).empty;
    errdefer peers.deinit(alloc);
    for (current_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (!containsNode(candidate_node_ids, intent.record.local_node_id)) continue;
        if (!candidateRoleMatches(candidate_domains, intent.record.local_node_id, placement_role)) continue;
        if (!candidateRetentionAllowed(candidate_domains, intent.record.local_node_id)) continue;
        var duplicate = false;
        for (peers.items) |peer| {
            if (peer.node_id == intent.record.local_node_id) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) continue;
        try peers.append(alloc, .{
            .node_id = intent.record.local_node_id,
            .replica_id = intent.record.replica_id,
        });
    }
    std.mem.sort(ExistingPeer, peers.items, {}, struct {
        fn lessThan(_: void, a: ExistingPeer, b: ExistingPeer) bool {
            if (a.replica_id == b.replica_id) return a.node_id < b.node_id;
            return a.replica_id < b.replica_id;
        }
    }.lessThan);

    const out = try alloc.alloc(u64, peers.items.len);
    for (peers.items, 0..) |peer, i| out[i] = peer.node_id;
    peers.deinit(alloc);
    return out;
}

fn findCurrentIntent(
    current_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    maybe_node_id: ?u64,
) ?raft_reconciler.PlacementIntent {
    for (current_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (maybe_node_id) |node_id| {
            if (intent.record.local_node_id != node_id) continue;
        }
        return intent;
    }
    return null;
}

fn load_byNode(load_by_node: *const std.AutoHashMapUnmanaged(u64, usize), node_id: u64) usize {
    return load_by_node.get(node_id) orelse 0;
}

fn candidatePriority(candidate_domains: []const CandidateDomain, node_id: u64) u8 {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.priority;
    }
    return 0;
}

fn candidateAvailableBytes(candidate_domains: []const CandidateDomain, node_id: u64) u64 {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.available_bytes;
    }
    return 0;
}

fn candidateLeasePressure(candidate_domains: []const CandidateDomain, node_id: u64) u32 {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.lease_pressure;
    }
    return 0;
}

fn candidateLoadPressure(candidate_domains: []const CandidateDomain, node_id: u64) u32 {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.read_load + candidate.write_load;
    }
    return 0;
}

fn candidateRetentionAllowed(candidate_domains: []const CandidateDomain, node_id: u64) bool {
    for (candidate_domains) |candidate| {
        if (candidate.node_id == node_id) return candidate.retain_current;
    }
    return true;
}

fn containsNode(nodes: []const u64, node_id: u64) bool {
    for (nodes) |existing| {
        if (existing == node_id) return true;
    }
    return false;
}

fn freeIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) void {
    if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
}

test "placement planner derives stable local intents from topology" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 7, .name = "docs", .desired_replica_count = 3 });
    try manager.upsertRange(.{
        .group_id = 701,
        .table_id = 7,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planLocalIntents(&manager, 2, &.{ 1, 2, 3 });
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 1), intents.len);
    try std.testing.expectEqual(@as(u64, 701), intents[0].record.group_id);
    try std.testing.expectEqual(@as(u64, 2), intents[0].record.local_node_id);
    try std.testing.expectEqual(@as(usize, 3), intents[0].peer_node_ids.len);
}

test "placement planner spreads multiple ranges across candidate nodes" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 8, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 801,
        .table_id = 8,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    try manager.upsertRange(.{
        .group_id = 802,
        .table_id = 8,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntents(&manager, &.{ 1, 2, 3 });
    defer planner.freeIntents(std.testing.allocator, intents);

    var counts = [_]usize{ 0, 0, 0 };
    for (intents) |intent| {
        if (intent.record.local_node_id >= 1 and intent.record.local_node_id <= 3) {
            counts[intent.record.local_node_id - 1] += 1;
        }
    }
    try std.testing.expect(counts[0] > 0);
    try std.testing.expect(counts[1] > 0);
    try std.testing.expect(counts[2] > 0);
}

test "placement planner preserves valid current peers before moving replicas" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 9, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 901,
        .table_id = 9,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 901, .replica_id = 1, .local_node_id = 1 }, .peer_node_ids = &.{ 1, 2 } },
        .{ .record = .{ .group_id = 901, .replica_id = 2, .local_node_id = 2 }, .peer_node_ids = &.{ 1, 2 } },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrent(&manager, &.{ 1, 2, 3 }, current[0..]);
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 2), intents.len);
    try std.testing.expectEqual(@as(u64, 1), intents[0].record.local_node_id);
    try std.testing.expectEqual(@as(u64, 2), intents[1].record.local_node_id);
}

test "placement planner anti-affinity rotates replica pairs across ranges" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 10, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 1001,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:g",
    });
    try manager.upsertRange(.{
        .group_id = 1002,
        .table_id = 10,
        .start_key = "doc:g",
        .end_key = "doc:n",
    });
    try manager.upsertRange(.{
        .group_id = 1003,
        .table_id = 10,
        .start_key = "doc:n",
        .end_key = "doc:z",
    });

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntents(&manager, &.{ 1, 2, 3 });
    defer planner.freeIntents(std.testing.allocator, intents);

    var pairs = std.AutoHashMapUnmanaged(u128, usize).empty;
    defer pairs.deinit(std.testing.allocator);
    for ([_]u64{ 1001, 1002, 1003 }) |group_id| {
        var peers = std.ArrayListUnmanaged(u64).empty;
        defer peers.deinit(std.testing.allocator);
        for (intents) |intent| {
            if (intent.record.group_id != group_id) continue;
            try peers.append(std.testing.allocator, intent.record.local_node_id);
        }
        try std.testing.expectEqual(@as(usize, 2), peers.items.len);
        const entry = try pairs.getOrPut(std.testing.allocator, pairKey(peers.items[0], peers.items[1]));
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), pairs.count());
}

test "placement planner prefers cross-domain peers for a range" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 11, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 1101,
        .table_id = 11,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .role = "data", .failure_domain = "rack-a" },
        .{ .node_id = 2, .role = "data", .failure_domain = "rack-a" },
        .{ .node_id = 3, .role = "data", .failure_domain = "rack-b" },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 1, 2, 3 }, &.{}, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 2), intents.len);
    try std.testing.expect(intents[0].record.local_node_id == 1 or intents[0].record.local_node_id == 2 or intents[1].record.local_node_id == 1 or intents[1].record.local_node_id == 2);
    try std.testing.expect(intents[0].record.local_node_id == 3 or intents[1].record.local_node_id == 3);
}

test "placement planner filters candidates by table placement role" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 12, .name = "hot_docs", .placement_role = "hot", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 1201,
        .table_id = 12,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .role = "hot", .failure_domain = "rack-a" },
        .{ .node_id = 2, .role = "cold", .failure_domain = "rack-b" },
        .{ .node_id = 3, .role = "hot", .failure_domain = "rack-c" },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 1, 2, 3 }, &.{}, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 2), intents.len);
    for (intents) |intent| try std.testing.expect(intent.record.local_node_id != 2);
}

test "placement planner supports explicit serving bulk archive classes" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 13, .name = "serving_docs", .placement_role = "serving", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 1301,
        .table_id = 13,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .role = "serving", .failure_domain = "rack-a" },
        .{ .node_id = 2, .role = "bulk", .failure_domain = "rack-b" },
        .{ .node_id = 3, .role = "serving", .failure_domain = "rack-c" },
        .{ .node_id = 4, .role = "archive", .failure_domain = "rack-d" },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 1, 2, 3, 4 }, &.{}, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 2), intents.len);
    for (intents) |intent| {
        try std.testing.expect(intent.record.local_node_id == 1 or intent.record.local_node_id == 3);
    }
}

test "placement planner rebalances away from overloaded current peers" {
    var manager = table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();

    try manager.upsertTable(.{ .table_id = 14, .name = "docs", .desired_replica_count = 2 });
    try manager.upsertRange(.{
        .group_id = 1401,
        .table_id = 14,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    const current = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 1401, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2 } },
        .{ .record = .{ .group_id = 1401, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .peer_node_ids = &.{ 1, 2 } },
    };
    const candidate_domains = [_]CandidateDomain{
        .{ .node_id = 1, .role = "data", .failure_domain = "rack-a", .priority = 2, .status_tag = .overloaded, .available_bytes = 950, .lease_pressure = 95, .read_load = 180, .write_load = 120, .retain_current = false },
        .{ .node_id = 2, .role = "data", .failure_domain = "rack-b", .priority = 0, .available_bytes = 850, .lease_pressure = 10, .read_load = 15, .write_load = 10 },
        .{ .node_id = 3, .role = "data", .failure_domain = "rack-c", .priority = 0, .available_bytes = 800, .lease_pressure = 12, .read_load = 18, .write_load = 10 },
    };

    var planner = PlacementPlanner.init(std.testing.allocator);
    const intents = try planner.planAllIntentsWithCurrentAndDomains(&manager, &.{ 1, 2, 3 }, &current, &candidate_domains);
    defer planner.freeIntents(std.testing.allocator, intents);

    try std.testing.expectEqual(@as(usize, 2), intents.len);
    var has_one = false;
    var has_two = false;
    var has_three = false;
    for (intents) |intent| {
        if (intent.record.local_node_id == 1) has_one = true;
        if (intent.record.local_node_id == 2) has_two = true;
        if (intent.record.local_node_id == 3) has_three = true;
    }
    try std.testing.expect(!has_one);
    try std.testing.expect(has_two);
    try std.testing.expect(has_three);
}
