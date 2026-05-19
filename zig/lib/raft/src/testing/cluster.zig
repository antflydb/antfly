// Copyright 2026 Antfly, Inc.
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
const core = @import("../core/mod.zig");
const message = @import("../core/message.zig");

fn edgeKey(from: core.types.NodeId, to: core.types.NodeId) u128 {
    return (@as(u128, from) << 64) | to;
}

pub const Cluster = struct {
    pub const default_election_tick: u32 = 3;
    pub const default_heartbeat_tick: u32 = 1;
    pub const Options = struct {
        election_tick: u32 = default_election_tick,
        heartbeat_tick: u32 = default_heartbeat_tick,
        random_seed: ?u64 = null,
        max_size_per_msg: usize = std.math.maxInt(usize),
        max_committed_size_per_ready: usize = 0,
        max_inflight_msgs: u32 = 256,
        max_inflight_bytes: usize = 0,
        max_uncommitted_entries_size: usize = std.math.maxInt(usize),
        async_storage_writes: bool = false,
        check_quorum: bool = true,
        pre_vote: bool = true,
        step_down_on_removal: bool = false,
        disable_proposal_forwarding: bool = false,
        disable_conf_change_validation: bool = false,
        read_only_option: core.ReadOnlyOption = .safe,
        initial_conf_state: ?core.types.ConfState = null,
        applied_indexes: ?[]const core.types.Index = null,
    };

    alloc: std.mem.Allocator,
    peer_ids: []core.types.NodeId,
    nodes: []core.RawNode,
    stores: []core.MemoryStorage,
    election_tick: u32 = default_election_tick,
    heartbeat_tick: u32 = default_heartbeat_tick,
    random_seed: ?u64 = null,
    max_size_per_msg: usize = std.math.maxInt(usize),
    max_committed_size_per_ready: usize = 0,
    max_inflight_msgs: u32 = 256,
    max_inflight_bytes: usize = 0,
    max_uncommitted_entries_size: usize = std.math.maxInt(usize),
    async_storage_writes: bool = false,
    check_quorum: bool = true,
    pre_vote: bool = true,
    step_down_on_removal: bool = false,
    disable_proposal_forwarding: bool = false,
    disable_conf_change_validation: bool = false,
    read_only_option: core.ReadOnlyOption = .safe,
    node_pre_vote: []bool,
    applied_indexes: []core.types.Index,
    initial_conf_state: core.types.ConfState = .{},
    committed: []std.ArrayListUnmanaged(core.types.Entry),
    read_states: []std.ArrayListUnmanaged(core.types.ReadState),
    network: std.ArrayListUnmanaged(message.Message) = .empty,
    blocked_links: std.AutoHashMapUnmanaged(u128, void) = .empty,
    active_nodes: std.AutoHashMapUnmanaged(core.types.NodeId, void) = .empty,

    pub fn init(alloc: std.mem.Allocator, peer_ids: []const core.types.NodeId) !Cluster {
        return try initWithOptions(alloc, peer_ids, .{});
    }

    pub fn initWithOptions(alloc: std.mem.Allocator, peer_ids: []const core.types.NodeId, options: Options) !Cluster {
        const owned_peer_ids = try alloc.dupe(core.types.NodeId, peer_ids);
        errdefer alloc.free(owned_peer_ids);

        var stores = try alloc.alloc(core.MemoryStorage, peer_ids.len);
        errdefer alloc.free(stores);
        for (stores) |*store| store.* = core.MemoryStorage.init(alloc);
        errdefer for (stores) |*store| store.deinit();

        var initial_conf_state = if (options.initial_conf_state) |conf_state| blk: {
            break :blk try conf_state.clone(alloc);
        } else blk: {
            var default_conf_state = core.types.ConfState{
                .voters = try alloc.dupe(core.types.NodeId, peer_ids),
            };
            defer default_conf_state.deinit(alloc);
            break :blk try default_conf_state.clone(alloc);
        };
        errdefer initial_conf_state.deinit(alloc);

        for (stores) |*store| {
            try store.seedConfState(initial_conf_state);
        }

        const committed = try alloc.alloc(std.ArrayListUnmanaged(core.types.Entry), peer_ids.len);
        errdefer alloc.free(committed);
        for (committed) |*entries| entries.* = .empty;
        errdefer for (committed) |*entries| {
            for (entries.items) |*entry| entry.deinit(alloc);
            entries.deinit(alloc);
        };

        const read_states = try alloc.alloc(std.ArrayListUnmanaged(core.types.ReadState), peer_ids.len);
        errdefer alloc.free(read_states);
        for (read_states) |*states| states.* = .empty;
        errdefer for (read_states) |*states| {
            for (states.items) |*read_state| read_state.deinit(alloc);
            states.deinit(alloc);
        };

        var nodes = try alloc.alloc(core.RawNode, peer_ids.len);
        errdefer alloc.free(nodes);

        const node_pre_vote = try alloc.alloc(bool, peer_ids.len);
        errdefer alloc.free(node_pre_vote);
        @memset(node_pre_vote, options.pre_vote);

        const applied_indexes = try alloc.alloc(core.types.Index, peer_ids.len);
        errdefer alloc.free(applied_indexes);
        if (options.applied_indexes) |src| {
            std.debug.assert(src.len == peer_ids.len);
            @memcpy(applied_indexes, src);
        } else {
            @memset(applied_indexes, 0);
        }

        var initialized: usize = 0;
        errdefer {
            for (nodes[0..initialized]) |*raw_node| raw_node.deinit();
        }

        for (peer_ids, 0..) |id, i| {
            nodes[i] = try initNode(alloc, owned_peer_ids, options, id, stores[i].storage());
            initialized += 1;
        }

        var active_nodes = std.AutoHashMapUnmanaged(core.types.NodeId, void){};
        errdefer active_nodes.deinit(alloc);
        for (peer_ids) |id| {
            if (!confStateContainsNode(initial_conf_state, id)) continue;
            try active_nodes.put(alloc, id, {});
        }

        return .{
            .alloc = alloc,
            .peer_ids = owned_peer_ids,
            .nodes = nodes,
            .stores = stores,
            .election_tick = options.election_tick,
            .heartbeat_tick = options.heartbeat_tick,
            .random_seed = options.random_seed,
            .max_size_per_msg = options.max_size_per_msg,
            .max_committed_size_per_ready = options.max_committed_size_per_ready,
            .max_inflight_msgs = options.max_inflight_msgs,
            .max_inflight_bytes = options.max_inflight_bytes,
            .max_uncommitted_entries_size = options.max_uncommitted_entries_size,
            .async_storage_writes = options.async_storage_writes,
            .check_quorum = options.check_quorum,
            .pre_vote = options.pre_vote,
            .step_down_on_removal = options.step_down_on_removal,
            .disable_proposal_forwarding = options.disable_proposal_forwarding,
            .disable_conf_change_validation = options.disable_conf_change_validation,
            .read_only_option = options.read_only_option,
            .node_pre_vote = node_pre_vote,
            .applied_indexes = applied_indexes,
            .initial_conf_state = initial_conf_state,
            .committed = committed,
            .read_states = read_states,
            .active_nodes = active_nodes,
        };
    }

    pub fn deinit(self: *Cluster) void {
        for (self.network.items) |*msg| msg.deinit(self.alloc);
        self.network.deinit(self.alloc);
        self.blocked_links.deinit(self.alloc);
        self.active_nodes.deinit(self.alloc);
        for (self.committed) |*entries| {
            for (entries.items) |*entry| entry.deinit(self.alloc);
            entries.deinit(self.alloc);
        }
        for (self.read_states) |*states| {
            for (states.items) |*read_state| read_state.deinit(self.alloc);
            states.deinit(self.alloc);
        }
        self.initial_conf_state.deinit(self.alloc);
        for (self.nodes) |*raw_node| raw_node.deinit();
        for (self.stores) |*store| store.deinit();
        self.alloc.free(self.node_pre_vote);
        self.alloc.free(self.applied_indexes);
        self.alloc.free(self.read_states);
        self.alloc.free(self.committed);
        self.alloc.free(self.nodes);
        self.alloc.free(self.stores);
        self.alloc.free(self.peer_ids);
        self.* = undefined;
    }

    pub fn nodeIndex(self: *const Cluster, id: core.types.NodeId) ?usize {
        for (self.peer_ids, 0..) |peer_id, i| {
            if (peer_id == id) return i;
        }
        return null;
    }

    pub fn node(self: *Cluster, id: core.types.NodeId) *core.RawNode {
        return &self.nodes[self.nodeIndex(id).?];
    }

    pub fn peerIds(self: *const Cluster) []const core.types.NodeId {
        return self.peer_ids;
    }

    pub fn initialConfState(self: *const Cluster) core.types.ConfState {
        return self.initial_conf_state;
    }

    pub fn pendingMessageSlice(self: *const Cluster) []const message.Message {
        return self.network.items;
    }

    pub fn isNodeActive(self: *const Cluster, id: core.types.NodeId) bool {
        return self.active_nodes.contains(id);
    }

    pub fn queuedCommittedSlice(self: *const Cluster, id: core.types.NodeId) []const core.types.Entry {
        return self.committed[self.nodeIndex(id).?].items;
    }

    pub fn queuedReadStateSlice(self: *const Cluster, id: core.types.NodeId) []const core.types.ReadState {
        return self.read_states[self.nodeIndex(id).?].items;
    }

    pub fn tick(self: *Cluster, id: core.types.NodeId, count: usize) !void {
        const node_ptr = self.node(id);
        for (0..count) |_| node_ptr.tick();
        try self.collectReady(id);
    }

    pub fn campaign(self: *Cluster, id: core.types.NodeId) !void {
        try self.node(id).campaign();
        try self.collectReady(id);
    }

    pub fn transferLeader(self: *Cluster, id: core.types.NodeId, transferee: core.types.NodeId) !void {
        try self.node(id).transferLeader(transferee);
        try self.collectReady(id);
    }

    pub fn forgetLeader(self: *Cluster, id: core.types.NodeId) !void {
        try self.node(id).forgetLeader();
        try self.collectReady(id);
    }

    pub fn propose(self: *Cluster, id: core.types.NodeId, data: []const u8) !void {
        try self.node(id).propose(data);
        try self.collectReady(id);
    }

    pub fn readIndex(self: *Cluster, id: core.types.NodeId, rctx: []const u8) !void {
        try self.node(id).readIndex(rctx);
        try self.collectReady(id);
    }

    pub fn proposeConfChangeV2(self: *Cluster, id: core.types.NodeId, conf_change: core.types.ConfChangeV2) !void {
        try self.node(id).proposeConfChangeV2(conf_change);
        try self.collectReady(id);
    }

    pub fn restart(self: *Cluster, id: core.types.NodeId) !void {
        return try self.restartWithApplied(id, self.applied_indexes[self.nodeIndex(id).?]);
    }

    pub fn restartWithApplied(self: *Cluster, id: core.types.NodeId, applied: core.types.Index) !void {
        const idx = self.nodeIndex(id).?;
        self.applied_indexes[idx] = applied;
        self.nodes[idx].deinit();
        self.nodes[idx] = try initNode(self.alloc, self.peer_ids, .{
            .election_tick = self.election_tick,
            .heartbeat_tick = self.heartbeat_tick,
            .random_seed = self.random_seed,
            .applied_indexes = self.applied_indexes,
            .max_size_per_msg = self.max_size_per_msg,
            .max_committed_size_per_ready = self.max_committed_size_per_ready,
            .max_inflight_msgs = self.max_inflight_msgs,
            .max_inflight_bytes = self.max_inflight_bytes,
            .max_uncommitted_entries_size = self.max_uncommitted_entries_size,
            .async_storage_writes = self.async_storage_writes,
            .check_quorum = self.check_quorum,
            .pre_vote = self.node_pre_vote[idx],
            .step_down_on_removal = self.step_down_on_removal,
            .disable_proposal_forwarding = self.disable_proposal_forwarding,
            .disable_conf_change_validation = self.disable_conf_change_validation,
            .read_only_option = self.read_only_option,
        }, id, self.stores[idx].storage());
        try self.collectReady(id);
    }

    pub fn setNodePreVote(self: *Cluster, id: core.types.NodeId, enabled: bool) void {
        const idx = self.nodeIndex(id).?;
        self.node_pre_vote[idx] = enabled;
        self.nodes[idx].raft.cfg.pre_vote = enabled;
    }

    pub fn setRandomizedElectionTimeout(self: *Cluster, id: core.types.NodeId, timeout: u32) void {
        self.node(id).raft.randomized_election_timeout = timeout;
    }

    pub fn compact(self: *Cluster, id: core.types.NodeId, index: core.types.Index) !void {
        const idx = self.nodeIndex(id).?;
        try self.stores[idx].compactTo(index, self.nodes[idx].status().conf_state);
    }

    pub fn collectReady(self: *Cluster, id: core.types.NodeId) anyerror!void {
        const idx = self.nodeIndex(id).?;
        const node_ptr = self.node(id);
        if (!node_ptr.hasReady()) return;

        const rd = node_ptr.ready();
        if (!node_ptr.async_storage_writes) {
            defer node_ptr.advance(rd);

            try self.persistStableReady(idx, rd);
            try self.queueReadStates(idx, rd.read_states);

            if (rd.messages.len > 0) {
                try self.network.ensureUnusedCapacity(self.alloc, rd.messages.len);
                for (rd.messages) |msg| self.network.appendAssumeCapacity(try msg.clone(self.alloc));
            }

            try self.queueCommittedAndApply(idx, rd.committed_entries);
            try self.persistConfState(idx);
            return;
        }

        try self.queueReadStates(idx, rd.read_states);
        const messages = try message.cloneMessages(self.alloc, rd.messages);
        defer message.freeMessages(self.alloc, messages);
        try self.handleAsyncReadyMessages(idx, id, messages);
        try self.persistConfState(idx);
    }

    pub fn collectCommitted(self: *Cluster, id: core.types.NodeId) ![]core.types.Entry {
        const idx = self.nodeIndex(id).?;
        if (self.committed[idx].items.len > 0) {
            const drained = try self.committed[idx].toOwnedSlice(self.alloc);
            self.committed[idx] = .empty;
            return drained;
        }

        if (self.nodes[idx].async_storage_writes) {
            if (self.node(id).hasReady()) try self.collectReady(id);
            if (self.committed[idx].items.len == 0) return &.{};
            const drained = try self.committed[idx].toOwnedSlice(self.alloc);
            self.committed[idx] = .empty;
            return drained;
        }

        const node_ptr = self.node(id);
        if (!node_ptr.hasReady()) return &.{};

        const rd = node_ptr.ready();
        defer node_ptr.advance(rd);
        try self.persistStableReady(idx, rd);
        try self.applyConfChanges(idx, rd.committed_entries);
        try self.persistConfState(idx);
        return try core.types.cloneEntries(self.alloc, rd.committed_entries);
    }

    pub fn collectReadStates(self: *Cluster, id: core.types.NodeId) ![]core.types.ReadState {
        const idx = self.nodeIndex(id).?;
        if (self.read_states[idx].items.len > 0) {
            const drained = try self.read_states[idx].toOwnedSlice(self.alloc);
            self.read_states[idx] = .empty;
            return drained;
        }

        if (self.nodes[idx].async_storage_writes) {
            if (self.node(id).hasReady()) try self.collectReady(id);
            if (self.read_states[idx].items.len == 0) return &.{};
            const drained = try self.read_states[idx].toOwnedSlice(self.alloc);
            self.read_states[idx] = .empty;
            return drained;
        }

        const node_ptr = self.node(id);
        if (!node_ptr.hasReady()) return &.{};

        const rd = node_ptr.ready();
        defer node_ptr.advance(rd);
        try self.persistStableReady(idx, rd);
        try self.queueCommittedAndApply(idx, rd.committed_entries);
        try self.persistConfState(idx);
        return try cloneReadStates(self.alloc, rd.read_states);
    }

    pub fn block(self: *Cluster, from: core.types.NodeId, to: core.types.NodeId) !void {
        try self.blocked_links.put(self.alloc, edgeKey(from, to), {});
    }

    pub fn unblock(self: *Cluster, from: core.types.NodeId, to: core.types.NodeId) void {
        _ = self.blocked_links.remove(edgeKey(from, to));
    }

    pub fn clearBlocks(self: *Cluster) void {
        self.blocked_links.clearRetainingCapacity();
    }

    pub fn isBlocked(self: *const Cluster, from: core.types.NodeId, to: core.types.NodeId) bool {
        return self.blocked_links.contains(edgeKey(from, to));
    }

    pub fn blockedLinkCount(self: *const Cluster) usize {
        return self.blocked_links.count();
    }

    pub fn rejectSnapshot(self: *Cluster, from: core.types.NodeId, to: core.types.NodeId) !void {
        while (true) {
            for (self.network.items, 0..) |msg, i| {
                if (msg.msg_type != .snapshot or msg.from != from or msg.to != to) continue;

                var dropped = self.network.orderedRemove(i);
                defer dropped.deinit(self.alloc);

                try self.node(from).step(.{
                    .msg_type = .snapshot_response,
                    .from = to,
                    .to = from,
                    .term = dropped.term,
                    .reject = true,
                });
                try self.collectReady(from);
                return;
            }

            if (!try self.deliverOne()) return error.SnapshotMessageNotFound;
        }
    }

    pub fn abortSnapshot(self: *Cluster, from: core.types.NodeId, to: core.types.NodeId, log_index: core.types.Index) !void {
        while (true) {
            for (self.network.items, 0..) |msg, i| {
                if (msg.msg_type != .snapshot or msg.from != from or msg.to != to) continue;

                var dropped = self.network.orderedRemove(i);
                defer dropped.deinit(self.alloc);

                try self.node(from).step(.{
                    .msg_type = .append_entries_response,
                    .from = to,
                    .to = from,
                    .term = dropped.term,
                    .log_index = log_index,
                });
                try self.collectReady(from);
                return;
            }

            if (!try self.deliverOne()) return error.SnapshotMessageNotFound;
        }
    }

    pub fn deliverOne(self: *Cluster) !bool {
        if (self.network.items.len == 0) return false;

        var msg = self.network.orderedRemove(0);
        defer msg.deinit(self.alloc);

        if (self.blocked_links.contains(edgeKey(msg.from, msg.to))) return true;
        if (!self.isNodeActive(msg.from) or !self.isNodeActive(msg.to)) return true;

        try self.node(msg.to).step(msg);
        try self.collectReady(msg.to);
        return true;
    }

    pub fn deliverAll(self: *Cluster) !void {
        while (true) {
            if (try self.deliverOne()) continue;
            if (try self.collectOneReady()) continue;
            break;
        }
    }

    pub fn deliverNext(self: *Cluster) !void {
        if (!try self.deliverOne()) return error.NoPendingMessages;
    }

    pub fn pendingMessages(self: *const Cluster) usize {
        return self.network.items.len;
    }

    fn persistStableReady(self: *Cluster, idx: usize, rd: core.Ready) !void {
        if (rd.snapshot) |snapshot| try self.stores[idx].applySnapshot(snapshot);
        if (rd.hard_state) |hard_state| self.stores[idx].setHardState(hard_state);
        if (rd.entries.len > 0) try self.stores[idx].append(rd.entries);
    }

    fn collectOneReady(self: *Cluster) !bool {
        for (self.peer_ids) |node_id| {
            if (!self.isNodeActive(node_id)) continue;
            if (!self.node(node_id).hasReady()) continue;
            try self.collectReady(node_id);
            return true;
        }
        return false;
    }

    fn handleAsyncReadyMessages(
        self: *Cluster,
        idx: usize,
        node_id: core.types.NodeId,
        messages: []const message.Message,
    ) anyerror!void {
        for (messages) |msg| {
            switch (msg.msg_type) {
                .storage_append => try self.handleStorageAppendMessage(idx, node_id, msg),
                .storage_apply => try self.handleStorageApplyMessage(idx, node_id, msg),
                else => try self.network.append(self.alloc, try msg.clone(self.alloc)),
            }
        }
    }

    fn handleStorageAppendMessage(
        self: *Cluster,
        idx: usize,
        node_id: core.types.NodeId,
        msg: message.Message,
    ) anyerror!void {
        if (msg.snapshot) |snapshot| try self.stores[idx].applySnapshot(snapshot);
        if (msg.term != 0 or msg.vote != null or msg.commit_index != 0) {
            self.stores[idx].setHardState(.{
                .current_term = msg.term,
                .voted_for = msg.vote,
                .commit_index = msg.commit_index,
            });
        }
        if (msg.entries.len > 0) try self.stores[idx].append(msg.entries);
        try self.dispatchLocalResponses(node_id, msg.responses);
    }

    fn handleStorageApplyMessage(
        self: *Cluster,
        idx: usize,
        node_id: core.types.NodeId,
        msg: message.Message,
    ) anyerror!void {
        try self.queueCommittedAndApply(idx, msg.entries);
        try self.dispatchLocalResponses(node_id, msg.responses);
    }

    fn dispatchLocalResponses(self: *Cluster, node_id: core.types.NodeId, responses: []const message.Message) anyerror!void {
        for (responses) |response| {
            if (response.to == node_id) {
                try self.node(node_id).step(response);
                try self.collectReady(node_id);
            } else {
                try self.network.append(self.alloc, try response.clone(self.alloc));
            }
        }
    }

    fn persistConfState(self: *Cluster, idx: usize) !void {
        try self.stores[idx].setConfState(self.nodes[idx].status().conf_state);
    }

    fn queueCommittedAndApply(self: *Cluster, idx: usize, entries: []const core.types.Entry) !void {
        if (entries.len > 0) {
            try self.committed[idx].ensureUnusedCapacity(self.alloc, entries.len);
            for (entries) |entry| self.committed[idx].appendAssumeCapacity(try entry.clone(self.alloc));
        }
        try self.applyConfChanges(idx, entries);
    }

    fn queueReadStates(self: *Cluster, idx: usize, read_states: []const core.types.ReadState) !void {
        if (read_states.len == 0) return;
        try self.read_states[idx].ensureUnusedCapacity(self.alloc, read_states.len);
        for (read_states) |read_state| self.read_states[idx].appendAssumeCapacity(try read_state.clone(self.alloc));
    }

    fn applyConfChanges(self: *Cluster, idx: usize, entries: []const core.types.Entry) !void {
        for (entries) |entry| {
            switch (entry.entry_type) {
                .conf_change => {
                    const before = self.nodes[idx].status().conf_state;
                    const conf_change = try core.types.ConfChange.decode(entry.data);
                    const after = try self.nodes[idx].applyConfChange(conf_change);
                    try self.syncNodeActivity(before, after);
                    self.dropRemovedConfStateMessages(before, after);
                },
                .conf_change_v2 => {
                    const before = self.nodes[idx].status().conf_state;
                    var conf_change = try core.types.ConfChangeV2.decode(entry.data, self.alloc);
                    defer conf_change.deinit(self.alloc);
                    const after = try self.nodes[idx].applyConfChangeV2(conf_change);
                    try self.syncNodeActivity(before, after);
                    self.dropRemovedConfStateMessages(before, after);
                },
                else => {},
            }
        }
    }

    fn dropMessagesForNode(self: *Cluster, node_id: core.types.NodeId) void {
        var keep: usize = 0;
        var i: usize = 0;
        while (i < self.network.items.len) : (i += 1) {
            const msg = self.network.items[i];
            if (msg.from == node_id or msg.to == node_id) {
                var dropped = msg;
                dropped.deinit(self.alloc);
                continue;
            }
            if (keep != i) self.network.items[keep] = self.network.items[i];
            keep += 1;
        }
        self.network.shrinkRetainingCapacity(keep);
    }

    fn activateNode(self: *Cluster, node_id: core.types.NodeId) !void {
        try self.active_nodes.put(self.alloc, node_id, {});
    }

    fn syncNodeActivity(
        self: *Cluster,
        before: core.types.ConfState,
        after: core.types.ConfState,
    ) !void {
        for (self.peer_ids) |node_id| {
            const before_contains = confStateContainsNode(before, node_id);
            const after_contains = confStateContainsNode(after, node_id);
            if (!before_contains and after_contains) {
                try self.activateNode(node_id);
            }
        }
    }

    fn dropRemovedConfStateMessages(
        self: *Cluster,
        before: core.types.ConfState,
        after: core.types.ConfState,
    ) void {
        for (self.peer_ids) |node_id| {
            if (!confStateContainsNode(before, node_id)) continue;
            if (confStateContainsNode(after, node_id)) continue;
            self.dropMessagesForNode(node_id);
        }
    }
};

fn initNode(
    alloc: std.mem.Allocator,
    peer_ids: []const core.types.NodeId,
    options: Cluster.Options,
    id: core.types.NodeId,
    storage: core.Storage,
) !core.RawNode {
    const id_index = for (peer_ids, 0..) |peer_id, i| {
        if (peer_id == id) break i;
    } else unreachable;
    return try core.RawNode.init(alloc, .{
        .id = id,
        .group_id = 1,
        .peers = peer_ids,
        .election_tick = options.election_tick,
        .heartbeat_tick = options.heartbeat_tick,
        .random_seed = deriveRandomSeed(options.random_seed, id),
        .applied = if (options.applied_indexes) |applied_indexes| applied_indexes[id_index] else 0,
        .max_size_per_msg = options.max_size_per_msg,
        .max_committed_size_per_ready = options.max_committed_size_per_ready,
        .max_inflight_msgs = options.max_inflight_msgs,
        .max_inflight_bytes = options.max_inflight_bytes,
        .max_uncommitted_entries_size = options.max_uncommitted_entries_size,
        .async_storage_writes = options.async_storage_writes,
        .check_quorum = options.check_quorum,
        .pre_vote = options.pre_vote,
        .step_down_on_removal = options.step_down_on_removal,
        .disable_proposal_forwarding = options.disable_proposal_forwarding,
        .disable_conf_change_validation = options.disable_conf_change_validation,
        .read_only_option = options.read_only_option,
    }, storage);
}

fn deriveRandomSeed(base: ?u64, id: core.types.NodeId) ?u64 {
    if (base == null) return null;

    var state = base.? ^ (@as(u64, id) *% 0x9e3779b97f4a7c15);
    state +%= 0x9e3779b97f4a7c15;
    var z = state;
    z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

fn cloneReadStates(alloc: std.mem.Allocator, read_states: []const core.types.ReadState) ![]core.types.ReadState {
    const out = try alloc.alloc(core.types.ReadState, read_states.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*state| state.deinit(alloc);
        alloc.free(out);
    }

    for (read_states, 0..) |read_state, i| {
        out[i] = try read_state.clone(alloc);
        initialized += 1;
    }
    return out;
}

fn confStateContainsNode(conf_state: core.types.ConfState, node_id: core.types.NodeId) bool {
    return containsNode(conf_state.voters, node_id) or
        containsNode(conf_state.voters_outgoing, node_id) or
        containsNode(conf_state.learners, node_id) or
        containsNode(conf_state.learners_next, node_id);
}

fn containsNode(nodes: []const core.types.NodeId, node_id: core.types.NodeId) bool {
    for (nodes) |node| {
        if (node == node_id) return true;
    }
    return false;
}
