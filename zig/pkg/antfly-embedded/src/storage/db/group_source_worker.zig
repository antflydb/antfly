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

//! Resumable group processing, separate from ordinary producer execution.
//! Every stage is reconstructed from durable owner state after a lost reply or
//! restart. No provider, network read or enrichment wait runs under apply.
const std = @import("std");
const groups = @import("group_sources.zig");
const index = @import("group_source_index.zig");
const state = @import("group_source_state.zig");
const contract = @import("group_source_contract.zig");
const reads = @import("group_source_reads.zig");
const baseline = @import("group_source_baseline.zig");
const A = std.mem.Allocator;
pub const Definition = struct {
    artifact: []const u8,
    definition: index.Definition,
    source: groups.Config,
    namespace: [24]u8,
    source_owner: state.Owner,
    route: @import("group_source_bytes.zig").Bytes,
    horizon: u16 = 0,
    retirement: bool = false,
};
pub const FanIn = struct { earlier_pending: bool = false, members: []const groups.Member, sources: []const contract.SourceCut, topology_epoch: u64 };
pub const Ports = struct {
    ptr: *anyopaque,
    /// Routes one authenticated private read to its current table owner.
    query_fn: *const fn (*anyopaque, A, []const u8, []const u8) anyerror![]const u8,
    /// Resolves ALL current source owners and checks their index readiness,
    /// configuration, pagination revision and topology before returning.
    fan_in_fn: *const fn (*anyopaque, A, Definition, index.Partition, i64) anyerror!FanIn,
    submit_fn: *const fn (*anyopaque, contract.Command) anyerror!void,
    /// Nonblocking ordinary producer/graph visibility observation.
    settled_fn: *const fn (*anyopaque, u64, []const []const u8) anyerror!bool,
};
pub const EventStatus = struct { source_revision: u64, propagated: bool, frontier: u64, settled: bool, retire_ready: bool = false };
pub const Status = struct {
    owner: state.Owner,
    namespace: [24]u8 = @splat(0),
    lease: ?state.Lease = null,
    pending: ?state.Pending = null,
    snapshot: ?state.Snapshot = null,
    event_status: ?EventStatus = null,
};
pub const Worker = struct {
    const Cursors = struct { source: []const u8 = "", group: []const u8 = "" };
    identity: groups.Digest,
    scopes: std.AutoHashMapUnmanaged(groups.Digest, Cursors) = .empty,
    definition_positions: std.AutoHashMapUnmanaged([24]u8, usize) = .empty,
    allocator: A,
    pub fn deinit(self: *Worker) void {
        var it = self.scopes.valueIterator();
        while (it.next()) |value| {
            if (value.source.len > 0) self.allocator.free(value.source);
            if (value.group.len > 0) self.allocator.free(value.group);
        }
        self.scopes.deinit(self.allocator);
        self.definition_positions.deinit(self.allocator);
        self.* = undefined;
    }
    pub fn nextDefinition(self: *Worker, namespace: [24]u8, count: usize) !usize {
        if (count == 0) return error.InvalidGroupedSourceConfig;
        if (self.definition_positions.count() >= 256 and !self.definition_positions.contains(namespace)) self.definition_positions.clearRetainingCapacity();
        const entry = try self.definition_positions.getOrPut(self.allocator, namespace);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        const selected = entry.value_ptr.* % count;
        entry.value_ptr.* +%= 1;
        return selected;
    }
    fn cursors(self: *Worker, definition: Definition) !*Cursors {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&definition.namespace);
        hash.update(&definition.definition.configuration);
        if (self.scopes.count() >= 512 and !self.scopes.contains(hash.peek())) {
            var old = self.scopes.valueIterator();
            while (old.next()) |value| {
                if (value.source.len > 0) self.allocator.free(value.source);
                if (value.group.len > 0) self.allocator.free(value.group);
            }
            self.scopes.clearRetainingCapacity();
        }
        const entry = try self.scopes.getOrPut(self.allocator, hash.finalResult());
        if (!entry.found_existing) entry.value_ptr.* = .{};
        return entry.value_ptr;
    }
    fn advance(self: *Worker, target: *[]const u8, next: []const u8) !void {
        const replacement = if (next.len > 0) try self.allocator.dupe(u8, next) else "";
        if (target.len > 0) self.allocator.free(target.*);
        target.* = replacement;
    }
    fn query(comptime T: type, a: A, ports: Ports, route: []const u8, value: anytype) !T {
        const request = try std.json.Stringify.valueAlloc(a, value, .{});
        const raw = try ports.query_fn(ports.ptr, a, route, request);
        return try std.json.parseFromSliceLeaky(T, a, raw, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    }
    /// One bounded activation or delivery slice. Callers yield between slices
    /// and back off transient route/readiness/provider failures.
    pub fn sourceStep(self: *Worker, a: A, ports: Ports, definition: Definition) !void {
        const activated = try query(bool, a, ports, definition.route.value, contract.Query{ .action = .activation, .artifact = definition.artifact, .definition = definition.definition });
        if (!activated) {
            try ports.submit_fn(ports.ptr, .{ .action = .activate, .artifact = definition.artifact, .definition = definition.definition, .namespace = definition.namespace, .backfill = .{ .route = definition.route } });
            return;
        }
        if (definition.retirement) {
            const progress = try query(@import("group_source_retirement.zig").Progress, a, ports, definition.route.value, contract.Query{ .action = .retirement, .artifact = definition.artifact, .definition = definition.definition });
            if (!progress.done) try ports.submit_fn(ports.ptr, .{ .action = .retire, .artifact = definition.artifact, .definition = definition.definition, .namespace = definition.namespace, .backfill = .{ .route = definition.route, .cursor = progress.cursor, .page = try query(@import("group_source_page.zig").Page, a, ports, definition.route.value, contract.Query{ .action = .retirement_page, .artifact = definition.artifact, .definition = definition.definition }) } });
            return;
        }
        const cursor_state = try self.cursors(definition);
        const progress = try query(baseline.Progress, a, ports, definition.route.value, contract.Query{ .action = .backfill, .artifact = definition.artifact, .definition = definition.definition });
        if (progress.phase != .ready) {
            try ports.submit_fn(ports.ptr, .{ .action = .backfill, .artifact = definition.artifact, .definition = definition.definition, .namespace = definition.namespace, .backfill = .{ .route = definition.route, .cursor = progress.cursor, .page = try query(@import("group_source_page.zig").Page, a, ports, definition.route.value, contract.Query{ .action = .backfill_page, .artifact = definition.artifact, .definition = definition.definition }) } });
            return;
        }
        const page = try query(reads.PointerPage, a, ports, definition.route.value, contract.Query{ .action = .outbox, .artifact = definition.artifact, .definition = definition.definition, .cursor = cursor_state.source, .limit = 128 });
        // One page is bounded; visit other queues even while this one waits
        // for expensive ordinary producers on a coordinator.
        for (page.rows) |pointer| {
            const selected = try reads.decode(a, pointer.key_base64);
            // Keep fair progress even if one remote owner or source event
            // fails; durable outboxes are retried after the cursor wraps.
            try self.advance(&cursor_state.source, try reads.encode(a, try @import("group_source_queue.zig").sourceKey(a, selected)));
            const raw = try reads.decode(a, pointer.value_base64);
            const event = try index.readInvalidation(a, selected, raw);
            if (!std.meta.eql(event.definition, definition.definition)) continue;
            const group = state.Group{ .definition = event.definition, .key = event.partition.group };
            const route = try group.anchor(a);
            const status = try query(Status, a, ports, route, .{ .action = "status", .artifact = definition.artifact, .definition = definition.definition, .group = group.key, .segment = event.partition.segment, .cell = event.partition.cell, .source_owner = definition.source_owner, .event = event });
            if (status.event_status) |receipt| {
                if (receipt.source_revision == event.source_revision and receipt.propagated and receipt.settled) {
                    try ports.submit_fn(ports.ptr, .{ .action = .acknowledge, .artifact = definition.artifact, .definition = definition.definition, .namespace = definition.namespace, .acknowledge = .{ .document = event.document, .key_base64 = pointer.key_base64, .observed_base64 = pointer.value_base64 } });
                    continue;
                }
                if (receipt.source_revision == event.source_revision) {
                    if (!event.delivered) {
                        try ports.submit_fn(ports.ptr, .{ .action = .acknowledge, .artifact = definition.artifact, .definition = definition.definition, .namespace = definition.namespace, .acknowledge = .{ .document = event.document, .key_base64 = pointer.key_base64, .observed_base64 = pointer.value_base64, .delivery = true } });
                        continue;
                    }
                    // Poll providers without appending empty Raft controls.
                    // Once a dependency is accepted, retire a bounded page.
                    if (receipt.propagated and !receipt.retire_ready) continue;
                }
            } else if (event.delivered) {
                // A replaced/restored coordinator must enroll the event again.
                try ports.submit_fn(ports.ptr, .{ .action = .acknowledge, .artifact = definition.artifact, .definition = definition.definition, .namespace = definition.namespace, .acknowledge = .{ .document = event.document, .key_base64 = pointer.key_base64, .observed_base64 = pointer.value_base64, .delivery = false } });
                continue;
            }
            try ports.submit_fn(ports.ptr, .{ .action = .invalidate, .artifact = definition.artifact, .definition = definition.definition, .namespace = status.namespace, .invalidate = .{ .owner = definition.source_owner, .event = event } });
        }
        try self.advance(&cursor_state.source, if (page.done) "" else page.cursor);
    }
    /// Group-local ordering and leases serialize only the dependent units.
    /// Ordinary enrichment runs on another scheduler lane; this slice polls
    /// visibility instead of blocking that producer waiting on itself.
    pub fn groupStep(self: *Worker, a: A, ports: Ports, definition: Definition, now_ms: u64) !void {
        if (definition.retirement) return;
        const cursor_state = try self.cursors(definition);
        const page = try query(reads.PointerPage, a, ports, definition.route.value, contract.Query{ .action = .pending, .artifact = definition.artifact, .definition = definition.definition, .cursor = cursor_state.group, .limit = 1 });
        for (page.rows) |pointer| {
            try self.advance(&cursor_state.group, pointer.key_base64);
            const descriptor = try reads.decode(a, pointer.value_base64);
            const group = try std.json.parseFromSliceLeaky(state.Group, a, descriptor, .{ .allocate = .alloc_always });
            if (!std.meta.eql(group.definition, definition.definition)) continue;
            const route = try group.anchor(a);
            const pending = (try query(?state.Pending, a, ports, route, contract.Query{ .action = .pending, .artifact = definition.artifact, .definition = definition.definition, .group = group.key })) orelse continue;
            const status = try query(Status, a, ports, route, contract.Query{ .action = .status, .artifact = definition.artifact, .definition = definition.definition, .group = group.key, .segment = pending.partition.segment, .cell = pending.partition.cell });
            var coordinator_definition = definition;
            coordinator_definition.namespace = status.namespace;
            const lease = status.lease orelse {
                try self.claim(ports, coordinator_definition, group, pending, now_ms);
                continue;
            };
            if (lease.incarnation != status.owner.incarnation or lease.expires_ms <= now_ms or lease.revision != pending.revision) {
                try self.claim(ports, coordinator_definition, group, pending, now_ms);
                continue;
            }
            if (!std.mem.eql(u8, &lease.owner, &self.identity)) continue;
            const held = contract.Held{ .group = group, .partition = pending.partition, .lease = lease, .now_ms = now_ms };
            if (lease.expires_ms - now_ms < 15000) try ports.submit_fn(ports.ptr, .{ .action = .heartbeat, .artifact = definition.artifact, .definition = definition.definition, .namespace = coordinator_definition.namespace, .held = held });
            if (status.snapshot) |snapshot| if (snapshot.revision == pending.revision) {
                if (try ports.settled_fn(ports.ptr, snapshot.sequence, try std.mem.concat(a, []const u8, &.{ snapshot.units, snapshot.retired }))) {
                    var hash = std.crypto.hash.sha2.Sha256.init(.{});
                    hash.update(&snapshot.input_digest);
                    var revision: [8]u8 = undefined;
                    std.mem.writeInt(u64, &revision, snapshot.revision, .big);
                    hash.update(&revision);
                    try ports.submit_fn(ports.ptr, .{ .action = .finish, .artifact = definition.artifact, .definition = definition.definition, .namespace = coordinator_definition.namespace, .finish = .{ .held = held, .input_digest = snapshot.input_digest, .through_sequence = snapshot.sequence, .publication = hash.finalResult() } });
                }
                continue;
            };
            const source = try ports.fan_in_fn(ports.ptr, a, definition, pending.partition, pending.ordinal);
            if (source.earlier_pending) continue;
            if (source.members.len > 65536 or source.sources.len == 0 or source.sources.len > 256) return error.GroupedSourcePartitionTooLarge;
            var seen: std.StringHashMapUnmanaged(void) = .empty;
            var bytes: usize = 0;
            for (source.members) |member| {
                if ((try seen.getOrPut(a, member.document)).found_existing) return error.EnrichmentSourceChanged;
                bytes = std.math.add(usize, bytes, member.text.len + member.document.len) catch return error.GroupedSourcePartitionTooLarge;
                if (bytes > contract.max_partition_bytes) return error.GroupedSourcePartitionTooLarge;
            }
            const units = try groups.units(a, definition.source, group.key, source.members);
            if (units.len > contract.max_page_items) return error.GroupedSourcePartitionTooLarge;
            if (units.len != 0 and units[0].ordinal_start > pending.ordinal) {
                const ordered = try ports.fan_in_fn(ports.ptr, a, definition, pending.partition, units[0].ordinal_start);
                if (ordered.earlier_pending) continue;
                if (ordered.topology_epoch != source.topology_epoch or ordered.sources.len != source.sources.len) return error.EnrichmentSourceChanged;
                for (ordered.sources, source.sources) |current, original| if (!std.meta.eql(current, original)) return error.EnrichmentSourceChanged;
            }
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update(&definition.definition.configuration);
            hash.update(&groups.digest(try std.json.Stringify.valueAlloc(a, pending.partition, .{})));
            for (units) |unit| hash.update(&unit.fingerprint);
            try ports.submit_fn(ports.ptr, .{ .action = .publish, .artifact = definition.artifact, .definition = definition.definition, .namespace = coordinator_definition.namespace, .publish = .{ .held = held, .topology_epoch = source.topology_epoch, .sources = source.sources, .units = units, .input_digest = hash.finalResult() } });
        }
        try self.advance(&cursor_state.group, if (page.done) "" else page.cursor);
    }
    fn claim(self: *Worker, ports: Ports, definition: Definition, group: state.Group, pending: state.Pending, now_ms: u64) !void {
        try ports.submit_fn(ports.ptr, .{ .action = .claim, .artifact = definition.artifact, .definition = definition.definition, .namespace = definition.namespace, .claim = .{ .group = group, .pending = pending, .owner = self.identity, .now_ms = now_ms, .ttl_ms = 30000 } });
    }
};

test "group worker resumes durable activation and yields when queues are empty" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Fixture = struct {
        phase: baseline.Phase = .scan,
        commands: usize = 0,
        fn query(ptr: *anyopaque, alloc: A, _: []const u8, raw: []const u8) ![]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const value = try std.json.parseFromSliceLeaky(std.json.Value, alloc, raw, .{});
            if (std.mem.eql(u8, value.object.get("action").?.string, "activation")) return alloc.dupe(u8, "true");
            if (std.mem.eql(u8, value.object.get("action").?.string, "backfill_page")) return std.json.Stringify.valueAlloc(alloc, @import("group_source_page.zig").Page{ .generation = 1, .phase = .scan, .cursor = "durable-cursor", .next = "", .done = true, .candidates = &.{} }, .{});
            if (std.mem.eql(u8, value.object.get("action").?.string, "backfill")) return std.json.Stringify.valueAlloc(alloc, baseline.Progress{ .generation = 1, .phase = self.phase, .cursor = "durable-cursor" }, .{});
            return std.json.Stringify.valueAlloc(alloc, reads.PointerPage{ .rows = &.{}, .cursor = "", .done = true }, .{});
        }
        fn submit(ptr: *anyopaque, command: contract.Command) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(contract.Action.backfill, command.action);
            try std.testing.expectEqualStrings("durable-cursor", command.backfill.?.cursor);
            self.commands += 1;
            self.phase = .ready;
        }
        fn fanIn(_: *anyopaque, _: A, _: Definition, _: index.Partition, _: i64) !FanIn {
            return error.TestUnexpectedResult;
        }
        fn settled(_: *anyopaque, _: u64, _: []const []const u8) !bool {
            return error.TestUnexpectedResult;
        }
    };
    var fixture = Fixture{};
    const ports = Ports{ .ptr = &fixture, .query_fn = Fixture.query, .submit_fn = Fixture.submit, .fan_in_fn = Fixture.fanIn, .settled_fn = Fixture.settled };
    const definition = Definition{ .artifact = "turns", .definition = .{ .table_incarnation = 9, .configuration = groups.digest("config") }, .source = .{ .group_by = "thread", .order_by = "ordinal", .field = "text" }, .namespace = @splat(1), .source_owner = .{ .namespace = @splat(1), .incarnation = 9 }, .route = .{ .value = "a" } };
    var worker = Worker{ .allocator = std.testing.allocator, .identity = groups.digest("worker") };
    defer worker.deinit();
    try worker.sourceStep(a, ports, definition);
    try worker.sourceStep(a, ports, definition);
    try worker.groupStep(a, ports, definition, 100);
    try std.testing.expectEqual(@as(usize, 1), fixture.commands);
}
