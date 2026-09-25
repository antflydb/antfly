// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Persistent ownership before dispatch. Every unfinished record is uncertainty
//! after a crash; elapsed time never removes it. Generations are durable and
//! serialized with all attempts, so a replacement owner fences stale writers.
//! Authenticated terminal/fence evidence is verified by the transport adapter
//! before invoking the corresponding retirement methods here.
const std = @import("std");
const protocol = @import("workload_attempt_protocol.zig");
const transactions = @import("transactions.zig");
pub const Config = @import("../common/workload_coordinator_config.zig").Config;
const base_charge: u64 = 2048;
const destination_charge: u64 = 512;
const attempt_charge: u64 = 1024;
const hard_attempts = 4096;
const key_capacity = 192;

const Metadata = struct {
    version: u16 = 2,
    generation: u64 = 1,
    sequence: u64 = 0,
    destinations: u32 = 0,
    attempts: u32 = 0,
    fn bytes(self: Metadata) u64 {
        return base_charge + @as(u64, self.destinations) * destination_charge + @as(u64, self.attempts) * attempt_charge;
    }
};
const Destination = struct { node: u64, namespace: u128 = 0, incarnation: u64 = 0, ready_generation: u64 = 0, sequence: u64 = 0, attempts: u32 = 0 };
const Record = struct { id: protocol.AttemptId };

pub const Store = struct {
    allocator: std.mem.Allocator,
    durable: *transactions.DurableSessionStore,
    node_id: u64,
    generation: u64 = 0,
    config: Config,

    pub const Usage = struct { attempts: u32, destinations: u32, bytes: u64, generation: u64 };
    const Op = union(enum) {
        open,
        ready: protocol.Fence,
        begin: struct { destination: u64, incarnation: u64, operation: u128 },
        terminal: protocol.AttemptId,
    };
    const Result = struct { generation: u64 = 0, id: ?protocol.AttemptId = null };

    pub fn init(alloc: std.mem.Allocator, durable: *transactions.DurableSessionStore, node_id: u64, config: Config) !Store {
        try config.validate();
        if (node_id == 0 or config.max_attempts == 0) return error.InvalidConfig;
        var self: Store = .{ .allocator = alloc, .durable = durable, .node_id = node_id, .config = config };
        self.generation = (try self.transact(.open)).generation;
        return self;
    }

    /// No live records are discarded. Restart recovery uses the same durable
    /// namespace; deleting/restoring that namespace is not a fencing operation.
    pub fn deinit(_: *Store) void {}

    pub fn begin(self: *Store, destination_id: u64, incarnation: u64, operation: u128) !protocol.AttemptId {
        if (destination_id == 0 or incarnation == 0 or operation == 0) return error.InvalidAttemptFrame;
        return (try self.transact(.{ .begin = .{ .destination = destination_id, .incarnation = incarnation, .operation = operation } })).id.?;
    }

    pub fn terminal(self: *Store, id: protocol.AttemptId) !void {
        _ = try self.transact(.{ .terminal = id });
    }

    /// Every lower sequence is durably retired. This is stronger than a
    /// sequence high-water mark: delayed or uncertain sends keep the floor
    /// behind their first owned record. Ordered keys need one cursor seek.
    pub fn acknowledgedThrough(self: *Store, destination_id: u64) !u64 {
        return switch (self.durable.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :blk try self.acknowledgedTxn(&txn, destination_id);
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                break :blk try self.acknowledgedTxn(&txn, destination_id);
            },
        };
    }

    fn acknowledgedTxn(self: *Store, txn: anytype, destination_id: u64) !u64 {
        const meta = try self.metadata(txn) orelse return error.InvalidCoordinatorJournal;
        if (meta.generation != self.generation) return error.CoordinatorGenerationSuperseded;
        const target = try self.destination(txn, destination_id) orelse return error.FencingRequired;
        if (target.ready_generation != self.generation) return error.FencingRequired;
        var buffer: [key_capacity]u8 = undefined;
        const prefix = try std.fmt.bufPrint(&buffer, "workload-attempt-coordinator/v2/{d}/attempt/{x:0>16}/{x:0>16}/", .{ self.node_id, self.generation, destination_id });
        var cursor = try txn.openCursor();
        defer cursor.close();
        if (try cursor.seekAtOrAfter(prefix)) |row| {
            if (std.mem.startsWith(u8, row.key, prefix)) {
                const id = try self.attemptRow(row.key, row.value);
                if (id.generation != self.generation or id.destination != destination_id) return error.InvalidCoordinatorJournal;
                return id.sequence - 1;
            }
        }
        return target.sequence;
    }

    /// Fence must cover all prior generations at this exact authenticated
    /// worker. It is not a health-check or an elapsed-deadline inference.
    pub fn ready(self: *Store, evidence: protocol.Fence) !void {
        if (evidence.version != 1 or evidence.coordinator != self.node_id or evidence.destination == 0 or evidence.worker_incarnation == 0 or evidence.worker_namespace == 0 or
            evidence.fenced_through != self.generation - 1 or evidence.quiesced_through != evidence.fenced_through)
            return error.FencingRequired;
        _ = try self.transact(.{ .ready = evidence });
    }

    pub fn usage(self: *Store) !Usage {
        return switch (self.durable.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :blk try self.usageTxn(&txn);
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                break :blk try self.usageTxn(&txn);
            },
        };
    }

    pub fn readyIncarnation(self: *Store, destination_id: u64) !?u64 {
        return switch (self.durable.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :blk try self.readyIncarnationTxn(&txn, destination_id);
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                break :blk try self.readyIncarnationTxn(&txn, destination_id);
            },
        };
    }

    fn readyIncarnationTxn(self: *Store, txn: anytype, destination_id: u64) !?u64 {
        const meta = try self.metadata(txn) orelse return error.InvalidCoordinatorJournal;
        if (meta.generation != self.generation) return error.CoordinatorGenerationSuperseded;
        const target = try self.destination(txn, destination_id) orelse return null;
        return if (target.ready_generation == self.generation) target.incarnation else null;
    }

    /// Bounded reconciliation snapshot, used on capacity pressure rather than
    /// scanning the journal on every successful dispatch.
    pub fn pending(self: *Store, alloc: std.mem.Allocator, destination_id: u64) ![]protocol.AttemptId {
        return switch (self.durable.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :blk try self.pendingTxn(&txn, alloc, destination_id);
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                break :blk try self.pendingTxn(&txn, alloc, destination_id);
            },
        };
    }

    fn pendingTxn(self: *Store, txn: anytype, alloc: std.mem.Allocator, destination_id: u64) ![]protocol.AttemptId {
        const meta = try self.metadata(txn) orelse return error.InvalidCoordinatorJournal;
        if (meta.generation != self.generation) return error.CoordinatorGenerationSuperseded;
        var result: std.ArrayListUnmanaged(protocol.AttemptId) = .empty;
        errdefer result.deinit(alloc);
        var buffer: [key_capacity]u8 = undefined;
        const prefix = try self.key(&buffer, "attempt/");
        var cursor = try txn.openCursor();
        defer cursor.close();
        var count: usize = 0;
        var entry = try cursor.seekAtOrAfter(prefix);
        while (entry) |row| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, prefix)) break;
            if (count >= hard_attempts) return error.InvalidCoordinatorJournal;
            count += 1;
            const id = try self.attemptRow(row.key, row.value);
            if (id.destination == destination_id) try result.append(alloc, id);
        }
        return try result.toOwnedSlice(alloc);
    }

    fn usageTxn(self: *Store, txn: anytype) !Usage {
        const meta = try self.metadata(txn) orelse return error.InvalidCoordinatorJournal;
        return .{ .attempts = meta.attempts, .destinations = meta.destinations, .bytes = meta.bytes(), .generation = meta.generation };
    }

    fn transact(self: *Store, op: Op) !Result {
        if (self.durable.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        return switch (self.durable.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                const result = try self.update(&txn, op);
                try txn.commit();
                break :blk result;
            },
            .runtime => |store| blk: {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                const result = try self.update(&txn, op);
                try txn.commit();
                break :blk result;
            },
        };
    }

    fn key(self: *Store, buffer: *[key_capacity]u8, suffix: []const u8) ![]const u8 {
        return std.fmt.bufPrint(buffer, "workload-attempt-coordinator/v2/{d}/{s}", .{ self.node_id, suffix });
    }
    fn destinationKey(self: *Store, buffer: *[key_capacity]u8, destination_id: u64) ![]const u8 {
        return std.fmt.bufPrint(buffer, "workload-attempt-coordinator/v2/{d}/destination/{x:0>16}", .{ self.node_id, destination_id });
    }
    fn attemptKey(self: *Store, buffer: *[key_capacity]u8, id: protocol.AttemptId) ![]const u8 {
        return std.fmt.bufPrint(buffer, "workload-attempt-coordinator/v2/{d}/attempt/{x:0>16}/{x:0>16}/{x:0>16}", .{ self.node_id, id.generation, id.destination, id.sequence });
    }
    fn attemptRow(self: *Store, name: []const u8, raw: []const u8) !protocol.AttemptId {
        const record = try self.decode(Record, raw, attempt_charge);
        const id = record.id;
        var expected: [key_capacity]u8 = undefined;
        if (id.coordinator != self.node_id or id.generation < 2 or id.sequence == 0 or id.destination == 0 or id.worker_incarnation == 0 or id.worker_namespace == 0 or id.operation == 0 or
            !std.mem.eql(u8, name, try self.attemptKey(&expected, id))) return error.InvalidCoordinatorJournal;
        return id;
    }
    fn decode(self: *Store, comptime T: type, raw: []const u8, maximum: u64) !T {
        if (raw.len > maximum) return error.InvalidCoordinatorJournal;
        var parsed = try std.json.parseFromSlice(T, self.allocator, raw, .{});
        defer parsed.deinit();
        return parsed.value;
    }
    fn get(self: *Store, comptime T: type, txn: anytype, name: []const u8, maximum: u64) !?T {
        const raw = txn.get(name) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try self.decode(T, raw, maximum);
    }
    fn put(self: *Store, txn: anytype, name: []const u8, value: anytype, charge: u64) !void {
        const raw = try std.json.Stringify.valueAlloc(self.allocator, value, .{});
        defer self.allocator.free(raw);
        if (name.len + raw.len > charge) return error.InvalidCoordinatorJournal;
        try txn.put(name, raw);
    }
    fn metadata(self: *Store, txn: anytype) !?Metadata {
        var buffer: [key_capacity]u8 = undefined;
        const meta = try self.get(Metadata, txn, try self.key(&buffer, "metadata"), base_charge) orelse return null;
        if (meta.version != 2 or meta.generation < 2 or meta.attempts > hard_attempts or meta.destinations > 256) return error.InvalidCoordinatorJournal;
        return meta;
    }
    fn destination(self: *Store, txn: anytype, node: u64) !?Destination {
        var buffer: [key_capacity]u8 = undefined;
        const value = try self.get(Destination, txn, try self.destinationKey(&buffer, node), destination_charge) orelse return null;
        if (value.node != node or node == 0 or value.attempts > hard_attempts) return error.InvalidCoordinatorJournal;
        return value;
    }

    fn update(self: *Store, txn: anytype, op: Op) !Result {
        const previous = try self.metadata(txn);
        var meta = previous orelse Metadata{};
        if (op != .open and (previous == null or meta.generation != self.generation)) return error.CoordinatorGenerationSuperseded;
        var result: Result = .{};
        switch (op) {
            .open => {
                var legacy_buffer: [key_capacity]u8 = undefined;
                const legacy = try std.fmt.bufPrint(&legacy_buffer, "workload-attempt-coordinator/v1/{d}/metadata", .{self.node_id});
                if (txn.get(legacy) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                }) |_| return error.CoordinatorJournalMigrationRequired;
                // Scan only at restart; validate point counters against the
                // authoritative bounded records before spending any capacity.
                try self.validateRecovery(txn, meta, previous != null);
                if (meta.generation == std.math.maxInt(u64)) return error.GenerationExhausted;
                // Advancing generation makes a future re-added destination's
                // reset sequence distinct from every prior attempt identity.
                // Never reset a destination sequence within a live generation.
                try self.removeIdleDestinations(txn, &meta);
                meta.generation += 1;
                if (meta.attempts > self.config.max_attempts or meta.destinations > self.config.max_destinations or meta.bytes() > self.config.max_bytes)
                    return error.AttemptCapacityExhausted;
                result.generation = meta.generation;
            },
            .ready => |evidence| {
                var destination_value = try self.destination(txn, evidence.destination) orelse fresh: {
                    if (meta.destinations >= self.config.max_destinations or meta.bytes() + destination_charge > self.config.max_bytes) return error.AttemptCapacityExhausted;
                    meta.destinations += 1;
                    break :fresh Destination{ .node = evidence.destination };
                };
                // A fence for a new incarnation may cover earlier process
                // incarnations only through the worker's continuous journal.
                // Never remove current-generation work using an older fence.
                if (destination_value.attempts != 0 and destination_value.namespace != evidence.worker_namespace) return error.FencingRequired;
                if (destination_value.namespace == evidence.worker_namespace and destination_value.incarnation > evidence.worker_incarnation) return error.FencingRequired;
                try self.retireFenced(txn, &meta, &destination_value, evidence.fenced_through);
                if (destination_value.attempts != 0 and destination_value.incarnation != evidence.worker_incarnation) return error.FencingRequired;
                destination_value.incarnation = evidence.worker_incarnation;
                destination_value.namespace = evidence.worker_namespace;
                if (destination_value.ready_generation != self.generation) destination_value.sequence = 0;
                destination_value.ready_generation = self.generation;
                var buffer: [key_capacity]u8 = undefined;
                try self.put(txn, try self.destinationKey(&buffer, destination_value.node), destination_value, destination_charge);
            },
            .begin => |request| {
                var target = try self.destination(txn, request.destination) orelse return error.FencingRequired;
                if (target.ready_generation != self.generation or target.incarnation != request.incarnation) return error.FencingRequired;
                if (meta.attempts >= self.config.max_attempts or target.attempts >= self.config.max_destination_attempts or meta.bytes() + attempt_charge > self.config.max_bytes) return error.AttemptCapacityExhausted;
                if (meta.sequence == std.math.maxInt(u64) or target.sequence == std.math.maxInt(u64)) return error.GenerationExhausted;
                meta.sequence += 1;
                target.sequence += 1;
                const id: protocol.AttemptId = .{ .coordinator = self.node_id, .generation = self.generation, .sequence = target.sequence, .operation = request.operation, .destination = request.destination, .worker_incarnation = request.incarnation, .worker_namespace = target.namespace };
                var buffer: [key_capacity]u8 = undefined;
                try self.put(txn, try self.attemptKey(&buffer, id), Record{ .id = id }, attempt_charge);
                meta.attempts += 1;
                target.attempts += 1;
                try self.put(txn, try self.destinationKey(&buffer, target.node), target, destination_charge);
                result.id = id;
            },
            .terminal => |id| {
                if (id.coordinator != self.node_id) return error.AttemptIdentityMismatch;
                var buffer: [key_capacity]u8 = undefined;
                const name = try self.attemptKey(&buffer, id);
                const record = try self.get(Record, txn, name, attempt_charge) orelse return result;
                if (!std.meta.eql(record.id, id)) return error.AttemptIdentityMismatch;
                var target = try self.destination(txn, id.destination) orelse return error.InvalidCoordinatorJournal;
                if (meta.attempts == 0 or target.attempts == 0) return error.InvalidCoordinatorJournal;
                try txn.delete(name);
                meta.attempts -= 1;
                target.attempts -= 1;
                try self.put(txn, try self.destinationKey(&buffer, target.node), target, destination_charge);
            },
        }
        var buffer: [key_capacity]u8 = undefined;
        try self.put(txn, try self.key(&buffer, "metadata"), meta, base_charge);
        return result;
    }

    fn validateRecovery(self: *Store, txn: anytype, meta: Metadata, existing: bool) !void {
        var counts: std.AutoHashMapUnmanaged(u64, u32) = .empty;
        defer counts.deinit(self.allocator);
        var buffer: [key_capacity]u8 = undefined;
        const prefix = try self.key(&buffer, "attempt/");
        var count: u32 = 0;
        {
            var cursor = try txn.openCursor();
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(prefix);
            while (entry) |row| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, prefix)) break;
                if (count >= hard_attempts) return error.InvalidCoordinatorJournal;
                const id = try self.attemptRow(row.key, row.value);
                if (!existing or id.generation > meta.generation or id.sequence > meta.sequence) return error.InvalidCoordinatorJournal;
                const target = try self.destination(txn, id.destination) orelse return error.InvalidCoordinatorJournal;
                if (id.worker_namespace == 0 or id.worker_namespace != target.namespace or id.worker_incarnation != target.incarnation) return error.InvalidCoordinatorJournal;
                const value = try counts.getOrPut(self.allocator, id.destination);
                if (!value.found_existing) value.value_ptr.* = 0;
                value.value_ptr.* += 1;
                count += 1;
            }
        }
        if (count != meta.attempts) return error.InvalidCoordinatorJournal;
        const destination_prefix = try self.key(&buffer, "destination/");
        var cursor = try txn.openCursor();
        defer cursor.close();
        var destinations: u32 = 0;
        var entry = try cursor.seekAtOrAfter(destination_prefix);
        while (entry) |row| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, destination_prefix)) break;
            if (destinations >= 256) return error.InvalidCoordinatorJournal;
            const target = try self.decode(Destination, row.value, destination_charge);
            var expected: [key_capacity]u8 = undefined;
            if (!existing or target.node == 0 or target.namespace == 0 or target.incarnation == 0 or target.ready_generation > meta.generation or target.attempts != (counts.get(target.node) orelse 0) or
                !std.mem.eql(u8, row.key, try self.destinationKey(&expected, target.node))) return error.InvalidCoordinatorJournal;
            _ = counts.remove(target.node);
            destinations += 1;
        }
        if (destinations != meta.destinations or counts.count() != 0) return error.InvalidCoordinatorJournal;
    }

    fn retireFenced(self: *Store, txn: anytype, meta: *Metadata, target: *Destination, through: u64) !void {
        var ids: std.ArrayListUnmanaged(protocol.AttemptId) = .empty;
        defer ids.deinit(self.allocator);
        var buffer: [key_capacity]u8 = undefined;
        const prefix = try self.key(&buffer, "attempt/");
        {
            var cursor = try txn.openCursor();
            defer cursor.close();
            var count: usize = 0;
            var entry = try cursor.seekAtOrAfter(prefix);
            while (entry) |row| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, prefix)) break;
                if (count >= hard_attempts) return error.InvalidCoordinatorJournal;
                count += 1;
                const id = try self.attemptRow(row.key, row.value);
                if (id.destination == target.node and id.generation <= through) try ids.append(self.allocator, id);
            }
        }
        for (ids.items) |id| {
            if (meta.attempts == 0 or target.attempts == 0) return error.InvalidCoordinatorJournal;
            try txn.delete(try self.attemptKey(&buffer, id));
            meta.attempts -= 1;
            target.attempts -= 1;
        }
    }

    fn removeIdleDestinations(self: *Store, txn: anytype, meta: *Metadata) !void {
        var nodes: [256]u64 = undefined;
        var count: usize = 0;
        var buffer: [key_capacity]u8 = undefined;
        const prefix = try self.key(&buffer, "destination/");
        {
            var cursor = try txn.openCursor();
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(prefix);
            while (entry) |row| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, prefix)) break;
                const target = try self.decode(Destination, row.value, destination_charge);
                if (target.attempts == 0) {
                    if (count == nodes.len) return error.InvalidCoordinatorJournal;
                    nodes[count] = target.node;
                    count += 1;
                }
            }
        }
        for (nodes[0..count]) |node| {
            try txn.delete(try self.destinationKey(&buffer, node));
            if (meta.destinations == 0) return error.InvalidCoordinatorJournal;
            meta.destinations -= 1;
        }
    }
};

test "workload admission durable coordinator preserves uncertainty and fences replacement generations" {
    const alloc = std.testing.allocator;
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var storage = try backend.runtimeStore(alloc, .{ .name = "system/coordinator-attempts" });
    defer storage.deinit();
    var durable = transactions.DurableSessionStore.initRuntime(alloc, &storage);
    const config: Config = .{ .max_attempts = 2, .max_bytes = 8192, .max_destination_attempts = 1, .max_destinations = 2 };
    var first = try Store.init(alloc, &durable, 7, config);
    defer first.deinit();
    try std.testing.expectEqual(@as(u64, 2), first.generation);
    for ([_]u64{ 8, 9 }) |destination| try first.ready(.{ .version = 1, .coordinator = 7, .destination = destination, .worker_namespace = 44, .worker_incarnation = 10, .fenced_through = 1, .quiesced_through = 1 });
    const uncertain = try first.begin(8, 10, 100);
    try std.testing.expectError(error.AttemptCapacityExhausted, first.begin(8, 10, 101));
    const healthy = try first.begin(9, 10, 101);
    try first.terminal(healthy);
    try first.terminal(healthy);
    var next = try Store.init(alloc, &durable, 7, config);
    defer next.deinit();
    try std.testing.expectEqual(@as(u32, 1), (try next.usage()).attempts);
    try std.testing.expectEqual(@as(u32, 1), (try next.usage()).destinations); // idle destination9 reclaimed only on generation advance
    try std.testing.expectError(error.CoordinatorGenerationSuperseded, first.terminal(uncertain));
    try std.testing.expectError(error.FencingRequired, next.begin(8, 10, 102));
    durable.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, next.ready(.{ .version = 1, .coordinator = 7, .destination = 8, .worker_namespace = 44, .worker_incarnation = 10, .fenced_through = 2, .quiesced_through = 2 }));
    durable.fail_writes_for_test = false;
    try std.testing.expectEqual(@as(u32, 1), (try next.usage()).attempts);
    try next.ready(.{ .version = 1, .coordinator = 7, .destination = 8, .worker_namespace = 44, .worker_incarnation = 10, .fenced_through = 2, .quiesced_through = 2 });
    try std.testing.expectEqual(@as(u32, 0), (try next.usage()).attempts);
    const after = try next.begin(8, 10, 102);
    try std.testing.expect(after.generation > uncertain.generation);
    try next.terminal(after);
}

test "workload admission coordinator rejects mismatched live attempt rows before reconciliation" {
    const alloc = std.testing.allocator;
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var storage = try backend.runtimeStore(alloc, .{ .name = "system/coordinator-attempts" });
    defer storage.deinit();
    var durable = transactions.DurableSessionStore.initRuntime(alloc, &storage);
    var coordinator = try Store.init(alloc, &durable, 7, .{
        .max_attempts = 2,
        .max_bytes = 8192,
        .max_destination_attempts = 2,
        .max_destinations = 2,
    });
    defer coordinator.deinit();
    const fence: protocol.Fence = .{
        .version = 1,
        .coordinator = 7,
        .destination = 8,
        .worker_namespace = 44,
        .worker_incarnation = 10,
        .fenced_through = 1,
        .quiesced_through = 1,
    };
    try coordinator.ready(fence);
    const owned = try coordinator.begin(8, 10, 100);
    var key_buffer: [key_capacity]u8 = undefined;
    const key = try coordinator.attemptKey(&key_buffer, owned);
    {
        var txn = try storage.beginWrite();
        errdefer txn.abort();
        var mismatched = owned;
        mismatched.sequence += 1;
        try coordinator.put(&txn, key, Record{ .id = mismatched }, attempt_charge);
        try txn.commit();
    }
    try std.testing.expectError(error.InvalidCoordinatorJournal, coordinator.acknowledgedThrough(8));
    try std.testing.expectError(error.InvalidCoordinatorJournal, coordinator.pending(alloc, 8));
    try std.testing.expectError(error.InvalidCoordinatorJournal, coordinator.ready(fence));
    try std.testing.expectEqual(@as(u32, 1), (try coordinator.usage()).attempts);
    {
        var txn = try storage.beginWrite();
        errdefer txn.abort();
        try coordinator.put(&txn, key, Record{ .id = owned }, attempt_charge);
        try txn.commit();
    }
    try std.testing.expectEqual(@as(u64, 0), try coordinator.acknowledgedThrough(8));
    const pending = try coordinator.pending(alloc, 8);
    defer alloc.free(pending);
    try std.testing.expectEqualSlices(protocol.AttemptId, &.{owned}, pending);
    try coordinator.terminal(owned);
}
