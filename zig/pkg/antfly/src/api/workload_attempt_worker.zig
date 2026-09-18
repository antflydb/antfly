// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Durable worker admission. Active records never expire into terminal records.
//! A fresh process preserves unknown prior-incarnation work and its capacity.
//! This opt-in prerequisite uses bounded O(N) snapshot mutations. Coordinator
//! rollout requires point-key storage/counters and performance qualification;
//! metrics and duplicate reconciliation already avoid durable writes.
const std = @import("std");
const protocol = @import("workload_attempt_protocol.zig");
const transactions = @import("transactions.zig");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
pub const Config = @import("../common/workload_worker_config.zig").Config;
const base_charge: u64 = 2048;
const record_charge: u64 = 1024;
const fence_charge: u64 = 128;
const hard_record_limit = 4096;
const hard_snapshot_bytes = 64 * 1024 * 1024;

const Record = struct {
    id: protocol.AttemptId,
    digest: [32]u8,
    terminal: bool = false,
};

fn testRequest(incarnation: u64, generation: u64, sequence: u64) protocol.Request {
    return .{ .version = 1, .attempt = .{ .coordinator = 7, .generation = generation, .sequence = sequence, .operation = 4, .destination = 8, .worker_incarnation = incarnation }, .remaining_ns = 100, .request_digest = protocol.requestDigest("POST", "/join", "{}") };
}

test "workload admission worker journal closes before cancellation and retries quiesced durability" {
    const alloc = std.testing.allocator;
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var storage = try backend.runtimeStore(alloc, .{ .name = "system/worker-attempts" });
    defer storage.deinit();
    var durable = transactions.DurableSessionStore.initRuntime(alloc, &storage);
    var worker = try Store.init(alloc, &durable, 8, 10, .{ .max_attempts = 2, .max_bytes = 8192 });
    defer worker.deinit();
    const request = testRequest(10, 2, 1);
    var lease = (try worker.begin(request)).started;
    try std.testing.expect((try worker.begin(request)) == .active);
    var changed = request;
    changed.request_digest[0] ^= 1;
    try std.testing.expectError(error.AttemptIdentityMismatch, worker.begin(changed));
    durable.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, worker.closeGeneration(7, 2, 10));
    try std.testing.expect(!lease.cancellation().isCancelled());
    durable.fail_writes_for_test = false;
    try std.testing.expect((try worker.closeGeneration(7, 2, 10)) == null);
    try std.testing.expect(lease.cancellation().isCancelled());
    try std.testing.expectError(error.AttemptGenerationClosed, worker.begin(testRequest(10, 2, 2)));
    worker.fail_terminal_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, lease.finish());
    try std.testing.expectEqual(@as(usize, 1), (try worker.usage()).attempts);
    worker.fail_terminal_for_test = false;
    const evidence = (try worker.closeGeneration(7, 2, 10)).?;
    try std.testing.expectEqual(@as(u64, 2), evidence.quiesced_through);
    try std.testing.expectEqual(@as(usize, 0), (try worker.usage()).attempts);
    var next = (try worker.begin(testRequest(10, 3, 1))).started;
    try next.finish();
    try std.testing.expect((try worker.begin(testRequest(10, 3, 1))) == .terminal);
}

test "workload admission worker restart preserves uncertainty and reduced durable capacity" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/worker-journal", .{tmp.sub_path});
    defer alloc.free(path);
    {
        var opened = try transactions.OpenedSessionStore.open(alloc, path);
        defer opened.deinit();
        var worker = try Store.init(alloc, opened.durableStore(), 8, 10, .{ .max_attempts = 2, .max_bytes = 8192 });
        defer worker.deinit();
        var first = (try worker.begin(testRequest(10, 2, 1))).started;
        var second = (try worker.begin(testRequest(10, 2, 2))).started;
        try std.testing.expectError(error.AttemptCapacityExhausted, worker.begin(testRequest(10, 2, 3)));
        durableReadCheck: {
            opened.durableStore().fail_writes_for_test = true;
            defer opened.durableStore().fail_writes_for_test = false;
            try std.testing.expect((try worker.begin(testRequest(10, 2, 1))) == .active);
            try std.testing.expectEqual(@as(usize, 2), (try worker.usage()).attempts);
            break :durableReadCheck;
        }
        try second.finish();
        worker.fail_terminal_for_test = true;
        try std.testing.expectError(error.InjectedSessionStoreFailure, first.finish());
        // Closing/reopening storage cannot reconstruct this process's local
        // knowledge that first finished; its durable record remains unknown.
    }
    var opened = try transactions.OpenedSessionStore.open(alloc, path);
    defer opened.deinit();
    var worker = try Store.init(alloc, opened.durableStore(), 8, 11, .{ .max_attempts = 1, .max_bytes = 4096 });
    defer worker.deinit();
    const usage = try worker.usage();
    try std.testing.expectEqual(@as(usize, 2), usage.attempts);
    try std.testing.expectEqual(@as(usize, 1), usage.uncertain);
    try std.testing.expect(usage.bytes > worker.config.max_bytes);
    try std.testing.expectError(error.WorkerUncertaintyUnreconciled, worker.begin(testRequest(11, 3, 1)));
    try std.testing.expect((try worker.closeGeneration(7, 2, 11)) == null);
    try std.testing.expectError(error.AttemptIdentityMismatch, worker.closeGeneration(7, 2, 10));
    try std.testing.expectEqual(@as(usize, 1), (try worker.usage()).uncertain);
}
const Closure = struct { coordinator: u64, through: u64 = 0 };
const Encoded = struct { version: u16, incarnation: u64, records: []Record = &.{}, closures: []Closure = &.{} };
const State = struct {
    incarnation: u64 = 0,
    records: std.ArrayListUnmanaged(Record) = .empty,
    closures: std.ArrayListUnmanaged(Closure) = .empty,
    fn deinit(self: *State, alloc: std.mem.Allocator) void {
        self.records.deinit(alloc);
        self.closures.deinit(alloc);
    }
    fn bytes(self: *const State) u64 {
        return base_charge + self.records.items.len * record_charge + self.closures.items.len * fence_charge;
    }
};

pub const Store = struct {
    allocator: std.mem.Allocator,
    durable: *transactions.DurableSessionStore,
    node_id: u64,
    incarnation: u64,
    config: Config,
    mutex: std.atomic.Mutex = .unlocked,
    live: []Live,
    fail_terminal_for_test: bool = false,

    const Live = struct {
        state: enum { free, executing, quiesced } = .free,
        id: protocol.AttemptId = undefined,
        cancelled: std.atomic.Value(bool) = .init(false),
    };
    pub const Lease = struct {
        owner: ?*Store,
        slot: usize,
        id: protocol.AttemptId,
        pub fn cancellation(self: Lease) CancellationToken {
            return CancellationToken.fromAtomic(&self.owner.?.live[self.slot].cancelled);
        }
        /// Call only after execution and all its temporary state have unwound.
        /// Failed durability keeps a quiesced slot for a later fence to retry.
        pub fn finish(self: *Lease) !void {
            const owner = self.owner orelse return;
            owner.lock();
            const live = &owner.live[self.slot];
            std.debug.assert(live.state != .free and std.meta.eql(live.id, self.id));
            live.state = .quiesced;
            owner.mutex.unlock();
            _ = try owner.transact(.{ .finish = self.id });
            owner.retireQuiesced(self.id);
            self.owner = null;
        }
    };
    pub const Begin = union(enum) { started: Lease, active, terminal };
    pub const Usage = struct { attempts: usize, bytes: u64, uncertain: usize };
    const Op = union(enum) {
        open,
        begin: protocol.Request,
        finish: protocol.AttemptId,
        close: struct { coordinator: u64, through: u64 },
        observe: struct { coordinator: u64, through: u64 },
        usage,
    };
    const Result = struct { disposition: enum { started, active, terminal } = .started, quiescent: bool = false, usage: Usage = .{ .attempts = 0, .bytes = 0, .uncertain = 0 } };

    /// incarnation must be a fresh secure random process identity. Store
    /// continuity is required; restoring/deleting this journal is not fencing.
    pub fn init(alloc: std.mem.Allocator, durable: *transactions.DurableSessionStore, node_id: u64, incarnation: u64, config: Config) !Store {
        try config.validate();
        if (config.max_attempts == 0 or node_id == 0 or incarnation == 0) return error.InvalidConfig;
        const live = try alloc.alloc(Live, config.max_attempts);
        errdefer alloc.free(live);
        for (live) |*entry| entry.* = .{};
        var self: Store = .{ .allocator = alloc, .durable = durable, .node_id = node_id, .incarnation = incarnation, .config = config, .live = live };
        _ = try self.transact(.open);
        return self;
    }

    pub fn deinit(self: *Store) void {
        for (self.live) |entry| std.debug.assert(entry.state != .executing);
        // A failed terminal write remains active in durable storage. Teardown
        // cannot infer terminality on behalf of a subsequently reopened owner.
        self.allocator.free(self.live);
    }

    fn lock(self: *Store) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn begin(self: *Store, request: protocol.Request) !Begin {
        if (request.version != 1 or request.attempt.coordinator == 0 or request.attempt.generation == 0 or
            request.attempt.sequence == 0 or request.attempt.operation == 0 or request.remaining_ns == 0 or
            request.attempt.destination != self.node_id or request.attempt.worker_incarnation != self.incarnation)
            return error.AttemptIdentityMismatch;
        // Reconciliation must work even when every execution slot is occupied.
        // The serialized write transaction below rechecks this read before any
        // new execution can start.
        if (try self.readOnly(request)) |known| return known;
        self.lock();
        const slot = for (self.live, 0..) |*entry, i| {
            if (entry.state == .free) {
                entry.id = request.attempt;
                entry.cancelled.store(false, .release);
                entry.state = .executing;
                break i;
            }
        } else {
            self.mutex.unlock();
            return error.AttemptCapacityExhausted;
        };
        self.mutex.unlock();
        var retained = false;
        defer if (!retained) {
            self.lock();
            self.live[slot].state = .free;
            self.mutex.unlock();
        };
        const result = try self.transact(.{ .begin = request });
        if (result.disposition == .active) return .active;
        if (result.disposition == .terminal) return .terminal;
        retained = true;
        return .{ .started = .{ .owner = self, .slot = slot, .id = request.attempt } };
    }

    fn retireQuiesced(self: *Store, id: protocol.AttemptId) void {
        self.lock();
        defer self.mutex.unlock();
        for (self.live) |*entry| if (entry.state == .quiesced and std.meta.eql(entry.id, id)) {
            entry.state = .free;
            return;
        };
    }

    /// Durable closure precedes cancellation. A null result is nonterminal;
    /// polling does not itself prove that executing or unknown work stopped.
    pub fn closeGeneration(self: *Store, coordinator: u64, through: u64, worker_incarnation: u64) !?protocol.Fence {
        if (coordinator == 0 or through == 0) return error.InvalidAttemptFrame;
        if (worker_incarnation != self.incarnation) return error.AttemptIdentityMismatch;
        _ = try self.transact(.{ .close = .{ .coordinator = coordinator, .through = through } });
        self.lock();
        for (self.live) |*entry| if (entry.state != .free and entry.id.coordinator == coordinator and entry.id.generation <= through) {
            entry.cancelled.store(true, .release);
        };
        self.mutex.unlock();
        // Reconcile only explicit in-process quiescence, never a timeout or a
        // journal entry whose former executor disappeared during restart.
        for (0..self.live.len) |i| {
            self.lock();
            const id = if (self.live[i].state == .quiesced) self.live[i].id else null;
            self.mutex.unlock();
            if (id) |value| {
                _ = try self.transact(.{ .finish = value });
                self.retireQuiesced(value);
            }
        }
        const result = try self.transact(.{ .observe = .{ .coordinator = coordinator, .through = through } });
        if (!result.quiescent) return null;
        return .{ .version = 1, .coordinator = coordinator, .destination = self.node_id, .worker_incarnation = worker_incarnation, .fenced_through = through, .quiesced_through = through };
    }

    pub fn usage(self: *Store) !Usage {
        var state = try self.readState();
        defer state.deinit(self.allocator);
        var result: Usage = .{ .attempts = state.records.items.len, .bytes = state.bytes(), .uncertain = 0 };
        for (state.records.items) |record| if (!record.terminal and record.id.worker_incarnation != self.incarnation) {
            result.uncertain += 1;
        };
        return result;
    }

    fn readOnly(self: *Store, request: protocol.Request) !?Begin {
        var state = try self.readState();
        defer state.deinit(self.allocator);
        if (state.incarnation != self.incarnation) return error.WorkerIncarnationSuperseded;
        for (state.closures.items) |closure| if (closure.coordinator == request.attempt.coordinator and closure.through >= request.attempt.generation)
            return error.AttemptGenerationClosed;
        for (state.records.items) |record| {
            if (record.id.coordinator == request.attempt.coordinator and record.id.generation == request.attempt.generation and record.id.sequence == request.attempt.sequence) {
                if (!std.meta.eql(record.id, request.attempt) or !std.mem.eql(u8, &record.digest, &request.request_digest)) return error.AttemptIdentityMismatch;
                return if (record.terminal) .terminal else .active;
            }
        }
        return null;
    }

    fn readState(self: *Store) !State {
        var key_buffer: [96]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buffer, "workload-attempt-worker/v1/{d}", .{self.node_id});
        const raw = switch (self.durable.backend) {
            .docstore => |store| try store.get(self.allocator, key),
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                break :blk try self.allocator.dupe(u8, try txn.get(key));
            },
        };
        defer self.allocator.free(raw);
        return self.decodeState(raw);
    }

    fn decodeState(self: *Store, raw: []const u8) !State {
        if (raw.len > hard_snapshot_bytes) return error.WorkerJournalTooLarge;
        var decoded = try std.json.parseFromSlice(Encoded, self.allocator, raw, .{});
        defer decoded.deinit();
        if (decoded.value.version != 1 or decoded.value.incarnation == 0 or decoded.value.records.len > hard_record_limit or decoded.value.closures.len > hard_record_limit)
            return error.InvalidWorkerJournal;
        var state: State = .{ .incarnation = decoded.value.incarnation };
        errdefer state.deinit(self.allocator);
        try state.records.appendSlice(self.allocator, decoded.value.records);
        try state.closures.appendSlice(self.allocator, decoded.value.closures);
        return state;
    }

    fn transact(self: *Store, op: Op) !Result {
        if (self.durable.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        if (op == .finish and self.fail_terminal_for_test) return error.InjectedSessionStoreFailure;
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

    fn update(self: *Store, txn: anytype, op: Op) !Result {
        var key_buffer: [96]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buffer, "workload-attempt-worker/v1/{d}", .{self.node_id});
        var state: State = .{};
        defer state.deinit(self.allocator);
        if (txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |raw| {
            state = try self.decodeState(raw);
        }
        var result: Result = .{};
        switch (op) {
            .open => {
                if (state.incarnation == self.incarnation) return error.WorkerIncarnationReused;
                state.incarnation = self.incarnation;
            },
            .begin => |request| {
                if (state.incarnation != self.incarnation) return error.WorkerIncarnationSuperseded;
                for (state.closures.items) |closure| if (closure.coordinator == request.attempt.coordinator and closure.through >= request.attempt.generation)
                    return error.AttemptGenerationClosed;
                for (state.records.items) |record| {
                    if (record.id.coordinator == request.attempt.coordinator and record.id.generation == request.attempt.generation and record.id.sequence == request.attempt.sequence) {
                        if (!std.meta.eql(record.id, request.attempt) or !std.mem.eql(u8, &record.digest, &request.request_digest)) return error.AttemptIdentityMismatch;
                        result.disposition = if (record.terminal) .terminal else .active;
                        return result;
                    }
                    if (!record.terminal and record.id.worker_incarnation != self.incarnation) return error.WorkerUncertaintyUnreconciled;
                }
                if (state.records.items.len >= self.config.max_attempts) return error.AttemptCapacityExhausted;
                try self.ensureCoordinator(&state, request.attempt.coordinator);
                try state.records.append(self.allocator, .{ .id = request.attempt, .digest = request.request_digest });
                if (state.bytes() > self.config.max_bytes) return error.AttemptCapacityExhausted;
            },
            .finish => |id| {
                for (state.records.items) |*record| {
                    if (std.meta.eql(record.id, id)) {
                        record.terminal = true;
                        break;
                    }
                } else return error.AttemptIdentityMismatch;
            },
            .close => |close| {
                if (state.incarnation != self.incarnation) return error.WorkerIncarnationSuperseded;
                try self.ensureCoordinator(&state, close.coordinator);
                for (state.closures.items) |*closure| if (closure.coordinator == close.coordinator) {
                    closure.through = @max(closure.through, close.through);
                };
            },
            .observe => |observe| {
                if (state.incarnation != self.incarnation) return error.WorkerIncarnationSuperseded;
                result.quiescent = true;
                for (state.records.items) |record| if (record.id.coordinator == observe.coordinator and record.id.generation <= observe.through and !record.terminal) {
                    result.quiescent = false;
                };
            },
            .usage => {},
        }
        // Only durable generation closure lets terminal tombstones be removed.
        var index: usize = 0;
        while (index < state.records.items.len) {
            const record = state.records.items[index];
            const closed = for (state.closures.items) |closure| {
                if (closure.coordinator == record.id.coordinator and closure.through >= record.id.generation) break true;
            } else false;
            if (record.terminal and closed) {
                _ = state.records.swapRemove(index);
            } else index += 1;
        }
        result.usage = .{ .attempts = state.records.items.len, .bytes = state.bytes(), .uncertain = 0 };
        for (state.records.items) |record| if (!record.terminal and record.id.worker_incarnation != self.incarnation) {
            result.usage.uncertain += 1;
        };
        const encoded = try std.json.Stringify.valueAlloc(self.allocator, Encoded{ .version = 1, .incarnation = state.incarnation, .records = state.records.items, .closures = state.closures.items }, .{});
        defer self.allocator.free(encoded);
        if (encoded.len > state.bytes() or encoded.len > hard_snapshot_bytes) return error.WorkerJournalTooLarge;
        try txn.put(key, encoded);
        return result;
    }

    fn ensureCoordinator(self: *Store, state: *State, coordinator: u64) !void {
        for (state.closures.items) |closure| if (closure.coordinator == coordinator) return;
        if (state.closures.items.len >= self.config.max_attempts or state.bytes() + fence_charge > self.config.max_bytes)
            return error.AttemptCapacityExhausted;
        try state.closures.append(self.allocator, .{ .coordinator = coordinator });
    }
};
