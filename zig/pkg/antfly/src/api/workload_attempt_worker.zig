// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Durable worker admission. Active records never expire into terminal records.
//! A fresh process preserves unknown prior-incarnation work and its capacity.
//! Point-key mutations and counters share one serialized write transaction.
//! Only startup reconstruction and generation fences scan bounded records.
const std = @import("std");
const protocol = @import("workload_attempt_protocol.zig");
const transactions = @import("transactions.zig");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
pub const Config = @import("../common/workload_worker_config.zig").Config;
const base_charge: u64 = 2048;
const record_charge: u64 = 1024;
const fence_charge: u64 = 384;
const hard_record_limit = 4096;
const key_capacity = 192;

const Record = struct {
    id: protocol.AttemptId,
    digest: [32]u8,
    terminal: bool = false,
};

fn testRequest(incarnation: u64, generation: u64, sequence: u64) protocol.Request {
    return .{ .version = 1, .attempt = .{ .coordinator = 7, .generation = generation, .sequence = sequence, .operation = 4, .destination = 8, .worker_incarnation = incarnation }, .remaining_ns = 100, .request_digest = protocol.requestDigest("POST", "/join", "{}") };
}

test "workload admission worker durable rejection floor preserves active work and rejects delayed sends" {
    const alloc = std.testing.allocator;
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var storage = try backend.runtimeStore(alloc, .{ .name = "system/worker-rejection-floor" });
    defer storage.deinit();
    var durable = transactions.DurableSessionStore.initRuntime(alloc, &storage);
    var worker = try Store.init(alloc, &durable, 8, 10, .{ .max_attempts = 1, .max_bytes = 8192 });
    defer worker.deinit();
    _ = try worker.closeGeneration(7, 1, worker.incarnation);
    var first = testRequest(worker.incarnation, 2, 1);
    first.version = 3;
    first.attempt.worker_namespace = worker.namespace;
    var live = (try worker.begin(first)).started;
    var third = first;
    third.attempt.sequence = 3;
    try std.testing.expect((try worker.begin(third)) == .rejected);
    try std.testing.expect(try worker.terminalStatus(third.attempt));
    try std.testing.expect(!try worker.terminalStatus(first.attempt));
    try std.testing.expect(!live.cancellation().isCancelled());
    try std.testing.expect((try worker.begin(first)) == .active);
    var delayed = first;
    delayed.attempt.sequence = 2;
    try std.testing.expect((try worker.begin(delayed)) == .rejected);
    var fourth = first;
    fourth.attempt.sequence = 4;
    durable.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, worker.begin(fourth));
    durable.fail_writes_for_test = false;
    try std.testing.expect(!try worker.terminalStatus(fourth.attempt));
    try live.finish();
    fourth.acknowledged_through = 3;
    var resumed = (try worker.begin(fourth)).started;
    try resumed.finish();
    try std.testing.expectEqual(@as(usize, 1), (try worker.usage()).attempts);
}

test "workload admission worker acknowledgements reclaim terminal records without retiring live work" {
    const alloc = std.testing.allocator;
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var storage = try backend.runtimeStore(alloc, .{ .name = "system/worker-acknowledgements" });
    defer storage.deinit();
    var durable = transactions.DurableSessionStore.initRuntime(alloc, &storage);
    var worker = try Store.init(alloc, &durable, 8, 10, .{ .max_attempts = 2, .max_bytes = 8192 });
    defer worker.deinit();
    try std.testing.expect((try worker.closeGeneration(7, 1, 10)) != null);
    var first = testRequest(10, 2, 1);
    first.version = 2;
    var first_lease = (try worker.begin(first)).started;
    var second = testRequest(10, 2, 2);
    second.version = 2;
    second.acknowledged_through = 1;
    var second_lease = (try worker.begin(second)).started;
    try std.testing.expectEqual(@as(usize, 2), (try worker.usage()).attempts);
    try second_lease.finish();
    var third = testRequest(10, 2, 3);
    third.version = 2;
    third.acknowledged_through = 2;
    try std.testing.expectError(error.AttemptCapacityExhausted, worker.begin(third));
    try std.testing.expect(!first_lease.cancellation().isCancelled());
    try first_lease.finish();
    var third_lease = (try worker.begin(third)).started;
    try std.testing.expectEqual(@as(usize, 1), (try worker.usage()).attempts);
    try std.testing.expect((try worker.begin(first)) == .terminal);
    try third_lease.finish();
    // The compacted rejection floor survives reopening, so delayed duplicates
    // cannot execute even after their individual records have been removed.
    var reopened = try Store.init(alloc, &durable, 8, 11, .{ .max_attempts = 2, .max_bytes = 8192 });
    defer reopened.deinit();
    first.attempt.worker_incarnation = 11;
    try std.testing.expect((try reopened.begin(first)) == .terminal);
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
    // Only the independently established exclusive native restart proof can
    // retire this old local execution. Failure to persist retains the debt.
    opened.durableStore().fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, worker.recoverPriorIncarnationAfterExclusiveRestart());
    opened.durableStore().fail_writes_for_test = false;
    try std.testing.expectEqual(@as(usize, 1), (try worker.usage()).uncertain);
    try worker.recoverPriorIncarnationAfterExclusiveRestart();
    try std.testing.expectEqual(@as(usize, 0), (try worker.usage()).uncertain);
    try std.testing.expect((try worker.closeGeneration(7, 2, 11)) != null);
    try std.testing.expectEqual(@as(usize, 0), (try worker.usage()).attempts);
}
test "workload admission worker point journal refuses legacy state and only actual old execution reconciles" {
    const alloc = std.testing.allocator;
    var backend = @import("../storage/mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var storage = try backend.runtimeStore(alloc, .{ .name = "system/worker-point-journal" });
    defer storage.deinit();
    var durable = transactions.DurableSessionStore.initRuntime(alloc, &storage);
    {
        var txn = try storage.beginWrite();
        errdefer txn.abort();
        try txn.put("workload-attempt-worker/v1/9", "{}");
        try txn.commit();
    }
    try std.testing.expectError(error.WorkerJournalMigrationRequired, Store.init(alloc, &durable, 9, 10, .{ .max_attempts = 2, .max_bytes = 8192 }));
    var previous = try Store.init(alloc, &durable, 8, 10, .{ .max_attempts = 2, .max_bytes = 8192 });
    defer previous.deinit();
    var lease = (try previous.begin(testRequest(10, 2, 1))).started;
    var current = try Store.init(alloc, &durable, 8, 11, .{ .max_attempts = 2, .max_bytes = 8192 });
    defer current.deinit();
    try std.testing.expectEqual(@as(usize, 1), (try current.usage()).uncertain);
    try std.testing.expectError(error.WorkerIncarnationSuperseded, previous.begin(testRequest(10, 3, 1)));
    try std.testing.expectError(error.WorkerUncertaintyUnreconciled, current.begin(testRequest(11, 3, 1)));
    try std.testing.expectError(error.WorkerIncarnationSuperseded, previous.closeGeneration(7, 2, 10));
    try std.testing.expect(!lease.cancellation().isCancelled());
    try std.testing.expect((try current.closeGeneration(7, 2, 11)) == null);
    // Only the still-live previous executor can supply actual completion.
    // Reopening and fencing alone left this uncertainty and charge intact.
    try lease.finish();
    try std.testing.expectEqual(@as(usize, 0), (try current.usage()).uncertain);
    try std.testing.expectEqual(@as(usize, 0), (try current.usage()).attempts);
    try std.testing.expect((try current.closeGeneration(7, 2, 11)) != null);
    var next = (try current.begin(testRequest(11, 3, 1))).started;
    try next.finish();
}

const Closure = struct {
    coordinator: u64,
    through: u64 = 0,
    acknowledged_generation: u64 = 0,
    acknowledged_through: u64 = 0,
    // This journal is already scoped to one immutable worker namespace.
    rejected_generation: u64 = 0,
    rejected_through: u64 = 0,
};
const Metadata = struct {
    version: u16 = 3,
    namespace: u128,
    initial_epoch: u64,
    incarnation: u64,
    attempts: u32 = 0,
    coordinators: u32 = 0,
    active: u32 = 0,
    uncertain: u32 = 0,

    fn bytes(self: Metadata) u64 {
        return base_charge + @as(u64, self.attempts) * record_charge + @as(u64, self.coordinators) * fence_charge;
    }
    fn validate(self: Metadata) !void {
        if (self.version != 3 or self.namespace == 0 or self.initial_epoch == 0 or self.incarnation < self.initial_epoch or self.attempts > hard_record_limit or self.coordinators > hard_record_limit or
            self.active > self.attempts or self.uncertain > self.active) return error.InvalidWorkerJournal;
    }
};

pub const Store = struct {
    allocator: std.mem.Allocator,
    durable: *transactions.DurableSessionStore,
    node_id: u64,
    incarnation: u64,
    namespace: u128 = 0,
    exclusive_restart_proven: bool = false,
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
    pub const Begin = union(enum) { started: Lease, active, terminal, rejected };
    pub const Usage = struct { attempts: usize, bytes: u64, uncertain: usize };
    const Op = union(enum) {
        open,
        recover_exclusive_restart,
        begin: protocol.Request,
        reject: protocol.Request,
        finish: protocol.AttemptId,
        close: struct { coordinator: u64, through: u64 },
        observe: struct { coordinator: u64, through: u64 },
    };
    const Result = struct { disposition: enum { started, active, terminal, rejected } = .started, quiescent: bool = false };

    /// The first incarnation seeds a monotonic epoch. Subsequent opens advance
    /// the journal's epoch atomically; caller entropy never replaces its UUID.
    /// Store continuity is required; deleting/restoring it is not fencing.
    pub fn init(alloc: std.mem.Allocator, durable: *transactions.DurableSessionStore, node_id: u64, incarnation: u64, config: Config) !Store {
        try config.validate();
        if (config.max_attempts == 0 or node_id == 0 or incarnation == 0) return error.InvalidConfig;
        const live = try alloc.alloc(Live, config.max_attempts);
        errdefer alloc.free(live);
        for (live) |*entry| entry.* = .{};
        var self: Store = .{ .allocator = alloc, .durable = durable, .node_id = node_id, .incarnation = incarnation, .config = config, .live = live };
        var threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer threaded.deinit();
        while (self.namespace == 0) try threaded.io().randomSecure(std.mem.asBytes(&self.namespace));
        _ = try self.transact(.open);
        return self;
    }

    pub fn deinit(self: *Store) void {
        for (self.live) |entry| std.debug.assert(entry.state != .executing);
        // A failed terminal write remains active in durable storage. Teardown
        // cannot infer terminality on behalf of a subsequently reopened owner.
        self.allocator.free(self.live);
    }

    /// Native startup only, before publishing this owner. The caller must
    /// prove exclusive ownership of this exact journal across the entire
    /// worker lifetime, and that all previous local execution is gone. A new
    /// random incarnation, timeout, journal copy, or network partition is not
    /// such proof. Remote child debt remains in its separate coordinator log.
    pub fn recoverPriorIncarnationAfterExclusiveRestart(self: *Store) !void {
        for (self.live) |entry| if (entry.state != .free) return error.InvalidWorkerRecoveryState;
        _ = try self.transact(.recover_exclusive_restart);
        self.exclusive_restart_proven = true;
    }

    fn lock(self: *Store) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn begin(self: *Store, original: protocol.Request) !Begin {
        var request = original;
        // Compatibility is internal to mechanism tests. Production ingress
        // requires V3; every persisted record nevertheless has exact identity.
        if (request.version < 3 and request.attempt.worker_namespace == 0) request.attempt.worker_namespace = self.namespace;
        if ((request.version != 1 and request.version != 2 and request.version != 3) or request.attempt.coordinator == 0 or request.attempt.generation == 0 or
            request.attempt.sequence == 0 or request.attempt.operation == 0 or request.remaining_ns == 0 or
            request.acknowledged_through >= request.attempt.sequence or (request.version == 1 and request.acknowledged_through != 0) or
            request.attempt.destination != self.node_id or request.attempt.worker_incarnation != self.incarnation or
            request.attempt.worker_namespace != self.namespace)
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
            return if (request.version == 3) self.reject(request) else error.AttemptCapacityExhausted;
        };
        self.mutex.unlock();
        var retained = false;
        defer if (!retained) {
            self.lock();
            self.live[slot].state = .free;
            self.mutex.unlock();
        };
        const result = self.transact(.{ .begin = request }) catch |err| {
            if (err == error.AttemptCapacityExhausted and request.version == 3) return self.reject(request);
            return err;
        };
        if (result.disposition == .active) return .active;
        if (result.disposition == .terminal) return .terminal;
        if (result.disposition == .rejected) return .rejected;
        retained = true;
        return .{ .started = .{ .owner = self, .slot = slot, .id = request.attempt } };
    }

    fn reject(self: *Store, request: protocol.Request) !Begin {
        const result = try self.transact(.{ .reject = request });
        return switch (result.disposition) {
            .active => .active,
            .terminal => .terminal,
            .rejected => .rejected,
            .started => unreachable,
        };
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
        const result = try self.readOperation(.{ .observe = .{ .coordinator = coordinator, .through = through } });
        if (!result.quiescent) return null;
        return .{ .version = 1, .coordinator = coordinator, .destination = self.node_id, .worker_incarnation = worker_incarnation, .worker_namespace = self.namespace, .fenced_through = through, .quiesced_through = through };
    }

    pub fn usage(self: *Store) !Usage {
        const meta = (try self.readOperation(.usage)).metadata.?;
        return .{ .attempts = meta.attempts, .bytes = meta.bytes(), .uncertain = meta.uncertain };
    }

    pub fn terminalStatus(self: *Store, id: protocol.AttemptId) !bool {
        if (id.destination != self.node_id or id.coordinator == 0 or id.generation == 0 or id.sequence == 0) return error.AttemptIdentityMismatch;
        return (try self.readOperation(.{ .terminal = id })).quiescent;
    }

    fn readOnly(self: *Store, request: protocol.Request) !?Begin {
        return (try self.readOperation(.{ .duplicate = request })).known;
    }

    const Read = union(enum) { usage, duplicate: protocol.Request, terminal: protocol.AttemptId, observe: struct { coordinator: u64, through: u64 } };
    const ReadResult = struct { metadata: ?Metadata = null, known: ?Begin = null, quiescent: bool = false };

    fn readOperation(self: *Store, op: Read) !ReadResult {
        return switch (self.durable.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :blk try self.readTxn(&txn, op);
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                break :blk try self.readTxn(&txn, op);
            },
        };
    }

    fn readTxn(self: *Store, txn: anytype, op: Read) !ReadResult {
        const meta = try self.loadMetadata(txn) orelse return error.InvalidWorkerJournal;
        if (op == .usage) return .{ .metadata = meta };
        if (meta.incarnation != self.incarnation) return error.WorkerIncarnationSuperseded;
        switch (op) {
            .duplicate => |request| return .{ .known = try self.knownAttempt(txn, request) },
            .terminal => |id| {
                if (id.worker_namespace != 0 and id.worker_namespace != meta.namespace) return error.AttemptIdentityMismatch;
                if (self.exclusive_restart_proven and id.worker_namespace == meta.namespace and
                    id.worker_incarnation >= meta.initial_epoch and id.worker_incarnation < meta.incarnation) return .{ .quiescent = true };
                if (try self.loadClosure(txn, id.coordinator)) |closure| {
                    if (closure.through >= id.generation) return .{ .quiescent = try self.observe(txn, id.coordinator, id.generation) };
                    if (closure.acknowledged_generation == id.generation and id.sequence <= closure.acknowledged_through) return .{ .quiescent = true };
                }
                var buffer: [key_capacity]u8 = undefined;
                const record = try self.get(Record, txn, try self.attemptKey(&buffer, id), record_charge) orelse {
                    if (try self.loadClosure(txn, id.coordinator)) |closure|
                        if (closure.rejected_generation == id.generation and id.sequence <= closure.rejected_through) return .{ .quiescent = true };
                    return .{};
                };
                if (!std.meta.eql(record.id, id)) return error.AttemptIdentityMismatch;
                return .{ .quiescent = record.terminal };
            },
            .observe => |value| return .{ .quiescent = try self.observe(txn, value.coordinator, value.through) },
            .usage => unreachable,
        }
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

    fn metadataKey(self: *Store, buffer: *[key_capacity]u8) ![]const u8 {
        return std.fmt.bufPrint(buffer, "workload-attempt-worker/v3/{d}/metadata", .{self.node_id});
    }
    fn attemptsPrefix(self: *Store, buffer: *[key_capacity]u8, coordinator: ?u64) ![]const u8 {
        return if (coordinator) |id|
            std.fmt.bufPrint(buffer, "workload-attempt-worker/v3/{d}/attempt/{x:0>16}/", .{ self.node_id, id })
        else
            std.fmt.bufPrint(buffer, "workload-attempt-worker/v3/{d}/attempt/", .{self.node_id});
    }
    fn attemptKey(self: *Store, buffer: *[key_capacity]u8, id: protocol.AttemptId) ![]const u8 {
        return std.fmt.bufPrint(buffer, "workload-attempt-worker/v3/{d}/attempt/{x:0>16}/{x:0>16}/{x:0>16}", .{ self.node_id, id.coordinator, id.generation, id.sequence });
    }
    fn closureKey(self: *Store, buffer: *[key_capacity]u8, coordinator: u64) ![]const u8 {
        return std.fmt.bufPrint(buffer, "workload-attempt-worker/v3/{d}/fence/{x:0>16}", .{ self.node_id, coordinator });
    }

    fn decode(self: *Store, comptime T: type, raw: []const u8, limit: u64) !T {
        if (raw.len > limit) return error.WorkerJournalTooLarge;
        var parsed = try std.json.parseFromSlice(T, self.allocator, raw, .{});
        defer parsed.deinit();
        // All records contain only scalar fields and fixed arrays.
        return parsed.value;
    }
    fn get(self: *Store, comptime T: type, txn: anytype, key: []const u8, limit: u64) !?T {
        const raw = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try self.decode(T, raw, limit);
    }
    fn put(self: *Store, txn: anytype, key: []const u8, value: anytype, charge: u64) !void {
        const raw = try std.json.Stringify.valueAlloc(self.allocator, value, .{});
        defer self.allocator.free(raw);
        if (key.len + raw.len > charge) return error.WorkerJournalTooLarge;
        try txn.put(key, raw);
    }
    fn loadMetadata(self: *Store, txn: anytype) !?Metadata {
        var buffer: [key_capacity]u8 = undefined;
        const value = try self.get(Metadata, txn, try self.metadataKey(&buffer), base_charge) orelse return null;
        try value.validate();
        return value;
    }
    fn loadClosure(self: *Store, txn: anytype, coordinator: u64) !?Closure {
        var buffer: [key_capacity]u8 = undefined;
        const value = try self.get(Closure, txn, try self.closureKey(&buffer, coordinator), fence_charge) orelse return null;
        if (value.coordinator != coordinator) return error.InvalidWorkerJournal;
        return value;
    }
    fn knownAttempt(self: *Store, txn: anytype, request: protocol.Request) !?Begin {
        if (try self.loadClosure(txn, request.attempt.coordinator)) |closure| {
            if (closure.through >= request.attempt.generation) return error.AttemptGenerationClosed;
            if (closure.acknowledged_generation == request.attempt.generation and request.attempt.sequence <= closure.acknowledged_through) return .terminal;
        }
        var buffer: [key_capacity]u8 = undefined;
        const record = try self.get(Record, txn, try self.attemptKey(&buffer, request.attempt), record_charge) orelse {
            if (try self.loadClosure(txn, request.attempt.coordinator)) |closure|
                if (closure.rejected_generation == request.attempt.generation and request.attempt.sequence <= closure.rejected_through) return .rejected;
            return null;
        };
        if (!std.meta.eql(record.id, request.attempt) or !std.mem.eql(u8, &record.digest, &request.request_digest)) return error.AttemptIdentityMismatch;
        return if (record.terminal) .terminal else .active;
    }

    fn update(self: *Store, txn: anytype, op: Op) !Result {
        const previous = try self.loadMetadata(txn);
        if (previous == null and op != .open) return error.InvalidWorkerJournal;
        var meta = previous orelse Metadata{ .namespace = self.namespace, .initial_epoch = self.incarnation, .incarnation = self.incarnation };
        var result: Result = .{};
        switch (op) {
            .open => {
                // Old binaries do not maintain v2 counters. Refuse an earlier
                // journal until an explicit offline migration fences writers;
                // ignoring its unknown attempts would manufacture capacity.
                var legacy_buffer: [key_capacity]u8 = undefined;
                const legacy_key = try std.fmt.bufPrint(&legacy_buffer, "workload-attempt-worker/v1/{d}", .{self.node_id});
                if (txn.get(legacy_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                }) |_| return error.WorkerJournalMigrationRequired;
                const v2_key = try std.fmt.bufPrint(&legacy_buffer, "workload-attempt-worker/v2/{d}/metadata", .{self.node_id});
                if (txn.get(v2_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                }) |_| return error.WorkerJournalMigrationRequired;
                if (previous) |value| {
                    if (value.incarnation == std.math.maxInt(u64)) return error.WorkerEpochExhausted;
                    self.namespace = value.namespace;
                    self.incarnation = value.incarnation + 1;
                }
                meta = try self.reconstruct(txn);
                meta.initial_epoch = if (previous) |value| value.initial_epoch else self.incarnation;
                if (previous == null and (meta.attempts != 0 or meta.coordinators != 0)) return error.InvalidWorkerJournal;
            },
            .recover_exclusive_restart => {
                if (meta.incarnation != self.incarnation) return error.WorkerIncarnationSuperseded;
                var records: std.ArrayListUnmanaged(Record) = .empty;
                defer records.deinit(self.allocator);
                var prefix_buffer: [key_capacity]u8 = undefined;
                const prefix = try self.attemptsPrefix(&prefix_buffer, null);
                {
                    var cursor = try txn.openCursor();
                    defer cursor.close();
                    var entry = try cursor.seekAtOrAfter(prefix);
                    var count: usize = 0;
                    while (entry) |row| : (entry = try cursor.next()) {
                        if (!std.mem.startsWith(u8, row.key, prefix)) break;
                        count += 1;
                        if (count > hard_record_limit) return error.WorkerJournalTooLarge;
                        var record = try self.decode(Record, row.value, record_charge);
                        try self.validateRecord(row.key, record);
                        if (!record.terminal and record.id.worker_incarnation != self.incarnation) {
                            record.terminal = true;
                            try records.append(self.allocator, record);
                        }
                    }
                }
                if (records.items.len != meta.uncertain or records.items.len > meta.active) return error.InvalidWorkerJournal;
                for (records.items) |record| {
                    var key_buffer: [key_capacity]u8 = undefined;
                    try self.put(txn, try self.attemptKey(&key_buffer, record.id), record, record_charge);
                }
                meta.active -= @intCast(records.items.len);
                meta.uncertain = 0;
            },
            .begin => |request| {
                if (meta.incarnation != self.incarnation) return error.WorkerIncarnationSuperseded;
                if (request.version >= 2) try self.reapAcknowledged(txn, &meta, request);
                if (try self.knownAttempt(txn, request)) |known| {
                    result.disposition = switch (known) {
                        .terminal => .terminal,
                        .active => .active,
                        .rejected => .rejected,
                        .started => unreachable,
                    };
                    return result;
                }
                if (meta.uncertain != 0) return error.WorkerUncertaintyUnreconciled;
                if (meta.attempts >= self.config.max_attempts) return error.AttemptCapacityExhausted;
                _ = try self.ensureCoordinator(txn, &meta, request.attempt.coordinator);
                if (meta.bytes() + record_charge > self.config.max_bytes) return error.AttemptCapacityExhausted;
                var buffer: [key_capacity]u8 = undefined;
                try self.put(txn, try self.attemptKey(&buffer, request.attempt), Record{ .id = request.attempt, .digest = request.request_digest }, record_charge);
                meta.attempts += 1;
                meta.active += 1;
            },
            .reject => |request| {
                if (meta.incarnation != self.incarnation) return error.WorkerIncarnationSuperseded;
                if (try self.knownAttempt(txn, request)) |known| {
                    result.disposition = switch (known) {
                        .terminal => .terminal,
                        .active => .active,
                        .rejected => .rejected,
                        .started => unreachable,
                    };
                    return result;
                }
                var closure = try self.ensureCoordinator(txn, &meta, request.attempt.coordinator);
                if (closure.rejected_generation > request.attempt.generation) return error.AttemptGenerationClosed;
                if (closure.rejected_generation != request.attempt.generation) {
                    closure.rejected_generation = request.attempt.generation;
                    closure.rejected_through = 0;
                }
                closure.rejected_through = @max(closure.rejected_through, request.attempt.sequence);
                var key_buffer: [key_capacity]u8 = undefined;
                try self.put(txn, try self.closureKey(&key_buffer, closure.coordinator), closure, fence_charge);
                result.disposition = .rejected;
            },
            .finish => |id| {
                var buffer: [key_capacity]u8 = undefined;
                const key = try self.attemptKey(&buffer, id);
                const closure = try self.loadClosure(txn, id.coordinator) orelse return error.InvalidWorkerJournal;
                var record = try self.get(Record, txn, key, record_charge) orelse {
                    // A concurrent fence can already have persisted this same
                    // quiescent completion and removed its closed tombstone.
                    if (closure.through >= id.generation) return result;
                    return error.AttemptIdentityMismatch;
                };
                if (!std.meta.eql(record.id, id)) return error.AttemptIdentityMismatch;
                if (!record.terminal) {
                    if (meta.active == 0) return error.InvalidWorkerJournal;
                    meta.active -= 1;
                    if (id.worker_incarnation != meta.incarnation) {
                        if (meta.uncertain == 0) return error.InvalidWorkerJournal;
                        meta.uncertain -= 1;
                    }
                }
                if (closure.through >= id.generation) {
                    try txn.delete(key);
                    if (meta.attempts == 0) return error.InvalidWorkerJournal;
                    meta.attempts -= 1;
                } else {
                    record.terminal = true;
                    try self.put(txn, key, record, record_charge);
                }
            },
            .close => |close| {
                if (meta.incarnation != self.incarnation) return error.WorkerIncarnationSuperseded;
                var closure = try self.ensureCoordinator(txn, &meta, close.coordinator);
                closure.through = @max(closure.through, close.through);
                var buffer: [key_capacity]u8 = undefined;
                try self.put(txn, try self.closureKey(&buffer, close.coordinator), closure, fence_charge);
                try self.removeClosedTerminals(txn, &meta, closure);
            },
            .observe => unreachable,
        }
        try meta.validate();
        var buffer: [key_capacity]u8 = undefined;
        try self.put(txn, try self.metadataKey(&buffer), meta, base_charge);
        return result;
    }

    fn ensureCoordinator(self: *Store, txn: anytype, meta: *Metadata, coordinator: u64) !Closure {
        if (try self.loadClosure(txn, coordinator)) |value| return value;
        if (meta.coordinators >= self.config.max_attempts or meta.bytes() + fence_charge > self.config.max_bytes)
            return error.AttemptCapacityExhausted;
        const value: Closure = .{ .coordinator = coordinator };
        var buffer: [key_capacity]u8 = undefined;
        try self.put(txn, try self.closureKey(&buffer, coordinator), value, fence_charge);
        meta.coordinators += 1;
        return value;
    }

    fn reapAcknowledged(self: *Store, txn: anytype, meta: *Metadata, request: protocol.Request) !void {
        var closure = try self.ensureCoordinator(txn, meta, request.attempt.coordinator);
        if (closure.through >= request.attempt.generation) return error.AttemptGenerationClosed;
        if (closure.acknowledged_generation != request.attempt.generation) {
            if (closure.through < request.attempt.generation - 1) return error.FencingRequired;
            closure.acknowledged_generation = request.attempt.generation;
            closure.acknowledged_through = 0;
        }
        var buffer: [key_capacity]u8 = undefined;
        var count: usize = 0;
        while (closure.acknowledged_through < request.acknowledged_through and count < hard_record_limit) : (count += 1) {
            var candidate = request.attempt;
            candidate.sequence = closure.acknowledged_through + 1;
            const name = try self.attemptKey(&buffer, candidate);
            const record = try self.get(Record, txn, name, record_charge) orelse {
                // V3 acknowledges durable retirement of every lower sequence,
                // including sends proven never delivered or fenced old epochs.
                // V2 only sent a high-water mark and cannot make this claim.
                if (request.version < 3) break;
                closure.acknowledged_through += 1;
                continue;
            };
            if (!record.terminal) break; // A sender acknowledgement never cancels or retires live work.
            try self.validateRecord(name, record);
            if (meta.attempts == 0) return error.InvalidWorkerJournal;
            try txn.delete(name);
            meta.attempts -= 1;
            closure.acknowledged_through += 1;
        }
        try self.put(txn, try self.closureKey(&buffer, closure.coordinator), closure, fence_charge);
    }

    fn validateRecord(self: *Store, key: []const u8, record: Record) !void {
        const id = record.id;
        if (id.worker_namespace != self.namespace) return error.InvalidWorkerJournal;
        if (id.coordinator == 0 or id.generation == 0 or id.sequence == 0 or id.operation == 0 or id.worker_incarnation == 0 or id.destination != self.node_id)
            return error.InvalidWorkerJournal;
        var buffer: [key_capacity]u8 = undefined;
        if (!std.mem.eql(u8, key, try self.attemptKey(&buffer, id))) return error.InvalidWorkerJournal;
    }

    fn reconstruct(self: *Store, txn: anytype) !Metadata {
        var meta: Metadata = .{ .namespace = self.namespace, .initial_epoch = self.incarnation, .incarnation = self.incarnation };
        var prefix_buffer: [key_capacity]u8 = undefined;
        const prefix = try self.attemptsPrefix(&prefix_buffer, null);
        {
            var cursor = try txn.openCursor();
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(prefix);
            while (entry) |row| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, prefix)) break;
                if (meta.attempts >= hard_record_limit) return error.WorkerJournalTooLarge;
                const record = try self.decode(Record, row.value, record_charge);
                try self.validateRecord(row.key, record);
                if (record.id.worker_incarnation == self.incarnation) return error.WorkerIncarnationReused;
                if ((try self.loadClosure(txn, record.id.coordinator)) == null) return error.InvalidWorkerJournal;
                meta.attempts += 1;
                if (!record.terminal) {
                    meta.active += 1;
                    meta.uncertain += 1;
                }
            }
        }
        const fence_prefix = try std.fmt.bufPrint(&prefix_buffer, "workload-attempt-worker/v3/{d}/fence/", .{self.node_id});
        var cursor = try txn.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(fence_prefix);
        while (entry) |row| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, fence_prefix)) break;
            if (meta.coordinators >= hard_record_limit) return error.WorkerJournalTooLarge;
            const closure = try self.decode(Closure, row.value, fence_charge);
            var buffer: [key_capacity]u8 = undefined;
            if (closure.coordinator == 0 or !std.mem.eql(u8, row.key, try self.closureKey(&buffer, closure.coordinator))) return error.InvalidWorkerJournal;
            meta.coordinators += 1;
        }
        return meta;
    }

    fn removeClosedTerminals(self: *Store, txn: anytype, meta: *Metadata, closure: Closure) !void {
        var ids: std.ArrayListUnmanaged(protocol.AttemptId) = .empty;
        defer ids.deinit(self.allocator);
        var prefix_buffer: [key_capacity]u8 = undefined;
        const prefix = try self.attemptsPrefix(&prefix_buffer, closure.coordinator);
        {
            var cursor = try txn.openCursor();
            defer cursor.close();
            var count: usize = 0;
            var entry = try cursor.seekAtOrAfter(prefix);
            while (entry) |row| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, prefix)) break;
                if (count >= hard_record_limit) return error.WorkerJournalTooLarge;
                count += 1;
                const record = try self.decode(Record, row.value, record_charge);
                try self.validateRecord(row.key, record);
                if (record.terminal and record.id.generation <= closure.through) try ids.append(self.allocator, record.id);
            }
        }
        // Never mutate the cursor's backing storage while borrowed keys exist.
        for (ids.items) |id| {
            var buffer: [key_capacity]u8 = undefined;
            try txn.delete(try self.attemptKey(&buffer, id));
            if (meta.attempts == 0) return error.InvalidWorkerJournal;
            meta.attempts -= 1;
        }
    }

    fn observe(self: *Store, txn: anytype, coordinator: u64, through: u64) !bool {
        const closure = try self.loadClosure(txn, coordinator) orelse return false;
        if (closure.through < through) return false;
        var buffer: [key_capacity]u8 = undefined;
        const prefix = try self.attemptsPrefix(&buffer, coordinator);
        var cursor = try txn.openCursor();
        defer cursor.close();
        var count: usize = 0;
        var entry = try cursor.seekAtOrAfter(prefix);
        while (entry) |row| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, prefix)) break;
            if (count >= hard_record_limit) return error.WorkerJournalTooLarge;
            count += 1;
            const record = try self.decode(Record, row.value, record_charge);
            try self.validateRecord(row.key, record);
            if (record.id.generation <= through and !record.terminal) return false;
        }
        return true;
    }
};
