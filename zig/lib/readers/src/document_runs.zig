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

//! Host-independent durable document run protocol. Payload storage is supplied
//! by Antfly artifacts; mutable ownership/lease state belongs to a CAS store.
const std = @import("std");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
pub const Ref = struct { id: []const u8, bytes: usize, sha256: Digest };
pub const Identity = struct { request: Digest, sources: Digest, definition: Digest, models: Digest, runtime: Digest };
pub const Charge = struct {
    calls: u64 = 0,
    tokens: u64 = 0,
    pixels: u64 = 0,
    output_bytes: u64 = 0,
    source_bytes: u64 = 0,
    fn plus(left: Charge, right: Charge) !Charge {
        var result: Charge = .{};
        inline for (@typeInfo(Charge).@"struct".field_names) |name|
            @field(result, name) = std.math.add(u64, @field(left, name), @field(right, name)) catch return error.DocumentBudgetExhausted;
        return result;
    }
    fn within(self: Charge, ceiling: Charge) bool {
        inline for (@typeInfo(Charge).@"struct".field_names) |name| if (@field(self, name) > @field(ceiling, name)) return false;
        return true;
    }
    fn minus(left: Charge, right: Charge) !Charge {
        if (!right.within(left)) return error.InvalidDocumentCheckpoint;
        var result: Charge = .{};
        inline for (@typeInfo(Charge).@"struct".field_names) |name| @field(result, name) = @field(left, name) - @field(right, name);
        return result;
    }
};
pub const Reservation = struct { key: Digest, attempt: u64, nonce: [8]u8, upper: Charge };
pub const Record = struct {
    format: u32 = 1,
    run_id: []const u8,
    owner: []const u8,
    revision: u64 = 1,
    attempt: u64 = 0,
    nonce: [8]u8 = @splat(0),
    lease_until_ms: u64 = 0,
    state: enum { queued, running, succeeded, failed, cancelled } = .queued,
    identity: Identity,
    limits: Charge,
    used: Charge = .{},
    /// Conservative charges for calls dispatched by superseded attempts.
    uncertain: Charge = .{},
    inputs: []const Ref,
    head: ?Ref = null,
    sequence: u64 = 0,
    result: ?Ref = null,
    pending: []const Reservation = &.{},
    pub fn version(self: Record) Version {
        return .{ .revision = self.revision, .attempt = self.attempt, .nonce = self.nonce };
    }
};
pub const Version = struct { revision: u64, attempt: u64, nonce: [8]u8 };
pub const Scope = struct { run_id: []const u8, owner: []const u8, attempt: u64, nonce: [8]u8 };
pub const Lease = Scope;
pub const Checkpoint = struct { format: u32 = 1, sequence: u64 = 0, key: Digest, payload: Ref, events: ?Ref = null, parent: ?Ref };
pub const Completed = struct { payload: Ref, events: ?Ref, sequence: u64 };
pub const EventGroup = struct { sequence: u64, key: Digest, payload: Ref };

/// The host authorizes metadata by principal, atomically compares revisions,
/// and retains payload reachability roots until run expiry. `put` uses a fenced
/// run/attempt upload scope, including for blobs orphaned by a failed CAS.
/// Native inference depends on this interface rather than database internals.
pub const Store = struct {
    ptr: *anyopaque,
    load_fn: *const fn (*anyopaque, A, []const u8, []const u8) anyerror!?Record,
    cas_fn: *const fn (*anyopaque, ?Version, Record) anyerror!bool,
    nonce_fn: *const fn (*anyopaque, []u8) void,
    put_fn: *const fn (*anyopaque, A, Scope, []const u8) anyerror!Ref,
    get_fn: *const fn (*anyopaque, A, Ref, usize) anyerror![]u8,

    pub fn load(self: Store, a: A, id: []const u8, owner: []const u8) !Record {
        const record = (try self.load_fn(self.ptr, a, id, owner)) orelse return error.DocumentRunNotFound;
        if (record.format != 1 or !std.mem.eql(u8, record.run_id, id) or !std.mem.eql(u8, record.owner, owner)) return error.DocumentRunNotFound;
        if (record.revision == 0 or !record.used.within(record.limits) or record.pending.len > 64) return error.InvalidDocumentCheckpoint;
        if ((record.head == null) != (record.sequence == 0)) return error.InvalidDocumentCheckpoint;
        var outstanding = record.uncertain;
        for (record.pending) |pending| outstanding = try outstanding.plus(pending.upper);
        if (!outstanding.within(record.used)) return error.InvalidDocumentCheckpoint;
        return record;
    }
    pub fn read(self: Store, a: A, ref: Ref, max_bytes: usize) ![]u8 {
        if (ref.bytes > max_bytes or ref.id.len == 0 or ref.id.len > 256) return error.DocumentOutputTooLarge;
        const bytes = try self.get_fn(self.ptr, a, ref, max_bytes);
        errdefer a.free(bytes);
        var digest: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        if (bytes.len != ref.bytes or !std.mem.eql(u8, &digest, &ref.sha256)) return error.InvalidDocumentCheckpoint;
        return bytes;
    }
    fn put(self: Store, a: A, scope: Scope, bytes: []const u8) !Ref {
        const ref = try self.put_fn(self.ptr, a, scope, bytes);
        var digest: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        if (ref.id.len == 0 or ref.id.len > 256 or ref.bytes != bytes.len or !std.mem.eql(u8, &ref.sha256, &digest)) return error.InvalidDocumentCheckpoint;
        return ref;
    }
    pub fn create(self: Store, record: Record) !void {
        if (record.format != 1 or record.run_id.len == 0 or record.owner.len == 0 or record.attempt != 0 or record.revision != 1 or record.state != .queued or record.head != null or record.sequence != 0 or record.result != null or record.pending.len != 0 or !std.meta.eql(record.used, Charge{}) or !std.meta.eql(record.uncertain, Charge{})) return error.InvalidDocumentCheckpoint;
        if (!try self.cas_fn(self.ptr, null, record)) return error.DocumentRunExists;
    }
    pub fn claim(self: Store, a: A, id: []const u8, owner: []const u8, identity: Identity, now_ms: u64, lease_ms: u64) !Lease {
        if (lease_ms == 0 or lease_ms > 1_800_000) return error.InvalidDocumentCheckpoint;
        var nonce: [8]u8 = undefined;
        self.nonce_fn(self.ptr, &nonce);
        if (std.mem.allEqual(u8, &nonce, 0)) return error.InvalidDocumentCheckpoint;
        for (0..16) |_| {
            var record = try self.load(a, id, owner);
            if (!std.meta.eql(identity, record.identity)) return error.DocumentModelRevisionMismatch;
            if (record.state == .succeeded or record.state == .cancelled or record.state == .failed) return error.DocumentRunTerminal;
            if (record.state == .running and now_ms < record.lease_until_ms) return error.DocumentRunBusy;
            const expected = record.version();
            const previous = record.revision;
            record.attempt = std.math.add(u64, record.attempt, 1) catch return error.InvalidDocumentCheckpoint;
            record.revision = std.math.add(u64, previous, 1) catch return error.InvalidDocumentCheckpoint;
            record.lease_until_ms = std.math.add(u64, now_ms, lease_ms) catch return error.InvalidDocumentCheckpoint;
            record.state = .running;
            record.nonce = nonce;
            // Old in-flight steps may no longer be chosen by an adaptive
            // policy. Retire their dispatch intents, retaining every charge.
            for (record.pending) |pending| record.uncertain = try record.uncertain.plus(pending.upper);
            record.pending = &.{};
            if (try self.cas_fn(self.ptr, expected, record)) return .{ .run_id = id, .owner = owner, .attempt = record.attempt, .nonce = nonce };
        }
        return error.DocumentRunBusy;
    }
    fn current(self: Store, a: A, lease: Lease, now_ms: u64) !Record {
        const record = try self.load(a, lease.run_id, lease.owner);
        if (record.state != .running or record.attempt != lease.attempt or !std.mem.eql(u8, &record.nonce, &lease.nonce) or now_ms >= record.lease_until_ms) return error.DocumentRunLeaseLost;
        return record;
    }
    /// Persist an upper bound before dispatch. Returns false for an already
    /// reserved call in this attempt: the caller must not dispatch it twice.
    /// A new attempt charges again for ambiguous calls left by an old worker.
    pub fn reserve(self: Store, a: A, lease: Lease, key: Digest, upper: Charge, now_ms: u64) !bool {
        for (0..16) |_| {
            var record = try self.current(a, lease, now_ms);
            var existing: ?usize = null;
            for (record.pending, 0..) |pending, i| if (std.mem.eql(u8, &pending.key, &key)) {
                if (pending.attempt == lease.attempt and std.mem.eql(u8, &pending.nonce, &lease.nonce)) return false;
                existing = i;
                break;
            };
            const charged = try record.used.plus(upper);
            if (!charged.within(record.limits)) return error.DocumentBudgetExhausted;
            const count = record.pending.len + @as(usize, @intFromBool(existing == null));
            if (count > 64) return error.DocumentBudgetExhausted;
            const pending = try a.alloc(Reservation, count);
            @memcpy(pending[0..record.pending.len], record.pending);
            pending[existing orelse record.pending.len] = .{ .key = key, .attempt = lease.attempt, .nonce = lease.nonce, .upper = upper };
            const expected = record.version();
            const previous = record.revision;
            record.pending = pending;
            record.used = charged;
            record.revision = std.math.add(u64, previous, 1) catch return error.InvalidDocumentCheckpoint;
            if (try self.cas_fn(self.ptr, expected, record)) return true;
        }
        return error.DocumentRunBusy;
    }
    /// Payload durability precedes the fenced checkpoint commit. Failed CAS
    /// attempts may leave scoped blobs for the host's ordinary artifact GC.
    /// Reconcile actual usage and publish the accepted step in one transition.
    pub fn complete(self: Store, a: A, lease: Lease, key: Digest, actual: Charge, payload: []const u8, now_ms: u64) !Ref {
        return (try self.completeWithEvents(a, lease, key, actual, payload, "", now_ms)).payload;
    }
    /// Publish an opaque encoded event chunk in the same fenced transition as
    /// the stage result. Chunk bytes consume the reserved output allowance.
    /// Sequence IDs are assigned by the successful CAS, never by a worker's
    /// wall clock or a process-local event counter.
    pub fn completeWithEvents(self: Store, a: A, lease: Lease, key: Digest, actual: Charge, payload: []const u8, events: []const u8, now_ms: u64) !Completed {
        const admitted = try self.current(a, lease, now_ms);
        if (events.len > 64 * 1024 or events.len > actual.output_bytes or payload.len > @max(actual.output_bytes - events.len, actual.source_bytes) or actual.output_bytes > admitted.limits.output_bytes or actual.source_bytes > admitted.limits.source_bytes) return error.DocumentOutputTooLarge;
        const reservation = for (admitted.pending) |pending| {
            if (pending.attempt == lease.attempt and std.mem.eql(u8, &pending.nonce, &lease.nonce) and std.mem.eql(u8, &pending.key, &key)) break pending;
        } else return error.InvalidDocumentCheckpoint;
        if (!actual.within(reservation.upper)) return error.DocumentBudgetExhausted;
        const value = try self.put(a, lease, payload);
        const event_ref = if (events.len > 0) try self.put(a, lease, events) else null;
        for (0..16) |_| {
            var record = try self.current(a, lease, now_ms);
            const index = for (record.pending, 0..) |pending, i| {
                if (pending.attempt == lease.attempt and std.mem.eql(u8, &pending.nonce, &lease.nonce) and std.mem.eql(u8, &pending.key, &key)) break i;
            } else return error.InvalidDocumentCheckpoint;
            if (!actual.within(record.pending[index].upper)) return error.DocumentBudgetExhausted;
            const remaining = try record.used.minus(record.pending[index].upper);
            const charged = try remaining.plus(actual);
            const sequence = std.math.add(u64, record.sequence, 1) catch return error.InvalidDocumentCheckpoint;
            const group = Checkpoint{ .key = key, .payload = value, .events = event_ref, .sequence = sequence, .parent = record.head };
            const encoded = try std.json.Stringify.valueAlloc(a, group, .{});
            const head = try self.put(a, lease, encoded);
            const pending = try a.alloc(Reservation, record.pending.len - 1);
            @memcpy(pending[0..index], record.pending[0..index]);
            @memcpy(pending[index..], record.pending[index + 1 ..]);
            const expected = record.version();
            const previous = record.revision;
            record.used = charged;
            record.head = head;
            record.sequence = sequence;
            record.pending = pending;
            record.revision = std.math.add(u64, previous, 1) catch return error.InvalidDocumentCheckpoint;
            if (try self.cas_fn(self.ptr, expected, record)) return .{ .payload = value, .events = event_ref, .sequence = sequence };
        }
        return error.DocumentRunBusy;
    }
    pub fn renew(self: Store, a: A, lease: Lease, now_ms: u64, lease_ms: u64) !void {
        if (lease_ms == 0 or lease_ms > 1_800_000) return error.InvalidDocumentCheckpoint;
        for (0..16) |_| {
            var record = try self.current(a, lease, now_ms);
            const expected = record.version();
            const previous = record.revision;
            record.lease_until_ms = std.math.add(u64, now_ms, lease_ms) catch return error.InvalidDocumentCheckpoint;
            record.revision = std.math.add(u64, previous, 1) catch return error.InvalidDocumentCheckpoint;
            if (try self.cas_fn(self.ptr, expected, record)) return;
        }
        return error.DocumentRunBusy;
    }
    pub fn cancel(self: Store, a: A, id: []const u8, owner: []const u8) !void {
        for (0..16) |_| {
            var record = try self.load(a, id, owner);
            if (record.state == .succeeded or record.state == .cancelled) return;
            const expected = record.version();
            const previous = record.revision;
            record.state = .cancelled;
            record.lease_until_ms = 0;
            record.revision = std.math.add(u64, previous, 1) catch return error.InvalidDocumentCheckpoint;
            if (try self.cas_fn(self.ptr, expected, record)) return;
        }
        return error.DocumentRunBusy;
    }
    pub fn finish(self: Store, a: A, lease: Lease, result: Ref, now_ms: u64) !void {
        for (0..16) |_| {
            var record = try self.current(a, lease, now_ms);
            if (record.pending.len != 0) return error.InvalidDocumentCheckpoint;
            // Final publication must refer to the most recently acknowledged
            // stage, rather than an arbitrary or merely uploaded artifact.
            const head = record.head orelse return error.InvalidDocumentCheckpoint;
            const raw = try self.read(a, head, 4096);
            const group = try std.json.parseFromSliceLeaky(Checkpoint, a, raw, .{ .duplicate_field_behavior = .@"error" });
            if (group.format != 1 or group.sequence != record.sequence or group.payload.bytes != result.bytes or !std.mem.eql(u8, group.payload.id, result.id) or !std.mem.eql(u8, &group.payload.sha256, &result.sha256)) return error.InvalidDocumentCheckpoint;
            const expected = record.version();
            const previous = record.revision;
            record.state = .succeeded;
            record.result = result;
            record.lease_until_ms = 0;
            record.revision = std.math.add(u64, previous, 1) catch return error.InvalidDocumentCheckpoint;
            if (try self.cas_fn(self.ptr, expected, record)) return;
        }
        return error.DocumentRunBusy;
    }
};

/// Scan the committed chain once when resuming, then use O(1) stage lookup.
/// Never rewrite the acknowledged prefix or embed all prior output in a record.
pub const Index = struct {
    arena: std.heap.ArenaAllocator,
    stages: std.AutoHashMapUnmanaged(Digest, Ref) = .empty,
    event_groups: std.ArrayList(EventGroup) = .empty,
    sequence: u64 = 0,
    pub fn init(a: A, store: Store, record: Record, max_stages: usize, max_manifest_bytes: usize) !Index {
        var result = Index{ .arena = .init(a), .sequence = record.sequence };
        errdefer result.deinit();
        const work = result.arena.allocator();
        var cursor = record.head;
        var bytes_left = max_manifest_bytes;
        var expected = record.sequence;
        while (cursor) |ref| {
            if (result.stages.count() >= max_stages or ref.bytes > bytes_left) return error.DocumentOutputTooLarge;
            bytes_left -= ref.bytes;
            const raw = try store.read(work, ref, bytes_left + ref.bytes);
            const group = try std.json.parseFromSliceLeaky(Checkpoint, work, raw, .{ .duplicate_field_behavior = .@"error" });
            if (group.format != 1 or group.payload.id.len == 0 or group.payload.id.len > 256 or group.payload.bytes > @max(record.limits.output_bytes, record.limits.source_bytes)) return error.InvalidDocumentCheckpoint;
            if (expected == 0 or group.sequence != expected) return error.InvalidDocumentCheckpoint;
            expected -= 1;
            if (group.events) |events| {
                if (events.id.len == 0 or events.id.len > 256 or events.bytes == 0 or events.bytes > @min(record.limits.output_bytes, 64 * 1024)) return error.InvalidDocumentCheckpoint;
                try result.event_groups.append(work, .{ .sequence = group.sequence, .key = group.key, .payload = events });
            }
            const entry = try result.stages.getOrPut(work, group.key);
            if (entry.found_existing) return error.InvalidDocumentCheckpoint;
            entry.value_ptr.* = group.payload;
            cursor = group.parent;
        }
        if (expected != 0) return error.InvalidDocumentCheckpoint;
        std.mem.reverse(EventGroup, result.event_groups.items);
        return result;
    }
    pub fn get(self: *const Index, key: Digest) ?Ref {
        return self.stages.get(key);
    }
    /// The owner authenticates the run before opening this index. Cursors are
    /// sequence numbers, never caller-supplied artifact references. Loading a
    /// run scans its journal once; replay pages then cost O(log N + page size).
    pub fn eventsAfter(self: *const Index, cursor: u64, max_groups: usize) ![]const EventGroup {
        if (cursor > self.sequence or max_groups == 0 or max_groups > 1024) return error.InvalidDocumentCheckpoint;
        var low: usize = 0;
        var high = self.event_groups.items.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (self.event_groups.items[middle].sequence <= cursor) low = middle + 1 else high = middle;
        }
        return self.event_groups.items[low..][0..@min(max_groups, self.event_groups.items.len - low)];
    }
    pub fn deinit(self: *Index) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// A bounded, concurrent executor facade over the durable protocol. Stage
/// payloads are immutable; successful stages replay without invoking providers.
/// A failed or ambiguous dispatch remains charged and cannot be retried by the
/// same attempt. The scheduler must acquire a new lease to retry that work.
pub const Session = struct {
    allocator: A,
    store: Store,
    lease: Lease,
    identity: Identity,
    clock: Clock,
    stages: std.AutoHashMapUnmanaged(Digest, Slot) = .empty,
    mutex: std.atomic.Mutex = .unlocked,
    max_stages: usize,

    const Slot = struct {
        complete: bool = false,
        id: [256]u8 = undefined,
        id_len: usize = 0,
        bytes: usize = 0,
        sha256: Digest = @splat(0),
        fn set(self: *Slot, ref: Ref) void {
            @memcpy(self.id[0..ref.id.len], ref.id);
            self.id_len = ref.id.len;
            self.bytes = ref.bytes;
            self.sha256 = ref.sha256;
            self.complete = true;
        }
    };
    pub const Output = struct { bytes: []const u8, events: []const u8 = "", actual: Charge };
    pub const Result = struct { bytes: []u8, replayed: bool };
    pub const Clock = struct {
        ptr: ?*anyopaque = null,
        now_fn: *const fn (?*anyopaque) u64,
        fn now(self: Clock) u64 {
            return self.now_fn(self.ptr);
        }
    };

    /// A lease is acquired by the task owner, never implicitly by a provider
    /// callback. Loading the index once keeps replay lookup independent of the
    /// length of the acknowledged journal.
    pub fn init(a: A, store: Store, lease: Lease, identity: Identity, clock: Clock, max_stages: usize, max_manifest_bytes: usize) !Session {
        if (max_stages == 0 or max_stages > 1_000_000) return error.InvalidDocumentCheckpoint;
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const record = try store.current(scratch.allocator(), lease, clock.now());
        if (!std.meta.eql(record.identity, identity)) return error.DocumentModelRevisionMismatch;
        var index = try Index.init(a, store, record, max_stages, max_manifest_bytes);
        defer index.deinit();
        var result = Session{ .allocator = a, .store = store, .lease = lease, .identity = identity, .clock = clock, .max_stages = max_stages };
        errdefer result.deinit();
        // No map growth or ownership allocation after a checkpoint commits.
        try result.stages.ensureTotalCapacity(a, @intCast(max_stages));
        var entries = index.stages.iterator();
        while (entries.next()) |entry| {
            var slot = Slot{};
            slot.set(entry.value_ptr.*);
            result.stages.putAssumeCapacity(entry.key_ptr.*, slot);
        }
        return result;
    }
    pub fn deinit(self: *Session) void {
        self.stages.deinit(self.allocator);
        self.* = undefined;
    }
    fn lock(self: *Session) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    /// Stage identity includes the complete pinned run identity. Callers add
    /// source/page/region/model/operation and renderer options in `description`.
    pub fn key(self: *const Session, description: []const u8) Digest {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-document-stage-v1");
        inline for (@typeInfo(Identity).@"struct".field_names) |name| hash.update(&@field(self.identity, name));
        hash.update(description);
        return hash.finalResult();
    }
    pub fn execute(self: *Session, a: A, stage: Digest, upper: Charge, max_payload_bytes: usize, context: anytype, comptime invoke: anytype) !Result {
        {
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            _ = try self.store.current(scratch.allocator(), self.lease, self.clock.now());
        }
        self.lock();
        if (self.stages.get(stage)) |slot| {
            self.mutex.unlock();
            if (!slot.complete) return error.DocumentStageInFlight;
            // `slot` owns its inline identifier: do not borrow a map pointer
            // while other workers publish different stages.
            return .{ .bytes = try self.store.read(a, .{ .id = slot.id[0..slot.id_len], .bytes = slot.bytes, .sha256 = slot.sha256 }, max_payload_bytes), .replayed = true };
        }
        if (self.stages.count() >= self.max_stages) {
            self.mutex.unlock();
            return error.DocumentOutputTooLarge;
        }
        self.stages.putAssumeCapacity(stage, .{});
        self.mutex.unlock();
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const work = scratch.allocator();
        if (!try self.store.reserve(work, self.lease, stage, upper, self.clock.now())) return error.DocumentStageInFlight;
        const output: Output = try invoke(context, work);
        if (output.bytes.len > max_payload_bytes) return error.DocumentOutputTooLarge;
        const committed = try self.store.completeWithEvents(work, self.lease, stage, output.actual, output.bytes, output.events, self.clock.now());
        self.lock();
        self.stages.getPtr(stage).?.set(committed.payload);
        self.mutex.unlock();
        return .{ .bytes = try a.dupe(u8, output.bytes), .replayed = false };
    }
};

const TestStore = struct {
    a: A,
    metadata: ?[]u8 = null,
    blobs: std.StringHashMapUnmanaged([]u8) = .empty,
    reject_once: bool = false,
    cas_error_once: bool = false,
    corrupt: bool = false,
    entropy: u8 = 0,
    fn nonce(raw: *anyopaque, bytes: []u8) void {
        const self: *TestStore = @ptrCast(@alignCast(raw));
        self.entropy +%= 1;
        @memset(bytes, self.entropy);
    }
    fn interface(self: *TestStore) Store {
        return .{ .ptr = self, .load_fn = load, .cas_fn = cas, .nonce_fn = nonce, .put_fn = put, .get_fn = get };
    }
    fn deinit(self: *TestStore) void {
        if (self.metadata) |bytes| self.a.free(bytes);
        var it = self.blobs.iterator();
        while (it.next()) |entry| {
            self.a.free(entry.key_ptr.*);
            self.a.free(entry.value_ptr.*);
        }
        self.blobs.deinit(self.a);
    }
    fn load(raw: *anyopaque, a: A, id: []const u8, owner: []const u8) !?Record {
        const self: *TestStore = @ptrCast(@alignCast(raw));
        const bytes = self.metadata orelse return null;
        const record = try std.json.parseFromSliceLeaky(Record, a, bytes, .{ .allocate = .alloc_always });
        if (!std.mem.eql(u8, record.run_id, id) or !std.mem.eql(u8, record.owner, owner)) return null;
        return record;
    }
    fn cas(raw: *anyopaque, expected: ?Version, record: Record) !bool {
        const self: *TestStore = @ptrCast(@alignCast(raw));
        if (self.cas_error_once) {
            self.cas_error_once = false;
            return error.TestStorageUnavailable;
        }
        if (self.reject_once and expected != null) {
            self.reject_once = false;
            return false;
        }
        if (self.metadata) |bytes| {
            var previous = try std.json.parseFromSlice(Record, self.a, bytes, .{});
            defer previous.deinit();
            if (expected == null or !std.meta.eql(previous.value.version(), expected.?) or !std.mem.eql(u8, previous.value.owner, record.owner) or record.revision != expected.?.revision + 1) return false;
        } else if (expected != null) return false;
        const bytes = try std.json.Stringify.valueAlloc(self.a, record, .{});
        if (self.metadata) |old| self.a.free(old);
        self.metadata = bytes;
        return true;
    }
    fn put(raw: *anyopaque, a: A, _: Scope, bytes: []const u8) !Ref {
        const self: *TestStore = @ptrCast(@alignCast(raw));
        var hash: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
        const id = std.fmt.bytesToHex(hash, .lower);
        if (!self.blobs.contains(&id)) {
            const copy = try self.a.dupe(u8, bytes);
            errdefer self.a.free(copy);
            const key = try self.a.dupe(u8, &id);
            errdefer self.a.free(key);
            try self.blobs.put(self.a, key, copy);
        }
        return .{ .id = try a.dupe(u8, &id), .sha256 = hash, .bytes = bytes.len };
    }
    fn get(raw: *anyopaque, a: A, ref: Ref, max_bytes: usize) ![]u8 {
        const self: *TestStore = @ptrCast(@alignCast(raw));
        const bytes = self.blobs.get(ref.id) orelse return error.NotFound;
        if (bytes.len > max_bytes) return error.DocumentOutputTooLarge;
        return a.dupe(u8, if (self.corrupt) "corrupt" else bytes);
    }
};

test "document durable checkpoints fence stale attempts and retain crash reservations" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "run", .owner = "alice", .identity = identity, .limits = .{ .calls = 5, .tokens = 100, .pixels = 100, .output_bytes = 1024 }, .inputs = &.{} });
    try std.testing.expectError(error.DocumentRunNotFound, store.load(work, "run", "bob"));
    const first = try store.claim(work, "run", "alice", identity, 1, 10);
    try std.testing.expectError(error.DocumentRunBusy, store.claim(work, "run", "alice", identity, 2, 10));
    const key: Digest = @splat(7);
    const upper: Charge = .{ .calls = 1, .tokens = 20, .pixels = 10, .output_bytes = 64 };
    memory.reject_once = true;
    try std.testing.expect(try store.reserve(work, first, key, upper, 2));
    try std.testing.expect(!try store.reserve(work, first, key, upper, 2));
    try std.testing.expectEqual(@as(u64, 1), (try store.load(work, "run", "alice")).used.calls);
    // A new process/worker reconstructs its index and claims an expired lease.
    const second = try store.claim(work, "run", "alice", identity, 12, 10);
    try std.testing.expectError(error.DocumentRunLeaseLost, store.complete(work, first, key, .{}, "", 13));
    try std.testing.expect(try store.reserve(work, second, key, upper, 13));
    const ref = try store.complete(work, second, key, .{ .calls = 1, .tokens = 7, .pixels = 4, .output_bytes = 4 }, "page", 14);
    const record = try store.load(work, "run", "alice");
    try std.testing.expectEqual(@as(u64, 2), record.used.calls);
    try std.testing.expectEqual(@as(u64, 27), record.used.tokens);
    try std.testing.expectEqual(@as(u64, 20), record.uncertain.tokens);
    try std.testing.expectEqual(@as(usize, 0), record.pending.len);
    var index = try Index.init(a, store, record, 100, 16 * 1024);
    defer index.deinit();
    const checkpoint = index.get(key).?;
    const value = try store.read(work, checkpoint, 4);
    try std.testing.expectEqualStrings("page", value);
    memory.corrupt = true;
    try std.testing.expectError(error.InvalidDocumentCheckpoint, store.read(work, checkpoint, 16));
    memory.corrupt = false;
    try std.testing.expectError(error.DocumentOutputTooLarge, store.read(work, checkpoint, 3));
    try store.renew(work, second, 15, 20);
    try store.finish(work, second, ref, 16);
    try std.testing.expectError(error.DocumentRunTerminal, store.claim(work, "run", "alice", identity, 17, 10));
}

test "document durable cancellation fences pending publication and identity changes reject resume" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "run", .owner = "alice", .identity = identity, .limits = .{ .calls = 1, .tokens = 10, .pixels = 10, .output_bytes = 10 }, .inputs = &.{} });
    var changed = identity;
    changed.sources[0] ^= 1;
    try std.testing.expectError(error.DocumentModelRevisionMismatch, store.claim(work, "run", "alice", changed, 1, 10));
    const lease = try store.claim(work, "run", "alice", identity, 1, 10);
    try std.testing.expect(try store.reserve(work, lease, @splat(8), .{ .calls = 1, .tokens = 10 }, 2));
    try std.testing.expectError(error.DocumentBudgetExhausted, store.reserve(work, lease, @splat(9), .{ .calls = 1 }, 2));
    try store.cancel(work, "run", "alice");
    try std.testing.expectError(error.DocumentRunLeaseLost, store.complete(work, lease, @splat(8), .{}, "", 3));
    const record = try store.load(work, "run", "alice");
    try std.testing.expectEqual(@as(u64, 1), record.used.calls);
    try std.testing.expectEqual(@as(u64, 10), record.used.tokens);
    try std.testing.expect(record.head == null);
}

test "document durable attempt nonces fence old workers after metadata rollback" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "run", .owner = "alice", .identity = identity, .limits = .{ .calls = 4, .output_bytes = 16 }, .inputs = &.{} });
    _ = try store.claim(work, "run", "alice", identity, 1, 10);
    const backup = try a.dupe(u8, memory.metadata.?);
    const old = try store.claim(work, "run", "alice", identity, 12, 10);
    var late = try store.load(work, "run", "alice");
    const expected = late.version();
    late.revision += 1;
    memory.a.free(memory.metadata.?);
    memory.metadata = backup;
    const current = try store.claim(work, "run", "alice", identity, 12, 10);
    try std.testing.expectEqual(old.attempt, current.attempt);
    try std.testing.expect(!std.mem.eql(u8, &old.nonce, &current.nonce));
    try std.testing.expect(!try memory.interface().cas_fn(&memory, expected, late));
    try std.testing.expectError(error.DocumentRunLeaseLost, store.reserve(work, old, @splat(9), .{ .calls = 1 }, 13));
    try std.testing.expect(try store.reserve(work, current, @splat(9), .{ .calls = 1 }, 13));
}

test "document durable session replays accepted stages and retries only ambiguous work after restart" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "run", .owner = "alice", .identity = identity, .limits = .{ .calls = 5, .tokens = 100, .output_bytes = 1024 }, .inputs = &.{} });
    const Fake = struct {
        store: Store,
        now: u64 = 1,
        calls: usize = 0,
        fail: bool = false,
        expire: bool = false,
        fn clock(raw: ?*anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.now;
        }
        fn invoke(self: *@This(), alloc: A) !Session.Output {
            const record = try self.store.load(alloc, "run", "alice");
            // Dispatch is acknowledged and the entire upper charge is durable
            // before a provider gets control.
            try std.testing.expect(record.pending.len > 0);
            self.calls += 1;
            if (self.fail) return error.ProviderConnectionLost;
            if (self.expire) self.now += 20;
            return .{ .bytes = "page", .actual = .{ .calls = 1, .tokens = 3, .output_bytes = 4 } };
        }
    };
    var fake = Fake{ .store = store };
    const clock: Session.Clock = .{ .ptr = &fake, .now_fn = Fake.clock };
    const upper: Charge = .{ .calls = 1, .tokens = 10, .output_bytes = 64 };
    const lease = try store.claim(work, "run", "alice", identity, fake.now, 10);
    var first = try Session.init(a, store, lease, identity, clock, 8, 16384);
    defer first.deinit();
    const accepted = first.key("source:0/page:1/read:fast");
    const ambiguous = first.key("source:0/page:2/read:fast");
    const initial = try first.execute(a, accepted, upper, 64, &fake, Fake.invoke);
    defer a.free(initial.bytes);
    try std.testing.expect(!initial.replayed);
    fake.fail = true;
    try std.testing.expectError(error.ProviderConnectionLost, first.execute(a, ambiguous, upper, 64, &fake, Fake.invoke));
    try std.testing.expectError(error.DocumentStageInFlight, first.execute(a, ambiguous, upper, 64, &fake, Fake.invoke));
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    fake.fail = false;
    fake.now = 12;
    const reclaimed = try store.claim(work, "run", "alice", identity, fake.now, 10);
    var resumed = try Session.init(a, store, reclaimed, identity, clock, 8, 16384);
    defer resumed.deinit();
    const replayed = try resumed.execute(a, accepted, upper, 64, &fake, Fake.invoke);
    defer a.free(replayed.bytes);
    try std.testing.expect(replayed.replayed);
    try std.testing.expectEqualStrings("page", replayed.bytes);
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    const retried = try resumed.execute(a, ambiguous, upper, 64, &fake, Fake.invoke);
    defer a.free(retried.bytes);
    const record = try store.load(work, "run", "alice");
    try std.testing.expectEqual(@as(u64, 3), record.used.calls);
    try std.testing.expectEqual(@as(u64, 16), record.used.tokens);
    try std.testing.expectEqual(@as(u64, 10), record.uncertain.tokens);
    fake.expire = true;
    try std.testing.expectError(error.DocumentRunLeaseLost, resumed.execute(a, resumed.key("page:3"), upper, 64, &fake, Fake.invoke));
    try std.testing.expectError(error.DocumentRunLeaseLost, resumed.execute(a, accepted, upper, 64, &fake, Fake.invoke));
    try std.testing.expectEqual(@as(usize, 4), fake.calls);
}

test "document durable recovery rejects malformed manifest references before populating the stage cache" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "run", .owner = "alice", .identity = identity, .limits = .{ .calls = 1, .output_bytes = 1024 }, .inputs = &.{} });
    const lease = try store.claim(work, "run", "alice", identity, 1, 10);
    const oversized_id = try work.alloc(u8, 257);
    @memset(oversized_id, 'a');
    const raw = try std.json.Stringify.valueAlloc(work, Checkpoint{ .key = @splat(8), .payload = .{ .id = oversized_id, .bytes = 4, .sha256 = @splat(0) }, .parent = null }, .{});
    var record = try store.load(work, "run", "alice");
    const expected = record.version();
    record.head = try store.put(work, lease, raw);
    record.sequence = 1;
    record.revision += 1;
    try std.testing.expect(try store.cas_fn(store.ptr, expected, record));
    try std.testing.expectError(error.InvalidDocumentCheckpoint, Index.init(a, store, record, 8, 16384));
}

test "document durable events commit atomically with stages and replay by stable sequence after crashes" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "run", .owner = "alice", .identity = identity, .limits = .{ .calls = 5, .output_bytes = 512 }, .inputs = &.{} });
    const lease = try store.claim(work, "run", "alice", identity, 1, 10);
    const upper: Charge = .{ .calls = 1, .output_bytes = 64 };
    const event = "{\"phase\":\"accepted\"}\n";
    const actual: Charge = .{ .calls = 1, .output_bytes = 4 + event.len };
    try std.testing.expect(try store.reserve(work, lease, @splat(8), upper, 2));
    memory.reject_once = true;
    const first = try store.completeWithEvents(work, lease, @splat(8), actual, "page", event, 2);
    try std.testing.expectEqual(@as(u64, 1), first.sequence);
    try std.testing.expectEqualStrings(event, try store.read(work, first.events.?, 64));
    try std.testing.expect(try store.reserve(work, lease, @splat(9), upper, 2));
    memory.cas_error_once = true;
    try std.testing.expectError(error.TestStorageUnavailable, store.completeWithEvents(work, lease, @splat(9), actual, "page", event, 2));
    {
        var index = try Index.init(a, store, try store.load(work, "run", "alice"), 8, 16384);
        defer index.deinit();
        // Uploaded event bytes are invisible until the checkpoint CAS commits.
        try std.testing.expectEqual(@as(usize, 1), (try index.eventsAfter(0, 8)).len);
    }
    const resumed = try store.claim(work, "run", "alice", identity, 12, 10);
    try std.testing.expect(try store.reserve(work, resumed, @splat(9), upper, 13));
    const second = try store.completeWithEvents(work, resumed, @splat(9), actual, "page", event, 13);
    try std.testing.expectEqual(@as(u64, 2), second.sequence);
    try std.testing.expect(try store.reserve(work, resumed, @splat(10), .{ .output_bytes = 4 }, 13));
    const final = try store.complete(work, resumed, @splat(10), .{ .output_bytes = 4 }, "done", 13);
    try std.testing.expectError(error.InvalidDocumentCheckpoint, store.finish(work, resumed, first.payload, 13));
    try store.finish(work, resumed, final, 13);
    const record = try store.load(work, "run", "alice");
    try std.testing.expectEqual(@as(u64, 3), record.sequence);
    try std.testing.expectEqual(@as(u64, 3), record.used.calls);
    var index = try Index.init(a, store, record, 8, 16384);
    defer index.deinit();
    const page1 = try index.eventsAfter(0, 1);
    const page2 = try index.eventsAfter(page1[0].sequence, 1);
    try std.testing.expectEqual(@as(u64, 1), page1[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), page2[0].sequence);
    try std.testing.expectEqual(@as(usize, 0), (try index.eventsAfter(page2[0].sequence, 1)).len);
    try std.testing.expectError(error.InvalidDocumentCheckpoint, index.eventsAfter(4, 1));
}
