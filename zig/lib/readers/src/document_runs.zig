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
    operation: enum { read, extract } = .read,
    connection: []const u8 = "local-inference",
    error_code: ?[]const u8 = null,
    revision: u64 = 1,
    attempt: u64 = 0,
    nonce: [8]u8 = @splat(0),
    lease_until_ms: u64 = 0,
    not_before_ms: u64 = 0,
    state: enum { queued, running, succeeded, failed, cancelled } = .queued,
    identity: Identity,
    /// Submission has a durable request but no model may run until the
    /// source snapshots and full graph/runtime identity have been sealed.
    prepared: bool = true,
    limits: Charge,
    used: Charge = .{},
    /// Conservative charges for calls dispatched by superseded attempts.
    uncertain: Charge = .{},
    /// Absolute retention deadline. Zero is reserved for legacy internal runs.
    /// Renewing a lease or replaying a result never extends retention.
    retention_until_ms: u64 = 0,
    /// A durable tombstone fences uploads before artifact collection starts.
    /// Keep this marker after collection so late uploads remain discoverable.
    retired: bool = false,
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
        const record = try self.inspect(a, id, owner);
        if (record.retired) return error.DocumentRunExpired;
        return record;
    }
    /// Public status/result/event reads must check the deadline even when
    /// maintenance has not yet retired the run or deletion is unavailable.
    pub fn loadAt(self: Store, a: A, id: []const u8, owner: []const u8, now_ms: u64) !Record {
        const record = try self.load(a, id, owner);
        if (record.retention_until_ms != 0 and now_ms >= record.retention_until_ms) return error.DocumentRunExpired;
        return record;
    }
    fn inspect(self: Store, a: A, id: []const u8, owner: []const u8) !Record {
        const record = (try self.load_fn(self.ptr, a, id, owner)) orelse return error.DocumentRunNotFound;
        if (record.format != 1 or !std.mem.eql(u8, record.run_id, id) or !std.mem.eql(u8, record.owner, owner)) return error.DocumentRunNotFound;
        if (record.revision == 0 or !record.used.within(record.limits) or record.pending.len > 256) return error.InvalidDocumentCheckpoint;
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
        if (record.format != 1 or record.run_id.len == 0 or record.owner.len == 0 or record.attempt != 0 or record.revision != 1 or record.state != .queued or record.retired or record.head != null or record.sequence != 0 or record.result != null or record.pending.len != 0 or !std.meta.eql(record.used, Charge{}) or !std.meta.eql(record.uncertain, Charge{})) return error.InvalidDocumentCheckpoint;
        if (!try self.cas_fn(self.ptr, null, record)) return error.DocumentRunExists;
    }
    pub fn claim(self: Store, a: A, id: []const u8, owner: []const u8, identity: Identity, now_ms: u64, lease_ms: u64) !Lease {
        if (lease_ms == 0 or lease_ms > 1_800_000) return error.InvalidDocumentCheckpoint;
        var nonce: [8]u8 = undefined;
        self.nonce_fn(self.ptr, &nonce);
        if (std.mem.allEqual(u8, &nonce, 0)) return error.InvalidDocumentCheckpoint;
        for (0..16) |_| {
            var record = try self.loadAt(a, id, owner, now_ms);
            if (!std.meta.eql(identity, record.identity)) return error.DocumentModelRevisionMismatch;
            if (record.state == .succeeded or record.state == .cancelled or record.state == .failed) return error.DocumentRunTerminal;
            if (record.not_before_ms > now_ms) return error.DocumentRunBusy;
            if (record.state == .running and now_ms < record.lease_until_ms) return error.DocumentRunBusy;
            const expected = record.version();
            const previous = record.revision;
            record.attempt = std.math.add(u64, record.attempt, 1) catch return error.InvalidDocumentCheckpoint;
            record.revision = std.math.add(u64, previous, 1) catch return error.InvalidDocumentCheckpoint;
            record.lease_until_ms = std.math.add(u64, now_ms, lease_ms) catch return error.InvalidDocumentCheckpoint;
            if (record.retention_until_ms != 0) record.lease_until_ms = @min(record.lease_until_ms, record.retention_until_ms);
            record.state = .running;
            record.not_before_ms = 0;
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
        const record = try self.loadAt(a, lease.run_id, lease.owner, now_ms);
        if (record.state != .running or record.attempt != lease.attempt or !std.mem.eql(u8, &record.nonce, &lease.nonce) or now_ms >= record.lease_until_ms) return error.DocumentRunLeaseLost;
        return record;
    }
    /// Persist an upper bound before dispatch. Returns false for an already
    /// reserved call in this attempt: the caller must not dispatch it twice.
    /// A new attempt charges again for ambiguous calls left by an old worker.
    pub fn reserve(self: Store, a: A, lease: Lease, key: Digest, upper: Charge, now_ms: u64) !bool {
        for (0..16) |_| {
            var record = try self.current(a, lease, now_ms);
            if (!record.prepared and upper.calls != 0) return error.InvalidDocumentCheckpoint;
            var existing: ?usize = null;
            for (record.pending, 0..) |pending, i| if (std.mem.eql(u8, &pending.key, &key)) {
                if (pending.attempt == lease.attempt and std.mem.eql(u8, &pending.nonce, &lease.nonce)) return false;
                existing = i;
                break;
            };
            const charged = try record.used.plus(upper);
            if (!charged.within(record.limits)) return error.DocumentBudgetExhausted;
            const count = record.pending.len + @as(usize, @intFromBool(existing == null));
            if (count > 256) return error.DocumentBudgetExhausted;
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
    /// A native batch reports aggregate usage. Transfer its token upper bounds
    /// to the first stage atomically before dispatch, preserving the total
    /// charge and each item's independent call/output reservations.
    pub fn combineTokenReservations(self: Store, a: A, lease: Lease, keys: []const Digest, now_ms: u64) !void {
        if (keys.len < 2 or keys.len > 64) return error.InvalidDocumentCheckpoint;
        for (0..16) |_| {
            var record = try self.current(a, lease, now_ms);
            const pending = try a.dupe(Reservation, record.pending);
            var first: ?usize = null;
            var total: u64 = 0;
            for (keys, 0..) |key, ordinal| {
                for (keys[0..ordinal]) |prior| if (std.mem.eql(u8, &prior, &key)) return error.InvalidDocumentCheckpoint;
                const index = for (pending, 0..) |intent, i| {
                    if (std.mem.eql(u8, &intent.key, &key) and intent.attempt == lease.attempt and std.mem.eql(u8, &intent.nonce, &lease.nonce)) break i;
                } else return error.InvalidDocumentCheckpoint;
                if (ordinal == 0) first = index;
                total = std.math.add(u64, total, pending[index].upper.tokens) catch return error.InvalidDocumentCheckpoint;
                pending[index].upper.tokens = 0;
            }
            pending[first.?].upper.tokens = total;
            const expected = record.version();
            record.pending = pending;
            record.revision = std.math.add(u64, record.revision, 1) catch return error.InvalidDocumentCheckpoint;
            if (try self.cas_fn(self.ptr, expected, record)) return;
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
            if (record.retention_until_ms != 0) record.lease_until_ms = @min(record.lease_until_ms, record.retention_until_ms);
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
    /// The acknowledged request is the first immutable input root. This is
    /// installed before submission is returned to the client.
    pub fn attachRequest(self: Store, a: A, lease: Lease, request: Ref, now_ms: u64) !void {
        for (0..16) |_| {
            var record = try self.current(a, lease, now_ms);
            if (record.prepared or record.inputs.len != 0 or record.pending.len != 0 or record.used.calls != 0 or !std.mem.eql(u8, &request.sha256, &record.identity.request)) return error.InvalidDocumentCheckpoint;
            const head = record.head orelse return error.InvalidDocumentCheckpoint;
            const checkpoint = try std.json.parseFromSliceLeaky(Checkpoint, a, try self.read(a, head, 4096), .{});
            if (!sameRef(checkpoint.payload, request)) return error.InvalidDocumentCheckpoint;
            const expected = record.version();
            record.inputs = try a.dupe(Ref, &.{request});
            record.revision = std.math.add(u64, record.revision, 1) catch return error.InvalidDocumentCheckpoint;
            if (try self.cas_fn(self.ptr, expected, record)) return;
        }
        return error.DocumentRunBusy;
    }
    /// Seal a complete immutable input/plan snapshot exactly once. No model
    /// dispatch or pending intent may precede this transition. Retry ceilings
    /// can only narrow, and the submitted request hash is immutable.
    pub fn prepare(self: Store, a: A, lease: Lease, identity: Identity, limits: Charge, inputs: []const Ref, now_ms: u64) !void {
        for (0..16) |_| {
            var record = try self.current(a, lease, now_ms);
            if (record.prepared or record.used.calls != 0 or record.pending.len != 0 or record.inputs.len != 1 or inputs.len == 0 or !sameRef(record.inputs[0], inputs[0]) or !std.mem.eql(u8, &record.identity.request, &identity.request) or !limits.within(record.limits) or !record.used.within(limits)) return error.InvalidDocumentCheckpoint;
            const expected = record.version();
            record.identity = identity;
            record.limits = limits;
            record.inputs = inputs;
            record.prepared = true;
            record.revision = std.math.add(u64, record.revision, 1) catch return error.InvalidDocumentCheckpoint;
            if (try self.cas_fn(self.ptr, expected, record)) return;
        }
        return error.DocumentRunBusy;
    }
    /// A bounded owner pass relinquishes its lease explicitly. Ambiguous
    /// dispatches remain charged; the next attempt may replay accepted work.
    pub fn yieldLease(self: Store, a: A, lease: Lease, now_ms: u64) !void {
        return self.yieldLeaseUntil(a, lease, now_ms, now_ms);
    }
    pub fn yieldLeaseUntil(self: Store, a: A, lease: Lease, now_ms: u64, not_before_ms: u64) !void {
        for (0..16) |_| {
            var record = try self.current(a, lease, now_ms);
            const expected = record.version();
            for (record.pending) |pending| record.uncertain = try record.uncertain.plus(pending.upper);
            record.pending = &.{};
            record.state = .queued;
            record.not_before_ms = @max(now_ms, not_before_ms);
            record.lease_until_ms = 0;
            record.revision = std.math.add(u64, record.revision, 1) catch return error.InvalidDocumentCheckpoint;
            if (try self.cas_fn(self.ptr, expected, record)) return;
        }
        return error.DocumentRunBusy;
    }
    pub fn fail(self: Store, a: A, lease: Lease, now_ms: u64) !void {
        return self.failWithCode(a, lease, now_ms, null);
    }
    pub fn failWithCode(self: Store, a: A, lease: Lease, now_ms: u64, code: ?[]const u8) !void {
        if (code) |text| if (text.len > 128) return error.InvalidDocumentCheckpoint;
        for (0..16) |_| {
            var record = try self.current(a, lease, now_ms);
            const expected = record.version();
            record.state = .failed;
            record.error_code = code;
            record.lease_until_ms = 0;
            record.revision = std.math.add(u64, record.revision, 1) catch return error.InvalidDocumentCheckpoint;
            if (try self.cas_fn(self.ptr, expected, record)) return;
        }
        return error.DocumentRunBusy;
    }
    fn sameRef(left: Ref, right: Ref) bool {
        return left.bytes == right.bytes and std.mem.eql(u8, left.id, right.id) and std.mem.eql(u8, &left.sha256, &right.sha256);
    }
    /// Expiry and loss of publication authority precede deletion. A tombstone
    /// retains run identity and the highest upload attempt, allowing bounded
    /// collectors to retry after a crash or an upload finishing late.
    pub fn retire(self: Store, a: A, id: []const u8, owner: []const u8, now_ms: u64) !Record {
        for (0..16) |_| {
            var record = try self.inspect(a, id, owner);
            if (record.retired) return record;
            if (record.retention_until_ms == 0 or now_ms < record.retention_until_ms) return error.DocumentRunRetentionActive;
            const expected = record.version();
            record.retired = true;
            record.state = .cancelled;
            record.lease_until_ms = 0;
            for (record.pending) |pending| record.uncertain = try record.uncertain.plus(pending.upper);
            record.pending = &.{};
            record.inputs = &.{};
            record.head = null;
            record.sequence = 0;
            record.result = null;
            record.revision = std.math.add(u64, record.revision, 1) catch return error.InvalidDocumentCheckpoint;
            if (try self.cas_fn(self.ptr, expected, record)) return record;
        }
        return error.DocumentRunBusy;
    }
    pub fn finish(self: Store, a: A, lease: Lease, result: Ref, now_ms: u64) !void {
        for (0..16) |_| {
            var record = try self.current(a, lease, now_ms);
            if (!record.prepared or record.pending.len != 0) return error.InvalidDocumentCheckpoint;
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
    head: ?Ref = null,
    manifest_bytes: usize = 0,
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
        result.manifest_bytes = max_manifest_bytes - bytes_left;
        result.head = if (record.head) |reference| .{ .id = try work.dupe(u8, reference.id), .bytes = reference.bytes, .sha256 = reference.sha256 } else null;
        return result;
    }
    fn sameRef(left: ?Ref, right: ?Ref) bool {
        if (left == null or right == null) return left == null and right == null;
        return left.?.bytes == right.?.bytes and std.mem.eql(u8, left.?.id, right.?.id) and std.mem.eql(u8, &left.?.sha256, &right.?.sha256);
    }
    /// Follow only the appended journal suffix. Validate the whole suffix in
    /// scratch storage before extending the live index, so failed reads leave
    /// the subscriber's cursor and acknowledged prefix unchanged.
    pub fn refresh(self: *Index, store: Store, record: Record, max_stages: usize, max_manifest_bytes: usize) !void {
        if (record.sequence < self.sequence) return error.InvalidDocumentCheckpoint;
        if (record.sequence == self.sequence) {
            if (!sameRef(self.head, record.head)) return error.InvalidDocumentCheckpoint;
            return;
        }
        var scratch = std.heap.ArenaAllocator.init(self.arena.child_allocator);
        defer scratch.deinit();
        const temp = scratch.allocator();
        var groups: std.ArrayList(Checkpoint) = .empty;
        var seen: std.AutoHashMapUnmanaged(Digest, void) = .empty;
        var cursor = record.head;
        var expected = record.sequence;
        var bytes_left = max_manifest_bytes -| self.manifest_bytes;
        while (expected > self.sequence) {
            const reference = cursor orelse return error.InvalidDocumentCheckpoint;
            if (groups.items.len >= max_stages -| self.stages.count() or reference.bytes > bytes_left) return error.DocumentOutputTooLarge;
            bytes_left -= reference.bytes;
            const raw = try store.read(temp, reference, reference.bytes);
            const group = try std.json.parseFromSliceLeaky(Checkpoint, temp, raw, .{ .duplicate_field_behavior = .@"error" });
            if (group.format != 1 or group.sequence != expected or self.stages.contains(group.key) or group.payload.id.len == 0 or group.payload.id.len > 256 or group.payload.bytes > @max(record.limits.source_bytes, record.limits.output_bytes)) return error.InvalidDocumentCheckpoint;
            if ((try seen.getOrPut(temp, group.key)).found_existing) return error.InvalidDocumentCheckpoint;
            if (group.events) |event| if (event.id.len == 0 or event.id.len > 256 or event.bytes == 0 or event.bytes > @min(record.limits.output_bytes, 64 * 1024)) return error.InvalidDocumentCheckpoint;
            try groups.append(temp, group);
            expected -= 1;
            cursor = group.parent;
        }
        if (!sameRef(cursor, self.head)) return error.InvalidDocumentCheckpoint;
        const work = self.arena.allocator();
        // Allocate/copy every reference before mutating the visible index.
        for (groups.items) |*group| {
            group.payload.id = try work.dupe(u8, group.payload.id);
            if (group.events) |*event| event.id = try work.dupe(u8, event.id);
        }
        const saved_head: ?Ref = if (record.head) |reference| .{ .id = try work.dupe(u8, reference.id), .bytes = reference.bytes, .sha256 = reference.sha256 } else null;
        try self.stages.ensureTotalCapacity(work, @intCast(self.stages.count() + groups.items.len));
        try self.event_groups.ensureUnusedCapacity(work, groups.items.len);
        std.mem.reverse(Checkpoint, groups.items);
        for (groups.items) |group| {
            self.stages.putAssumeCapacity(group.key, group.payload);
            if (group.events) |event| self.event_groups.appendAssumeCapacity(.{ .sequence = group.sequence, .key = group.key, .payload = event });
        }
        self.manifest_bytes = max_manifest_bytes - bytes_left;
        self.sequence = record.sequence;
        self.head = saved_head;
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

/// Executor-owned physical token accounting. A remote storage peer cannot
/// reset or forge this budget. Replay never enters it; ambiguous provider work
/// retains its reservation and makes measured billing unavailable.
pub const DispatchMeter = struct {
    limit: u64,
    used: u64 = 0,
    pending: u64 = 0,
    known: bool = true,
    mutex: std.atomic.Mutex = .unlocked,
    fn lock(self: *DispatchMeter) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn acquire(self: *DispatchMeter, tokens: u64) !bool {
        self.lock();
        defer self.mutex.unlock();
        if (tokens > self.limit -| (self.used - self.pending)) return error.DocumentBudgetExhausted;
        if (tokens > self.limit -| self.used) return false;
        self.used += tokens;
        self.pending += tokens;
        return true;
    }
    pub fn settle(self: *DispatchMeter, upper: u64, actual: ?u64) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(upper <= self.pending);
        self.pending -= upper;
        if (actual) |count| {
            if (count <= upper) {
                self.used -= upper - count;
            } else {
                self.used +|= count - upper;
                self.known = false;
            }
        } else self.known = false;
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
    prepared_limits: ?Charge = null,
    meter: ?*DispatchMeter = null,
    remaining_calls: std.atomic.Value(u64) = .init(0),
    io: ?std.Io = null,
    check_context: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,

    const Slot = struct {
        complete: bool = false,
        failed: bool = false,
        upper_calls: u64 = 0,
        meter_tokens: u64 = 0,
        metered: bool = false,
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
        result.remaining_calls.store(record.limits.calls - record.used.calls, .monotonic);
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
    pub fn reference(self: *Session, a: A, stage: Digest) !Ref {
        self.lock();
        const slot = self.stages.get(stage);
        self.mutex.unlock();
        const value = slot orelse return error.InvalidDocumentCheckpoint;
        if (!value.complete) return error.DocumentStageInFlight;
        return .{ .id = try a.dupe(u8, value.id[0..value.id_len]), .bytes = value.bytes, .sha256 = value.sha256 };
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
    pub fn ensureCapacity(self: *Session, capacity: usize) !void {
        if (capacity > 100000) return error.DocumentOutputTooLarge;
        self.lock();
        defer self.mutex.unlock();
        try self.stages.ensureTotalCapacity(self.allocator, @intCast(capacity));
        self.max_stages = @max(self.max_stages, capacity);
    }
    fn settleMeter(self: *Session, stage: Digest, actual: ?u64) void {
        const meter = self.meter orelse return;
        self.lock();
        defer self.mutex.unlock();
        const slot = self.stages.getPtr(stage) orelse return;
        if (!slot.metered) return;
        slot.metered = false;
        meter.settle(slot.meter_tokens, actual);
    }
    /// Transfer native aggregate-batch usage to its reporting ticket in both
    /// durable reservations and the executor's independent physical meter.
    pub fn combineTokenReservations(self: *Session, a: A, keys: []const Digest) !void {
        try self.store.combineTokenReservations(a, self.lease, keys, self.clock.now());
        self.lock();
        defer self.mutex.unlock();
        var total: u64 = 0;
        for (keys) |stage_key| total += self.stages.getPtr(stage_key).?.meter_tokens;
        for (keys, 0..) |stage_key, i| self.stages.getPtr(stage_key).?.meter_tokens = if (i == 0) total else 0;
    }
    pub fn beginIntent(self: *Session, stage: Digest, upper: Charge) !void {
        self.lock();
        if (self.stages.contains(stage) or self.stages.count() >= self.max_stages) {
            self.mutex.unlock();
            return error.DocumentStageInFlight;
        }
        self.stages.putAssumeCapacity(stage, .{ .upper_calls = upper.calls });
        self.mutex.unlock();
        errdefer self.failed(stage);
        try self.reserve(stage, upper);
    }
    pub fn commitIntent(self: *Session, a: A, stage: Digest, actual: Charge, payload: []const u8, events: []const u8) !Ref {
        errdefer self.failed(stage);
        var scratch = std.heap.ArenaAllocator.init(self.allocator);
        defer scratch.deinit();
        self.settleMeter(stage, actual.tokens);
        const completed = try self.store.completeWithEvents(scratch.allocator(), self.lease, stage, actual, payload, events, self.clock.now());
        self.lock();
        const slot = self.stages.getPtr(stage).?;
        if (slot.upper_calls > actual.calls) _ = self.remaining_calls.fetchAdd(slot.upper_calls - actual.calls, .monotonic);
        slot.set(completed.payload);
        self.mutex.unlock();
        var saved = completed.payload;
        saved.id = try a.dupe(u8, saved.id);
        return saved;
    }
    /// Publish already-computed immutable data with an exact reservation.
    /// Parent assembly never reserves capacity while invoking child stages.
    pub fn publish(self: *Session, a: A, stage: Digest, payload: []const u8, events: []const u8) !Ref {
        self.lock();
        const known = self.stages.get(stage);
        self.mutex.unlock();
        if (known) |slot| {
            if (!slot.complete) return error.DocumentStageInFlight;
            return self.reference(a, stage);
        }
        const charge: Charge = .{ .output_bytes = std.math.add(u64, payload.len, events.len) catch return error.DocumentOutputTooLarge };
        try self.beginIntent(stage, charge);
        return self.commitIntent(a, stage, charge, payload, events);
    }
    fn pause(self: *Session) !void {
        if (self.check_fn) |check| try check(self.check_context);
        const io = self.io orelse return error.DocumentStageInFlight;
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    fn failed(self: *Session, stage: Digest) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.stages.getPtr(stage)) |slot| if (!slot.complete) {
            slot.failed = true;
        };
    }
    pub fn replay(self: *Session, a: A, stage: Digest, max_payload_bytes: usize) !?Result {
        while (true) {
            var scratch = std.heap.ArenaAllocator.init(self.allocator);
            defer scratch.deinit();
            _ = try self.store.current(scratch.allocator(), self.lease, self.clock.now());
            self.lock();
            const known = self.stages.get(stage);
            self.mutex.unlock();
            const slot = known orelse return null;
            if (slot.complete) return .{ .bytes = try self.store.read(a, .{ .id = slot.id[0..slot.id_len], .bytes = slot.bytes, .sha256 = slot.sha256 }, max_payload_bytes), .replayed = true };
            if (slot.failed) return error.DocumentStageInFlight;
            try self.pause();
        }
    }
    fn reserve(self: *Session, stage: Digest, upper: Charge) !void {
        if (self.meter) |meter| if (upper.calls > 0) {
            while (!try meter.acquire(upper.tokens)) try self.pause();
            self.lock();
            self.stages.getPtr(stage).?.meter_tokens = upper.tokens;
            self.stages.getPtr(stage).?.metered = true;
            self.mutex.unlock();
        };
        errdefer self.settleMeter(stage, 0);

        while (true) {
            var scratch = std.heap.ArenaAllocator.init(self.allocator);
            defer scratch.deinit();
            const a = scratch.allocator();
            if (self.store.reserve(a, self.lease, stage, upper, self.clock.now())) |admitted| {
                if (!admitted) return error.DocumentStageInFlight;
                if (upper.calls != 0) _ = self.remaining_calls.fetchSub(upper.calls, .monotonic);
                return;
            } else |err| {
                if (err != error.DocumentBudgetExhausted or self.io == null) return err;
                const record = try self.store.current(a, self.lease, self.clock.now());
                var pending: Charge = .{};
                for (record.pending) |intent| pending = try pending.plus(intent.upper);
                const settled = try record.used.minus(pending);
                const proposed = settled.plus(upper) catch return error.DocumentBudgetExhausted;
                // Wait only when another live reservation can release capacity.
                // A parent composite must never hold an output reservation while
                // running children; it publishes its exact encoded result later.
                if (record.pending.len == 0 or !proposed.within(record.limits)) return err;
                try self.pause();
            }
        }
    }
    pub fn execute(self: *Session, a: A, stage: Digest, upper: Charge, max_payload_bytes: usize, context: anytype, comptime invoke: anytype) !Result {
        while (true) {
            if (try self.replay(a, stage, max_payload_bytes)) |cached| return cached;
            self.lock();
            if (self.stages.contains(stage)) {
                self.mutex.unlock();
                continue;
            }
            if (self.stages.count() >= self.max_stages) {
                self.mutex.unlock();
                return error.DocumentOutputTooLarge;
            }
            self.stages.putAssumeCapacity(stage, .{ .upper_calls = upper.calls });
            self.mutex.unlock();
            break;
        }
        errdefer self.failed(stage);
        var scratch = std.heap.ArenaAllocator.init(self.allocator);
        defer scratch.deinit();
        const work = scratch.allocator();
        try self.reserve(stage, upper);
        const output: Output = invoke(context, work) catch |err| {
            self.settleMeter(stage, null);
            return err;
        };
        self.settleMeter(stage, output.actual.tokens);
        if (output.bytes.len > max_payload_bytes) return error.DocumentOutputTooLarge;
        const committed = try self.store.completeWithEvents(work, self.lease, stage, output.actual, output.bytes, output.events, self.clock.now());
        self.lock();
        if (upper.calls > output.actual.calls) _ = self.remaining_calls.fetchAdd(upper.calls - output.actual.calls, .monotonic);
        self.stages.getPtr(stage).?.set(committed.payload);
        self.mutex.unlock();
        return .{ .bytes = try a.dupe(u8, output.bytes), .replayed = false };
    }
};

/// A bounded pixel credit grant amortizes the tiny-image case without granting
/// free retries. It is durable before CPU work and reconciles when rendering
/// finishes. A crash retains the outstanding upper bound as uncertainty.
pub const PixelMeter = struct {
    key: Digest = @splat(0),
    upper: u64 = 0,
    used: u64 = 0,
    pub fn charge(self: *PixelMeter, session: *Session, count: u64) !void {
        if (count == 0) return;
        if (count <= self.upper - self.used) {
            self.used += count;
            return;
        }
        try self.flush(session);
        var scratch = std.heap.ArenaAllocator.init(session.allocator);
        defer scratch.deinit();
        const record = try session.store.current(scratch.allocator(), session.lease, session.clock.now());
        var pending: u64 = 0;
        for (record.pending) |intent| pending = std.math.add(u64, pending, intent.upper.pixels) catch return error.InvalidDocumentCheckpoint;
        const available = record.limits.pixels - (record.used.pixels - pending);
        if (count > available) return error.DocumentPixelLimitExceeded;
        const upper = @max(count, @min(1_000_000, available));
        var nonce: [32]u8 = undefined;
        session.store.nonce_fn(session.store.ptr, &nonce);
        const key = session.key(&nonce);
        try session.beginIntent(key, .{ .pixels = upper });
        self.* = .{ .key = key, .upper = upper, .used = count };
    }
    pub fn flush(self: *PixelMeter, session: *Session) !void {
        if (self.upper == 0) return;
        var scratch = std.heap.ArenaAllocator.init(session.allocator);
        defer scratch.deinit();
        _ = try session.commitIntent(scratch.allocator(), self.key, .{ .pixels = self.used }, "", "");
        self.* = .{};
    }
};

const TestStore = @import("document_test_store.zig").Store;

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

test "document durable retention fences leases and tombstones permanently preserve upload authority" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    const initial: Record = .{ .run_id = "expires", .owner = "alice", .identity = identity, .retention_until_ms = 100, .limits = .{ .calls = 4, .output_bytes = 1024 }, .inputs = &.{} };
    try store.create(initial);
    const lease = try store.claim(work, "expires", "alice", identity, 1, 1000);
    try std.testing.expectEqual(@as(u64, 100), (try store.load(work, "expires", "alice")).lease_until_ms);
    try store.renew(work, lease, 2, 1000);
    try std.testing.expectEqual(@as(u64, 100), (try store.load(work, "expires", "alice")).lease_until_ms);
    try std.testing.expect(try store.reserve(work, lease, @splat(1), .{ .calls = 1, .output_bytes = 64 }, 3));
    _ = try store.complete(work, lease, @splat(1), .{ .calls = 1, .output_bytes = 8 }, "accepted", 4);
    try std.testing.expect(try store.reserve(work, lease, @splat(2), .{ .calls = 1 }, 5));
    try std.testing.expectError(error.DocumentRunNotFound, store.retire(work, "expires", "bob", 100));
    try std.testing.expectError(error.DocumentRunRetentionActive, store.retire(work, "expires", "alice", 99));
    try std.testing.expectError(error.DocumentRunExpired, store.complete(work, lease, @splat(2), .{ .calls = 1 }, "", 100));
    try std.testing.expectError(error.DocumentRunExpired, store.loadAt(work, "expires", "alice", 100));
    const tombstone = try store.retire(work, "expires", "alice", 100);
    try std.testing.expect(tombstone.retired);
    try std.testing.expectEqual(lease.attempt, tombstone.attempt);
    try std.testing.expectEqual(@as(u64, 2), tombstone.used.calls);
    try std.testing.expectEqual(@as(u64, 1), tombstone.uncertain.calls);
    try std.testing.expect(tombstone.head == null and tombstone.result == null and tombstone.pending.len == 0);
    try std.testing.expectEqual(tombstone.version(), (try store.retire(work, "expires", "alice", 101)).version());
    try std.testing.expectError(error.DocumentRunExpired, store.claim(work, "expires", "alice", identity, 101, 10));
    try std.testing.expectError(error.DocumentRunExists, store.create(initial));
}

test "document durable submission seals immutable inputs before model dispatch and yields explicit ownership" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    var request_hash: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash("request", &request_hash, .{});
    const initial = Identity{ .request = request_hash, .sources = @splat(0), .definition = @splat(0), .models = @splat(0), .runtime = @splat(0) };
    const limits: Charge = .{ .calls = 10, .tokens = 100, .output_bytes = 1024, .source_bytes = 1024 };
    try store.create(.{ .run_id = "submit", .owner = "alice", .identity = initial, .prepared = false, .limits = limits, .inputs = &.{} });
    const first = try store.claim(work, "submit", "alice", initial, 1, 100);
    try std.testing.expectError(error.InvalidDocumentCheckpoint, store.reserve(work, first, @splat(2), .{ .calls = 1 }, 2));
    try std.testing.expect(try store.reserve(work, first, @splat(1), .{ .source_bytes = 7 }, 2));
    const request = try store.complete(work, first, @splat(1), .{ .source_bytes = 7 }, "request", 2);
    try store.attachRequest(work, first, request, 2);
    try store.yieldLease(work, first, 2);
    const second = try store.claim(work, "submit", "alice", initial, 3, 100);
    try std.testing.expectError(error.DocumentRunLeaseLost, store.reserve(work, first, @splat(3), .{}, 3));
    const identity = Identity{ .request = request_hash, .sources = @splat(1), .definition = @splat(2), .models = @splat(3), .runtime = @splat(4) };
    var expanded = limits;
    expanded.calls += 1;
    try std.testing.expectError(error.InvalidDocumentCheckpoint, store.prepare(work, second, identity, expanded, &.{request}, 3));
    try store.prepare(work, second, identity, limits, &.{request}, 3);
    try std.testing.expectError(error.InvalidDocumentCheckpoint, store.prepare(work, second, identity, limits, &.{request}, 3));
    try std.testing.expect(try store.reserve(work, second, @splat(3), .{ .calls = 1, .tokens = 10 }, 3));
    try store.yieldLease(work, second, 4);
    const third = try store.claim(work, "submit", "alice", identity, 4, 100);
    const recovered = try store.load(work, "submit", "alice");
    try std.testing.expectEqual(@as(u64, 10), recovered.uncertain.tokens);
    try std.testing.expectEqual(@as(usize, 1), recovered.inputs.len);
    try std.testing.expectEqualStrings(request.id, recovered.inputs[0].id);
    try std.testing.expect(recovered.prepared);
    try store.fail(work, third, 5);
    try std.testing.expectError(error.DocumentRunTerminal, store.claim(work, "submit", "alice", identity, 6, 100));
}

test "document durable pixel credits reconcile completed rendering and retain ambiguous CPU work" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "pixels", .owner = "alice", .identity = identity, .limits = .{ .pixels = 4_000_000, .output_bytes = 4096 }, .inputs = &.{} });
    const Clock = struct {
        fn now(_: ?*anyopaque) u64 {
            return 1;
        }
    };
    const first = try store.claim(work, "pixels", "alice", identity, 1, 10);
    {
        var session = try Session.init(a, store, first, identity, .{ .now_fn = Clock.now }, 16, 65536);
        defer session.deinit();
        var meter: PixelMeter = .{};
        try meter.charge(&session, 100);
        try std.testing.expectEqual(@as(u64, 1_000_000), (try store.load(work, "pixels", "alice")).used.pixels);
        try meter.charge(&session, 200);
        try meter.flush(&session);
        try std.testing.expectEqual(@as(u64, 300), (try store.load(work, "pixels", "alice")).used.pixels);
        try meter.charge(&session, 2000);
        // Crash before reconciliation: the complete grant remains charged.
    }
    const second = try store.claim(work, "pixels", "alice", identity, 12, 10);
    const LaterClock = struct {
        fn now(_: ?*anyopaque) u64 {
            return 12;
        }
    };
    var session = try Session.init(a, store, second, identity, .{ .now_fn = LaterClock.now }, 16, 65536);
    defer session.deinit();
    var meter: PixelMeter = .{};
    try meter.charge(&session, 2000);
    try meter.flush(&session);
    const recovered = try store.load(work, "pixels", "alice");
    try std.testing.expectEqual(@as(u64, 1_000_000), recovered.uncertain.pixels);
    try std.testing.expectEqual(@as(u64, 1_002_300), recovered.used.pixels);
    try std.testing.expectError(error.DocumentPixelLimitExceeded, meter.charge(&session, 4_000_000));
}

test "document durable event followers extend only committed suffixes and preserve cursors on corruption" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "run", .owner = "alice", .identity = identity, .limits = .{ .calls = 5, .output_bytes = 1024 }, .inputs = &.{} });
    const lease = try store.claim(work, "run", "alice", identity, 1, 100);
    var index = try Index.init(a, store, try store.load(work, "run", "alice"), 8, 16384);
    defer index.deinit();
    const events = "[{\"phase\":\"accepted\"}]";
    for (0..3) |n| {
        const key: Digest = @splat(@as(u8, @intCast(n + 1)));
        try std.testing.expect(try store.reserve(work, lease, key, .{ .calls = 1, .output_bytes = 64 }, 2));
        _ = try store.completeWithEvents(work, lease, key, .{ .calls = 1, .output_bytes = 4 + events.len }, "page", events, 2);
        const record = try store.load(work, "run", "alice");
        memory.corrupt = true;
        try std.testing.expectError(error.InvalidDocumentCheckpoint, index.refresh(store, record, 8, 16384));
        try std.testing.expectEqual(@as(u64, n), index.sequence);
        memory.corrupt = false;
        try index.refresh(store, record, 8, 16384);
        try index.refresh(store, record, 8, 16384);
        const next = try index.eventsAfter(n, 8);
        try std.testing.expectEqual(@as(usize, 1), next.len);
        try std.testing.expectEqual(@as(u64, n + 1), next[0].sequence);
        try std.testing.expectEqual(@as(usize, n + 1), index.stages.count());
    }
    const saved = try store.load(work, "run", "alice");
    var changed = saved;
    changed.head.?.sha256[0] ^= 1;
    try std.testing.expectError(error.InvalidDocumentCheckpoint, index.refresh(store, changed, 8, 16384));
    try std.testing.expectEqual(@as(u64, 3), index.sequence);
}

test "document durable native batch transfers aggregate token reservations without changing cumulative charge" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "run", .owner = "alice", .identity = identity, .limits = .{ .calls = 5, .tokens = 200, .output_bytes = 1024 }, .inputs = &.{} });
    const lease = try store.claim(work, "run", "alice", identity, 1, 100);
    const keys = [_]Digest{ @splat(8), @splat(9) };
    for (keys) |key| try std.testing.expect(try store.reserve(work, lease, key, .{ .calls = 1, .tokens = 100, .output_bytes = 64 }, 2));
    try store.combineTokenReservations(work, lease, &keys, 2);
    const reserved = try store.load(work, "run", "alice");
    try std.testing.expectEqual(@as(u64, 200), reserved.used.tokens);
    try std.testing.expectEqual(@as(u64, 200), reserved.pending[0].upper.tokens);
    try std.testing.expectEqual(@as(u64, 0), reserved.pending[1].upper.tokens);
    _ = try store.complete(work, lease, keys[0], .{ .calls = 1, .tokens = 150, .output_bytes = 4 }, "page", 2);
    _ = try store.complete(work, lease, keys[1], .{ .calls = 1, .output_bytes = 4 }, "page", 2);
    const settled = try store.load(work, "run", "alice");
    try std.testing.expectEqual(@as(u64, 150), settled.used.tokens);
    try std.testing.expectEqual(@as(u64, 2), settled.used.calls);
}

test "document durable dispatch meter cannot be reset by storage and reconciles physical batching" {
    var meter = DispatchMeter{ .limit = 100 };
    try std.testing.expect(try meter.acquire(60));
    try std.testing.expect(!try meter.acquire(60));
    meter.settle(60, 10);
    try std.testing.expect(try meter.acquire(60));
    meter.settle(60, 20);
    try std.testing.expectEqual(@as(u64, 30), meter.used);
    try std.testing.expectError(error.DocumentBudgetExhausted, meter.acquire(71));
    try std.testing.expect(try meter.acquire(70));
    meter.settle(70, null);
    try std.testing.expectEqual(@as(u64, 100), meter.used);
    try std.testing.expectEqual(@as(u64, 0), meter.pending);
    try std.testing.expect(!meter.known);
    var overrun = DispatchMeter{ .limit = 10 };
    try std.testing.expect(try overrun.acquire(5));
    overrun.settle(5, 11);
    try std.testing.expectEqual(@as(u64, 11), overrun.used);
    try std.testing.expect(!overrun.known);
    try std.testing.expectError(error.DocumentBudgetExhausted, overrun.acquire(1));
}

test "document durable retry deadline survives handoff without releasing uncertain charges" {
    const a = std.testing.allocator;
    var memory = TestStore{ .a = a };
    defer memory.deinit();
    const store = memory.interface();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const identity: Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "retry", .owner = "alice", .identity = identity, .inputs = &.{}, .limits = .{ .calls = 4, .tokens = 100 } });
    const first = try store.claim(work, "retry", "alice", identity, 1, 10);
    try std.testing.expect(try store.reserve(work, first, @splat(1), .{ .calls = 1, .tokens = 9 }, 2));
    try store.yieldLeaseUntil(work, first, 3, 50);
    try std.testing.expectError(error.DocumentRunBusy, store.claim(work, "retry", "alice", identity, 49, 10));
    const next = try store.claim(work, "retry", "alice", identity, 50, 10);
    try std.testing.expect(next.attempt > first.attempt);
    const record = try store.load(work, "retry", "alice");
    try std.testing.expectEqual(@as(u64, 9), record.used.tokens);
    try std.testing.expectEqual(@as(u64, 9), record.uncertain.tokens);
    try std.testing.expectEqual(@as(u64, 0), record.not_before_ms);
}
