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

//! Shared query/task ownership. The claim and accepted output occupy the same
//! CAS record: a delayed worker cannot publish through a superseded claim.
const std = @import("std");
const storage = @import("objectstore");
const catalog = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog;
const A = std.mem.Allocator;
pub const Digest = [64]u8;
pub fn validateDigest(value: Digest) !void {
    for (value) |byte| if (!std.ascii.isHex(byte)) return error.InvalidSqlExecutionState;
}
/// Expiry is part of the physical query namespace, so a delayed write remains
/// discoverable after cleanup has removed the submission/discovery marker.
pub const Query = struct {
    id: Digest,
    expires_ms: u64,
    pub fn validate(self: Query) !void {
        try validateDigest(self.id);
        if (self.expires_ms == 0) return error.InvalidSqlExecutionState;
    }
};
pub const max_task_attempts: u16 = 16;
/// Immutable extension metadata; mutable v2 records retain their frozen schema.
pub const Admission = struct {
    version: u8 = 1,
    tenant: Digest,
    query: Query,
    task_attempt_limit: u16,
};
pub const Witness = struct {
    key: []const u8,
    hash: Digest,
    bytes: u64,
    rows: u64,
    partitions: u16,
    index_bytes: u32,
    index_hash: ?Digest,
    pub fn output(self: Witness) Output {
        return .{ .key = self.key, .hash = self.hash, .bytes = self.bytes, .rows = self.rows, .partitions = self.partitions, .index_bytes = self.index_bytes, .index_hash = self.index_hash };
    }
};
pub const Output = struct {
    key: []const u8,
    hash: Digest,
    bytes: u64,
    rows: u64 = 0,
    partitions: u16 = 0,
    index_bytes: u32 = 0,
    index_hash: ?Digest = null,
    matches: ?Witness = null,
    pub fn witness(self: Output) Witness {
        return .{ .key = self.key, .hash = self.hash, .bytes = self.bytes, .rows = self.rows, .partitions = self.partitions, .index_bytes = self.index_bytes, .index_hash = self.index_hash };
    }
};
fn sameWitness(left: ?Witness, right: ?Witness) bool {
    if (left == null or right == null) return left == null and right == null;
    const l = left.?;
    const r = right.?;
    return std.mem.eql(u8, l.key, r.key) and std.meta.eql(l.hash, r.hash) and l.bytes == r.bytes and l.rows == r.rows and l.partitions == r.partitions and l.index_bytes == r.index_bytes and std.meta.eql(l.index_hash, r.index_hash);
}
pub const Status = enum { pending, running, succeeded, failed, cancelled };
pub const Record = struct {
    /// Absent only on pre-scope records; services validate their immutable plan.
    tenant: ?Digest = null,
    /// Read-only compatibility with the short-lived inline admission format.
    /// Writers preserve the original v2 wire schema for rolling upgrades.
    task_attempt_limit: ?u16 = null,
    version: u8 = 2,
    plan: Digest,
    expires_ms: u64,
    status: Status = .pending,
    owner: []const u8 = "",
    epoch: u64 = 0,
    until_ms: u64 = 0,
    stage: u32 = 0,
    cursor: u32 = 0,
    pending: []const u32 = &.{},
    /// Last reconciled task ID; absent on legacy records and after a full pass.
    pending_after: ?u32 = null,
    pending_admission: bool = false,
    reduce_boundaries: []const u16 = &.{},
    skew: []const @import("physical.zig").Part = &.{},
    physical: ?Output = null,
    adaptive: ?Output = null,
    result_progress: ?Output = null,
    cancel_requested: bool = false,
    output: ?Output = null,
    failure: ?[]const u8 = null,
    pub fn jsonStringify(self: Record, jw: anytype) !void {
        try jw.beginObject();
        inline for (@typeInfo(Record).@"struct".field_names) |field| {
            if (comptime !std.mem.eql(u8, field, "task_attempt_limit")) {
                try jw.objectField(field);
                try jw.write(@field(self, field));
            }
        }
        try jw.endObject();
    }
    pub fn terminal(self: Record) bool {
        return self.status == .succeeded or self.status == .failed or self.status == .cancelled;
    }
};
pub const Claim = struct { owner: []const u8, epoch: u64 };
pub const Loaded = struct {
    object: storage.GetResult,
    value: Record,
    parsed: std.json.Parsed(Record),
    pub fn deinit(self: *Loaded, a: A) void {
        self.parsed.deinit();
        self.object.deinit(a);
    }
};
pub const Store = struct {
    client: storage.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: catalog.types.Context = .{},
    now_ms: u64,
    tenant: ?Digest = null,
    pub fn key(self: Store, a: A, query: Query, stage: ?u32, task: u32) ![]const u8 {
        try query.validate();
        if (stage) |s| return std.fmt.allocPrint(a, "{s}/sql-execution-v2/{d:0>20}/{s}/stage/{d}/task/{d}.json", .{ self.prefix, query.expires_ms, query.id, s, task });
        return std.fmt.allocPrint(a, "{s}/sql-execution-v2/{d:0>20}/{s}/query.json", .{ self.prefix, query.expires_ms, query.id });
    }
    pub fn admissionKey(self: Store, a: A, query: Query) ![]const u8 {
        try query.validate();
        return std.fmt.allocPrint(a, "{s}/sql-execution-v2/{d:0>20}/{s}/admission-v1.json", .{ self.prefix, query.expires_ms, query.id });
    }
    pub fn publishAdmission(self: Store, a: A, query: Query, limit: u16) !void {
        if (limit == 0 or limit > max_task_attempts) return error.InvalidSqlExecutionState;
        try self.context.ensureActive();
        const path = try self.admissionKey(a, query);
        defer a.free(path);
        const bytes = try std.json.Stringify.valueAlloc(a, Admission{ .tenant = self.tenant orelse return error.SqlExecutionUnauthorized, .query = query, .task_attempt_limit = limit }, .{});
        defer a.free(bytes);
        var client = self.client;
        client.allocator = a;
        const cancellation = catalog.types.contextCancellation(&self.context);
        var accepted = client.putObject(self.bucket, path, bytes, .{ .if_none_match = true, .cancellation = cancellation }) catch |err| {
            // Resolve both duplicate publication and an accepted PUT's lost ACK.
            var previous = client.getObject(self.bucket, path, .{ .skip_metadata_probe = true, .max_response_bytes = 1024, .cancellation = cancellation }) catch return err;
            defer previous.deinit(a);
            if (!std.mem.eql(u8, previous.body, bytes)) return error.SqlExecutionIdReused;
            return;
        };
        accepted.deinit(a);
    }
    pub fn load(self: Store, a: A, path: []const u8) !?Loaded {
        try self.context.ensureActive();
        var client = self.client;
        client.allocator = a;
        var object = client.getObject(self.bucket, path, .{ .skip_metadata_probe = true, .max_response_bytes = 16384, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => return null,
            else => return err,
        };
        errdefer object.deinit(a);
        const parsed = try std.json.parseFromSlice(Record, a, object.body, .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        const value = parsed.value;
        try self.validateRecord(value);
        return .{ .object = object, .value = value, .parsed = parsed };
    }
    pub const TaskStatus = struct {
        status: Status,
        epoch: u64 = 0,
        until_ms: u64,
        failure: ?[]const u8,
        needs_check: bool = false,
        read_error: ?anyerror = null,
    };
    /// Native waves overlap at most sixteen GETs using private response arenas.
    /// Hosted stores use their bounded eight-object batch ABI instead.
    pub fn taskStatuses(self: Store, a: A, query: Query, stage: u32, ids: []const u32, io: ?std.Io) ![]TaskStatus {
        return self.taskWindow(a, query, stage, ids, io, false);
    }
    pub fn admitTasks(self: Store, a: A, query: Query, stage: u32, ids: []const u32, io: ?std.Io) ![]TaskStatus {
        return self.taskWindow(a, query, stage, ids, io, true);
    }
    fn taskWindow(self: Store, a: A, query: Query, stage: u32, ids: []const u32, io: ?std.Io, admission: bool) ![]TaskStatus {
        if (ids.len > 256) return error.InvalidSqlExecutionState;
        const result = try a.alloc(TaskStatus, ids.len);
        var done: usize = 0;
        errdefer {
            for (result[0..done]) |status| if (status.failure) |failure| a.free(failure);
            a.free(result);
        }
        const Job = struct {
            arena: std.heap.ArenaAllocator,
            store: Store,
            path: []const u8,
            query: Query,
            create: bool,
            value: ?TaskStatus = null,
            failure: ?anyerror = null,
            fn run(self_: *@This()) void {
                const allocator = self_.arena.allocator();
                if (self_.create) {
                    const created = self_.store.registerTask(allocator, self_.path, self_.query) catch |err| {
                        self_.failure = err;
                        return;
                    };
                    // Existing intent is inspected by the next reconciliation
                    // pass, not by a second storage round-trip in this quantum.
                    self_.value = .{ .status = .pending, .until_ms = 0, .failure = null, .needs_check = !created };
                    return;
                }
                var value = (self_.store.load(allocator, self_.path) catch |err| {
                    self_.failure = err;
                    return;
                }) orelse {
                    self_.failure = error.InvalidSqlExecutionState;
                    return;
                };
                defer value.deinit(allocator);
                self_.store.validateTaskRecord(value.value, self_.query) catch |err| {
                    self_.failure = err;
                    return;
                };
                self_.value = .{ .status = value.value.status, .epoch = value.value.epoch, .until_ms = value.value.until_ms, .failure = if (value.value.failure) |failure| allocator.dupe(u8, failure) catch |err| {
                    self_.failure = err;
                    return;
                } else null };
            }
        };
        const native_parallel = !@import("builtin").single_threaded and io != null and (admission or self.client.vtable.get_objects == null);
        while (done < ids.len) {
            const count = @min(ids.len - done, if (native_parallel) @as(usize, 16) else 8);
            var jobs: [16]Job = undefined;
            var initialized: usize = 0;
            defer for (jobs[0..initialized]) |*job| {
                job.arena.deinit();
                a.free(job.path);
            };
            for (jobs[0..count], ids[done..][0..count]) |*job, id| {
                job.* = .{ .arena = .init(self.client.allocator), .store = self, .path = try self.key(a, query, stage, id), .query = query, .create = admission };
                initialized += 1;
            }
            if (native_parallel) {
                var group: std.Io.Group = .init;
                defer group.cancel(io.?);
                for (jobs[0..count]) |*job| try group.concurrent(io.?, Job.run, .{job});
                try group.await(io.?);
            } else if (!admission and self.client.vtable.get_objects != null) {
                var requests: [8]storage.GetObjectRequest = undefined;
                for (requests[0..count], jobs[0..count]) |*request, job| request.* = .{ .key = job.path, .options = .{ .skip_metadata_probe = true, .max_response_bytes = 16384, .cancellation = catalog.types.contextCancellation(&self.context) } };
                var client = self.client;
                client.allocator = a;
                const objects = try client.getObjects(self.bucket, requests[0..count]);
                defer {
                    for (objects) |*object| if (object.*) |*value| value.deinit(a);
                    a.free(objects);
                }
                for (objects, jobs[0..count]) |object, *job| {
                    const value = object orelse return error.InvalidSqlExecutionState;
                    const parsed = try std.json.parseFromSlice(Record, job.arena.allocator(), value.body, .{});
                    defer parsed.deinit();
                    try self.validateTaskRecord(parsed.value, query);
                    job.value = .{ .status = parsed.value.status, .epoch = parsed.value.epoch, .until_ms = parsed.value.until_ms, .failure = if (parsed.value.failure) |failure| try job.arena.allocator().dupe(u8, failure) else null };
                }
            } else for (jobs[0..count]) |*job| job.run();
            for (jobs[0..count]) |job| {
                if (job.failure) |err| {
                    if (admission) return err;
                    // A slow sibling must not discard successful reads. The
                    // caller decides whether an error permits deferring this ID.
                    result[done] = .{ .status = .pending, .until_ms = 0, .failure = null, .read_error = err };
                    done += 1;
                    continue;
                }
                const value = job.value.?;
                result[done] = .{ .status = value.status, .epoch = value.epoch, .until_ms = value.until_ms, .failure = if (value.failure) |failure| try a.dupe(u8, failure) else null, .needs_check = value.needs_check };
                done += 1;
            }
        }
        return result;
    }
    fn validateTaskRecord(self: Store, value: Record, query: Query) !void {
        try self.validateRecord(value);
        if (!std.meta.eql(value.plan, query.id) or value.expires_ms != query.expires_ms) return error.SqlExecutionIdReused;
    }
    /// One conditional write per task admission. Existing records are inspected
    /// in a later read phase before they can be dispatched or counted complete.
    fn registerTask(self: Store, a: A, path: []const u8, query: Query) !bool {
        if (query.expires_ms <= self.now_ms) return error.SqlQueryExpired;
        self.write(a, path, null, .{ .plan = query.id, .expires_ms = query.expires_ms, .tenant = self.tenant }) catch |err| switch (err) {
            error.PreconditionFailed => return false,
            else => return err,
        };
        return true;
    }
    fn validateRecord(self: Store, value: Record) !void {
        if (value.task_attempt_limit) |limit| if (limit == 0 or limit > max_task_attempts) return error.InvalidSqlExecutionState;
        if (value.pending_after) |after| if (value.pending.len == 0 or after >= value.cursor) return error.InvalidSqlExecutionState;
        if (self.tenant) |scope| if (value.tenant) |admitted| {
            if (!std.meta.eql(scope, admitted)) return error.SqlExecutionUnauthorized;
        };
        if (value.version != 2 or value.owner.len > 256 or (value.status == .succeeded and value.output == null)) return error.InvalidSqlExecutionState;
    }
    fn write(self: Store, a: A, path: []const u8, before: ?Loaded, value: Record) !void {
        try self.context.ensureActive();
        if (value.task_attempt_limit) |limit| {
            // Live mutations preserve authority before dropping the inline
            // field. Cancellation/terminal writes need no retry authority and
            // must remain available during an admission-storage outage.
            if (!value.cancel_requested and !value.terminal()) {
                var scoped = self;
                scoped.tenant = value.tenant orelse self.tenant;
                try scoped.publishAdmission(a, .{ .id = value.plan, .expires_ms = value.expires_ms }, limit);
            }
        }
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(bytes);
        if (bytes.len > 16384) return error.InvalidSqlExecutionState;
        var client = self.client;
        client.allocator = a;
        var result = try client.compareAndPut(self.bucket, path, bytes, if (before) |old| old.object.metadata.writeRevision() orelse return error.MissingObjectRevision else null, .{ .cancellation = catalog.types.contextCancellation(&self.context) });
        result.deinit(a);
    }
    pub fn create(self: Store, a: A, path: []const u8, plan: Digest, expires_ms: u64) !void {
        _ = try self.ensure(a, path, plan, expires_ms);
    }
    pub fn ensure(self: Store, a: A, path: []const u8, plan: Digest, expires_ms: u64) !bool {
        if (expires_ms <= self.now_ms) return error.SqlQueryExpired;
        self.write(a, path, null, .{ .plan = plan, .expires_ms = expires_ms, .tenant = self.tenant }) catch |err| {
            // Existing jobs and lost successful responses are both resolved by
            // identity. Never overwrite state advanced by another participant.
            var existing = (self.load(a, path) catch return err) orelse return err;
            defer existing.deinit(a);
            if (!std.mem.eql(u8, &existing.value.plan, &plan) or existing.value.expires_ms != expires_ms) return error.SqlExecutionIdReused;
            return false;
        };
        return true;
    }
    pub const RetryAdmission = struct {
        ptr: *anyopaque,
        load: *const fn (*anyopaque) anyerror!u16,
        /// Local capacity is acquired only after durable duplicate/live-lease
        /// checks, and before spending an execution epoch or publishing a lease.
        prepare: ?*const fn (*anyopaque, u64, u16) anyerror!void = null,
    };
    pub const TaskClaim = struct { claim: Claim, attempt_limit: u16 };
    pub fn claim(self: Store, a: A, path: []const u8, owner: []const u8, ttl_ms: u64) !?Claim {
        const value = (try self.claimWithAdmission(a, path, owner, ttl_ms, null)) orelse return null;
        return value.claim;
    }
    pub fn claimTask(self: Store, a: A, path: []const u8, owner: []const u8, ttl_ms: u64, admission: RetryAdmission) !?TaskClaim {
        return self.claimWithAdmission(a, path, owner, ttl_ms, admission);
    }
    fn claimWithAdmission(self: Store, a: A, path: []const u8, owner: []const u8, ttl_ms: u64, admission: ?RetryAdmission) !?TaskClaim {
        if (owner.len == 0 or owner.len > 256 or ttl_ms == 0 or ttl_ms > 60_000) return error.InvalidSqlExecutionClaim;
        var old = (try self.load(a, path)) orelse return error.SqlExecutionNotFound;
        defer old.deinit(a);
        var next = old.value;
        if (next.expires_ms <= self.now_ms) return error.SqlQueryExpired;
        if (next.terminal() or (next.status == .running and next.until_ms +| 30_000 >= self.now_ms)) return null;
        // Read immutable authority only for an actual retry, before consuming
        // a new epoch/lease. A sidecar outage must not spend execution attempts.
        // Exhaustion of the hard cap itself needs no metadata read.
        const limit = if (admission != null and next.epoch != 0 and next.epoch < max_task_attempts) try admission.?.load(admission.?.ptr) else max_task_attempts;
        if (limit == 0 or limit > max_task_attempts) return error.InvalidSqlExecutionState;
        const epoch = try std.math.add(u64, next.epoch, 1);
        if (admission) |value| if (value.prepare) |prepare| try prepare(value.ptr, epoch, limit);
        next.status = .running;
        next.owner = owner;
        next.epoch = epoch;
        next.until_ms = @min(next.expires_ms, self.now_ms +| ttl_ms);
        self.write(a, path, old, next) catch |err| switch (err) {
            error.PreconditionFailed => return null,
            else => return err,
        };
        return .{ .claim = .{ .owner = owner, .epoch = next.epoch }, .attempt_limit = limit };
    }
    fn owned(self: Store, a: A, path: []const u8, claim_: Claim) !Loaded {
        var old = (try self.load(a, path)) orelse return error.SqlExecutionNotFound;
        errdefer old.deinit(a);
        if (old.value.expires_ms <= self.now_ms) return error.SqlQueryExpired;
        if (old.value.status != .running or old.value.epoch != claim_.epoch or !std.mem.eql(u8, old.value.owner, claim_.owner) or old.value.until_ms <= self.now_ms) return error.SqlExecutionFenced;
        return old;
    }
    pub fn renew(self: Store, a: A, path: []const u8, claim_: Claim, ttl_ms: u64) !void {
        if (ttl_ms == 0 or ttl_ms > 60_000) return error.InvalidSqlExecutionClaim;
        var old = try self.owned(a, path, claim_);
        defer old.deinit(a);
        var next = old.value;
        next.until_ms = @min(next.expires_ms, self.now_ms +| ttl_ms);
        try self.write(a, path, old, next);
    }
    pub fn checkpoint(self: Store, a: A, path: []const u8, claim_: Claim, stage: u32, cursor: u32) !void {
        return self.checkpointTasks(a, path, claim_, stage, cursor, &.{});
    }
    pub fn checkpointTasks(self: Store, a: A, path: []const u8, claim_: Claim, stage: u32, cursor: u32, pending: []const u32) !void {
        return self.checkpointTasksAndReducers(a, path, claim_, stage, cursor, pending, null);
    }
    pub fn checkpointTasksAndReducers(self: Store, a: A, path: []const u8, claim_: Claim, stage: u32, cursor: u32, pending: []const u32, boundaries: ?[]const u16) !void {
        return self.checkpointPhysical(a, path, claim_, stage, cursor, pending, boundaries, null);
    }
    pub fn checkpointPhysical(self: Store, a: A, path: []const u8, claim_: Claim, stage: u32, cursor: u32, pending: []const u32, boundaries: ?[]const u16, skew: ?[]const @import("physical.zig").Part) !void {
        return self.checkpointResolved(a, path, claim_, stage, cursor, pending, boundaries, skew, null);
    }
    pub fn checkpointResolved(self: Store, a: A, path: []const u8, claim_: Claim, stage: u32, cursor: u32, pending: []const u32, boundaries: ?[]const u16, skew: ?[]const @import("physical.zig").Part, physical_ref: ?Output) !void {
        return self.checkpointProgress(a, path, claim_, stage, cursor, pending, null, false, boundaries, skew, physical_ref);
    }
    pub fn checkpointTasksProgress(self: Store, a: A, path: []const u8, claim_: Claim, stage: u32, cursor: u32, pending: []const u32, after: ?u32) !void {
        return self.checkpointProgress(a, path, claim_, stage, cursor, pending, after, false, null, null, null);
    }
    pub fn checkpointTasksPhase(self: Store, a: A, path: []const u8, claim_: Claim, stage: u32, cursor: u32, pending: []const u32, after: ?u32, admission: bool) !void {
        return self.checkpointProgress(a, path, claim_, stage, cursor, pending, after, admission, null, null, null);
    }
    fn checkpointProgress(self: Store, a: A, path: []const u8, claim_: Claim, stage: u32, cursor: u32, pending: []const u32, after: ?u32, admission: bool, boundaries: ?[]const u16, skew: ?[]const @import("physical.zig").Part, physical_ref: ?Output) !void {
        if (after) |id| if (pending.len == 0 or id >= cursor) return error.InvalidSqlExecutionState;
        if (pending.len > 256) return error.InvalidSqlExecutionState;
        for (pending, 0..) |id, index| {
            if (id >= cursor or (index != 0 and id <= pending[index - 1])) return error.InvalidSqlExecutionState;
        }
        var old = try self.owned(a, path, claim_);
        defer old.deinit(a);
        if (stage < old.value.stage or (stage == old.value.stage and cursor < old.value.cursor)) return error.InvalidSqlExecutionState;
        if ((after != null or admission) and stage != old.value.stage) return error.InvalidSqlExecutionState;
        var next = old.value;
        next.stage = stage;
        next.cursor = cursor;
        next.pending = pending;
        next.pending_after = after;
        next.pending_admission = admission;
        if (physical_ref) |ref| {
            if (old.value.stage != 0 or stage != 1 or old.value.physical != null) return error.InvalidSqlExecutionState;
            next.physical = ref;
            next.adaptive = null;
        }
        if (boundaries) |value| {
            if (old.value.stage != 0 or stage != 1 or old.value.reduce_boundaries.len != 0 or value.len < 2 or value.len > 1025) return error.InvalidSqlExecutionState;
            next.reduce_boundaries = value;
            next.adaptive = null;
            if (skew) |parts| next.skew = parts;
        }
        try self.write(a, path, old, next);
    }
    pub fn saveResultProgress(self: Store, a: A, path: []const u8, claim_: Claim, output: Output) !void {
        var old = try self.owned(a, path, claim_);
        defer old.deinit(a);
        var next = old.value;
        next.result_progress = output;
        try self.write(a, path, old, next);
    }
    pub fn saveAdaptive(self: Store, a: A, path: []const u8, claim_: Claim, output: Output) !void {
        var old = try self.owned(a, path, claim_);
        defer old.deinit(a);
        if (old.value.stage != 0 or old.value.pending.len != 0) return error.InvalidSqlExecutionState;
        var next = old.value;
        next.adaptive = output;
        try self.write(a, path, old, next);
    }
    /// A read-authorized submitter can request cancellation without stealing
    /// the coordinator lease. The current fenced owner performs termination.
    pub fn requestCancel(self: Store, a: A, path: []const u8) !void {
        for (0..8) |_| {
            var old = (try self.load(a, path)) orelse return error.SqlExecutionNotFound;
            defer old.deinit(a);
            if (old.value.terminal() or old.value.cancel_requested) return;
            var next = old.value;
            next.cancel_requested = true;
            self.write(a, path, old, next) catch |err| switch (err) {
                error.PreconditionFailed => continue,
                else => return err,
            };
            return;
        }
        return error.PreconditionFailed;
    }
    fn retireDiscovery(self: Store, a: A, path: []const u8, record: Record) void {
        if (!std.mem.endsWith(u8, path, "/query.json")) return;
        // The terminal CAS is durable authority. Expiry GC enumerates the
        // actual state/exchange roots, so removing this hint cannot leak data.
        const key_ = std.fmt.allocPrint(a, "{s}/sql-query-expiry/{d:0>20}/{s}.json", .{ self.prefix, record.expires_ms, record.plan }) catch return;
        defer a.free(key_);
        var client = self.client;
        client.allocator = a;
        client.deleteObject(self.bucket, key_, .{ .cancellation = catalog.types.contextCancellation(&self.context) }) catch {};
    }
    pub fn complete(self: Store, a: A, path: []const u8, claim_: Claim, output: Output) !void {
        var old = (try self.load(a, path)) orelse return error.SqlExecutionNotFound;
        defer old.deinit(a);
        // Exact accepted retries survive an ACK loss, including after expiry.
        if (old.value.status == .succeeded) {
            if (old.value.epoch != claim_.epoch or !std.mem.eql(u8, old.value.owner, claim_.owner) or !std.meta.eql(old.value.output.?.hash, output.hash) or !std.mem.eql(u8, old.value.output.?.key, output.key) or old.value.output.?.bytes != output.bytes or old.value.output.?.rows != output.rows or old.value.output.?.partitions != output.partitions or old.value.output.?.index_bytes != output.index_bytes or !std.meta.eql(old.value.output.?.index_hash, output.index_hash) or !sameWitness(old.value.output.?.matches, output.matches)) return error.SqlExecutionFenced;
            self.retireDiscovery(a, path, old.value);
            return;
        }
        if (old.value.expires_ms <= self.now_ms) return error.SqlQueryExpired;
        if (old.value.status != .running or old.value.epoch != claim_.epoch or !std.mem.eql(u8, old.value.owner, claim_.owner) or old.value.until_ms <= self.now_ms) return error.SqlExecutionFenced;
        var next = old.value;
        if (old.value.cancel_requested) {
            next.status = .cancelled;
            next.failure = "Cancelled";
        } else {
            next.status = .succeeded;
            next.output = output;
        }
        try self.write(a, path, old, next);
        self.retireDiscovery(a, path, next);
    }
    pub fn terminate(self: Store, a: A, path: []const u8, claim_: Claim, status: Status, failure: []const u8) !void {
        if ((status != .failed and status != .cancelled) or failure.len > 256) return error.InvalidSqlExecutionState;
        var old = try self.owned(a, path, claim_);
        defer old.deinit(a);
        var next = old.value;
        next.status = status;
        next.failure = failure;
        try self.write(a, path, old, next);
        self.retireDiscovery(a, path, next);
    }
};

test "distributed SQL state fences delayed owners and resolves accepted completion ACK loss" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(a, memory.client());
    defer faults.deinit();
    var store: Store = .{ .client = faults.client(), .bucket = "queries", .prefix = "tenant", .now_ms = 1000 };
    const key_ = try store.key(scratch, .{ .id = @splat('a'), .expires_ms = 1000000 }, 0, 0);
    try store.create(scratch, key_, @splat('b'), 1000000);
    const first = (try store.claim(scratch, key_, "pod-a", 1000)).?;
    try std.testing.expect(try store.claim(scratch, key_, "pod-b", 1000) == null);
    store.now_ms = 32001;
    const second = (try store.claim(scratch, key_, "pod-b", 1000)).?;
    const output: Output = .{ .key = "result", .hash = @splat('c'), .bytes = 10 };
    try std.testing.expectError(error.SqlExecutionFenced, store.complete(scratch, key_, first, output));
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    try std.testing.expectError(error.ConnectionResetByPeer, store.complete(scratch, key_, second, output));
    try store.complete(scratch, key_, second, output);
    try std.testing.expect(try store.claim(scratch, key_, "pod-c", 1000) == null);
    try std.testing.expectError(error.SqlExecutionIdReused, store.create(scratch, key_, @splat('z'), 1000000));
}

test "distributed SQL state keeps cancellation terminal and rejects query path injection" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    const store: Store = .{ .client = memory.client(), .bucket = "queries", .prefix = "tenant", .now_ms = 1000 };
    try std.testing.expectError(error.InvalidSqlExecutionState, store.key(sa, .{ .id = @splat('/'), .expires_ms = 1000000 }, 0, 0));
    const key_ = try store.key(sa, .{ .id = @splat('a'), .expires_ms = 1000000 }, null, 0);
    try store.create(sa, key_, @splat('b'), 100000);
    const owner = (try store.claim(sa, key_, "coordinator", 1000)).?;
    try store.terminate(sa, key_, owner, .cancelled, "Cancelled");
    try std.testing.expectError(error.SqlExecutionFenced, store.checkpoint(sa, key_, owner, 1, 0));
    try std.testing.expectError(error.SqlExecutionFenced, store.complete(sa, key_, owner, .{ .key = "ignored", .hash = @splat('c'), .bytes = 1 }));
    try std.testing.expect(try store.claim(sa, key_, "replacement", 1000) == null);
}

test "distributed SQL cancellation wins a later result CAS without stealing ownership" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    const store: Store = .{ .client = memory.client(), .bucket = "execution", .prefix = "queries", .now_ms = 1 };
    const query: Query = .{ .id = @splat('a'), .expires_ms = 1000000 };
    const key_ = try store.key(sa, query, null, 0);
    try store.create(sa, key_, query.id, query.expires_ms);
    const claim_ = (try store.claim(sa, key_, "coordinator", 60000)).?;
    try store.requestCancel(sa, key_);
    try store.complete(sa, key_, claim_, .{ .key = "result", .hash = @splat('b'), .bytes = 0 });
    var value = (try store.load(sa, key_)).?;
    defer value.deinit(sa);
    try std.testing.expectEqual(Status.cancelled, value.value.status);
    try std.testing.expectEqualStrings("coordinator", value.value.owner);
    try std.testing.expect(value.value.output == null);
}

test "distributed SQL pending scan checkpoints survive lost ACKs and fence takeover" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(a, memory.client());
    defer faults.deinit();
    var store: Store = .{ .client = faults.client(), .bucket = "queries", .prefix = "tenant", .now_ms = 1000 };
    const query: Query = .{ .id = @splat('a'), .expires_ms = 1000000 };
    const key_ = try store.key(scratch, query, null, 0);
    try store.create(scratch, key_, query.id, query.expires_ms);
    const first = (try store.claim(scratch, key_, "first", 1000)).?;
    try store.checkpointTasks(scratch, key_, first, 0, 8, &.{ 0, 2, 7 });
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    try std.testing.expectError(error.ConnectionResetByPeer, store.checkpointTasksPhase(scratch, key_, first, 0, 8, &.{ 0, 7 }, 3, true));
    var accepted = (try store.load(scratch, key_)).?;
    defer accepted.deinit(scratch);
    try std.testing.expectEqual(@as(?u32, 3), accepted.value.pending_after);
    try std.testing.expect(accepted.value.pending_admission);
    try std.testing.expectEqualSlices(u32, &.{ 0, 7 }, accepted.value.pending);
    store.now_ms = 32001;
    const second = (try store.claim(scratch, key_, "second", 1000)).?;
    try std.testing.expectError(error.SqlExecutionFenced, store.checkpointTasksProgress(scratch, key_, first, 0, 8, &.{0}, null));
    try store.checkpointTasksProgress(scratch, key_, second, 0, 8, &.{0}, null);
    try store.checkpointTasks(scratch, key_, second, 1, 0, &.{});
    var next = (try store.load(scratch, key_)).?;
    defer next.deinit(scratch);
    try std.testing.expect(next.value.pending_after == null);
    try std.testing.expectEqual(@as(u32, 1), next.value.stage);
}

test "distributed SQL admission ACK replay does not couple creation and status reads" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(a, memory.client());
    defer faults.deinit();
    const store: Store = .{ .client = faults.client(), .bucket = "queries", .prefix = "tenant", .now_ms = 1000 };
    const query: Query = .{ .id = @splat('a'), .expires_ms = 1000000 };
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    try std.testing.expectError(error.ConnectionResetByPeer, store.admitTasks(scratch, query, 0, &.{0}, null));
    const before = faults.get_attempts;
    const admitted = try store.admitTasks(scratch, query, 0, &.{0}, null);
    try std.testing.expect(admitted[0].needs_check);
    try std.testing.expectEqual(before, faults.get_attempts);
    const statuses = try store.taskStatuses(scratch, query, 0, &.{0}, null);
    try std.testing.expectEqual(Status.pending, statuses[0].status);
    try std.testing.expect(statuses[0].read_error == null);
    const conflicting = try store.key(scratch, query, 1, 0);
    try store.create(scratch, conflicting, @splat('b'), query.expires_ms);
    const unknown = try store.admitTasks(scratch, query, 1, &.{0}, null);
    try std.testing.expect(unknown[0].needs_check);
    const checked = try store.taskStatuses(scratch, query, 1, &.{0}, null);
    try std.testing.expectEqual(@as(?anyerror, error.SqlExecutionIdReused), checked[0].read_error);
}
