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

//! Metadata owns durable population jobs; the requesting node owns the one
//! producer evaluation. Every control binds the admitted job and staging plan.
const std = @import("std");
const jobs = @import("../api/restore_jobs.zig");
const stages = @import("restore_staging.zig");
const operation = @import("antfly_local_sources").api_operation;
const A = std.mem.Allocator;
pub const max_command_bytes = 2 * stages.max_encoded_bytes + 64 * 1024;
pub const max_response_bytes = 512 * 1024;

pub const Reference = struct {
    job_id: u64,
    namespace: []const u8,
    key: []const u8,
    fingerprint: []const u8,
    plan_digest: [32]u8,

    pub fn validate(self: Reference, a: A) !void {
        if (self.namespace.len == 0 or self.namespace.len > 256 or self.key.len == 0 or self.key.len > 256 or
            self.fingerprint.len != 64 or std.mem.allEqual(u8, &self.plan_digest, 0)) return error.InvalidGenerationPublication;
        if (self.job_id != try jobs.jobIdForIdempotency(a, self.namespace, self.key)) return error.InvalidGenerationPublication;
    }
    pub fn requireState(self: Reference, a: A, state: jobs.JobState) !void {
        try self.validate(a);
        if (state.source_kind != .query_population or state.job_id != self.job_id or
            !std.mem.eql(u8, self.namespace, state.idempotency_namespace) or !std.mem.eql(u8, self.key, state.idempotency_key) or
            !std.mem.eql(u8, self.fingerprint, state.request_fingerprint)) return error.IdempotencyConflict;
    }
};
pub const Request = union(enum) {
    admit: jobs.StartRequest,
    observe: Reference,
    cancel: Reference,
    pub fn isMutation(self: Request) bool {
        return self != .observe;
    }
    pub fn validate(self: Request, a: A) !void {
        switch (self) {
            .admit => |req| {
                if (req.scope != .cluster or req.source_kind != .query_population or req.table_name != null or
                    req.rewrite_plan_json != null or req.generation_plan_json == null or req.idempotency_key == null or
                    req.table_names == null or req.table_names.?.len != 1 or
                    req.generation_plan_json.?.len > stages.max_encoded_bytes or
                    !std.mem.eql(u8, req.location, "metadata://query-population") or !std.mem.eql(u8, req.connection, "internal")) return error.InvalidGenerationPublication;
            },
            .observe, .cancel => |ref| try ref.validate(a),
        }
    }
};
/// Only a receiver's explicit not-proposed witness may expose these errors.
/// A dropped response or a post-proposal error remains an unknown admission.
pub fn definiteHttpFailure(status: u16, body: []const u8) ?anyerror {
    const domain = @import("antfly_local_sources").system_catalog_domain;
    inline for (.{ error.IdempotencyConflict, error.InvalidGenerationPublication, error.Forbidden, error.NotFound, error.AsyncRestoreUnavailable, error.NotLeader, error.TableTopologyProtocolUpgradeRequired, error.RelationalTopologyProtocolUpgradeRequired }) |err| {
        if (status == domain.httpStatus(err) and std.mem.eql(u8, body, @errorName(err))) return err;
    }
    return null;
}
/// The producer is not necessarily a replica of the target. Admission returns
/// the persisted plan commitment through administrative job authority; it must
/// not borrow a target-owner placement grant just to prove its own evaluation.
pub const Admission = struct {
    result: jobs.Store.Admission,
    plan_digest: ?stages.Digest = null,
};

/// Takes ownership of result; failures after durable admission remain unknown.
pub fn bindAdmission(a: A, source: anytype, context: operation.RequestContext, result: jobs.Store.Admission) !Admission {
    errdefer switch (result) {
        inline else => |bytes| a.free(bytes),
    };
    if (result == .unknown) return .{ .result = result };
    var state = std.json.parseFromSlice(jobs.JobState, a, result.accepted, .{ .ignore_unknown_fields = true }) catch return error.MetadataMutationOutcomeUnknown;
    defer state.deinit();
    if (state.value.phase == .failed) return .{ .result = result };
    const id = stages.idForAttempt(state.value.job_id, state.value.staging_attempt_id) catch return error.MetadataMutationOutcomeUnknown;
    const bytes = (source.getRestoreStaging(a, id, context) catch return error.MetadataMutationOutcomeUnknown) orelse return error.MetadataMutationOutcomeUnknown;
    defer a.free(bytes);
    var header = std.json.parseFromSlice(struct { plan_digest: stages.Digest, plan: struct { id: stages.Id } }, a, bytes, .{ .ignore_unknown_fields = true }) catch return error.MetadataMutationOutcomeUnknown;
    defer header.deinit();
    if (!std.mem.eql(u8, &header.value.plan.id, &id) or std.mem.allEqual(u8, &header.value.plan_digest, 0)) return error.MetadataMutationOutcomeUnknown;
    return .{ .result = result, .plan_digest = header.value.plan_digest };
}

pub const Port = struct {
    ptr: *anyopaque,
    execute: *const fn (*anyopaque, A, operation.RequestContext, Request) anyerror![]u8,
};
pub const Input = struct { alloc: A, context: operation.RequestContext, request: Request };

test "SQL scope retirement population controls bind exact job custody and keep observations read only" {
    const a = std.testing.allocator;
    const Call = @import("../system_catalog/server_call.zig").Call;
    const ref: Reference = .{ .job_id = try jobs.jobIdForIdempotency(a, "population-test", "capture-1"), .namespace = "population-test", .key = "capture-1", .fingerprint = &@as([64]u8, @splat('a')), .plan_digest = @splat(2) };
    try ref.validate(a);
    for ([_]Request{ .{ .observe = ref }, .{ .cancel = ref } }) |request| {
        const call: Call = .{ .query_population_job = request };
        try std.testing.expect(call.requiresAdministrativeGrant());
        try std.testing.expect(call.requiresSettingAuthorityReadGrant());
        try std.testing.expectEqual(request != .observe, call.isMutation());
    }
    var changed = ref;
    changed.key = "capture-2";
    try std.testing.expectError(error.InvalidGenerationPublication, changed.validate(a));
    changed = ref;
    changed.plan_digest = @splat(0);
    try std.testing.expectError(error.InvalidGenerationPublication, changed.validate(a));
    const state: jobs.JobState = .{
        .format_version = 1,
        .job_id = ref.job_id,
        .enqueue_sequence = 1,
        .dispatch_sequence = 1,
        .scope = .cluster,
        .source_kind = .query_population,
        .backup_id = "definition",
        .location = "metadata://query-population",
        .connection = "internal",
        .idempotency_namespace = ref.namespace,
        .idempotency_key = ref.key,
        .request_fingerprint = ref.fingerprint,
        .created_at_ms = 1,
        .updated_at_ms = 1,
        .expires_at_ms = 100,
    };
    try ref.requireState(a, state);
    var other = state;
    other.source_kind = .table_snapshot;
    try std.testing.expectError(error.IdempotencyConflict, ref.requireState(a, other));
    other = state;
    other.request_fingerprint = &@as([64]u8, @splat('b'));
    try std.testing.expectError(error.IdempotencyConflict, ref.requireState(a, other));
}
