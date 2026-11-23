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

//! Closed Source-purpose owner bridge. execute rejoins native birth/upload and
//! current enrollment before retaining real root/artifact resources. This is
//! never a caller-authored Expected JSON signing service.
const std = @import("std");
const callbacks = @import("../runtime_callback_abi.zig");
const origin = @import("../system_catalog/logical_replication_origin.zig");
const artifact = @import("../storage/artifact_identity.zig");
const Context = @import("operation.zig").RequestContext;
pub const Upload = struct {
    token: u64,
    nonce: [16]u8,
    expires_at_unix_ns: u64,
    pub fn sameIdentity(self: @This(), other: @This()) bool {
        return self.token == other.token and std.mem.eql(u8, &self.nonce, &other.nonce);
    }
    pub fn validate(self: @This()) !void {
        if (self.token == 0 or self.token == std.math.maxInt(u64) or zero(self.nonce) or self.expires_at_unix_ns == 0) return error.InvalidReplicationCutoverIntent;
    }
};
/// Actual managed backend identity, fixed for this Source birth. A caller's
/// claim is not evidence; the producer/Writer independently resolve its kind.
pub const ArtifactCustody = struct {
    namespace_identity: [32]u8,
    shared_across_owners: bool,
    pub fn validate(self: @This()) !void {
        if (zero(self.namespace_identity)) return error.InvalidReplicationPayloadAdmission;
    }
};
/// There is no fabricated upload token in this pre-effect request.
pub const OriginRef = struct {
    metadata_group: u64,
    incarnation: [32]u8,
    owner: origin.Owner,
    birth: u64,
    expected_revision: u64,
    pub fn validate(self: @This()) !void {
        try self.owner.validate();
        if (self.metadata_group == 0 or !@import("../metadata/incarnation.zig").isValid(self.incarnation) or self.birth == 0 or self.expected_revision == 0) return error.InvalidReplicationCutoverIntent;
    }
};
pub const ArtifactRequest = struct {
    origin: OriginRef,
    upload_nonce: [16]u8,
    pub fn validate(self: @This()) !void {
        try self.origin.validate();
        if (zero(self.upload_nonce)) return error.InvalidReplicationCutoverIntent;
    }
};

/// Terminal cleanup is rooted in retained native origin/event custody, never
/// the current recipe or an external provider caller's Expected object.
pub const CleanupRequest = struct {
    origin: OriginRef,
    action: union(enum) { event: @import("replication_source_reference.zig").Reference, uploads, origin },
    pub fn validate(self: @This()) !void {
        try self.origin.validate();
        if (self.action == .event) {
            const reference = self.action.event;
            try reference.validate();
            if (!reference.owner.eql(self.origin.owner) or reference.birth != self.origin.birth) return error.InvalidReplicationCutoverIntent;
        }
    }
};
pub const Purpose = struct {
    metadata_group: u64,
    incarnation: [32]u8,
    owner: origin.Owner,
    birth: u64,
    source_digest: [32]u8,
    recipe_digest: [32]u8,
    roster_digest: [32]u8,
    upload: Upload,
    pub fn sameIdentity(self: @This(), other: @This()) bool {
        return self.metadata_group == other.metadata_group and self.birth == other.birth and self.owner.eql(other.owner) and self.upload.sameIdentity(other.upload) and
            std.mem.eql(u8, &self.incarnation, &other.incarnation) and std.mem.eql(u8, &self.source_digest, &other.source_digest) and
            std.mem.eql(u8, &self.recipe_digest, &other.recipe_digest) and std.mem.eql(u8, &self.roster_digest, &other.roster_digest);
    }
    pub fn validate(self: @This()) !void {
        try self.owner.validate();
        try self.upload.validate();
        if (self.metadata_group == 0 or self.birth == 0 or zero(self.incarnation) or zero(self.source_digest) or zero(self.recipe_digest) or zero(self.roster_digest)) return error.InvalidReplicationCutoverIntent;
    }
};
pub const Ready = struct {
    upload: Upload,
    capture: artifact.Retained,
    execution_context: artifact.Retained,
    source_point: u64,
    snapshot_token_digest: [32]u8,
    total_rows: u64,
    pub fn validate(self: @This()) !void {
        try self.upload.validate();
        try self.capture.validate();
        try self.execution_context.validate();
        if (!std.meta.eql(self.capture.scope, self.execution_context.scope) or self.capture.scope.fencingToken() != self.upload.token or self.source_point == 0 or zero(self.snapshot_token_digest)) return error.InvalidReplicationCutoverIntent;
    }
};
pub const Member = struct {
    index: u16,
    owner_group_id: u64,
    namespace: @import("../storage/db/doc_identity_namespace.zig").Namespace,
    topology_epoch: u64,
    schema_version: u32,
    pregrant_body_digest: [32]u8,
    request_digest: [32]u8,
    pub fn validate(self: @This()) !void {
        if (self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0) return error.InvalidReplicationCutoverIntent;
        if (self.owner_group_id == 0 or self.topology_epoch == 0 or zero(self.pregrant_body_digest) or zero(self.request_digest)) return error.InvalidReplicationCutoverIntent;
    }
};
pub const Intent = struct {
    transaction_id: [16]u8,
    event_digest: [32]u8,
    manifest_digest: [32]u8,
    payload: artifact.Retained,
    execution_context: artifact.Retained,
    members: []const Member,
    pub fn validate(self: @This()) !void {
        try self.payload.validate();
        try self.execution_context.validate();
        if (zero(self.transaction_id) or zero(self.event_digest) or zero(self.manifest_digest) or self.members.len > 64) return error.InvalidReplicationCutoverIntent;
        for (self.members, 0..) |member, i| {
            try member.validate();
            if (member.index != i) return error.InvalidReplicationCutoverIntent;
        }
    }
};
pub const Seal = union(enum) {
    ready: Ready,
    intent: Intent,
    pub fn validate(self: @This()) !void {
        switch (self) {
            inline else => |value| try value.validate(),
        }
    }
};
/// A single synchronous Source-purpose proof operation. The real host owns
/// its managed root, artifact resources and current actor for this entire call;
/// no idle callback handle can retain a borrowed API server or request scope.
pub const Port = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, Purpose, Seal, Context) anyerror![]u8,
    artifact_fn: ?*const fn (*anyopaque, std.mem.Allocator, ArtifactRequest, Context) anyerror![]u8 = null,
    cleanup_fn: ?*const fn (*anyopaque, std.mem.Allocator, CleanupRequest, Context) anyerror![]u8 = null,
    boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const VTable = struct {
        cleanup_fn: *const fn (*anyopaque, std.mem.Allocator, CleanupRequest, Context) anyerror![]u8,
        execute_fn: *const fn (*anyopaque, std.mem.Allocator, Purpose, Seal, Context) anyerror![]u8,
        artifact_fn: *const fn (*anyopaque, std.mem.Allocator, ArtifactRequest, Context) anyerror![]u8,
    };
    const Boundary = callbacks.Boundary(VTable);
    pub fn cleanup(self: @This(), a: std.mem.Allocator, request: CleanupRequest, context: Context) ![]u8 {
        try context.ensureActive();
        try request.validate();
        const callback = self.cleanup_fn orelse return error.UnsupportedExactCutover;
        const bytes = try Boundary.call("cleanup_fn", self.boundary_dispatch, callback, .{ self.ptr, a, request, context });
        errdefer a.free(bytes);
        if (bytes.len == 0 or bytes.len > 4096) return error.InvalidReplicationPayloadAdmission;
        try context.ensureActive();
        return bytes;
    }
    pub fn artifact(self: @This(), a: std.mem.Allocator, request: ArtifactRequest, context: Context) ![]u8 {
        try context.ensureActive();
        try request.validate();
        const callback = self.artifact_fn orelse return error.UnsupportedExactCutover;
        const bytes = try Boundary.call("artifact_fn", self.boundary_dispatch, callback, .{ self.ptr, a, request, context });
        errdefer a.free(bytes);
        if (bytes.len == 0 or bytes.len > 4096) return error.InvalidReplicationPayloadAdmission;
        try context.ensureActive();
        return bytes;
    }
    pub fn execute(self: @This(), a: std.mem.Allocator, purpose: Purpose, request: Seal, context: Context) ![]u8 {
        try context.ensureActive();
        try purpose.validate();
        try request.validate();
        const bytes = try Boundary.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, a, purpose, request, context });
        errdefer a.free(bytes);
        if (bytes.len == 0 or bytes.len > 4096) return error.InvalidReplicationPayloadAdmission;
        try context.ensureActive();
        return bytes;
    }
};
fn zero(value: anytype) bool {
    return std.mem.allEqual(u8, &value, 0);
}
