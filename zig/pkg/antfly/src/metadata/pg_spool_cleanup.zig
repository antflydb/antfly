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

//! Bounded runtime reclamation of a slot's original immutable upload scope.
//! Registry authorization is an internal maintenance capability, not a public
//! feed credential. The registry must resolve the recorded storage namespace.
//! Garbage is irreversible; concurrent collectors may safely repeat physical
//! work, but only a matching metadata checkpoint can return capacity.
const std = @import("std");
const control = @import("changefeed_control.zig");
const ledger = @import("pg_spool_control.zig");
const artifacts = @import("../serverless/artifacts/store.zig");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const codec = std.base64.url_safe_no_pad;
const max_decoded_token = ledger.max_cleanup_token_bytes / 4 * 3;
pub const Limits = struct {
    work: usize = 256,
    deletions: usize = 64,
    pub fn validate(self: Limits) !void {
        if (self.work < 4 or self.work > 4096 or self.deletions == 0 or self.deletions > 1024) return error.InvalidChangefeed;
    }
};
pub const Result = struct {
    status: enum { idle, waiting, checkpointed, collected, retained },
    work: usize = 0,
    deletions: usize = 0,
};
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
fn load(svc: anytype, a: A, id: control.Id, request: anytype) !control.Parsed {
    try request.ensureActive();
    const bytes = try control.readOnService(svc, a, id, request);
    defer a.free(bytes);
    var parsed = try control.parse(a, bytes);
    errdefer parsed.deinit();
    if (!same(parsed.value.id, id)) return error.ChangefeedResynchronizationRequired;
    return parsed;
}
fn mutate(svc: anytype, a: A, state: control.State, operation: control.pg_slot.Mutation, request: anytype) !void {
    const definition = state.definition orelse return error.ChangefeedConflict;
    return control.mutateOnService(svc, a, .{ .id = state.id, .authority = state.authority, .owner = definition.owner, .authorization = definition.authorization, .expected_revision = state.revision, .expected_epoch = state.epoch, .mutation = .{ .slot = operation } }, request);
}
fn Context(comptime Request: type) type {
    return struct {
        const Self = @This();
        request: Request,
        store: *artifacts.ArtifactStore,
        scope: artifacts.UploadScope,
        limits: Limits,
        work: usize = 1, // retirement barrier
        deletions: usize = 0,
        token: [ledger.max_cleanup_token_bytes]u8 = undefined,
        token_len: usize = 0,
        retained: bool = false,
        fn check(raw: *const anyopaque) !void {
            const self: *const Self = @ptrCast(@alignCast(raw));
            try self.request.ensureActive();
        }
        fn cancellation(self: *const Self) CancellationToken {
            return .{ .ptr = self, .check_fn = check };
        }
        fn maintenance(raw: *anyopaque, operation: artifacts.ScopedUploadVisitor.Maintenance) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            try self.request.ensureActive();
            // Leave one work unit for the independent physical-absence proof.
            if (self.work >= self.limits.work - 1 or (operation == .remove and self.deletions == self.limits.deletions)) return error.ArtifactEnumerationPaused;
            self.work += 1;
            if (operation == .remove) self.deletions += 1;
        }
        fn visit(raw: *anyopaque, scope: artifacts.UploadScope, id: []const u8) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            if (!std.meta.eql(self.scope, scope) or !std.meta.eql(self.scope, (try artifacts.uploadScopeFromArtifactId(id)) orelse return error.InvalidArtifactUploadScope)) return error.InvalidArtifactUploadScope;
            try maintenance(raw, .remove);
            self.store.deleteWithCancellation(id, self.cancellation()) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
        }
        fn checkpoint(raw: *anyopaque, token: []const u8) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            if (token.len > max_decoded_token) return error.PgSpoolCleanupTokenTooLarge;
            self.token_len = codec.Encoder.encode(&self.token, token).len;
        }
        fn retain(raw: *anyopaque, scope: artifacts.UploadScope) !void {
            const self: *Self = @ptrCast(@alignCast(raw));
            if (!std.meta.eql(self.scope, scope)) return error.InvalidArtifactUploadScope;
            self.retained = true;
        }
    };
}
/// At most one bounded sweep page and one metadata checkpoint. Call again to
/// continue. Unknown metadata responses escape without publishing further work;
/// the next call re-reads the exact durable progress before touching storage.
pub fn cleanup(svc: anytype, registry: anytype, a: A, id: control.Id, limits: Limits, request: anytype) !Result {
    try limits.validate();
    var before = try load(svc, a, id, request);
    defer before.deinit();
    const state = before.value;
    try registry.authorizeCleanup(state, request);
    const slot = state.slot orelse return error.ChangefeedCapabilityUnsupported;
    if (slot.spool.records.len == 0) return .{ .status = .idle };
    const record = slot.spool.records[0];
    if (record.phase != .garbage) return .{ .status = .waiting };
    var store = try registry.artifactStore(record.scope, request);
    if (store.namespace_identity == null or !same(store.namespace_identity.?, record.scope.namespace) or store.vtable.retire_scoped_uploads == null or store.vtable.scoped_uploads_reclaimed == null or store.vtable.visit_scoped_uploads == null) return error.PgSpoolStorageUnsupported;
    const scope: artifacts.UploadScope = .{ .domain = record.scope.domain, .attempt = record.scope.attempt };
    if (store.upload_scope) |bound| if (!std.meta.eql(bound, scope)) return error.InvalidArtifactUploadScope;
    var context: Context(@TypeOf(request)) = .{ .request = request, .store = &store, .scope = scope, .limits = limits };
    @memcpy(context.token[0..record.cleanup.continuation.len], record.cleanup.continuation);
    context.token_len = record.cleanup.continuation.len;
    try store.retireScopedUploads(scope.domain, record.id, context.cancellation());
    var progress = record.cleanup;
    var result: Result = .{ .status = .checkpointed, .work = 1 };
    if (!progress.fenced) {
        progress.fenced = true;
    } else if (progress.complete) {
        context.work += 1;
        if (!try store.scopedUploadsReclaimed(scope, context.cancellation())) return error.PgSpoolCleanupEvidenceChanged;
        result.status = .collected;
    } else {
        var decoded: [max_decoded_token]u8 = undefined;
        const size = try codec.Decoder.calcSizeForSlice(record.cleanup.continuation);
        try codec.Decoder.decode(decoded[0..size], record.cleanup.continuation);
        var exhausted = true;
        store.visitScopedUploads(scope.domain, .{ .ptr = &context, .visit = Context(@TypeOf(request)).visit, .maintenance = Context(@TypeOf(request)).maintenance, .checkpoint = Context(@TypeOf(request)).checkpoint, .retain_scope = Context(@TypeOf(request)).retain, .continuation = if (size == 0) null else decoded[0..size], .only_attempt = scope.attempt, .fencing_floor = record.id, .fencing_cutoff = record.id + 1, .max_entries = limits.work - 2, .cleanup_staging = true, .reclaim_retired_inventory = true }, context.cancellation()) catch |err| switch (err) {
            error.ArtifactEnumerationPaused => exhausted = false,
            error.InvalidArtifactEnumerationContinuation => {
                // This private cursor carries no client acknowledgement. The
                // irreversible scope fence makes rediscovery safe after a
                // provider checkpoint format/incarnation change.
                context.token_len = 0;
                exhausted = false;
            },
            else => return err,
        };
        context.work += 1;
        if (try store.scopedUploadsReclaimed(scope, context.cancellation())) {
            progress.complete = true;
            progress.proof = control.hash(.{ .domain = "antfly:pg-spool-reclaimed:v1", .feed = state.id, .epoch = state.epoch, .scope = record.scope, .manifest = record.manifest, .content = record.content });
            progress.continuation = "";
        } else {
            // A provider can exhaust an attempt while retaining unknown files.
            // Restart discovery instead of persisting an end token over them.
            if (exhausted) context.token_len = 0;
            progress.continuation = context.token[0..context.token_len];
            if (exhausted or context.retained) result.status = .retained;
        }
    }
    result.work = context.work;
    result.deletions = context.deletions;
    // Recheck authority after physical work. Concurrent collectors may have
    // advanced or removed this record; never overwrite their opaque cursor.
    var after = try load(svc, a, id, request);
    defer after.deinit();
    try registry.authorizeCleanup(after.value, request);
    if (after.value.epoch != state.epoch or !same(after.value.authority, state.authority) or !same(after.value.definition.?.identity(), state.definition.?.identity())) return error.ChangefeedResynchronizationRequired;
    const current_slot = after.value.slot orelse return error.ChangefeedConflict;
    if (current_slot.spool.collected_through >= record.id) {
        result.status = .collected;
        return result;
    }
    const current = current_slot.spool.find(record.id) orelse return error.ChangefeedConflict;
    if (current.id != current_slot.spool.records[0].id or current.phase != .garbage or !std.meta.eql(current.scope, record.scope) or !std.meta.eql(current.spec, record.spec) or !current.cleanup.eql(record.cleanup)) return error.ChangefeedConflict;
    if (result.status == .collected) {
        try mutate(svc, a, after.value, .{ .reclaim_spool = .{ .id = record.id, .storage = record.spec.storage, .proof = record.cleanup.proof } }, request);
    } else if (!progress.eql(record.cleanup)) {
        progress.revision = std.math.add(u64, record.cleanup.revision, 1) catch return error.ChangefeedConflict;
        try mutate(svc, a, after.value, .{ .checkpoint_spool_cleanup = .{ .id = record.id, .storage = record.spec.storage, .expected = record.cleanup.revision, .progress = progress } }, request);
    } else if (result.status == .checkpointed) result.status = .waiting;
    return result;
}
