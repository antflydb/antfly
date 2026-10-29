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

const std = @import("std");

test "serverless scoped artifact identities validate every routing component" {
    const a = std.testing.allocator;
    const scope: UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    const checksum = z17RepeatString("a", 64);
    const id = try scope.artifactId(checksum);
    try std.testing.expectEqualStrings(checksum, try sha256ChecksumFromArtifactId(&id));
    try std.testing.expectEqual(scope, (try uploadScopeFromArtifactId(&id)).?);
    const suffix = try storageSuffixAlloc(a, &id);
    defer a.free(suffix);
    try std.testing.expectEqualStrings("graph/" ++ z17RepeatString("01", 32) ++ "/" ++ z17RepeatString("02", 16) ++ "/" ++ checksum, suffix);
    for ([_]usize{ 0, 7, 71, 78, 142, 143, 174 }) |offset| {
        var corrupt = id;
        corrupt[offset] = '/';
        try std.testing.expectError(error.InvalidArtifactId, sha256ChecksumFromArtifactId(&corrupt));
        try std.testing.expectError(error.InvalidArtifactId, storageSuffixAlloc(a, &corrupt));
    }
    try std.testing.expectError(error.InvalidArtifactUploadScope, (UploadScope{ .domain = @splat(0), .attempt = scope.attempt }).artifactId(checksum));
    try std.testing.expectError(error.InvalidArtifactUploadScope, (UploadScope{ .domain = scope.domain, .attempt = @splat(0) }).artifactId(checksum));
}

pub fn chargeReadBudget(remaining: *u64, amount: u64) !void {
    if (amount > remaining.*) return error.ArtifactReadBudgetExceeded;
    remaining.* -= amount;
}
const Allocator = std.mem.Allocator;
const CancellationToken = @import("antfly_cancellation").CancellationToken;
pub const ReadContext = @import("antfly_local_sources").serverless_query_lake_read_context.Context;
const execution_wait = @import("antfly_local_sources").sql_execution_wait;

pub const sha256_checksum_len: usize = std.crypto.hash.sha2.Sha256.digest_length * 2;
pub const sha256_artifact_id_prefix = "sha256:";

/// A publication attempt owns newly uploaded pages, while immutable references
/// may reuse pages belonging to older attempts. Attempt identities must never
/// be reused after loss of publication authority. Scoped object names are also
/// the durable inventory: late uploads remain discoverable after a failed build.
pub const UploadScope = struct {
    domain: [32]u8,
    attempt: [16]u8,

    pub fn validate(self: UploadScope) !void {
        if (std.mem.allEqual(u8, &self.domain, 0) or self.fencingToken() == 0 or std.mem.allEqual(u8, self.attempt[8..16], 0))
            return error.InvalidArtifactUploadScope;
    }

    pub fn fencingToken(self: UploadScope) u64 {
        return std.mem.readInt(u64, self.attempt[0..8], .big);
    }

    pub fn forPublication(domain: [32]u8, token: u64, io: std.Io) !UploadScope {
        var scope = UploadScope{ .domain = domain, .attempt = undefined };
        std.mem.writeInt(u64, scope.attempt[0..8], token, .big);
        while (true) {
            io.random(scope.attempt[8..16]);
            if (!std.mem.allEqual(u8, scope.attempt[8..16], 0)) break;
        }
        try scope.validate();
        return scope;
    }

    pub fn artifactId(self: UploadScope, checksum: []const u8) ![175]u8 {
        try self.validate();
        try validateSha256Checksum(checksum);
        var id: [175]u8 = undefined;
        @memcpy(id[0..7], "sha256:");
        @memcpy(id[7..71], checksum);
        @memcpy(id[71..78], ":graph:");
        @memcpy(id[78..142], &std.fmt.bytesToHex(&self.domain, .lower));
        id[142] = ':';
        @memcpy(id[143..175], &std.fmt.bytesToHex(&self.attempt, .lower));
        return id;
    }
};

pub fn uploadScopeFromArtifactId(id: []const u8) !?UploadScope {
    if ((id.len != 71 and id.len != 175) or !std.mem.startsWith(u8, id, "sha256:")) return error.InvalidArtifactId;
    try validateSha256Checksum(id[7..71]);
    if (id.len == 71) return null;
    if (id.len != 175 or !std.mem.eql(u8, id[71..78], ":graph:") or id[142] != ':') return error.InvalidArtifactId;
    try validateSha256Checksum(id[78..142]);
    for (id[143..175]) |byte| if (!isLowerHex(byte)) return error.InvalidArtifactId;
    var scope: UploadScope = undefined;
    _ = std.fmt.hexToBytes(&scope.domain, id[78..142]) catch return error.InvalidArtifactId;
    _ = std.fmt.hexToBytes(&scope.attempt, id[143..175]) catch return error.InvalidArtifactId;
    scope.validate() catch return error.InvalidArtifactId;
    return scope;
}

/// Validates the complete identity before deriving a filesystem/object key.
pub fn storageSuffixAlloc(alloc: Allocator, id: []const u8) ![]u8 {
    const checksum = try sha256ChecksumFromArtifactId(id);
    if (try uploadScopeFromArtifactId(id)) |_| return std.fmt.allocPrint(alloc, "graph/{s}/{s}/{s}", .{ id[78..142], id[143..175], checksum });
    return std.fmt.allocPrint(alloc, "sha256/{s}/{s}", .{ checksum[0..2], checksum[2..] });
}

pub const ScopedUploadVisitor = struct {
    /// Backend-owned durable continuation. Callers persist it only after all
    /// delivered entries have been processed. Lexical providers may ignore it.
    continuation: ?[]const u8 = null,
    /// Remove deterministic local staging files while visiting fenced records.
    cleanup_staging: bool = false,
    checkpoint: ?*const fn (*anyopaque, []const u8) anyerror!void = null,
    after_suffix: ?[]const u8 = null,
    fencing_floor: u64 = 0,
    exclude_attempt: ?[16]u8 = null,
    only_attempt: ?[16]u8 = null,
    fencing_cutoff: ?u64 = null,
    max_entries: ?usize = null,
    ptr: *anyopaque,
    /// ID is borrowed only for this call. Enumeration is namespace-local and
    /// bounded; visitors must not assume a snapshot of concurrent late uploads.
    visit: *const fn (*anyopaque, UploadScope, []const u8) anyerror!void,
};

/// Parse only canonical objects below graph/<domain>/. Temporary local files
/// are not artifact identities and are deliberately excluded from this API.
pub fn visitScopedSuffix(domain: [32]u8, suffix: []const u8, visitor: ScopedUploadVisitor) !void {
    if (suffix.len != 97 or suffix[32] != '/') return;
    for (suffix[0..32]) |byte| if (!isLowerHex(byte)) return error.InvalidArtifactId;
    try validateSha256Checksum(suffix[33..97]);
    var scope: UploadScope = .{ .domain = domain, .attempt = undefined };
    _ = std.fmt.hexToBytes(&scope.attempt, suffix[0..32]) catch return error.InvalidArtifactId;
    const id = try scope.artifactId(suffix[33..97]);
    try visitor.visit(visitor.ptr, scope, &id);
}

/// Returns the checksum portion of a canonical content-addressed artifact ID.
/// Artifact stores use this before any filesystem or object-store access so a
/// malformed ID cannot select an arbitrary cache key or silently weaken
/// payload verification.
pub fn sha256ChecksumFromArtifactId(artifact_id: []const u8) ![]const u8 {
    _ = try uploadScopeFromArtifactId(artifact_id);
    return artifact_id[sha256_artifact_id_prefix.len..71];
}

pub fn validateSha256Checksum(checksum: []const u8) !void {
    if (checksum.len != sha256_checksum_len) return error.InvalidArtifactId;
    for (checksum) |byte| {
        if (!isLowerHex(byte)) return error.InvalidArtifactId;
    }
}

pub fn sha256DigestFromChecksum(checksum: []const u8) ![std.crypto.hash.sha2.Sha256.digest_length]u8 {
    try validateSha256Checksum(checksum);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    _ = std.fmt.hexToBytes(&digest, checksum) catch return error.InvalidArtifactId;
    return digest;
}

pub fn validateSha256ArtifactIdentity(artifact_id: []const u8, checksum: []const u8) !void {
    try validateSha256Checksum(checksum);
    const id_checksum = try sha256ChecksumFromArtifactId(artifact_id);
    if (!std.mem.eql(u8, id_checksum, checksum)) return error.InvalidArtifactId;
}

fn isLowerHex(byte: u8) bool {
    return (byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f');
}

pub const ArtifactMetadata = struct {
    provider_version: ?[]u8 = null,
    artifact_id: []u8,
    byte_len: u64,
    checksum: []u8,

    pub fn deinit(self: *ArtifactMetadata, alloc: Allocator) void {
        if (self.provider_version) |value| alloc.free(value);
        alloc.free(self.artifact_id);
        alloc.free(self.checksum);
        self.* = undefined;
    }
};

/// Backend evidence must change after namespace deletion, replacement or restore.
pub const RetentionWitness = struct { scope: UploadScope, stamp: [32]u8 };

/// Operation-owned scheduling capability; never installed on a shared backend.
/// A batch reserves its full fan-out before starting children. Children borrow
/// a copy with this capability removed and drain before the owner resumes CPU.
pub const IoPhase = struct {
    io: std.Io,
    ptr: *anyopaque,
    max_parallel: usize,
    scheduler: ?*@import("antfly_local_sources").sql_parallel_scheduler.Scheduler = null,
    begin: *const fn (*anyopaque, usize) anyerror!void,
    finish: *const fn (*anyopaque) anyerror!void,
    abort: *const fn (*anyopaque) void,
    /// Joining an independently admitted CPU child parks without occupying an
    /// I/O lane. The child may need the last I/O lane to finish its scratch.
    park: ?*const fn (*anyopaque) void = null,
    resume_cpu: ?*const fn (*anyopaque) anyerror!void = null,
};
pub const ExecutionIo = struct {
    execution: *@import("antfly_local_sources").sql_parallel_scheduler.Execution,
    io: std.Io,
    pending: ?@import("antfly_local_sources").sql_parallel_scheduler.Execution.Io = null,
    bridge: IoPhase = undefined,
    pub fn capability(self: *ExecutionIo) ?*const IoPhase {
        // Disabling I/O lanes retains the established required-inline fallback.
        if (self.execution.scheduler.max_io_workers == 0) return null;
        self.bridge = .{ .io = self.io, .ptr = self, .max_parallel = @min(8, self.execution.scheduler.max_io_workers), .scheduler = self.execution.scheduler, .begin = begin, .finish = finish, .abort = abort, .park = park, .resume_cpu = resumeCpu };
        return &self.bridge;
    }
    fn park(raw: *anyopaque) void {
        const self: *ExecutionIo = @ptrCast(@alignCast(raw));
        std.debug.assert(self.pending == null);
        self.execution.pause();
    }
    fn resumeCpu(raw: *anyopaque) !void {
        const self: *ExecutionIo = @ptrCast(@alignCast(raw));
        try self.execution.enter(self.io);
    }
    fn begin(raw: *anyopaque, count: usize) !void {
        const self: *ExecutionIo = @ptrCast(@alignCast(raw));
        std.debug.assert(self.pending == null);
        self.pending = try self.execution.beginIo(self.io, count);
    }
    fn finish(raw: *anyopaque) !void {
        const self: *ExecutionIo = @ptrCast(@alignCast(raw));
        var pending = self.pending.?;
        self.pending = null;
        try pending.finish();
    }
    fn abort(raw: *anyopaque) void {
        const self: *ExecutionIo = @ptrCast(@alignCast(raw));
        if (self.pending) |*pending| pending.abort();
        self.pending = null;
    }
};
pub const IoGuard = struct {
    phase: ?*const IoPhase = null,
    pub fn abort(self: *IoGuard) void {
        if (self.phase) |phase| phase.abort(phase.ptr);
        self.phase = null;
    }
    /// Restore CPU admission before callers handle recoverable backend errors.
    /// Cancellation may still end resumption without retaining any I/O permit.
    pub fn failed(self: *IoGuard, failure: anyerror) anyerror {
        self.finish() catch |err| return err;
        return failure;
    }
    pub fn finish(self: *IoGuard) !void {
        const phase = self.phase orelse return;
        self.phase = null;
        try phase.finish(phase.ptr);
    }
};
pub const ArtifactStore = struct {
    /// The host guarantees that replacement owners resolve the same immutable namespace.
    shared_across_owners: bool = false,
    namespace_identity: ?[32]u8 = null,
    allocator: Allocator,
    ptr: *anyopaque,
    vtable: *const VTable,
    /// A borrowed publication-local capability. All newly created artifacts,
    /// including flat/search segments, belong to this fenced attempt. Reused
    /// immutable references keep their original identities.
    upload_scope: ?UploadScope = null,
    // A serial publication's write/retention capability. Reads may escape to
    // independently admitted prefetch owners and never borrow its CPU callback.
    io_phase: ?*const IoPhase = null,
    /// Borrowed by this operation only; shared provider handles stay unmodified.
    write_context: ?ReadContext = null,
    pub fn beginIo(self: *const ArtifactStore, count: usize) !IoGuard {
        if (self.write_context) |context| if (context.execution != null) return .{ .phase = null };
        if (self.io_phase) |phase| try phase.begin(phase.ptr, count);
        return .{ .phase = self.io_phase };
    }

    pub const VTable = struct {
        get_bounded_alloc_for: ?*const fn (*anyopaque, Allocator, []const u8, usize, CancellationToken, ReadContext) anyerror![]u8 = null,
        get_range_alloc_for: ?*const fn (*anyopaque, Allocator, []const u8, u64, usize, CancellationToken, ReadContext) anyerror![]u8 = null,

        verify_content_for: ?*const fn (*anyopaque, Allocator, []const u8, u64, []const u8, CancellationToken, ReadContext) anyerror!void = null,
        retention_witness_for: ?*const fn (*anyopaque, UploadScope, CancellationToken, ReadContext) anyerror!?RetentionWitness = null,
        retained_closure_for: ?*const fn (*anyopaque, []const u8, CancellationToken, ReadContext) anyerror!bool = null,
        remember_closure_for: ?*const fn (*anyopaque, []const u8, []const RetentionWitness, CancellationToken, ReadContext) anyerror!void = null,
        inherit_closure_for: ?*const fn (*anyopaque, []const u8, ?[]const u8, UploadScope, CancellationToken, ReadContext) anyerror!void = null,
        seal_closures_for: ?*const fn (*anyopaque, CancellationToken, ReadContext) anyerror!void = null,

        delete_for: ?*const fn (*anyopaque, []const u8, CancellationToken, ReadContext) anyerror!void = null,
        visit_scoped_uploads_for: ?*const fn (*anyopaque, [32]u8, ScopedUploadVisitor, CancellationToken, ReadContext) anyerror!void = null,
        reclaim_retired_scoped_inventory_for: ?*const fn (*anyopaque, [32]u8, u64, u64, CancellationToken, ReadContext) anyerror!void = null,
        cleanup_retired_scoped_temporaries_for: ?*const fn (*anyopaque, [32]u8, u64, u64, CancellationToken, ReadContext) anyerror!void = null,

        put_for: ?*const fn (*anyopaque, Allocator, ?UploadScope, []const u8, CancellationToken, ReadContext) anyerror!ArtifactMetadata = null,
        object_store: ?*const fn (*anyopaque) *@import("object_store.zig").ObjectStore = null,
        retention_witness: ?*const fn (*anyopaque, UploadScope, CancellationToken) anyerror!?RetentionWitness = null,
        retained_closure: ?*const fn (*anyopaque, []const u8, CancellationToken) anyerror!bool = null,
        remember_closure: ?*const fn (*anyopaque, []const u8, []const RetentionWitness, CancellationToken) anyerror!void = null,
        inherit_closure: ?*const fn (*anyopaque, []const u8, ?[]const u8, UploadScope, CancellationToken) anyerror!void = null,
        seal_closures: ?*const fn (*anyopaque, CancellationToken) anyerror!void = null,
        deinit: *const fn (Allocator, *anyopaque) void,
        put: *const fn (*anyopaque, Allocator, []const u8) anyerror!ArtifactMetadata,
        put_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, CancellationToken) anyerror!ArtifactMetadata = null,
        get_alloc: *const fn (*anyopaque, Allocator, []const u8) anyerror![]u8,
        get_alloc_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, CancellationToken) anyerror![]u8 = null,
        /// Return the complete object without a metadata probe, bounded by the
        /// expected size. The caller verifies exact length and SHA-256; a range
        /// read cannot substitute because it would hide appended bytes.
        get_bounded_alloc_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, usize, CancellationToken) anyerror![]u8 = null,
        get_range_alloc: *const fn (*anyopaque, Allocator, []const u8, u64, usize) anyerror![]u8,
        get_range_alloc_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, u64, usize, CancellationToken) anyerror![]u8 = null,
        get_verified_range_alloc_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, u64, []const u8, u64, usize, CancellationToken) anyerror![]u8 = null,
        get_verified_range_alloc_with_budget: ?*const fn (*anyopaque, Allocator, []const u8, u64, []const u8, u64, usize, CancellationToken, *u64) anyerror![]u8 = null,
        stat: *const fn (*anyopaque, Allocator, []const u8) anyerror!ArtifactMetadata,
        stat_with_cancellation: ?*const fn (*anyopaque, Allocator, []const u8, CancellationToken) anyerror!ArtifactMetadata = null,
        verify_content: ?*const fn (*anyopaque, Allocator, []const u8, u64, []const u8, CancellationToken) anyerror!void = null,
        delete: *const fn (*anyopaque, []const u8) anyerror!void,
        delete_with_cancellation: ?*const fn (*anyopaque, []const u8, CancellationToken) anyerror!void = null,
        put_scoped: ?*const fn (*anyopaque, Allocator, UploadScope, []const u8, CancellationToken) anyerror!ArtifactMetadata = null,
        visit_scoped_uploads: ?*const fn (*anyopaque, [32]u8, ScopedUploadVisitor, CancellationToken) anyerror!void = null,
        reclaim_retired_scoped_inventory: ?*const fn (*anyopaque, [32]u8, u64, u64, CancellationToken) anyerror!void = null,
        cleanup_retired_scoped_temporaries: ?*const fn (*anyopaque, [32]u8, u64, u64, CancellationToken) anyerror!void = null,
    };

    pub fn providerStore(self: ArtifactStore) ?*@import("object_store.zig").ObjectStore {
        return if (self.vtable.object_store) |call| call(self.ptr) else null;
    }
    pub fn retentionWitness(self: *ArtifactStore, scope: UploadScope, cancellation: CancellationToken) !?RetentionWitness {
        try scope.validate();
        try cancellation.check();
        if (self.write_context) |context| if (self.vtable.retention_witness_for) |call| {
            const op: @import("../provider_operation.zig").Operation = .{ .context = context, .cancellation = cancellation };
            try op.ready();
            const value = try call(self.ptr, scope, cancellation, context);
            try op.ready();
            return value;
        };
        const call = self.vtable.retention_witness orelse return null;
        var phase = try self.beginIo(1);
        defer phase.abort();
        const result = call(self.ptr, scope, cancellation) catch |err| return phase.failed(err);
        try phase.finish();
        return result;
    }
    pub fn retainedClosure(self: *ArtifactStore, id: []const u8, cancellation: CancellationToken) !bool {
        try cancellation.check();
        if (self.write_context) |context| if (self.vtable.retained_closure_for) |call| {
            const op: @import("../provider_operation.zig").Operation = .{ .context = context, .cancellation = cancellation };
            try op.ready();
            const value = try call(self.ptr, id, cancellation, context);
            try op.ready();
            return value;
        };
        const call = self.vtable.retained_closure orelse return false;
        var phase = try self.beginIo(1);
        defer phase.abort();
        const result = call(self.ptr, id, cancellation) catch |err| return phase.failed(err);
        try phase.finish();
        return result;
    }
    pub fn rememberClosure(self: *ArtifactStore, id: []const u8, witnesses: []const RetentionWitness, cancellation: CancellationToken) !void {
        try cancellation.check();
        if (self.write_context) |context| if (self.vtable.remember_closure_for) |call| {
            const op: @import("../provider_operation.zig").Operation = .{ .context = context, .cancellation = cancellation };
            try op.ready();
            try call(self.ptr, id, witnesses, cancellation, context);
            try op.ready();
            return;
        };
        if (self.vtable.remember_closure) |call| {
            var phase = try self.beginIo(1);
            defer phase.abort();
            call(self.ptr, id, witnesses, cancellation) catch |err| return phase.failed(err);
            try phase.finish();
        }
    }
    pub fn inheritClosure(self: *ArtifactStore, id: []const u8, previous: ?[]const u8, cancellation: CancellationToken) !void {
        try cancellation.check();
        if (self.write_context) |context| if (self.vtable.inherit_closure_for) |call| {
            const op: @import("../provider_operation.zig").Operation = .{ .context = context, .cancellation = cancellation };
            try op.ready();
            try call(self.ptr, id, previous, self.upload_scope orelse return error.InvalidArtifactUploadScope, cancellation, context);
            try op.ready();
            return;
        };
        if (self.vtable.inherit_closure) |call| {
            var phase = try self.beginIo(1);
            defer phase.abort();
            call(self.ptr, id, previous, self.upload_scope orelse return error.InvalidArtifactUploadScope, cancellation) catch |err| return phase.failed(err);
            try phase.finish();
        }
    }
    pub fn sealClosures(self: *ArtifactStore, cancellation: CancellationToken) !void {
        try cancellation.check();
        if (self.write_context) |context| if (self.vtable.seal_closures_for) |call| {
            const op: @import("../provider_operation.zig").Operation = .{ .context = context, .cancellation = cancellation };
            try op.ready();
            try call(self.ptr, cancellation, context);
            try op.ready();
            return;
        };
        if (self.vtable.seal_closures) |call| {
            var phase = try self.beginIo(1);
            defer phase.abort();
            call(self.ptr, cancellation) catch |err| return phase.failed(err);
            try phase.finish();
        }
    }

    pub fn deinit(self: *ArtifactStore) void {
        self.vtable.deinit(self.allocator, self.ptr);
        self.* = undefined;
    }

    pub fn put(self: *ArtifactStore, contents: []const u8) !ArtifactMetadata {
        return try self.putWithCancellation(contents, .none);
    }

    pub fn putScoped(self: *ArtifactStore, scope: UploadScope, contents: []const u8, cancellation: CancellationToken) !ArtifactMetadata {
        try scope.validate();
        if (self.upload_scope) |authority| {
            if (!std.mem.eql(u8, &scope.domain, &authority.domain) or scope.fencingToken() != authority.fencingToken())
                return error.InvalidArtifactUploadScope;
        }
        try cancellation.check();
        const put_scoped = self.vtable.put_scoped orelse return error.ArtifactUploadScopesUnsupported;
        var phase = try self.beginIo(1);
        defer phase.abort();
        var metadata = (if (self.write_context != null and self.vtable.put_for != null)
            self.vtable.put_for.?(self.ptr, self.allocator, scope, contents, cancellation, self.write_context.?)
        else
            put_scoped(self.ptr, self.allocator, scope, contents, cancellation)) catch |err| return phase.failed(err);
        errdefer metadata.deinit(self.allocator);
        try phase.finish();
        const expected = try scope.artifactId(metadata.checksum);
        if (metadata.byte_len != contents.len or !std.mem.eql(u8, &expected, metadata.artifact_id)) return error.ArtifactIntegrityMismatch;
        try cancellation.check();
        return metadata;
    }

    pub fn visitScopedUploads(self: *ArtifactStore, domain: [32]u8, visitor: ScopedUploadVisitor, cancellation: CancellationToken) !void {
        if (std.mem.allEqual(u8, &domain, 0)) return error.InvalidArtifactUploadScope;
        try cancellation.check();
        if (self.write_context) |context| if (self.vtable.visit_scoped_uploads_for) |call| return call(self.ptr, domain, visitor, cancellation, context);
        const visit = self.vtable.visit_scoped_uploads orelse return error.ArtifactUploadScopesUnsupported;
        try visit(self.ptr, domain, visitor, cancellation);
    }

    /// After fenced sweeping, reclaim only discovery metadata whose payload
    /// attempt is empty. Remote providers need no local inventory cleanup.
    pub fn reclaimRetiredScopedInventory(self: *ArtifactStore, domain: [32]u8, floor: u64, cutoff: u64, cancellation: CancellationToken) !void {
        if (std.mem.allEqual(u8, &domain, 0) or floor > cutoff or cutoff == 0) return error.InvalidArtifactUploadScope;
        try cancellation.check();
        if (self.write_context) |context| if (self.vtable.reclaim_retired_scoped_inventory_for) |call| return call(self.ptr, domain, floor, cutoff, cancellation, context);
        if (self.vtable.reclaim_retired_scoped_inventory) |reclaim| try reclaim(self.ptr, domain, floor, cutoff, cancellation);
    }

    /// Only a collector that has fenced all older publications may call this.
    /// Backends without local staging files need no extra cleanup operation.
    pub fn cleanupRetiredScopedTemporaries(self: *ArtifactStore, domain: [32]u8, cutoff: u64, cancellation: CancellationToken) !void {
        return self.cleanupRetiredScopedTemporaryRange(domain, 0, cutoff, cancellation);
    }
    /// A rolling upgrade can fence new readers without proving old readers
    /// have drained. Preserve staging files below that admission floor.
    pub fn cleanupRetiredScopedTemporaryRange(self: *ArtifactStore, domain: [32]u8, floor: u64, cutoff: u64, cancellation: CancellationToken) !void {
        if (cutoff == 0 or std.mem.allEqual(u8, &domain, 0)) return error.InvalidArtifactUploadScope;
        if (floor > cutoff) return error.InvalidArtifactUploadScope;
        try cancellation.check();
        if (self.write_context) |context| if (self.vtable.cleanup_retired_scoped_temporaries_for) |call| return call(self.ptr, domain, floor, cutoff, cancellation, context);
        if (self.vtable.cleanup_retired_scoped_temporaries) |cleanup| try cleanup(self.ptr, domain, floor, cutoff, cancellation);
    }

    pub fn putWithCancellation(self: *ArtifactStore, contents: []const u8, cancellation: CancellationToken) !ArtifactMetadata {
        if (self.upload_scope) |scope| return self.putScoped(scope, contents, cancellation);
        try cancellation.check();
        var phase = try self.beginIo(1);
        defer phase.abort();
        var metadata = (if (self.write_context != null and self.vtable.put_for != null)
            self.vtable.put_for.?(self.ptr, self.allocator, null, contents, cancellation, self.write_context.?)
        else if (self.vtable.put_with_cancellation) |put_with_cancellation|
            put_with_cancellation(self.ptr, self.allocator, contents, cancellation)
        else
            self.vtable.put(self.ptr, self.allocator, contents)) catch |err| return phase.failed(err);
        errdefer metadata.deinit(self.allocator);
        try phase.finish();
        try cancellation.check();
        return metadata;
    }

    pub const max_upload_batch = 8;
    /// Borrow bounded payloads until every upload drains. The caller provides a
    /// concurrency-safe store allocator when opting into parallel I/O. Results
    /// retain input order; no result ownership transfers on failure, including
    /// cancellation while the publication owner resumes CPU execution.
    pub fn putBatchWithCancellation(self: *ArtifactStore, io: ?std.Io, contents: []const []const u8, result: []ArtifactMetadata, cancellation: CancellationToken) !void {
        std.debug.assert(contents.len == result.len and contents.len <= max_upload_batch);
        if (self.write_context) |context| if (context.execution != null) {
            return self.putBatchFor(context, contents, result, cancellation);
        };
        var completed: [max_upload_batch]?ArtifactMetadata = @splat(null);
        defer for (&completed) |*item| if (item.*) |*metadata| metadata.deinit(self.allocator);
        var begin: usize = 0;
        const width = if (self.io_phase) |phase| @max(1, @min(max_upload_batch, phase.max_parallel)) else max_upload_batch;
        while (begin < contents.len) {
            try cancellation.check();
            const end = @min(contents.len, begin + width);
            var phase = try self.beginIo(end - begin);
            defer phase.abort();
            var backend = self.*;
            backend.io_phase = null;
            const Worker = struct {
                fn run(owner: *ArtifactStore, bytes: []const u8, token: CancellationToken, output: *?ArtifactMetadata, failure: *?anyerror) void {
                    output.* = owner.putWithCancellation(bytes, token) catch |err| {
                        failure.* = err;
                        return;
                    };
                }
            };
            var failures: [max_upload_batch]?anyerror = @splat(null);
            if (io) |executor| {
                var group: std.Io.Group = .init;
                for (begin..end) |index| group.async(executor, Worker.run, .{ &backend, contents[index], cancellation, &completed[index], &failures[index] });
                group.await(executor) catch |err| {
                    group.cancel(executor);
                    return phase.failed(err);
                };
            } else for (begin..end) |index| Worker.run(&backend, contents[index], cancellation, &completed[index], &failures[index]);
            for (failures[begin..end]) |failure| if (failure) |err| return phase.failed(err);
            try phase.finish();
            begin = end;
        }
        try cancellation.check();
        for (result, completed[0..result.len]) |*output, *metadata| {
            output.* = metadata.*.?;
            metadata.* = null;
        }
    }

    /// Each upload owns a retained child before publication. The coordinator
    /// returns CPU before workers may activate, and all workers and provider
    /// output owners drain before that same coordinator resumes.
    fn putBatchFor(self: *ArtifactStore, context: ReadContext, contents: []const []const u8, result: []ArtifactMetadata, cancellation: CancellationToken) !void {
        const Runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
        const executor = context.io orelse return error.BackendRuntimeIoUnavailable;
        const parent = context.execution.?;
        const class = try parent.retainedClass();
        const Work = struct {
            owner: ArtifactStore,
            execution: Runnable.Execution = .{},
            output: ?ArtifactMetadata = null,
            failure: ?anyerror = null,
            fn run(job: *@This(), bytes: []const u8, token: CancellationToken) void {
                job.execution.activateChild() catch |err| {
                    job.failure = err;
                    return;
                };
                // A finished worker cannot pin CPU while its siblings queue.
                // Keep its entry/output retained until the coordinator joins.
                defer job.execution.park(.resource_blocked) catch {};
                job.owner.write_context = job.owner.write_context.?.withExecution(&job.execution);
                job.output = job.owner.putWithCancellation(bytes, token) catch |err| {
                    job.failure = err;
                    return;
                };
            }
        };
        var work: [max_upload_batch]Work = undefined;
        var reserved: usize = 0;
        defer for (work[0..reserved]) |*item| {
            item.execution.finish();
            if (item.output) |*metadata| metadata.deinit(self.allocator);
        };
        for (contents, 0..) |bytes, index| {
            try context.ensureActive();
            try cancellation.check();
            work[index] = .{ .owner = self.* };
            // A physical provider call owns its own wait; never borrow the
            // serial coordinator's I/O-phase callback into concurrent work.
            work[index].owner.io_phase = null;
            try work[index].execution.reserveChild(parent, executor, @sizeOf(Work) +| bytes.len);
            reserved += 1;
        }
        try parent.park(.resource_blocked);
        var group: std.Io.Group = .init;
        for (contents, 0..) |bytes, index| group.async(executor, Work.run, .{ &work[index], bytes, cancellation });
        const joined = group.await(executor);
        if (joined) |_| {} else |_| group.cancel(executor);
        // Retained entries outlive physical joining, including denied
        // activation or CPU return. Parent authority is never replaced.
        for (work[0..reserved]) |*item| item.execution.finish();
        parent.enter(class) catch |err| {
            try cancellation.check();
            try context.checkLifetime();
            return err;
        };
        try context.ensureActive();
        try cancellation.check();
        try joined;
        for (work[0..reserved]) |item| if (item.failure) |err| return err;
        for (work[0..reserved], result) |*item, *output| {
            output.* = item.output.?;
            item.output = null;
        }
    }

    pub fn getAlloc(self: *ArtifactStore, artifact_id: []const u8) ![]u8 {
        return try self.getAllocWithCancellation(artifact_id, .none);
    }

    pub fn getAllocWithCancellation(
        self: *ArtifactStore,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getAllocWithCancellationUsingAllocator(self.allocator, artifact_id, cancellation);
    }

    pub fn getAllocWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        const payload = if (self.vtable.get_alloc_with_cancellation) |get_with_cancellation|
            try get_with_cancellation(self.ptr, result_alloc, artifact_id, cancellation)
        else
            try self.vtable.get_alloc(self.ptr, result_alloc, artifact_id);
        errdefer result_alloc.free(payload);
        try cancellation.check();
        return payload;
    }

    /// Loads one content-addressed artifact without trusting either the backing
    /// store or its metadata to honor the manifest contract. The declared size
    /// bounds the transport allocation and the digest is checked before bytes
    /// can reach a decoder or cache.
    pub fn getVerifiedAllocWithCancellation(
        self: *ArtifactStore,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getVerifiedAllocWithCancellationUsingAllocator(
            self.allocator,
            artifact_id,
            expected_byte_len,
            expected_checksum,
            cancellation,
        );
    }

    pub fn getVerifiedAllocWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        return self.getVerifiedAllocFor(result_alloc, artifact_id, expected_byte_len, expected_checksum, cancellation, .{});
    }

    /// The read context is borrowed only for this synchronous operation.
    /// Provider calls relinquish CPU; integrity verification runs after resume.
    pub fn getVerifiedAllocFor(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
        context: ReadContext,
    ) ![]u8 {
        try context.ensureActive();
        try cancellation.check();
        const lifetime: ReadCancellation = .{ .context = context, .parent = cancellation };
        const token = lifetime.token();
        // Only converted providers may lend native CPU at verified physical
        // boundaries. Explicit legacy wait-only callers retain their contract.
        const legacy_wait = if (context.execution == null) context.wait else null;
        validateSha256ArtifactIdentity(artifact_id, expected_checksum) catch
            return error.ArtifactIntegrityMismatch;
        const expected_len = std.math.cast(usize, expected_byte_len) orelse
            return error.ArtifactTooLarge;

        if (self.vtable.get_bounded_alloc_for != null or self.vtable.get_bounded_alloc_with_cancellation != null) {
            const payload = (if (self.vtable.get_bounded_alloc_for) |call|
                call(self.ptr, result_alloc, artifact_id, expected_len, token, context)
            else
                execution_wait.runOwned(legacy_wait, ArtifactStore.getBoundedAlloc, .{ self, result_alloc, artifact_id, expected_len, token }, discardPayload, .{result_alloc})) catch |err| {
                try context.checkLifetime();
                return err;
            };
            errdefer result_alloc.free(payload);
            try context.ensureActive();
            if (payload.len != expected_len) return error.ArtifactIntegrityMismatch;
            try validatePayloadSha256For(payload, expected_checksum, token, context);
            return payload;
        }

        {
            var metadata = execution_wait.runOwned(legacy_wait, ArtifactStore.statWithCancellationUsingAllocator, .{ self, result_alloc, artifact_id, token }, discardMetadata, .{result_alloc}) catch |err| {
                try context.checkLifetime();
                return err;
            };
            defer metadata.deinit(result_alloc);
            try context.ensureActive();
            if (!std.mem.eql(u8, metadata.artifact_id, artifact_id) or
                metadata.byte_len != expected_byte_len or
                !std.mem.eql(u8, metadata.checksum, expected_checksum))
            {
                return error.ArtifactIntegrityMismatch;
            }
        }

        if (expected_len == 0) {
            const payload = try result_alloc.alloc(u8, 0);
            errdefer result_alloc.free(payload);
            try context.ensureActive();
            try validatePayloadSha256For(payload, expected_checksum, token, context);
            return payload;
        }

        const payload = execution_wait.runOwned(legacy_wait, ArtifactStore.getRangeAllocWithCancellationUsingAllocator, .{ self, result_alloc, artifact_id, @as(u64, 0), expected_len, token }, discardPayload, .{result_alloc}) catch |err| {
            try context.checkLifetime();
            return err;
        };
        errdefer result_alloc.free(payload);
        try context.ensureActive();
        if (payload.len != expected_len) return error.ArtifactIntegrityMismatch;
        try validatePayloadSha256For(payload, expected_checksum, token, context);
        return payload;
    }

    fn getBoundedAlloc(self: *ArtifactStore, alloc: Allocator, id: []const u8, length: usize, cancellation: CancellationToken) ![]u8 {
        return self.vtable.get_bounded_alloc_with_cancellation.?(self.ptr, alloc, id, length, cancellation);
    }

    pub fn getRangeAlloc(self: *ArtifactStore, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        return try self.getRangeAllocWithCancellation(artifact_id, offset, len, .none);
    }

    pub fn getRangeAllocWithCancellation(
        self: *ArtifactStore,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getRangeAllocWithCancellationUsingAllocator(self.allocator, artifact_id, offset, len, cancellation);
    }

    /// Only the physical read parks the caller. Authentication and decoding
    /// execute after reacquiring this call's CPU grant.
    pub fn getRangeAllocFor(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
        context: ReadContext,
    ) ![]u8 {
        try context.ensureActive();
        try cancellation.check();
        const lifetime: ReadCancellation = .{ .context = context, .parent = cancellation };
        if (self.vtable.get_range_alloc_for) |call| {
            const payload = call(self.ptr, result_alloc, artifact_id, offset, len, lifetime.token(), context) catch |err| {
                try context.checkLifetime();
                try cancellation.check();
                return err;
            };
            errdefer result_alloc.free(payload);
            try context.ensureActive();
            try cancellation.check();
            return payload;
        }
        const legacy_wait = if (context.execution == null) context.wait else null;
        const payload = execution_wait.runOwned(legacy_wait, ArtifactStore.getRangeAllocWithCancellationUsingAllocator, .{ self, result_alloc, artifact_id, offset, len, lifetime.token() }, discardPayload, .{result_alloc}) catch |err| {
            try context.checkLifetime();
            try cancellation.check();
            return err;
        };
        errdefer result_alloc.free(payload);
        try context.ensureActive();
        try cancellation.check();
        return payload;
    }

    pub fn getRangeAllocWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        if (len == 0) {
            _ = try sha256ChecksumFromArtifactId(artifact_id);
            var metadata = try self.statWithCancellationUsingAllocator(result_alloc, artifact_id, cancellation);
            defer metadata.deinit(result_alloc);
            if (offset > metadata.byte_len) return error.InvalidRange;
            const payload = try result_alloc.alloc(u8, 0);
            errdefer result_alloc.free(payload);
            try cancellation.check();
            return payload;
        }
        const payload = if (self.vtable.get_range_alloc_with_cancellation) |get_with_cancellation|
            try get_with_cancellation(self.ptr, result_alloc, artifact_id, offset, len, cancellation)
        else
            try self.vtable.get_range_alloc(self.ptr, result_alloc, artifact_id, offset, len);
        errdefer result_alloc.free(payload);
        try cancellation.check();
        return payload;
    }

    /// Loads a range from the exact object or file identity authenticated by
    /// `expected_checksum`. Native backends pin the provider generation, ETag,
    /// or open file identity across verification and reading. The fallback is
    /// retained for test and custom stores, but production stores should
    /// implement the vtable entry so replacement cannot race a range read.
    pub fn getVerifiedRangeAllocWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        validateSha256ArtifactIdentity(artifact_id, expected_checksum) catch
            return error.ArtifactIntegrityMismatch;
        const end = std.math.add(u64, offset, std.math.cast(u64, len) orelse return error.InvalidRange) catch
            return error.InvalidRange;
        if (end > expected_byte_len) return error.InvalidRange;
        if (len == 0) {
            try self.verifyContentWithCancellationUsingAllocator(
                result_alloc,
                artifact_id,
                expected_byte_len,
                expected_checksum,
                cancellation,
            );
            const empty = try result_alloc.alloc(u8, 0);
            errdefer result_alloc.free(empty);
            try cancellation.check();
            return empty;
        }

        const payload = if (self.vtable.get_verified_range_alloc_with_cancellation) |get_verified_range|
            try get_verified_range(
                self.ptr,
                result_alloc,
                artifact_id,
                expected_byte_len,
                expected_checksum,
                offset,
                len,
                cancellation,
            )
        else blk: {
            try self.verifyContentWithCancellationUsingAllocator(
                result_alloc,
                artifact_id,
                expected_byte_len,
                expected_checksum,
                cancellation,
            );
            break :blk try self.getRangeAllocWithCancellationUsingAllocator(
                result_alloc,
                artifact_id,
                offset,
                len,
                cancellation,
            );
        };
        errdefer result_alloc.free(payload);
        if (payload.len != len) return error.ArtifactIntegrityMismatch;
        try cancellation.check();
        return payload;
    }

    /// A shared read allowance covers both the requested range and any cold
    /// full-content authentication. Native backends charge only cache misses.
    pub fn getVerifiedRangeAllocWithBudget(
        self: *ArtifactStore,
        alloc: Allocator,
        artifact_id: []const u8,
        byte_len: u64,
        checksum: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
        remaining: *u64,
    ) ![]u8 {
        try cancellation.check();
        try validateSha256ArtifactIdentity(artifact_id, checksum);
        const end = std.math.add(u64, offset, len) catch return error.InvalidRange;
        if (end > byte_len) return error.InvalidRange;
        try chargeReadBudget(remaining, len);
        const bytes = if (self.vtable.get_verified_range_alloc_with_budget) |read|
            try read(self.ptr, alloc, artifact_id, byte_len, checksum, offset, len, cancellation, remaining)
        else blk: {
            try chargeReadBudget(remaining, byte_len);
            break :blk try self.getVerifiedRangeAllocWithCancellationUsingAllocator(alloc, artifact_id, byte_len, checksum, offset, len, cancellation);
        };
        errdefer alloc.free(bytes);
        if (bytes.len != len) return error.ArtifactIntegrityMismatch;
        try cancellation.check();
        return bytes;
    }

    pub fn stat(self: *ArtifactStore, artifact_id: []const u8) !ArtifactMetadata {
        return try self.statWithCancellation(artifact_id, .none);
    }

    pub fn statWithCancellation(self: *ArtifactStore, artifact_id: []const u8, cancellation: CancellationToken) !ArtifactMetadata {
        return try self.statWithCancellationUsingAllocator(self.allocator, artifact_id, cancellation);
    }

    pub fn statWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) !ArtifactMetadata {
        try cancellation.check();
        var metadata = if (self.vtable.stat_with_cancellation) |stat_with_cancellation|
            try stat_with_cancellation(self.ptr, result_alloc, artifact_id, cancellation)
        else
            try self.vtable.stat(self.ptr, result_alloc, artifact_id);
        errdefer metadata.deinit(result_alloc);
        try cancellation.check();
        return metadata;
    }

    pub fn delete(self: *ArtifactStore, artifact_id: []const u8) !void {
        try self.deleteWithCancellation(artifact_id, .none);
    }
    pub fn deleteWithCancellation(self: *ArtifactStore, artifact_id: []const u8, token: CancellationToken) !void {
        try token.check();
        if (self.write_context) |context| if (self.vtable.delete_for) |call| return call(self.ptr, artifact_id, token, context);
        if (self.vtable.delete_with_cancellation) |call| try call(self.ptr, artifact_id, token) else try self.vtable.delete(self.ptr, artifact_id);
        try token.check();
    }

    /// Verifies a content-addressed artifact with bounded memory. Backends may
    /// provide a native streaming verifier; the portable fallback hashes
    /// bounded ranges and therefore never allocates the full artifact.
    pub fn verifyContentWithCancellationUsingAllocator(
        self: *ArtifactStore,
        result_alloc: Allocator,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
    ) !void {
        try cancellation.check();
        validateSha256ArtifactIdentity(artifact_id, expected_checksum) catch return error.ArtifactIntegrityMismatch;

        if (self.write_context) |context| if (self.vtable.verify_content_for) |call| {
            const op: @import("../provider_operation.zig").Operation = .{ .context = context, .cancellation = cancellation };
            try op.ready();
            try call(self.ptr, result_alloc, artifact_id, expected_byte_len, expected_checksum, cancellation, context);
            try op.ready();
            return;
        };

        // A backend verifier owns the complete identity, length, and content
        // check. Calling stat here as well would duplicate provider HEADs on
        // every verification and defeat backend identity caches.
        if (self.vtable.verify_content) |verify_content| {
            try verify_content(self.ptr, result_alloc, artifact_id, expected_byte_len, expected_checksum, cancellation);
            return;
        }

        // Portable backends without a native verifier first validate the
        // declared metadata, then hash bounded ranges below.
        var metadata = try self.statWithCancellationUsingAllocator(result_alloc, artifact_id, cancellation);
        defer metadata.deinit(result_alloc);
        if (metadata.byte_len != expected_byte_len or
            !std.mem.eql(u8, metadata.artifact_id, artifact_id) or
            !std.mem.eql(u8, metadata.checksum, expected_checksum)) return error.ArtifactIntegrityMismatch;

        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        const chunk_bytes: usize = 8 * 1024 * 1024;
        var offset: u64 = 0;
        while (offset < expected_byte_len) {
            try cancellation.check();
            const remaining = expected_byte_len - offset;
            const len: usize = @intCast(@min(remaining, chunk_bytes));
            const chunk = try self.getRangeAllocWithCancellationUsingAllocator(result_alloc, artifact_id, offset, len, cancellation);
            defer result_alloc.free(chunk);
            if (chunk.len != len) return error.ArtifactIntegrityMismatch;
            hasher.update(chunk);
            offset += len;
        }
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        hasher.final(&digest);
        const actual = std.fmt.bytesToHex(digest, .lower);
        if (!std.mem.eql(u8, &actual, expected_checksum)) return error.ArtifactIntegrityMismatch;
        try cancellation.check();
    }
};

const ReadCancellation = struct {
    context: ReadContext,
    parent: CancellationToken,
    fn canceled(raw: *const anyopaque) bool {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        self.context.checkLifetime() catch return true;
        self.parent.check() catch return true;
        return false;
    }
    fn token(self: *const @This()) CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = canceled };
    }
};
fn discardPayload(bytes: []u8, alloc: Allocator) void {
    alloc.free(bytes);
}
fn discardMetadata(value: ArtifactMetadata, alloc: Allocator) void {
    var owned = value;
    owned.deinit(alloc);
}

pub fn validatePayloadSha256WithCancellation(
    payload: []const u8,
    expected_checksum: []const u8,
    cancellation: CancellationToken,
) !void {
    return validatePayloadSha256For(payload, expected_checksum, cancellation, .{});
}

pub fn validatePayloadSha256For(payload: []const u8, expected_checksum: []const u8, cancellation: CancellationToken, context: ReadContext) !void {
    try context.ensureActive();
    validateSha256Checksum(expected_checksum) catch return error.ArtifactIntegrityMismatch;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    const cancellation_chunk_bytes = 256 * 1024;
    var offset: usize = 0;
    while (offset < payload.len) {
        try context.ensureActive();
        try cancellation.check();
        const chunk_len = @min(cancellation_chunk_bytes, payload.len - offset);
        hasher.update(payload[offset..][0..chunk_len]);
        offset += chunk_len;
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const actual = std.fmt.bytesToHex(digest, .lower);
    if (!std.mem.eql(u8, &actual, expected_checksum)) return error.ArtifactIntegrityMismatch;
    try context.ensureActive();
    try cancellation.check();
}

test "artifact identities require canonical matching sha256 values" {
    const checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const artifact_id = sha256_artifact_id_prefix ++ checksum;
    try validateSha256ArtifactIdentity(artifact_id, checksum);
    try std.testing.expectEqualStrings(checksum, try sha256ChecksumFromArtifactId(artifact_id));

    try std.testing.expectError(error.InvalidArtifactId, sha256ChecksumFromArtifactId("sha256:abcd"));
    try std.testing.expectError(
        error.InvalidArtifactId,
        sha256ChecksumFromArtifactId("sha256:0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef"),
    );
    try std.testing.expectError(
        error.InvalidArtifactId,
        validateSha256ArtifactIdentity(
            artifact_id,
            "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ),
    );
}

test "serverless verified empty artifacts avoid invalid cloud ranges" {
    const alloc = std.testing.allocator;
    const empty_checksum = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const empty_artifact_id = sha256_artifact_id_prefix ++ empty_checksum;

    const State = struct {
        range_calls: usize = 0,

        pub fn deinit(_: Allocator, _: *anyopaque) void {}

        fn put(_: *anyopaque, _: Allocator, _: []const u8) anyerror!ArtifactMetadata {
            return error.UnexpectedCall;
        }

        pub fn getAlloc(_: *anyopaque, _: Allocator, _: []const u8) anyerror![]u8 {
            return error.UnexpectedCall;
        }

        fn getRangeAlloc(
            raw: *anyopaque,
            _: Allocator,
            _: []const u8,
            _: u64,
            _: usize,
        ) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.range_calls += 1;
            return error.UnexpectedRangeRequest;
        }

        fn stat(raw: *anyopaque, result_alloc: Allocator, artifact_id: []const u8) anyerror!ArtifactMetadata {
            _ = raw;
            if (!std.mem.eql(u8, artifact_id, empty_artifact_id)) return error.FileNotFound;
            const artifact_id_copy = try result_alloc.dupe(u8, empty_artifact_id);
            errdefer result_alloc.free(artifact_id_copy);
            return .{
                .artifact_id = artifact_id_copy,
                .byte_len = 0,
                .checksum = try result_alloc.dupe(u8, empty_checksum),
            };
        }

        fn delete(_: *anyopaque, _: []const u8) anyerror!void {
            return error.UnexpectedCall;
        }
    };

    const vtable = ArtifactStore.VTable{
        .deinit = State.deinit,
        .put = State.put,
        .get_alloc = State.getAlloc,
        .get_range_alloc = State.getRangeAlloc,
        .stat = State.stat,
        .delete = State.delete,
    };
    var state = State{};
    var store = ArtifactStore{
        .allocator = alloc,
        .ptr = &state,
        .vtable = &vtable,
    };

    const payload = try store.getVerifiedAllocWithCancellation(
        empty_artifact_id,
        0,
        empty_checksum,
        .none,
    );
    defer alloc.free(payload);
    try std.testing.expectEqual(@as(usize, 0), payload.len);
    try std.testing.expectEqual(@as(usize, 0), state.range_calls);

    const empty_range = try store.getRangeAllocWithCancellationUsingAllocator(
        alloc,
        empty_artifact_id,
        0,
        0,
        .none,
    );
    defer alloc.free(empty_range);
    try std.testing.expectEqual(@as(usize, 0), empty_range.len);
    try std.testing.expectEqual(@as(usize, 0), state.range_calls);
    try std.testing.expectError(
        error.InvalidArtifactId,
        store.getRangeAllocWithCancellationUsingAllocator(alloc, "not-an-artifact", 0, 0, .none),
    );
    try std.testing.expectError(
        error.InvalidRange,
        store.getRangeAllocWithCancellationUsingAllocator(alloc, empty_artifact_id, 1, 0, .none),
    );
    try std.testing.expectError(
        error.FileNotFound,
        store.getRangeAllocWithCancellationUsingAllocator(
            alloc,
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            0,
            0,
            .none,
        ),
    );
}

fn z17RepeatString(comptime bytes: []const u8, comptime repetitions: usize) *const [bytes.len * repetitions:0]u8 {
    const result = comptime blk: {
        @setEvalBranchQuota(@intCast(@min(std.math.maxInt(u32), 100000 +| (repetitions *| 16))));
        var repeated: [bytes.len * repetitions:0]u8 = undefined;
        for (0..repetitions) |i| @memcpy(repeated[i * bytes.len ..][0..bytes.len], bytes);
        repeated[bytes.len * repetitions] = 0;
        break :blk repeated;
    };
    return &result;
}

test "artifact reads suspend only provider waits and retire payloads on lost authority" {
    const local = @import("antfly_local_sources");
    const Runnable = local.common_request_admission.runnable_scheduler;
    const Case = enum { success, canceled, expired, closed, provider_error, corrupt, source_canceled, caller_canceled, unscheduled };
    const Clock = struct {
        var value: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return value.load(.acquire);
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Fixture = struct {
        scheduler: *Runnable.Scheduler,
        execution: *Runnable.Execution,
        signal: *std.atomic.Value(bool),
        caller: *std.atomic.Value(bool),
        case: Case,
        bytes: []const u8,
        checksum: []const u8,
        qualified: bool,
        calls: usize = 0,
        controls: usize = 0,
        checkpoints: usize = 0,
        fn from(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn check(raw: *anyopaque) !void {
            const self = from(raw);
            try self.execution.checkpoint();
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().runnable_slots);
            self.checkpoints += 1;
        }
        fn provider(self: *@This()) !void {
            self.calls += 1;
            if (self.case == .unscheduled or !self.qualified) {
                try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().runnable_slots);
            } else {
                try std.testing.expectEqual(@as(usize, 0), self.scheduler.stats().runnable_slots);
                try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().resource_blocked);
                var control: Runnable.Execution = .{};
                try control.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
                try control.checkpoint();
                control.finish();
                self.controls += 1;
            }
        }
        fn boundedFor(raw: *anyopaque, a: Allocator, id: []const u8, len: usize, token: CancellationToken, context: ReadContext) ![]u8 {
            return @import("antfly_local_sources").sql_execution_wait.runOwned(context.wait, bounded, .{ raw, a, id, len, token }, discardPayload, .{a});
        }
        fn bounded(raw: *anyopaque, a: Allocator, _: []const u8, len: usize, token: CancellationToken) ![]u8 {
            const self = from(raw);
            try self.provider();
            try token.check();
            if (self.case == .provider_error) return error.InputOutput;
            try std.testing.expectEqual(self.bytes.len, len);
            const bytes = try a.dupe(u8, self.bytes);
            switch (self.case) {
                .canceled => self.signal.store(true, .release),
                .caller_canceled => self.caller.store(true, .release),
                .expired => Clock.value.store(10, .release),
                .closed => self.scheduler.close(),
                .corrupt => bytes[0] ^= 1,
                else => {},
            }
            return bytes;
        }
        fn stat(raw: *anyopaque, a: Allocator, id: []const u8) !ArtifactMetadata {
            const self = from(raw);
            try self.provider();
            const owned = try a.dupe(u8, id);
            errdefer a.free(owned);
            return .{ .artifact_id = owned, .byte_len = self.bytes.len, .checksum = try a.dupe(u8, self.checksum) };
        }
        fn range(raw: *anyopaque, a: Allocator, id: []const u8, offset: u64, len: usize, token: CancellationToken) ![]u8 {
            try std.testing.expectEqual(@as(u64, 0), offset);
            return bounded(raw, a, id, len, token);
        }
        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn put(_: *anyopaque, _: Allocator, _: []const u8) !ArtifactMetadata {
            return error.UnexpectedCall;
        }
        fn get(_: *anyopaque, _: Allocator, _: []const u8) ![]u8 {
            return error.UnexpectedCall;
        }
        fn rawRange(_: *anyopaque, _: Allocator, _: []const u8, _: u64, _: usize) ![]u8 {
            return error.UnexpectedCall;
        }
        fn delete(_: *anyopaque, _: []const u8) !void {
            return error.UnexpectedCall;
        }
    };
    const a = std.testing.allocator;
    const payload: [300 * 1024]u8 = @splat(7);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&payload, &digest, .{});
    const checksum = std.fmt.bytesToHex(digest, .lower);
    const id = try std.fmt.allocPrint(a, "sha256:{s}", .{checksum});
    defer a.free(id);
    for ([_]bool{ true, false }) |bounded| for (std.enums.values(Case)) |case| {
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 2, .max_bytes = 64 * 1024 * 1024 });
        var signal: std.atomic.Value(bool) = .init(false);
        var caller: std.atomic.Value(bool) = .init(false);
        Clock.value.store(0, .release);
        var execution: Runnable.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 10, .cancellation = .{ .ptr = &signal, .requested = Clock.requested } }, .query);
        defer execution.finish();
        var fixture: Fixture = .{ .scheduler = &scheduler, .execution = &execution, .signal = &signal, .caller = &caller, .case = case, .qualified = bounded, .bytes = &payload, .checksum = &checksum };
        const vtable: ArtifactStore.VTable = .{ .deinit = Fixture.deinit, .put = Fixture.put, .get_alloc = Fixture.get, .get_range_alloc = Fixture.rawRange, .get_range_alloc_with_cancellation = Fixture.range, .stat = Fixture.stat, .delete = Fixture.delete, .get_bounded_alloc_for = if (bounded) Fixture.boundedFor else null, .get_bounded_alloc_with_cancellation = if (bounded) Fixture.bounded else null };
        var store: ArtifactStore = .{ .allocator = a, .ptr = &fixture, .vtable = &vtable };
        var context: ReadContext = .{ .io = std.testing.io, .cancellation = .fromAtomic(&signal) };
        if (case != .unscheduled) {
            context = context.withExecution(&execution);
            context.checkpoint = .{ .ptr = &fixture, .check = Fixture.check };
        }
        if (case == .source_canceled) signal.store(true, .release);
        const result = store.getVerifiedAllocFor(a, id, payload.len, &checksum, .{ .ptr = &caller, .is_cancelled_fn = Clock.requested }, context);
        switch (case) {
            .success, .unscheduled => {
                const bytes = try result;
                defer a.free(bytes);
                try std.testing.expectEqualSlices(u8, &payload, bytes);
                if (case == .success) try std.testing.expect(fixture.checkpoints >= 4);
            },
            .canceled, .source_canceled, .caller_canceled => try std.testing.expectError(error.Canceled, result),
            .expired => try std.testing.expectError(error.DeadlineExceeded, result),
            .closed => try std.testing.expectError(error.SchedulerClosed, result),
            .provider_error => try std.testing.expectError(error.InputOutput, result),
            .corrupt => try std.testing.expectError(error.ArtifactIntegrityMismatch, result),
        }
        const expected_calls: usize = if (case == .source_canceled) 0 else if (bounded) 1 else 2;
        try std.testing.expectEqual(expected_calls, fixture.calls);
        if (case != .unscheduled and bounded) try std.testing.expectEqual(fixture.calls, fixture.controls) else try std.testing.expectEqual(@as(usize, 0), fixture.controls);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    };
}
