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

//! Route-independent, checksum-addressed storage for frozen source plans.
const std = @import("std");
const artifacts = @import("../serverless/artifacts/store.zig");
const native = @import("antfly_local_sources").storage_source_plan_storage;
pub const Store = artifacts.ArtifactStore;
const Context = @import("antfly_local_sources").api_operation.RequestContext;
const CancellationToken = @import("antfly_cancellation").CancellationToken;

pub fn supportsRetention(store: Store) bool {
    return store.shared_across_owners and store.namespace_identity != null and
        store.vtable.put_scoped != null and store.vtable.retire_scoped_uploads != null and
        store.vtable.visit_scoped_uploads != null;
}

pub const Reference = struct {
    namespace: [32]u8,
    upload: artifacts.UploadScope,
    checksum: [32]u8,
    bytes: u64,

    pub fn validate(self: Reference) !void {
        try self.upload.validate();
        if (std.mem.allEqual(u8, &self.namespace, 0) or std.mem.allEqual(u8, &self.checksum, 0) or
            self.bytes < native.frozen_header_bytes or self.bytes > native.max_bytes)
            return error.InvalidSourceAdmission;
    }

    fn requireStore(self: Reference, store: *const artifacts.ArtifactStore) !void {
        try self.validate();
        if (!supportsRetention(store.*) or !std.meta.eql(store.namespace_identity, @as(?[32]u8, self.namespace)))
            return error.SourcePayloadStorageUnavailable;
    }

    /// Persist a uniquely scoped object so cleanup of a settled attempt can
    /// never delete an identical payload retained for a different attempt.
    pub fn put(self: Reference, store: *artifacts.ArtifactStore, payload: []const u8, token: CancellationToken) !void {
        try self.requireStore(store);
        if (payload.len != self.bytes) return error.SourcePayloadConflict;
        const checksum = std.fmt.bytesToHex(self.checksum, .lower);
        try artifacts.validatePayloadSha256WithCancellation(payload, &checksum, token);
        const id = try self.upload.artifactId(&checksum);
        var saved = try store.putScoped(self.upload, payload, token);
        defer saved.deinit(store.allocator);
        if (saved.byte_len != self.bytes or !std.mem.eql(u8, saved.checksum, &checksum) or
            !std.mem.eql(u8, saved.artifact_id, &id)) return error.SourcePayloadConflict;
    }

    /// Once the attempt is settled, prevent a stale uploader from restoring
    /// the object before removing its content-addressed key.
    pub fn reclaim(self: Reference, store: *Store, token: CancellationToken) !void {
        try self.requireStore(store);
        try store.retireScopedUploads(self.upload.domain, self.upload.fencingToken(), token);
        const checksum = std.fmt.bytesToHex(self.checksum, .lower);
        const id = try self.upload.artifactId(&checksum);
        store.deleteWithCancellation(&id, token) catch |err| switch (err) {
            error.FileNotFound, error.ObjectNotFound => {},
            else => return err,
        };
        try token.check();
    }

    /// Collect the fenced prefix, including uploads that failed before their
    /// reservation CAS. A newer token is outside this collector's authority.
    pub fn sweep(self: Reference, store: *Store, previous: []const u8, token: CancellationToken) !Sweep {
        try self.reclaim(store, token);
        const through = self.upload.fencingToken();
        var ctx: SweepContext = .{ .store = store, .token = token, .domain = self.upload.domain, .through = through };
        var decoded: [max_cursor_bytes]u8 = undefined;
        const decoder = std.base64.url_safe_no_pad.Decoder;
        const length = try decoder.calcSizeForSlice(previous);
        if (length > decoded.len) return error.InvalidSourceControl;
        try decoder.decode(decoded[0..length], previous);
        store.visitScopedUploads(self.upload.domain, .{
            .ptr = &ctx,
            .visit = SweepContext.visit,
            .checkpoint = SweepContext.checkpoint,
            .retain_scope = SweepContext.retain,
            .continuation = if (length == 0) null else decoded[0..length],
            .fencing_cutoff = std.math.add(u64, through, 1) catch return error.SourceReceiptExpired,
            .max_entries = 32,
            .cleanup_staging = true,
            .reclaim_retired_inventory = true,
        }, token) catch |err| switch (err) {
            error.ArtifactEnumerationPaused => return ctx.progress,
            error.InvalidArtifactEnumerationContinuation => return .{},
            else => return err,
        };
        return .{ .done = !ctx.retained };
    }

    pub fn load(self: Reference, store: *artifacts.ArtifactStore, a: std.mem.Allocator, token: CancellationToken) ![]u8 {
        try self.requireStore(store);
        const checksum = std.fmt.bytesToHex(self.checksum, .lower);
        const id = try self.upload.artifactId(&checksum);
        return store.getVerifiedAllocWithCancellationUsingAllocator(a, &id, self.bytes, &checksum, token);
    }
};

const max_cursor_bytes = @import("../metadata/source_control.zig").max_payload_cleanup_cursor_bytes;
pub const Sweep = struct {
    done: bool = false,
    cursor: [max_cursor_bytes]u8 = undefined,
    cursor_len: usize = 0,
};
const SweepContext = struct {
    store: *Store,
    token: CancellationToken,
    domain: [32]u8,
    through: u64,
    progress: Sweep = .{},
    retained: bool = false,

    fn checkScope(self: *SweepContext, scope: artifacts.UploadScope) !void {
        if (!std.meta.eql(scope.domain, self.domain) or scope.fencingToken() > self.through)
            return error.InvalidArtifactUploadScope;
    }
    fn visit(raw: *anyopaque, scope: artifacts.UploadScope, id: []const u8) !void {
        const self: *SweepContext = @ptrCast(@alignCast(raw));
        try self.checkScope(scope);
        const actual = (try artifacts.uploadScopeFromArtifactId(id)) orelse return error.InvalidArtifactUploadScope;
        if (!std.meta.eql(scope, actual)) return error.InvalidArtifactUploadScope;
        self.store.deleteWithCancellation(id, self.token) catch |err| switch (err) {
            error.FileNotFound, error.ObjectNotFound => {},
            else => return err,
        };
    }
    fn checkpoint(raw: *anyopaque, cursor: []const u8) !void {
        const self: *SweepContext = @ptrCast(@alignCast(raw));
        const encoder = std.base64.url_safe_no_pad.Encoder;
        if (encoder.calcSize(cursor.len) > self.progress.cursor.len) return error.InvalidSourceControl;
        self.progress.cursor_len = encoder.encode(&self.progress.cursor, cursor).len;
    }
    fn retain(raw: *anyopaque, scope: artifacts.UploadScope) !void {
        const self: *SweepContext = @ptrCast(@alignCast(raw));
        try self.checkScope(scope);
        self.retained = true;
    }
};

pub const Cancellation = struct {
    context: Context,

    fn check(raw: *const anyopaque) !void {
        const self: *const Cancellation = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
    }

    pub fn token(self: *const Cancellation) CancellationToken {
        return .{ .ptr = self, .check_fn = check };
    }
};

test "completion frozen plan shared payload survives owner reopen and fences late uploads" {
    const a = std.testing.allocator;
    const FsStore = @import("../serverless/artifacts/fs_store.zig").FsStore;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source-payload", .{tmp.sub_path});
    defer a.free(path);
    var original = try FsStore.init(a, path);
    defer original.deinit();
    const initialized = try original.spoolArtifactStore(.none);
    var store = try original.sharedSpoolArtifactStore(initialized.namespace_identity.?, .none);
    const bytes: [native.frozen_header_bytes]u8 = @splat(19);
    var checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&bytes, &checksum, .{});
    const domain: [32]u8 = @splat(7);
    const reference: Reference = .{
        .namespace = store.namespace_identity.?,
        .upload = try artifacts.UploadScope.forPublication(domain, 1, std.testing.io),
        .checksum = checksum,
        .bytes = bytes.len,
    };
    try std.testing.expectError(error.SourcePayloadConflict, reference.put(&store, bytes[1..], .none));
    try reference.put(&store, &bytes, .none);

    var replacement = try FsStore.init(a, path);
    defer replacement.deinit();
    var reopened = try replacement.sharedSpoolArtifactStore(reference.namespace, .none);
    const retained = try reference.load(&reopened, a, .none);
    defer a.free(retained);
    try std.testing.expectEqualSlices(u8, &bytes, retained);
    var wrong_store = reopened;
    wrong_store.namespace_identity = @splat(4);
    try std.testing.expectError(error.SourcePayloadStorageUnavailable, reference.load(&wrong_store, a, .none));

    var newer = reference;
    newer.upload = try artifacts.UploadScope.forPublication(domain, 2, std.testing.io);
    try newer.put(&reopened, &bytes, .none);
    var orphan = reference;
    orphan.upload = try artifacts.UploadScope.forPublication(domain, 1, std.testing.io);
    try orphan.put(&reopened, &bytes, .none);
    for (0..35) |_| {
        var abandoned = reference;
        abandoned.upload = try artifacts.UploadScope.forPublication(domain, 1, std.testing.io);
        try abandoned.put(&reopened, &bytes, .none);
    }
    var progress = try reference.sweep(&reopened, "", .none);
    try std.testing.expect(!progress.done);
    try std.testing.expect(progress.cursor_len > 0);
    for (0..8) |_| {
        progress = try reference.sweep(&reopened, progress.cursor[0..progress.cursor_len], .none);
        if (progress.done) break;
    }
    try std.testing.expect(progress.done);
    try std.testing.expect((try reference.sweep(&reopened, "", .none)).done);
    try std.testing.expectError(error.FileNotFound, orphan.load(&store, a, .none));
    try std.testing.expectError(error.ArtifactUploadAttemptRetired, reference.put(&store, &bytes, .none));
    try std.testing.expectError(error.FileNotFound, reference.load(&store, a, .none));
    const remaining = try newer.load(&store, a, .none);
    defer a.free(remaining);
    try std.testing.expectEqualSlices(u8, &bytes, remaining);
}
