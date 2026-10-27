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
const fs_paths = @import("antfly_runtime_fs").fs_paths;

const Sha256 = std.crypto.hash.sha2.Sha256;
const envelope_magic = "AFRSPAY\x00";
const envelope_version: u32 = 1;
const envelope_header_len: usize = 72;
const manifest_envelope_version: u32 = 2;
const manifest_header_len: usize = 112;
pub const max_application_manifest_bytes = @import("raft_engine").runtime.storage_iface.SnapshotArtifact.max_application_manifest_bytes;
const digest_domain = "antfly-raft-snapshot-payload-v1\x00";
const copy_buffer_len = 64 * 1024;
pub const max_payload_bytes: u64 = 1 << 30;

var publish_nonce = std.atomic.Value(u64).init(1);

pub const SnapshotIdentity = struct { index: u64 = 0, term: u64 = 0 };

/// Produced only by successful streamed publication or full cold validation.
/// Keeping this small value with the durable replica cursor avoids hashing a
/// gigabyte-sized image again while holding publication serialization to pin it.
pub const PayloadReceipt = struct {
    identity: SnapshotIdentity,
    payload_len: u64,
    digest: [32]u8,
    manifest_len: u64,
    manifest_digest: [32]u8,
};

/// Expected immutable body from an authenticated durable capture record.
/// This is not a caller-supplied claim that a body has passed verification.
pub const CapturedPayload = struct { payload_len: u64, digest: [32]u8 };

/// A local retention reference, not a quorum certificate or restore grant.
/// The caller owns the lifecycle commitment and must retire it only after the
/// corresponding metadata obligation is durably discharged. No path is supplied
/// by the caller. References cannot be rebound to different immutable bytes.
pub const RetainedSnapshot = struct {
    reference: [32]u8,
    identity: SnapshotIdentity,
    payload_len: u64,
    digest: [32]u8,
    application_manifest: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.application_manifest);
        self.* = undefined;
    }
};

/// Prepare the destination outside replica publication serialization. pin()
/// performs only bounded envelope checks, an inode link/open, and no fsync or
/// payload/manifest scan. finish() owns the slow durability work after unlock.
/// The caller must persist its retention intent before pinning; abandoning this
/// object never unlinks a possible outstanding lifecycle obligation.
pub const RetentionTarget = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    directory: []u8,
    path: []u8,
    reference: [32]u8,
    file: ?std.Io.File = null,
    expected: ?PayloadReceipt = null,
    copy_required: bool = false,

    pub const Mode = enum { automatic, copy };

    pub fn init(alloc: std.mem.Allocator, io: std.Io, destination: []const u8, reference: [32]u8) !@This() {
        if (std.mem.allEqual(u8, &reference, 0)) return error.InvalidSnapshotRetention;
        const directory = try retainedDirAlloc(alloc, destination);
        errdefer alloc.free(directory);
        const path = try retainedPathAlloc(alloc, destination, reference);
        errdefer alloc.free(path);
        try fs_paths.createDirPathPortable(io, directory);
        try fs_paths.syncDirPortable(io, destination);
        return .{ .alloc = alloc, .io = io, .directory = directory, .path = path, .reference = reference };
    }

    pub fn deinit(self: *@This()) void {
        if (self.file) |file| file.close(self.io);
        self.alloc.free(self.path);
        self.alloc.free(self.directory);
        self.* = undefined;
    }

    /// The provider must qualify this exact durable checkpoint and serialize
    /// against source supersession until this method has pinned an open inode.
    /// The Io runtime belongs to the target, not the potentially retiring group.
    pub fn pin(self: *@This(), source: []const u8, receipt: PayloadReceipt, mode: Mode) !void {
        if (self.file != null or receipt.identity.index == 0 or receipt.identity.term == 0) return error.InvalidSnapshotRetention;
        var buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
        const path = try std.fmt.bufPrint(&buffer, "{s}/state-{d}-{d}.snap", .{ source, receipt.identity.index, receipt.identity.term });
        var file = try std.Io.Dir.cwd().openFile(self.io, path, .{});
        errdefer file.close(self.io);
        const envelope = try readEnvelope(file, self.io, receipt.identity.index, receipt.identity.term);
        if (!std.meta.eql(receipt, envelope.receipt())) return error.SnapshotPayloadIdentityConflict;
        var copy = mode == .copy;
        if (!copy) std.Io.Dir.hardLink(.cwd(), path, .cwd(), self.path, self.io, .{}) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            // Multiple local disks and filesystems without hard links use one
            // streaming verified copy after releasing publication ownership.
            error.CrossDevice, error.OperationUnsupported, error.LinkQuotaExceeded => copy = true,
            else => return err,
        };
        if (!copy) {
            const retained = try std.Io.Dir.cwd().openFile(self.io, self.path, .{});
            errdefer retained.close(self.io);
            const existing = try readEnvelope(retained, self.io, receipt.identity.index, receipt.identity.term);
            if (!std.meta.eql(receipt, existing.receipt())) return error.SnapshotPayloadIdentityConflict;
            file.close(self.io);
            file = retained;
        }
        self.file = file;
        self.expected = receipt;
        self.copy_required = copy;
    }

    /// No replica-state pointer or publication lock is needed from here on.
    /// Cross-filesystem copies hash each payload byte once with a fixed buffer,
    /// fsync before atomic no-replace publication, and never rebind a reference.
    pub fn finish(self: *@This(), alloc: std.mem.Allocator) !RetainedSnapshot {
        return self.finishWithContext(alloc, UnboundedContext{});
    }

    pub fn finishWithContext(self: *@This(), alloc: std.mem.Allocator, context: anytype) !RetainedSnapshot {
        try context.ensureActive();
        const expected = self.expected orelse return error.SnapshotNotPublished;
        if (self.copy_required) try self.publishCopy(expected, context);
        const file = self.file orelse return error.SnapshotNotPublished;
        const envelope = try readEnvelope(file, self.io, expected.identity.index, expected.identity.term);
        if (!std.meta.eql(expected, envelope.receipt())) return error.SnapshotPayloadIdentityConflict;
        const manifest = try readOpenManifest(alloc, file, self.io, envelope);
        errdefer alloc.free(manifest);
        // Retry this even when the link already existed: the previous attempt
        // may have completed linking but lost the directory-sync response.
        try fs_paths.syncDirPortable(self.io, self.directory);
        try context.ensureActive();
        return .{ .reference = self.reference, .identity = expected.identity, .payload_len = envelope.payload_len, .digest = envelope.digest, .application_manifest = manifest };
    }

    fn publishCopy(self: *@This(), expected: PayloadReceipt, context: anytype) !void {
        const source = self.file orelse return error.SnapshotNotPublished;
        const envelope = try readEnvelope(source, self.io, expected.identity.index, expected.identity.term);
        if (!std.meta.eql(expected, envelope.receipt())) return error.SnapshotPayloadIdentityConflict;
        const base = std.fs.path.dirname(self.directory) orelse return error.InvalidSnapshotRetention;
        // The standard snapshot orphan collector recognizes this temporary
        // name. It is never a published retention reference or source receipt.
        const temporary = try std.fmt.allocPrint(self.alloc, "{s}/state-{d}-{d}.snap.tmp-{d}", .{ base, expected.identity.index, expected.identity.term, publish_nonce.fetchAdd(1, .monotonic) });
        defer self.alloc.free(temporary);
        var copied = try std.Io.Dir.cwd().createFile(self.io, temporary, .{ .exclusive = true });
        var copied_open = true;
        defer std.Io.Dir.cwd().deleteFile(self.io, temporary) catch {};
        defer if (copied_open) copied.close(self.io);
        const length = envelope.headerLen() + envelope.manifest_len + envelope.payload_len;
        var digest = Sha256.init(.{});
        updateDigestIdentity(&digest, envelope.index, envelope.term, envelope.payload_len);
        updateManifestIdentity(&digest, envelope.manifest_len);
        var buffer: [copy_buffer_len]u8 = undefined;
        var offset: u64 = 0;
        while (offset < length) {
            try context.ensureActive();
            const wanted: usize = @intCast(@min(buffer.len, length - offset));
            const count = try source.readPositionalAll(self.io, buffer[0..wanted], offset);
            if (count != wanted) return error.SnapshotPayloadTruncated;
            const skip: usize = @intCast(@min(count, envelope.headerLen() -| offset));
            digest.update(buffer[skip..count]);
            try copied.writePositionalAll(self.io, buffer[0..count], offset);
            offset += count;
        }
        var actual: [32]u8 = undefined;
        digest.final(&actual);
        if (!std.crypto.timing_safe.eql([32]u8, actual, expected.digest)) return error.SnapshotPayloadChecksumMismatch;
        try context.ensureActive();
        try copied.sync(self.io);
        // Close before rename/unlink as required by filesystems that cannot
        // rename an open writer. All failure exits close before cleanup too.
        copied.close(self.io);
        copied_open = false;
        std.Io.Dir.renamePreserve(.cwd(), temporary, .cwd(), self.path, self.io) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
        const installed = try std.Io.Dir.cwd().openFile(self.io, self.path, .{});
        errdefer installed.close(self.io);
        const selected = try readEnvelope(installed, self.io, expected.identity.index, expected.identity.term);
        if (!std.meta.eql(expected, selected.receipt())) return error.SnapshotPayloadIdentityConflict;
        source.close(self.io);
        self.file = installed;
        self.copy_required = false;
    }
};

/// Synchronous entry for providers already on a private control
/// lane. Shared Raft callers use RetentionTarget to finish outside their mutex.
pub fn retainPublished(alloc: std.mem.Allocator, io: std.Io, snapshot_dir: []const u8, receipt: PayloadReceipt, reference: [32]u8) !RetainedSnapshot {
    var target = try RetentionTarget.init(alloc, io, snapshot_dir, reference);
    defer target.deinit();
    try target.pin(snapshot_dir, receipt, .automatic);
    return target.finish(alloc);
}

/// Recovery of an already-retained reference does not require the latest Raft
/// snapshot to still name it. Full payload validation remains streaming/bounded.
pub fn readRetained(alloc: std.mem.Allocator, io: std.Io, snapshot_dir: []const u8, reference: [32]u8, identity: SnapshotIdentity) !RetainedSnapshot {
    var reader = try openRetained(alloc, io, snapshot_dir, reference, identity);
    defer reader.deinit();
    return reader.read(alloc);
}

/// Owns the opened immutable inode, not a replica-state pointer or borrowed
/// snapshot path. Open under the owner's serialization, then validate outside
/// it on an independently leased IO runtime. Latest-snapshot GC and reference
/// retirement cannot redirect this reader to replacement bytes.
pub const RetainedReader = struct {
    file: std.Io.File,
    io: std.Io,
    reference: [32]u8,
    identity: SnapshotIdentity,
    lifetime: ?struct { ptr: *anyopaque, release: *const fn (*anyopaque) void } = null,

    pub fn deinit(self: *@This()) void {
        self.file.close(self.io);
        if (self.lifetime) |lifetime| lifetime.release(lifetime.ptr);
        self.* = undefined;
    }

    pub fn read(self: *@This(), alloc: std.mem.Allocator) !RetainedSnapshot {
        return self.readWithContext(alloc, UnboundedContext{});
    }

    /// Recover the exact image already authenticated by the producer's durable
    /// capture. Check its envelope, length and independently checksummed bounded
    /// manifest without rereading a potentially gigabyte-sized body each turn.
    /// The receiving stream still verifies the complete body before publication.
    pub fn readCaptured(self: *@This(), alloc: std.mem.Allocator, expected: CapturedPayload, context: anytype) !RetainedSnapshot {
        try context.ensureActive();
        const envelope = try readEnvelope(self.file, self.io, self.identity.index, self.identity.term);
        if (envelope.payload_len != expected.payload_len or !std.crypto.timing_safe.eql([32]u8, envelope.digest, expected.digest))
            return error.SnapshotPayloadIdentityConflict;
        const manifest = try readOpenManifest(alloc, self.file, self.io, envelope);
        errdefer alloc.free(manifest);
        try context.ensureActive();
        return .{ .reference = self.reference, .identity = self.identity, .payload_len = envelope.payload_len, .digest = envelope.digest, .application_manifest = manifest };
    }

    pub fn readWithContext(self: *@This(), alloc: std.mem.Allocator, context: anytype) !RetainedSnapshot {
        try context.ensureActive();
        const envelope = try readEnvelope(self.file, self.io, self.identity.index, self.identity.term);
        try validateOpenFileWithContext(self.file, self.io, envelope, context);
        const manifest = try readOpenManifest(alloc, self.file, self.io, envelope);
        errdefer alloc.free(manifest);
        try context.ensureActive();
        return .{ .reference = self.reference, .identity = self.identity, .payload_len = envelope.payload_len, .digest = envelope.digest, .application_manifest = manifest };
    }
};

pub fn openRetained(alloc: std.mem.Allocator, io: std.Io, snapshot_dir: []const u8, reference: [32]u8, identity: SnapshotIdentity) !RetainedReader {
    if (identity.index == 0 or identity.term == 0 or std.mem.allEqual(u8, &reference, 0)) return error.InvalidSnapshotRetention;
    const path = try retainedPathAlloc(alloc, snapshot_dir, reference);
    defer alloc.free(path);
    return .{ .file = try std.Io.Dir.cwd().openFile(io, path, .{}), .io = io, .reference = reference, .identity = identity };
}

/// Requires the caller's independently authorized retirement decision. This is
/// deliberately not performed by latest-snapshot GC. Missing links are exact
/// idempotent retries; a conflicting body must never release another reference.
pub fn releaseRetained(alloc: std.mem.Allocator, io: std.Io, snapshot_dir: []const u8, retained: RetainedSnapshot) !void {
    return releaseReference(alloc, io, snapshot_dir, retained.reference, retained.identity, .{ .payload_len = retained.payload_len, .digest = retained.digest });
}

/// Trusted retirement lane only. A reserved, never-submitted producer can
/// discharge its exact reference without a capture receipt. Accepted captures
/// must supply their authenticated saved length/digest. This is not permission
/// to retire either kind: the caller owns the durable lifecycle decision.
pub fn releaseReference(alloc: std.mem.Allocator, io: std.Io, snapshot_dir: []const u8, reference: [32]u8, identity: SnapshotIdentity, expected: ?CapturedPayload) !void {
    if (identity.index == 0 or identity.term == 0 or std.mem.allEqual(u8, &reference, 0)) return error.InvalidSnapshotRetention;
    const path = try retainedPathAlloc(alloc, snapshot_dir, reference);
    defer alloc.free(path);
    const retained_dir = try retainedDirAlloc(alloc, snapshot_dir);
    defer alloc.free(retained_dir);
    if (std.Io.Dir.cwd().openFile(io, path, .{})) |file| {
        defer file.close(io);
        const envelope = try readEnvelope(file, io, identity.index, identity.term);
        if (expected) |capture| if (envelope.payload_len != capture.payload_len or !std.crypto.timing_safe.eql([32]u8, envelope.digest, capture.digest))
            return error.SnapshotPayloadIdentityConflict;
        try std.Io.Dir.cwd().deleteFile(io, path);
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
    // Re-sync on missing-link retries too: the prior unlink may have succeeded
    // while its directory synchronization failed.
    try fs_paths.syncDirPortable(io, retained_dir);
}

fn retainedDirAlloc(alloc: std.mem.Allocator, snapshot_dir: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}/retained", .{snapshot_dir});
}

fn retainedPathAlloc(alloc: std.mem.Allocator, snapshot_dir: []const u8, reference: [32]u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}/retained/{s}.snap", .{ snapshot_dir, std.fmt.bytesToHex(reference, .lower) });
}

const SliceSource = struct {
    bytes: []const u8,

    fn len(self: @This()) u64 {
        return self.bytes.len;
    }

    fn writeTo(self: @This(), writer: *std.Io.Writer) !void {
        try writer.writeAll(self.bytes);
    }
};

const Envelope = struct {
    index: u64,
    term: u64,
    payload_len: u64,
    digest: [Sha256.digest_length]u8,
    manifest_len: u64 = 0,
    manifest_digest: [Sha256.digest_length]u8 = @splat(0),

    fn headerLen(self: @This()) usize {
        return if (self.manifest_len == 0) envelope_header_len else manifest_header_len;
    }
    fn payloadOffset(self: @This()) u64 {
        return self.headerLen() + self.manifest_len;
    }
    fn receipt(self: @This()) PayloadReceipt {
        return .{ .identity = .{ .index = self.index, .term = self.term }, .payload_len = self.payload_len, .digest = self.digest, .manifest_len = self.manifest_len, .manifest_digest = self.manifest_digest };
    }
};

pub fn writeAtomically(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
    data: []const u8,
) !void {
    _ = try writeWithReceipt(alloc, io, snapshot_dir, index, term, data);
}

pub fn writeWithReceipt(alloc: std.mem.Allocator, io: std.Io, snapshot_dir: []const u8, index: u64, term: u64, data: []const u8) !PayloadReceipt {
    return writeSourceAtomically(alloc, io, snapshot_dir, index, term, SliceSource{ .bytes = data });
}

pub fn writeArtifactAtomically(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
    artifact: anytype,
) !void {
    _ = try writeArtifactWithReceipt(alloc, io, snapshot_dir, index, term, artifact);
}

pub fn writeArtifactWithReceipt(alloc: std.mem.Allocator, io: std.Io, snapshot_dir: []const u8, index: u64, term: u64, artifact: anytype) !PayloadReceipt {
    return writeSourceAtomically(alloc, io, snapshot_dir, index, term, artifact);
}

fn writeSourceAtomically(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
    source: anytype,
) !PayloadReceipt {
    try fs_paths.createDirPathPortable(io, snapshot_dir);
    const final_path = try pathAlloc(alloc, snapshot_dir, index, term);
    defer alloc.free(final_path);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{d}", .{
        final_path,
        publish_nonce.fetchAdd(1, .monotonic),
    });
    defer alloc.free(tmp_path);
    errdefer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    const payload_len = source.len();
    if (payload_len > max_payload_bytes) return error.SnapshotPayloadTooLarge;
    const Source = switch (@typeInfo(@TypeOf(source))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(source),
    };
    const manifest = if (comptime @hasDecl(Source, "applicationManifest")) source.applicationManifest() else "";
    if (manifest.len > max_application_manifest_bytes) return error.SnapshotPayloadTooLarge;
    const header_len: usize = if (manifest.len == 0) envelope_header_len else manifest_header_len;
    var published_envelope: Envelope = undefined;
    {
        var file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
        defer file.close(io);

        var file_buffer: [copy_buffer_len]u8 = undefined;
        var file_writer = file.writer(io, &file_buffer);
        try file_writer.interface.splatByteAll(0, header_len);

        var hash_buffer: [copy_buffer_len]u8 = undefined;
        var hashed = file_writer.interface.hashed(Sha256.init(.{}), &hash_buffer);
        updateDigestIdentity(&hashed.hasher, index, term, payload_len);
        updateManifestIdentity(&hashed.hasher, manifest.len);
        try hashed.writer.writeAll(manifest);
        try source.writeTo(&hashed.writer);
        try hashed.writer.flush();
        try file_writer.end();

        const expected_file_len = std.math.add(u64, header_len + manifest.len, payload_len) catch
            return error.SnapshotPayloadTooLarge;
        if (try file.length(io) != expected_file_len) return error.SnapshotArtifactSizeMismatch;

        var digest: [Sha256.digest_length]u8 = undefined;
        hashed.hasher.final(&digest);
        published_envelope = .{
            .index = index,
            .term = term,
            .payload_len = payload_len,
            .digest = digest,
            .manifest_len = manifest.len,
        };
        if (manifest.len != 0) Sha256.hash(manifest, &published_envelope.manifest_digest, .{});
        const header = encodeEnvelope(published_envelope);
        try file.writePositionalAll(io, header[0..header_len], 0);
        try file.sync(io);
    }

    if (try existingPayloadMatches(io, final_path, published_envelope)) {
        try std.Io.Dir.cwd().deleteFile(io, tmp_path);
        // The prior rename may have succeeded while its directory sync failed.
        // An exact retry must finish that barrier before returning a receipt.
        try fs_paths.syncDirPortable(io, snapshot_dir);
        return published_envelope.receipt();
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), final_path, io);
    try fs_paths.syncDirPortable(io, snapshot_dir);
    return published_envelope.receipt();
}

fn existingPayloadMatches(io: std.Io, path: []const u8, expected: Envelope) !bool {
    var file = std.Io.Dir.cwd().openFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer file.close(io);
    const existing = try readEnvelope(file, io, expected.index, expected.term);
    try validateOpenFile(file, io, existing);
    if (existing.payload_len != expected.payload_len or
        !std.crypto.timing_safe.eql([Sha256.digest_length]u8, existing.digest, expected.digest))
    {
        return error.SnapshotPayloadIdentityConflict;
    }
    return true;
}

pub fn readAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
) ![]u8 {
    const path = try pathAlloc(alloc, snapshot_dir, index, term);
    defer alloc.free(path);
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const envelope = try readEnvelope(file, io, index, term);
    if (envelope.payload_len > std.math.maxInt(usize)) return error.SnapshotPayloadTooLarge;
    const manifest = try readOpenManifest(alloc, file, io, envelope);
    defer alloc.free(manifest);

    const payload = try alloc.alloc(u8, @intCast(envelope.payload_len));
    errdefer alloc.free(payload);
    if (try file.readPositionalAll(io, payload, envelope.payloadOffset()) != payload.len)
        return error.SnapshotPayloadSizeMismatch;
    try verifyDigest(envelope, manifest, payload);
    return payload;
}

pub fn validate(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
) !void {
    _ = try validateWithReceipt(alloc, io, snapshot_dir, index, term);
}

pub fn validateWithReceipt(alloc: std.mem.Allocator, io: std.Io, snapshot_dir: []const u8, index: u64, term: u64) !PayloadReceipt {
    const path = try pathAlloc(alloc, snapshot_dir, index, term);
    defer alloc.free(path);
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const envelope = try readEnvelope(file, io, index, term);

    try validateOpenFile(file, io, envelope);
    return envelope.receipt();
}

/// Bounded, independently checksummed application bytes. This proves neither
/// payload validity nor replica-checkpoint publication: callers must join the
/// committed checkpoint and validate the payload before granting authority.
pub fn readApplicationManifest(alloc: std.mem.Allocator, io: std.Io, snapshot_dir: []const u8, index: u64, term: u64) ![]u8 {
    const path = try pathAlloc(alloc, snapshot_dir, index, term);
    defer alloc.free(path);
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    return readOpenManifest(alloc, file, io, try readEnvelope(file, io, index, term));
}

fn readOpenManifest(alloc: std.mem.Allocator, file: std.Io.File, io: std.Io, envelope: Envelope) ![]u8 {
    const result = try alloc.alloc(u8, @intCast(envelope.manifest_len));
    errdefer alloc.free(result);
    if (result.len != 0) {
        if (try file.readPositionalAll(io, result, envelope.headerLen()) != result.len) return error.SnapshotPayloadSizeMismatch;
        var digest: [32]u8 = undefined;
        Sha256.hash(result, &digest, .{});
        if (!std.crypto.timing_safe.eql([32]u8, digest, envelope.manifest_digest)) return error.SnapshotPayloadChecksumMismatch;
    }
    return result;
}

const UnboundedContext = struct {
    pub fn ensureActive(_: @This()) !void {}
};

fn validateOpenFile(file: std.Io.File, io: std.Io, envelope: Envelope) !void {
    return validateOpenFileWithContext(file, io, envelope, UnboundedContext{});
}

fn validateOpenFileWithContext(file: std.Io.File, io: std.Io, envelope: Envelope, context: anytype) !void {
    try context.ensureActive();
    var hasher = Sha256.init(.{});
    updateDigestIdentity(&hasher, envelope.index, envelope.term, envelope.payload_len);
    updateManifestIdentity(&hasher, envelope.manifest_len);
    var buffer: [copy_buffer_len]u8 = undefined;
    // Validate compact navigation/provenance before touching the potentially
    // huge payload, with the SAME fixed buffer reused for streaming below.
    if (envelope.manifest_len != 0) {
        const manifest = buffer[0..@intCast(envelope.manifest_len)];
        if (try file.readPositionalAll(io, manifest, envelope.headerLen()) != manifest.len) return error.SnapshotPayloadSizeMismatch;
        var digest: [32]u8 = undefined;
        Sha256.hash(manifest, &digest, .{});
        if (!std.crypto.timing_safe.eql([32]u8, digest, envelope.manifest_digest)) return error.SnapshotPayloadChecksumMismatch;
        hasher.update(manifest);
    }
    var offset: u64 = 0;
    while (offset < envelope.payload_len) {
        try context.ensureActive();
        const wanted: usize = @intCast(@min(buffer.len, envelope.payload_len - offset));
        const read = try file.readPositionalAll(io, buffer[0..wanted], envelope.payloadOffset() + offset);
        if (read != wanted) return error.SnapshotPayloadSizeMismatch;
        hasher.update(buffer[0..read]);
        offset += read;
    }
    var actual: [Sha256.digest_length]u8 = undefined;
    hasher.final(&actual);
    if (!std.crypto.timing_safe.eql([Sha256.digest_length]u8, actual, envelope.digest))
        return error.SnapshotPayloadChecksumMismatch;
}

fn readEnvelope(file: std.Io.File, io: std.Io, expected_index: u64, expected_term: u64) !Envelope {
    if (try file.length(io) < envelope_header_len) return error.InvalidSnapshotPayloadHeader;
    var header: [manifest_header_len]u8 = @splat(0);
    if (try file.readPositionalAll(io, header[0..envelope_header_len], 0) != envelope_header_len) return error.InvalidSnapshotPayloadHeader;
    if (!std.mem.eql(u8, header[0..envelope_magic.len], envelope_magic))
        return error.InvalidSnapshotPayloadHeader;
    const version = std.mem.readInt(u32, header[8..12], .little);
    if (version != envelope_version and version != manifest_envelope_version)
        return error.UnsupportedSnapshotPayloadVersion;
    const header_len: usize = if (version == envelope_version) envelope_header_len else manifest_header_len;
    if (std.mem.readInt(u32, header[12..16], .little) != header_len)
        return error.InvalidSnapshotPayloadHeader;
    var manifest_len: u64 = 0;
    if (version == manifest_envelope_version) {
        if (try file.readPositionalAll(io, header[envelope_header_len..], envelope_header_len) != manifest_header_len - envelope_header_len) return error.InvalidSnapshotPayloadHeader;
        manifest_len = std.mem.readInt(u64, header[72..80], .little);
        if (manifest_len == 0 or manifest_len > max_application_manifest_bytes) return error.SnapshotPayloadTooLarge;
    }

    const index = std.mem.readInt(u64, header[16..24], .little);
    const term = std.mem.readInt(u64, header[24..32], .little);
    const payload_len = std.mem.readInt(u64, header[32..40], .little);
    if (index != expected_index or term != expected_term) return error.SnapshotPayloadIdentityMismatch;
    if (payload_len > max_payload_bytes) return error.SnapshotPayloadTooLarge;
    const expected_file_len = std.math.add(u64, header_len + manifest_len, payload_len) catch
        return error.SnapshotPayloadTooLarge;
    if (try file.length(io) != expected_file_len) return error.SnapshotPayloadSizeMismatch;

    var digest: [Sha256.digest_length]u8 = undefined;
    @memcpy(&digest, header[40..72]);
    return .{ .index = index, .term = term, .payload_len = payload_len, .digest = digest, .manifest_len = manifest_len, .manifest_digest = if (manifest_len == 0) @splat(0) else header[80..112].* };
}

fn encodeEnvelope(envelope: Envelope) [manifest_header_len]u8 {
    var header: [manifest_header_len]u8 = @splat(0);
    @memcpy(header[0..8], envelope_magic);
    std.mem.writeInt(u32, header[8..12], if (envelope.manifest_len == 0) envelope_version else manifest_envelope_version, .little);
    std.mem.writeInt(u32, header[12..16], @intCast(envelope.headerLen()), .little);
    std.mem.writeInt(u64, header[16..24], envelope.index, .little);
    std.mem.writeInt(u64, header[24..32], envelope.term, .little);
    std.mem.writeInt(u64, header[32..40], envelope.payload_len, .little);
    @memcpy(header[40..72], &envelope.digest);
    if (envelope.manifest_len != 0) {
        std.mem.writeInt(u64, header[72..80], envelope.manifest_len, .little);
        header[80..112].* = envelope.manifest_digest;
    }
    return header;
}

fn updateDigestIdentity(hasher: *Sha256, index: u64, term: u64, payload_len: u64) void {
    hasher.update(digest_domain);
    var encoded: [24]u8 = undefined;
    std.mem.writeInt(u64, encoded[0..8], index, .little);
    std.mem.writeInt(u64, encoded[8..16], term, .little);
    std.mem.writeInt(u64, encoded[16..24], payload_len, .little);
    hasher.update(&encoded);
}

fn updateManifestIdentity(hasher: *Sha256, length: u64) void {
    if (length == 0) return; // Preserve every existing version-one byte/digest.
    hasher.update("antfly-raft-snapshot-application-manifest-v1\x00");
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, length, .little);
    hasher.update(&encoded);
}

fn verifyDigest(envelope: Envelope, manifest: []const u8, payload: []const u8) !void {
    var hasher = Sha256.init(.{});
    updateDigestIdentity(&hasher, envelope.index, envelope.term, envelope.payload_len);
    updateManifestIdentity(&hasher, manifest.len);
    hasher.update(manifest);
    hasher.update(payload);
    var actual: [Sha256.digest_length]u8 = undefined;
    hasher.final(&actual);
    if (!std.crypto.timing_safe.eql([Sha256.digest_length]u8, actual, envelope.digest))
        return error.SnapshotPayloadChecksumMismatch;
}

pub fn delete(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
) void {
    if (index == 0) return;
    const path = pathAlloc(alloc, snapshot_dir, index, term) catch return;
    defer alloc.free(path);
    std.Io.Dir.cwd().deleteFile(io, path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => std.log.warn("raft snapshot payload cleanup failed path={s} error={s}", .{ path, @errorName(err) }),
    };
}

pub fn cleanupOrphans(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    current_index: u64,
    current_term: u64,
) !void {
    const keep_name = if (current_index == 0)
        null
    else
        try std.fmt.allocPrint(alloc, "state-{d}-{d}.snap", .{ current_index, current_term });
    defer if (keep_name) |name| alloc.free(name);

    var dir = std.Io.Dir.cwd().openDir(io, snapshot_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer dir.close(io);

    var iter = dir.iterateAssumeFirstIteration();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file or !isManagedPayloadName(entry.name)) continue;
        if (keep_name) |name| if (std.mem.eql(u8, entry.name, name)) continue;
        dir.deleteFile(io, entry.name) catch |err| switch (err) {
            error.FileNotFound => {},
            else => std.log.warn("raft orphan snapshot payload cleanup failed path={s}/{s} error={s}", .{
                snapshot_dir,
                entry.name,
                @errorName(err),
            }),
        };
    }
}

fn isManagedPayloadName(name: []const u8) bool {
    if (!std.mem.startsWith(u8, name, "state-")) return false;
    const snapshot_suffix = std.mem.indexOf(u8, name, ".snap") orelse return false;
    const identity = name["state-".len..snapshot_suffix];
    const separator = std.mem.indexOfScalar(u8, identity, '-') orelse return false;
    if (separator == 0 or separator + 1 == identity.len) return false;
    _ = std.fmt.parseInt(u64, identity[0..separator], 10) catch return false;
    _ = std.fmt.parseInt(u64, identity[separator + 1 ..], 10) catch return false;

    const remainder = name[snapshot_suffix + ".snap".len ..];
    if (remainder.len == 0) return true;
    if (!std.mem.startsWith(u8, remainder, ".tmp-") or remainder.len == ".tmp-".len) return false;
    _ = std.fmt.parseInt(u64, remainder[".tmp-".len..], 10) catch return false;
    return true;
}

pub fn pathAlloc(alloc: std.mem.Allocator, snapshot_dir: []const u8, index: u64, term: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/state-{d}-{d}.snap", .{ snapshot_dir, index, term });
}

test "raft snapshot retention binds the qualified receipt and preserves application provenance" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/retention", .{tmp.sub_path});
    defer a.free(dir);
    const Source = struct {
        pub fn len(_: @This()) u64 {
            return "payload".len;
        }
        pub fn applicationManifest(_: @This()) []const u8 {
            return "actual-native-stream-receipt";
        }
        pub fn writeTo(_: @This(), writer: *std.Io.Writer) !void {
            try writer.writeAll("payload");
        }
    };
    const receipt = try writeArtifactWithReceipt(a, io, dir, 4, 2, Source{});
    const reference: [32]u8 = @splat(7);
    var bad = receipt;
    bad.digest[0] ^= 1;
    try std.testing.expectError(error.SnapshotPayloadIdentityConflict, retainPublished(a, io, dir, bad, reference));
    try std.testing.expectError(error.InvalidSnapshotRetention, retainPublished(a, io, dir, receipt, @splat(0)));
    var retained = try retainPublished(a, io, dir, receipt, reference);
    defer retained.deinit(a);
    var retry = try retainPublished(a, io, dir, receipt, reference);
    defer retry.deinit(a);
    try std.testing.expectEqualStrings((Source{}).applicationManifest(), retained.application_manifest);
    try std.testing.expectEqualDeep(retained.digest, retry.digest);
    delete(a, io, dir, 4, 2);
    try cleanupOrphans(a, io, dir, 0, 0);
    var recovered = try readRetained(a, io, dir, reference, receipt.identity);
    defer recovered.deinit(a);
    try std.testing.expectEqualStrings(retained.application_manifest, recovered.application_manifest);
    try std.testing.expectEqualDeep(receipt.digest, recovered.digest);
    const path = try retainedPathAlloc(a, dir, reference);
    defer a.free(path);
    {
        var file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
        defer file.close(io);
        try file.writePositionalAll(io, "X", manifest_header_len + receipt.manifest_len);
        try file.sync(io);
    }
    try std.testing.expectError(error.SnapshotPayloadChecksumMismatch, readRetained(a, io, dir, reference, receipt.identity));
    // Authorized retirement must remain possible even if the source payload
    // later develops bit rot. It still checks the exact immutable body identity.
    try releaseRetained(a, io, dir, retained);
    try releaseRetained(a, io, dir, retained);
}

test "raft node retention finishes after replica deletion and publishes bounded copies without reference rebinding" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const source = try std.fs.path.join(a, &.{ root, "group-7" });
    defer a.free(source);
    const destination = try std.fs.path.join(a, &.{ root, "node-retention" });
    defer a.free(destination);
    const Artifact = struct {
        pub fn len(_: @This()) u64 {
            return 3 * copy_buffer_len;
        }
        pub fn applicationManifest(_: @This()) []const u8 {
            return "retained-manifest";
        }
        pub fn writeTo(_: @This(), writer: *std.Io.Writer) !void {
            try writer.splatByteAll('A', 3 * copy_buffer_len);
        }
    };
    for ([_]RetentionTarget.Mode{ .automatic, .copy }, 0..) |mode, iteration| {
        const receipt = try writeArtifactWithReceipt(a, io, source, 5, 2, Artifact{});
        const reference: [32]u8 = @splat(@intCast(iteration + 1));
        var target = try RetentionTarget.init(a, io, destination, reference);
        defer target.deinit();
        try target.pin(source, receipt, mode);
        // No replica/provider pointer or path is retained by the pending pin.
        try std.Io.Dir.cwd().deleteTree(io, source);
        if (mode == .copy) {
            const Budget = struct {
                checks: usize = 0,
                fn ensureActive(self: *@This()) !void {
                    self.checks += 1;
                    if (self.checks == 4) return error.Canceled;
                }
            };
            var budget: Budget = .{};
            try std.testing.expectError(error.Canceled, target.finishWithContext(a, &budget));
            try std.testing.expectEqual(@as(usize, 4), budget.checks);
            try std.testing.expectError(error.FileNotFound, readRetained(a, io, destination, reference, receipt.identity));
        }
        var retained = try target.finish(a);
        defer retained.deinit(a);
        try std.testing.expectEqual(receipt.digest, retained.digest);
        try std.testing.expectEqualStrings((Artifact{}).applicationManifest(), retained.application_manifest);
        var cold = try readRetained(a, io, destination, reference, receipt.identity);
        defer cold.deinit(a);
        try std.testing.expectEqual(retained.digest, cold.digest);
        // A different published body with the same index/term cannot replace
        // this exact reference, whether linking or copying is selected.
        const conflicting = try writeWithReceipt(a, io, source, 5, 2, "different image");
        var other = try RetentionTarget.init(a, io, destination, reference);
        defer other.deinit();
        if (mode == .automatic) {
            try std.testing.expectError(error.SnapshotPayloadIdentityConflict, other.pin(source, conflicting, mode));
        } else {
            try other.pin(source, conflicting, mode);
            try std.testing.expectError(error.SnapshotPayloadIdentityConflict, other.finish(a));
        }
        var unchanged = try readRetained(a, io, destination, reference, receipt.identity);
        defer unchanged.deinit(a);
        try std.testing.expectEqual(receipt.digest, unchanged.digest);
        // This test authorizes only its own fixture's local retirement; it
        // does not claim a metadata obligation or synthesize an adoption ACK.
        try releaseRetained(a, io, destination, retained);
        try releaseRetained(a, io, destination, retained);
        try std.Io.Dir.cwd().deleteTree(io, source);
    }
}

test "raft copied retention rejects corrupt source bytes and leaves no published partial image" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const source = try std.fs.path.join(a, &.{ root, "source" });
    defer a.free(source);
    const destination = try std.fs.path.join(a, &.{ root, "destination" });
    defer a.free(destination);
    const receipt = try writeWithReceipt(a, io, source, 5, 2, "actual published source");
    const reference: [32]u8 = @splat(5);
    var target = try RetentionTarget.init(a, io, destination, reference);
    defer target.deinit();
    try target.pin(source, receipt, .copy);
    const path = try pathAlloc(a, source, 5, 2);
    defer a.free(path);
    {
        var corrupted = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
        defer corrupted.close(io);
        try corrupted.writePositionalAll(io, "X", envelope_header_len);
        try corrupted.sync(io);
    }
    try std.testing.expectError(error.SnapshotPayloadChecksumMismatch, target.finish(a));
    try std.testing.expectError(error.FileNotFound, readRetained(a, io, destination, reference, receipt.identity));
    var directory = try std.Io.Dir.cwd().openDir(io, destination, .{ .iterate = true });
    defer directory.close(io);
    var iterator = directory.iterate();
    var count: usize = 0;
    while (try iterator.next(io)) |entry| {
        try std.testing.expectEqualStrings("retained", entry.name);
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), count);
}

test "raft retained snapshot reader survives unlink and cancels bounded verification" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/detached-retained", .{tmp.sub_path});
    defer a.free(dir);
    const bytes = try a.alloc(u8, 3 * copy_buffer_len);
    defer a.free(bytes);
    @memset(bytes, 'a');
    const receipt = try writeWithReceipt(a, io, dir, 4, 2, bytes);
    const reference: [32]u8 = @splat(19);
    var retained = try retainPublished(a, io, dir, receipt, reference);
    defer retained.deinit(a);
    var reader = try openRetained(a, io, dir, reference, receipt.identity);
    defer reader.deinit();
    const Context = struct {
        checks: usize = 0,
        pub fn ensureActive(self: *@This()) !void {
            self.checks += 1;
            if (self.checks == 4) return error.DeadlineExceeded;
        }
    };
    var context: Context = .{};
    try std.testing.expectError(error.DeadlineExceeded, reader.readWithContext(a, &context));
    try std.testing.expectEqual(@as(usize, 4), context.checks);
    var captured_context: Context = .{};
    const authenticated: CapturedPayload = .{ .payload_len = receipt.payload_len, .digest = receipt.digest };
    var captured = try reader.readCaptured(a, authenticated, &captured_context);
    defer captured.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), captured_context.checks);
    try std.testing.expectEqualDeep(receipt.digest, captured.digest);
    var wrong_capture = authenticated;
    wrong_capture.digest[0] ^= 1;
    try std.testing.expectError(error.SnapshotPayloadIdentityConflict, reader.readCaptured(a, wrong_capture, UnboundedContext{}));
    wrong_capture = authenticated;
    wrong_capture.payload_len += 1;
    try std.testing.expectError(error.SnapshotPayloadIdentityConflict, reader.readCaptured(a, wrong_capture, UnboundedContext{}));
    // A cancelled attempt owns no metadata authority and does not unlink the
    // producer's durable obligation. A cold open still verifies the same image.
    var cold = try readRetained(a, io, dir, reference, receipt.identity);
    defer cold.deinit(a);
    try std.testing.expectEqualDeep(receipt.digest, cold.digest);
    const Lifetime = struct {
        released: bool = false,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(!self.released);
            self.released = true;
        }
    };
    var lifetime: Lifetime = .{};
    {
        var owned = try openRetained(a, io, dir, reference, receipt.identity);
        owned.lifetime = .{ .ptr = &lifetime, .release = Lifetime.release };
        defer owned.deinit();
        try std.testing.expectError(error.SnapshotPayloadIdentityMismatch, blk: {
            owned.identity.term += 1;
            break :blk owned.read(a);
        });
        try std.testing.expect(!lifetime.released);
    }
    try std.testing.expect(lifetime.released);
    delete(a, io, dir, 4, 2);
    try releaseRetained(a, io, dir, retained);
    try std.testing.expectError(error.FileNotFound, readRetained(a, io, dir, reference, receipt.identity));
    _ = try writeWithReceipt(a, io, dir, 5, 3, "replacement");
    // The old descriptor remains valid even after all its directory entries
    // disappear. It cannot follow a replacement CURRENT or reference path.
    var detached = try reader.read(a);
    defer detached.deinit(a);
    try std.testing.expectEqualDeep(receipt.digest, detached.digest);
    try std.testing.expectEqual(@as(u64, bytes.len), detached.payload_len);
    try std.testing.expectEqualDeep(receipt.identity, detached.identity);
}

test "raft snapshot payload envelope validates identity length and checksum" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const snapshot_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_dir);

    try writeAtomically(std.testing.allocator, io, snapshot_dir, 9, 3, "current");
    try validate(std.testing.allocator, io, snapshot_dir, 9, 3);
    const current = try readAlloc(std.testing.allocator, io, snapshot_dir, 9, 3);
    defer std.testing.allocator.free(current);
    try std.testing.expectEqualStrings("current", current);
    try writeAtomically(std.testing.allocator, io, snapshot_dir, 9, 3, "current");
    try std.testing.expectError(
        error.SnapshotPayloadIdentityConflict,
        writeAtomically(std.testing.allocator, io, snapshot_dir, 9, 3, "different"),
    );
    const unchanged = try readAlloc(std.testing.allocator, io, snapshot_dir, 9, 3);
    defer std.testing.allocator.free(unchanged);
    try std.testing.expectEqualStrings("current", unchanged);
    const path = try pathAlloc(std.testing.allocator, snapshot_dir, 9, 3);
    defer std.testing.allocator.free(path);
    var file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    var wrong_index: [8]u8 = undefined;
    std.mem.writeInt(u64, &wrong_index, 8, .little);
    try file.writePositionalAll(io, &wrong_index, 16);
    try std.testing.expectError(error.SnapshotPayloadIdentityMismatch, validate(std.testing.allocator, io, snapshot_dir, 9, 3));
    std.mem.writeInt(u64, &wrong_index, 9, .little);
    try file.writePositionalAll(io, &wrong_index, 16);
    try file.writePositionalAll(io, "X", envelope_header_len + 1);
    try std.testing.expectError(error.SnapshotPayloadChecksumMismatch, validate(std.testing.allocator, io, snapshot_dir, 9, 3));
}

test "raft snapshot payload application manifest is atomic bounded and independently checksummed" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/manifest-payload", .{tmp.sub_path});
    defer a.free(dir);
    const io = std.testing.io;
    const Source = struct {
        manifest: []const u8,
        pub fn len(_: @This()) u64 {
            return "payload".len;
        }
        pub fn writeTo(_: @This(), writer: *std.Io.Writer) !void {
            try writer.writeAll("payload");
        }
        pub fn applicationManifest(self: @This()) []const u8 {
            return self.manifest;
        }
    };
    var source: Source = .{ .manifest = "actual-native-stream-receipt" };
    try writeArtifactAtomically(a, io, dir, 9, 3, &source);
    try validate(a, io, dir, 9, 3);
    const payload = try readAlloc(a, io, dir, 9, 3);
    defer a.free(payload);
    try std.testing.expectEqualStrings("payload", payload);
    const manifest = try readApplicationManifest(a, io, dir, 9, 3);
    defer a.free(manifest);
    try std.testing.expectEqualStrings(source.manifest, manifest);
    try writeArtifactAtomically(a, io, dir, 9, 3, source);
    try std.testing.expectError(error.SnapshotPayloadIdentityConflict, writeArtifactAtomically(a, io, dir, 9, 3, Source{ .manifest = "different" }));
    const too_large = try a.alloc(u8, max_application_manifest_bytes + 1);
    defer a.free(too_large);
    try std.testing.expectError(error.SnapshotPayloadTooLarge, writeArtifactAtomically(a, io, dir, 10, 3, Source{ .manifest = too_large }));
    const path = try pathAlloc(a, dir, 9, 3);
    defer a.free(path);
    var file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    // A valid small manifest is not proof that the separately stored payload
    // is intact or that a replica checkpoint has committed this artifact.
    try file.writePositionalAll(io, "X", manifest_header_len + source.manifest.len);
    const retained = try readApplicationManifest(a, io, dir, 9, 3);
    defer a.free(retained);
    try std.testing.expectEqualStrings(source.manifest, retained);
    try std.testing.expectError(error.SnapshotPayloadChecksumMismatch, validate(a, io, dir, 9, 3));
    try std.testing.expectError(error.SnapshotPayloadChecksumMismatch, readAlloc(a, io, dir, 9, 3));
    try file.writePositionalAll(io, "X", manifest_header_len);
    try std.testing.expectError(error.SnapshotPayloadChecksumMismatch, readApplicationManifest(a, io, dir, 9, 3));
    try std.testing.expectError(error.SnapshotPayloadChecksumMismatch, validate(a, io, dir, 9, 3));
    // A bad tiny manifest must reject before allocating a large sparse body.
    const large_payload = 32 * 1024 * 1024;
    var size_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &size_bytes, large_payload, .little);
    try file.writePositionalAll(io, &size_bytes, 32);
    try file.setLength(io, manifest_header_len + source.manifest.len + large_payload);
    var bounded_memory: [8192]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&bounded_memory);
    try std.testing.expectError(error.SnapshotPayloadChecksumMismatch, readAlloc(bounded.allocator(), io, dir, 9, 3));
    try std.testing.expectError(error.SnapshotPayloadChecksumMismatch, validate(a, io, dir, 9, 3));
    try writeAtomically(a, io, dir, 11, 3, "legacy-payload");
    const absent = try readApplicationManifest(a, io, dir, 11, 3);
    defer a.free(absent);
    try std.testing.expectEqual(@as(usize, 0), absent.len);
}

test "raft snapshot payload publication rejects an artifact length contract violation" {
    const MisreportedSource = struct {
        bytes: []const u8,

        fn len(self: @This()) u64 {
            return self.bytes.len + 1;
        }

        fn writeTo(self: @This(), writer: *std.Io.Writer) !void {
            try writer.writeAll(self.bytes);
        }
    };
    const OversizedSource = struct {
        fn len(_: @This()) u64 {
            return max_payload_bytes + 1;
        }

        fn writeTo(_: @This(), _: *std.Io.Writer) !void {
            unreachable;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const snapshot_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/artifact-length", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_dir);

    try std.testing.expectError(
        error.SnapshotArtifactSizeMismatch,
        writeSourceAtomically(std.testing.allocator, io, snapshot_dir, 4, 2, MisreportedSource{ .bytes = "short" }),
    );
    try std.testing.expectError(
        error.SnapshotPayloadTooLarge,
        writeSourceAtomically(std.testing.allocator, io, snapshot_dir, 5, 2, OversizedSource{}),
    );
    const path = try pathAlloc(std.testing.allocator, snapshot_dir, 4, 2);
    defer std.testing.allocator.free(path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, path, .{}));
}

test "raft snapshot payload cleanup retains only the durable identity" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const snapshot_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_dir);

    try writeAtomically(std.testing.allocator, io, snapshot_dir, 7, 2, "old");
    try writeAtomically(std.testing.allocator, io, snapshot_dir, 9, 3, "current");
    const stale_tmp = try std.fmt.allocPrint(std.testing.allocator, "{s}/state-10-4.snap.tmp-1", .{snapshot_dir});
    defer std.testing.allocator.free(stale_tmp);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = stale_tmp, .data = "partial" });

    try cleanupOrphans(std.testing.allocator, io, snapshot_dir, 9, 3);
    const current = try readAlloc(std.testing.allocator, io, snapshot_dir, 9, 3);
    defer std.testing.allocator.free(current);
    try std.testing.expectEqualStrings("current", current);

    const old_path = try pathAlloc(std.testing.allocator, snapshot_dir, 7, 2);
    defer std.testing.allocator.free(old_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, old_path, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, stale_tmp, .{}));
}
