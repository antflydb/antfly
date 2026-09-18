// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Replica-local transport of an already certified source cut. The replicated
//! ledger supplies authority; transfer input cannot create or change a cut.
//! Data is fsynced before its receipt, and publication follows full shared AFB
//! verification. Interrupted transfer resumes at an exact bounded byte offset.
const std = @import("std");
const Allocator = std.mem.Allocator;
const DB = @import("db.zig").DB;
const pin = @import("source_pin.zig");
const seal = @import("native_backup_seal.zig");
const backup = @import("native_backup.zig");
const fs = @import("../../common/fs_paths.zig");
const snapshot = @import("../source_snapshot.zig");
const Scope = @import("online_source_contract.zig").Scope;
const Cancellation = @import("types.zig").CancellationToken;
pub const max_chunk_bytes = 1024 * 1024;
pub const Descriptor = struct { scope: Scope, certificate: snapshot.Certificate, total_bytes: u64 };
pub const Status = struct { next_offset: u64, complete: bool, verification_bytes: u64 = 0 };
pub const Chunk = struct { offset: u64, data: []const u8, digest: [32]u8 };
/// Authenticated owner-private wire. Base64 keeps one bounded chunk from
/// expanding into a million JSON array nodes at the compiled boundary.
pub const Request = union(enum) {
    describe: Scope,
    read: struct { descriptor: Descriptor, offset: u64 },
    status: Descriptor,
    write: struct { descriptor: Descriptor, offset: u64, data_base64: []const u8, digest: [32]u8 },
    finish: Descriptor,
    reset: Descriptor,

    pub fn scope(self: Request) Scope {
        return switch (self) {
            .describe => |value| value,
            .status, .finish, .reset => |value| value.scope,
            .read => |value| value.descriptor.scope,
            .write => |value| value.descriptor.scope,
        };
    }
};
pub const ReadResponse = struct { offset: u64, data_base64: []const u8, digest: [32]u8 };

pub fn executeJson(db: *DB, alloc: Allocator, request: Request, cancellation: Cancellation) ![]u8 {
    try cancellation.check();
    return switch (request) {
        .describe => |scope| std.json.Stringify.valueAlloc(alloc, try describe(db, scope, cancellation), .{}),
        .status => |descriptor| std.json.Stringify.valueAlloc(alloc, try status(db, descriptor, cancellation), .{}),
        .finish => |descriptor| std.json.Stringify.valueAlloc(alloc, try finish(db, descriptor, cancellation), .{}),
        .reset => |descriptor| std.json.Stringify.valueAlloc(alloc, try reset(db, descriptor, cancellation), .{}),
        .write => |value| blk: {
            const size = std.base64.standard.Decoder.calcSizeForSlice(value.data_base64) catch return error.InvalidSourceSnapshot;
            if (size == 0 or size > max_chunk_bytes) return error.InvalidSourceSnapshot;
            const data = try alloc.alloc(u8, size);
            defer alloc.free(data);
            std.base64.standard.Decoder.decode(data, value.data_base64) catch return error.InvalidSourceSnapshot;
            break :blk std.json.Stringify.valueAlloc(alloc, try receive(db, value.descriptor, .{ .offset = value.offset, .data = data, .digest = value.digest }, cancellation), .{});
        },
        .read => |value| blk: {
            var chunk = try read(db, alloc, value.descriptor, value.offset, cancellation);
            defer chunk.deinit(alloc);
            const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(chunk.data.len));
            defer alloc.free(encoded);
            break :blk std.json.Stringify.valueAlloc(alloc, ReadResponse{ .offset = chunk.offset, .data_base64 = std.base64.standard.Encoder.encode(encoded, chunk.data), .digest = chunk.digest }, .{});
        },
    };
}
pub const OwnedChunk = struct {
    offset: u64,
    data: []u8,
    digest: [32]u8,
    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.data);
        self.* = undefined;
    }
};
pub fn checksum(bytes: []const u8) [32]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    return hash;
}

fn authorize(db: *DB, descriptor: Descriptor) !void {
    if (db.open_mode == .query_readonly or db.open_mode == .status_only) return error.ReadOnly;
    try descriptor.scope.validate();
    if (descriptor.total_bytes == 0 or descriptor.total_bytes > std.math.maxInt(i64)) return error.InvalidSourceSnapshot;
    const progress = try db.onlineSourceStatus(descriptor.scope);
    if (progress.phase == .released or progress.snapshot_phase != .published) return error.OnlineSourceScopeChanged;
    if (!descriptor.certificate.cut.namespace.eql(descriptor.scope.fence.namespace) or
        descriptor.certificate.cut.applied_index != progress.admitted_applied_index or
        descriptor.certificate.cut.retained_start != progress.start or
        !std.mem.eql(u8, &try descriptor.certificate.digest(), &progress.snapshot_certificate)) return error.SourceSnapshotCutMismatch;
}

const Lease = struct {
    db: *DB,
    scope: Scope,
    alloc: Allocator,
    io: std.Io,
    root: []u8,
    held: seal.StoreLock,
    fn init(db: *DB, scope: Scope, cancellation: Cancellation) !Lease {
        try cancellation.check();
        if (db.open_mode == .query_readonly or db.open_mode == .status_only) return error.ReadOnly;
        const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
        const found = try pin.locate(db, scope);
        if (found.progress.phase == .released) return error.OnlineSourceScopeChanged;
        const lock_path = try pin.lockPath(db.alloc, db.core.path, found.slot);
        defer db.alloc.free(lock_path);
        var held = try seal.StoreLock.acquire(db.alloc, io, lock_path, cancellation);
        errdefer held.deinit();
        const current = try pin.locate(db, scope);
        if (current.slot != found.slot or current.progress.phase == .released) return error.OnlineSourceScopeChanged;
        return .{ .db = db, .scope = scope, .alloc = db.alloc, .io = io, .root = try pin.pathAlloc(db.alloc, db.core.path, scope), .held = held };
    }
    fn path(self: Lease, name: []const u8) ![]u8 {
        return std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ self.root, name });
    }
    fn deinit(self: *Lease) void {
        self.held.deinit();
        pin.reclaimReleased(self.db, self.scope) catch {};
        self.alloc.free(self.root);
    }
};

// The transport receipt is fixed-size and independent of artifact size. The
// scope hash includes both copy-attempt components and every fence field.
const Receipt = struct {
    pin_digest: [32]u8,
    certificate_digest: [32]u8,
    total: u64,
    next: u64,
    fn encode(self: Receipt) [112]u8 {
        var bytes: [112]u8 = undefined;
        bytes[0..32].* = self.pin_digest;
        bytes[32..64].* = self.certificate_digest;
        std.mem.writeInt(u64, bytes[64..72], self.total, .little);
        std.mem.writeInt(u64, bytes[72..80], self.next, .little);
        bytes[80..112].* = checksum(bytes[0..80]);
        return bytes;
    }
    fn decode(raw: []const u8, descriptor: Descriptor) !Receipt {
        if (raw.len != 112 or !std.mem.eql(u8, raw[80..112], &checksum(raw[0..80]))) return error.SourceSnapshotCorrupt;
        const value: Receipt = .{ .pin_digest = raw[0..32].*, .certificate_digest = raw[32..64].*, .total = std.mem.readInt(u64, raw[64..72], .little), .next = std.mem.readInt(u64, raw[72..80], .little) };
        if (!std.mem.eql(u8, &value.pin_digest, &descriptor.scope.pin()) or !std.mem.eql(u8, &value.certificate_digest, &try descriptor.certificate.digest()) or value.total != descriptor.total_bytes) return error.SourceSnapshotCutMismatch;
        if (value.next > value.total or (value.next != value.total and value.next % max_chunk_bytes != 0)) return error.SourceSnapshotCorrupt;
        return value;
    }
};

fn loadReceipt(lease: Lease, descriptor: Descriptor) !Receipt {
    const path = try lease.path("source.transfer");
    defer lease.alloc.free(path);
    const raw = backup.readFileAlloc(lease.alloc, lease.io, path, 113) catch |err| switch (err) {
        error.FileNotFound => return .{ .pin_digest = descriptor.scope.pin(), .certificate_digest = try descriptor.certificate.digest(), .total = descriptor.total_bytes, .next = 0 },
        else => return err,
    };
    defer lease.alloc.free(raw);
    return Receipt.decode(raw, descriptor);
}
fn saveReceipt(lease: Lease, receipt: Receipt) !void {
    const staging = try lease.path("source.transfer.staging");
    defer lease.alloc.free(staging);
    const path = try lease.path("source.transfer");
    defer lease.alloc.free(path);
    _ = try backup.writeFileDurable(lease.io, staging, &receipt.encode());
    try std.Io.Dir.rename(.cwd(), staging, .cwd(), path, lease.io);
    try fs.syncDirPortable(lease.io, lease.root);
}

// A receiver creates a new local stat receipt only after verifying every AFB
// block and the complete transferable logical certificate. No seal digest is
// borrowed from another replica, and no original inode is required.
pub fn transferredCertificate(db: *DB, scope: Scope, root: []const u8) !?snapshot.Certificate {
    const certificate_size = snapshot.encoded_size;
    const body_size = certificate_size + 64;
    const io = db.backend_runtime.filesystemIo() orelse return error.BackendRuntimeIoUnavailable;
    const path = try std.fmt.allocPrint(db.alloc, "{s}/source.transferred", .{root});
    defer db.alloc.free(path);
    const raw = backup.readFileAlloc(db.alloc, io, path, body_size + 33) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer db.alloc.free(raw);
    if (raw.len != body_size + 32 or !std.mem.eql(u8, raw[body_size..], &checksum(raw[0..body_size])) or !std.mem.eql(u8, raw[certificate_size..][0..32], &scope.pin())) return error.SourceSnapshotCorrupt;
    const certificate = try snapshot.Certificate.decode(raw[0..certificate_size]);
    const size = std.mem.readInt(u64, raw[certificate_size + 40 ..][0..8], .little);
    try authorize(db, .{ .scope = scope, .certificate = certificate, .total_bytes = size });
    const artifact = try std.fmt.allocPrint(db.alloc, "{s}/source.afb2", .{root});
    defer db.alloc.free(artifact);
    const stat = try backup.statRegularFile(io, artifact);
    if (stat.inode != std.mem.readInt(u64, raw[certificate_size + 32 ..][0..8], .little) or stat.size != size or stat.mtime.toNanoseconds() != std.mem.readInt(i128, raw[certificate_size + 48 ..][0..16], .little)) return error.BackupSealSourceChanged;
    return certificate;
}

pub fn describe(db: *DB, scope: Scope, cancellation: Cancellation) !Descriptor {
    if (db.open_mode == .query_readonly or db.open_mode == .status_only) return error.ReadOnly;
    const progress = try db.onlineSourceStatus(scope);
    if (progress.phase == .released or progress.snapshot_phase != .published) return error.OnlineSourceScopeChanged;
    const certificate = try pin.preparePublication(db, scope, cancellation);
    var lease = try Lease.init(db, scope, cancellation);
    defer lease.deinit();
    const path = try lease.path("source.afb2");
    defer lease.alloc.free(path);
    const descriptor: Descriptor = .{ .scope = scope, .certificate = certificate, .total_bytes = (try backup.statRegularFile(lease.io, path)).size };
    try authorize(db, descriptor);
    return descriptor;
}

pub fn read(db: *DB, alloc: Allocator, descriptor: Descriptor, offset: u64, cancellation: Cancellation) !OwnedChunk {
    const actual = try describe(db, descriptor.scope, cancellation);
    if (actual.total_bytes != descriptor.total_bytes or !actual.certificate.eql(descriptor.certificate)) return error.SourceSnapshotCutMismatch;
    var lease = try Lease.init(db, descriptor.scope, cancellation);
    defer lease.deinit();
    try authorize(db, descriptor);
    if (offset >= descriptor.total_bytes or offset % max_chunk_bytes != 0) return error.InvalidSourceSnapshot;
    const path = try lease.path("source.afb2");
    defer lease.alloc.free(path);
    const file = try std.Io.Dir.cwd().openFile(lease.io, path, .{});
    defer file.close(lease.io);
    const data = try alloc.alloc(u8, @intCast(@min(max_chunk_bytes, descriptor.total_bytes - offset)));
    errdefer alloc.free(data);
    if (try file.readPositionalAll(lease.io, data, offset) != data.len) return error.SourceSnapshotCorrupt;
    try cancellation.check();
    try authorize(db, descriptor);
    return .{ .offset = offset, .data = data, .digest = checksum(data) };
}

fn currentStatus(db: *DB, lease: Lease, descriptor: Descriptor) !Status {
    if (try transferredCertificate(db, descriptor.scope, lease.root)) |certificate| {
        if (!certificate.eql(descriptor.certificate)) return error.SourceSnapshotCutMismatch;
        const path = try lease.path("source.afb2");
        defer lease.alloc.free(path);
        if ((try backup.statRegularFile(lease.io, path)).size != descriptor.total_bytes) return error.SourceSnapshotCutMismatch;
        return .{ .next_offset = descriptor.total_bytes, .complete = true };
    }
    const receipt = try loadReceipt(lease, descriptor);
    if (receipt.next != 0) {
        const receiving = try lease.path("source.receiving");
        defer lease.alloc.free(receiving);
        const stat = backup.statRegularFile(lease.io, receiving) catch |err| switch (err) {
            error.FileNotFound => blk: {
                if (receipt.next != receipt.total) return error.SourceSnapshotCorrupt;
                const artifact = try lease.path("source.afb2");
                defer lease.alloc.free(artifact);
                break :blk try backup.statRegularFile(lease.io, artifact);
            },
            else => return err,
        };
        if (stat.size < receipt.next or stat.size > receipt.total) return error.SourceSnapshotCorrupt;
    }
    return .{ .next_offset = receipt.next, .complete = false };
}
pub fn status(db: *DB, descriptor: Descriptor, cancellation: Cancellation) !Status {
    var lease = try Lease.init(db, descriptor.scope, cancellation);
    defer lease.deinit();
    try authorize(db, descriptor);
    return currentStatus(db, lease, descriptor);
}

pub fn receive(db: *DB, descriptor: Descriptor, chunk: Chunk, cancellation: Cancellation) !Status {
    try cancellation.check();
    try authorize(db, descriptor);
    if (chunk.offset >= descriptor.total_bytes or chunk.offset % max_chunk_bytes != 0 or chunk.data.len != @min(max_chunk_bytes, descriptor.total_bytes - chunk.offset) or !std.mem.eql(u8, &checksum(chunk.data), &chunk.digest)) return error.SourceSnapshotCorrupt;
    var lease = try Lease.init(db, descriptor.scope, cancellation);
    defer lease.deinit();
    try authorize(db, descriptor);
    const current = try currentStatus(db, lease, descriptor);
    if (chunk.offset > current.next_offset) return error.SourceSnapshotIncomplete;
    try fs.createDirPathPortable(lease.io, lease.root);
    if (current.next_offset == 0) {
        const parent = std.fs.path.dirname(lease.root).?;
        try fs.syncDirPortable(lease.io, parent);
        // A replica installed from native state may not have the source-pin
        // parent at all. Persist that newly created directory entry too.
        try fs.syncDirPortable(lease.io, std.fs.path.dirname(parent) orelse ".");
    }
    const path = try lease.path(if (current.complete) "source.afb2" else "source.receiving");
    defer lease.alloc.free(path);
    const file = if (chunk.offset < current.next_offset)
        std.Io.Dir.cwd().openFile(lease.io, path, .{}) catch |err| switch (err) {
            error.FileNotFound => blk: {
                if (current.next_offset != descriptor.total_bytes) return error.SourceSnapshotCorrupt;
                const published = try lease.path("source.afb2");
                defer lease.alloc.free(published);
                break :blk try std.Io.Dir.cwd().openFile(lease.io, published, .{});
            },
            else => return err,
        }
    else
        try std.Io.Dir.cwd().createFile(lease.io, path, .{ .read = true, .truncate = false });
    defer file.close(lease.io);
    if (chunk.offset < current.next_offset) {
        const prior = try db.alloc.alloc(u8, chunk.data.len);
        defer db.alloc.free(prior);
        if (try file.readPositionalAll(lease.io, prior, chunk.offset) != prior.len or !std.mem.eql(u8, prior, chunk.data)) return error.SourceSnapshotCorrupt;
        return current;
    }
    try file.writePositionalAll(lease.io, chunk.data, chunk.offset);
    const next = chunk.offset + chunk.data.len;
    // Discard any unacknowledged tail from a crash before its receipt. This
    // never preallocates the claimed total or retains abandoned physical data.
    try file.setLength(lease.io, next);
    try file.sync(lease.io);
    try cancellation.check();
    try authorize(db, descriptor);
    try saveReceipt(lease, .{ .pin_digest = descriptor.scope.pin(), .certificate_digest = try descriptor.certificate.digest(), .total = descriptor.total_bytes, .next = next });
    return .{ .next_offset = next, .complete = false };
}

pub fn finish(db: *DB, descriptor: Descriptor, cancellation: Cancellation) !Status {
    var lease = try Lease.init(db, descriptor.scope, cancellation);
    defer lease.deinit();
    try authorize(db, descriptor);
    const current = try currentStatus(db, lease, descriptor);
    if (current.complete) return current;
    if (current.next_offset != descriptor.total_bytes) return error.SourceSnapshotIncomplete;
    const staging = try lease.path("source.receiving");
    defer lease.alloc.free(staging);
    const artifact = try lease.path("source.afb2");
    defer lease.alloc.free(artifact);
    // A crash after rename but before the publication receipt resumes final
    // verification against the already renamed file, never the mutable owner.
    const file = std.Io.Dir.cwd().openFile(lease.io, staging, .{}) catch |err| switch (err) {
        error.FileNotFound => try std.Io.Dir.cwd().openFile(lease.io, artifact, .{}),
        else => return err,
    };
    defer file.close(lease.io);
    if ((try file.stat(lease.io)).size != descriptor.total_bytes) return error.SourceSnapshotCorrupt;
    const verified = try @import("../portable_source_verifier.zig").step(db.alloc, lease.io, file, lease.root, descriptor.scope.pin(), descriptor.certificate, cancellation, .{});
    if (!verified.complete) return .{ .next_offset = descriptor.total_bytes, .complete = false, .verification_bytes = verified.verified_bytes };
    try cancellation.check();
    try authorize(db, descriptor);
    std.Io.Dir.rename(.cwd(), staging, .cwd(), artifact, lease.io) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    try fs.syncDirPortable(lease.io, lease.root);
    const stat = try file.stat(lease.io);
    const certificate_size = snapshot.encoded_size;
    const body_size = certificate_size + 64;
    var receipt: [body_size + 32]u8 = undefined;
    receipt[0..certificate_size].* = try descriptor.certificate.encode();
    receipt[certificate_size..][0..32].* = descriptor.scope.pin();
    std.mem.writeInt(u64, receipt[certificate_size + 32 ..][0..8], stat.inode, .little);
    std.mem.writeInt(u64, receipt[certificate_size + 40 ..][0..8], stat.size, .little);
    std.mem.writeInt(i128, receipt[certificate_size + 48 ..][0..16], stat.mtime.toNanoseconds(), .little);
    receipt[body_size..].* = checksum(receipt[0..body_size]);
    const receipt_staging = try lease.path("source.transferred.staging");
    defer lease.alloc.free(receipt_staging);
    const receipt_path = try lease.path("source.transferred");
    defer lease.alloc.free(receipt_path);
    _ = try backup.writeFileDurable(lease.io, receipt_staging, &receipt);
    try std.Io.Dir.rename(.cwd(), receipt_staging, .cwd(), receipt_path, lease.io);
    try fs.syncDirPortable(lease.io, lease.root);
    try authorize(db, descriptor);
    return .{ .next_offset = descriptor.total_bytes, .complete = true };
}

/// Explicit repair restarts only unpublished transport. A completed verified
/// artifact is immutable; callers cannot reset it or the donor's native pin.
pub fn reset(db: *DB, descriptor: Descriptor, cancellation: Cancellation) !Status {
    var lease = try Lease.init(db, descriptor.scope, cancellation);
    defer lease.deinit();
    try authorize(db, descriptor);
    if (try transferredCertificate(db, descriptor.scope, lease.root)) |_| return currentStatus(db, lease, descriptor);
    try fs.createDirPathPortable(lease.io, lease.root);
    // Remove the data before rolling the durable offset back. A crash between
    // them remains repairable by repeating this exact reset, never by reading
    // from a new mutable source cut.
    for ([_][]const u8{ "source.receiving", "source.transfer", "source.transfer.staging", "source.verify", "source.verify.staging", "source.verify.index" }) |name| {
        try cancellation.check();
        const path = try lease.path(name);
        defer lease.alloc.free(path);
        std.Io.Dir.cwd().deleteFile(lease.io, path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
    try fs.syncDirPortable(lease.io, lease.root);
    try authorize(db, descriptor);
    return .{ .next_offset = 0, .complete = false };
}
