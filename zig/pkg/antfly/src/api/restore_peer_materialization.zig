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

//! Certified peer artifact ingestion into the existing shared restore source
//! generation. The caller holds that generation's exclusive lease. One turn
//! receives one bounded chunk, verifies one bounded certificate slice, or
//! imports one bounded logical page. A partial archive is never a decoder.
const std = @import("std");
const contract = @import("restore_owner_contract.zig");
const staging = @import("../storage/db/restore_staging_contract.zig");
const transfer = @import("../storage/db/source_artifact_transfer.zig");
const native = @import("../storage/db/native_backup.zig");
const fs = @import("../common/fs_paths.zig");
const Cancellation = @import("../common/cancellation.zig").CancellationToken;
pub const Result = struct { complete: bool, next_offset: u64 };

const Receipt = struct {
    scope: [32]u8,
    certificate: [32]u8,
    total: u64,
    next: u64 = 0,

    fn encode(self: Receipt) [120]u8 {
        var bytes: [120]u8 = @splat(0);
        bytes[0..4].* = "RPA1".*;
        bytes[8..40].* = self.scope;
        bytes[40..72].* = self.certificate;
        std.mem.writeInt(u64, bytes[72..80], self.total, .little);
        std.mem.writeInt(u64, bytes[80..88], self.next, .little);
        bytes[88..120].* = transfer.checksum(bytes[0..88]);
        return bytes;
    }
    fn decode(bytes: []const u8, expected: Receipt) !Receipt {
        if (bytes.len != 120 or !std.mem.eql(u8, bytes[0..4], "RPA1") or !std.mem.allEqual(u8, bytes[4..8], 0) or
            !std.mem.eql(u8, bytes[88..120], &transfer.checksum(bytes[0..88]))) return error.InvalidRestoreSourceCheckpoint;
        const result: Receipt = .{ .scope = bytes[8..40].*, .certificate = bytes[40..72].*, .total = std.mem.readInt(u64, bytes[72..80], .little), .next = std.mem.readInt(u64, bytes[80..88], .little) };
        if (!std.mem.eql(u8, &result.scope, &expected.scope) or !std.mem.eql(u8, &result.certificate, &expected.certificate) or result.total != expected.total) return error.RestoreStagingScopeChanged;
        if (result.next > result.total or (result.next != result.total and result.next % transfer.max_chunk_bytes != 0)) return error.InvalidRestoreSourceCheckpoint;
        return result;
    }
};

fn save(alloc: std.mem.Allocator, io: std.Io, root: []const u8, receipt: Receipt) !void {
    const path = try std.fmt.allocPrint(alloc, "{s}/peer.progress", .{root});
    defer alloc.free(path);
    const next = try std.fmt.allocPrint(alloc, "{s}/peer.progress.next", .{root});
    defer alloc.free(next);
    _ = try native.writeFileDurable(io, next, &receipt.encode());
    try std.Io.Dir.rename(.cwd(), next, .cwd(), path, io);
    try fs.syncDirPortable(io, root);
}

pub fn step(alloc: std.mem.Allocator, io: std.Io, source: contract.Source, scope: staging.Scope, owner_range: @import("../storage/docstore.zig").ByteRange, root: []const u8, chunk: ?transfer.ReadResponse, cancellation: Cancellation, byte_budget: usize) !Result {
    try cancellation.check();
    const descriptor = source.peer_descriptor orelse return error.RestoreSourceProofMissing;
    try contract.validatePeerSource(source, scope, descriptor);
    if (byte_budget < 64 * 1024 or byte_budget > @import("restore_materialization.zig").chunk_bytes or
        !std.mem.eql(u8, &try source.artifact.digest(alloc), &scope.source_descriptor_digest)) return error.RestoreSourceProofMissing;
    try fs.createDirPathPortable(io, root);
    const receipt_path = try std.fmt.allocPrint(alloc, "{s}/peer.progress", .{root});
    defer alloc.free(receipt_path);
    const raw = native.readFileAlloc(alloc, io, receipt_path, 121) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (raw) |bytes| alloc.free(bytes);
    var receipt: Receipt = .{ .scope = scope.digest(), .certificate = try descriptor.certificate.digest(), .total = descriptor.total_bytes };
    if (raw) |bytes| receipt = try Receipt.decode(bytes, receipt);
    const artifact_path = try std.fmt.allocPrint(alloc, "{s}/source.afb", .{root});
    defer alloc.free(artifact_path);
    const artifact = try fs.createFilePortable(io, artifact_path, .{ .read = true, .truncate = false });
    defer artifact.close(io);
    const size = (try artifact.stat(io)).size;
    if (size < receipt.next or size > receipt.total) return error.InvalidRestoreSourceCheckpoint;
    if (chunk) |incoming| {
        const count = std.base64.standard.Decoder.calcSizeForSlice(incoming.data_base64) catch return error.SourceSnapshotCorrupt;
        if (incoming.offset >= receipt.total or incoming.offset % transfer.max_chunk_bytes != 0 or count != @min(transfer.max_chunk_bytes, receipt.total - incoming.offset) or count > byte_budget) return error.InvalidSourceSnapshot;
        if (incoming.offset > receipt.next) return error.SourceSnapshotIncomplete;
        const data = try alloc.alloc(u8, count);
        defer alloc.free(data);
        std.base64.standard.Decoder.decode(data, incoming.data_base64) catch return error.SourceSnapshotCorrupt;
        if (!std.mem.eql(u8, &transfer.checksum(data), &incoming.digest)) return error.SourceSnapshotCorrupt;
        if (incoming.offset < receipt.next) {
            const prior = try alloc.alloc(u8, count);
            defer alloc.free(prior);
            if (try artifact.readPositionalAll(io, prior, incoming.offset) != prior.len or !std.mem.eql(u8, prior, data)) return error.SourceSnapshotCorrupt;
        } else {
            try artifact.writePositionalAll(io, data, incoming.offset);
            receipt.next += count;
            // A failed previous receipt may leave an unacknowledged suffix.
            // Never preallocate the claimed artifact length.
            try artifact.setLength(io, receipt.next);
            try artifact.sync(io);
            try cancellation.check();
            try save(alloc, io, root, receipt);
        }
        return .{ .complete = false, .next_offset = receipt.next };
    }
    if (receipt.next != receipt.total) return .{ .complete = false, .next_offset = receipt.next };
    if (size != receipt.total) return error.InvalidRestoreSourceCheckpoint;
    const verified = try @import("../storage/portable_source_verifier.zig").step(alloc, io, artifact, root, scope.digest(), descriptor.certificate, cancellation, .{ .bytes = @min(byte_budget, transfer.max_chunk_bytes) });
    if (!verified.complete) return .{ .complete = false, .next_offset = receipt.next };
    return .{ .complete = try @import("restore_materialization.zig").stepPortableDecoder(alloc, io, artifact, source.artifact, scope, owner_range, root, cancellation), .next_offset = receipt.next };
}
