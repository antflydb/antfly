// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Disposable, bounded retained-frame assembly beneath the existing restore
//! source generation. Its receipt is not target progress: only the shared
//! restore page's replicated CAS can acknowledge transformed effects.
const std = @import("std");
const contract = @import("db/relational_rewrite_contract.zig");
const staging = @import("db/restore_staging_contract.zig");
const native = @import("db/native_backup.zig");
const fs = @import("../common/fs_paths.zig");
const Digest = [32]u8;
const Receipt = struct {
    scope: Digest,
    sequence: u64,
    digest: Digest,
    total: u32,
    next: u32,

    fn encode(self: Receipt) [112]u8 {
        var bytes: [112]u8 = undefined;
        @memcpy(bytes[0..32], &self.scope);
        std.mem.writeInt(u64, bytes[32..40], self.sequence, .little);
        @memcpy(bytes[40..72], &self.digest);
        std.mem.writeInt(u32, bytes[72..76], self.total, .little);
        std.mem.writeInt(u32, bytes[76..80], self.next, .little);
        std.crypto.hash.Blake3.hash(bytes[0..80], bytes[80..112], .{});
        return bytes;
    }
    fn decode(bytes: []const u8) !Receipt {
        if (bytes.len != 112) return error.InvalidRestoreSourceCheckpoint;
        var digest: Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..80], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[80..112])) return error.InvalidRestoreSourceCheckpoint;
        const value: Receipt = .{ .scope = bytes[0..32].*, .sequence = std.mem.readInt(u64, bytes[32..40], .little), .digest = bytes[40..72].*, .total = std.mem.readInt(u32, bytes[72..76], .little), .next = std.mem.readInt(u32, bytes[76..80], .little) };
        if (value.sequence == 0 or value.total == 0 or value.total > 16 * 1024 * 1024 or value.next > value.total) return error.InvalidRestoreSourceCheckpoint;
        return value;
    }
};

pub const Result = struct {
    next: u32,
    frame: ?[]u8 = null,
    pub fn deinit(self: Result, alloc: std.mem.Allocator) void {
        if (self.frame) |bytes| alloc.free(bytes);
    }
};

/// Return a resumable donor offset without reading frame payload. A complete
/// frame returns its final byte so a bounded duplicate chunk can resume the
/// remaining transformed effects after restart, without retransferring it.
pub fn resumeOffset(alloc: std.mem.Allocator, io: std.Io, root: []const u8, scope: staging.Scope, after: u64) !u32 {
    const path = try std.fmt.allocPrint(alloc, "{s}/rewrite-frame.receipt", .{root});
    defer alloc.free(path);
    const raw = native.readFileAlloc(alloc, io, path, 113) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer alloc.free(raw);
    const receipt = try Receipt.decode(raw);
    if (!std.mem.eql(u8, &receipt.scope, &scope.digest())) return error.RestoreStagingScopeChanged;
    if (receipt.sequence <= after) return 0;
    if (receipt.sequence != try std.math.add(u64, after, 1)) return error.RestoreStagingProgressChanged;
    return if (receipt.next == receipt.total) receipt.next - 1 else receipt.next;
}

/// Caller holds the existing source-generation transition lock, shared with
/// terminal cleanup. One acknowledged chunk performs at most64KiB payload IO;
/// completing a frame verifies at most the fixed16MiB retained-frame ceiling.
pub fn receive(alloc: std.mem.Allocator, io: std.Io, root: []const u8, scope: staging.Scope, after: u64, chunk: contract.TailChunk) !Result {
    try chunk.validate();
    const binding = scope.rewrite orelse return error.InvalidRestoreStagingCommand;
    if (!std.mem.eql(u8, &chunk.pin, &binding.retained_pin) or chunk.sequence != try std.math.add(u64, after, 1)) return error.RestoreStagingProgressChanged;
    try fs.createDirPathPortable(io, root);
    const state_path = try std.fmt.allocPrint(alloc, "{s}/rewrite-frame.receipt", .{root});
    defer alloc.free(state_path);
    const path = try std.fmt.allocPrint(alloc, "{s}/rewrite-frame.ref3", .{root});
    defer alloc.free(path);
    const raw = native.readFileAlloc(alloc, io, state_path, 113) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    defer if (raw) |bytes| alloc.free(bytes);
    var receipt: Receipt = .{ .scope = scope.digest(), .sequence = chunk.sequence, .digest = chunk.frame_digest, .total = chunk.total, .next = 0 };
    if (raw) |bytes| {
        const previous = try Receipt.decode(bytes);
        if (!std.mem.eql(u8, &previous.scope, &receipt.scope) or previous.sequence > chunk.sequence) return error.RestoreStagingScopeChanged;
        if (previous.sequence == chunk.sequence) {
            if (previous.total != chunk.total or !std.mem.eql(u8, &previous.digest, &chunk.frame_digest)) return error.RestoreStagingScopeChanged;
            receipt = previous;
        }
    }
    const file = try fs.createFilePortable(io, path, .{ .read = true, .truncate = false });
    defer file.close(io);
    if (receipt.next != 0 and (try file.stat(io)).size < receipt.next) return error.InvalidRestoreSourceCheckpoint;
    if (chunk.offset > receipt.next) return .{ .next = receipt.next };
    if (chunk.offset < receipt.next) {
        if (chunk.data.len > receipt.next - chunk.offset) return error.RestoreStagingProgressChanged;
        const existing = try alloc.alloc(u8, chunk.data.len);
        defer alloc.free(existing);
        if (try file.readPositionalAll(io, existing, chunk.offset) != existing.len or !std.mem.eql(u8, existing, chunk.data)) return error.InvalidRestoreSourceCheckpoint;
    } else {
        // Ignore an unacknowledged torn suffix from a previous process.
        try file.setLength(io, receipt.next);
        try file.writePositionalAll(io, chunk.data, chunk.offset);
        try file.sync(io);
        receipt.next += @intCast(chunk.data.len);
        const pending = try std.fmt.allocPrint(alloc, "{s}.pending", .{state_path});
        defer alloc.free(pending);
        _ = try native.writeFileDurable(io, pending, &receipt.encode());
        try std.Io.Dir.rename(.cwd(), pending, .cwd(), state_path, io);
        try fs.syncDirPortable(io, root);
    }
    if (receipt.next != receipt.total) return .{ .next = receipt.next };
    const frame = try native.readFileAlloc(alloc, io, path, 16 * 1024 * 1024);
    errdefer alloc.free(frame);
    if (frame.len != receipt.total) return error.InvalidRestoreSourceCheckpoint;
    const verified = try @import("retained_effects.zig").Reader.init(frame, receipt.sequence);
    if (!std.mem.eql(u8, &verified.frame_digest, &receipt.digest)) return error.RetainedEffectsCorrupt;
    return .{ .next = receipt.next, .frame = frame };
}
