// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Bounded, single-use CLI upload of an existing native KV checkpoint.
const std = @import("std");
const httpx = @import("httpx");
const wire = @import("../server/native_kv_wire.zig");

const json_headers = [_][2][]const u8{.{ "Content-Type", wire.content_type }};
const stream_headers = [_][2][]const u8{ .{ "Content-Type", wire.content_type }, .{ "Accept", "text/event-stream" } };

pub const Upload = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    file: std.Io.File,
    prefix: []u8,
    prefix_offset: usize = 0,
    file_offset: u64 = 0,
    file_bytes: u64,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, path: []const u8, metadata: []const u8) !Upload {
        const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only, .allow_directory = false, .follow_symlinks = false });
        return initFile(allocator, io, file, metadata);
    }

    fn initFile(allocator: std.mem.Allocator, io: std.Io, file: std.Io.File, metadata: []const u8) !Upload {
        errdefer file.close(io);
        const stat = try file.stat(io);
        const limits: wire.Limits = .{};
        if (stat.kind != .file or stat.size < 184 or stat.size > limits.max_checkpoint_bytes)
            return error.InvalidNativeCheckpointFile;
        const Prefix = struct {
            allocator: std.mem.Allocator,
            bytes: std.ArrayList(u8) = .empty,
            pub fn writeAll(self: *@This(), bytes: []const u8) !void {
                try self.bytes.appendSlice(self.allocator, bytes);
            }
        };
        const Control = struct {
            pub fn check(_: @This()) !void {}
        };
        var prefix: Prefix = .{ .allocator = allocator };
        defer prefix.bytes.deinit(allocator);
        _ = try wire.writeStart(&prefix, metadata, @intCast(stat.size), limits, Control{});
        return .{ .allocator = allocator, .io = io, .file = file, .prefix = try prefix.bytes.toOwnedSlice(allocator), .file_bytes = stat.size };
    }

    pub fn deinit(self: *Upload) void {
        self.file.close(self.io);
        self.allocator.free(self.prefix);
        self.* = undefined;
    }

    pub fn requestOptions(self: *Upload, stream: bool) httpx.RequestOptions {
        return .{ .body_stream = .{ .context = self, .read = read }, .headers = if (stream) &stream_headers else &json_headers, .timeout_ms = 300_000, .max_retries = 0, .follow_redirects = false };
    }

    fn read(raw: *anyopaque, output: []u8) !usize {
        const self: *Upload = @ptrCast(@alignCast(raw));
        if (output.len == 0) return 0;
        if (self.prefix_offset < self.prefix.len) {
            const count = @min(output.len, self.prefix.len - self.prefix_offset);
            @memcpy(output[0..count], self.prefix[self.prefix_offset..][0..count]);
            self.prefix_offset += count;
            return count;
        }
        if (self.file_offset == self.file_bytes) return 0;
        const count: usize = @intCast(@min(output.len, self.file_bytes - self.file_offset));
        if (try self.file.readPositionalAll(self.io, output[0..count], self.file_offset) != count)
            return error.NativeCheckpointFileTruncated;
        self.file_offset += count;
        return count;
    }
};

/// requestToWriter supports a streamed upload and a bounded buffered JSON
/// response as well as the ordinary incremental SSE output sink.
pub const JsonResponse = struct {
    allocator: std.mem.Allocator,
    bytes: std.ArrayList(u8) = .empty,
    pub fn writeAll(self: *JsonResponse, bytes: []const u8) !void {
        if (bytes.len > 2 * 1024 * 1024 - self.bytes.items.len) return error.GenerateResponseTooLarge;
        try self.bytes.appendSlice(self.allocator, bytes);
    }
    pub fn deinit(self: *JsonResponse) void {
        self.bytes.deinit(self.allocator);
    }
};

test "context CLI native checkpoint streams one framed immutable file without upload copies or replay" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var data: [1025]u8 = undefined;
    for (&data, 0..) |*byte, index| byte.* = @truncate(index * 17 + 3);
    try tmp.dir.writeFile(io, .{ .sub_path = "state.kv", .data = &data });
    const file = try tmp.dir.openFile(io, "state.kv", .{ .mode = .read_only });
    const metadata = "{\"model\":\"consumed\",\"stream\":true}";
    var upload = try Upload.initFile(a, io, file, metadata);
    defer upload.deinit();
    const options = upload.requestOptions(true);
    try std.testing.expect(options.json == null and options.body == null and options.borrowed_body == null);
    try std.testing.expectEqual(@as(?u32, 0), options.max_retries);
    try std.testing.expectEqual(@as(?bool, false), options.follow_redirects);
    var framed: std.ArrayList(u8) = .empty;
    defer framed.deinit(a);
    var bytes: [7]u8 = undefined;
    while (true) {
        const count = try options.body_stream.?.read(options.body_stream.?.context, &bytes);
        if (count == 0) break;
        try framed.appendSlice(a, bytes[0..count]);
    }
    try std.testing.expectEqualStrings("AFATT001", framed.items[0..8]);
    try std.testing.expectEqual(@as(u64, metadata.len), std.mem.readInt(u64, framed.items[8..16], .little));
    try std.testing.expectEqual(@as(u64, data.len), std.mem.readInt(u64, framed.items[32..40], .little));
    try std.testing.expectEqualSlices(u8, metadata, framed.items[40..][0..metadata.len]);
    try std.testing.expectEqualSlices(u8, wire.checkpoint_mime, framed.items[40 + metadata.len ..][0..wire.checkpoint_mime.len]);
    try std.testing.expectEqualSlices(u8, &data, framed.items[upload.prefix.len..]);
    try std.testing.expectEqual(@as(usize, 0), try options.body_stream.?.read(options.body_stream.?.context, &bytes));
}

test "context CLI native checkpoint rejects a short file and detects truncation during upload" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "short.kv", .data = "short" });
    try std.testing.expectError(error.InvalidNativeCheckpointFile, Upload.initFile(a, io, try tmp.dir.openFile(io, "short.kv", .{ .mode = .read_only }), "{}"));
    const data: [256]u8 = @splat(1);
    try tmp.dir.writeFile(io, .{ .sub_path = "state.kv", .data = &data });
    var upload = try Upload.initFile(a, io, try tmp.dir.openFile(io, "state.kv", .{ .mode = .read_only }), "{}");
    defer upload.deinit();
    try tmp.dir.writeFile(io, .{ .sub_path = "state.kv", .data = "truncated" });
    var bytes: [4096]u8 = undefined;
    _ = try Upload.read(&upload, &bytes);
    try std.testing.expectError(error.NativeCheckpointFileTruncated, Upload.read(&upload, &bytes));
}
