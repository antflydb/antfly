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
const admission = @import("../common/read_admission.zig");

/// Preserve the existing intent envelope: plain, delete, or JSON plus prepared
/// row. The caller retains the original owner and input buffers synchronously.
pub fn encode(allocator: std.mem.Allocator, intent: anytype, control: ?admission.Port) ![]u8 {
    if (control) |owner| try owner.checkpoint();
    const value = intent.value;
    const row = if (value != null) intent.prepared_row else null;
    const header: usize = if (row != null) 5 else 1;
    if (row != null and value.?.len > std.math.maxInt(u32)) return error.TransactionTooLarge;
    const value_length = if (value) |bytes| bytes.len else 0;
    const row_length = if (row) |bytes| bytes.len else 0;
    const total = std.math.add(usize, std.math.add(usize, header, value_length) catch return error.TransactionTooLarge, row_length) catch return error.TransactionTooLarge;
    const output = try allocator.alloc(u8, total);
    errdefer allocator.free(output);
    output[0] = if (value == null) 1 else if (row != null) 2 else 0;
    if (row != null) std.mem.writeInt(u32, output[1..5], @intCast(value_length), .little);
    if (value) |bytes| try copy(output[header..][0..value_length], bytes, control);
    if (row) |bytes| try copy(output[header + value_length ..], bytes, control);
    if (control) |owner| try owner.checkpoint();
    return output;
}

fn copy(destination: []u8, source: []const u8, control: ?admission.Port) !void {
    var offset: usize = 0;
    while (offset < source.len) {
        if (control) |owner| try owner.checkpoint();
        const end = offset + @min(source.len - offset, 4096);
        @memcpy(destination[offset..end], source[offset..end]);
        offset = end;
    }
}

/// Match retained JSON cost without materializing another encoded payload.
/// Ordinary byte-preserving documents avoid JSON parsing as before.
pub fn payloadBytes(allocator: std.mem.Allocator, value: []const u8, control: ?admission.Port) !u64 {
    if (control) |owner| try owner.checkpoint();
    const special = try contains(value, "_edges", control) or try contains(value, "_embeddings", control) or try contains(value, "\\", control);
    if (!special) {
        if (control) |owner| try owner.checkpoint();
        return value.len;
    }
    const output = @import("../sql/cooperative_json_output.zig");
    const Scope = struct {
        original: admission.Port,
        failure: ?anyerror = null,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.original.checkpoint() catch |cause| {
                self.failure = cause;
                return cause;
            };
        }
    };
    var scope: Scope = .{ .original = control orelse undefined };
    const callback: output.Control = .{ .ptr = &scope, .check = Scope.check };
    var parsed = (if (control != null)
        @import("../sql/cooperative_json_source.zig").parse(std.json.Value, allocator, value, .{ .parse_numbers = false }, callback)
    else
        std.json.parseFromSlice(std.json.Value, allocator, value, .{ .parse_numbers = false })) catch |err| {
        if (scope.failure) |cause| return cause;
        return if (err == error.OutOfMemory) err else error.InvalidTxnRecord;
    };
    defer parsed.deinit();
    var measured: u64 = value.len;
    if (parsed.value == .object) {
        const count = if (control != null) blk: {
            var encoder: output.Encoder = undefined;
            encoder.init(allocator, callback, .{});
            defer encoder.deinit();
            break :blk try encoder.countValue(parsed.value);
        } else blk: {
            var buffer: [1024]u8 = undefined;
            var counter = std.Io.Writer.Discarding.init(&buffer);
            try std.json.Stringify.value(parsed.value, .{}, &counter.writer);
            break :blk counter.fullCount();
        };
        measured = @max(measured, count);
    }
    if (control) |owner| try owner.checkpoint();
    return measured;
}

fn contains(value: []const u8, needle: []const u8, control: ?admission.Port) !bool {
    if (control == null) return std.mem.indexOf(u8, value, needle) != null;
    var offset: usize = 0;
    while (offset < value.len) {
        try control.?.checkpoint();
        const end = offset + @min(value.len - offset, 4096);
        const overlap = end + @min(value.len - end, needle.len - 1);
        if (std.mem.indexOf(u8, value[offset..overlap], needle) != null) return true;
        offset = end;
    }
    return false;
}

pub fn encodedComponentBytes(bytes: []const u8, control: ?admission.Port) !usize {
    if (control) |owner| try owner.checkpoint();
    var length = std.math.add(usize, bytes.len, 2) catch return error.TransactionTooLarge;
    var offset: usize = 0;
    while (offset < bytes.len) {
        if (control) |owner| try owner.checkpoint();
        const end = offset + @min(bytes.len - offset, 4096);
        for (bytes[offset..end]) |byte| if (byte == 0) {
            length = std.math.add(usize, length, 1) catch return error.TransactionTooLarge;
        };
        offset = end;
    }
    if (control) |owner| try owner.checkpoint();
    return length;
}

/// Preserve byte-identical transaction keys while copying under the existing
/// caller authority; all borrowed parts remain live until this call returns.
pub fn concatKey(allocator: std.mem.Allocator, parts: []const []const u8, control: ?admission.Port) ![]u8 {
    if (control) |owner| try owner.checkpoint();
    var total: usize = 0;
    for (parts, 0..) |part, index| {
        if (index % 64 == 0) if (control) |owner| try owner.checkpoint();
        total = std.math.add(usize, total, part.len) catch return error.TransactionTooLarge;
    }
    const output = try allocator.alloc(u8, total);
    errdefer allocator.free(output);
    var offset: usize = 0;
    for (parts) |part| {
        if (control) |owner| try owner.checkpoint();
        try copy(output[offset..][0..part.len], part, control);
        offset += part.len;
    }
    if (control) |owner| try owner.checkpoint();
    return output;
}

pub fn hasPrefix(bytes: []const u8, prefix: []const u8, control: ?admission.Port) !bool {
    if (control) |owner| try owner.checkpoint();
    if (bytes.len < prefix.len) return false;
    var offset: usize = 0;
    while (offset < prefix.len) {
        if (control) |owner| try owner.checkpoint();
        const end = offset + @min(prefix.len - offset, 4096);
        if (!std.mem.eql(u8, bytes[offset..end], prefix[offset..end])) return false;
        offset = end;
    }
    if (control) |owner| try owner.checkpoint();
    return true;
}

pub fn userKey(allocator: std.mem.Allocator, key: []const u8, kind: u8, control: ?admission.Port) ![]u8 {
    const keys = @import("internal_keys.zig");
    const length = try encodedComponentBytes(key, control);
    const output = try allocator.alloc(u8, std.math.add(usize, length, 2) catch return error.TransactionTooLarge);
    errdefer allocator.free(output);
    output[0] = keys.user_namespace;
    var written: usize = 1;
    for (key, 0..) |byte, index| {
        if (index % 4096 == 0) if (control) |owner| try owner.checkpoint();
        output[written] = byte;
        written += 1;
        if (byte == 0) {
            output[written] = 0xff;
            written += 1;
        }
    }
    output[written] = 0;
    output[written + 1] = 0;
    output[written + 2] = kind;
    if (control) |owner| try owner.checkpoint();
    return output;
}

pub fn equalBytes(left: []const u8, right: []const u8, control: ?admission.Port) !bool {
    if (left.len != right.len) {
        if (control) |owner| try owner.checkpoint();
        return false;
    }
    return hasPrefix(left, right, control);
}

pub fn sha256(bytes: []const u8, control: ?admission.Port) ![32]u8 {
    if (control) |owner| try owner.checkpoint();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: usize = 0;
    while (offset < bytes.len) {
        if (control) |owner| try owner.checkpoint();
        const end = offset + @min(bytes.len - offset, 4096);
        hash.update(bytes[offset..end]);
        offset = end;
    }
    const digest = hash.finalResult();
    if (control) |owner| try owner.checkpoint();
    return digest;
}
