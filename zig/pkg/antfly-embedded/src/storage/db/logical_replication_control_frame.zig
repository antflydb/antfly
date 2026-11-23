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

//! Checksummed, bounded publication owner controls in the standby journal.
const std = @import("std");
const A = std.mem.Allocator;
const capture = @import("../../system_catalog/logical_replication_capture.zig");
const positions = @import("receipt_position.zig");

const magic = "ALPC\x02\x00\x00\x00";
const max_command_bytes = 32 * 1024;
const trailer_bytes = positions.Position.encoded_len + 32;

pub const Payload = struct {
    alloc: A,
    bytes: []u8,
    prefix_hash: std.crypto.hash.Blake3,
    namespace: @import("doc_identity_namespace.zig").Namespace,
    pub fn init(a: A, control: capture.Control, root: u128) !@This() {
        try control.validate();
        if (root == 0) return error.InvalidLogicalCaptureControl;
        const command = try std.json.Stringify.valueAlloc(a, control, .{});
        defer a.free(command);
        if (command.len > max_command_bytes) return error.InvalidLogicalCaptureControl;
        const bytes = try a.alloc(u8, 28 + command.len + trailer_bytes);
        @memcpy(bytes[0..8], magic);
        std.mem.writeInt(u128, bytes[8..24], root, .little);
        std.mem.writeInt(u32, bytes[24..28], @intCast(command.len), .little);
        @memcpy(bytes[28..][0..command.len], command);
        @memset(bytes[28 + command.len ..], 0);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update(bytes[0 .. 28 + command.len]);
        return .{ .alloc = a, .bytes = bytes, .prefix_hash = hash, .namespace = control.binding.namespace };
    }
    pub fn finalize(self: *@This(), position: positions.Native) ![]const u8 {
        try position.validate();
        if (!position.namespace.eql(self.namespace)) return error.IdentityNamespaceMismatch;
        const offset = self.bytes.len - trailer_bytes;
        const encoded = try (positions.Position{ .native = position }).encode();
        @memcpy(self.bytes[offset..][0..positions.Position.encoded_len], &encoded);
        var hash = self.prefix_hash;
        hash.update(&encoded);
        hash.final(self.bytes[self.bytes.len - 32 ..][0..32]);
        return self.bytes;
    }
    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.bytes);
    }
};
pub const Frame = struct {
    control: capture.Control,
    root: u128,
    position: positions.Native,
    pub fn isFrame(bytes: []const u8) bool {
        return bytes.len >= magic.len and std.mem.eql(u8, bytes[0..magic.len], magic);
    }
    pub fn decode(a: A, bytes: []const u8) !@This() {
        if (!isFrame(bytes) or bytes.len < 28 + trailer_bytes or bytes.len > 28 + max_command_bytes + trailer_bytes) return error.InvalidLogicalCaptureControl;
        const len = std.mem.readInt(u32, bytes[24..28], .little);
        if (len != bytes.len - 28 - trailer_bytes) return error.InvalidLogicalCaptureControl;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[bytes.len - 32 ..])) return error.InvalidLogicalCaptureControl;
        const control = try std.json.parseFromSliceLeaky(capture.Control, a, bytes[28..][0..len], .{ .ignore_unknown_fields = false });
        try control.validate();
        const canonical = try std.json.Stringify.valueAlloc(a, control, .{});
        defer a.free(canonical);
        if (!std.mem.eql(u8, canonical, bytes[28..][0..len])) return error.InvalidLogicalCaptureControl;
        const position = try positions.Position.decode(bytes[bytes.len - trailer_bytes ..][0..positions.Position.encoded_len]);
        const root = std.mem.readInt(u128, bytes[8..24], .little);
        if (position != .native or root == 0) return error.InvalidLogicalCaptureControl;
        try position.requireNamespace(control.binding.namespace);
        return .{ .control = control, .root = root, .position = position.native };
    }
};
