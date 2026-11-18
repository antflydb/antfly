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

//! Immutable restore inputs live beside the compact mutable job. Admission
//! writes both in one transaction. Readers authenticate the inputs before use.
const std = @import("std");
pub const version: u32 = 6;
pub const suffix = ".inputs";
pub const max_bytes: usize = 64 * 1024 * 1024;

pub const Split = struct {
    record: []u8,
    inputs: ?[]u8 = null,

    pub fn deinit(self: Split, alloc: std.mem.Allocator) void {
        alloc.free(self.record);
        if (self.inputs) |value| alloc.free(value);
    }
};

pub fn digest(bytes: []const u8) [64]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
    return std.fmt.bytesToHex(hash, .lower);
}

pub fn packAdmission(alloc: std.mem.Allocator, encoded: []const u8) ![]u8 {
    const parts = try split(alloc, encoded);
    defer parts.deinit(alloc);
    const inputs = parts.inputs orelse return alloc.dupe(u8, encoded);
    if (inputs.len < 64 * 1024) return alloc.dupe(u8, encoded);
    var compressed = try std.Io.Writer.Allocating.initCapacity(alloc, 4096);
    defer compressed.deinit();
    const history = try alloc.alloc(u8, std.compress.flate.max_window_len);
    defer alloc.free(history);
    var compressor = try std.compress.flate.Compress.init(&compressed.writer, history, .zlib, .default);
    try compressor.writer.writeAll(inputs);
    try compressor.finish();
    const base64 = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(compressed.written().len));
    defer alloc.free(base64);
    _ = std.base64.standard.Encoder.encode(base64, compressed.written());
    var tree = try std.json.parseFromSlice(std.json.Value, alloc, parts.record, .{});
    defer tree.deinit();
    try tree.value.object.put(tree.arena.allocator(), "cluster_binding_deflate", .{ .string = base64 });
    return std.json.Stringify.valueAlloc(alloc, tree.value, .{});
}

pub fn split(alloc: std.mem.Allocator, encoded: []const u8) !Split {
    var tree = std.json.parseFromSlice(std.json.Value, alloc, encoded, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return .{ .record = try alloc.dupe(u8, encoded) },
    };
    defer tree.deinit();
    if (tree.value != .object) return .{ .record = try alloc.dupe(u8, encoded) };
    const v = tree.value.object.get("format_version") orelse return .{ .record = try alloc.dupe(u8, encoded) };
    if (v != .integer or v.integer != version) return .{ .record = try alloc.dupe(u8, encoded) };
    if (tree.value.object.get("cluster_binding_deflate")) |compressed_input| {
        if (compressed_input != .string or tree.value.object.get("cluster_binding") != null) return error.CorruptRestoreJobStore;
        const compressed = try alloc.alloc(u8, try std.base64.standard.Decoder.calcSizeForSlice(compressed_input.string));
        defer alloc.free(compressed);
        try std.base64.standard.Decoder.decode(compressed, compressed_input.string);
        var reader = std.Io.Reader.fixed(compressed);
        const history = try alloc.alloc(u8, std.compress.flate.max_window_len);
        defer alloc.free(history);
        var decoder = std.compress.flate.Decompress.init(&reader, .zlib, history);
        const bytes = try decoder.reader.allocRemaining(alloc, .limited(max_bytes));
        errdefer alloc.free(bytes);
        const expected = tree.value.object.get("cluster_binding_sha256") orelse return error.CorruptRestoreJobStore;
        if (expected != .string or !std.mem.eql(u8, expected.string, &digest(bytes))) return error.CorruptRestoreJobStore;
        _ = tree.value.object.orderedRemove("cluster_binding_deflate");
        return .{ .record = try std.json.Stringify.valueAlloc(alloc, tree.value, .{}), .inputs = bytes };
    }
    const binding = tree.value.object.get("cluster_binding") orelse .null;
    if (binding == .null) return .{ .record = try alloc.dupe(u8, encoded) };
    const bytes = try std.json.Stringify.valueAlloc(alloc, binding, .{ .emit_null_optional_fields = false });
    errdefer alloc.free(bytes);
    if (bytes.len > max_bytes) return error.RestoreJobRecordTooLarge;
    const hash = digest(bytes);
    _ = tree.value.object.orderedRemove("cluster_binding");
    try tree.value.object.put(tree.arena.allocator(), "cluster_binding_sha256", .{ .string = &hash });
    return .{ .record = try std.json.Stringify.valueAlloc(alloc, tree.value, .{}), .inputs = bytes };
}

pub fn validateReference(alloc: std.mem.Allocator, next: []const u8, previous: ?[]const u8) !void {
    const Header = struct { cluster_binding_sha256: ?[]const u8 = null };
    var wanted = std.json.parseFromSlice(Header, alloc, next, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return,
    };
    defer wanted.deinit();
    const hash = wanted.value.cluster_binding_sha256 orelse return;
    const current = previous orelse return error.CorruptRestoreJobStore;
    var existing = try std.json.parseFromSlice(Header, alloc, current, .{ .ignore_unknown_fields = true });
    defer existing.deinit();
    if (!std.mem.eql(u8, hash, existing.value.cluster_binding_sha256 orelse return error.CorruptRestoreJobStore))
        return error.CorruptRestoreJobStore;
}

pub fn hydrate(alloc: std.mem.Allocator, encoded: []const u8, inputs: ?[]const u8) ![]u8 {
    const Header = struct { format_version: u32, cluster_binding_sha256: ?[]const u8 = null };
    var header = std.json.parseFromSlice(Header, alloc, encoded, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return alloc.dupe(u8, encoded),
    };
    defer header.deinit();
    const expected = header.value.cluster_binding_sha256 orelse return alloc.dupe(u8, encoded);
    if (header.value.format_version != version) return error.CorruptRestoreJobStore;
    const bytes = inputs orelse return error.CorruptRestoreJobStore;
    if (bytes.len > max_bytes or !std.mem.eql(u8, expected, &digest(bytes))) return error.CorruptRestoreJobStore;
    var tree = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer tree.deinit();
    var binding = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{});
    defer binding.deinit();
    try tree.value.object.put(tree.arena.allocator(), "cluster_binding", binding.value);
    return std.json.Stringify.valueAlloc(alloc, tree.value, .{});
}

test "restore runtime store persists checkpoints and requeues interrupted work authenticates immutable inputs" {
    const a = std.testing.allocator;
    const original = "{\"format_version\":6,\"cluster_binding\":{\"child_pins\":\"AQ==\"},\"phase\":\"queued\"}";
    const parts = try split(a, original);
    defer parts.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, parts.record, "AQ==") == null);
    try validateReference(a, parts.record, parts.record);
    try std.testing.expectError(error.CorruptRestoreJobStore, validateReference(a, parts.record, null));
    const padding = try a.alloc(u8, 4 * 1024 * 1024);
    defer a.free(padding);
    @memset(padding, 'a');
    const large = try std.json.Stringify.valueAlloc(a, .{ .format_version = version, .cluster_binding = .{ .recovery_manifests_json = padding } }, .{});
    defer a.free(large);
    const compressed_input = try packAdmission(a, large);
    defer a.free(compressed_input);
    try std.testing.expect(compressed_input.len < 64 * 1024);
    const unpacked = try split(a, compressed_input);
    defer unpacked.deinit(a);
    try std.testing.expect(unpacked.inputs.?.len > padding.len);
    const full = try hydrate(a, parts.record, parts.inputs);
    defer a.free(full);
    try std.testing.expect(std.mem.indexOf(u8, full, "AQ==") != null);
    try std.testing.expectError(error.CorruptRestoreJobStore, hydrate(a, parts.record, "{}"));
    try std.testing.expectError(error.CorruptRestoreJobStore, hydrate(a, parts.record, null));
}
