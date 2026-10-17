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

//! Content-addressed, query-independent source preparation. Cache artifacts are
//! bounded, private, atomic and disposable; invalid entries fall back to parsing.
const std = @import("std");
const source = @import("source.zig");
const A = std.mem.Allocator;
pub const Line = struct { start: usize, end: usize };
pub const Prepared = struct { lines: []Line, index: source.Index };
const limit = 1024 * 1024;
const slots = 1024;
const parser_digest = @import("grep_cache_options").parser_digest;
fn identity(path: []const u8, bytes: []const u8, delimiter: u8, crlf: bool, scope: ?[32]u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-source-preparation-v1");
    hash.update(&parser_digest);
    var path_len: [8]u8 = undefined;
    std.mem.writeInt(u64, &path_len, path.len, .little);
    hash.update(&path_len);
    hash.update(path);
    hash.update(&.{ delimiter, @intFromBool(crlf) });
    hash.update(if (scope) |*value| value else &@as([32]u8, @splat(0)));
    hash.update(bytes);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
pub fn get(a: A, io: std.Io, o: *const @import("options.zig").Options, path: []const u8, bytes: []const u8, scope: ?[32]u8, hit: *bool) !Prepared {
    const delimiter: u8 = if (o.null_data) 0 else '\n';
    const enabled = !o.no_cache and o.cache_dir != null;
    const key = if (enabled) identity(path, bytes, delimiter, o.crlf, scope) else @as([32]u8, @splat(0));
    hit.* = false;
    if (enabled) if (load(a, io, o.cache_dir.?, key, bytes, delimiter, o.crlf)) |prepared| {
        hit.* = true;
        return prepared;
    };
    const prepared = Prepared{ .lines = try lines(a, bytes, delimiter, o.crlf), .index = try source.parse(a, path, bytes) };
    if (enabled and !o.cached_only) store(a, io, o.cache_dir.?, key, prepared);
    return prepared;
}
pub fn lines(a: A, bytes: []const u8, delimiter: u8, crlf: bool) ![]Line {
    var out: std.ArrayList(Line) = .empty;
    errdefer out.deinit(a);
    var start: usize = 0;
    for (bytes, 0..) |byte, i| if (byte == delimiter) {
        try out.append(a, .{ .start = start, .end = if (crlf and i > start and bytes[i - 1] == '\r') i - 1 else i });
        start = i + 1;
    };
    if (start < bytes.len) try out.append(a, .{ .start = start, .end = bytes.len });
    return out.toOwnedSlice(a);
}
fn filename(a: A, directory: []const u8, key: [32]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}/source-v1-{x:0>3}", .{ directory, std.mem.readInt(u16, key[0..2], .little) % slots });
}
fn load(a: A, io: std.Io, directory: []const u8, key: [32]u8, bytes: []const u8, delimiter: u8, crlf: bool) ?Prepared {
    const path = filename(a, directory, key) catch return null;
    defer a.free(path);
    const record = std.Io.Dir.cwd().readFileAlloc(io, path, a, .limited(limit)) catch return null;
    // Returned strings borrow the record/parsed allocations in the caller arena.
    if (record.len < 64 or !std.mem.eql(u8, record[0..32], &key)) return null;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(record[64..], &digest, .{});
    if (!std.mem.eql(u8, record[32..64], &digest)) return null;
    const value = std.json.parseFromSliceLeaky(Prepared, a, record[64..], .{ .duplicate_field_behavior = .@"error" }) catch return null;
    // Verify line boundaries in one monotonic pass. No source-size x region
    // validation, and no cached offset can escape this exact content revision.
    var start: usize = 0;
    var index: usize = 0;
    for (bytes, 0..) |byte, i| if (byte == delimiter) {
        const end = if (crlf and i > start and bytes[i - 1] == '\r') i - 1 else i;
        if (index >= value.lines.len or value.lines[index].start != start or value.lines[index].end != end) return null;
        index += 1;
        start = i + 1;
    };
    if (start < bytes.len) {
        if (index >= value.lines.len or value.lines[index].start != start or value.lines[index].end != bytes.len) return null;
        index += 1;
    }
    if (index != value.lines.len) return null;
    for (value.index.declarations, 0..) |decl, i| {
        if (decl.start < 1 or decl.end < decl.start or decl.end > value.lines.len or decl.label_line < decl.start or decl.label_line > decl.end or decl.start_byte > decl.end_byte or decl.end_byte > bytes.len) return null;
        if (decl.parent) |parent| if (parent >= value.index.declarations.len or parent == i) return null;
    }
    for (value.index.calls) |call| if (call.line < 1 or call.line > value.lines.len or call.column < 1 or call.start_byte > call.end_byte or call.end_byte > bytes.len) return null;
    return value;
}
fn store(a: A, io: std.Io, directory: []const u8, key: [32]u8, value: Prepared) void {
    var budget = @import("antfly_regex").Budget{ .backing = a, .limit = limit };
    const ba = budget.allocator();
    const body = std.json.Stringify.valueAlloc(ba, value, .{}) catch return;
    defer ba.free(body);
    if (body.len > limit - 64) return;
    const path = filename(a, directory, key) catch return;
    defer a.free(path);
    std.Io.Dir.cwd().createDirPath(io, directory) catch return;
    var nonce: [12]u8 = undefined;
    io.random(&nonce);
    const temporary = std.fmt.allocPrint(a, "{s}.{s}.tmp", .{ path, std.fmt.bytesToHex(nonce, .lower) }) catch return;
    defer a.free(temporary);
    defer std.Io.Dir.cwd().deleteFile(io, temporary) catch {};
    {
        const file = std.Io.Dir.cwd().createFile(io, temporary, .{ .exclusive = true, .permissions = .fromMode(0o600) }) catch return;
        defer file.close(io);
        var header: [64]u8 = undefined;
        header[0..32].* = key;
        std.crypto.hash.sha2.Sha256.hash(body, header[32..64], .{});
        file.writeStreamingAll(io, &header) catch return;
        file.writeStreamingAll(io, body) catch return;
    }
    std.Io.Dir.cwd().rename(temporary, .cwd(), path, io) catch {};
}
