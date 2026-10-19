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
pub const State = struct { legacy: ?bool = null, views: ?*@import("source_views.zig").Store = null };
pub const Prepared = struct { lines: []Line, index: source.Index };
const limit = 256 * 1024;
fn cache(directory: []const u8) @import("artifact_cache.zig").Cache {
    return .{ .directory = directory, .namespace = "source-v3", .buckets = 1024, .limit = limit };
}
const parser_digest = @import("grep_cache_options").parser_digest;
fn identity(path: []const u8, delimiter: u8, crlf: bool, scope: ?[32]u8, content_digest: [32]u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-source-preparation-v2");
    hash.update(&parser_digest);
    var path_len: [8]u8 = undefined;
    std.mem.writeInt(u64, &path_len, path.len, .little);
    hash.update(&path_len);
    hash.update(path);
    hash.update(&.{ delimiter, @intFromBool(crlf) });
    hash.update(if (scope) |*value| value else &@as([32]u8, @splat(0)));
    hash.update(&content_digest);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
pub fn get(a: A, io: std.Io, o: *const @import("options.zig").Options, path: []const u8, bytes: []const u8, scope: ?[32]u8, hit: *bool, state: *State) !Prepared {
    return getHashed(a, io, o, path, bytes, scope, hit, state, null);
}
/// A captured immutable revision can supply its SHA-256 without a second
/// source-sized hashing pass. Callers must bind the digest to these exact bytes.
/// Disk offsets are still independently validated against content on every load.
pub fn getHashed(a: A, io: std.Io, o: *const @import("options.zig").Options, path: []const u8, bytes: []const u8, scope: ?[32]u8, hit: *bool, state: *State, captured: ?[32]u8) !Prepared {
    const delimiter: u8 = if (o.null_data) 0 else '\n';
    const enabled = !o.no_cache and o.cache_dir != null;
    const key = if (enabled) blk: {
        const digest = captured orelse digest: {
            var value: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(bytes, &value, .{});
            break :digest value;
        };
        break :blk identity(path, delimiter, o.crlf, scope, digest);
    } else @as([32]u8, @splat(0));
    hit.* = false;
    if (enabled) if (load(a, io, o, key, bytes, delimiter, o.crlf, !o.cached_only, state)) |prepared| {
        hit.* = true;
        return prepared;
    };
    const prepared = Prepared{ .lines = try lines(a, bytes, delimiter, o.crlf), .index = try source.parse(a, path, bytes) };
    if (enabled and !o.cached_only) store(a, io, o, key, prepared, state);
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
fn load(a: A, io: std.Io, o: *const @import("options.zig").Options, key: [32]u8, bytes: []const u8, delimiter: u8, crlf: bool, migrate: bool, state: *State) ?Prepared {
    const directory = o.cache_dir.?;
    var legacy = false;
    var promote = false;
    const record = (if (state.views) |views| views.loadPrepared(a, io, o, key) else null) orelse blk4: {
        promote = true;
        break :blk4 (if (state.views) |views| views.loadLegacyPrepared(a, io, o, key) orelse views.load(a, io, cache(directory), key) else cache(directory).load(a, io, key));
    } orelse blk: {
        if (state.legacy == null) state.legacy = @import("artifact_cache.zig").LegacyPresence.scan(io, directory).sources;
        if (!state.legacy.?) return null;
        const old: @import("artifact_cache.zig").Legacy = .{ .directory = directory, .namespace = "source-v2", .buckets = 1024, .limit = limit };
        const body = old.load(a, io, key) orelse return null;
        legacy = true;
        break :blk body;
    };
    var reader = @import("cache_codec.zig").Reader{ .bytes = record };
    const value = reader.get(Prepared, a) catch return null;
    if (reader.bytes.len != 0) return null;
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
    // Large V3 entries already occupy their final tier. Promotion must not
    // rewrite an unchanged bucket on every warm read.
    if (migrate and (legacy or (promote and state.views != null and record.len <= @import("source_views.zig").Store.prepared_body_limit))) store(a, io, o, key, value, state);
    return value;
}
fn store(a: A, io: std.Io, o: *const @import("options.zig").Options, key: [32]u8, value: Prepared, state: *State) void {
    var budget = @import("antfly_regex").Budget{ .backing = a, .limit = limit };
    const ba = budget.allocator();
    const body = @import("cache_codec.zig").encode(ba, value) catch return;
    defer ba.free(body);
    if (state.views) |views| if (views.storePrepared(a, io, o, key, body)) return;
    cache(o.cache_dir.?).store(a, io, key, body);
}
