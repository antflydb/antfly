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

//! Versioned source identity shared by lexical and decision result events.
const std = @import("std");
pub const Provenance = struct {
    backend: enum { local, antfly } = .local,
    uri: ?[]const u8 = null,
    table: ?[]const u8 = null,
    document_id: ?[]const u8 = null,
    revision: ?[]const u8 = null,
    indexed_at: ?i64 = null,
    observed_at: ?i64 = null,
    snapshot: ?[]const u8 = null,
    freshness: enum { live, indexed, snapshot } = .live,
    metadata: ?std.json.Value = null,
    source_regions: ?std.json.Value = null,
};
pub const Document = struct {
    path: []const u8,
    content: []const u8,
    evidence: []const @import("antfly_source_evidence").Unit = &.{},
    evidence_loader: ?@import("antfly_source_evidence").Loader = null,
    cache_scope: ?[32]u8 = null,
    provenance: Provenance,
    modified_at: ?i64 = null,
    created_at: ?i64 = null,
    accessed_at: ?i64 = null,
    pub fn parse(value: std.json.Value, table: []const u8, id: []const u8, snapshot: ?[]const u8, now: i64) !Document {
        return parseSelected(value, table, id, snapshot, now, false);
    }
    pub fn parseListing(value: std.json.Value, table: []const u8, id: []const u8, snapshot: ?[]const u8, now: i64) !Document {
        return parseSelected(value, table, id, snapshot, now, true);
    }
    fn parseSelected(value: std.json.Value, table: []const u8, id: []const u8, snapshot: ?[]const u8, now: i64, listing: bool) !Document {
        const version = integer(field(value, "version")) orelse return error.UnsupportedDocumentVersion;
        if (version != 1 and version != 2) return error.UnsupportedDocumentVersion;
        if (version == 1 and field(value, "evidence") != null) return error.UnsupportedDocumentVersion;
        const path = try string(value, "path");
        if (path.len == 0 or path.len > 16384 or std.mem.indexOfScalar(u8, path, 0) != null) return error.InvalidDocumentPath;
        const content = if (listing) "" else try string(value, "content");
        if (!std.unicode.utf8ValidateSlice(content)) return error.InvalidDocumentEncoding;
        const uri = try string(value, "uri");
        if (uri.len > 16384 or !std.unicode.utf8ValidateSlice(uri) or std.mem.indexOfScalar(u8, uri, 0) != null) return error.InvalidSourceUri;
        const parsed = std.Uri.parse(uri) catch return error.InvalidSourceUri;
        if (parsed.scheme.len == 0 or std.ascii.eqlIgnoreCase(parsed.scheme, "data") or parsed.user != null or parsed.password != null) return error.InvalidSourceUri;
        const revision = try string(value, "revision");
        if (revision.len == 0 or revision.len > 4096) return error.InvalidDocumentRevision;
        const indexed_at = integer(field(value, "indexed_at")) orelse return error.InvalidIndexedTimestamp;
        const permissions = field(value, "permissions") orelse return error.MissingDocumentPermissions;
        if (permissions != .object or field(permissions, "public") == null or field(permissions, "public").? != .bool) return error.InvalidDocumentPermissions;
        const readers = field(permissions, "readers") orelse return error.InvalidDocumentPermissions;
        if (readers != .array) return error.InvalidDocumentPermissions;
        for (readers.array.items) |reader| if (reader != .string) return error.InvalidDocumentPermissions;
        if (field(value, "metadata")) |metadata| if (metadata != .object) return error.InvalidDocumentMetadata;
        if (!listing) if (field(value, "regions")) |regions| {
            if (regions != .array) return error.InvalidDocumentRegions;
            const line_count = std.mem.count(u8, content, "\n") + 1;
            for (regions.array.items) |region| {
                const start = integer(field(region, "start_line")) orelse return error.InvalidDocumentRegions;
                const end = integer(field(region, "end_line")) orelse return error.InvalidDocumentRegions;
                if (start < 1 or end < start or end > line_count) return error.InvalidDocumentRegions;
            }
        };
        return .{ .path = path, .content = content, .modified_at = integer(field(value, "modified_at")), .created_at = integer(field(value, "created_at")), .accessed_at = integer(field(value, "accessed_at")), .provenance = .{ .backend = .antfly, .uri = uri, .table = table, .document_id = id, .revision = revision, .indexed_at = indexed_at, .observed_at = now, .snapshot = snapshot, .freshness = .indexed, .metadata = field(value, "metadata"), .source_regions = field(value, "regions") } };
    }
};
pub fn allowed(value: std.json.Value, principals: []const []const u8) bool {
    const permissions = field(value, "permissions") orelse return false;
    if (field(permissions, "public")) |public| if (public == .bool and public.bool) return true;
    const readers = field(permissions, "readers") orelse return false;
    if (readers != .array) return false;
    for (readers.array.items) |reader| if (reader == .string) for (principals) |principal| {
        if (std.mem.eql(u8, principal, reader.string)) return true;
    };
    return false;
}
pub fn field(value: std.json.Value, name: []const u8) ?std.json.Value {
    return if (value == .object) value.object.get(name) else null;
}
pub fn at(value: std.json.Value, path: []const u8) ?std.json.Value {
    var current = value;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| current = field(current, part) orelse return null;
    return current;
}
pub fn integer(value: ?std.json.Value) ?i64 {
    return if (value) |v| switch (v) {
        .integer => v.integer,
        // Native stored-source views preserve number lexemes until encoding.
        .number_string => std.fmt.parseInt(i64, v.number_string, 10) catch null,
        else => null,
    } else null;
}
pub fn string(value: std.json.Value, name: []const u8) ![]const u8 {
    const v = field(value, name) orelse return error.InvalidDocument;
    return if (v == .string) v.string else error.InvalidDocument;
}

pub fn localUri(a: std.mem.Allocator, io: std.Io, path: []const u8) ![]u8 {
    const cwd = try std.process.currentPathAlloc(io, a);
    defer a.free(cwd);
    return localUriAt(a, cwd, path);
}

/// Search workers share one immutable invocation directory, avoiding getcwd
/// and its filesystem traversal for every JSON provenance record.
pub fn localUriAt(a: std.mem.Allocator, cwd: []const u8, path: []const u8) ![]u8 {
    // Most discovery paths are already normalized. Preserve resolveAlloc's
    // lexical semantics for dot components, repeated separators and Windows;
    // ordinary POSIX paths need neither a joined path nor a growable writer.
    if (comptime @import("builtin").os.tag != .windows) {
        if (normalizedPosix(path)) {
            if (std.fs.path.isAbsolute(path)) return encodeUri(a, &.{path});
            if (normalizedPosix(cwd) and std.fs.path.isAbsolute(cwd))
                return encodeUri(a, if (std.mem.eql(u8, cwd, "/")) &.{ cwd, path } else &.{ cwd, "/", path });
        }
    }
    const absolute = try std.fs.path.resolveAlloc(a, &.{ cwd, path });
    defer a.free(absolute);
    return encodeUri(a, &.{absolute});
}
fn normalizedPosix(path: []const u8) bool {
    if (path.len == 0) return false;
    if (std.mem.eql(u8, path, "/")) return true;
    var components = std.mem.splitScalar(u8, if (path[0] == '/') path[1..] else path, '/');
    while (components.next()) |part| {
        if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return false;
    }
    return true;
}
fn uriSafe(byte: u8) bool {
    return std.ascii.isAlphanumeric(byte) or switch (byte) {
        '/', '-', '_', '.', '~', ':' => true,
        else => false,
    };
}
fn encodeUri(a: std.mem.Allocator, parts: []const []const u8) ![]u8 {
    var size: usize = "file://".len;
    for (parts) |part| for (part) |byte| {
        size = try std.math.add(usize, size, if (uriSafe(byte)) 1 else 3);
    };
    const out = try a.alloc(u8, size);
    @memcpy(out[0..7], "file://");
    var cursor: usize = 7;
    const hex = "0123456789ABCDEF";
    for (parts) |part| for (part) |byte| {
        if (uriSafe(byte)) {
            out[cursor] = byte;
            cursor += 1;
        } else {
            out[cursor..][0..3].* = .{ '%', hex[byte >> 4], hex[byte & 15] };
            cursor += 3;
        }
    };
    return out;
}

test "local URI fast paths preserve resolved and percent encoded byte identities" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "/", "/tmp/work", "/tmp/a b", "/tmp/../work" }) |cwd| {
        for ([_][]const u8{ "src/file.txt", "/src/file.txt", ".", "", "./a", "../a", "a//b", "a/", "/", "/a/../b", "é ?#%\xff", "a:b", "a/./b" }) |path| {
            const resolved = try std.fs.path.resolveAlloc(a, &.{ cwd, path });
            defer a.free(resolved);
            var reference = std.Io.Writer.Allocating.init(a);
            defer reference.deinit();
            try reference.writer.writeAll("file://");
            for (resolved) |byte| {
                if (std.ascii.isAlphanumeric(byte) or std.mem.indexOfScalar(u8, "/-_.~:", byte) != null)
                    try reference.writer.writeByte(byte)
                else
                    try reference.writer.print("%{X:0>2}", .{byte});
            }
            const actual = try localUriAt(a, cwd, path);
            defer a.free(actual);
            try std.testing.expectEqualStrings(reference.written(), actual);
        }
    }
}
