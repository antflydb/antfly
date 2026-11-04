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

//! Consumer-independent source fields and native textual artifact bindings.
const std = @import("std");
const document = @import("document.zig");
const A = std.mem.Allocator;
pub const Fields = struct {
    uri: []const u8 = "source.uri",
    path: []const u8 = "source.path",
    revision: []const u8 = "source.revision",
    indexed_at: []const u8 = "source.indexed_at",
    permissions: []const u8 = "permissions",
    metadata: []const u8 = "metadata",
};
pub const Stream = struct {
    name: []const u8,
    artifact: ?[]const u8 = null,
    index: ?[]const u8 = null,
    field: []const u8 = "text",
    purpose: ?[]const u8 = null,
    default: bool = false,
    generated: bool = false,
    boundary: ?[]const u8 = null,
    location_field: ?[]const u8 = null,
};
pub fn validate(fields: Fields, streams: []const Stream) !void {
    for ([_][]const u8{ fields.uri, fields.path, fields.revision, fields.indexed_at, fields.permissions, fields.metadata }) |field| {
        if (field.len == 0 or std.mem.indexOfAny(u8, field, "\x00\r\n") != null) return error.InvalidSourceFields;
    }
    if (streams.len > 64) return error.TooManyArtifactStreams;
    for (streams, 0..) |stream, i| {
        if (stream.name.len == 0 or stream.field.len == 0 or std.mem.indexOfAny(u8, stream.name, "\x00\r\n") != null or std.mem.indexOfAny(u8, stream.field, "\x00\r\n") != null) return error.InvalidArtifactStream;
        for ([_]?[]const u8{ stream.artifact, stream.index, stream.purpose, stream.location_field }) |field| {
            if (field) |value| if (std.mem.indexOfAny(u8, value, "\x00\r\n") != null) return error.InvalidArtifactStream;
        }
        if (stream.artifact == null and stream.index != null) return error.InvalidArtifactStream;
        if (stream.artifact) |artifact| if (artifact.len == 0 or stream.index == null or stream.index.?.len == 0) return error.InvalidArtifactStream;
        if (stream.boundary) |boundary| if (!std.mem.eql(u8, boundary, "record") and !std.mem.eql(u8, boundary, "unit")) return error.UnsupportedArtifactBoundary;
        for (streams[0..i]) |prior| if (std.mem.eql(u8, prior.name, stream.name)) return error.DuplicateArtifactStream;
    }
}

pub fn parentFields(a: A, fields: Fields) ![]const []const u8 {
    return a.dupe([]const u8, &.{ fields.uri, fields.path, fields.revision, fields.indexed_at, fields.permissions, fields.metadata });
}

// This is a borrowed, request-local view for the existing matcher. It is never
// persisted and cannot install authority; the current source ACL is retained.
pub fn view(a: A, source: std.json.Value, fields: Fields, content: []const u8) !std.json.Value {
    var out: std.json.ObjectMap = .empty;
    try out.put(a, "version", .{ .integer = 1 });
    try out.put(a, "content", .{ .string = content });
    inline for (.{ "uri", "path", "revision", "indexed_at", "permissions", "metadata" }) |name| {
        const path = @field(fields, name);
        const value = document.at(source, path) orelse if (comptime std.mem.eql(u8, name, "metadata")) std.json.Value{ .object = .empty } else return error.MissingSourceField;
        try out.put(a, name, value);
    }
    return .{ .object = out };
}

pub fn text(source: std.json.Value, path: []const u8) ![]const u8 {
    const value = if (std.mem.eql(u8, path, "$")) source else document.at(source, path) orelse return error.MissingArtifactText;
    if (value != .string) return error.InvalidArtifactText;
    return value.string;
}

pub fn selected(stream: Stream, requested: []const []const u8, all: bool) bool {
    if (all) return true;
    if (requested.len == 0) return stream.default;
    for (requested) |name| {
        if (std.mem.eql(u8, name, stream.name)) return true;
        if (stream.artifact) |artifact| if (std.mem.eql(u8, name, artifact)) return true;
    }
    return false;
}
