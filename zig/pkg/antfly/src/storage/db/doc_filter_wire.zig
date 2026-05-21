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

const std = @import("std");
const Allocator = std.mem.Allocator;
const doc_identity = @import("doc_identity.zig");
const doc_set = @import("doc_set.zig");
const types = @import("types.zig");
const roaring = @import("../../encoding/roaring.zig");

pub const field_name = "_resolved_doc_filter";

pub fn destroyResolvedDocFilter(alloc: Allocator, ptr: *const anyopaque) void {
    const filter: *doc_set.ResolvedDocFilter = @ptrCast(@alignCast(@constCast(ptr)));
    filter.deinit(alloc);
    alloc.destroy(filter);
}

pub fn appendSearchRequestFieldAlloc(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    req: types.SearchRequest,
) !void {
    const ptr = req.resolved_doc_filter orelse return;
    const ctx = req.resolved_doc_filter_wire_context orelse return error.UnsupportedQueryRequest;
    try appendFilterFieldAlloc(alloc, out, first, ptr, ctx);
}

pub fn appendFilterFieldAlloc(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    ptr: *const anyopaque,
    ctx: types.ResolvedDocFilterWireContext,
) !void {
    const filter: *const doc_set.ResolvedDocFilter = @ptrCast(@alignCast(ptr));
    try appendJsonFieldName(alloc, out, first, field_name);
    try appendFilterEnvelopeAlloc(alloc, out, ctx, filter);
}

pub fn parseIntoSearchRequestAlloc(
    alloc: Allocator,
    value: std.json.Value,
    req: *types.SearchRequest,
) !void {
    if (req.resolved_doc_filter != null) return error.InvalidQueryRequest;
    if (value != .object) return error.InvalidQueryRequest;
    const parsed = try parseFilterEnvelopeAlloc(alloc, value);
    req.resolved_doc_filter = parsed.resolved_doc_filter;
    req.resolved_doc_filter_owned = true;
    req.resolved_doc_filter_wire_context = parsed.context;
    req.identity_read_generation = parsed.context.identity_read_generation;
}

pub const ParsedResolvedDocFilter = struct {
    resolved_doc_filter: *doc_set.ResolvedDocFilter,
    context: types.ResolvedDocFilterWireContext,

    pub fn deinit(self: *ParsedResolvedDocFilter, alloc: Allocator) void {
        destroyResolvedDocFilter(alloc, self.resolved_doc_filter);
        self.* = undefined;
    }
};

pub fn parseFilterEnvelopeAlloc(
    alloc: Allocator,
    value: std.json.Value,
) !ParsedResolvedDocFilter {
    if (value != .object) return error.InvalidQueryRequest;
    const namespace_value = value.object.get("namespace") orelse return error.InvalidQueryRequest;
    const namespace = try parseNamespace(namespace_value);
    const generation = try parseRequiredU64(value.object.get("identity_read_generation"));
    var include = try parseSetAlloc(alloc, value.object.get("include") orelse return error.InvalidQueryRequest);
    errdefer include.deinit(alloc);
    var exclude = try parseSetAlloc(alloc, value.object.get("exclude") orelse return error.InvalidQueryRequest);
    errdefer exclude.deinit(alloc);

    const filter = try alloc.create(doc_set.ResolvedDocFilter);
    errdefer alloc.destroy(filter);
    filter.* = .{
        .include = include,
        .exclude = exclude,
    };
    include = .none;
    exclude = .none;

    return .{
        .resolved_doc_filter = filter,
        .context = .{
            .namespace = namespace,
            .identity_read_generation = generation,
        },
    };
}

fn appendFilterEnvelopeAlloc(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    ctx: types.ResolvedDocFilterWireContext,
    filter: *const doc_set.ResolvedDocFilter,
) !void {
    try out.append(alloc, '{');
    var first = true;
    try appendJsonFieldName(alloc, out, &first, "namespace");
    try appendNamespaceAlloc(alloc, out, ctx.namespace);
    try appendJsonFieldU64(alloc, out, &first, "identity_read_generation", ctx.identity_read_generation);
    try appendJsonFieldName(alloc, out, &first, "include");
    try appendSetAlloc(alloc, out, &filter.include);
    try appendJsonFieldName(alloc, out, &first, "exclude");
    try appendSetAlloc(alloc, out, &filter.exclude);
    try out.append(alloc, '}');
}

fn appendNamespaceAlloc(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), namespace: doc_identity.Namespace) !void {
    try out.append(alloc, '{');
    var first = true;
    try appendJsonFieldU64(alloc, out, &first, "table_id", namespace.table_id);
    try appendJsonFieldU64(alloc, out, &first, "shard_id", namespace.shard_id);
    try appendJsonFieldU64(alloc, out, &first, "range_id", namespace.range_id);
    try out.append(alloc, '}');
}

fn appendSetAlloc(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), set: *const doc_set.ResolvedDocSet) !void {
    try out.append(alloc, '{');
    var first = true;
    switch (set.*) {
        .all => try appendJsonFieldString(alloc, out, &first, "kind", "all"),
        .none => try appendJsonFieldString(alloc, out, &first, "kind", "none"),
        .doc_keys => |keys| {
            try appendJsonFieldString(alloc, out, &first, "kind", "doc_keys");
            try appendJsonFieldName(alloc, out, &first, "values_b64");
            try appendBase64StringArrayAlloc(alloc, out, keys);
        },
        .ordinals => |ordinals| {
            try appendJsonFieldString(alloc, out, &first, "kind", "ordinals");
            try appendJsonFieldName(alloc, out, &first, "values");
            try appendU32ArrayAlloc(alloc, out, ordinals);
        },
        .ordinal_bitmap => |*bitmap| {
            try appendJsonFieldString(alloc, out, &first, "kind", "bitmap");
            const bytes = try bitmap.toBytes(alloc);
            defer alloc.free(bytes);
            const encoded = try base64EncodeAlloc(alloc, bytes);
            defer alloc.free(encoded);
            try appendJsonFieldString(alloc, out, &first, "bytes_b64", encoded);
        },
    }
    try out.append(alloc, '}');
}

fn parseSetAlloc(alloc: Allocator, value: std.json.Value) !doc_set.ResolvedDocSet {
    if (value != .object) return error.InvalidQueryRequest;
    const kind_value = value.object.get("kind") orelse return error.InvalidQueryRequest;
    if (kind_value != .string) return error.InvalidQueryRequest;
    if (std.mem.eql(u8, kind_value.string, "all")) return .all;
    if (std.mem.eql(u8, kind_value.string, "none")) return .none;
    if (std.mem.eql(u8, kind_value.string, "doc_keys")) {
        const keys = try parseBase64StringArrayAlloc(alloc, value.object.get("values_b64") orelse return error.InvalidQueryRequest);
        return .{ .doc_keys = keys };
    }
    if (std.mem.eql(u8, kind_value.string, "ordinals")) {
        const ordinals = try parseU32ArrayAlloc(alloc, value.object.get("values") orelse return error.InvalidQueryRequest);
        defer alloc.free(ordinals);
        return try doc_set.fromOrdinalsAlloc(alloc, ordinals);
    }
    if (std.mem.eql(u8, kind_value.string, "bitmap")) {
        const bytes_value = value.object.get("bytes_b64") orelse return error.InvalidQueryRequest;
        if (bytes_value != .string) return error.InvalidQueryRequest;
        const bytes = try base64DecodeAlloc(alloc, bytes_value.string);
        defer alloc.free(bytes);
        return .{ .ordinal_bitmap = try roaring.RoaringBitmap.fromBytes(alloc, bytes) };
    }
    return error.InvalidQueryRequest;
}

fn parseNamespace(value: std.json.Value) !doc_identity.Namespace {
    if (value != .object) return error.InvalidQueryRequest;
    return .{
        .table_id = try parseRequiredU64(value.object.get("table_id")),
        .shard_id = try parseRequiredU64(value.object.get("shard_id")),
        .range_id = try parseRequiredU64(value.object.get("range_id")),
    };
}

fn parseRequiredU64(value_opt: ?std.json.Value) !u64 {
    const value = value_opt orelse return error.InvalidQueryRequest;
    if (value != .integer or value.integer < 0) return error.InvalidQueryRequest;
    return std.math.cast(u64, value.integer) orelse return error.InvalidQueryRequest;
}

fn parseU32ArrayAlloc(alloc: Allocator, value: std.json.Value) ![]doc_set.DocOrdinal {
    if (value != .array) return error.InvalidQueryRequest;
    const out = try alloc.alloc(doc_set.DocOrdinal, value.array.items.len);
    errdefer alloc.free(out);
    for (value.array.items, 0..) |item, i| {
        if (item != .integer or item.integer < 0) return error.InvalidQueryRequest;
        out[i] = std.math.cast(doc_set.DocOrdinal, item.integer) orelse return error.InvalidQueryRequest;
    }
    return out;
}

fn parseBase64StringArrayAlloc(alloc: Allocator, value: std.json.Value) ![]const []const u8 {
    if (value != .array) return error.InvalidQueryRequest;
    const out = try alloc.alloc([]const u8, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(@constCast(item));
        alloc.free(out);
    }
    for (value.array.items, 0..) |item, i| {
        if (item != .string) return error.InvalidQueryRequest;
        out[i] = try base64DecodeAlloc(alloc, item.string);
        initialized += 1;
    }
    return out;
}

fn appendBase64StringArrayAlloc(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), values: []const []const u8) !void {
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try base64EncodeAlloc(alloc, value);
        defer alloc.free(encoded);
        try appendJsonStringAlloc(alloc, out, encoded);
    }
    try out.append(alloc, ']');
}

fn appendU32ArrayAlloc(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), values: []const doc_set.DocOrdinal) !void {
    try out.append(alloc, '[');
    for (values, 0..) |value, i| {
        if (i > 0) try out.append(alloc, ',');
        const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(rendered);
        try out.appendSlice(alloc, rendered);
    }
    try out.append(alloc, ']');
}

fn appendJsonFieldName(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8) !void {
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    try appendJsonStringAlloc(alloc, out, name);
    try out.append(alloc, ':');
}

fn appendJsonFieldString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: []const u8) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendJsonStringAlloc(alloc, out, value);
}

fn appendJsonFieldU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), first: *bool, name: []const u8, value: u64) !void {
    try appendJsonFieldName(alloc, out, first, name);
    const rendered = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(rendered);
    try out.appendSlice(alloc, rendered);
}

fn appendJsonStringAlloc(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    defer writer.deinit();
    try std.json.Stringify.value(value, .{}, &writer.writer);
    try out.appendSlice(alloc, writer.written());
}

fn base64EncodeAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const size = std.base64.standard.Encoder.calcSize(bytes.len);
    const out = try alloc.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, bytes);
    return out;
}

fn base64DecodeAlloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    const size = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
    const out = try alloc.alloc(u8, size);
    errdefer alloc.free(out);
    try std.base64.standard.Decoder.decode(out, encoded);
    return out;
}

test "doc filter wire round-trips ordinal and doc-key filters" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{
        .include = try doc_set.fromOrdinalsAlloc(alloc, &.{ 7, 3, 7 }),
        .exclude = try doc_set.cloneDocKeysAlloc(alloc, &.{ "doc:a", "doc:\x00b" }),
    };
    defer filter.deinit(alloc);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    var first = true;
    try out.append(alloc, '{');
    try appendSearchRequestFieldAlloc(alloc, &out, &first, .{
        .resolved_doc_filter = &filter,
        .resolved_doc_filter_wire_context = .{
            .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .identity_read_generation = 9,
        },
    });
    try out.append(alloc, '}');

    var parsed_json = try std.json.parseFromSlice(std.json.Value, alloc, out.items, .{});
    defer parsed_json.deinit();
    var req = types.SearchRequest{};
    defer if (req.resolved_doc_filter_owned) destroyResolvedDocFilter(alloc, req.resolved_doc_filter.?);
    try parseIntoSearchRequestAlloc(alloc, parsed_json.value.object.get(field_name).?, &req);
    try std.testing.expectEqual(@as(?u64, 9), req.identity_read_generation);
    try std.testing.expect(req.resolved_doc_filter_wire_context.?.namespace.eql(.{ .table_id = 1, .shard_id = 2, .range_id = 3 }));
    const parsed_filter: *const doc_set.ResolvedDocFilter = @ptrCast(@alignCast(req.resolved_doc_filter.?));
    try std.testing.expect(parsed_filter.include.containsOrdinal(3));
    try std.testing.expect(parsed_filter.include.containsOrdinal(7));
    try std.testing.expectEqual(@as(usize, 2), parsed_filter.exclude.doc_keys.len);
}
