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
const Allocator = std.mem.Allocator;
const external_binding = @import("catalog_binding.zig");
pub const OwnedExternalTableBinding = struct {
    binding: external_binding.Binding,
    table_id: []u8,
    source_uri: []u8,
    credential_ref_id: ?[]u8 = null,
    credential_scope: ?[]u8 = null,
    snapshot_value: ?[]u8 = null,
    schema_fingerprint: []u8,

    pub fn deinit(self: *OwnedExternalTableBinding, alloc: Allocator) void {
        alloc.free(self.table_id);
        alloc.free(self.source_uri);
        if (self.credential_ref_id) |value| alloc.free(value);
        if (self.credential_scope) |value| alloc.free(value);
        if (self.snapshot_value) |value| alloc.free(value);
        alloc.free(self.schema_fingerprint);
        self.* = undefined;
    }
};

pub fn externalBindingFromSchemaJsonAlloc(
    alloc: Allocator,
    schema_json: []const u8,
) !?OwnedExternalTableBinding {
    if (schema_json.len == 0) return null;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidExternalTableBinding,
    };
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidExternalTableBinding,
    };
    const source_value = root.get("base_source") orelse return null;
    if (source_value == .null) return null;
    const source = switch (source_value) {
        .object => |object| object,
        else => return error.InvalidExternalTableBinding,
    };
    const kind = try requiredJsonString(source, "kind");
    if (!std.mem.eql(u8, kind, "external")) return null;

    const table_id = try alloc.dupe(u8, try requiredJsonString(source, "table_id"));
    errdefer alloc.free(table_id);
    const source_uri = try alloc.dupe(u8, try requiredJsonString(source, "uri"));
    errdefer alloc.free(source_uri);
    const format: @import("types.zig").Format = blk: {
        const value = try requiredJsonString(source, "format");
        if (std.mem.eql(u8, value, "parquet")) break :blk .parquet;
        if (std.mem.eql(u8, value, "iceberg")) break :blk .iceberg;
        if (std.mem.eql(u8, value, "lance")) break :blk .lance;
        return error.InvalidExternalTableBinding;
    };

    const credentials = if (source.get("credentials")) |value| switch (value) {
        .object => |object| object,
        .null => null,
        else => return error.InvalidExternalTableBinding,
    } else null;
    const credential_ref_id = if (credentials) |value|
        try alloc.dupe(u8, try requiredJsonString(value, "ref"))
    else
        null;
    errdefer if (credential_ref_id) |value| alloc.free(value);
    const credential_scope = if (credentials) |value|
        if (value.get("scope")) |scope| switch (scope) {
            .string => |string| try alloc.dupe(u8, string),
            else => return error.InvalidExternalTableBinding,
        } else try alloc.dupe(u8, "")
    else
        null;
    errdefer if (credential_scope) |value| alloc.free(value);

    var snapshot_tag: enum { current, snapshot_id, object_version_digest } = .current;
    var snapshot_borrowed: ?[]const u8 = null;
    if (source.get("snapshot")) |snapshot| switch (snapshot) {
        .string => |value| {
            if (!std.mem.eql(u8, value, "current")) return error.InvalidExternalTableBinding;
        },
        .object => |object| {
            const mode = try requiredJsonString(object, "mode");
            if (std.mem.eql(u8, mode, "snapshot_id")) {
                snapshot_tag = .snapshot_id;
                snapshot_borrowed = try requiredJsonString(object, "id");
            } else if (std.mem.eql(u8, mode, "object_version_digest")) {
                snapshot_tag = .object_version_digest;
                snapshot_borrowed = try requiredJsonString(object, "digest");
            } else if (!std.mem.eql(u8, mode, "current")) {
                return error.InvalidExternalTableBinding;
            }
        },
        else => return error.InvalidExternalTableBinding,
    };
    const snapshot_value = if (snapshot_borrowed) |value| try alloc.dupe(u8, value) else null;
    errdefer if (snapshot_value) |value| alloc.free(value);
    const schema_fingerprint = try alloc.dupe(u8, if (source.get("schema_fingerprint") != null) try requiredJsonString(source, "schema_fingerprint") else "auto");
    errdefer alloc.free(schema_fingerprint);
    const write_policy: external_binding.WritePolicy = if (source.get("write_policy")) |value| blk: {
        const string = switch (value) {
            .string => |item| item,
            else => return error.InvalidExternalTableBinding,
        };
        if (std.mem.eql(u8, string, "read_only")) break :blk .read_only;
        if (std.mem.eql(u8, string, "materialized_overlay")) break :blk .materialized_overlay;
        if (std.mem.eql(u8, string, "iceberg_writer")) break :blk .iceberg_writer;
        if (std.mem.eql(u8, string, "lake_native_relational")) break :blk .lake_native_relational;
        return error.InvalidExternalTableBinding;
    } else .read_only;

    const binding = external_binding.Binding{
        .table_id = table_id,
        .format = format,
        .source_uri = source_uri,
        .credential_ref = if (credential_ref_id) |ref_id| .{
            .ref_id = ref_id,
            .scope = credential_scope orelse &.{},
        } else null,
        .snapshot_mode = switch (snapshot_tag) {
            .current => .current,
            .snapshot_id => .{ .snapshot_id = snapshot_value orelse return error.InvalidExternalTableBinding },
            .object_version_digest => .{ .object_version_digest = snapshot_value.? },
        },
        .schema_fingerprint = schema_fingerprint,
        .write_policy = write_policy,
        .object_mutability = if (source.get("object_mutability")) |_| std.meta.stringToEnum(external_binding.ObjectMutability, try requiredJsonString(source, "object_mutability")) orelse return error.InvalidExternalTableBinding else .mutable,
    };
    try binding.validateReadOnlyMvp();

    return .{
        .binding = binding,
        .table_id = table_id,
        .source_uri = source_uri,
        .credential_ref_id = credential_ref_id,
        .credential_scope = credential_scope,
        .snapshot_value = snapshot_value,
        .schema_fingerprint = schema_fingerprint,
    };
}

fn requiredJsonString(object: anytype, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidExternalTableBinding;
    return switch (value) {
        .string => |string| string,
        else => error.InvalidExternalTableBinding,
    };
}

pub fn cloneAlloc(alloc: Allocator, source: OwnedExternalTableBinding) !OwnedExternalTableBinding {
    const b = source.binding;
    const table_id = try alloc.dupe(u8, b.table_id);
    errdefer alloc.free(table_id);
    const uri = try alloc.dupe(u8, b.source_uri);
    errdefer alloc.free(uri);
    const fingerprint = try alloc.dupe(u8, b.schema_fingerprint);
    errdefer alloc.free(fingerprint);
    const ref = if (b.credential_ref) |c| try alloc.dupe(u8, c.ref_id) else null;
    errdefer if (ref) |v| alloc.free(v);
    const scope = if (b.credential_ref) |c| try alloc.dupe(u8, c.scope) else null;
    errdefer if (scope) |v| alloc.free(v);
    const snapshot = if (b.snapshot_mode.pinnedSnapshotId()) |v| try alloc.dupe(u8, v) else null;
    var binding = b;
    binding.table_id = table_id;
    binding.source_uri = uri;
    binding.schema_fingerprint = fingerprint;
    binding.credential_ref = if (ref) |v| .{ .ref_id = v, .scope = scope.? } else null;
    binding.snapshot_mode = switch (b.snapshot_mode) {
        .current => .current,
        .snapshot_id => .{ .snapshot_id = snapshot.? },
        .object_version_digest => .{ .object_version_digest = snapshot.? },
    };
    return .{ .binding = binding, .table_id = table_id, .source_uri = uri, .schema_fingerprint = fingerprint, .credential_ref_id = ref, .credential_scope = scope, .snapshot_value = snapshot };
}
