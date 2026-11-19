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
const public = @import("antfly_public_openapi");
const public_server = @import("antfly_public_server_openapi");
const metadata = @import("antfly_metadata_openapi");
const metadata_server = @import("antfly_metadata_server_openapi");
const usermgr = @import("antfly_usermgr_openapi");
const usermgr_server = @import("antfly_usermgr_server_openapi");

test "server parsers use the same schema types as embedded code" {
    const public_result = @TypeOf(public_server.server.parseQueryBuilderAgentBody(undefined, undefined));
    try std.testing.expect(@typeInfo(public_result).error_union.payload == std.json.Parsed(public.QueryBuilderRequest));
    try std.testing.expect(!@hasDecl(public, "server"));
    try std.testing.expect(!@hasDecl(metadata, "server"));
    try std.testing.expect(!@hasDecl(usermgr, "server"));
    const metadata_result = @TypeOf(metadata_server.server.parseQueryTableBody(undefined, undefined));
    const usermgr_result = @TypeOf(usermgr_server.server.parseCreateUserBody(undefined, undefined));
    try std.testing.expect(@typeInfo(metadata_result).error_union.payload == std.json.Parsed(metadata.StatefulQueryRequest));
    try std.testing.expect(@typeInfo(usermgr_result).error_union.payload == std.json.Parsed(usermgr.CreateUserRequest));
}

const package_wire = @import("extension_client_types.zig");

fn expectNativeSQLPackageEnum(comptime Generated: type, comptime declaration: []const u8) !void {
    // Embed native bytes without importing the physical catalog graph.
    const source = @embedFile("sql_routine_records_source.zig");
    const start = (std.mem.indexOf(u8, source, declaration) orelse return error.TestNativeEnumMissing) + declaration.len;
    const end = start + (std.mem.indexOf(u8, source[start..], "};") orelse return error.TestNativeEnumMissing);
    var fields = std.mem.tokenizeAny(u8, source[start..end], ", \t\r\n");
    var count: usize = 0;
    while (fields.next()) |field| {
        const name = if (std.mem.startsWith(u8, field, "@\"") and std.mem.endsWith(u8, field, "\"")) field[2 .. field.len - 1] else field;
        try std.testing.expect(std.meta.stringToEnum(Generated, name) != null);
        count += 1;
    }
    try std.testing.expectEqual(count, @typeInfo(Generated).@"enum".field_names.len);
}

test "generated SQL package enums preserve every native scalar and volatility domain" {
    try expectNativeSQLPackageEnum(package_wire.SQLPackageScalarType, "pub const Scalar = enum {");
    try expectNativeSQLPackageEnum(package_wire.SQLPackageVolatility, "pub const Volatility = enum {");
}

test "generated package models roundtrip native SQL controls and construct typed exports" {
    const a = std.testing.allocator;
    const native =
        \\{"manifest_api_version":"extensions/v1","name":"configured_sql","version":"2","kind":"extension","digest":"sha256:1111111111111111111111111111111111111111111111111111111111111111","install":{"scopes_supported":["embedded_db"],"runtimes":[{"name":"sql","mode":"wasm","artifact":"module.wasm"}]},"artifacts":[{"kind":"wasm","path":"module.wasm","digest":"sha256:2222222222222222222222222222222222222222222222222222222222222222"}],"sql":{"default_version":"2","schema":null,"exports":[{"name":"marker","runtime":"sql","export_name":"marker","arguments":[{"name":null,"type":{"scalar":"int64","array":false}}],"result":{"scalar":"int64","array":false},"strict":true,"volatility":"immutable"}],"updates_from":["1"]}}
    ;
    var parsed = try std.json.parseFromSlice(package_wire.PackageManifest, a, native, .{});
    defer parsed.deinit();
    const control = parsed.value.sql orelse return error.TestExpectedSQLControl;
    try std.testing.expectEqualStrings("2", control.default_version);
    try std.testing.expectEqual(@as(usize, 1), control.exports.len);
    try std.testing.expectEqual(package_wire.SQLPackageScalarType.int64, control.exports[0].result.scalar);
    const encoded = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
    defer a.free(encoded);
    var decoded = try std.json.parseFromSlice(package_wire.PackageManifest, a, encoded, .{});
    defer decoded.deinit();
    try std.testing.expectEqualDeep(parsed.value, decoded.value);
    var created = parsed.value;
    created.version = "3";
    created.sql = .{ .default_version = "3", .schema = .null_value, .exports = &.{.{ .name = "next_marker", .runtime = "sql", .export_name = "next_marker", .result = .{ .scalar = .numeric, .array = true }, .strict = true, .volatility = .stable }}, .updates_from = &.{"2"} };
    const created_json = try std.json.Stringify.valueAlloc(a, created, .{});
    defer a.free(created_json);
    var roundtrip = try std.json.parseFromSlice(package_wire.PackageManifest, a, created_json, .{});
    defer roundtrip.deinit();
    try std.testing.expectEqualDeep(created, roundtrip.value);
    if (std.json.parseFromSlice(package_wire.SQLPackageType, a, "{\"scalar\":\"invented_domain\",\"array\":false}", .{})) |invalid_| {
        var invalid = invalid_;
        defer invalid.deinit();
        return error.TestUnexpectedResult;
    } else |_| {}
}
