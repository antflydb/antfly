// Copyright 2026 Antfly, Inc.
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

//! A2A response helpers for protocol and integration tests.

const std = @import("std");
const ant_json = @import("antfly-json");

pub const JsonRpcError = struct {
    code: i64,
    message: []const u8,
    data: ?std.json.Value = null,
};

pub const JsonRpcResponse = struct {
    jsonrpc: []const u8,
    id: ?std.json.Value = null,
    result: ?std.json.Value = null,
    @"error": ?JsonRpcError = null,
};

pub fn parseJsonRpcResponse(alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(JsonRpcResponse) {
    return try std.json.parseFromSlice(JsonRpcResponse, alloc, body, .{ .ignore_unknown_fields = true });
}

pub fn expectResultSubset(alloc: std.mem.Allocator, body: []const u8, expected_result_json: []const u8) !void {
    var response = try parseJsonRpcResponse(alloc, body);
    defer response.deinit();
    try std.testing.expectEqualStrings("2.0", response.value.jsonrpc);
    try std.testing.expect(response.value.@"error" == null);
    const result = response.value.result orelse return error.TestExpectedEqual;
    try ant_json.testing.expectSubsetJsonValue(alloc, expected_result_json, result);
}

pub fn expectError(alloc: std.mem.Allocator, body: []const u8, code: i64, message: []const u8) !void {
    var response = try parseJsonRpcResponse(alloc, body);
    defer response.deinit();
    try std.testing.expectEqualStrings("2.0", response.value.jsonrpc);
    try std.testing.expect(response.value.result == null);
    const rpc_error = response.value.@"error" orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(code, rpc_error.code);
    try std.testing.expectEqualStrings(message, rpc_error.message);
}

pub fn expectResultArrayContainsSubsets(
    alloc: std.mem.Allocator,
    body: []const u8,
    expected_json: []const []const u8,
) !void {
    var response = try parseJsonRpcResponse(alloc, body);
    defer response.deinit();
    try std.testing.expectEqualStrings("2.0", response.value.jsonrpc);
    try std.testing.expect(response.value.@"error" == null);
    const result = response.value.result orelse return error.TestExpectedEqual;
    if (result != .array) return error.TestExpectedEqual;
    try expectArrayContainsSubsets(alloc, result.array.items, expected_json);
}

pub fn expectJsonLinesContainSubsets(
    alloc: std.mem.Allocator,
    json_lines: []const u8,
    expected_json: []const []const u8,
) !void {
    const matched = try alloc.alloc(bool, expected_json.len);
    defer alloc.free(matched);
    @memset(matched, false);

    var lines = std.mem.splitScalar(u8, json_lines, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        var actual = try std.json.parseFromSlice(std.json.Value, alloc, line, .{});
        defer actual.deinit();
        for (expected_json, matched) |expected_text, *is_matched| {
            if (is_matched.*) continue;
            var expected = try std.json.parseFromSlice(std.json.Value, alloc, expected_text, .{});
            defer expected.deinit();
            if (ant_json.testing.valueIsSubset(expected.value, actual.value)) is_matched.* = true;
        }
    }
    for (matched) |is_matched| try std.testing.expect(is_matched);
}

fn expectArrayContainsSubsets(
    alloc: std.mem.Allocator,
    actual_values: []const std.json.Value,
    expected_json: []const []const u8,
) !void {
    for (expected_json) |expected_text| {
        var expected = try std.json.parseFromSlice(std.json.Value, alloc, expected_text, .{});
        defer expected.deinit();
        var found = false;
        for (actual_values) |actual| {
            if (ant_json.testing.valueIsSubset(expected.value, actual)) {
                found = true;
                break;
            }
        }
        try std.testing.expect(found);
    }
}

test "A2A testing validates JSON-RPC and JSON Lines structurally" {
    const alloc = std.testing.allocator;
    try expectResultSubset(
        alloc,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"status\":{\"state\":\"completed\"}}}",
        "{\"status\":{\"state\":\"completed\"}}",
    );
    try expectError(
        alloc,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32004,\"message\":\"task not found\"}}",
        -32004,
        "task not found",
    );
    try expectJsonLinesContainSubsets(
        alloc,
        "{\"kind\":\"artifact-update\",\"taskId\":\"t1\"}\n{\"kind\":\"status-update\",\"status\":{\"state\":\"completed\"}}\n",
        &.{ "{\"kind\":\"artifact-update\"}", "{\"status\":{\"state\":\"completed\"}}" },
    );
    try expectResultArrayContainsSubsets(
        alloc,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":[{\"kind\":\"artifact-update\"},{\"kind\":\"status-update\"}]}",
        &.{ "{\"kind\":\"status-update\"}", "{\"kind\":\"artifact-update\"}" },
    );
}
