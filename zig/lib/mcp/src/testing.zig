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

//! MCP response helpers for protocol and integration tests.

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

pub const ToolContent = struct {
    type: []const u8,
    text: ?[]const u8 = null,
};

pub const ToolCallResult = struct {
    content: []const ToolContent,
    structuredContent: ?std.json.Value = null,
    isError: bool = false,
};

pub const ToolCallResponse = struct {
    jsonrpc: []const u8,
    id: ?std.json.Value = null,
    result: ToolCallResult,
};

pub const ListedTool = struct {
    name: []const u8,
    description: []const u8 = "",
    inputSchema: std.json.Value,
};

pub const ToolsListResponse = struct {
    result: struct {
        tools: []const ListedTool,
    },
};

pub fn parseToolsListResponse(alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(ToolsListResponse) {
    return try std.json.parseFromSlice(ToolsListResponse, alloc, body, .{ .ignore_unknown_fields = true });
}

pub fn findTool(tools: []const ListedTool, name: []const u8) ?*const ListedTool {
    for (tools) |*tool| {
        if (std.mem.eql(u8, tool.name, name)) return tool;
    }
    return null;
}

pub fn parseJsonRpcResponse(alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(JsonRpcResponse) {
    return try std.json.parseFromSlice(JsonRpcResponse, alloc, body, .{ .ignore_unknown_fields = true });
}

pub fn parseToolCallResponse(alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(ToolCallResponse) {
    return try std.json.parseFromSlice(ToolCallResponse, alloc, body, .{ .ignore_unknown_fields = true });
}

pub fn findTextContent(content: []const ToolContent) ?[]const u8 {
    for (content) |item| {
        if (std.mem.eql(u8, item.type, "text")) return item.text;
    }
    return null;
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

pub fn expectToolResultTextContains(
    alloc: std.mem.Allocator,
    body: []const u8,
    is_error: bool,
    expected_text: []const u8,
) !void {
    var response = try parseToolCallResponse(alloc, body);
    defer response.deinit();
    try std.testing.expectEqual(is_error, response.value.result.isError);
    const text_content = findTextContent(response.value.result.content) orelse return error.TestExpectedEqual;
    try std.testing.expect(std.mem.indexOf(u8, text_content, expected_text) != null);
}

pub fn expectToolStructuredSubset(
    alloc: std.mem.Allocator,
    body: []const u8,
    expected_structured_json: []const u8,
) !void {
    var response = try parseToolCallResponse(alloc, body);
    defer response.deinit();
    const structured = response.value.result.structuredContent orelse return error.TestExpectedEqual;
    try ant_json.testing.expectSubsetJsonValue(alloc, expected_structured_json, structured);
}

/// Assert that a JSON-valued MCP result exposes one canonical value through
/// both TextContent and structuredContent.
pub fn expectToolJsonRepresentationsEqual(alloc: std.mem.Allocator, body: []const u8) !void {
    var response = try parseToolCallResponse(alloc, body);
    defer response.deinit();
    const text_content = findTextContent(response.value.result.content) orelse return error.TestExpectedEqual;
    const structured = response.value.result.structuredContent orelse return error.TestExpectedEqual;
    try ant_json.testing.expectEqualJsonValue(alloc, text_content, structured);
}

test "MCP testing parses tools list responses and finds tools by name" {
    const alloc = std.testing.allocator;
    var parsed = try parseToolsListResponse(
        alloc,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[{\"name\":\"query\",\"description\":\"Search\",\"inputSchema\":{\"type\":\"object\"}},{\"name\":\"batch\",\"description\":\"Write\",\"inputSchema\":{\"type\":\"object\",\"required\":[\"tableName\"]}}]}}",
    );
    defer parsed.deinit();

    const batch = findTool(parsed.value.result.tools, "batch") orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("Write", batch.description);
    try std.testing.expectEqualStrings("object", batch.inputSchema.object.get("type").?.string);
    try std.testing.expect(findTool(parsed.value.result.tools, "missing") == null);
}

test "MCP testing validates JSON-RPC results, errors, and tool content structurally" {
    const alloc = std.testing.allocator;
    try expectResultSubset(
        alloc,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{}}}",
        "{\"protocolVersion\":\"2025-06-18\"}",
    );
    try expectError(
        alloc,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32602,\"message\":\"invalid params\"}}",
        -32602,
        "invalid params",
    );

    var call = try parseToolCallResponse(
        alloc,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"hello\"}],\"structuredContent\":{\"ok\":true}}}",
    );
    defer call.deinit();
    try std.testing.expectEqualStrings("hello", findTextContent(call.value.result.content).?);
    try ant_json.testing.expectEqualJsonValue(alloc, "{\"ok\":true}", call.value.result.structuredContent.?);

    try expectToolJsonRepresentationsEqual(
        alloc,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"{\\\"ok\\\":true}\"}],\"structuredContent\":{\"ok\":true}}}",
    );
}
