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
