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

pub const CatalogMode = enum {
    public_bootstrap,
    tenant,
};

pub const CatalogOptions = struct {
    mode: CatalogMode = .public_bootstrap,
    publisher_domain: []const u8 = "antfly.local",
    display_name: []const u8 = "Antfly",
};

const Entry = struct {
    identifier_suffix: []const u8,
    display_name: []const u8,
    media_type: []const u8,
    description: []const u8,
    url: ?[]const u8 = null,
    data: ?[]const u8 = null,
    metadata: ?[]const u8 = null,

    fn write(self: Entry, writer: *std.Io.Writer, publisher_domain: []const u8) !void {
        try writer.writeAll("{\"identifier\":");
        try writeStringFmt(writer, "urn:ai:{s}:antfly:{s}", .{ publisher_domain, self.identifier_suffix });
        try writer.writeAll(",\"displayName\":");
        try std.json.Stringify.value(self.display_name, .{}, writer);
        try writer.writeAll(",\"type\":");
        try std.json.Stringify.value(self.media_type, .{}, writer);
        try writer.writeAll(",\"description\":");
        try std.json.Stringify.value(self.description, .{}, writer);
        if (self.url) |url| {
            try writer.writeAll(",\"url\":");
            try std.json.Stringify.value(url, .{}, writer);
        } else if (self.data) |data| {
            try writer.writeAll(",\"data\":");
            try writer.writeAll(data);
        } else {
            return error.InvalidArdEntry;
        }
        if (self.metadata) |metadata| {
            try writer.writeAll(",\"metadata\":");
            try writer.writeAll(metadata);
        }
        try writer.writeByte('}');
    }
};

pub fn catalogJsonAlloc(alloc: std.mem.Allocator, options: CatalogOptions) ![]u8 {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();

    try writer.writer.writeAll("{\"specVersion\":\"1.0\",\"host\":{\"displayName\":");
    try std.json.Stringify.value(options.display_name, .{}, &writer.writer);
    try writer.writer.writeAll(",\"identifier\":");
    try writeStringFmt(&writer.writer, "did:web:{s}", .{options.publisher_domain});
    try writer.writer.writeAll("},\"entries\":[");

    var first = true;
    try writeEntry(&writer.writer, options.publisher_domain, &first, .{
        .identifier_suffix = "registry:default",
        .display_name = "Antfly ARD Registry",
        .media_type = "application/ai-registry+json",
        .description = "Authenticated Antfly ARD registry base.",
        .url = "/ard/v1",
    });
    try writeEntry(&writer.writer, options.publisher_domain, &first, .{
        .identifier_suffix = "catalog:tenant",
        .display_name = "Antfly Tenant ARD Catalog",
        .media_type = "application/ai-catalog+json",
        .description = "Authenticated Antfly ARD catalog for visible tenant resources.",
        .url = "/ard/v1/catalog",
    });
    try writeEntry(&writer.writer, options.publisher_domain, &first, .{
        .identifier_suffix = "a2a:default",
        .display_name = "Antfly A2A Agent",
        .media_type = "application/a2a-agent-card+json",
        .description = "Antfly A2A agent card.",
        .url = "/.well-known/agent-card.json",
        .metadata = "{\"endpoint\":\"/a2a\"}",
    });

    if (options.mode == .tenant) {
        try writeEntry(&writer.writer, options.publisher_domain, &first, .{
            .identifier_suffix = "mcp:default",
            .display_name = "Antfly MCP Server",
            .media_type = "application/mcp-server+json",
            .description = "Aggregate Antfly MCP server for built-in and visible extension tools.",
            .data = "{\"name\":\"antfly\",\"endpoint\":\"/mcp/v1\"}",
            .metadata = "{\"endpoint\":\"/mcp/v1\"}",
        });
    }

    try writer.writer.writeAll("]}");
    return try writer.toOwnedSlice();
}

fn writeEntry(
    writer: *std.Io.Writer,
    publisher_domain: []const u8,
    first: *bool,
    entry: Entry,
) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try entry.write(writer, publisher_domain);
}

fn writeStringFmt(writer: *std.Io.Writer, comptime fmt: []const u8, args: anytype) !void {
    var buf: [512]u8 = undefined;
    const value = try std.fmt.bufPrint(&buf, fmt, args);
    try std.json.Stringify.value(value, .{}, writer);
}

test "ARD catalog entries contain required value or reference fields" {
    const body = try catalogJsonAlloc(std.testing.allocator, .{ .mode = .tenant });
    defer std.testing.allocator.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();

    const root = parsed.value.object;
    try std.testing.expect(root.get("specVersion") != null);
    const entries = root.get("entries").?.array.items;
    try std.testing.expect(entries.len >= 4);
    for (entries) |entry| {
        const object = entry.object;
        try std.testing.expect(object.get("identifier") != null);
        try std.testing.expect(object.get("displayName") != null);
        try std.testing.expect(object.get("type") != null);
        const has_url = object.get("url") != null;
        const has_data = object.get("data") != null;
        try std.testing.expect(has_url != has_data);
    }
}
