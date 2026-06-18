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
const extension_domain = @import("../extensions/mod.zig");
const usermgr = @import("../usermgr/mod.zig");

pub const CatalogMode = enum {
    public_bootstrap,
    tenant,
};

pub const CatalogOptions = struct {
    mode: CatalogMode = .public_bootstrap,
    publisher_domain: []const u8 = "antfly.local",
    display_name: []const u8 = "Antfly",
};

pub const ExtensionCatalogContext = struct {
    installed_extensions: []const extension_domain.InstalledExtension = &.{},
    extension_members: []const extension_domain.ExtensionMember = &.{},
    permissions: ?[]const usermgr.Permission = null,
};

const Entry = struct {
    identifier_suffix: []const u8,
    display_name: []const u8,
    media_type: []const u8,
    description: []const u8,
    url: ?[]const u8 = null,
    data: ?[]const u8 = null,
    metadata: ?[]const u8 = null,
    tags: []const []const u8 = &.{},
    capabilities: []const []const u8 = &.{},
    representative_queries: []const []const u8 = &.{},

    fn write(self: Entry, writer: *std.Io.Writer, publisher_domain: []const u8) !void {
        try writer.writeByte('{');
        try self.writeFields(writer, publisher_domain);
        try writer.writeByte('}');
    }

    fn writeSearchResult(self: Entry, writer: *std.Io.Writer, publisher_domain: []const u8, score: u16) !void {
        try writer.writeByte('{');
        try self.writeFields(writer, publisher_domain);
        try writer.writeAll(",\"score\":");
        try writer.print("{d}", .{score});
        try writer.writeAll(",\"source\":\"/ard/v1/catalog\"}");
    }

    fn writeFields(self: Entry, writer: *std.Io.Writer, publisher_domain: []const u8) !void {
        try writer.writeAll("\"identifier\":");
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
        if (self.tags.len > 0) {
            try writer.writeAll(",\"tags\":");
            try writeStringArray(writer, self.tags);
        }
        if (self.capabilities.len > 0) {
            try writer.writeAll(",\"capabilities\":");
            try writeStringArray(writer, self.capabilities);
        }
        if (self.representative_queries.len > 0) {
            try writer.writeAll(",\"representativeQueries\":");
            try writeStringArray(writer, self.representative_queries);
        }
        if (self.metadata) |metadata| {
            try writer.writeAll(",\"metadata\":");
            try writer.writeAll(metadata);
        }
    }
};

const Skill = struct {
    slug: []const u8,
    url: []const u8,
    display_name: []const u8,
    description: []const u8,
    capabilities: []const []const u8,
    representative_queries: []const []const u8,
    body: []const u8,
};

const skills = [_]Skill{
    .{
        .slug = "antfly-query-builder",
        .url = "/ard/v1/skills/antfly-query-builder",
        .display_name = "Antfly Query Builder",
        .description = "Translate user intent into Antfly table queries and query-builder requests.",
        .capabilities = &.{ "query-builder", "table-query", "schema-aware-search" },
        .representative_queries = &.{
            "turn this question into an Antfly query",
            "build a query for relevant customer support tickets",
            "ask a clarifying question before querying the table",
        },
        .body =
        \\# Antfly Query Builder
        \\
        \\Use this skill when an agent needs to translate user intent into Antfly table queries or query-builder API requests.
        \\
        \\Prefer `/api/v1/tables/{table}/query-builder` when the user intent is underspecified and `/api/v1/tables/{table}/query` when the caller already has a concrete Antfly query.
        \\
        \\Keep table and field selection scoped to resources visible to the authenticated tenant identity.
        \\
        ,
    },
    .{
        .slug = "antfly-retrieval",
        .url = "/ard/v1/skills/antfly-retrieval",
        .display_name = "Antfly Retrieval",
        .description = "Retrieve, rank, and synthesize context from Antfly tables.",
        .capabilities = &.{ "retrieval", "hybrid-search", "context-synthesis" },
        .representative_queries = &.{
            "retrieve context for this incident",
            "find the most relevant documents in this table",
            "summarize results from Antfly retrieval",
        },
        .body =
        \\# Antfly Retrieval
        \\
        \\Use this skill when an agent needs tenant-scoped context retrieval from Antfly tables.
        \\
        \\Use `/api/v1/tables/{table}/retrieval-agent` for end-to-end retrieval and synthesis. Use `/api/v1/tables/{table}/query` for direct search when the caller already knows the query shape.
        \\
        \\Do not infer access to tables or rows that are not visible through the caller's Antfly identity.
        \\
        ,
    },
    .{
        .slug = "antfly-schema-design",
        .url = "/ard/v1/skills/antfly-schema-design",
        .display_name = "Antfly Schema Design",
        .description = "Design Antfly tables, schemas, indexes, enrichments, and query processors.",
        .capabilities = &.{ "schema-design", "index-design", "table-management" },
        .representative_queries = &.{
            "design a schema for these documents",
            "choose indexes for semantic and keyword search",
            "configure enrichments for this table",
        },
        .body =
        \\# Antfly Schema Design
        \\
        \\Use this skill when an agent needs to create or evolve Antfly table contracts.
        \\
        \\Use the table lifecycle APIs for creation and updates, and prefer generated OpenAPI schemas as the source of request shape.
        \\
        \\Validate that requested indexes and enrichments are available in the deployment before recommending them.
        \\
        ,
    },
    .{
        .slug = "antfly-extension-management",
        .url = "/ard/v1/skills/antfly-extension-management",
        .display_name = "Antfly Extension Management",
        .description = "Install, configure, enable, disable, and inspect Antfly extensions.",
        .capabilities = &.{ "extension-install", "extension-config", "extension-lifecycle" },
        .representative_queries = &.{
            "install this Antfly extension",
            "show visible MCP tools for an extension",
            "configure an extension for a tenant table",
        },
        .body =
        \\# Antfly Extension Management
        \\
        \\Use this skill when an agent needs to inspect or manage Antfly extensions.
        \\
        \\Use `/extensions/v1` for package and lifecycle operations. Use `/mcp/v1/extensions/{extension}` only after the extension is installed, enabled, and visible to the tenant identity.
        \\
        \\Never expose extension tools through ARD that the same identity cannot list through MCP.
        \\
        ,
    },
};

const static_entries = [_]Entry{
    .{
        .identifier_suffix = "registry:default",
        .display_name = "Antfly ARD Registry",
        .media_type = "application/ai-registry+json",
        .description = "Authenticated Antfly ARD registry base.",
        .url = "/ard/v1",
        .tags = &.{ "registry", "search", "dynamic" },
        .capabilities = &.{ "catalog-search", "catalog-explore", "agent-list" },
        .representative_queries = &.{ "find Antfly resources for a task", "search available tenant tools and skills" },
    },
    .{
        .identifier_suffix = "catalog:tenant",
        .display_name = "Antfly Tenant ARD Catalog",
        .media_type = "application/ai-catalog+json",
        .description = "Authenticated Antfly ARD catalog for visible tenant resources.",
        .url = "/ard/v1/catalog",
        .tags = &.{ "catalog", "tenant" },
        .capabilities = &.{"catalog-export"},
    },
    .{
        .identifier_suffix = "a2a:default",
        .display_name = "Antfly A2A Agent",
        .media_type = "application/a2a-agent-card+json",
        .description = "Antfly A2A agent card.",
        .url = "/.well-known/agent-card.json",
        .metadata = "{\"endpoint\":\"/a2a\"}",
        .tags = &.{ "a2a", "agent" },
        .capabilities = &.{ "retrieval", "query-builder" },
        .representative_queries = &.{ "ask Antfly to retrieve context", "ask Antfly to build a table query" },
    },
};

const tenant_entries = [_]Entry{
    .{
        .identifier_suffix = "mcp:default",
        .display_name = "Antfly MCP Server",
        .media_type = "application/mcp-server+json",
        .description = "Aggregate Antfly MCP server for built-in and visible extension tools.",
        .data = "{\"name\":\"antfly\",\"endpoint\":\"/mcp/v1\"}",
        .metadata = "{\"endpoint\":\"/mcp/v1\"}",
        .tags = &.{ "mcp", "tools" },
        .capabilities = &.{ "table-search", "retrieval", "query-builder", "extension-tools" },
        .representative_queries = &.{ "search an Antfly table", "list extension MCP tools", "run a retrieval workflow" },
    },
    .{
        .identifier_suffix = "openapi:public",
        .display_name = "Antfly ARD OpenAPI",
        .media_type = "application/openapi+yaml",
        .description = "Machine-readable OpenAPI specification for Antfly ARD discovery APIs.",
        .url = "/ard/v1/openapi.yaml",
        .metadata = "{\"sourceSpec\":\"ard:v1\",\"requiredPermissions\":[\"tenant-api\"]}",
        .tags = &.{ "openapi", "api", "public" },
        .capabilities = &.{ "table-management", "query", "retrieval", "extensions" },
        .representative_queries = &.{ "call the Antfly table query API", "manage Antfly extensions through HTTP", "inspect table schemas through OpenAPI" },
    },
};

pub fn catalogJsonAlloc(alloc: std.mem.Allocator, options: CatalogOptions) ![]u8 {
    return try catalogJsonWithExtensionsAlloc(alloc, options, null);
}

pub fn catalogJsonWithExtensionsAlloc(alloc: std.mem.Allocator, options: CatalogOptions, extension_context: ?ExtensionCatalogContext) ![]u8 {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();

    try writeCatalogPrefix(&writer.writer, options);
    var first = true;
    try writeScopedEntries(&writer.writer, options, &first, null, null);
    if (options.mode == .tenant) {
        if (extension_context) |ctx| try writeExtensionEntries(alloc, &writer.writer, options.publisher_domain, &first, ctx, null, null);
    }
    try writer.writer.writeAll("]}");
    return try writer.toOwnedSlice();
}

pub fn searchJsonAlloc(alloc: std.mem.Allocator, options: CatalogOptions, body: []const u8, explore: bool) ![]u8 {
    return try searchJsonWithExtensionsAlloc(alloc, options, body, explore, null);
}

pub fn searchJsonWithExtensionsAlloc(alloc: std.mem.Allocator, options: CatalogOptions, body: []const u8, explore: bool, extension_context: ?ExtensionCatalogContext) ![]u8 {
    const request = try parseSearchRequest(alloc, body);
    defer request.deinit();

    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();

    try writer.writer.writeAll("{\"results\":[");
    var first = true;
    var matched: usize = 0;
    try writeMatchedEntries(&writer.writer, options, &first, request.text, request.filter, &matched);
    if (options.mode == .tenant) {
        if (extension_context) |ctx| try writeMatchedExtensionEntries(alloc, &writer.writer, options.publisher_domain, &first, ctx, request.text, request.filter, &matched);
    }
    try writer.writer.writeAll("],\"federation\":\"none\",\"count\":");
    try writer.writer.print("{d}", .{matched});
    if (explore) {
        try writer.writer.writeAll(",\"facets\":{\"type\":[\"application/a2a-agent-card+json\",\"application/mcp-server+json\",\"application/openapi+yaml\",\"application/ai-skill+md\"]}");
    }
    try writer.writer.writeByte('}');
    return try writer.toOwnedSlice();
}

pub fn agentsJsonAlloc(alloc: std.mem.Allocator, options: CatalogOptions) ![]u8 {
    return try agentsJsonWithExtensionsAlloc(alloc, options, null);
}

pub fn agentsJsonWithExtensionsAlloc(alloc: std.mem.Allocator, options: CatalogOptions, extension_context: ?ExtensionCatalogContext) ![]u8 {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();

    try writer.writer.writeAll("{\"agents\":[");
    var first = true;
    var count: usize = 0;
    try writeAgentEntries(&writer.writer, options, &first, &count);
    if (options.mode == .tenant) {
        if (extension_context) |ctx| try writeAgentExtensionEntries(alloc, &writer.writer, options.publisher_domain, &first, ctx, &count);
    }
    try writer.writer.writeAll("],\"count\":");
    try writer.writer.print("{d}", .{count});
    try writer.writer.writeByte('}');
    return try writer.toOwnedSlice();
}

pub fn skillMarkdownAlloc(alloc: std.mem.Allocator, slug: []const u8) !?[]u8 {
    const skill = findSkill(slug) orelse return null;
    return try alloc.dupe(u8, skill.body);
}

pub fn mcpDescriptorJsonAlloc(alloc: std.mem.Allocator, name: []const u8) !?[]u8 {
    if (!std.mem.eql(u8, name, "default")) return null;
    return try alloc.dupe(u8,
        \\{"name":"antfly","endpoint":"/mcp/v1","description":"Aggregate Antfly MCP server for built-in and visible extension tools.","capabilities":["table-search","retrieval","query-builder","extension-tools"]}
    );
}

const SearchRequest = struct {
    parsed: ?std.json.Parsed(std.json.Value) = null,
    text: ?[]const u8 = null,
    filter: ?std.json.Value = null,

    fn deinit(self: SearchRequest) void {
        if (self.parsed) |parsed| parsed.deinit();
    }
};

fn parseSearchRequest(alloc: std.mem.Allocator, body: []const u8) !SearchRequest {
    if (body.len == 0) return .{};
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidArdSearchRequest;
    errdefer parsed.deinit();
    if (parsed.value != .object) return error.InvalidArdSearchRequest;
    const query = parsed.value.object.get("query") orelse return .{ .parsed = parsed };
    if (query != .object) return error.InvalidArdSearchRequest;
    const text: ?[]const u8 = if (query.object.get("text")) |value| switch (value) {
        .string => |text_value| text_value,
        .null => null,
        else => return error.InvalidArdSearchRequest,
    } else null;
    const filter = query.object.get("filter");
    if (filter) |value| {
        if (value != .object) return error.InvalidArdSearchRequest;
    }
    return .{
        .parsed = parsed,
        .text = text,
        .filter = filter,
    };
}

fn writeCatalogPrefix(writer: *std.Io.Writer, options: CatalogOptions) !void {
    try writer.writeAll("{\"specVersion\":\"1.0\",\"host\":{\"displayName\":");
    try std.json.Stringify.value(options.display_name, .{}, writer);
    try writer.writeAll(",\"identifier\":");
    try writeStringFmt(writer, "did:web:{s}", .{options.publisher_domain});
    try writer.writeAll("},\"entries\":[");
}

fn writeScopedEntries(
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    text: ?[]const u8,
    filter: ?std.json.Value,
) !void {
    for (static_entries) |entry| {
        if (entryMatches(entry, options.publisher_domain, text, filter)) try writeEntry(writer, options.publisher_domain, first, entry);
    }
    if (options.mode == .tenant) {
        for (tenant_entries) |entry| {
            if (entryMatches(entry, options.publisher_domain, text, filter)) try writeEntry(writer, options.publisher_domain, first, entry);
        }
        for (skills) |skill| {
            const entry = skillEntry(skill);
            if (entryMatches(entry, options.publisher_domain, text, filter)) try writeEntry(writer, options.publisher_domain, first, entry);
        }
    }
}

fn writeMatchedEntries(
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    text: ?[]const u8,
    filter: ?std.json.Value,
    matched: *usize,
) !void {
    for (static_entries) |entry| {
        if (entryMatches(entry, options.publisher_domain, text, filter)) try writeSearchEntry(writer, options.publisher_domain, first, entry, matched, text);
    }
    if (options.mode == .tenant) {
        for (tenant_entries) |entry| {
            if (entryMatches(entry, options.publisher_domain, text, filter)) try writeSearchEntry(writer, options.publisher_domain, first, entry, matched, text);
        }
        for (skills) |skill| {
            const entry = skillEntry(skill);
            if (entryMatches(entry, options.publisher_domain, text, filter)) try writeSearchEntry(writer, options.publisher_domain, first, entry, matched, text);
        }
    }
}

fn writeAgentEntries(writer: *std.Io.Writer, options: CatalogOptions, first: *bool, count: *usize) !void {
    for (static_entries) |entry| {
        if (isAgentLike(entry)) {
            try writeEntry(writer, options.publisher_domain, first, entry);
            count.* += 1;
        }
    }
    if (options.mode == .tenant) {
        for (tenant_entries) |entry| {
            if (isAgentLike(entry)) {
                try writeEntry(writer, options.publisher_domain, first, entry);
                count.* += 1;
            }
        }
    }
}

fn writeExtensionEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    publisher_domain: []const u8,
    first: *bool,
    ctx: ExtensionCatalogContext,
    text: ?[]const u8,
    filter: ?std.json.Value,
) !void {
    for (ctx.installed_extensions) |installed| {
        const has_visible_mcp = try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions);
        if ((installedExtensionVisible(installed, ctx.permissions) or has_visible_mcp) and
            dynamicEntryMatches(installed.name, "application/antfly-installed-extension+json", "extension", text, filter, publisher_domain))
        {
            try writeInstalledExtensionEntry(writer, publisher_domain, first, installed);
        }
        if (has_visible_mcp) {
            if (dynamicEntryMatches(installed.name, "application/mcp-server+json", "mcp extension", text, filter, publisher_domain)) {
                try writeExtensionMcpEntry(writer, publisher_domain, first, installed);
            }
        }
    }
}

fn writeMatchedExtensionEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    publisher_domain: []const u8,
    first: *bool,
    ctx: ExtensionCatalogContext,
    text: ?[]const u8,
    filter: ?std.json.Value,
    matched: *usize,
) !void {
    for (ctx.installed_extensions) |installed| {
        const has_visible_mcp = try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions);
        if ((installedExtensionVisible(installed, ctx.permissions) or has_visible_mcp) and
            dynamicEntryMatches(installed.name, "application/antfly-installed-extension+json", "extension", text, filter, publisher_domain))
        {
            try writeSearchInstalledExtensionEntry(writer, publisher_domain, first, installed, matched, text);
        }
        if (has_visible_mcp) {
            if (dynamicEntryMatches(installed.name, "application/mcp-server+json", "mcp extension", text, filter, publisher_domain)) {
                try writeSearchExtensionMcpEntry(writer, publisher_domain, first, installed, matched, text);
            }
        }
    }
}

fn writeAgentExtensionEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    publisher_domain: []const u8,
    first: *bool,
    ctx: ExtensionCatalogContext,
    count: *usize,
) !void {
    for (ctx.installed_extensions) |installed| {
        if (!(try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions))) continue;
        try writeExtensionMcpEntry(writer, publisher_domain, first, installed);
        count.* += 1;
    }
}

fn writeInstalledExtensionEntry(writer: *std.Io.Writer, publisher_domain: []const u8, first: *bool, installed: extension_domain.InstalledExtension) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeInstalledExtensionFields(writer, publisher_domain, installed);
    try writer.writeByte('}');
}

fn writeExtensionMcpEntry(writer: *std.Io.Writer, publisher_domain: []const u8, first: *bool, installed: extension_domain.InstalledExtension) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeExtensionMcpFields(writer, publisher_domain, installed);
    try writer.writeByte('}');
}

fn writeSearchInstalledExtensionEntry(writer: *std.Io.Writer, publisher_domain: []const u8, first: *bool, installed: extension_domain.InstalledExtension, matched: *usize, text: ?[]const u8) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    matched.* += 1;
    try writer.writeByte('{');
    try writeInstalledExtensionFields(writer, publisher_domain, installed);
    try writer.writeAll(",\"score\":");
    try writer.print("{d}", .{if (text == null or text.?.len == 0) @as(u16, 100) else 90});
    try writer.writeAll(",\"source\":\"/ard/v1/catalog\"}");
}

fn writeSearchExtensionMcpEntry(writer: *std.Io.Writer, publisher_domain: []const u8, first: *bool, installed: extension_domain.InstalledExtension, matched: *usize, text: ?[]const u8) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    matched.* += 1;
    try writer.writeByte('{');
    try writeExtensionMcpFields(writer, publisher_domain, installed);
    try writer.writeAll(",\"score\":");
    try writer.print("{d}", .{if (text == null or text.?.len == 0) @as(u16, 100) else 90});
    try writer.writeAll(",\"source\":\"/ard/v1/catalog\"}");
}

fn writeInstalledExtensionFields(writer: *std.Io.Writer, publisher_domain: []const u8, installed: extension_domain.InstalledExtension) !void {
    try writer.writeAll("\"identifier\":");
    try writeStringFmt(writer, "urn:ai:{s}:antfly:extension:{s}:installed", .{ publisher_domain, installed.name });
    try writer.writeAll(",\"displayName\":");
    try writeStringFmt(writer, "Antfly Extension {s}", .{installed.name});
    try writer.writeAll(",\"type\":\"application/antfly-installed-extension+json\",\"description\":");
    try writeStringFmt(writer, "Installed Antfly extension {s}.", .{installed.name});
    try writer.writeAll(",\"data\":{\"name\":");
    try std.json.Stringify.value(installed.name, .{}, writer);
    try writer.writeAll(",\"packageName\":");
    try std.json.Stringify.value(installed.package_name, .{}, writer);
    try writer.writeAll(",\"packageVersion\":");
    try std.json.Stringify.value(installed.package_version, .{}, writer);
    try writer.writeAll(",\"status\":");
    try std.json.Stringify.value(@tagName(installed.status), .{}, writer);
    try writer.writeAll(",\"scope\":");
    try writeExtensionScope(writer, installed.scope);
    try writer.writeAll("},\"tags\":[\"extension\",\"installed\"],\"capabilities\":");
    try writeCapabilitiesFromGrants(writer, installed.granted_capabilities);
    try writer.writeAll(",\"metadata\":{\"digest\":");
    try std.json.Stringify.value(installed.package_digest, .{}, writer);
    try writer.writeAll(",\"endpoint\":");
    try writeStringFmt(writer, "/extensions/v1/installed/{s}", .{installed.name});
    try writer.writeAll(",\"status\":");
    try std.json.Stringify.value(@tagName(installed.status), .{}, writer);
    try writer.writeByte('}');
}

fn writeExtensionMcpFields(writer: *std.Io.Writer, publisher_domain: []const u8, installed: extension_domain.InstalledExtension) !void {
    try writer.writeAll("\"identifier\":");
    try writeStringFmt(writer, "urn:ai:{s}:antfly:extension:{s}:mcp", .{ publisher_domain, installed.name });
    try writer.writeAll(",\"displayName\":");
    try writeStringFmt(writer, "Antfly Extension MCP {s}", .{installed.name});
    try writer.writeAll(",\"type\":\"application/mcp-server+json\",\"description\":");
    try writeStringFmt(writer, "MCP server for visible tools owned by Antfly extension {s}.", .{installed.name});
    try writer.writeAll(",\"data\":{\"name\":");
    try std.json.Stringify.value(installed.name, .{}, writer);
    try writer.writeAll(",\"endpoint\":");
    try writeStringFmt(writer, "/mcp/v1/extensions/{s}", .{installed.name});
    try writer.writeAll("},\"tags\":[\"mcp\",\"extension\"],\"capabilities\":");
    try writeCapabilitiesFromGrants(writer, installed.granted_capabilities);
    try writer.writeAll(",\"metadata\":{\"endpoint\":");
    try writeStringFmt(writer, "/mcp/v1/extensions/{s}", .{installed.name});
    try writer.writeAll(",\"extension\":");
    try std.json.Stringify.value(installed.name, .{}, writer);
    try writer.writeByte('}');
}

fn writeExtensionScope(writer: *std.Io.Writer, scope: extension_domain.ExtensionScope) !void {
    try writer.writeAll("{\"kind\":");
    try std.json.Stringify.value(@tagName(scope.kind), .{}, writer);
    if (scope.kind == .table) {
        try writer.writeAll(",\"tableName\":");
        try std.json.Stringify.value(scope.table_name, .{}, writer);
    }
    try writer.writeByte('}');
}

fn writeCapabilitiesFromGrants(writer: *std.Io.Writer, capabilities: []const extension_domain.Capability) !void {
    try writer.writeByte('[');
    for (capabilities, 0..) |capability, index| {
        if (index > 0) try writer.writeByte(',');
        try std.json.Stringify.value(capability.name, .{}, writer);
    }
    try writer.writeByte(']');
}

fn installedExtensionVisible(installed: extension_domain.InstalledExtension, permissions: ?[]const usermgr.Permission) bool {
    if (installed.status != .ready) return false;
    const perms = permissions orelse return true;
    if (installed.scope.kind == .table) return identityHasPermission(perms, .table, installed.scope.table_name, .read);
    return identityHasPermission(perms, .@"*", "*", .admin);
}

fn installedExtensionHasVisibleMcpTool(
    alloc: std.mem.Allocator,
    installed: extension_domain.InstalledExtension,
    members: []const extension_domain.ExtensionMember,
    permissions: ?[]const usermgr.Permission,
) !bool {
    if (installed.status != .ready) return false;
    for (members) |member| {
        if (member.object_kind != .mcp_tool) continue;
        if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
        if (try extensionMcpMemberVisible(alloc, installed, member, permissions)) return true;
    }
    return false;
}

fn extensionMcpMemberVisible(
    alloc: std.mem.Allocator,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    permissions: ?[]const usermgr.Permission,
) !bool {
    const perms = permissions orelse return true;
    const required = try requiredCapabilitiesAlloc(alloc, member.owner_metadata_json);
    defer freeParsedCapabilities(alloc, required);
    if (required.len == 0) return extensionScopeVisible(installed, member, perms);
    var checked_table_permission = false;
    for (required) |capability| {
        const permission_type = permissionTypeForCapability(capability.name) orelse continue;
        checked_table_permission = true;
        const table = tableResourceForCapability(installed, member, capability);
        if (!identityHasPermission(perms, .table, table, permission_type)) return false;
    }
    return checked_table_permission or extensionScopeVisible(installed, member, perms);
}

fn extensionScopeVisible(installed: extension_domain.InstalledExtension, member: extension_domain.ExtensionMember, permissions: []const usermgr.Permission) bool {
    if (member.scope.kind == .table) return identityHasPermission(permissions, .table, member.scope.table_name, .read);
    if (installed.scope.kind == .table) return identityHasPermission(permissions, .table, installed.scope.table_name, .read);
    return identityHasPermission(permissions, .@"*", "*", .admin);
}

fn permissionTypeForCapability(name: []const u8) ?usermgr.PermissionType {
    if (std.mem.eql(u8, name, "db:read") or std.mem.eql(u8, name, "read:table")) return .read;
    if (std.mem.eql(u8, name, "db:write") or std.mem.eql(u8, name, "write:table")) return .write;
    if (std.mem.eql(u8, name, "db:admin") or std.mem.eql(u8, name, "admin:table")) return .admin;
    return null;
}

fn tableResourceForCapability(installed: extension_domain.InstalledExtension, member: extension_domain.ExtensionMember, capability: extension_domain.Capability) []const u8 {
    if (capability.scope.len != 0 and !std.mem.eql(u8, capability.scope, installed.package_name)) return capability.scope;
    if (member.scope.kind == .table) return member.scope.table_name;
    if (installed.scope.kind == .table) return installed.scope.table_name;
    return "*";
}

fn identityHasPermission(permissions: []const usermgr.Permission, resource_type: usermgr.ResourceType, resource: []const u8, permission_type: usermgr.PermissionType) bool {
    for (permissions) |permission| {
        const type_match = permission.resource_type == .@"*" or permission.resource_type == resource_type;
        const resource_match = std.mem.eql(u8, permission.resource, "*") or std.mem.eql(u8, permission.resource, resource);
        if (!type_match or !resource_match) continue;
        if (permission.type == .admin or permission.type == permission_type) return true;
    }
    return false;
}

fn requiredCapabilitiesAlloc(alloc: std.mem.Allocator, metadata_json: []const u8) ![]extension_domain.Capability {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, metadata_json, .{}) catch return &.{};
    defer parsed.deinit();
    if (parsed.value != .object) return &.{};
    const value = parsed.value.object.get("required_capabilities") orelse return &.{};
    if (value != .array) return &.{};
    var out = std.ArrayListUnmanaged(extension_domain.Capability).empty;
    errdefer {
        freeParsedCapabilityValues(alloc, out.items);
        out.deinit(alloc);
    }
    for (value.array.items) |item| {
        switch (item) {
            .string => |name| {
                const owned_name = try alloc.dupe(u8, name);
                errdefer alloc.free(owned_name);
                try out.append(alloc, .{ .name = owned_name });
            },
            .object => |object| {
                const name_value = object.get("name") orelse continue;
                if (name_value != .string) continue;
                const scope_value = object.get("scope");
                const scope = if (scope_value) |scope_inner| switch (scope_inner) {
                    .string => |scope_text| scope_text,
                    else => "",
                } else "";
                const owned_name = try alloc.dupe(u8, name_value.string);
                errdefer alloc.free(owned_name);
                const owned_scope = if (scope.len == 0) "" else try alloc.dupe(u8, scope);
                errdefer if (owned_scope.len > 0) alloc.free(owned_scope);
                try out.append(alloc, .{ .name = owned_name, .scope = owned_scope });
            },
            else => {},
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn freeParsedCapabilities(alloc: std.mem.Allocator, capabilities: []const extension_domain.Capability) void {
    freeParsedCapabilityValues(alloc, capabilities);
    if (capabilities.len > 0) alloc.free(@constCast(capabilities));
}

fn freeParsedCapabilityValues(alloc: std.mem.Allocator, capabilities: []const extension_domain.Capability) void {
    for (capabilities) |capability| {
        alloc.free(capability.name);
        if (capability.scope.len > 0) alloc.free(capability.scope);
    }
}

fn dynamicEntryMatches(name: []const u8, media_type: []const u8, description: []const u8, text: ?[]const u8, filter: ?std.json.Value, publisher_domain: []const u8) bool {
    if (text) |query| {
        if (std.mem.trim(u8, query, " \t\r\n").len > 0 and
            !containsIgnoreCase(name, query) and
            !containsIgnoreCase(media_type, query) and
            !containsIgnoreCase(description, query)) return false;
    }
    if (filter) |filter_value| {
        var iterator = filter_value.object.iterator();
        while (iterator.next()) |kv| {
            const key = kv.key_ptr.*;
            const value = kv.value_ptr.*;
            if (std.mem.eql(u8, key, "type")) {
                if (!jsonValueMatchesString(value, media_type)) return false;
            } else if (std.mem.eql(u8, key, "tags")) {
                if (!jsonValueMatchesString(value, "extension") and !jsonValueMatchesString(value, "mcp")) return false;
            } else if (std.mem.eql(u8, key, "publisher") or std.mem.eql(u8, key, "publisherId")) {
                if (!jsonValueMatchesString(value, publisher_domain)) return false;
            } else {
                return false;
            }
        }
    }
    return true;
}

fn isAgentLike(entry: Entry) bool {
    return std.mem.eql(u8, entry.media_type, "application/a2a-agent-card+json") or
        std.mem.eql(u8, entry.media_type, "application/mcp-server+json");
}

fn skillEntry(skill: Skill) Entry {
    return .{
        .identifier_suffix = skill.slug,
        .display_name = skill.display_name,
        .media_type = "application/ai-skill+md",
        .description = skill.description,
        .url = skill.url,
        .metadata = "{\"scope\":\"tenant\"}",
        .tags = &.{ "skill", "workflow" },
        .capabilities = skill.capabilities,
        .representative_queries = skill.representative_queries,
    };
}

fn findSkill(slug: []const u8) ?Skill {
    for (skills) |skill| {
        if (std.mem.eql(u8, skill.slug, slug)) return skill;
    }
    return null;
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

fn writeSearchEntry(
    writer: *std.Io.Writer,
    publisher_domain: []const u8,
    first: *bool,
    entry: Entry,
    matched: *usize,
    text: ?[]const u8,
) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    matched.* += 1;
    try entry.writeSearchResult(writer, publisher_domain, if (text == null or text.?.len == 0) 100 else 90);
}

fn entryMatches(entry: Entry, publisher_domain: []const u8, text: ?[]const u8, filter: ?std.json.Value) bool {
    if (text) |query| {
        if (std.mem.trim(u8, query, " \t\r\n").len > 0 and !entryTextMatches(entry, query)) return false;
    }
    if (filter) |filter_value| {
        var iterator = filter_value.object.iterator();
        while (iterator.next()) |kv| {
            if (!entryMatchesFilter(entry, publisher_domain, kv.key_ptr.*, kv.value_ptr.*)) return false;
        }
    }
    return true;
}

fn entryTextMatches(entry: Entry, query: []const u8) bool {
    return containsIgnoreCase(entry.display_name, query) or
        containsIgnoreCase(entry.description, query) or
        containsIgnoreCase(entry.media_type, query) or
        containsIgnoreCase(entry.identifier_suffix, query) or
        anyContainsIgnoreCase(entry.tags, query) or
        anyContainsIgnoreCase(entry.capabilities, query) or
        anyContainsIgnoreCase(entry.representative_queries, query);
}

fn entryMatchesFilter(entry: Entry, publisher_domain: []const u8, key: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, key, "type")) return jsonValueMatchesString(value, entry.media_type);
    if (std.mem.eql(u8, key, "tags")) return jsonValueMatchesAnyString(value, entry.tags);
    if (std.mem.eql(u8, key, "capabilities")) return jsonValueMatchesAnyString(value, entry.capabilities);
    if (std.mem.eql(u8, key, "publisher") or std.mem.eql(u8, key, "publisherId")) return jsonValueMatchesString(value, publisher_domain);
    if (std.mem.eql(u8, key, "metadata.endpoint")) return entry.metadata != null and containsJsonStringField(entry.metadata.?, "endpoint", value);
    return false;
}

fn jsonValueMatchesString(value: std.json.Value, expected: []const u8) bool {
    return switch (value) {
        .string => |actual| std.mem.eql(u8, actual, expected),
        .array => |array| blk: {
            for (array.items) |item| {
                if (jsonValueMatchesString(item, expected)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

fn jsonValueMatchesAnyString(value: std.json.Value, expected_values: []const []const u8) bool {
    for (expected_values) |expected| {
        if (jsonValueMatchesString(value, expected)) return true;
    }
    return false;
}

fn containsJsonStringField(json: []const u8, field: []const u8, value: std.json.Value) bool {
    return switch (value) {
        .string => |expected| std.mem.indexOf(u8, json, field) != null and std.mem.indexOf(u8, json, expected) != null,
        .array => |array| blk: {
            for (array.items) |item| {
                if (containsJsonStringField(json, field, item)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

fn anyContainsIgnoreCase(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| {
        if (containsIgnoreCase(value, needle)) return true;
    }
    return false;
}

fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    const trimmed = std.mem.trim(u8, needle, " \t\r\n");
    if (trimmed.len == 0) return true;
    if (trimmed.len > haystack.len) return false;
    var i: usize = 0;
    while (i + trimmed.len <= haystack.len) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + trimmed.len], trimmed)) return true;
    }
    return false;
}

fn writeStringArray(writer: *std.Io.Writer, values: []const []const u8) !void {
    try writer.writeByte('[');
    for (values, 0..) |value, index| {
        if (index > 0) try writer.writeByte(',');
        try std.json.Stringify.value(value, .{}, writer);
    }
    try writer.writeByte(']');
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
    try std.testing.expect(entries.len >= 9);
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

test "ARD search filters scoped catalog entries" {
    const body = try searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"retrieval\",\"filter\":{\"type\":[\"application/ai-skill+md\"]}},\"federation\":\"none\"}", false);
    defer std.testing.allocator.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();

    const results = parsed.value.object.get("results").?.array.items;
    try std.testing.expect(results.len >= 1);
    for (results) |result| {
        try std.testing.expectEqualStrings("application/ai-skill+md", result.object.get("type").?.string);
    }
}
