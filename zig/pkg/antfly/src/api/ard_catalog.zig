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
const ard_query = @import("ard/query.zig");
const extension_domain = @import("../extensions/mod.zig");
const usermgr = @import("../usermgr/mod.zig");

pub const CatalogMode = enum {
    public_bootstrap,
    tenant,
};

pub const CatalogOptions = struct {
    mode: CatalogMode = .public_bootstrap,
    base_url: ?[]const u8 = null,
    publisher_domain: []const u8 = "antfly.local",
    display_name: []const u8 = "Antfly",
    a2a_enabled: bool = false,
    is_admin: bool = false,
    permissions: ?[]const usermgr.Permission = null,
    profile: ?[]const u8 = null,
    types: ?[]const u8 = null,
    include: ?[]const u8 = null,
};

pub const ExtensionCatalogContext = struct {
    extension_packages: []const extension_domain.PackageManifest = &.{},
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
    trust_manifest: ?[]const u8 = null,
    tags: []const []const u8 = &.{},
    capabilities: []const []const u8 = &.{},
    representative_queries: []const []const u8 = &.{},
    admin_only: bool = false,
    required_permission: RequiredPermission = .none,

    fn write(self: Entry, writer: *std.Io.Writer, options: CatalogOptions) !void {
        try writer.writeByte('{');
        try self.writeFields(writer, options);
        try writer.writeByte('}');
    }

    fn writeSearchResult(self: Entry, writer: *std.Io.Writer, options: CatalogOptions, score: u16) !void {
        try writer.writeByte('{');
        try self.writeFields(writer, options);
        try writer.writeAll(",\"score\":");
        try writer.print("{d}", .{score});
        try writer.writeAll(",\"source\":\"/ard/v1/catalog\"}");
    }

    fn writeFields(self: Entry, writer: *std.Io.Writer, options: CatalogOptions) !void {
        try writer.writeAll("\"identifier\":");
        try writeStringFmt(writer, "urn:ai:{s}:antfly:{s}", .{ options.publisher_domain, self.identifier_suffix });
        try writer.writeAll(",\"displayName\":");
        try std.json.Stringify.value(self.display_name, .{}, writer);
        try writer.writeAll(",\"type\":");
        try std.json.Stringify.value(self.media_type, .{}, writer);
        try writer.writeAll(",\"description\":");
        try std.json.Stringify.value(self.description, .{}, writer);
        if (self.url) |url| {
            try writer.writeAll(",\"url\":");
            try writeUrlValue(writer, options.base_url, url);
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
        if (self.trust_manifest) |trust_manifest| {
            try writer.writeAll(",\"trustManifest\":");
            try writer.writeAll(trust_manifest);
        }
    }
};

const RequiredPermission = enum {
    none,
    table_read,
    table_admin,
    admin,
};

const default_skills_manifest_json = @embedFile("ard/default_skills.json");

const DefaultSkillManifest = struct {
    slug: []const u8,
    identifier_suffix: ?[]const u8 = null,
    url: []const u8,
    display_name: []const u8,
    description: []const u8,
    metadata: []const u8 = "{\"scope\":\"tenant\"}",
    tags: []const []const u8 = &.{ "skill", "workflow" },
    capabilities: []const []const u8,
    representative_queries: []const []const u8,
    body: []const u8,
    admin_only: bool = false,
    required_permission: []const u8 = "none",
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
        .identifier_suffix = "openapi:ard",
        .display_name = "Antfly ARD OpenAPI",
        .media_type = "application/openapi+yaml",
        .description = "Machine-readable OpenAPI specification for Antfly ARD discovery APIs.",
        .url = "/ard/v1/openapi.yaml",
        .metadata = "{\"sourceSpec\":\"ard:v1\",\"requiredPermissions\":\"tenant-api\"}",
        .tags = &.{ "openapi", "api", "public" },
        .capabilities = &.{ "table-management", "query", "retrieval", "extensions" },
        .representative_queries = &.{ "call the Antfly table query API", "manage Antfly extensions through HTTP", "inspect table schemas through OpenAPI" },
    },
    .{
        .identifier_suffix = "openapi:public",
        .display_name = "Antfly Public OpenAPI",
        .media_type = "application/openapi+yaml",
        .description = "Joined public OpenAPI specification for Antfly server APIs.",
        .url = "/ard/v1/openapi/antfly.yaml",
        .metadata = "{\"sourceSpec\":\"openapi.yaml\",\"requiredPermissions\":\"tenant-api\"}",
        .tags = &.{ "openapi", "api", "public" },
        .capabilities = &.{ "table-management", "query", "retrieval", "transactions", "extensions", "auth" },
        .representative_queries = &.{ "call Antfly APIs from an OpenAPI client", "generate an Antfly SDK", "inspect Antfly API request schemas" },
    },
    .{
        .identifier_suffix = "openapi:metadata",
        .display_name = "Antfly Metadata OpenAPI",
        .media_type = "application/openapi+yaml",
        .description = "OpenAPI specification for Antfly table, transaction, retrieval, and admin metadata APIs.",
        .url = "/ard/v1/openapi/metadata.yaml",
        .metadata = "{\"sourceSpec\":\"specs/openapi/antfly/metadata.yaml\",\"requiredPermissions\":\"tenant-api\"}",
        .tags = &.{ "openapi", "api", "metadata" },
        .capabilities = &.{ "table-management", "query", "retrieval", "transactions", "cluster-status" },
        .representative_queries = &.{ "inspect Antfly table APIs", "call the retrieval agent API", "manage Antfly transactions" },
    },
    .{
        .identifier_suffix = "openapi:extensions",
        .display_name = "Antfly Extensions OpenAPI",
        .media_type = "application/openapi+yaml",
        .description = "OpenAPI specification for extension package and lifecycle management.",
        .url = "/ard/v1/openapi/extensions.yaml",
        .metadata = "{\"sourceSpec\":\"specs/openapi/extensions/api.yaml\",\"requiredPermissions\":\"admin\"}",
        .tags = &.{ "openapi", "api", "extensions", "admin" },
        .capabilities = &.{ "extension-install", "extension-config", "extension-lifecycle", "package-catalog" },
        .representative_queries = &.{ "install an Antfly extension through OpenAPI", "inspect installed extension config", "list extension packages" },
        .admin_only = true,
    },
    .{
        .identifier_suffix = "openapi:auth",
        .display_name = "Antfly Auth OpenAPI",
        .media_type = "application/openapi+yaml",
        .description = "OpenAPI specification for user, API key, permission, role, and row-filter management.",
        .url = "/ard/v1/openapi/auth.yaml",
        .metadata = "{\"sourceSpec\":\"specs/openapi/auth/api.yaml\",\"requiredPermissions\":\"admin\"}",
        .tags = &.{ "openapi", "api", "auth", "admin" },
        .capabilities = &.{ "user-management", "api-key-management", "permissions", "row-filters", "roles" },
        .representative_queries = &.{ "create an Antfly API key", "assign table permissions to a user", "configure row filters" },
        .admin_only = true,
    },
    .{
        .identifier_suffix = "openapi:inference-config",
        .display_name = "Antfly Inference OpenAPI",
        .media_type = "application/openapi+yaml",
        .description = "OpenAPI specification for Antfly inference and model-serving APIs.",
        .url = "/ard/v1/openapi/inference-config.yaml",
        .metadata = "{\"sourceSpec\":\"specs/openapi/inference/config.yaml\",\"requiredPermissions\":\"inference-api\"}",
        .tags = &.{ "openapi", "api", "inference" },
        .capabilities = &.{ "embedding", "chunking", "reranking", "generation", "extraction", "transcription" },
        .representative_queries = &.{ "embed text through Antfly inference", "rerank search results", "list local inference models" },
    },
};

const aggregate_mcp_entry = Entry{
    .identifier_suffix = "mcp:default",
    .display_name = "Antfly MCP Server",
    .media_type = "application/mcp-server+json",
    .description = "Aggregate Antfly MCP server for built-in and visible extension tools.",
    .url = "/ard/v1/resources/mcp/default",
    .metadata = "{\"endpoint\":\"/mcp/v1\"}",
    .tags = &.{ "mcp", "tools" },
    .capabilities = &.{ "table-search", "retrieval", "query-builder", "extension-tools" },
    .representative_queries = &.{ "search an Antfly table", "list extension MCP tools", "run a retrieval workflow" },
};

const copilot_mcp_profile_entry = Entry{
    .identifier_suffix = "mcp-profile:copilot",
    .display_name = "Antfly Copilot MCP Profile",
    .media_type = "application/mcp-server+json",
    .description = "Profile-scoped Antfly MCP endpoint for Copilot-style clients.",
    .url = "/ard/v1/resources/mcp/profiles/copilot",
    .metadata = "{\"endpoint\":\"/mcp/v1/extensions/profiles/copilot\",\"profile\":\"copilot\"}",
    .tags = &.{ "mcp", "tools", "profile", "copilot" },
    .capabilities = &.{ "table-search", "retrieval", "query-builder", "extension-tools" },
    .representative_queries = &.{ "search Antfly from Copilot", "list Copilot-visible extension MCP tools" },
};

pub fn catalogJsonAlloc(alloc: std.mem.Allocator, options: CatalogOptions) ![]u8 {
    return try catalogJsonWithExtensionsAlloc(alloc, options, null);
}

pub fn catalogJsonWithExtensionsAlloc(alloc: std.mem.Allocator, options: CatalogOptions, extension_context: ?ExtensionCatalogContext) ![]u8 {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();

    try writeCatalogPrefix(&writer.writer, options);
    var first = true;
    try writeScopedEntries(alloc, &writer.writer, options, &first, null, null);
    if (options.mode == .tenant) {
        try writeAggregateMcpEntry(alloc, &writer.writer, options, &first, extension_context, null, null);
        try writeCopilotMcpProfileEntry(alloc, &writer.writer, options, &first, extension_context, null, null);
        if (extension_context) |ctx| try writeExtensionEntries(alloc, &writer.writer, options, &first, ctx, null, null);
    }
    try writer.writer.writeAll("]}");
    return try writer.toOwnedSlice();
}

pub fn searchJsonAlloc(alloc: std.mem.Allocator, options: CatalogOptions, body: []const u8, explore: bool) ![]u8 {
    return try searchJsonWithExtensionsAlloc(alloc, options, body, explore, null);
}

pub fn searchJsonWithExtensionsAlloc(alloc: std.mem.Allocator, options: CatalogOptions, body: []const u8, explore: bool, extension_context: ?ExtensionCatalogContext) ![]u8 {
    const request = try ard_query.parseSearchRequest(alloc, body);
    defer request.deinit();
    if (!explore and (request.text == null or std.mem.trim(u8, request.text.?, " \t\r\n").len == 0)) return error.InvalidArdSearchRequest;
    if (explore) return try exploreJsonWithExtensionsAlloc(alloc, options, request, extension_context);

    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();

    try writer.writer.writeAll("{\"results\":[");
    var first = true;
    var matched: usize = 0;
    try writeMatchedEntries(alloc, &writer.writer, options, &first, request.text, request.filter, request.page_start, request.page_size, &matched);
    if (options.mode == .tenant) {
        try writeSearchAggregateMcpEntry(alloc, &writer.writer, options, &first, extension_context, request.text, request.filter, request.page_start, request.page_size, &matched);
        try writeSearchCopilotMcpProfileEntry(alloc, &writer.writer, options, &first, extension_context, request.text, request.filter, request.page_start, request.page_size, &matched);
        if (extension_context) |ctx| try writeMatchedExtensionEntries(alloc, &writer.writer, options, &first, ctx, request.text, request.filter, request.page_start, request.page_size, &matched);
    }
    try writer.writer.writeAll("],\"federation\":");
    try std.json.Stringify.value(request.federation.name(), .{}, &writer.writer);
    if (request.federation.includesReferrals()) try writer.writer.writeAll(",\"referrals\":[]");
    try writer.writer.writeAll(",\"count\":");
    try writer.writer.print("{d}", .{matched});
    const page_end = request.page_start + request.page_size;
    if (matched > page_end) {
        try writer.writer.writeAll(",\"pageToken\":");
        try writeStringFmt(&writer.writer, "{d}", .{page_end});
    }
    try writer.writer.writeByte('}');
    return try writer.toOwnedSlice();
}

fn exploreJsonWithExtensionsAlloc(
    alloc: std.mem.Allocator,
    options: CatalogOptions,
    request: ard_query.SearchRequest,
    extension_context: ?ExtensionCatalogContext,
) ![]u8 {
    var facets = try initFacetAccumulators(request);
    defer deinitFacetAccumulators(alloc, facets.slice());

    var matched: usize = 0;
    try collectStaticFacets(alloc, facets.slice(), options, request.text, request.filter, &matched);
    if (options.mode == .tenant) {
        try collectAggregateMcpFacets(alloc, facets.slice(), options, extension_context, request.text, request.filter, &matched);
        try collectCopilotMcpProfileFacets(alloc, facets.slice(), options, extension_context, request.text, request.filter, &matched);
        if (extension_context) |ctx| try collectExtensionFacets(alloc, facets.slice(), options, ctx, request.text, request.filter, &matched);
    }

    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();
    try writer.writer.writeAll("{\"resultType\":\"facets\",\"facets\":{");
    for (facets.slice(), 0..) |facet, index| {
        if (index > 0) try writer.writer.writeByte(',');
        try std.json.Stringify.value(facet.field, .{}, &writer.writer);
        try writer.writer.writeAll(":{\"buckets\":[");
        for (facet.buckets.items, 0..) |bucket, bucket_index| {
            if (bucket_index > 0) try writer.writer.writeByte(',');
            try writer.writer.writeAll("{\"value\":");
            try std.json.Stringify.value(bucket.value, .{}, &writer.writer);
            try writer.writer.writeAll(",\"count\":");
            try writer.writer.print("{d}", .{bucket.count});
            try writer.writer.writeByte('}');
        }
        try writer.writer.writeAll("],\"otherCount\":0}");
    }
    try writer.writer.writeAll("},\"count\":");
    try writer.writer.print("{d}", .{matched});
    try writer.writer.writeByte('}');
    return try writer.toOwnedSlice();
}

pub fn agentsJsonAlloc(alloc: std.mem.Allocator, options: CatalogOptions) ![]u8 {
    return try agentsJsonWithExtensionsAlloc(alloc, options, null);
}

pub fn agentsJsonWithExtensionsAlloc(alloc: std.mem.Allocator, options: CatalogOptions, extension_context: ?ExtensionCatalogContext) ![]u8 {
    return try agentsJsonWithExtensionsQueryAlloc(alloc, options, "", extension_context);
}

pub fn agentsJsonWithExtensionsQueryAlloc(alloc: std.mem.Allocator, options: CatalogOptions, query: []const u8, extension_context: ?ExtensionCatalogContext) ![]u8 {
    const request = try ard_query.parseAgentsRequest(alloc, query);
    defer request.deinit();

    var agents = std.ArrayListUnmanaged(AgentOutput).empty;
    defer {
        for (agents.items) |*agent| agent.deinit(alloc);
        agents.deinit(alloc);
    }

    try collectAgentOutputs(alloc, &agents, options, extension_context, request.filter);
    if (request.order_by.field != .natural) {
        std.mem.sort(AgentOutput, agents.items, request.order_by, agentOrderLessThan);
    }

    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();

    try writer.writer.writeAll("{\"agents\":[");
    var first = true;
    const page_end = request.page_start + request.page_size;
    for (agents.items, 0..) |agent, index| {
        if (index < request.page_start or index >= page_end) continue;
        if (first) {
            first = false;
        } else {
            try writer.writer.writeByte(',');
        }
        try writer.writer.writeAll(agent.json);
    }
    try writer.writer.writeAll("],\"count\":");
    try writer.writer.print("{d}", .{agents.items.len});
    if (agents.items.len > page_end) {
        try writer.writer.writeAll(",\"pageToken\":");
        try writeStringFmt(&writer.writer, "{d}", .{page_end});
    }
    try writer.writer.writeByte('}');
    return try writer.toOwnedSlice();
}

const AgentOutput = struct {
    json: []u8,
    identifier: []u8,
    display_name: []u8,
    media_type: []const u8,

    fn deinit(self: *AgentOutput, alloc: std.mem.Allocator) void {
        alloc.free(self.json);
        alloc.free(self.identifier);
        alloc.free(self.display_name);
    }
};

pub fn skillMarkdownAlloc(alloc: std.mem.Allocator, options: CatalogOptions, slug: []const u8) !?[]u8 {
    var parsed_skills = try parseDefaultSkills(alloc);
    defer parsed_skills.deinit();
    const skill = findDefaultSkill(parsed_skills.value, slug) orelse return null;
    const entry = try defaultSkillEntry(skill);
    if (!catalogOptionsAllowStaticEntry(options, entry)) return null;
    return try alloc.dupe(u8, skill.body);
}

pub fn extensionSkillMarkdownAlloc(alloc: std.mem.Allocator, options: CatalogOptions, route: []const u8, ctx: ExtensionCatalogContext) !?[]u8 {
    const parsed_route = parseExtensionSkillRoute(route) orelse return null;
    for (ctx.installed_extensions) |installed| {
        if (!std.mem.eql(u8, installed.name, parsed_route.extension_name)) continue;
        for (ctx.extension_members) |member| {
            if (member.object_kind != .skill) continue;
            if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
            if (!std.mem.eql(u8, member.object_name, parsed_route.skill_name)) continue;
            if (!(try extensionMemberVisible(alloc, installed, member, ctx.permissions))) return null;
            var skill = try parseExtensionSkillDescriptor(alloc, installed, member);
            defer skill.deinit();
            if (!extensionSkillAllowedAndMatches(options, installed, member, skill, null, null)) return null;
            if (skill.body.len > 0) return try alloc.dupe(u8, skill.body);
            return try extensionSkillMarkdownFromDescriptorAlloc(alloc, installed, member, skill);
        }
    }
    return null;
}

pub fn mcpDescriptorJsonAlloc(alloc: std.mem.Allocator, name: []const u8, options: CatalogOptions, extension_context: ?ExtensionCatalogContext) !?[]u8 {
    if (std.mem.eql(u8, name, "profiles/copilot")) {
        if (!(try copilotMcpProfileVisible(alloc, extension_context))) return null;
        if (!catalogOptionsAllowStaticEntry(options, copilot_mcp_profile_entry)) return null;
        return try copilotMcpProfileDescriptorJsonAlloc(alloc, options.base_url, extension_context);
    }
    if (std.mem.startsWith(u8, name, "extensions/")) {
        const extension_name = name["extensions/".len..];
        if (extension_name.len == 0 or std.mem.indexOfScalar(u8, extension_name, '/') != null) return null;
        const ctx = extension_context orelse return null;
        for (ctx.installed_extensions) |installed| {
            if (!std.mem.eql(u8, installed.name, extension_name)) continue;
            if (!(try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions))) return null;
            if (!catalogOptionsAllowMedia(options, "application/mcp-server+json", &.{ "mcp", "extension" })) return null;
            return try extensionMcpDescriptorJsonAlloc(alloc, installed, options.base_url, ctx);
        }
        return null;
    }
    if (!std.mem.eql(u8, name, "default")) return null;
    if (!(try aggregateMcpVisible(alloc, extension_context))) return null;
    if (!catalogOptionsAllowStaticEntry(options, aggregate_mcp_entry)) return null;
    return try aggregateMcpDescriptorJsonAlloc(alloc, options.base_url, extension_context);
}

pub fn agentDescriptorJsonAlloc(alloc: std.mem.Allocator, name: []const u8, options: CatalogOptions, extension_context: ?ExtensionCatalogContext) !?[]u8 {
    const parsed_route = parseExtensionAgentRoute(name) orelse return null;
    const ctx = extension_context orelse return null;
    for (ctx.installed_extensions) |installed| {
        if (!std.mem.eql(u8, installed.name, parsed_route.extension_name)) continue;
        for (ctx.extension_members) |member| {
            if (member.object_kind != .agent) continue;
            if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
            if (!std.mem.eql(u8, member.object_name, parsed_route.agent_name)) continue;
            if (!(try extensionMemberVisible(alloc, installed, member, ctx.permissions))) return null;
            var agent = try parseExtensionAgentDescriptor(alloc, installed, member);
            defer agent.deinit();
            if (!extensionAgentAllowedAndMatches(options, installed, member, agent, null, null)) return null;
            return try extensionAgentDescriptorJsonAlloc(alloc, installed, member, agent, options.base_url);
        }
    }
    return null;
}

const default_facet_fields = [_][]const u8{ "type", "capabilities", "tags", "publisher" };

const FacetBucket = struct {
    value: []const u8,
    count: usize,
};

const FacetAccumulator = struct {
    field: []const u8,
    buckets: std.ArrayListUnmanaged(FacetBucket) = .empty,

    fn increment(self: *FacetAccumulator, alloc: std.mem.Allocator, value: []const u8) !void {
        if (value.len == 0) return;
        for (self.buckets.items) |*bucket| {
            if (std.mem.eql(u8, bucket.value, value)) {
                bucket.count += 1;
                return;
            }
        }
        const owned_value = try alloc.dupe(u8, value);
        errdefer alloc.free(owned_value);
        try self.buckets.append(alloc, .{ .value = owned_value, .count = 1 });
    }

    fn deinit(self: *FacetAccumulator, alloc: std.mem.Allocator) void {
        for (self.buckets.items) |bucket| alloc.free(bucket.value);
        self.buckets.deinit(alloc);
    }
};

const FacetSet = struct {
    items: [ard_query.SearchRequest.max_facets]FacetAccumulator = undefined,
    count: usize = 0,

    fn slice(self: *FacetSet) []FacetAccumulator {
        return self.items[0..self.count];
    }
};

fn initFacetAccumulators(request: ard_query.SearchRequest) !FacetSet {
    var set: FacetSet = .{};
    const fields = if (request.facet_field_count > 0) request.facetFields() else default_facet_fields[0..];
    for (fields) |field| {
        if (set.count >= ard_query.SearchRequest.max_facets) break;
        set.items[set.count] = .{ .field = field };
        set.count += 1;
    }
    return set;
}

fn deinitFacetAccumulators(alloc: std.mem.Allocator, facets: []FacetAccumulator) void {
    for (facets) |*facet| facet.deinit(alloc);
}

fn writeCatalogPrefix(writer: *std.Io.Writer, options: CatalogOptions) !void {
    try writer.writeAll("{\"specVersion\":\"1.0\",\"host\":{\"displayName\":");
    try std.json.Stringify.value(options.display_name, .{}, writer);
    try writer.writeAll(",\"identifier\":");
    try writeStringFmt(writer, "did:web:{s}", .{options.publisher_domain});
    try writer.writeAll(",\"trustManifest\":");
    try writeTrustManifestPrefix(writer, options.publisher_domain);
    try writer.writeByte('}');
    try writer.writeAll("},\"entries\":[");
}

fn collectStaticFacets(
    alloc: std.mem.Allocator,
    facets: []FacetAccumulator,
    options: CatalogOptions,
    text: ?[]const u8,
    filter: ?std.json.Value,
    matched: *usize,
) !void {
    for (static_entries) |entry| {
        if (catalogOptionsAllowStaticEntry(options, entry) and entryMatches(entry, options.publisher_domain, text, filter)) {
            matched.* += 1;
            try addEntryFacets(alloc, facets, options.publisher_domain, entry);
        }
    }
    if (options.mode == .tenant) {
        for (tenant_entries) |entry| {
            if (catalogOptionsAllowStaticEntry(options, entry) and entryMatches(entry, options.publisher_domain, text, filter)) {
                matched.* += 1;
                try addEntryFacets(alloc, facets, options.publisher_domain, entry);
            }
        }
        var parsed_skills = try parseDefaultSkills(alloc);
        defer parsed_skills.deinit();
        for (parsed_skills.value) |skill| {
            const entry = try defaultSkillEntry(skill);
            if (catalogOptionsAllowStaticEntry(options, entry) and entryMatches(entry, options.publisher_domain, text, filter)) {
                matched.* += 1;
                try addEntryFacets(alloc, facets, options.publisher_domain, entry);
            }
        }
    }
}

fn collectExtensionFacets(
    alloc: std.mem.Allocator,
    facets: []FacetAccumulator,
    options: CatalogOptions,
    ctx: ExtensionCatalogContext,
    text: ?[]const u8,
    filter: ?std.json.Value,
    matched: *usize,
) !void {
    for (ctx.installed_extensions, 0..) |installed, index| {
        const has_visible_mcp = try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions);
        const has_visible_skill = try installedExtensionHasVisibleSkill(alloc, installed, ctx.extension_members, ctx.permissions);
        const has_visible_agent = try installedExtensionHasVisibleAgent(alloc, installed, ctx.extension_members, ctx.permissions);
        const installed_capabilities = try capabilityNamesAlloc(alloc, installed.granted_capabilities);
        defer alloc.free(installed_capabilities);

        const can_expose_extension = try visibleInstalledCanExposeExtension(alloc, installed, ctx);
        if (can_expose_extension) {
            if (findInstalledPackage(ctx.extension_packages, installed)) |package| {
                const package_capabilities = try capabilityNamesAlloc(alloc, package.capabilities_requested);
                defer alloc.free(package_capabilities);
                if (!try visiblePackageAlreadyEmitted(alloc, ctx, package.*, index) and
                    catalogOptionsAllowMedia(options, "application/antfly-extension-package+json", &.{ "extension", "package" }) and
                    extensionPackageEntryMatches(package.*, package_capabilities, text, filter, options.publisher_domain))
                {
                    matched.* += 1;
                    try addDynamicFacets(alloc, facets, options.publisher_domain, "application/antfly-extension-package+json", &.{ "extension", "package" }, package_capabilities);
                }
            }
        }
        if (has_visible_skill) {
            try collectExtensionSkillFacets(alloc, facets, options, ctx, installed, text, filter, matched);
        }
        if (has_visible_agent) {
            try collectExtensionAgentFacets(alloc, facets, options, ctx, installed, text, filter, matched);
        }
        if ((installedExtensionVisible(installed, ctx.permissions) or has_visible_mcp or has_visible_skill or has_visible_agent) and
            catalogOptionsAllowMedia(options, "application/antfly-installed-extension+json", &.{ "extension", "installed" }) and
            installedExtensionEntryMatches(installed, installed_capabilities, text, filter, options.publisher_domain))
        {
            matched.* += 1;
            try addDynamicFacets(alloc, facets, options.publisher_domain, "application/antfly-installed-extension+json", &.{ "extension", "installed" }, installed_capabilities);
        }
        if (has_visible_mcp) {
            if (catalogOptionsAllowMedia(options, "application/mcp-server+json", &.{ "mcp", "extension" }) and
                extensionMcpEntryMatches(installed, installed_capabilities, text, filter, options.publisher_domain))
            {
                matched.* += 1;
                try addDynamicFacets(alloc, facets, options.publisher_domain, "application/mcp-server+json", &.{ "mcp", "extension" }, installed_capabilities);
            }
        }
    }
}

fn addEntryFacets(alloc: std.mem.Allocator, facets: []FacetAccumulator, publisher_domain: []const u8, entry: Entry) !void {
    for (facets) |*facet| {
        if (std.mem.eql(u8, facet.field, "type")) {
            try facet.increment(alloc, entry.media_type);
        } else if (std.mem.eql(u8, facet.field, "publisher") or std.mem.eql(u8, facet.field, "publisherId")) {
            try facet.increment(alloc, publisher_domain);
        } else if (std.mem.eql(u8, facet.field, "tags")) {
            for (entry.tags) |tag| try facet.increment(alloc, tag);
        } else if (std.mem.eql(u8, facet.field, "capabilities")) {
            for (entry.capabilities) |capability| try facet.increment(alloc, capability);
        } else if (std.mem.eql(u8, facet.field, "displayName")) {
            try facet.increment(alloc, entry.display_name);
        }
    }
}

fn addDynamicFacets(
    alloc: std.mem.Allocator,
    facets: []FacetAccumulator,
    publisher_domain: []const u8,
    media_type: []const u8,
    tags: []const []const u8,
    capabilities: []const []const u8,
) !void {
    for (facets) |*facet| {
        if (std.mem.eql(u8, facet.field, "type")) {
            try facet.increment(alloc, media_type);
        } else if (std.mem.eql(u8, facet.field, "publisher") or std.mem.eql(u8, facet.field, "publisherId")) {
            try facet.increment(alloc, publisher_domain);
        } else if (std.mem.eql(u8, facet.field, "tags")) {
            for (tags) |tag| try facet.increment(alloc, tag);
        } else if (std.mem.eql(u8, facet.field, "capabilities")) {
            for (capabilities) |capability| try facet.increment(alloc, capability);
        }
    }
}

fn writeScopedEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    text: ?[]const u8,
    filter: ?std.json.Value,
) !void {
    for (static_entries) |entry| {
        if (catalogOptionsAllowStaticEntry(options, entry) and entryMatches(entry, options.publisher_domain, text, filter)) try writeEntry(writer, options, first, entry);
    }
    if (options.mode == .tenant) {
        for (tenant_entries) |entry| {
            if (catalogOptionsAllowStaticEntry(options, entry) and entryMatches(entry, options.publisher_domain, text, filter)) try writeEntry(writer, options, first, entry);
        }
        var parsed_skills = try parseDefaultSkills(alloc);
        defer parsed_skills.deinit();
        for (parsed_skills.value) |skill| {
            const entry = try defaultSkillEntry(skill);
            if (catalogOptionsAllowStaticEntry(options, entry) and entryMatches(entry, options.publisher_domain, text, filter)) try writeEntry(writer, options, first, entry);
        }
    }
}

fn writeMatchedEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    text: ?[]const u8,
    filter: ?std.json.Value,
    page_start: usize,
    page_size: usize,
    matched: *usize,
) !void {
    for (static_entries) |entry| {
        if (catalogOptionsAllowStaticEntry(options, entry) and entryMatches(entry, options.publisher_domain, text, filter)) try writeSearchEntry(writer, options, first, entry, page_start, page_size, matched, text);
    }
    if (options.mode == .tenant) {
        for (tenant_entries) |entry| {
            if (catalogOptionsAllowStaticEntry(options, entry) and entryMatches(entry, options.publisher_domain, text, filter)) try writeSearchEntry(writer, options, first, entry, page_start, page_size, matched, text);
        }
        var parsed_skills = try parseDefaultSkills(alloc);
        defer parsed_skills.deinit();
        for (parsed_skills.value) |skill| {
            const entry = try defaultSkillEntry(skill);
            if (catalogOptionsAllowStaticEntry(options, entry) and entryMatches(entry, options.publisher_domain, text, filter)) try writeSearchEntry(writer, options, first, entry, page_start, page_size, matched, text);
        }
    }
}

fn writeAgentEntries(writer: *std.Io.Writer, options: CatalogOptions, first: *bool, count: *usize) !void {
    for (static_entries) |entry| {
        if (isAgentLike(entry) and catalogOptionsAllowStaticEntry(options, entry)) {
            try writeEntry(writer, options, first, entry);
            count.* += 1;
        }
    }
    if (options.mode == .tenant) {
        for (tenant_entries) |entry| {
            if (isAgentLike(entry) and catalogOptionsAllowStaticEntry(options, entry)) {
                try writeEntry(writer, options, first, entry);
                count.* += 1;
            }
        }
    }
}

fn collectAgentOutputs(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(AgentOutput),
    options: CatalogOptions,
    extension_context: ?ExtensionCatalogContext,
    filter: ?std.json.Value,
) !void {
    for (static_entries) |entry| {
        if (!isAgentLike(entry) or !catalogOptionsAllowStaticEntry(options, entry) or !entryMatches(entry, options.publisher_domain, null, filter)) continue;
        try appendStaticAgentOutput(alloc, out, options, entry);
    }
    if (options.mode != .tenant) return;
    for (tenant_entries) |entry| {
        if (!isAgentLike(entry) or !catalogOptionsAllowStaticEntry(options, entry) or !entryMatches(entry, options.publisher_domain, null, filter)) continue;
        try appendStaticAgentOutput(alloc, out, options, entry);
    }
    if (try aggregateMcpVisible(alloc, extension_context)) {
        if (catalogOptionsAllowStaticEntry(options, aggregate_mcp_entry) and entryMatches(aggregate_mcp_entry, options.publisher_domain, null, filter)) {
            try appendStaticAgentOutput(alloc, out, options, aggregate_mcp_entry);
        }
    }
    if (try copilotMcpProfileVisible(alloc, extension_context)) {
        if (catalogOptionsAllowStaticEntry(options, copilot_mcp_profile_entry) and entryMatches(copilot_mcp_profile_entry, options.publisher_domain, null, filter)) {
            try appendStaticAgentOutput(alloc, out, options, copilot_mcp_profile_entry);
        }
    }
    if (extension_context) |ctx| {
        for (ctx.installed_extensions) |installed| {
            if (!(try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions))) continue;
            const installed_capabilities = try capabilityNamesAlloc(alloc, installed.granted_capabilities);
            defer alloc.free(installed_capabilities);
            if (!catalogOptionsAllowMedia(options, "application/mcp-server+json", &.{ "mcp", "extension" }) or
                !extensionMcpEntryMatches(installed, installed_capabilities, null, filter, options.publisher_domain)) continue;
            try appendExtensionMcpAgentOutput(alloc, out, options, installed);
        }
        for (ctx.installed_extensions) |installed| {
            if (!(try installedExtensionHasVisibleAgent(alloc, installed, ctx.extension_members, ctx.permissions))) continue;
            for (ctx.extension_members) |member| {
                if (member.object_kind != .agent) continue;
                if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
                if (!(try extensionMemberVisible(alloc, installed, member, ctx.permissions))) continue;
                var agent = try parseExtensionAgentDescriptor(alloc, installed, member);
                defer agent.deinit();
                if (!extensionAgentAllowedAndMatches(options, installed, member, agent, null, filter)) continue;
                try appendExtensionAgentOutput(alloc, out, options, installed, member, agent);
            }
        }
    }
}

fn appendStaticAgentOutput(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(AgentOutput),
    options: CatalogOptions,
    entry: Entry,
) !void {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();
    try entry.write(&writer.writer, options);
    const json = try writer.toOwnedSlice();
    errdefer alloc.free(json);
    const identifier = try std.fmt.allocPrint(alloc, "urn:ai:{s}:antfly:{s}", .{ options.publisher_domain, entry.identifier_suffix });
    errdefer alloc.free(identifier);
    const display_name = try alloc.dupe(u8, entry.display_name);
    errdefer alloc.free(display_name);
    try out.append(alloc, .{
        .json = json,
        .identifier = identifier,
        .display_name = display_name,
        .media_type = entry.media_type,
    });
}

fn appendExtensionMcpAgentOutput(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(AgentOutput),
    options: CatalogOptions,
    installed: extension_domain.InstalledExtension,
) !void {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();
    try writer.writer.writeByte('{');
    try writeExtensionMcpFields(&writer.writer, options, installed);
    try writer.writer.writeByte('}');
    const json = try writer.toOwnedSlice();
    errdefer alloc.free(json);
    const identifier = try std.fmt.allocPrint(alloc, "urn:ai:{s}:antfly:extension:{s}:mcp", .{ options.publisher_domain, installed.name });
    errdefer alloc.free(identifier);
    const display_name = try std.fmt.allocPrint(alloc, "Antfly Extension MCP {s}", .{installed.name});
    errdefer alloc.free(display_name);
    try out.append(alloc, .{
        .json = json,
        .identifier = identifier,
        .display_name = display_name,
        .media_type = "application/mcp-server+json",
    });
}

fn appendExtensionAgentOutput(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(AgentOutput),
    options: CatalogOptions,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    agent: ParsedExtensionAgent,
) !void {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();
    try writer.writer.writeByte('{');
    try writeExtensionAgentFields(&writer.writer, options, installed, member, agent);
    try writer.writer.writeByte('}');
    const json = try writer.toOwnedSlice();
    errdefer alloc.free(json);
    const identifier = try std.fmt.allocPrint(alloc, "urn:ai:{s}:antfly:extension:{s}:agent:{s}", .{ options.publisher_domain, installed.name, member.object_name });
    errdefer alloc.free(identifier);
    const display_name = try alloc.dupe(u8, agent.display_name);
    errdefer alloc.free(display_name);
    try out.append(alloc, .{
        .json = json,
        .identifier = identifier,
        .display_name = display_name,
        .media_type = "application/antfly-agent+json",
    });
}

fn agentOrderLessThan(order: ard_query.AgentOrder, lhs: AgentOutput, rhs: AgentOutput) bool {
    const lhs_value = agentOrderValue(lhs, order.field);
    const rhs_value = agentOrderValue(rhs, order.field);
    const ordered = std.mem.order(u8, lhs_value, rhs_value);
    if (ordered == .eq) return std.mem.order(u8, lhs.identifier, rhs.identifier) == .lt;
    return if (order.desc) ordered == .gt else ordered == .lt;
}

fn agentOrderValue(agent: AgentOutput, field: ard_query.AgentOrder.Field) []const u8 {
    return switch (field) {
        .natural, .identifier => agent.identifier,
        .displayName => agent.display_name,
        .type => agent.media_type,
    };
}

fn writeExtensionEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    ctx: ExtensionCatalogContext,
    text: ?[]const u8,
    filter: ?std.json.Value,
) !void {
    for (ctx.installed_extensions, 0..) |installed, index| {
        const has_visible_mcp = try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions);
        const has_visible_skill = try installedExtensionHasVisibleSkill(alloc, installed, ctx.extension_members, ctx.permissions);
        const has_visible_agent = try installedExtensionHasVisibleAgent(alloc, installed, ctx.extension_members, ctx.permissions);
        const installed_capabilities = try capabilityNamesAlloc(alloc, installed.granted_capabilities);
        defer alloc.free(installed_capabilities);
        const can_expose_extension = try visibleInstalledCanExposeExtension(alloc, installed, ctx);
        if (can_expose_extension) {
            if (findInstalledPackage(ctx.extension_packages, installed)) |package| {
                const package_capabilities = try capabilityNamesAlloc(alloc, package.capabilities_requested);
                defer alloc.free(package_capabilities);
                if (!try visiblePackageAlreadyEmitted(alloc, ctx, package.*, index) and
                    catalogOptionsAllowMedia(options, "application/antfly-extension-package+json", &.{ "extension", "package" }) and
                    extensionPackageEntryMatches(package.*, package_capabilities, text, filter, options.publisher_domain))
                {
                    try writeExtensionPackageEntry(writer, options, first, package.*);
                }
            }
        }
        if (has_visible_skill) {
            try writeExtensionSkillEntries(alloc, writer, options, first, ctx, installed, text, filter);
        }
        if (has_visible_agent) {
            try writeExtensionAgentEntries(alloc, writer, options, first, ctx, installed, text, filter);
        }
        if ((installedExtensionVisible(installed, ctx.permissions) or has_visible_mcp or has_visible_skill or has_visible_agent) and
            catalogOptionsAllowMedia(options, "application/antfly-installed-extension+json", &.{ "extension", "installed" }) and
            installedExtensionEntryMatches(installed, installed_capabilities, text, filter, options.publisher_domain))
        {
            try writeInstalledExtensionEntry(writer, options, first, installed);
        }
        if (has_visible_mcp) {
            if (catalogOptionsAllowMedia(options, "application/mcp-server+json", &.{ "mcp", "extension" }) and
                extensionMcpEntryMatches(installed, installed_capabilities, text, filter, options.publisher_domain))
            {
                try writeExtensionMcpEntry(writer, options, first, installed);
            }
        }
    }
}

fn writeMatchedExtensionEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    ctx: ExtensionCatalogContext,
    text: ?[]const u8,
    filter: ?std.json.Value,
    page_start: usize,
    page_size: usize,
    matched: *usize,
) !void {
    for (ctx.installed_extensions, 0..) |installed, index| {
        const has_visible_mcp = try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions);
        const has_visible_skill = try installedExtensionHasVisibleSkill(alloc, installed, ctx.extension_members, ctx.permissions);
        const has_visible_agent = try installedExtensionHasVisibleAgent(alloc, installed, ctx.extension_members, ctx.permissions);
        const installed_capabilities = try capabilityNamesAlloc(alloc, installed.granted_capabilities);
        defer alloc.free(installed_capabilities);
        const can_expose_extension = try visibleInstalledCanExposeExtension(alloc, installed, ctx);
        if (can_expose_extension) {
            if (findInstalledPackage(ctx.extension_packages, installed)) |package| {
                const package_capabilities = try capabilityNamesAlloc(alloc, package.capabilities_requested);
                defer alloc.free(package_capabilities);
                if (!try visiblePackageAlreadyEmitted(alloc, ctx, package.*, index) and
                    catalogOptionsAllowMedia(options, "application/antfly-extension-package+json", &.{ "extension", "package" }) and
                    extensionPackageEntryMatches(package.*, package_capabilities, text, filter, options.publisher_domain))
                {
                    try writeSearchExtensionPackageEntry(writer, options, first, package.*, page_start, page_size, matched, text);
                }
            }
        }
        if (has_visible_skill) {
            try writeSearchExtensionSkillEntries(alloc, writer, options, first, ctx, installed, page_start, page_size, matched, text, filter);
        }
        if (has_visible_agent) {
            try writeSearchExtensionAgentEntries(alloc, writer, options, first, ctx, installed, page_start, page_size, matched, text, filter);
        }
        if ((installedExtensionVisible(installed, ctx.permissions) or has_visible_mcp or has_visible_skill or has_visible_agent) and
            catalogOptionsAllowMedia(options, "application/antfly-installed-extension+json", &.{ "extension", "installed" }) and
            installedExtensionEntryMatches(installed, installed_capabilities, text, filter, options.publisher_domain))
        {
            try writeSearchInstalledExtensionEntry(writer, options, first, installed, page_start, page_size, matched, text);
        }
        if (has_visible_mcp) {
            if (catalogOptionsAllowMedia(options, "application/mcp-server+json", &.{ "mcp", "extension" }) and
                extensionMcpEntryMatches(installed, installed_capabilities, text, filter, options.publisher_domain))
            {
                try writeSearchExtensionMcpEntry(writer, options, first, installed, page_start, page_size, matched, text);
            }
        }
    }
}

fn writeAgentExtensionEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    ctx: ExtensionCatalogContext,
    count: *usize,
) !void {
    for (ctx.installed_extensions) |installed| {
        if (!(try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions))) continue;
        if (!catalogOptionsAllowMedia(options, "application/mcp-server+json", &.{ "mcp", "extension" })) continue;
        try writeExtensionMcpEntry(writer, options, first, installed);
        count.* += 1;
    }
}

fn collectAggregateMcpFacets(
    alloc: std.mem.Allocator,
    facets: []FacetAccumulator,
    options: CatalogOptions,
    extension_context: ?ExtensionCatalogContext,
    text: ?[]const u8,
    filter: ?std.json.Value,
    matched: *usize,
) !void {
    if (!(try aggregateMcpVisible(alloc, extension_context))) return;
    if (!catalogOptionsAllowStaticEntry(options, aggregate_mcp_entry) or !entryMatches(aggregate_mcp_entry, options.publisher_domain, text, filter)) return;
    matched.* += 1;
    try addEntryFacets(alloc, facets, options.publisher_domain, aggregate_mcp_entry);
}

fn writeAggregateMcpEntry(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    extension_context: ?ExtensionCatalogContext,
    text: ?[]const u8,
    filter: ?std.json.Value,
) !void {
    if (!(try aggregateMcpVisible(alloc, extension_context))) return;
    if (!catalogOptionsAllowStaticEntry(options, aggregate_mcp_entry) or !entryMatches(aggregate_mcp_entry, options.publisher_domain, text, filter)) return;
    try writeEntry(writer, options, first, aggregate_mcp_entry);
}

fn writeSearchAggregateMcpEntry(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    extension_context: ?ExtensionCatalogContext,
    text: ?[]const u8,
    filter: ?std.json.Value,
    page_start: usize,
    page_size: usize,
    matched: *usize,
) !void {
    if (!(try aggregateMcpVisible(alloc, extension_context))) return;
    if (!catalogOptionsAllowStaticEntry(options, aggregate_mcp_entry) or !entryMatches(aggregate_mcp_entry, options.publisher_domain, text, filter)) return;
    try writeSearchEntry(writer, options, first, aggregate_mcp_entry, page_start, page_size, matched, text);
}

fn writeAgentAggregateMcpEntry(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    extension_context: ?ExtensionCatalogContext,
    count: *usize,
) !void {
    if (!(try aggregateMcpVisible(alloc, extension_context))) return;
    if (!isAgentLike(aggregate_mcp_entry) or !catalogOptionsAllowStaticEntry(options, aggregate_mcp_entry)) return;
    try writeEntry(writer, options, first, aggregate_mcp_entry);
    count.* += 1;
}

fn collectCopilotMcpProfileFacets(
    alloc: std.mem.Allocator,
    facets: []FacetAccumulator,
    options: CatalogOptions,
    extension_context: ?ExtensionCatalogContext,
    text: ?[]const u8,
    filter: ?std.json.Value,
    matched: *usize,
) !void {
    if (!(try copilotMcpProfileVisible(alloc, extension_context))) return;
    if (!catalogOptionsAllowStaticEntry(options, copilot_mcp_profile_entry) or !entryMatches(copilot_mcp_profile_entry, options.publisher_domain, text, filter)) return;
    matched.* += 1;
    try addEntryFacets(alloc, facets, options.publisher_domain, copilot_mcp_profile_entry);
}

fn writeCopilotMcpProfileEntry(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    extension_context: ?ExtensionCatalogContext,
    text: ?[]const u8,
    filter: ?std.json.Value,
) !void {
    if (!(try copilotMcpProfileVisible(alloc, extension_context))) return;
    if (!catalogOptionsAllowStaticEntry(options, copilot_mcp_profile_entry) or !entryMatches(copilot_mcp_profile_entry, options.publisher_domain, text, filter)) return;
    try writeEntry(writer, options, first, copilot_mcp_profile_entry);
}

fn writeSearchCopilotMcpProfileEntry(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    extension_context: ?ExtensionCatalogContext,
    text: ?[]const u8,
    filter: ?std.json.Value,
    page_start: usize,
    page_size: usize,
    matched: *usize,
) !void {
    if (!(try copilotMcpProfileVisible(alloc, extension_context))) return;
    if (!catalogOptionsAllowStaticEntry(options, copilot_mcp_profile_entry) or !entryMatches(copilot_mcp_profile_entry, options.publisher_domain, text, filter)) return;
    try writeSearchEntry(writer, options, first, copilot_mcp_profile_entry, page_start, page_size, matched, text);
}

fn writeAgentCopilotMcpProfileEntry(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    extension_context: ?ExtensionCatalogContext,
    count: *usize,
) !void {
    if (!(try copilotMcpProfileVisible(alloc, extension_context))) return;
    if (!isAgentLike(copilot_mcp_profile_entry) or !catalogOptionsAllowStaticEntry(options, copilot_mcp_profile_entry)) return;
    try writeEntry(writer, options, first, copilot_mcp_profile_entry);
    count.* += 1;
}

fn writeExtensionPackageEntry(writer: *std.Io.Writer, options: CatalogOptions, first: *bool, package: extension_domain.PackageManifest) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeExtensionPackageFields(writer, options, package);
    try writer.writeByte('}');
}

fn writeInstalledExtensionEntry(writer: *std.Io.Writer, options: CatalogOptions, first: *bool, installed: extension_domain.InstalledExtension) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeInstalledExtensionFields(writer, options, installed);
    try writer.writeByte('}');
}

fn writeExtensionMcpEntry(writer: *std.Io.Writer, options: CatalogOptions, first: *bool, installed: extension_domain.InstalledExtension) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeExtensionMcpFields(writer, options, installed);
    try writer.writeByte('}');
}

fn writeExtensionSkillEntry(
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    skill: ParsedExtensionSkill,
) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeExtensionSkillFields(writer, options, installed, member, skill);
    try writer.writeByte('}');
}

fn writeExtensionAgentEntry(
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    agent: ParsedExtensionAgent,
) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeExtensionAgentFields(writer, options, installed, member, agent);
    try writer.writeByte('}');
}

fn writeSearchInstalledExtensionEntry(writer: *std.Io.Writer, options: CatalogOptions, first: *bool, installed: extension_domain.InstalledExtension, page_start: usize, page_size: usize, matched: *usize, text: ?[]const u8) !void {
    if (!recordSearchMatch(page_start, page_size, matched)) return;
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeInstalledExtensionFields(writer, options, installed);
    try writer.writeAll(",\"score\":");
    try writer.print("{d}", .{if (text == null or text.?.len == 0) @as(u16, 100) else 90});
    try writer.writeAll(",\"source\":\"/ard/v1/catalog\"}");
}

fn writeSearchExtensionPackageEntry(writer: *std.Io.Writer, options: CatalogOptions, first: *bool, package: extension_domain.PackageManifest, page_start: usize, page_size: usize, matched: *usize, text: ?[]const u8) !void {
    if (!recordSearchMatch(page_start, page_size, matched)) return;
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeExtensionPackageFields(writer, options, package);
    try writer.writeAll(",\"score\":");
    try writer.print("{d}", .{if (text == null or text.?.len == 0) @as(u16, 100) else 90});
    try writer.writeAll(",\"source\":\"/ard/v1/catalog\"}");
}

fn writeSearchExtensionSkillEntry(
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    skill: ParsedExtensionSkill,
    page_start: usize,
    page_size: usize,
    matched: *usize,
    text: ?[]const u8,
) !void {
    if (!recordSearchMatch(page_start, page_size, matched)) return;
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeExtensionSkillFields(writer, options, installed, member, skill);
    try writer.writeAll(",\"score\":");
    try writer.print("{d}", .{if (text == null or text.?.len == 0) @as(u16, 100) else 90});
    try writer.writeAll(",\"source\":\"/ard/v1/catalog\"}");
}

fn writeSearchExtensionAgentEntry(
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    agent: ParsedExtensionAgent,
    page_start: usize,
    page_size: usize,
    matched: *usize,
    text: ?[]const u8,
) !void {
    if (!recordSearchMatch(page_start, page_size, matched)) return;
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeExtensionAgentFields(writer, options, installed, member, agent);
    try writer.writeAll(",\"score\":");
    try writer.print("{d}", .{if (text == null or text.?.len == 0) @as(u16, 100) else 90});
    try writer.writeAll(",\"source\":\"/ard/v1/catalog\"}");
}

fn writeSearchExtensionMcpEntry(writer: *std.Io.Writer, options: CatalogOptions, first: *bool, installed: extension_domain.InstalledExtension, page_start: usize, page_size: usize, matched: *usize, text: ?[]const u8) !void {
    if (!recordSearchMatch(page_start, page_size, matched)) return;
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try writer.writeByte('{');
    try writeExtensionMcpFields(writer, options, installed);
    try writer.writeAll(",\"score\":");
    try writer.print("{d}", .{if (text == null or text.?.len == 0) @as(u16, 100) else 90});
    try writer.writeAll(",\"source\":\"/ard/v1/catalog\"}");
}

fn collectExtensionSkillFacets(
    alloc: std.mem.Allocator,
    facets: []FacetAccumulator,
    options: CatalogOptions,
    ctx: ExtensionCatalogContext,
    installed: extension_domain.InstalledExtension,
    text: ?[]const u8,
    filter: ?std.json.Value,
    matched: *usize,
) !void {
    for (ctx.extension_members) |member| {
        if (member.object_kind != .skill) continue;
        if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
        if (!(try extensionMemberVisible(alloc, installed, member, ctx.permissions))) continue;
        var skill = try parseExtensionSkillDescriptor(alloc, installed, member);
        defer skill.deinit();
        if (!extensionSkillAllowedAndMatches(options, installed, member, skill, text, filter)) continue;
        matched.* += 1;
        try addDynamicFacets(alloc, facets, options.publisher_domain, "application/ai-skill+md", skill.tags(), skill.capabilities());
    }
}

fn collectExtensionAgentFacets(
    alloc: std.mem.Allocator,
    facets: []FacetAccumulator,
    options: CatalogOptions,
    ctx: ExtensionCatalogContext,
    installed: extension_domain.InstalledExtension,
    text: ?[]const u8,
    filter: ?std.json.Value,
    matched: *usize,
) !void {
    for (ctx.extension_members) |member| {
        if (member.object_kind != .agent) continue;
        if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
        if (!(try extensionMemberVisible(alloc, installed, member, ctx.permissions))) continue;
        var agent = try parseExtensionAgentDescriptor(alloc, installed, member);
        defer agent.deinit();
        if (!extensionAgentAllowedAndMatches(options, installed, member, agent, text, filter)) continue;
        matched.* += 1;
        try addDynamicFacets(alloc, facets, options.publisher_domain, "application/antfly-agent+json", agent.tags(), agent.capabilities());
    }
}

fn writeExtensionSkillEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    ctx: ExtensionCatalogContext,
    installed: extension_domain.InstalledExtension,
    text: ?[]const u8,
    filter: ?std.json.Value,
) !void {
    for (ctx.extension_members) |member| {
        if (member.object_kind != .skill) continue;
        if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
        if (!(try extensionMemberVisible(alloc, installed, member, ctx.permissions))) continue;
        var skill = try parseExtensionSkillDescriptor(alloc, installed, member);
        defer skill.deinit();
        if (!extensionSkillAllowedAndMatches(options, installed, member, skill, text, filter)) continue;
        try writeExtensionSkillEntry(writer, options, first, installed, member, skill);
    }
}

fn writeExtensionAgentEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    ctx: ExtensionCatalogContext,
    installed: extension_domain.InstalledExtension,
    text: ?[]const u8,
    filter: ?std.json.Value,
) !void {
    for (ctx.extension_members) |member| {
        if (member.object_kind != .agent) continue;
        if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
        if (!(try extensionMemberVisible(alloc, installed, member, ctx.permissions))) continue;
        var agent = try parseExtensionAgentDescriptor(alloc, installed, member);
        defer agent.deinit();
        if (!extensionAgentAllowedAndMatches(options, installed, member, agent, text, filter)) continue;
        try writeExtensionAgentEntry(writer, options, first, installed, member, agent);
    }
}

fn writeSearchExtensionSkillEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    ctx: ExtensionCatalogContext,
    installed: extension_domain.InstalledExtension,
    page_start: usize,
    page_size: usize,
    matched: *usize,
    text: ?[]const u8,
    filter: ?std.json.Value,
) !void {
    for (ctx.extension_members) |member| {
        if (member.object_kind != .skill) continue;
        if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
        if (!(try extensionMemberVisible(alloc, installed, member, ctx.permissions))) continue;
        var skill = try parseExtensionSkillDescriptor(alloc, installed, member);
        defer skill.deinit();
        if (!extensionSkillAllowedAndMatches(options, installed, member, skill, text, filter)) continue;
        try writeSearchExtensionSkillEntry(writer, options, first, installed, member, skill, page_start, page_size, matched, text);
    }
}

fn writeSearchExtensionAgentEntries(
    alloc: std.mem.Allocator,
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    ctx: ExtensionCatalogContext,
    installed: extension_domain.InstalledExtension,
    page_start: usize,
    page_size: usize,
    matched: *usize,
    text: ?[]const u8,
    filter: ?std.json.Value,
) !void {
    for (ctx.extension_members) |member| {
        if (member.object_kind != .agent) continue;
        if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
        if (!(try extensionMemberVisible(alloc, installed, member, ctx.permissions))) continue;
        var agent = try parseExtensionAgentDescriptor(alloc, installed, member);
        defer agent.deinit();
        if (!extensionAgentAllowedAndMatches(options, installed, member, agent, text, filter)) continue;
        try writeSearchExtensionAgentEntry(writer, options, first, installed, member, agent, page_start, page_size, matched, text);
    }
}

fn writeExtensionPackageFields(writer: *std.Io.Writer, options: CatalogOptions, package: extension_domain.PackageManifest) !void {
    try writer.writeAll("\"identifier\":");
    try writeStringFmt(writer, "urn:ai:{s}:antfly:extension:package:{s}:{s}", .{ options.publisher_domain, package.name, package.version });
    try writer.writeAll(",\"displayName\":");
    try writeStringFmt(writer, "Antfly Extension Package {s} {s}", .{ package.name, package.version });
    try writer.writeAll(",\"type\":\"application/antfly-extension-package+json\",\"description\":");
    if (package.description.len > 0) {
        try std.json.Stringify.value(package.description, .{}, writer);
    } else {
        try writeStringFmt(writer, "Antfly extension package {s} version {s}.", .{ package.name, package.version });
    }
    try writer.writeAll(",\"url\":");
    try writeUrlFmt(writer, options.base_url, "/extensions/v1/packages/{s}/versions/{s}", .{ package.name, package.version });
    try writer.writeAll(",\"tags\":[\"extension\",\"package\"],\"capabilities\":");
    try writeCapabilitiesFromGrants(writer, package.capabilities_requested);
    try writer.writeAll(",\"metadata\":{\"digest\":");
    try std.json.Stringify.value(package.digest, .{}, writer);
    try writer.writeAll(",\"kind\":");
    try std.json.Stringify.value(@tagName(package.kind), .{}, writer);
    try writer.writeAll(",\"trusted\":");
    try std.json.Stringify.value(package.trusted, .{}, writer);
    try writer.writeAll(",\"artifactCount\":");
    try writer.print("{d}", .{package.artifacts.len});
    try writer.writeAll(",\"capabilitiesRequestedCount\":");
    try writer.print("{d}", .{package.capabilities_requested.len});
    try writer.writeAll("},\"trustManifest\":");
    try writePackageTrustManifest(writer, options, package);
}

fn writePackageTrustManifest(writer: *std.Io.Writer, options: CatalogOptions, package: extension_domain.PackageManifest) !void {
    try writeTrustManifestPrefix(writer, options.publisher_domain);
    try writer.writeAll(",\"provenance\":[");
    try writer.writeAll("{\"relation\":\"publishedFrom\",\"sourceId\":");
    try writeUrlFmt(writer, options.base_url, "/extensions/v1/packages/{s}/versions/{s}", .{ package.name, package.version });
    try writer.writeAll(",\"sourceDigest\":");
    try std.json.Stringify.value(package.digest, .{}, writer);
    try writer.writeByte('}');
    for (package.artifacts) |artifact| {
        try writer.writeByte(',');
        try writer.writeAll("{\"relation\":\"derivedFrom\",\"sourceId\":");
        try std.json.Stringify.value(artifact.path, .{}, writer);
        if (artifact.digest.len > 0) {
            try writer.writeAll(",\"sourceDigest\":");
            try std.json.Stringify.value(artifact.digest, .{}, writer);
        }
        try writer.writeByte('}');
    }
    try writer.writeAll("]}");
}

fn writeInstalledExtensionFields(writer: *std.Io.Writer, options: CatalogOptions, installed: extension_domain.InstalledExtension) !void {
    try writer.writeAll("\"identifier\":");
    try writeStringFmt(writer, "urn:ai:{s}:antfly:extension:{s}:installed", .{ options.publisher_domain, installed.name });
    try writer.writeAll(",\"displayName\":");
    try writeStringFmt(writer, "Antfly Extension {s}", .{installed.name});
    try writer.writeAll(",\"type\":\"application/antfly-installed-extension+json\",\"description\":");
    try writeStringFmt(writer, "Installed Antfly extension {s}.", .{installed.name});
    try writer.writeAll(",\"url\":");
    try writeUrlFmt(writer, options.base_url, "/extensions/v1/installed/{s}", .{installed.name});
    try writer.writeAll(",\"tags\":[\"extension\",\"installed\"],\"capabilities\":");
    try writeCapabilitiesFromGrants(writer, installed.granted_capabilities);
    try writer.writeAll(",\"metadata\":{\"digest\":");
    try std.json.Stringify.value(installed.package_digest, .{}, writer);
    try writer.writeAll(",\"packageName\":");
    try std.json.Stringify.value(installed.package_name, .{}, writer);
    try writer.writeAll(",\"packageVersion\":");
    try std.json.Stringify.value(installed.package_version, .{}, writer);
    try writer.writeAll(",\"endpoint\":");
    try writeStringFmt(writer, "/extensions/v1/installed/{s}", .{installed.name});
    try writer.writeAll(",\"status\":");
    try std.json.Stringify.value(@tagName(installed.status), .{}, writer);
    try writer.writeAll(",\"scope\":");
    try writeExtensionScopeString(writer, installed.scope);
    try writer.writeAll(",\"scopeKind\":");
    try std.json.Stringify.value(@tagName(installed.scope.kind), .{}, writer);
    if (installed.scope.kind == .table) {
        try writer.writeAll(",\"scopeTableName\":");
        try std.json.Stringify.value(installed.scope.table_name, .{}, writer);
    }
    try writer.writeAll(",\"grantedCapabilitiesCount\":");
    try writer.print("{d}", .{installed.granted_capabilities.len});
    try writer.writeAll("},\"trustManifest\":");
    try writeInstalledExtensionTrustManifest(writer, options, installed);
}

fn writeInstalledExtensionTrustManifest(writer: *std.Io.Writer, options: CatalogOptions, installed: extension_domain.InstalledExtension) !void {
    try writeTrustManifestPrefix(writer, options.publisher_domain);
    try writer.writeAll(",\"provenance\":[{\"relation\":\"derivedFrom\",\"sourceId\":");
    try writeUrlFmt(writer, options.base_url, "/extensions/v1/packages/{s}/versions/{s}", .{ installed.package_name, installed.package_version });
    if (installed.package_digest.len > 0) {
        try writer.writeAll(",\"sourceDigest\":");
        try std.json.Stringify.value(installed.package_digest, .{}, writer);
    }
    try writer.writeAll("}]}");
}

fn writeExtensionMcpFields(writer: *std.Io.Writer, options: CatalogOptions, installed: extension_domain.InstalledExtension) !void {
    try writer.writeAll("\"identifier\":");
    try writeStringFmt(writer, "urn:ai:{s}:antfly:extension:{s}:mcp", .{ options.publisher_domain, installed.name });
    try writer.writeAll(",\"displayName\":");
    try writeStringFmt(writer, "Antfly Extension MCP {s}", .{installed.name});
    try writer.writeAll(",\"type\":\"application/mcp-server+json\",\"description\":");
    try writeStringFmt(writer, "MCP server for visible tools owned by Antfly extension {s}.", .{installed.name});
    try writer.writeAll(",\"url\":");
    try writeUrlFmt(writer, options.base_url, "/ard/v1/resources/mcp/extensions/{s}", .{installed.name});
    try writer.writeAll(",\"tags\":[\"mcp\",\"extension\"],\"capabilities\":");
    try writeCapabilitiesFromGrants(writer, installed.granted_capabilities);
    try writer.writeAll(",\"metadata\":{\"endpoint\":");
    try writeStringFmt(writer, "/mcp/v1/extensions/{s}", .{installed.name});
    try writer.writeAll(",\"extension\":");
    try std.json.Stringify.value(installed.name, .{}, writer);
    try writer.writeByte('}');
}

fn writeExtensionSkillFields(
    writer: *std.Io.Writer,
    options: CatalogOptions,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    skill: ParsedExtensionSkill,
) !void {
    try writer.writeAll("\"identifier\":");
    try writeStringFmt(writer, "urn:ai:{s}:antfly:extension:{s}:skill:{s}", .{ options.publisher_domain, installed.name, member.object_name });
    try writer.writeAll(",\"displayName\":");
    try std.json.Stringify.value(skill.display_name, .{}, writer);
    try writer.writeAll(",\"type\":\"application/ai-skill+md\",\"description\":");
    try std.json.Stringify.value(skill.description, .{}, writer);
    try writer.writeAll(",\"url\":");
    try writeUrlFmt(writer, options.base_url, "/ard/v1/skills/extensions/{s}/{s}", .{ installed.name, member.object_name });
    try writer.writeAll(",\"tags\":");
    try writeStringArray(writer, skill.tags());
    if (skill.capabilities().len > 0) {
        try writer.writeAll(",\"capabilities\":");
        try writeStringArray(writer, skill.capabilities());
    }
    if (skill.representativeQueries().len > 0) {
        try writer.writeAll(",\"representativeQueries\":");
        try writeStringArray(writer, skill.representativeQueries());
    }
    try writer.writeAll(",\"metadata\":{\"scope\":\"extension\",\"extension\":");
    try std.json.Stringify.value(installed.name, .{}, writer);
    try writer.writeAll(",\"skill\":");
    try std.json.Stringify.value(member.object_name, .{}, writer);
    try writer.writeAll(",\"objectKind\":\"skill\"");
    if (skill.profile) |profile| {
        try writer.writeAll(",\"profile\":");
        try std.json.Stringify.value(profile, .{}, writer);
    }
    try writer.writeByte('}');
}

fn writeExtensionAgentFields(
    writer: *std.Io.Writer,
    options: CatalogOptions,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    agent: ParsedExtensionAgent,
) !void {
    try writer.writeAll("\"identifier\":");
    try writeStringFmt(writer, "urn:ai:{s}:antfly:extension:{s}:agent:{s}", .{ options.publisher_domain, installed.name, member.object_name });
    try writer.writeAll(",\"displayName\":");
    try std.json.Stringify.value(agent.display_name, .{}, writer);
    try writer.writeAll(",\"type\":\"application/antfly-agent+json\",\"description\":");
    try std.json.Stringify.value(agent.description, .{}, writer);
    try writer.writeAll(",\"url\":");
    try writeUrlFmt(writer, options.base_url, "/ard/v1/resources/agents/extensions/{s}/{s}", .{ installed.name, member.object_name });
    try writer.writeAll(",\"tags\":");
    try writeStringArray(writer, agent.tags());
    if (agent.capabilities().len > 0) {
        try writer.writeAll(",\"capabilities\":");
        try writeStringArray(writer, agent.capabilities());
    }
    if (agent.representativeQueries().len > 0) {
        try writer.writeAll(",\"representativeQueries\":");
        try writeStringArray(writer, agent.representativeQueries());
    }
    try writer.writeAll(",\"metadata\":{\"scope\":\"extension\",\"extension\":");
    try std.json.Stringify.value(installed.name, .{}, writer);
    try writer.writeAll(",\"agent\":");
    try std.json.Stringify.value(member.object_name, .{}, writer);
    try writer.writeAll(",\"objectKind\":\"agent\",\"protocols\":");
    try writeStringArray(writer, agent.protocols());
    try writer.writeAll(",\"runEndpoint\":");
    try writeUrlFmt(writer, options.base_url, "/agents/v1/extensions/{s}/{s}/runs", .{ installed.name, member.object_name });
    if (agent.profile) |profile| {
        try writer.writeAll(",\"profile\":");
        try std.json.Stringify.value(profile, .{}, writer);
    }
    if (agent.handler) |handler| {
        try writer.writeAll(",\"handler\":");
        try std.json.Stringify.value(handler, .{}, writer);
    }
    if (agent.stream_handler) |stream_handler| {
        try writer.writeAll(",\"streamHandler\":");
        try std.json.Stringify.value(stream_handler, .{}, writer);
    }
    try writer.writeByte('}');
}

fn extensionMcpDescriptorJsonAlloc(alloc: std.mem.Allocator, installed: extension_domain.InstalledExtension, base_url: ?[]const u8, ctx: ExtensionCatalogContext) ![]u8 {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();

    try writer.writer.writeAll("{\"name\":");
    try std.json.Stringify.value(installed.name, .{}, &writer.writer);
    try writer.writer.writeAll(",\"endpoint\":");
    try writeUrlFmt(&writer.writer, base_url, "/mcp/v1/extensions/{s}", .{installed.name});
    try writer.writer.writeAll(",\"extension\":");
    try std.json.Stringify.value(installed.name, .{}, &writer.writer);
    try writer.writer.writeAll(",\"tools\":[");
    var first = true;
    for (ctx.extension_members) |member| {
        if (member.object_kind != .mcp_tool) continue;
        if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
        if (!(try extensionMemberVisible(alloc, installed, member, ctx.permissions))) continue;
        if (first) {
            first = false;
        } else {
            try writer.writer.writeByte(',');
        }
        try writer.writer.writeAll("{\"name\":");
        try std.json.Stringify.value(member.object_name, .{}, &writer.writer);
        if (member.table_name.len > 0) {
            try writer.writer.writeAll(",\"table\":");
            try std.json.Stringify.value(member.table_name, .{}, &writer.writer);
        }
        try writer.writer.writeByte('}');
    }
    try writer.writer.writeAll("]}");
    return try writer.toOwnedSlice();
}

fn extensionAgentDescriptorJsonAlloc(
    alloc: std.mem.Allocator,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    agent: ParsedExtensionAgent,
    base_url: ?[]const u8,
) ![]u8 {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();
    try writer.writer.writeAll("{\"name\":");
    try std.json.Stringify.value(member.object_name, .{}, &writer.writer);
    try writer.writer.writeAll(",\"displayName\":");
    try std.json.Stringify.value(agent.display_name, .{}, &writer.writer);
    try writer.writer.writeAll(",\"description\":");
    try std.json.Stringify.value(agent.description, .{}, &writer.writer);
    try writer.writer.writeAll(",\"extension\":");
    try std.json.Stringify.value(installed.name, .{}, &writer.writer);
    try writer.writer.writeAll(",\"protocols\":");
    try writeStringArray(&writer.writer, agent.protocols());
    try writer.writer.writeAll(",\"capabilities\":");
    try writeStringArray(&writer.writer, agent.capabilities());
    try writer.writer.writeAll(",\"runEndpoint\":");
    try writeUrlFmt(&writer.writer, base_url, "/agents/v1/extensions/{s}/{s}/runs", .{ installed.name, member.object_name });
    try writer.writer.writeAll(",\"statusEndpointTemplate\":");
    try writeUrlFmt(&writer.writer, base_url, "/agents/v1/extensions/{s}/{s}/runs/{{run_id}}", .{ installed.name, member.object_name });
    try writer.writer.writeAll(",\"eventsEndpointTemplate\":");
    try writeUrlFmt(&writer.writer, base_url, "/agents/v1/extensions/{s}/{s}/runs/{{run_id}}/events", .{ installed.name, member.object_name });
    try writer.writer.writeAll(",\"cancelEndpointTemplate\":");
    try writeUrlFmt(&writer.writer, base_url, "/agents/v1/extensions/{s}/{s}/runs/{{run_id}}/cancel", .{ installed.name, member.object_name });
    if (agent.profile) |profile| {
        try writer.writer.writeAll(",\"profile\":");
        try std.json.Stringify.value(profile, .{}, &writer.writer);
    }
    if (agent.handler) |handler| {
        try writer.writer.writeAll(",\"handler\":");
        try std.json.Stringify.value(handler, .{}, &writer.writer);
    }
    if (agent.stream_handler) |stream_handler| {
        try writer.writer.writeAll(",\"streamHandler\":");
        try std.json.Stringify.value(stream_handler, .{}, &writer.writer);
    }
    try writer.writer.writeByte('}');
    return try writer.toOwnedSlice();
}

const BuiltinMcpToolKind = enum {
    create_table,
    drop_table,
    list_tables,
    create_index,
    drop_index,
    list_indexes,
    get_document,
    query,
    backup,
    restore,
    batch,
};

const BuiltinMcpTool = struct {
    kind: BuiltinMcpToolKind,
    name: []const u8,
};

const builtin_mcp_tools = [_]BuiltinMcpTool{
    .{ .kind = .create_table, .name = "create_table" },
    .{ .kind = .drop_table, .name = "drop_table" },
    .{ .kind = .list_tables, .name = "list_tables" },
    .{ .kind = .create_index, .name = "create_index" },
    .{ .kind = .drop_index, .name = "drop_index" },
    .{ .kind = .list_indexes, .name = "list_indexes" },
    .{ .kind = .get_document, .name = "get_document" },
    .{ .kind = .query, .name = "query" },
    .{ .kind = .backup, .name = "backup" },
    .{ .kind = .restore, .name = "restore" },
    .{ .kind = .batch, .name = "batch" },
};

fn aggregateMcpDescriptorJsonAlloc(alloc: std.mem.Allocator, base_url: ?[]const u8, extension_context: ?ExtensionCatalogContext) ![]u8 {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();

    try writer.writer.writeAll("{\"name\":\"antfly\",\"endpoint\":");
    try writeUrlValue(&writer.writer, base_url, "/mcp/v1");
    try writer.writer.writeAll(",\"description\":\"Aggregate Antfly MCP server for built-in and visible extension tools.\",\"capabilities\":[\"table-search\",\"retrieval\",\"query-builder\",\"extension-tools\"],\"tools\":[");
    try writeVisibleMcpTools(alloc, &writer.writer, extension_context);
    try writer.writer.writeAll("]}");
    return try writer.toOwnedSlice();
}

fn copilotMcpProfileDescriptorJsonAlloc(alloc: std.mem.Allocator, base_url: ?[]const u8, extension_context: ?ExtensionCatalogContext) ![]u8 {
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();

    try writer.writer.writeAll("{\"name\":\"antfly-copilot\",\"endpoint\":");
    try writeUrlValue(&writer.writer, base_url, "/mcp/v1/extensions/profiles/copilot");
    try writer.writer.writeAll(",\"profile\":\"copilot\",\"description\":\"Profile-scoped Antfly MCP server for Copilot-style clients.\",\"capabilities\":[\"table-search\",\"retrieval\",\"query-builder\",\"extension-tools\"],\"tools\":[");
    try writeVisibleMcpTools(alloc, &writer.writer, extension_context);
    try writer.writer.writeAll("]}");
    return try writer.toOwnedSlice();
}

fn writeVisibleMcpTools(alloc: std.mem.Allocator, writer: *std.Io.Writer, extension_context: ?ExtensionCatalogContext) !void {
    var first = true;
    const permissions = if (extension_context) |ctx| ctx.permissions else null;
    for (builtin_mcp_tools) |tool| {
        if (!builtinMcpToolVisible(tool.kind, permissions)) continue;
        if (first) {
            first = false;
        } else {
            try writer.writeByte(',');
        }
        try writer.writeAll("{\"name\":");
        try std.json.Stringify.value(tool.name, .{}, writer);
        try writer.writeAll(",\"source\":\"builtin\"}");
    }
    if (extension_context) |ctx| {
        for (ctx.installed_extensions) |installed| {
            if (!(try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions))) continue;
            for (ctx.extension_members) |member| {
                if (member.object_kind != .mcp_tool) continue;
                if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
                if (!(try extensionMemberVisible(alloc, installed, member, ctx.permissions))) continue;
                if (first) {
                    first = false;
                } else {
                    try writer.writeByte(',');
                }
                try writer.writeAll("{\"name\":");
                try std.json.Stringify.value(member.object_name, .{}, writer);
                try writer.writeAll(",\"source\":\"extension\",\"extension\":");
                try std.json.Stringify.value(installed.name, .{}, writer);
                if (member.table_name.len > 0) {
                    try writer.writeAll(",\"table\":");
                    try std.json.Stringify.value(member.table_name, .{}, writer);
                }
                try writer.writeByte('}');
            }
        }
    }
}

const ExtensionSkillRoute = struct {
    extension_name: []const u8,
    skill_name: []const u8,
};

const ExtensionAgentRoute = struct {
    extension_name: []const u8,
    agent_name: []const u8,
};

const ParsedExtensionSkill = struct {
    const max_tags = 16;
    const max_capabilities = 16;
    const max_representative_queries = 8;

    parsed: std.json.Parsed(std.json.Value),
    display_name: []const u8,
    description: []const u8,
    body: []const u8 = "",
    profile: ?[]const u8 = null,
    tag_values: [max_tags][]const u8 = undefined,
    tag_count: usize = 0,
    capability_values: [max_capabilities][]const u8 = undefined,
    capability_count: usize = 0,
    representative_query_values: [max_representative_queries][]const u8 = undefined,
    representative_query_count: usize = 0,

    fn deinit(self: *ParsedExtensionSkill) void {
        self.parsed.deinit();
    }

    fn tags(self: *const ParsedExtensionSkill) []const []const u8 {
        return self.tag_values[0..self.tag_count];
    }

    fn capabilities(self: *const ParsedExtensionSkill) []const []const u8 {
        return self.capability_values[0..self.capability_count];
    }

    fn representativeQueries(self: *const ParsedExtensionSkill) []const []const u8 {
        return self.representative_query_values[0..self.representative_query_count];
    }

    fn addTag(self: *ParsedExtensionSkill, value: []const u8) void {
        if (value.len == 0 or self.tag_count >= max_tags or jsonStringSliceContains(self.tags(), value)) return;
        self.tag_values[self.tag_count] = value;
        self.tag_count += 1;
    }

    fn addCapability(self: *ParsedExtensionSkill, value: []const u8) void {
        if (value.len == 0 or self.capability_count >= max_capabilities or jsonStringSliceContains(self.capabilities(), value)) return;
        self.capability_values[self.capability_count] = value;
        self.capability_count += 1;
    }

    fn addRepresentativeQuery(self: *ParsedExtensionSkill, value: []const u8) void {
        if (value.len == 0 or self.representative_query_count >= max_representative_queries or jsonStringSliceContains(self.representativeQueries(), value)) return;
        self.representative_query_values[self.representative_query_count] = value;
        self.representative_query_count += 1;
    }
};

const ParsedExtensionAgent = struct {
    const max_tags = 16;
    const max_capabilities = 16;
    const max_protocols = 8;
    const max_representative_queries = 8;

    parsed: std.json.Parsed(std.json.Value),
    display_name: []const u8,
    description: []const u8,
    profile: ?[]const u8 = null,
    handler: ?[]const u8 = null,
    stream_handler: ?[]const u8 = null,
    tag_values: [max_tags][]const u8 = undefined,
    tag_count: usize = 0,
    capability_values: [max_capabilities][]const u8 = undefined,
    capability_count: usize = 0,
    protocol_values: [max_protocols][]const u8 = undefined,
    protocol_count: usize = 0,
    representative_query_values: [max_representative_queries][]const u8 = undefined,
    representative_query_count: usize = 0,

    fn deinit(self: *ParsedExtensionAgent) void {
        self.parsed.deinit();
    }

    fn tags(self: *const ParsedExtensionAgent) []const []const u8 {
        return self.tag_values[0..self.tag_count];
    }

    fn capabilities(self: *const ParsedExtensionAgent) []const []const u8 {
        return self.capability_values[0..self.capability_count];
    }

    fn protocols(self: *const ParsedExtensionAgent) []const []const u8 {
        return self.protocol_values[0..self.protocol_count];
    }

    fn representativeQueries(self: *const ParsedExtensionAgent) []const []const u8 {
        return self.representative_query_values[0..self.representative_query_count];
    }

    fn addTag(self: *ParsedExtensionAgent, value: []const u8) void {
        if (value.len == 0 or self.tag_count >= max_tags or jsonStringSliceContains(self.tags(), value)) return;
        self.tag_values[self.tag_count] = value;
        self.tag_count += 1;
    }

    fn addCapability(self: *ParsedExtensionAgent, value: []const u8) void {
        if (value.len == 0 or self.capability_count >= max_capabilities or jsonStringSliceContains(self.capabilities(), value)) return;
        self.capability_values[self.capability_count] = value;
        self.capability_count += 1;
    }

    fn addProtocol(self: *ParsedExtensionAgent, value: []const u8) void {
        if (value.len == 0 or self.protocol_count >= max_protocols or jsonStringSliceContains(self.protocols(), value)) return;
        self.protocol_values[self.protocol_count] = value;
        self.protocol_count += 1;
    }

    fn addRepresentativeQuery(self: *ParsedExtensionAgent, value: []const u8) void {
        if (value.len == 0 or self.representative_query_count >= max_representative_queries or jsonStringSliceContains(self.representativeQueries(), value)) return;
        self.representative_query_values[self.representative_query_count] = value;
        self.representative_query_count += 1;
    }
};

fn parseExtensionSkillRoute(route: []const u8) ?ExtensionSkillRoute {
    const prefix = "extensions/";
    if (!std.mem.startsWith(u8, route, prefix)) return null;
    const rest = route[prefix.len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return null;
    const extension_name = rest[0..slash];
    const skill_name = rest[slash + 1 ..];
    if (extension_name.len == 0 or skill_name.len == 0 or std.mem.indexOfScalar(u8, skill_name, '/') != null) return null;
    return .{ .extension_name = extension_name, .skill_name = skill_name };
}

fn parseExtensionAgentRoute(route: []const u8) ?ExtensionAgentRoute {
    const prefix = "extensions/";
    if (!std.mem.startsWith(u8, route, prefix)) return null;
    const rest = route[prefix.len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return null;
    const extension_name = rest[0..slash];
    const agent_name = rest[slash + 1 ..];
    if (extension_name.len == 0 or agent_name.len == 0 or std.mem.indexOfScalar(u8, agent_name, '/') != null) return null;
    return .{ .extension_name = extension_name, .agent_name = agent_name };
}

fn parseExtensionSkillDescriptor(
    alloc: std.mem.Allocator,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
) !ParsedExtensionSkill {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, member.owner_metadata_json, .{});
    errdefer parsed.deinit();
    const object = parsed.value.object;
    var out = ParsedExtensionSkill{
        .parsed = parsed,
        .display_name = jsonObjectStringField(object, "displayName") orelse
            jsonObjectStringField(object, "display_name") orelse
            jsonObjectStringField(object, "name") orelse
            member.object_name,
        .description = jsonObjectStringField(object, "description") orelse "Antfly extension skill.",
        .body = jsonObjectStringField(object, "body") orelse
            jsonObjectStringField(object, "markdown") orelse "",
        .profile = jsonObjectStringField(object, "profile"),
    };

    out.addTag("skill");
    out.addTag("workflow");
    out.addTag("extension");
    out.addTag(installed.name);
    out.addTag(member.object_name);
    if (out.profile) |profile| out.addTag(profile);
    addJsonStringArrayFieldToTags(&out, object, "tags");

    addJsonStringArrayFieldToCapabilities(&out, object, "capabilities");
    addJsonCapabilityArrayFieldToCapabilities(&out, object, "required_capabilities");
    if (out.capabilities().len == 0) {
        for (installed.granted_capabilities) |capability| out.addCapability(capability.name);
    }

    addJsonStringArrayFieldToRepresentativeQueries(&out, object, "representativeQueries");
    addJsonStringArrayFieldToRepresentativeQueries(&out, object, "representative_queries");
    return out;
}

fn parseExtensionAgentDescriptor(
    alloc: std.mem.Allocator,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
) !ParsedExtensionAgent {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, member.owner_metadata_json, .{});
    errdefer parsed.deinit();
    const object = parsed.value.object;
    var out = ParsedExtensionAgent{
        .parsed = parsed,
        .display_name = jsonObjectStringField(object, "displayName") orelse
            jsonObjectStringField(object, "display_name") orelse
            jsonObjectStringField(object, "name") orelse
            member.object_name,
        .description = jsonObjectStringField(object, "description") orelse "Antfly extension agent.",
        .profile = jsonObjectStringField(object, "profile"),
        .handler = jsonObjectStringField(object, "handler"),
        .stream_handler = jsonObjectStringField(object, "stream_handler") orelse jsonObjectStringField(object, "streamHandler"),
    };

    out.addTag("agent");
    out.addTag("extension");
    out.addTag(installed.name);
    out.addTag(member.object_name);
    if (out.profile) |profile| out.addTag(profile);
    addJsonStringArrayFieldToAgentTags(&out, object, "tags");

    out.addProtocol("agents-api");
    addJsonStringArrayFieldToAgentProtocols(&out, object, "protocols");
    if (out.stream_handler != null) out.addProtocol("stream");

    addJsonStringArrayFieldToAgentCapabilities(&out, object, "capabilities");
    addJsonCapabilityArrayFieldToAgentCapabilities(&out, object, "required_capabilities");
    if (out.capabilities().len == 0) {
        for (installed.granted_capabilities) |capability| out.addCapability(capability.name);
    }

    addJsonStringArrayFieldToAgentRepresentativeQueries(&out, object, "representativeQueries");
    addJsonStringArrayFieldToAgentRepresentativeQueries(&out, object, "representative_queries");
    return out;
}

fn extensionSkillAllowedAndMatches(
    options: CatalogOptions,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    skill: ParsedExtensionSkill,
    text: ?[]const u8,
    filter: ?std.json.Value,
) bool {
    if (!catalogOptionsAllowMedia(options, "application/ai-skill+md", skill.tags())) return false;
    if (text) |query| {
        if (std.mem.trim(u8, query, " \t\r\n").len > 0 and
            !containsIgnoreCase(skill.display_name, query) and
            !containsIgnoreCase(skill.description, query) and
            !containsIgnoreCase("application/ai-skill+md", query) and
            !containsIgnoreCase(installed.name, query) and
            !containsIgnoreCase(member.object_name, query) and
            !anyContainsIgnoreCase(skill.tags(), query) and
            !anyContainsIgnoreCase(skill.capabilities(), query) and
            !anyContainsIgnoreCase(skill.representativeQueries(), query)) return false;
    }
    if (filter) |filter_value| {
        var iterator = filter_value.object.iterator();
        while (iterator.next()) |kv| {
            const key = kv.key_ptr.*;
            const value = kv.value_ptr.*;
            if (std.mem.eql(u8, key, "type")) {
                if (!jsonValueMatchesString(value, "application/ai-skill+md")) return false;
            } else if (std.mem.eql(u8, key, "tags")) {
                if (!jsonValueMatchesAnyString(value, skill.tags())) return false;
            } else if (std.mem.eql(u8, key, "capabilities")) {
                if (!jsonValueMatchesAnyString(value, skill.capabilities())) return false;
            } else if (std.mem.eql(u8, key, "publisher") or std.mem.eql(u8, key, "publisherId")) {
                if (!jsonValueMatchesString(value, options.publisher_domain)) return false;
            } else if (std.mem.eql(u8, key, "metadata.scope")) {
                if (!jsonValueMatchesString(value, "extension")) return false;
            } else if (std.mem.eql(u8, key, "metadata.extension")) {
                if (!jsonValueMatchesString(value, installed.name)) return false;
            } else if (std.mem.eql(u8, key, "metadata.skill")) {
                if (!jsonValueMatchesString(value, member.object_name)) return false;
            } else if (std.mem.eql(u8, key, "metadata.objectKind")) {
                if (!jsonValueMatchesString(value, "skill")) return false;
            } else if (std.mem.eql(u8, key, "metadata.profile")) {
                if (skill.profile == null or !jsonValueMatchesString(value, skill.profile.?)) return false;
            } else {
                return false;
            }
        }
    }
    return true;
}

fn extensionAgentAllowedAndMatches(
    options: CatalogOptions,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    agent: ParsedExtensionAgent,
    text: ?[]const u8,
    filter: ?std.json.Value,
) bool {
    if (!catalogOptionsAllowMedia(options, "application/antfly-agent+json", agent.tags())) return false;
    if (text) |query| {
        if (std.mem.trim(u8, query, " \t\r\n").len > 0 and
            !containsIgnoreCase(agent.display_name, query) and
            !containsIgnoreCase(agent.description, query) and
            !containsIgnoreCase("application/antfly-agent+json", query) and
            !containsIgnoreCase(installed.name, query) and
            !containsIgnoreCase(member.object_name, query) and
            !anyContainsIgnoreCase(agent.tags(), query) and
            !anyContainsIgnoreCase(agent.capabilities(), query) and
            !anyContainsIgnoreCase(agent.protocols(), query) and
            !anyContainsIgnoreCase(agent.representativeQueries(), query)) return false;
    }
    if (filter) |filter_value| {
        var iterator = filter_value.object.iterator();
        while (iterator.next()) |kv| {
            const key = kv.key_ptr.*;
            const value = kv.value_ptr.*;
            if (std.mem.eql(u8, key, "type")) {
                if (!jsonValueMatchesString(value, "application/antfly-agent+json")) return false;
            } else if (std.mem.eql(u8, key, "tags")) {
                if (!jsonValueMatchesAnyString(value, agent.tags())) return false;
            } else if (std.mem.eql(u8, key, "capabilities")) {
                if (!jsonValueMatchesAnyString(value, agent.capabilities())) return false;
            } else if (std.mem.eql(u8, key, "publisher") or std.mem.eql(u8, key, "publisherId")) {
                if (!jsonValueMatchesString(value, options.publisher_domain)) return false;
            } else if (std.mem.eql(u8, key, "metadata.scope")) {
                if (!jsonValueMatchesString(value, "extension")) return false;
            } else if (std.mem.eql(u8, key, "metadata.extension")) {
                if (!jsonValueMatchesString(value, installed.name)) return false;
            } else if (std.mem.eql(u8, key, "metadata.agent")) {
                if (!jsonValueMatchesString(value, member.object_name)) return false;
            } else if (std.mem.eql(u8, key, "metadata.objectKind")) {
                if (!jsonValueMatchesString(value, "agent")) return false;
            } else if (std.mem.eql(u8, key, "metadata.profile")) {
                if (agent.profile == null or !jsonValueMatchesString(value, agent.profile.?)) return false;
            } else if (std.mem.eql(u8, key, "metadata.protocols")) {
                if (!jsonValueMatchesAnyString(value, agent.protocols())) return false;
            } else {
                return false;
            }
        }
    }
    return true;
}

fn extensionSkillMarkdownFromDescriptorAlloc(
    alloc: std.mem.Allocator,
    installed: extension_domain.InstalledExtension,
    member: extension_domain.ExtensionMember,
    skill: ParsedExtensionSkill,
) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        \\# {s}
        \\
        \\{s}
        \\
        \\Extension: `{s}`
        \\
        \\Use `/mcp/v1/extensions/{s}` only when the same Antfly identity can discover the extension's MCP tools.
        \\
        \\Skill object: `{s}`
        \\
    ,
        .{ skill.display_name, skill.description, installed.name, installed.name, member.object_name },
    );
}

fn jsonObjectStringField(object: anytype, name: []const u8) ?[]const u8 {
    const value = object.get(name) orelse return null;
    if (value != .string) return null;
    return value.string;
}

fn addJsonStringArrayFieldToTags(skill: *ParsedExtensionSkill, object: anytype, name: []const u8) void {
    const value = object.get(name) orelse return;
    if (value != .array) return;
    for (value.array.items) |item| {
        if (item == .string) skill.addTag(item.string);
    }
}

fn addJsonStringArrayFieldToCapabilities(skill: *ParsedExtensionSkill, object: anytype, name: []const u8) void {
    const value = object.get(name) orelse return;
    if (value != .array) return;
    for (value.array.items) |item| {
        if (item == .string) skill.addCapability(item.string);
    }
}

fn addJsonCapabilityArrayFieldToCapabilities(skill: *ParsedExtensionSkill, object: anytype, name: []const u8) void {
    const value = object.get(name) orelse return;
    if (value != .array) return;
    for (value.array.items) |item| {
        switch (item) {
            .string => |capability| skill.addCapability(capability),
            .object => |capability_object| {
                const capability_name = jsonObjectStringField(capability_object, "name") orelse continue;
                skill.addCapability(capability_name);
            },
            else => {},
        }
    }
}

fn addJsonStringArrayFieldToRepresentativeQueries(skill: *ParsedExtensionSkill, object: anytype, name: []const u8) void {
    const value = object.get(name) orelse return;
    if (value != .array) return;
    for (value.array.items) |item| {
        if (item == .string) skill.addRepresentativeQuery(item.string);
    }
}

fn addJsonStringArrayFieldToAgentTags(agent: *ParsedExtensionAgent, object: anytype, name: []const u8) void {
    const value = object.get(name) orelse return;
    if (value != .array) return;
    for (value.array.items) |item| {
        if (item == .string) agent.addTag(item.string);
    }
}

fn addJsonStringArrayFieldToAgentCapabilities(agent: *ParsedExtensionAgent, object: anytype, name: []const u8) void {
    const value = object.get(name) orelse return;
    if (value != .array) return;
    for (value.array.items) |item| {
        if (item == .string) agent.addCapability(item.string);
    }
}

fn addJsonCapabilityArrayFieldToAgentCapabilities(agent: *ParsedExtensionAgent, object: anytype, name: []const u8) void {
    const value = object.get(name) orelse return;
    if (value != .array) return;
    for (value.array.items) |item| {
        switch (item) {
            .string => |capability| agent.addCapability(capability),
            .object => |capability_object| {
                const capability_name = jsonObjectStringField(capability_object, "name") orelse continue;
                agent.addCapability(capability_name);
            },
            else => {},
        }
    }
}

fn addJsonStringArrayFieldToAgentProtocols(agent: *ParsedExtensionAgent, object: anytype, name: []const u8) void {
    const value = object.get(name) orelse return;
    if (value != .array) return;
    for (value.array.items) |item| {
        if (item == .string) agent.addProtocol(item.string);
    }
}

fn addJsonStringArrayFieldToAgentRepresentativeQueries(agent: *ParsedExtensionAgent, object: anytype, name: []const u8) void {
    const value = object.get(name) orelse return;
    if (value != .array) return;
    for (value.array.items) |item| {
        if (item == .string) agent.addRepresentativeQuery(item.string);
    }
}

fn writeExtensionScopeString(writer: *std.Io.Writer, scope: extension_domain.ExtensionScope) !void {
    if (scope.kind == .table) {
        try writeStringFmt(writer, "table:{s}", .{scope.table_name});
    } else {
        try std.json.Stringify.value(@tagName(scope.kind), .{}, writer);
    }
}

fn writeCapabilitiesFromGrants(writer: *std.Io.Writer, capabilities: []const extension_domain.Capability) !void {
    try writer.writeByte('[');
    for (capabilities, 0..) |capability, index| {
        if (index > 0) try writer.writeByte(',');
        try std.json.Stringify.value(capability.name, .{}, writer);
    }
    try writer.writeByte(']');
}

fn writeTrustManifestPrefix(writer: *std.Io.Writer, publisher_domain: []const u8) !void {
    try writer.writeAll("{\"identity\":");
    try writeStringFmt(writer, "did:web:{s}", .{publisher_domain});
    try writer.writeAll(",\"identityType\":\"did\",\"trustSchema\":{\"identifier\":\"urn:antfly:ard:trust-schema:v1\",\"version\":\"1\",\"verificationMethods\":[\"did\",\"dns-01\"]}");
}

fn capabilityNamesAlloc(alloc: std.mem.Allocator, capabilities: []const extension_domain.Capability) ![][]const u8 {
    const names = try alloc.alloc([]const u8, capabilities.len);
    for (capabilities, 0..) |capability, index| names[index] = capability.name;
    return names;
}

fn installedExtensionVisible(installed: extension_domain.InstalledExtension, permissions: ?[]const usermgr.Permission) bool {
    if (installed.status != .ready) return false;
    const perms = permissions orelse return true;
    if (installed.scope.kind == .table) return identityHasPermission(perms, .table, installed.scope.table_name, .read);
    return identityHasPermission(perms, .@"*", "*", .admin);
}

fn visibleInstalledCanExposeExtension(
    alloc: std.mem.Allocator,
    installed: extension_domain.InstalledExtension,
    ctx: ExtensionCatalogContext,
) !bool {
    return installedExtensionVisible(installed, ctx.permissions) or
        try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions) or
        try installedExtensionHasVisibleSkill(alloc, installed, ctx.extension_members, ctx.permissions) or
        try installedExtensionHasVisibleAgent(alloc, installed, ctx.extension_members, ctx.permissions);
}

fn findInstalledPackage(packages: []const extension_domain.PackageManifest, installed: extension_domain.InstalledExtension) ?*const extension_domain.PackageManifest {
    for (packages) |*package| {
        if (std.mem.eql(u8, package.name, installed.package_name) and
            std.mem.eql(u8, package.version, installed.package_version) and
            (installed.package_digest.len == 0 or package.digest.len == 0 or std.mem.eql(u8, package.digest, installed.package_digest)))
        {
            return package;
        }
    }
    return null;
}

fn visiblePackageAlreadyEmitted(
    alloc: std.mem.Allocator,
    ctx: ExtensionCatalogContext,
    package: extension_domain.PackageManifest,
    before_index: usize,
) !bool {
    for (ctx.installed_extensions[0..before_index]) |installed| {
        const previous_package = findInstalledPackage(&.{package}, installed) orelse continue;
        _ = previous_package;
        if (try visibleInstalledCanExposeExtension(alloc, installed, ctx)) return true;
    }
    return false;
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
        if (try extensionMemberVisible(alloc, installed, member, permissions)) return true;
    }
    return false;
}

fn aggregateMcpVisible(alloc: std.mem.Allocator, extension_context: ?ExtensionCatalogContext) !bool {
    return try visibleMcpToolCount(alloc, extension_context) > 0;
}

fn copilotMcpProfileVisible(alloc: std.mem.Allocator, extension_context: ?ExtensionCatalogContext) !bool {
    return try visibleMcpToolCount(alloc, extension_context) > 0;
}

fn visibleMcpToolCount(alloc: std.mem.Allocator, extension_context: ?ExtensionCatalogContext) !usize {
    var count: usize = 0;
    const permissions = if (extension_context) |ctx| ctx.permissions else null;
    for (builtin_mcp_tools) |tool| {
        if (builtinMcpToolVisible(tool.kind, permissions)) count += 1;
    }
    if (extension_context) |ctx| {
        for (ctx.installed_extensions) |installed| {
            if (!(try installedExtensionHasVisibleMcpTool(alloc, installed, ctx.extension_members, ctx.permissions))) continue;
            for (ctx.extension_members) |member| {
                if (member.object_kind != .mcp_tool) continue;
                if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
                if (try extensionMemberVisible(alloc, installed, member, ctx.permissions)) count += 1;
            }
        }
    }
    return count;
}

fn builtinMcpToolVisible(kind: BuiltinMcpToolKind, permissions: ?[]const usermgr.Permission) bool {
    const perms = permissions orelse return true;
    return switch (kind) {
        .list_tables => identityHasPermission(perms, .table, "*", .read),
        .query, .get_document, .list_indexes => identityHasAnyPermission(perms, .table, .read),
        .batch => identityHasAnyPermission(perms, .table, .write),
        .create_table, .drop_table, .create_index, .drop_index, .backup, .restore => identityHasAnyPermission(perms, .table, .admin),
    };
}

fn installedExtensionHasVisibleSkill(
    alloc: std.mem.Allocator,
    installed: extension_domain.InstalledExtension,
    members: []const extension_domain.ExtensionMember,
    permissions: ?[]const usermgr.Permission,
) !bool {
    if (installed.status != .ready) return false;
    for (members) |member| {
        if (member.object_kind != .skill) continue;
        if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
        if (try extensionMemberVisible(alloc, installed, member, permissions)) return true;
    }
    return false;
}

fn installedExtensionHasVisibleAgent(
    alloc: std.mem.Allocator,
    installed: extension_domain.InstalledExtension,
    members: []const extension_domain.ExtensionMember,
    permissions: ?[]const usermgr.Permission,
) !bool {
    if (installed.status != .ready) return false;
    for (members) |member| {
        if (member.object_kind != .agent) continue;
        if (!std.mem.eql(u8, member.extension_name, installed.name)) continue;
        if (try extensionMemberVisible(alloc, installed, member, permissions)) return true;
    }
    return false;
}

fn extensionMemberVisible(
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

fn identityHasAnyPermission(permissions: []const usermgr.Permission, resource_type: usermgr.ResourceType, permission_type: usermgr.PermissionType) bool {
    for (permissions) |permission| {
        const type_match = permission.resource_type == .@"*" or permission.resource_type == resource_type;
        if (!type_match) continue;
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

fn extensionPackageEntryMatches(
    package: extension_domain.PackageManifest,
    capabilities: []const []const u8,
    text: ?[]const u8,
    filter: ?std.json.Value,
    publisher_domain: []const u8,
) bool {
    return dynamicEntryMatches(
        package.name,
        "application/antfly-extension-package+json",
        if (package.description.len > 0) package.description else "extension package",
        &.{ "extension", "package" },
        capabilities,
        text,
        filter,
        publisher_domain,
        true,
        struct {
            fn matches(ctx: *const anyopaque, key: []const u8, value: std.json.Value) bool {
                const item: *const extension_domain.PackageManifest = @ptrCast(@alignCast(ctx));
                if (std.mem.eql(u8, key, "digest")) return jsonValueMatchesString(value, item.digest);
                if (std.mem.eql(u8, key, "kind")) return jsonValueMatchesString(value, @tagName(item.kind));
                if (std.mem.eql(u8, key, "trusted")) return jsonValueMatchesBool(value, item.trusted);
                if (std.mem.eql(u8, key, "artifactCount")) return jsonValueMatchesInteger(value, @intCast(item.artifacts.len));
                if (std.mem.eql(u8, key, "capabilitiesRequestedCount")) return jsonValueMatchesInteger(value, @intCast(item.capabilities_requested.len));
                return false;
            }
        }.matches,
        struct {
            fn matches(ctx: *const anyopaque, key: []const u8, value: std.json.Value) bool {
                const item: *const extension_domain.PackageManifest = @ptrCast(@alignCast(ctx));
                if (std.mem.eql(u8, key, "provenance.relation")) return jsonValueMatchesString(value, "publishedFrom") or jsonValueMatchesString(value, "derivedFrom");
                if (std.mem.eql(u8, key, "provenance.sourceDigest")) {
                    if (jsonValueMatchesString(value, item.digest)) return true;
                    for (item.artifacts) |artifact| {
                        if (artifact.digest.len > 0 and jsonValueMatchesString(value, artifact.digest)) return true;
                    }
                    return false;
                }
                if (std.mem.eql(u8, key, "provenance.sourceId")) {
                    if (jsonValueMatchesFmt(value, "/extensions/v1/packages/{s}/versions/{s}", .{ item.name, item.version })) return true;
                    for (item.artifacts) |artifact| {
                        if (jsonValueMatchesString(value, artifact.path)) return true;
                    }
                    return false;
                }
                return false;
            }
        }.matches,
        &package,
    );
}

fn installedExtensionEntryMatches(
    installed: extension_domain.InstalledExtension,
    capabilities: []const []const u8,
    text: ?[]const u8,
    filter: ?std.json.Value,
    publisher_domain: []const u8,
) bool {
    return dynamicEntryMatches(
        installed.name,
        "application/antfly-installed-extension+json",
        "extension",
        &.{ "extension", "installed" },
        capabilities,
        text,
        filter,
        publisher_domain,
        true,
        struct {
            fn matches(ctx: *const anyopaque, key: []const u8, value: std.json.Value) bool {
                const item: *const extension_domain.InstalledExtension = @ptrCast(@alignCast(ctx));
                if (std.mem.eql(u8, key, "digest")) return jsonValueMatchesString(value, item.package_digest);
                if (std.mem.eql(u8, key, "packageName")) return jsonValueMatchesString(value, item.package_name);
                if (std.mem.eql(u8, key, "packageVersion")) return jsonValueMatchesString(value, item.package_version);
                if (std.mem.eql(u8, key, "endpoint")) return jsonValueMatchesFmt(value, "/extensions/v1/installed/{s}", .{item.name});
                if (std.mem.eql(u8, key, "status")) return jsonValueMatchesString(value, @tagName(item.status));
                if (std.mem.eql(u8, key, "scope")) return extensionScopeMatchesFilter(item.scope, value);
                if (std.mem.eql(u8, key, "scopeKind")) return jsonValueMatchesString(value, @tagName(item.scope.kind));
                if (std.mem.eql(u8, key, "scopeTableName")) return item.scope.kind == .table and jsonValueMatchesString(value, item.scope.table_name);
                if (std.mem.eql(u8, key, "grantedCapabilitiesCount")) return jsonValueMatchesInteger(value, @intCast(item.granted_capabilities.len));
                return false;
            }
        }.matches,
        struct {
            fn matches(ctx: *const anyopaque, key: []const u8, value: std.json.Value) bool {
                const item: *const extension_domain.InstalledExtension = @ptrCast(@alignCast(ctx));
                if (std.mem.eql(u8, key, "provenance.relation")) return jsonValueMatchesString(value, "derivedFrom");
                if (std.mem.eql(u8, key, "provenance.sourceDigest")) return item.package_digest.len > 0 and jsonValueMatchesString(value, item.package_digest);
                if (std.mem.eql(u8, key, "provenance.sourceId")) return jsonValueMatchesFmt(value, "/extensions/v1/packages/{s}/versions/{s}", .{ item.package_name, item.package_version });
                return false;
            }
        }.matches,
        &installed,
    );
}

fn extensionMcpEntryMatches(
    installed: extension_domain.InstalledExtension,
    capabilities: []const []const u8,
    text: ?[]const u8,
    filter: ?std.json.Value,
    publisher_domain: []const u8,
) bool {
    return dynamicEntryMatches(
        installed.name,
        "application/mcp-server+json",
        "mcp extension",
        &.{ "mcp", "extension" },
        capabilities,
        text,
        filter,
        publisher_domain,
        false,
        struct {
            fn matches(ctx: *const anyopaque, key: []const u8, value: std.json.Value) bool {
                const item: *const extension_domain.InstalledExtension = @ptrCast(@alignCast(ctx));
                if (std.mem.eql(u8, key, "endpoint")) return jsonValueMatchesFmt(value, "/mcp/v1/extensions/{s}", .{item.name});
                if (std.mem.eql(u8, key, "extension")) return jsonValueMatchesString(value, item.name);
                return false;
            }
        }.matches,
        struct {
            fn matches(_: *const anyopaque, _: []const u8, _: std.json.Value) bool {
                return false;
            }
        }.matches,
        &installed,
    );
}

fn dynamicEntryMatches(
    name: []const u8,
    media_type: []const u8,
    description: []const u8,
    tags: []const []const u8,
    capabilities: []const []const u8,
    text: ?[]const u8,
    filter: ?std.json.Value,
    publisher_domain: []const u8,
    has_trust_manifest: bool,
    metadataMatches: *const fn (*const anyopaque, []const u8, std.json.Value) bool,
    trustManifestMatches: *const fn (*const anyopaque, []const u8, std.json.Value) bool,
    metadata_context: *const anyopaque,
) bool {
    if (text) |query| {
        if (std.mem.trim(u8, query, " \t\r\n").len > 0 and
            !containsIgnoreCase(name, query) and
            !containsIgnoreCase(media_type, query) and
            !containsIgnoreCase(description, query) and
            !anyContainsIgnoreCase(tags, query) and
            !anyContainsIgnoreCase(capabilities, query)) return false;
    }
    if (filter) |filter_value| {
        var iterator = filter_value.object.iterator();
        while (iterator.next()) |kv| {
            const key = kv.key_ptr.*;
            const value = kv.value_ptr.*;
            if (std.mem.eql(u8, key, "type")) {
                if (!jsonValueMatchesString(value, media_type)) return false;
            } else if (std.mem.eql(u8, key, "tags")) {
                if (!jsonValueMatchesAnyString(value, tags)) return false;
            } else if (std.mem.eql(u8, key, "capabilities")) {
                if (!jsonValueMatchesAnyString(value, capabilities)) return false;
            } else if (std.mem.eql(u8, key, "publisher") or std.mem.eql(u8, key, "publisherId")) {
                if (!jsonValueMatchesString(value, publisher_domain)) return false;
            } else if (std.mem.startsWith(u8, key, "metadata.")) {
                if (!metadataMatches(metadata_context, key["metadata.".len..], value)) return false;
            } else if (std.mem.startsWith(u8, key, "trustManifest.")) {
                const trust_key = key["trustManifest.".len..];
                if (!has_trust_manifest or
                    (!commonTrustManifestMatches(publisher_domain, trust_key, value) and
                        !trustManifestMatches(metadata_context, trust_key, value))) return false;
            } else {
                return false;
            }
        }
    }
    return true;
}

fn isAgentLike(entry: Entry) bool {
    return std.mem.eql(u8, entry.media_type, "application/a2a-agent-card+json") or
        std.mem.eql(u8, entry.media_type, "application/mcp-server+json") or
        std.mem.eql(u8, entry.media_type, "application/antfly-agent+json");
}

fn catalogOptionsAllowStaticEntry(options: CatalogOptions, entry: Entry) bool {
    if (!options.a2a_enabled and std.mem.eql(u8, entry.media_type, "application/a2a-agent-card+json")) return false;
    if (entry.admin_only and !options.is_admin) return false;
    if (!catalogOptionsAllowRequiredPermission(options, entry.required_permission)) return false;
    return catalogOptionsAllowMedia(options, entry.media_type, entry.tags);
}

fn catalogOptionsAllowRequiredPermission(options: CatalogOptions, required: RequiredPermission) bool {
    return switch (required) {
        .none => true,
        .admin => options.is_admin,
        .table_read => if (options.permissions) |permissions| identityHasAnyPermission(permissions, .table, .read) else true,
        .table_admin => if (options.permissions) |permissions| identityHasAnyPermission(permissions, .table, .admin) else true,
    };
}

fn catalogOptionsAllowMedia(options: CatalogOptions, media_type: []const u8, tags: []const []const u8) bool {
    if (options.types) |types| {
        if (!commaListContains(types, media_type)) return false;
    }
    if (options.include) |include| {
        if (!entryClassIncluded(include, media_type, tags)) return false;
    }
    if (options.profile) |profile| {
        if (!std.mem.eql(u8, profile, "copilot")) return false;
        return jsonStringSliceContains(tags, profile);
    }
    return true;
}

fn entryClassIncluded(include: []const u8, media_type: []const u8, tags: []const []const u8) bool {
    if (commaListContains(include, "mcp") and std.mem.eql(u8, media_type, "application/mcp-server+json")) return true;
    if (commaListContains(include, "agents") and std.mem.eql(u8, media_type, "application/antfly-agent+json")) return true;
    if (commaListContains(include, "a2a") and std.mem.eql(u8, media_type, "application/a2a-agent-card+json")) return true;
    if (commaListContains(include, "openapi") and (std.mem.eql(u8, media_type, "application/openapi+yaml") or std.mem.eql(u8, media_type, "application/openapi+json"))) return true;
    if (commaListContains(include, "skills") and std.mem.eql(u8, media_type, "application/ai-skill+md")) return true;
    if (commaListContains(include, "extensions") and jsonStringSliceContains(tags, "extension")) return true;
    if (commaListContains(include, "registry") and std.mem.eql(u8, media_type, "application/ai-registry+json")) return true;
    if (commaListContains(include, "catalog") and std.mem.eql(u8, media_type, "application/ai-catalog+json")) return true;
    return false;
}

fn commaListContains(csv: []const u8, expected: []const u8) bool {
    var it = std.mem.splitScalar(u8, csv, ',');
    while (it.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        if (std.mem.eql(u8, trimmed, expected)) return true;
    }
    return false;
}

fn jsonStringSliceContains(values: []const []const u8, expected: []const u8) bool {
    for (values) |value| {
        if (std.mem.eql(u8, value, expected)) return true;
    }
    return false;
}

fn parseDefaultSkills(alloc: std.mem.Allocator) !std.json.Parsed([]DefaultSkillManifest) {
    return try std.json.parseFromSlice([]DefaultSkillManifest, alloc, default_skills_manifest_json, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
}

fn defaultSkillEntry(skill: DefaultSkillManifest) !Entry {
    return .{
        .identifier_suffix = skill.identifier_suffix orelse skill.slug,
        .display_name = skill.display_name,
        .media_type = "application/ai-skill+md",
        .description = skill.description,
        .url = skill.url,
        .metadata = skill.metadata,
        .tags = skill.tags,
        .capabilities = skill.capabilities,
        .representative_queries = skill.representative_queries,
        .admin_only = skill.admin_only,
        .required_permission = try parseRequiredPermission(skill.required_permission),
    };
}

fn findDefaultSkill(skills: []const DefaultSkillManifest, slug: []const u8) ?DefaultSkillManifest {
    for (skills) |skill| {
        if (std.mem.eql(u8, skill.slug, slug)) return skill;
    }
    return null;
}

fn parseRequiredPermission(value: []const u8) !RequiredPermission {
    if (std.mem.eql(u8, value, "none")) return .none;
    if (std.mem.eql(u8, value, "table_read")) return .table_read;
    if (std.mem.eql(u8, value, "table_admin")) return .table_admin;
    if (std.mem.eql(u8, value, "admin")) return .admin;
    return error.InvalidArdDefaultSkillPermission;
}

fn writeEntry(
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    entry: Entry,
) !void {
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try entry.write(writer, options);
}

fn writeSearchEntry(
    writer: *std.Io.Writer,
    options: CatalogOptions,
    first: *bool,
    entry: Entry,
    page_start: usize,
    page_size: usize,
    matched: *usize,
    text: ?[]const u8,
) !void {
    if (!recordSearchMatch(page_start, page_size, matched)) return;
    if (first.*) {
        first.* = false;
    } else {
        try writer.writeByte(',');
    }
    try entry.writeSearchResult(writer, options, if (text == null or text.?.len == 0) 100 else 90);
}

fn recordSearchMatch(page_start: usize, page_size: usize, matched: *usize) bool {
    matched.* += 1;
    return matched.* > page_start and matched.* <= page_start + page_size;
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
    if (std.mem.startsWith(u8, key, "metadata.")) return entry.metadata != null and containsJsonStringField(entry.metadata.?, key["metadata.".len..], value);
    if (std.mem.startsWith(u8, key, "trustManifest.")) {
        const trust_key = key["trustManifest.".len..];
        const trust_manifest = entry.trust_manifest orelse return false;
        return commonTrustManifestMatches(publisher_domain, trust_key, value) or containsJsonStringField(trust_manifest, trust_key, value);
    }
    return false;
}

fn commonTrustManifestMatches(publisher_domain: []const u8, key: []const u8, value: std.json.Value) bool {
    if (std.mem.eql(u8, key, "identity")) return jsonValueMatchesFmt(value, "did:web:{s}", .{publisher_domain});
    if (std.mem.eql(u8, key, "identityType")) return jsonValueMatchesString(value, "did");
    if (std.mem.eql(u8, key, "trustSchema.identifier")) return jsonValueMatchesString(value, "urn:antfly:ard:trust-schema:v1");
    if (std.mem.eql(u8, key, "trustSchema.version")) return jsonValueMatchesString(value, "1");
    if (std.mem.eql(u8, key, "trustSchema.verificationMethods")) return jsonValueMatchesString(value, "did") or jsonValueMatchesString(value, "dns-01");
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

fn jsonValueMatchesFmt(value: std.json.Value, comptime fmt: []const u8, args: anytype) bool {
    var buf: [512]u8 = undefined;
    const expected = std.fmt.bufPrint(&buf, fmt, args) catch return false;
    return jsonValueMatchesString(value, expected);
}

fn jsonValueMatchesBool(value: std.json.Value, expected: bool) bool {
    return switch (value) {
        .bool => |actual| actual == expected,
        .string => |actual| std.mem.eql(u8, actual, if (expected) "true" else "false"),
        .array => |array| blk: {
            for (array.items) |item| {
                if (jsonValueMatchesBool(item, expected)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

fn jsonValueMatchesInteger(value: std.json.Value, expected: i64) bool {
    return switch (value) {
        .integer => |actual| actual == expected,
        .string => |actual| blk: {
            const parsed = std.fmt.parseInt(i64, actual, 10) catch break :blk false;
            break :blk parsed == expected;
        },
        .array => |array| blk: {
            for (array.items) |item| {
                if (jsonValueMatchesInteger(item, expected)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

fn extensionScopeMatchesFilter(scope: extension_domain.ExtensionScope, value: std.json.Value) bool {
    if (scope.kind != .table) return jsonValueMatchesString(value, @tagName(scope.kind));
    return jsonValueMatchesFmt(value, "table:{s}", .{scope.table_name});
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

fn writeUrlValue(writer: *std.Io.Writer, base_url: ?[]const u8, url: []const u8) !void {
    const base = base_url orelse return try std.json.Stringify.value(url, .{}, writer);
    if (base.len == 0 or !std.mem.startsWith(u8, url, "/")) return try std.json.Stringify.value(url, .{}, writer);
    try writeStringFmt(writer, "{s}{s}", .{ trimTrailingSlashes(base), url });
}

fn writeUrlFmt(writer: *std.Io.Writer, base_url: ?[]const u8, comptime fmt: []const u8, args: anytype) !void {
    var buf: [512]u8 = undefined;
    const url = try std.fmt.bufPrint(&buf, fmt, args);
    try writeUrlValue(writer, base_url, url);
}

fn writeStringFmt(writer: *std.Io.Writer, comptime fmt: []const u8, args: anytype) !void {
    var buf: [512]u8 = undefined;
    const value = try std.fmt.bufPrint(&buf, fmt, args);
    try std.json.Stringify.value(value, .{}, writer);
}

fn trimTrailingSlashes(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') end -= 1;
    return value[0..end];
}

test "ARD catalog entries contain required value or reference fields" {
    const packages = [_]extension_domain.PackageManifest{.{
        .name = "docsaf",
        .version = "1.0.0",
        .digest = "sha256:docs",
        .trusted = true,
        .capabilities_requested = &.{.{ .name = "db:read", .scope = "docsaf" }},
        .artifacts = &.{.{ .kind = .wasm, .path = "docsaf.wasm", .digest = "sha256:docs-wasm" }},
        .install = .{ .scopes_supported = &.{.table} },
    }};
    const installed = [_]extension_domain.InstalledExtension{.{
        .name = "docsaf",
        .package_name = "docsaf",
        .package_version = "1.0.0",
        .package_digest = "sha256:docs",
        .scope = .{ .kind = .table, .table_name = "docs" },
        .granted_capabilities = &.{.{ .name = "db:read", .scope = "docsaf" }},
        .status = .ready,
    }};
    const members = [_]extension_domain.ExtensionMember{
        .{
            .extension_name = "docsaf",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .mcp_tool,
            .object_name = "search_docs",
            .table_name = "docs",
            .owner_metadata_json = "{\"description\":\"Search docs\",\"input_schema\":{\"type\":\"object\"}}",
        },
        .{
            .extension_name = "docsaf",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .skill,
            .object_name = "docs",
            .owner_metadata_json = "{\"displayName\":\"Docsaf Skill\",\"description\":\"Use Docsaf from ARD.\",\"tags\":[\"docs\"],\"capabilities\":[\"docs-search\"],\"body\":\"# Docsaf\"}",
        },
    };
    const ctx: ExtensionCatalogContext = .{
        .extension_packages = &packages,
        .installed_extensions = &installed,
        .extension_members = &members,
    };

    const body = try catalogJsonWithExtensionsAlloc(std.testing.allocator, .{ .mode = .tenant }, ctx);
    defer std.testing.allocator.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();

    const root = parsed.value.object;
    try std.testing.expect(root.get("specVersion") != null);
    try std.testing.expect(root.get("host").?.object.get("trustManifest") != null);
    const entries = root.get("entries").?.array.items;
    try std.testing.expect(entries.len >= 13);
    _ = try findCatalogEntryByIdentifierSuffix(parsed.value, ":extension:package:docsaf:1.0.0");
    _ = try findCatalogEntryByIdentifierSuffix(parsed.value, ":extension:docsaf:installed");
    _ = try findCatalogEntryByIdentifierSuffix(parsed.value, ":extension:docsaf:mcp");
    _ = try findCatalogEntryByIdentifierSuffix(parsed.value, ":extension:docsaf:skill:docs");
    const builtin_skill_entry = try findCatalogEntryByIdentifierSuffix(parsed.value, ":skill:antfly-retrieval");
    try std.testing.expectEqualStrings("/ard/v1/skills/antfly-retrieval", builtin_skill_entry.object.get("url").?.string);
    for (entries) |entry| {
        const object = entry.object;
        try std.testing.expect(object.get("identifier") != null);
        try std.testing.expect(object.get("displayName") != null);
        try std.testing.expect(object.get("type") != null);
        const has_url = object.get("url") != null;
        const has_data = object.get("data") != null;
        try std.testing.expect(has_url != has_data);
        if (object.get("metadata")) |metadata| try expectMetadataValuesAreScalars(metadata);
    }
}

test "ARD catalog omits A2A discovery when disabled" {
    const hidden = try catalogJsonAlloc(std.testing.allocator, .{ .a2a_enabled = false });
    defer std.testing.allocator.free(hidden);
    try std.testing.expect(std.mem.indexOf(u8, hidden, "application/a2a-agent-card+json") == null);
    try std.testing.expect(std.mem.indexOf(u8, hidden, "/.well-known/agent-card.json") == null);

    const enabled = try catalogJsonAlloc(std.testing.allocator, .{ .a2a_enabled = true });
    defer std.testing.allocator.free(enabled);
    try std.testing.expect(std.mem.indexOf(u8, enabled, "application/a2a-agent-card+json") != null);
    try std.testing.expect(std.mem.indexOf(u8, enabled, "/.well-known/agent-card.json") != null);
}

test "ARD catalog resolves artifact urls against configured base url" {
    const packages = [_]extension_domain.PackageManifest{.{
        .name = "docsaf",
        .version = "1.0.0",
        .digest = "sha256:docs",
        .capabilities_requested = &.{.{ .name = "db:read", .scope = "docsaf" }},
        .install = .{ .scopes_supported = &.{.table} },
    }};
    const installed = [_]extension_domain.InstalledExtension{.{
        .name = "docsaf",
        .package_name = "docsaf",
        .package_version = "1.0.0",
        .package_digest = "sha256:docs",
        .scope = .{ .kind = .table, .table_name = "docs" },
        .granted_capabilities = &.{.{ .name = "db:read", .scope = "docsaf" }},
        .status = .ready,
    }};
    const members = [_]extension_domain.ExtensionMember{.{
        .extension_name = "docsaf",
        .scope = .{ .kind = .table, .table_name = "docs" },
        .object_kind = .mcp_tool,
        .object_name = "search_docs",
        .table_name = "docs",
        .owner_metadata_json = "{\"description\":\"Search docs\",\"input_schema\":{\"type\":\"object\"}}",
    }};
    const ctx: ExtensionCatalogContext = .{
        .extension_packages = &packages,
        .installed_extensions = &installed,
        .extension_members = &members,
    };

    const body = try catalogJsonWithExtensionsAlloc(
        std.testing.allocator,
        .{ .mode = .tenant, .base_url = "https://tenant.example.com/" },
        ctx,
    );
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"url\":\"https://tenant.example.com/ard/v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"url\":\"https://tenant.example.com/ard/v1/resources/mcp/default\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"url\":\"https://tenant.example.com/extensions/v1/packages/docsaf/versions/1.0.0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"url\":\"https://tenant.example.com/extensions/v1/installed/docsaf\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"url\":\"https://tenant.example.com/ard/v1/resources/mcp/extensions/docsaf\"") != null);
}

test "ARD MCP descriptors resolve endpoints against configured base url" {
    const installed = [_]extension_domain.InstalledExtension{.{
        .name = "docsaf",
        .package_name = "docsaf",
        .package_version = "1.0.0",
        .package_digest = "sha256:docs",
        .scope = .{ .kind = .table, .table_name = "docs" },
        .granted_capabilities = &.{.{ .name = "db:read", .scope = "docsaf" }},
        .status = .ready,
    }};
    const members = [_]extension_domain.ExtensionMember{.{
        .extension_name = "docsaf",
        .scope = .{ .kind = .table, .table_name = "docs" },
        .object_kind = .mcp_tool,
        .object_name = "search_docs",
        .table_name = "docs",
        .owner_metadata_json = "{\"description\":\"Search docs\",\"input_schema\":{\"type\":\"object\"}}",
    }};
    const ctx: ExtensionCatalogContext = .{
        .installed_extensions = &installed,
        .extension_members = &members,
    };

    const aggregate = (try mcpDescriptorJsonAlloc(std.testing.allocator, "default", .{ .base_url = "https://tenant.example.com/" }, ctx)).?;
    defer std.testing.allocator.free(aggregate);
    try std.testing.expect(std.mem.indexOf(u8, aggregate, "\"endpoint\":\"https://tenant.example.com/mcp/v1\"") != null);

    const extension = (try mcpDescriptorJsonAlloc(std.testing.allocator, "extensions/docsaf", .{ .base_url = "https://tenant.example.com/" }, ctx)).?;
    defer std.testing.allocator.free(extension);
    try std.testing.expect(std.mem.indexOf(u8, extension, "\"endpoint\":\"https://tenant.example.com/mcp/v1/extensions/docsaf\"") != null);

    const profile = (try mcpDescriptorJsonAlloc(std.testing.allocator, "profiles/copilot", .{ .base_url = "https://tenant.example.com/" }, ctx)).?;
    defer std.testing.allocator.free(profile);
    try std.testing.expect(std.mem.indexOf(u8, profile, "\"endpoint\":\"https://tenant.example.com/mcp/v1/extensions/profiles/copilot\"") != null);

    const local = (try mcpDescriptorJsonAlloc(std.testing.allocator, "default", .{}, ctx)).?;
    defer std.testing.allocator.free(local);
    try std.testing.expect(std.mem.indexOf(u8, local, "\"endpoint\":\"/mcp/v1\"") != null);
}

test "ARD catalog hides admin-only built-in skills from non-admin entries" {
    var read_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(std.testing.allocator, .table, "docs", .read),
    };
    defer read_permission[0].deinit(std.testing.allocator);
    const non_admin = try catalogJsonAlloc(std.testing.allocator, .{ .mode = .tenant, .is_admin = false, .permissions = &read_permission });
    defer std.testing.allocator.free(non_admin);
    try std.testing.expect(std.mem.indexOf(u8, non_admin, "Antfly Extension Management") == null);

    const admin = try catalogJsonAlloc(std.testing.allocator, .{ .mode = .tenant, .is_admin = true, .permissions = &read_permission });
    defer std.testing.allocator.free(admin);
    try std.testing.expect(std.mem.indexOf(u8, admin, "Antfly Extension Management") != null);

    const hidden_skill = try skillMarkdownAlloc(std.testing.allocator, .{ .mode = .tenant, .is_admin = false, .permissions = &read_permission }, "antfly-extension-management");
    try std.testing.expect(hidden_skill == null);

    const visible_skill = (try skillMarkdownAlloc(std.testing.allocator, .{ .mode = .tenant, .is_admin = true, .permissions = &read_permission }, "antfly-extension-management")).?;
    defer std.testing.allocator.free(visible_skill);
    try std.testing.expect(std.mem.indexOf(u8, visible_skill, "# Antfly Extension Management") != null);
}

test "ARD catalog applies declared table permissions to built-in skills" {
    const no_permission = try catalogJsonAlloc(std.testing.allocator, .{
        .mode = .tenant,
        .permissions = &.{},
    });
    defer std.testing.allocator.free(no_permission);
    try std.testing.expect(std.mem.indexOf(u8, no_permission, "Antfly Query Builder") == null);
    try std.testing.expect(std.mem.indexOf(u8, no_permission, "Antfly Retrieval") == null);
    try std.testing.expect(std.mem.indexOf(u8, no_permission, "Antfly Schema Design") == null);

    var read_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(std.testing.allocator, .table, "docs", .read),
    };
    defer read_permission[0].deinit(std.testing.allocator);
    const read_catalog = try catalogJsonAlloc(std.testing.allocator, .{
        .mode = .tenant,
        .permissions = &read_permission,
    });
    defer std.testing.allocator.free(read_catalog);
    try std.testing.expect(std.mem.indexOf(u8, read_catalog, "Antfly Query Builder") != null);
    try std.testing.expect(std.mem.indexOf(u8, read_catalog, "Antfly Retrieval") != null);
    try std.testing.expect(std.mem.indexOf(u8, read_catalog, "Antfly Schema Design") == null);

    var admin_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(std.testing.allocator, .table, "docs", .admin),
    };
    defer admin_permission[0].deinit(std.testing.allocator);
    const table_admin_catalog = try catalogJsonAlloc(std.testing.allocator, .{
        .mode = .tenant,
        .permissions = &admin_permission,
    });
    defer std.testing.allocator.free(table_admin_catalog);
    try std.testing.expect(std.mem.indexOf(u8, table_admin_catalog, "Antfly Schema Design") != null);

    const hidden_retrieval = try skillMarkdownAlloc(std.testing.allocator, .{ .mode = .tenant, .permissions = &.{} }, "antfly-retrieval");
    try std.testing.expect(hidden_retrieval == null);
    const visible_retrieval = (try skillMarkdownAlloc(std.testing.allocator, .{ .mode = .tenant, .permissions = &read_permission }, "antfly-retrieval")).?;
    defer std.testing.allocator.free(visible_retrieval);
    try std.testing.expect(std.mem.indexOf(u8, visible_retrieval, "# Antfly Retrieval") != null);
}

test "ARD extension package entries use trust provenance for artifact digests" {
    const packages = [_]extension_domain.PackageManifest{.{
        .name = "docsaf",
        .version = "1.0.0",
        .digest = "sha256:docs",
        .trusted = true,
        .capabilities_requested = &.{.{ .name = "db:read", .scope = "docsaf" }},
        .artifacts = &.{
            .{ .kind = .manifest, .path = "extension.json", .digest = "sha256:docs-manifest" },
            .{ .kind = .wasm, .path = "docsaf.wasm", .digest = "sha256:docs-wasm" },
        },
        .install = .{ .scopes_supported = &.{.table} },
    }};
    const installed = [_]extension_domain.InstalledExtension{.{
        .name = "docsaf",
        .package_name = "docsaf",
        .package_version = "1.0.0",
        .package_digest = "sha256:docs",
        .scope = .{ .kind = .table, .table_name = "docs" },
        .granted_capabilities = &.{.{ .name = "db:read", .scope = "docsaf" }},
        .status = .ready,
    }};
    const ctx: ExtensionCatalogContext = .{
        .extension_packages = &packages,
        .installed_extensions = &installed,
    };

    const body = try catalogJsonWithExtensionsAlloc(std.testing.allocator, .{ .mode = .tenant, .base_url = "https://tenant.example.com/" }, ctx);
    defer std.testing.allocator.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();

    const package_entry = try findCatalogEntryByIdentifierSuffix(parsed.value, ":extension:package:docsaf:1.0.0");
    const package_metadata = package_entry.object.get("metadata").?;
    try expectMetadataValuesAreScalars(package_metadata);
    try std.testing.expect(package_metadata.object.get("artifacts") == null);
    try std.testing.expectEqual(@as(i64, 2), package_metadata.object.get("artifactCount").?.integer);

    const trust_manifest = package_entry.object.get("trustManifest").?;
    try std.testing.expectEqualStrings("did:web:antfly.local", trust_manifest.object.get("identity").?.string);
    const provenance = trust_manifest.object.get("provenance").?.array.items;
    try std.testing.expect(provenance.len >= 3);
    try std.testing.expectEqualStrings("https://tenant.example.com/extensions/v1/packages/docsaf/versions/1.0.0", provenance[0].object.get("sourceId").?.string);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"sourceDigest\":\"sha256:docs-wasm\"") != null);

    const installed_entry = try findCatalogEntryByIdentifierSuffix(parsed.value, ":extension:docsaf:installed");
    try expectMetadataValuesAreScalars(installed_entry.object.get("metadata").?);
    try std.testing.expectEqualStrings("table:docs", installed_entry.object.get("metadata").?.object.get("scope").?.string);
    const installed_trust_manifest = installed_entry.object.get("trustManifest").?;
    const installed_provenance = installed_trust_manifest.object.get("provenance").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), installed_provenance.len);
    try std.testing.expectEqualStrings("https://tenant.example.com/extensions/v1/packages/docsaf/versions/1.0.0", installed_provenance[0].object.get("sourceId").?.string);
}

test "ARD search supports extension metadata filters" {
    const packages = [_]extension_domain.PackageManifest{.{
        .name = "docsaf",
        .version = "1.0.0",
        .digest = "sha256:docs",
        .trusted = true,
        .capabilities_requested = &.{.{ .name = "db:read", .scope = "docsaf" }},
        .artifacts = &.{
            .{ .kind = .manifest, .path = "extension.json", .digest = "sha256:docs-manifest" },
            .{ .kind = .wasm, .path = "docsaf.wasm", .digest = "sha256:docs-wasm" },
        },
        .install = .{ .scopes_supported = &.{.table} },
    }};
    const installed = [_]extension_domain.InstalledExtension{.{
        .name = "docsaf",
        .package_name = "docsaf",
        .package_version = "1.0.0",
        .package_digest = "sha256:docs",
        .scope = .{ .kind = .table, .table_name = "docs" },
        .granted_capabilities = &.{.{ .name = "db:read", .scope = "docsaf" }},
        .status = .ready,
    }};
    const members = [_]extension_domain.ExtensionMember{
        .{
            .extension_name = "docsaf",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .mcp_tool,
            .object_name = "search_docs",
            .table_name = "docs",
            .owner_metadata_json = "{\"description\":\"Search docs\",\"input_schema\":{\"type\":\"object\"}}",
        },
        .{
            .extension_name = "docsaf",
            .scope = .{ .kind = .table, .table_name = "docs" },
            .object_kind = .skill,
            .object_name = "docs",
            .table_name = "docs",
            .owner_metadata_json = "{\"displayName\":\"Docsaf Skill\",\"description\":\"Use Docsaf from ARD.\",\"profile\":\"copilot\",\"tags\":[\"docs\"],\"capabilities\":[\"docs-search\"],\"body\":\"# Docsaf\"}",
        },
    };
    const ctx: ExtensionCatalogContext = .{
        .extension_packages = &packages,
        .installed_extensions = &installed,
        .extension_members = &members,
    };

    const package_body = try searchJsonWithExtensionsAlloc(
        std.testing.allocator,
        .{ .mode = .tenant },
        "{\"query\":{\"text\":\"docsaf\",\"filter\":{\"type\":[\"application/antfly-extension-package+json\"],\"metadata.trusted\":[true],\"metadata.artifactCount\":[2],\"metadata.digest\":[\"sha256:docs\"]}},\"federation\":\"none\"}",
        false,
        ctx,
    );
    defer std.testing.allocator.free(package_body);
    var package_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, package_body, .{});
    defer package_parsed.deinit();
    const package_results = package_parsed.value.object.get("results").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), package_results.len);
    try std.testing.expect(std.mem.indexOf(u8, package_results[0].object.get("identifier").?.string, ":extension:package:docsaf:1.0.0") != null);

    const installed_body = try searchJsonWithExtensionsAlloc(
        std.testing.allocator,
        .{ .mode = .tenant },
        "{\"query\":{\"text\":\"docsaf\",\"filter\":{\"type\":[\"application/antfly-installed-extension+json\"],\"metadata.scope\":[\"table:docs\"],\"metadata.scopeKind\":[\"table\"],\"metadata.status\":[\"ready\"]}},\"federation\":\"none\"}",
        false,
        ctx,
    );
    defer std.testing.allocator.free(installed_body);
    var installed_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, installed_body, .{});
    defer installed_parsed.deinit();
    const installed_results = installed_parsed.value.object.get("results").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), installed_results.len);
    try std.testing.expect(std.mem.indexOf(u8, installed_results[0].object.get("identifier").?.string, ":extension:docsaf:installed") != null);

    const mcp_body = try searchJsonWithExtensionsAlloc(
        std.testing.allocator,
        .{ .mode = .tenant },
        "{\"query\":{\"text\":\"docsaf\",\"filter\":{\"type\":[\"application/mcp-server+json\"],\"metadata.extension\":[\"docsaf\"],\"metadata.endpoint\":[\"/mcp/v1/extensions/docsaf\"]}},\"federation\":\"none\"}",
        false,
        ctx,
    );
    defer std.testing.allocator.free(mcp_body);
    var mcp_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, mcp_body, .{});
    defer mcp_parsed.deinit();
    const mcp_results = mcp_parsed.value.object.get("results").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), mcp_results.len);
    try std.testing.expect(std.mem.indexOf(u8, mcp_results[0].object.get("identifier").?.string, ":extension:docsaf:mcp") != null);

    const skill_body = try searchJsonWithExtensionsAlloc(
        std.testing.allocator,
        .{ .mode = .tenant },
        "{\"query\":{\"text\":\"docsaf\",\"filter\":{\"type\":[\"application/ai-skill+md\"],\"metadata.scope\":[\"extension\"],\"metadata.extension\":[\"docsaf\"],\"metadata.skill\":[\"docs\"],\"metadata.objectKind\":[\"skill\"],\"metadata.profile\":[\"copilot\"]}},\"federation\":\"none\"}",
        false,
        ctx,
    );
    defer std.testing.allocator.free(skill_body);
    var skill_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, skill_body, .{});
    defer skill_parsed.deinit();
    const skill_results = skill_parsed.value.object.get("results").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), skill_results.len);
    try std.testing.expect(std.mem.indexOf(u8, skill_results[0].object.get("identifier").?.string, ":extension:docsaf:skill:docs") != null);

    const unknown_skill_filter_body = try searchJsonWithExtensionsAlloc(
        std.testing.allocator,
        .{ .mode = .tenant },
        "{\"query\":{\"text\":\"docsaf\",\"filter\":{\"type\":[\"application/ai-skill+md\"],\"metadata.unknown\":[\"docsaf\"]}},\"federation\":\"none\"}",
        false,
        ctx,
    );
    defer std.testing.allocator.free(unknown_skill_filter_body);
    var unknown_skill_filter_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, unknown_skill_filter_body, .{});
    defer unknown_skill_filter_parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), unknown_skill_filter_parsed.value.object.get("results").?.array.items.len);

    const package_trust_body = try searchJsonWithExtensionsAlloc(
        std.testing.allocator,
        .{ .mode = .tenant },
        "{\"query\":{\"text\":\"docsaf\",\"filter\":{\"type\":[\"application/antfly-extension-package+json\"],\"trustManifest.identity\":[\"did:web:antfly.local\"],\"trustManifest.identityType\":[\"did\"],\"trustManifest.trustSchema.identifier\":[\"urn:antfly:ard:trust-schema:v1\"],\"trustManifest.provenance.sourceDigest\":[\"sha256:docs-wasm\"],\"trustManifest.provenance.relation\":[\"derivedFrom\"]}},\"federation\":\"none\"}",
        false,
        ctx,
    );
    defer std.testing.allocator.free(package_trust_body);
    var package_trust_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, package_trust_body, .{});
    defer package_trust_parsed.deinit();
    const package_trust_results = package_trust_parsed.value.object.get("results").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), package_trust_results.len);
    try std.testing.expect(std.mem.indexOf(u8, package_trust_results[0].object.get("identifier").?.string, ":extension:package:docsaf:1.0.0") != null);

    const installed_trust_body = try searchJsonWithExtensionsAlloc(
        std.testing.allocator,
        .{ .mode = .tenant },
        "{\"query\":{\"text\":\"docsaf\",\"filter\":{\"type\":[\"application/antfly-installed-extension+json\"],\"trustManifest.provenance.sourceDigest\":[\"sha256:docs\"],\"trustManifest.provenance.relation\":[\"derivedFrom\"]}},\"federation\":\"none\"}",
        false,
        ctx,
    );
    defer std.testing.allocator.free(installed_trust_body);
    var installed_trust_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, installed_trust_body, .{});
    defer installed_trust_parsed.deinit();
    const installed_trust_results = installed_trust_parsed.value.object.get("results").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), installed_trust_results.len);
    try std.testing.expect(std.mem.indexOf(u8, installed_trust_results[0].object.get("identifier").?.string, ":extension:docsaf:installed") != null);

    const mcp_trust_body = try searchJsonWithExtensionsAlloc(
        std.testing.allocator,
        .{ .mode = .tenant },
        "{\"query\":{\"text\":\"docsaf\",\"filter\":{\"type\":[\"application/mcp-server+json\"],\"trustManifest.identity\":[\"did:web:antfly.local\"]}},\"federation\":\"none\"}",
        false,
        ctx,
    );
    defer std.testing.allocator.free(mcp_trust_body);
    var mcp_trust_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, mcp_trust_body, .{});
    defer mcp_trust_parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), mcp_trust_parsed.value.object.get("results").?.array.items.len);
}

test "ARD profile filter keeps only profile-compatible skills" {
    const installed = [_]extension_domain.InstalledExtension{.{
        .name = "memoryaf",
        .package_name = "memoryaf",
        .package_version = "1.0.0",
        .package_digest = "sha256:memory",
        .scope = .{ .kind = .table, .table_name = "memories" },
        .status = .ready,
    }};
    const members = [_]extension_domain.ExtensionMember{.{
        .extension_name = "memoryaf",
        .scope = .{ .kind = .table, .table_name = "memories" },
        .object_kind = .skill,
        .object_name = "memory",
        .table_name = "memories",
        .owner_metadata_json = "{\"displayName\":\"Memoryaf\",\"description\":\"Use Memoryaf from Copilot.\",\"profile\":\"copilot\",\"tags\":[\"memory\"],\"capabilities\":[\"memory-search\"],\"body\":\"# Memoryaf\"}",
    }};
    const ctx: ExtensionCatalogContext = .{
        .installed_extensions = &installed,
        .extension_members = &members,
    };

    const body = try catalogJsonWithExtensionsAlloc(
        std.testing.allocator,
        .{ .mode = .tenant, .profile = "copilot" },
        ctx,
    );
    defer std.testing.allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "Antfly Copilot MCP Profile") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "urn:ai:antfly.local:antfly:extension:memoryaf:skill:memory") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "Antfly MCP Server") == null);
    try std.testing.expect(std.mem.indexOf(u8, body, "Antfly Query Builder") == null);
    try std.testing.expect(std.mem.indexOf(u8, body, "Antfly Retrieval") == null);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();
    const entries = parsed.value.object.get("entries").?.array.items;
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.object.get("type").?.string, "application/ai-skill+md")) {
            const metadata = entry.object.get("metadata").?.object;
            try std.testing.expectEqualStrings("copilot", metadata.get("profile").?.string);
        }
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

test "ARD search requires text while explore accepts filter-only requests" {
    try std.testing.expectError(error.InvalidArdSearchRequest, searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"filter\":{\"type\":[\"application/ai-skill+md\"]}}}", false));
    try std.testing.expectError(error.InvalidArdSearchRequest, searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"   \",\"filter\":{\"type\":[\"application/ai-skill+md\"]}}}", false));

    const body = try searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"filter\":{\"type\":[\"application/ai-skill+md\"]}},\"resultType\":{\"facets\":[{\"field\":\"type\"}]}}", true);
    defer std.testing.allocator.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("facets", parsed.value.object.get("resultType").?.string);
    try expectFacetBucket(parsed.value.object.get("facets").?.object.get("type").?, "application/ai-skill+md");
}

test "ARD search supports publisher and metadata filters" {
    const body = try searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"OpenAPI\",\"filter\":{\"publisher\":[\"antfly.local\"],\"metadata.sourceSpec\":[\"openapi.yaml\"]}},\"federation\":\"none\"}", false);
    defer std.testing.allocator.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();

    const results = parsed.value.object.get("results").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("Antfly Public OpenAPI", results[0].object.get("displayName").?.string);
}

test "ARD search validates federation and returns referral envelope" {
    const default_body = try searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"retrieval\"}}", false);
    defer std.testing.allocator.free(default_body);

    var default_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, default_body, .{});
    defer default_parsed.deinit();

    try std.testing.expectEqualStrings("auto", default_parsed.value.object.get("federation").?.string);
    try std.testing.expect(default_parsed.value.object.get("referrals") != null);
    try std.testing.expectEqual(@as(usize, 0), default_parsed.value.object.get("referrals").?.array.items.len);

    const body = try searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"retrieval\"},\"federation\":\"referrals\"}", false);
    defer std.testing.allocator.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("referrals", parsed.value.object.get("federation").?.string);
    try std.testing.expect(parsed.value.object.get("referrals") != null);
    try std.testing.expectEqual(@as(usize, 0), parsed.value.object.get("referrals").?.array.items.len);

    const paged_body = try searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"Antfly\"},\"federation\":\"none\",\"pageSize\":2}", false);
    defer std.testing.allocator.free(paged_body);

    var paged_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, paged_body, .{});
    defer paged_parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), paged_parsed.value.object.get("results").?.array.items.len);
    try std.testing.expect(paged_parsed.value.object.get("count").?.integer > 2);
    try std.testing.expectEqualStrings("2", paged_parsed.value.object.get("pageToken").?.string);

    const next_page_body = try searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"Antfly\"},\"federation\":\"none\",\"pageSize\":2,\"pageToken\":\"2\"}", false);
    defer std.testing.allocator.free(next_page_body);

    var next_page_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, next_page_body, .{});
    defer next_page_parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), next_page_parsed.value.object.get("results").?.array.items.len);
    try std.testing.expectEqual(paged_parsed.value.object.get("count").?.integer, next_page_parsed.value.object.get("count").?.integer);

    try std.testing.expectError(error.InvalidArdSearchRequest, searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"retrieval\"},\"federation\":\"recursive\"}", false));
    try std.testing.expectError(error.InvalidArdSearchRequest, searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"retrieval\"},\"pageSize\":0}", false));
    try std.testing.expectError(error.InvalidArdSearchRequest, searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"retrieval\"},\"pageSize\":101}", false));
    try std.testing.expectError(error.InvalidArdSearchRequest, searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"retrieval\"},\"pageToken\":2}", false));
    try std.testing.expectError(error.InvalidArdSearchRequest, searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"text\":\"retrieval\"},\"pageToken\":\"not-a-token\"}", false));
}

test "ARD explore returns requested facet buckets over scoped entries" {
    const body = try searchJsonAlloc(std.testing.allocator, .{ .mode = .tenant }, "{\"query\":{\"filter\":{\"capabilities\":[\"retrieval\"]}},\"resultType\":{\"facets\":[{\"field\":\"type\"},{\"field\":\"capabilities\"},{\"field\":\"publisher\"}]}}", true);
    defer std.testing.allocator.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer parsed.deinit();

    try std.testing.expectEqualStrings("facets", parsed.value.object.get("resultType").?.string);
    const facets = parsed.value.object.get("facets").?.object;
    try expectFacetBucket(facets.get("type").?, "application/mcp-server+json");
    try expectFacetBucket(facets.get("capabilities").?, "retrieval");
    try expectFacetBucket(facets.get("publisher").?, "antfly.local");
}

fn expectFacetBucket(facet: std.json.Value, expected: []const u8) !void {
    const buckets = facet.object.get("buckets").?.array.items;
    for (buckets) |bucket| {
        if (std.mem.eql(u8, bucket.object.get("value").?.string, expected)) return;
    }
    return error.MissingFacetBucket;
}

fn expectMetadataValuesAreScalars(metadata: std.json.Value) !void {
    if (metadata != .object) return error.MetadataMustBeObject;
    var iterator = metadata.object.iterator();
    while (iterator.next()) |kv| {
        switch (kv.value_ptr.*) {
            .string, .integer, .float, .bool, .null => {},
            else => return error.NonScalarMetadataValue,
        }
    }
}

fn findCatalogEntryByIdentifierSuffix(root: std.json.Value, suffix: []const u8) !std.json.Value {
    for (root.object.get("entries").?.array.items) |entry| {
        const identifier = entry.object.get("identifier").?.string;
        if (std.mem.endsWith(u8, identifier, suffix)) return entry;
    }
    return error.CatalogEntryNotFound;
}
