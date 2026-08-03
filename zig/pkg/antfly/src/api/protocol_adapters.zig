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
const routes = @import("http_routes.zig");
const catalog_resources = @import("catalog_resources.zig");
const http_common = @import("../raft/transport/http_common.zig");
const extension_domain = @import("../extensions/mod.zig");
const wasmtime_runtime = extension_domain.wasmtime_runtime;
const usermgr = @import("../usermgr/mod.zig");
const mcp = @import("antfly_mcp");
const a2a = @import("antfly_a2a");

const trusted_principal_header = "X-Antfly-Trusted-Principal";
const max_mcp_sample_documents_limit: i64 = 100;

pub const DelegatedIdentity = struct {
    username: []const u8,
    permissions: []const usermgr.Permission = &.{},
    row_filter: []const usermgr.RowFilterEntry = &.{},
    metadata_json: []const u8 = &.{},
    roles: []const []const u8 = &.{},
    role_settings: []const usermgr.RoleSetting = &.{},
    role_runtime_settings: []const usermgr.RoleSetting = &.{},
};

const McpToolKind = enum {
    create_table,
    drop_table,
    list_tables,
    describe_table,
    create_index,
    drop_index,
    list_indexes,
    describe_indexes,
    get_document,
    sample_documents,
    query,
    describe_query_request,
    describe_mcp_capabilities,
    backup,
    restore,
    batch,
};

const McpToolSpec = struct {
    kind: McpToolKind,
    name: []const u8,
    description: []const u8,
    permission: McpToolPermission,
    table_argument: ?[]const u8 = null,
    wildcard_table: bool = false,
    fields: []const McpToolFieldSpec = &.{},
};

const McpToolPermission = enum {
    none,
    read,
    write,
    admin,
};

const McpToolFieldType = enum {
    string,
    integer,
    boolean,
    array,
    object,
};

const McpToolFieldSpec = struct {
    name: []const u8,
    schema_type: McpToolFieldType,
    required: bool = false,
    description: ?[]const u8 = null,
    default_json: ?[]const u8 = null,
    items_json: ?[]const u8 = null,
    schema_json: ?[]const u8 = null,
};

const catalog_table_fields = [_]McpToolFieldSpec{
    .{ .name = "database", .schema_type = .string, .description = "Database name. Defaults to the server default database." },
    .{ .name = "namespace", .schema_type = .string, .description = "Namespace name. Defaults to public." },
    .{ .name = "tableName", .schema_type = .string, .required = true },
};

const a2a_catalog_scopes = [_][]const u8{
    "database:*",
    "namespace:*",
    "table:*",
};

const full_text_search_schema_json =
    \\{"oneOf":[{"type":"string"},{"type":"object","additionalProperties":true}],"description":"Full-text query string shorthand, or the generic full_text_search query object accepted by the REST API"}
;

const query_request_schema_json =
    \\{"type":"object","additionalProperties":true,"description":"Raw Antfly QueryRequest body for POST /tables/{tableName}/query. Use this to access the full OpenAPI query contract. Mutually exclusive with query shorthand arguments.","properties":{"query":{"type":"object","additionalProperties":true},"full_text_search":{"type":"object","additionalProperties":true},"filter_query":{"type":"object","additionalProperties":true},"exclusion_query":{"type":"object","additionalProperties":true},"semantic_search":{"type":"string"},"embedding_template":{"type":"string"},"indexes":{"type":"array","items":{"type":"string"}},"embeddings":{"type":"object","additionalProperties":true},"fields":{"type":"array","items":{"type":"string"}},"limit":{"type":"integer"},"offset":{"type":"integer"},"timeout_ms":{"type":"integer","minimum":0},"order_by":{"type":"array"},"search_after":{"type":"array","items":{}},"search_before":{"type":"array","items":{}},"filter_prefix":{"type":"string"},"distance_under":{"type":"number"},"distance_over":{"type":"number"},"search_effort":{"type":"number"},"merge_config":{"type":"object","additionalProperties":true},"count":{"type":"boolean"},"profile":{"type":"boolean"},"reranker":{"type":"object","additionalProperties":true},"aggregations":{"type":"object","additionalProperties":true},"graph_searches":{"type":"object","additionalProperties":true},"expand_strategy":{"type":"string"},"document_renderer":{"type":"string"},"pruner":{"type":"object","additionalProperties":true},"join":{"type":"object","additionalProperties":true},"foreign_sources":{"type":"object","additionalProperties":true}}}
;

const query_request_description_json =
    \\{"openapi_schema":"specs/openapi/antfly/metadata.yaml#/components/schemas/QueryRequest","query_ast_schema":"specs/openapi/antfly/query.yaml#/components/schemas/Query","mcp_usage":{"tool":"query","path_table_argument":"tableName","raw_body_argument":"queryRequest","rules":["queryRequest is forwarded unchanged as the POST /tables/{tableName}/query body.","queryRequest is mutually exclusive with shorthand query arguments such as fullTextSearch, semanticSearch, fields, limit, orderBy, indexes, and filterPrefix.","queryRequest.table is rejected because tableName selects the table-scoped route.","Use describe_query_request for this compact schema instead of relying on tools/list to inline the full recursive OpenAPI schema.","Structured filter_query.geo_bbox accepts field, min_lat, min_lon, max_lat, and max_lon; min_lon greater than max_lon represents an antimeridian-wrapped box."]},"top_level_fields":["query","full_text_search","semantic_search","embedding_template","indexes","filter_prefix","filter_query","exclusion_query","aggregations","embeddings","search_effort","fields","limit","offset","timeout_ms","order_by","search_after","search_before","distance_under","distance_over","merge_config","count","profile","reranker","analyses","graph_searches","expand_strategy","document_renderer","pruner","join","foreign_sources"],"examples":{"fielded_full_text":{"full_text_search":{"match":"hello","field":"body"},"fields":["title","body"],"limit":5,"timeout_ms":5000},"hybrid":{"full_text_search":{"match":"raft","field":"body"},"semantic_search":"raft snapshot architecture","indexes":["body_embedding"],"merge_config":{"strategy":"rrf"},"fields":["title","body"],"limit":20,"profile":true},"filtered":{"query":{"bool":{"must":[{"match":{"field":"body","text":"computer"}}],"filter":[{"term":{"path":"/tenant","value":"acme"}}]}},"fields":["title","url"],"limit":10},"geo_bbox_filter":{"filter_query":{"geo_bbox":{"field":"location","min_lat":-1,"min_lon":179.5,"max_lat":1,"max_lon":-179.5}},"limit":10}}}
;

const mcp_capabilities_description_json =
    \\{"protocol":"mcp","protocol_version":"2025-06-18","transport":"streamable_http","endpoint":"/mcp/v1","sessions":{"initialize_returns_session_id":true,"delete_closes_session":true,"last_event_id_cursor_supported":true,"historical_replay":false},"query_builder":"Use the A2A query-builder skill for agentic natural-language query planning. MCP stays focused on deterministic database tools and raw QueryRequest execution.","tools":{"deterministic":["list_tables","describe_table","list_indexes","describe_indexes","get_document","sample_documents","query","describe_query_request"],"write":["create_table","drop_table","create_index","drop_index","batch","backup","restore"],"schema_helpers":["describe_query_request","describe_mcp_capabilities"]},"query":{"raw_query_request":true,"raw_query_request_argument":"queryRequest","shorthand_arguments":["fullTextSearch","fullTextSearchField","semanticSearch","fields","limit","orderBy","indexes","filterPrefix"],"raw_mode_exclusive":true}}
;

const mcp_tool_specs = [_]McpToolSpec{
    .{
        .kind = .create_table,
        .name = "create_table",
        .description = "Create an Antfly table",
        .permission = .admin,
        .table_argument = "tableName",
        .fields = &.{
            catalog_table_fields[0],
            catalog_table_fields[1],
            .{ .name = "tableName", .schema_type = .string, .required = true },
            // The runtime default is topology-aware: one for standalone and
            // three for distributed deployments.
            .{ .name = "numShards", .schema_type = .integer },
            .{ .name = "key", .schema_type = .string },
            .{ .name = "fields", .schema_type = .string, .description = "JSON object defining field types" },
        },
    },
    .{
        .kind = .drop_table,
        .name = "drop_table",
        .description = "Drop an Antfly table",
        .permission = .admin,
        .table_argument = "tableName",
        .fields = &catalog_table_fields,
    },
    .{
        .kind = .list_tables,
        .name = "list_tables",
        .description = "List Antfly tables",
        .permission = .read,
        .wildcard_table = true,
        .fields = &.{
            catalog_table_fields[0],
            catalog_table_fields[1],
        },
    },
    .{
        .kind = .describe_table,
        .name = "describe_table",
        .description = "Describe an Antfly table, including schema, indexes, ranges, and storage status when available",
        .permission = .read,
        .table_argument = "tableName",
        .fields = &catalog_table_fields,
    },
    .{
        .kind = .create_index,
        .name = "create_index",
        .description = "Create an Antfly index",
        .permission = .admin,
        .table_argument = "tableName",
        .fields = &.{
            catalog_table_fields[0],
            catalog_table_fields[1],
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "indexName", .schema_type = .string, .required = true },
            .{ .name = "field", .schema_type = .string },
            .{ .name = "template", .schema_type = .string },
            .{ .name = "dimension", .schema_type = .integer },
            .{ .name = "embedder", .schema_type = .string },
            .{ .name = "summarizer", .schema_type = .string },
        },
    },
    .{
        .kind = .drop_index,
        .name = "drop_index",
        .description = "Drop an Antfly index",
        .permission = .admin,
        .table_argument = "tableName",
        .fields = &.{
            catalog_table_fields[0],
            catalog_table_fields[1],
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "indexName", .schema_type = .string, .required = true },
        },
    },
    .{
        .kind = .list_indexes,
        .name = "list_indexes",
        .description = "List indexes for an Antfly table",
        .permission = .read,
        .table_argument = "tableName",
        .fields = &catalog_table_fields,
    },
    .{
        .kind = .describe_indexes,
        .name = "describe_indexes",
        .description = "Describe indexes for an Antfly table",
        .permission = .read,
        .table_argument = "tableName",
        .fields = &catalog_table_fields,
    },
    .{
        .kind = .get_document,
        .name = "get_document",
        .description = "Get an Antfly document by key",
        .permission = .read,
        .table_argument = "tableName",
        .fields = &.{
            catalog_table_fields[0],
            catalog_table_fields[1],
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "key", .schema_type = .string, .required = true },
            .{ .name = "fields", .schema_type = .array, .items_json = "{\"type\":\"string\"}" },
        },
    },
    .{
        .kind = .sample_documents,
        .name = "sample_documents",
        .description = "Return a bounded NDJSON sample from a table using the table lookup/scan route",
        .permission = .read,
        .table_argument = "tableName",
        .fields = &.{
            catalog_table_fields[0],
            catalog_table_fields[1],
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "limit", .schema_type = .integer, .default_json = "5" },
            .{ .name = "from", .schema_type = .string },
            .{ .name = "to", .schema_type = .string },
            .{ .name = "inclusiveFrom", .schema_type = .boolean, .description = "Set to true to include the from key." },
            .{ .name = "fields", .schema_type = .array, .items_json = "{\"type\":\"string\"}" },
        },
    },
    .{
        .kind = .query,
        .name = "query",
        .description = "Run an Antfly table query",
        .permission = .read,
        .table_argument = "tableName",
        .fields = &.{
            catalog_table_fields[0],
            catalog_table_fields[1],
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "queryRequest", .schema_type = .object, .schema_json = query_request_schema_json },
            .{ .name = "fullTextSearch", .schema_type = .object, .schema_json = full_text_search_schema_json },
            .{ .name = "full_text_search", .schema_type = .object, .description = "Generic REST-shaped full_text_search query object" },
            .{ .name = "fullTextSearchField", .schema_type = .string, .description = "Field to search when fullTextSearch is a string shorthand, for example content" },
            .{ .name = "semanticSearch", .schema_type = .string },
            .{ .name = "fields", .schema_type = .array, .items_json = "{\"type\":\"string\"}" },
            .{ .name = "limit", .schema_type = .integer, .default_json = "10" },
            .{ .name = "orderBy", .schema_type = .array },
            .{ .name = "indexes", .schema_type = .array, .items_json = "{\"type\":\"string\"}" },
            .{ .name = "filterPrefix", .schema_type = .string },
        },
    },
    .{
        .kind = .describe_query_request,
        .name = "describe_query_request",
        .description = "Return compact guidance for the raw Antfly QueryRequest accepted by query.queryRequest",
        .permission = .none,
    },
    .{
        .kind = .describe_mcp_capabilities,
        .name = "describe_mcp_capabilities",
        .description = "Describe Antfly MCP capabilities, deterministic tools, and A2A query-builder handoff guidance",
        .permission = .none,
    },
    .{
        .kind = .backup,
        .name = "backup",
        .description = "Backup an Antfly table",
        .permission = .admin,
        .table_argument = "tableName",
        .fields = &.{
            catalog_table_fields[0],
            catalog_table_fields[1],
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "backupId", .schema_type = .string, .required = true },
            .{ .name = "location", .schema_type = .string, .required = true },
        },
    },
    .{
        .kind = .restore,
        .name = "restore",
        .description = "Restore an Antfly table",
        .permission = .admin,
        .table_argument = "tableName",
        .fields = &.{
            catalog_table_fields[0],
            catalog_table_fields[1],
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "backupId", .schema_type = .string, .required = true },
            .{ .name = "location", .schema_type = .string, .required = true },
        },
    },
    .{
        .kind = .batch,
        .name = "batch",
        .description = "Insert and delete documents in an Antfly table",
        .permission = .write,
        .table_argument = "tableName",
        .fields = &.{
            catalog_table_fields[0],
            catalog_table_fields[1],
            .{ .name = "tableName", .schema_type = .string, .required = true },
            .{ .name = "writes", .schema_type = .object },
            .{ .name = "deletes", .schema_type = .array, .items_json = "{\"type\":\"string\"}" },
        },
    },
};

const ExtensionMcpTool = struct {
    member: *const extension_domain.ExtensionMember,
    description: []u8,
    input_schema_json: []u8,
    handler: []u8,
    required_capabilities: []extension_domain.Capability = &.{},
    runtime_binding: ?ExtensionRuntimeBinding = null,

    fn deinit(self: ExtensionMcpTool, alloc: std.mem.Allocator) void {
        alloc.free(self.description);
        alloc.free(self.input_schema_json);
        alloc.free(self.handler);
        freeExtensionCapabilities(alloc, self.required_capabilities);
        if (self.runtime_binding) |binding| binding.deinit(alloc);
    }
};

const ExtensionRuntimeBinding = struct {
    package_name: []u8,
    package_version: []u8,
    package_digest: []u8,
    runtime_name: []u8,
    artifact: []u8,
    entrypoint: []u8,

    fn deinit(self: ExtensionRuntimeBinding, alloc: std.mem.Allocator) void {
        alloc.free(self.package_name);
        alloc.free(self.package_version);
        alloc.free(self.package_digest);
        alloc.free(self.runtime_name);
        alloc.free(self.artifact);
        alloc.free(self.entrypoint);
    }

    fn runtime(self: ExtensionRuntimeBinding) wasmtime_runtime.RuntimeBinding {
        return .{
            .package_name = self.package_name,
            .package_version = self.package_version,
            .package_digest = self.package_digest,
            .runtime_name = self.runtime_name,
            .artifact = self.artifact,
            .entrypoint = self.entrypoint,
        };
    }
};

pub fn handleMcpRequest(server_ptr: anytype, req: http_common.HttpRequest, authenticated_identity: anytype) !http_common.HttpResponse {
    return try handleMcpRequestFiltered(server_ptr, req, authenticated_identity, routes.Routes.mcp_v1, null);
}

pub fn handleExtensionMcpRequest(server_ptr: anytype, req: http_common.HttpRequest, authenticated_identity: anytype, extension_name: []const u8) !http_common.HttpResponse {
    return try handleMcpRequestFiltered(server_ptr, req, authenticated_identity, req.uri, extension_name);
}

fn handleMcpRequestFiltered(server_ptr: anytype, req: http_common.HttpRequest, authenticated_identity: anytype, endpoint_path: []const u8, extension_name_filter: ?[]const u8) !http_common.HttpResponse {
    const Server = @TypeOf(server_ptr);
    const ToolContext = struct {
        server: Server,
        authorization: ?[]const u8,
        trusted_principal: ?[]const u8,
        permissions: ?[]const usermgr.Permission,
        spec: McpToolSpec,

        fn handler(ctx: *@This()) mcp.ToolHandler {
            return .{ .ptr = ctx, .call_fn = call };
        }

        fn call(ptr: *anyopaque, alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            if (!try mcpToolInvocationAllowed(alloc, ctx.spec, args, ctx.permissions)) {
                return mcpError(alloc, "permission denied");
            }
            return switch (ctx.spec.kind) {
                .create_table => try ctx.createTable(alloc, args),
                .drop_table => try ctx.dropTable(alloc, args),
                .list_tables => try ctx.listTables(alloc, args),
                .describe_table => try ctx.tableRoute(alloc, args, .GET, "tableName", null, ""),
                .create_index => try ctx.createIndex(alloc, args),
                .drop_index => try ctx.indexRoute(alloc, args, .DELETE, ""),
                .list_indexes => try ctx.listIndexes(alloc, args),
                .describe_indexes => try ctx.listIndexes(alloc, args),
                .get_document => try ctx.getDocument(alloc, args),
                .sample_documents => try ctx.sampleDocuments(alloc, args),
                .query => try ctx.query(alloc, args),
                .describe_query_request => try ctx.describeQueryRequest(alloc),
                .describe_mcp_capabilities => try ctx.describeMcpCapabilities(alloc),
                .backup => try ctx.backupRestore(alloc, args, "backup"),
                .restore => try ctx.backupRestore(alloc, args, "restore"),
                .batch => try ctx.batch(alloc, args),
            };
        }

        fn createTable(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const target = catalogLifecycleTableTargetArg(args) catch |err| return catalogTableArgError(alloc, err);
            var body = std.json.ObjectMap.empty;
            const default_shards: i64 = if (ctx.server.cfg.deployment_mode.isStandalone()) 1 else 3;
            try body.put(alloc, "num_shards", .{ .integer = jsonIntArg(args, "numShards") orelse default_shards });
            if (jsonStringArg(args, "fields")) |fields_json| {
                if (fields_json.len != 0) {
                    const fields = std.json.parseFromSliceLeaky(std.json.Value, alloc, fields_json, .{}) catch return mcpError(alloc, "invalid fields JSON");
                    try body.put(alloc, "schema", fields);
                }
            }
            if (jsonStringArg(args, "key")) |key| {
                if (key.len != 0) try body.put(alloc, "key", .{ .string = key });
            }
            const body_json = try stringifyJsonValue(alloc, .{ .object = body });
            const uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            return try ctx.simpleRoute(alloc, .POST, uri, body_json);
        }

        fn dropTable(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const target = catalogLifecycleTableTargetArg(args) catch |err| return catalogTableArgError(alloc, err);
            const uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            return try ctx.simpleRoute(alloc, .DELETE, uri, "");
        }

        fn listTables(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const target = catalogNamespaceTargetArg(args);
            const uri = try catalogNamespaceTablesRouteAlloc(alloc, target);
            return try ctx.simpleRoute(alloc, .GET, uri, "");
        }

        fn createIndex(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const target = catalogLifecycleTableTargetArg(args) catch |err| return catalogTableArgError(alloc, err);
            const index_name = jsonStringArg(args, "indexName") orelse return mcpError(alloc, "missing indexName");
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "name", .{ .string = index_name });
            try body.put(alloc, "type", .{ .string = "embeddings" });
            if (jsonIntArg(args, "dimension")) |dimension| try body.put(alloc, "dimension", .{ .integer = dimension });
            if (jsonStringArg(args, "field")) |field| if (field.len != 0) try body.put(alloc, "field", .{ .string = field });
            if (jsonStringArg(args, "template")) |template| if (template.len != 0) try body.put(alloc, "template", .{ .string = template });
            if (jsonStringArg(args, "embedder")) |embedder_json| {
                if (embedder_json.len != 0) try body.put(alloc, "embedder", std.json.parseFromSliceLeaky(std.json.Value, alloc, embedder_json, .{}) catch return mcpError(alloc, "invalid embedder JSON"));
            }
            if (jsonStringArg(args, "summarizer")) |summarizer_json| {
                if (summarizer_json.len != 0) try body.put(alloc, "summarizer", std.json.parseFromSliceLeaky(std.json.Value, alloc, summarizer_json, .{}) catch return mcpError(alloc, "invalid summarizer JSON"));
            }
            const table_uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            defer alloc.free(table_uri);
            const uri = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ table_uri, index_name });
            return try ctx.simpleRoute(alloc, .POST, uri, try stringifyJsonValue(alloc, .{ .object = body }));
        }

        fn listIndexes(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const target = catalogLifecycleTableTargetArg(args) catch |err| return catalogTableArgError(alloc, err);
            const table_uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            defer alloc.free(table_uri);
            const uri = try std.fmt.allocPrint(alloc, "{s}/indexes", .{table_uri});
            var result = try ctx.simpleRoute(alloc, .GET, uri, "");
            if (result.structured) |structured| {
                if (structured == .array) {
                    var wrapped = std.json.ObjectMap.empty;
                    try wrapped.put(alloc, "indexes", structured);
                    result.structured = .{ .object = wrapped };
                }
            }
            return result;
        }

        fn getDocument(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const target = catalogLifecycleTableTargetArg(args) catch |err| return catalogTableArgError(alloc, err);
            const key = jsonStringArg(args, "key") orelse return mcpError(alloc, "missing key");

            var uri = std.ArrayListUnmanaged(u8).empty;
            errdefer uri.deinit(alloc);
            const table_uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            defer alloc.free(table_uri);
            try uri.appendSlice(alloc, table_uri);
            try uri.appendSlice(alloc, routes.Routes.documents_marker);
            try appendUriPathSegment(alloc, &uri, key);

            if (jsonValueArg(args, "fields")) |fields| {
                if (fields != .array) return mcpError(alloc, "fields must be an array");
                if (fields.array.items.len > 0) {
                    try uri.appendSlice(alloc, "?fields=");
                    for (fields.array.items, 0..) |field, i| {
                        if (field != .string) return mcpError(alloc, "fields must contain strings");
                        if (i != 0) try uri.append(alloc, ',');
                        try uri.appendSlice(alloc, field.string);
                    }
                }
            }

            return try ctx.simpleRoute(alloc, .GET, try uri.toOwnedSlice(alloc), "");
        }

        fn sampleDocuments(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const target = catalogLifecycleTableTargetArg(args) catch |err| return catalogTableArgError(alloc, err);
            const limit = jsonIntArg(args, "limit") orelse 5;
            if (limit <= 0) return mcpError(alloc, "limit must be greater than 0");
            if (limit > max_mcp_sample_documents_limit) return mcpError(alloc, "limit exceeds maximum sample size");
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "limit", .{ .integer = limit });
            if (jsonStringArg(args, "from")) |from| if (from.len != 0) try body.put(alloc, "from", .{ .string = from });
            if (jsonStringArg(args, "to")) |to| if (to.len != 0) try body.put(alloc, "to", .{ .string = to });
            if (jsonBoolArg(args, "inclusiveFrom")) |inclusive| try body.put(alloc, "inclusive_from", .{ .bool = inclusive });
            if (jsonValueArg(args, "fields")) |fields| try body.put(alloc, "fields", fields);
            const table_uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            defer alloc.free(table_uri);
            const uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ table_uri, routes.Routes.documents_suffix });
            return try ctx.simpleRoute(alloc, .POST, uri, try stringifyJsonValue(alloc, .{ .object = body }));
        }

        fn indexRoute(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value, method: http_common.Method, body: []const u8) !mcp.CallToolResult {
            const target = catalogLifecycleTableTargetArg(args) catch |err| return catalogTableArgError(alloc, err);
            const index_name = jsonStringArg(args, "indexName") orelse return mcpError(alloc, "missing indexName");
            const table_uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            defer alloc.free(table_uri);
            const uri = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ table_uri, index_name });
            return try ctx.simpleRoute(alloc, method, uri, body);
        }

        fn query(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const target = catalogLifecycleTableTargetArg(args) catch |err| return catalogTableArgError(alloc, err);
            if (jsonValueArg(args, "queryRequest")) |query_request| {
                if (query_request != .object) return mcpError(alloc, "queryRequest must be an object");
                if (hasNonRawQueryArg(args)) return mcpError(alloc, "queryRequest cannot be combined with shorthand query arguments");
                if (query_request.object.get("table") != null) return mcpError(alloc, "queryRequest.table is not allowed; use tableName");

                const table_uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
                defer alloc.free(table_uri);
                const uri = try std.fmt.allocPrint(alloc, "{s}/query", .{table_uri});
                return try ctx.simpleRoute(alloc, .POST, uri, try stringifyJsonValue(alloc, query_request));
            }

            var body = std.json.ObjectMap.empty;
            putFullTextSearchArg(alloc, &body, args) catch return mcpError(alloc, "invalid fullTextSearch");
            if (jsonStringArg(args, "semanticSearch")) |semantic| if (semantic.len != 0) try body.put(alloc, "semantic_search", .{ .string = semantic });
            if (jsonValueArg(args, "fields")) |fields| try body.put(alloc, "fields", fields);
            if (jsonValueArg(args, "orderBy")) |order_by| try body.put(alloc, "order_by", order_by);
            if (jsonValueArg(args, "indexes")) |indexes| try body.put(alloc, "indexes", indexes);
            if (jsonStringArg(args, "filterPrefix")) |prefix| if (prefix.len != 0) try body.put(alloc, "filter_prefix", .{ .string = prefix });
            try body.put(alloc, "limit", .{ .integer = jsonIntArg(args, "limit") orelse 10 });
            const table_uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            defer alloc.free(table_uri);
            const uri = try std.fmt.allocPrint(alloc, "{s}/query", .{table_uri});
            return try ctx.simpleRoute(alloc, .POST, uri, try stringifyJsonValue(alloc, .{ .object = body }));
        }

        fn describeQueryRequest(_: *@This(), alloc: std.mem.Allocator) !mcp.CallToolResult {
            const structured = try std.json.parseFromSliceLeaky(std.json.Value, alloc, query_request_description_json, .{});
            return .{
                .text = try alloc.dupe(u8, "Antfly QueryRequest schema summary for query.queryRequest"),
                .structured = structured,
            };
        }

        fn describeMcpCapabilities(_: *@This(), alloc: std.mem.Allocator) !mcp.CallToolResult {
            const structured = try std.json.parseFromSliceLeaky(std.json.Value, alloc, mcp_capabilities_description_json, .{});
            return .{
                .text = try alloc.dupe(u8, "Antfly MCP capabilities"),
                .structured = structured,
            };
        }

        fn backupRestore(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value, operation: []const u8) !mcp.CallToolResult {
            const target = catalogLifecycleTableTargetArg(args) catch |err| return catalogTableArgError(alloc, err);
            const backup_id = jsonStringArg(args, "backupId") orelse return mcpError(alloc, "missing backupId");
            const location = jsonStringArg(args, "location") orelse return mcpError(alloc, "missing location");
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "backup_id", .{ .string = backup_id });
            try body.put(alloc, "location", .{ .string = location });
            const table_uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            defer alloc.free(table_uri);
            const uri = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ table_uri, operation });
            return try ctx.simpleRoute(alloc, .POST, uri, try stringifyJsonValue(alloc, .{ .object = body }));
        }

        fn batch(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const target = catalogLifecycleTableTargetArg(args) catch |err| return catalogTableArgError(alloc, err);
            var body = std.json.ObjectMap.empty;
            if (jsonValueArg(args, "writes")) |writes| try body.put(alloc, "inserts", writes);
            if (jsonValueArg(args, "deletes")) |deletes| try body.put(alloc, "deletes", deletes);
            const table_uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            defer alloc.free(table_uri);
            const uri = try std.fmt.allocPrint(alloc, "{s}/batch", .{table_uri});
            return try ctx.simpleRoute(alloc, .POST, uri, try stringifyJsonValue(alloc, .{ .object = body }));
        }

        fn tableRoute(ctx: *@This(), alloc: std.mem.Allocator, args: std.json.Value, method: http_common.Method, table_arg: []const u8, suffix: ?[]const u8, body: []const u8) !mcp.CallToolResult {
            _ = table_arg;
            const target = catalogLifecycleTableTargetArg(args) catch |err| return catalogTableArgError(alloc, err);
            const table_uri = try catalogTableRouteAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
            defer alloc.free(table_uri);
            const uri = if (suffix) |route_suffix|
                try std.fmt.allocPrint(alloc, "{s}/{s}", .{ table_uri, route_suffix })
            else
                try alloc.dupe(u8, table_uri);
            return try ctx.simpleRoute(alloc, method, uri, body);
        }

        fn simpleRoute(ctx: *@This(), alloc: std.mem.Allocator, method: http_common.Method, uri: []const u8, body: []const u8) !mcp.CallToolResult {
            const headers: []const http_common.RequestHeader = if (ctx.trusted_principal) |trusted_principal|
                &[_]http_common.RequestHeader{.{ .name = trusted_principal_header, .value = trusted_principal }}
            else
                &.{};
            var resp = try ctx.server.handle(.{
                .method = method,
                .uri = uri,
                .headers = headers,
                .authorization = ctx.authorization,
                .content_type = if (body.len == 0) null else "application/json",
                .body = body,
            });
            defer resp.deinit(ctx.server.alloc);
            return try mcpResultFromHttpResponse(alloc, resp);
        }
    };
    const ExtensionToolContext = struct {
        server: Server,
        authorization: ?[]const u8,
        trusted_principal: ?[]const u8,
        authenticated_identity: ?DelegatedIdentity,
        installed: *const extension_domain.InstalledExtension,
        tool: *const ExtensionMcpTool,

        fn handler(ctx: *@This()) mcp.ToolHandler {
            return .{ .ptr = ctx, .call_fn = call };
        }

        fn call(ptr: *anyopaque, alloc: std.mem.Allocator, args: std.json.Value) !mcp.CallToolResult {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            if (ctx.authenticated_identity) |identity| {
                if (!extensionMcpToolAllowedForPermissions(ctx.installed.*, ctx.tool.*, identity.permissions)) {
                    return mcpError(alloc, "permission denied");
                }
            }
            return try callExtensionMcpTool(alloc, ctx.server, ctx.authorization, ctx.trusted_principal, ctx.authenticated_identity, ctx.installed, ctx.tool.*, args);
        }
    };

    var contexts: [mcp_tool_specs.len]ToolContext = undefined;
    for (&contexts, mcp_tool_specs) |*ctx, spec| {
        ctx.* = .{
            .server = server_ptr,
            .authorization = req.authorization,
            .trusted_principal = req.header(trusted_principal_header),
            .permissions = if (authenticated_identity) |identity| identity.permissions else null,
            .spec = spec,
        };
    }

    var snapshot_opt = try server_ptr.source.adminSnapshot();
    defer if (snapshot_opt) |*snapshot| server_ptr.source.freeAdminSnapshot(snapshot);

    var extension_tools = std.ArrayListUnmanaged(ExtensionMcpTool).empty;
    defer {
        for (extension_tools.items) |tool| tool.deinit(server_ptr.alloc);
        extension_tools.deinit(server_ptr.alloc);
    }
    if (snapshot_opt) |*snapshot| {
        if (extension_name_filter) |extension_name| {
            if (!extensionRuntimeMemberVisible(snapshot.installed_extensions, extension_name)) {
                return try textResponse(server_ptr.alloc, 404, "extension not found");
            }
        }
        for (snapshot.extension_members) |*member| {
            if (member.object_kind != .mcp_tool) continue;
            if (extension_name_filter) |extension_name| {
                if (!std.mem.eql(u8, member.extension_name, extension_name)) continue;
            }
            if (!extensionRuntimeMemberVisible(snapshot.installed_extensions, member.extension_name)) continue;
            try extension_tools.append(server_ptr.alloc, try extensionMcpToolFromMemberAlloc(server_ptr.alloc, member, snapshot));
        }
    } else if (extension_name_filter != null) {
        return try textResponse(server_ptr.alloc, 404, "extension not found");
    }
    const extension_contexts = try server_ptr.alloc.alloc(ExtensionToolContext, extension_tools.items.len);
    defer if (extension_contexts.len > 0) server_ptr.alloc.free(extension_contexts);
    for (extension_contexts, 0..) |*ctx, i| {
        const installed = findInstalledExtensionForRuntimeTool(snapshot_opt.?.installed_extensions, extension_tools.items[i].member.extension_name) orelse return try textResponse(server_ptr.alloc, 404, "extension not found");
        ctx.* = .{
            .server = server_ptr,
            .authorization = req.authorization,
            .trusted_principal = req.header(trusted_principal_header),
            .authenticated_identity = if (authenticated_identity) |identity| .{
                .username = identity.username,
                .permissions = identity.permissions,
                .row_filter = identity.row_filter,
                .metadata_json = identity.metadata_json,
                .roles = identity.roles,
                .role_settings = identity.role_settings,
                .role_runtime_settings = identity.role_runtime_settings,
            } else null,
            .installed = installed,
            .tool = &extension_tools.items[i],
        };
    }

    var input_schemas: [mcp_tool_specs.len][]u8 = undefined;
    var input_schema_count: usize = 0;
    defer {
        for (input_schemas[0..input_schema_count]) |schema| server_ptr.alloc.free(schema);
    }
    for (mcp_tool_specs, 0..) |spec, i| {
        input_schemas[i] = try buildMcpInputSchema(server_ptr.alloc, spec);
        input_schema_count += 1;
    }

    var protocol_server = mcp.Server{
        .implementation = .{ .name = "antfly", .version = "1.0.0" },
        .session_store = server_ptr.mcp_sessions.iface(),
    };
    defer protocol_server.deinit(server_ptr.alloc);
    if (extension_name_filter == null) {
        for (&contexts, mcp_tool_specs, 0..) |*ctx, spec, i| {
            if (!mcpToolVisibleForIdentity(spec, authenticated_identity)) continue;
            try protocol_server.addTool(server_ptr.alloc, .{
                .name = spec.name,
                .description = spec.description,
                .input_schema_json = input_schemas[i],
                .handler = ctx.handler(),
            });
        }
    }
    for (extension_tools.items, extension_contexts) |tool, *ctx| {
        if (!extensionMcpToolVisibleForIdentity(ctx.installed, &tool, authenticated_identity)) continue;
        try protocol_server.addTool(server_ptr.alloc, .{
            .name = tool.member.object_name,
            .description = tool.description,
            .input_schema_json = tool.input_schema_json,
            .handler = ctx.handler(),
        });
    }

    var transport = switch (req.method) {
        .GET => protocol_server.handleStreamableHttpGetWithSession(
            server_ptr.alloc,
            endpoint_path,
            req.header(mcp.session_id_header),
            req.header(mcp.last_event_id_header),
        ) catch |err| switch (err) {
            error.InvalidLastEventId => return try textResponse(server_ptr.alloc, 400, "invalid Last-Event-ID"),
            error.McpEventIdExhausted => return try textResponse(server_ptr.alloc, 409, "MCP event sequence exhausted"),
            else => return err,
        },
        .POST => protocol_server.handleStreamableHttpPostWithSession(
            server_ptr.alloc,
            req.body,
            req.header(mcp.session_id_header),
        ) catch |err| switch (err) {
            error.McpSessionCapacityExceeded => return try textResponse(server_ptr.alloc, 429, "MCP session capacity exceeded"),
            else => return err,
        },
        .DELETE => try protocol_server.handleStreamableHttpDelete(server_ptr.alloc, req.header(mcp.session_id_header)),
        else => return try textResponse(server_ptr.alloc, 405, "method not allowed"),
    };
    defer transport.deinit(server_ptr.alloc);
    return try mcpBodyResponseWithStatus(server_ptr.alloc, transport);
}

fn mcpToolVisibleForIdentity(spec: McpToolSpec, authenticated_identity: anytype) bool {
    const identity = authenticated_identity orelse return true;
    if (spec.kind == .list_tables) return identityCanListTables(identity.permissions);
    const required = permissionTypeForMcpTool(spec.permission) orelse return true;
    if (spec.wildcard_table) {
        return identityHasPermission(identity.permissions, .table, "*", required);
    }
    return identityHasAnyPermission(identity.permissions, .table, required);
}

/// Discovery filtering is useful UX, but it is not an authorization boundary:
/// a client can retain an old tool schema or hand-craft tools/call. Enforce the
/// effective table permission again immediately before dispatch so denied MCP
/// calls cannot reach an extension, router, or metadata mutation path.
fn mcpToolInvocationAllowed(
    alloc: std.mem.Allocator,
    spec: McpToolSpec,
    args: std.json.Value,
    permissions: ?[]const usermgr.Permission,
) !bool {
    const effective_permissions = permissions orelse return true;
    const required = permissionTypeForMcpTool(spec.permission) orelse return true;
    if (spec.kind == .list_tables) {
        if (identityHasPermission(effective_permissions, .table, "*", required)) return true;
        const target = catalogNamespaceTargetArg(args);
        const resource = try catalog_resources.namespaceResourceNameAlloc(
            alloc,
            target.database_name,
            target.namespace_name,
        );
        defer alloc.free(resource);
        return identityHasPermission(effective_permissions, .namespace, resource, required);
    }
    if (spec.wildcard_table) {
        return identityHasPermission(effective_permissions, .table, "*", required);
    }
    const table_argument = spec.table_argument orelse return false;
    // Malformed or missing resource identity must fail closed here. The tool
    // handler will still return its normal validation error to callers that
    // are permitted to invoke it, but an unscoped call must never cross the
    // authorization boundary.
    if (jsonStringArg(args, table_argument) == null) return false;
    const target = catalogLifecycleTableTargetArg(args) catch return false;
    const resource = try catalog_resources.tableResourceNameAlloc(
        alloc,
        target.database_name,
        target.namespace_name,
        target.table_name,
    );
    defer alloc.free(resource);
    return identityHasPermission(effective_permissions, .table, resource, required);
}

fn permissionTypeForMcpTool(permission: McpToolPermission) ?usermgr.PermissionType {
    return switch (permission) {
        .none => null,
        .read => .read,
        .write => .write,
        .admin => .admin,
    };
}

const CatalogToolArgError = error{
    MissingTableName,
    ExplicitCatalogTableRouteUnsupported,
};

fn catalogTableNameArg(args: std.json.Value) CatalogToolArgError![]const u8 {
    const table_name = jsonStringArg(args, "tableName") orelse return error.MissingTableName;
    try ensureDefaultCatalogArgs(args);
    return table_name;
}

fn catalogLifecycleTableTargetArg(args: std.json.Value) CatalogToolArgError!catalog_resources.TableTarget {
    const table_name = jsonStringArg(args, "tableName") orelse return error.MissingTableName;
    return catalog_resources.tableTargetFromOptional(jsonStringArg(args, "database"), jsonStringArg(args, "namespace"), table_name) catch return error.ExplicitCatalogTableRouteUnsupported;
}

fn catalogNamespaceTargetArg(args: std.json.Value) catalog_resources.NamespaceTarget {
    return catalog_resources.namespaceTargetFromOptional(jsonStringArg(args, "database"), jsonStringArg(args, "namespace"));
}

fn catalogNamespaceTablesRouteAlloc(alloc: std.mem.Allocator, target: catalog_resources.NamespaceTarget) ![]u8 {
    if (catalog_resources.namespaceIsDefaultPublic(target)) {
        return try alloc.dupe(u8, routes.Routes.tables);
    }
    var uri = std.ArrayListUnmanaged(u8).empty;
    errdefer uri.deinit(alloc);
    try uri.appendSlice(alloc, routes.Routes.databases);
    try uri.append(alloc, '/');
    try appendUriPathSegment(alloc, &uri, target.database_name);
    try uri.appendSlice(alloc, "/namespaces/");
    try appendUriPathSegment(alloc, &uri, target.namespace_name);
    try uri.appendSlice(alloc, "/tables");
    return try uri.toOwnedSlice(alloc);
}

fn catalogTableRouteAlloc(
    alloc: std.mem.Allocator,
    database_name: []const u8,
    namespace_name: []const u8,
    table_name: []const u8,
) ![]u8 {
    const target = try catalog_resources.tableTargetFromOptional(database_name, namespace_name, table_name);
    if (catalog_resources.tableIsDefaultPublic(target)) {
        var uri = std.ArrayListUnmanaged(u8).empty;
        errdefer uri.deinit(alloc);
        try uri.appendSlice(alloc, routes.Routes.tables);
        try uri.append(alloc, '/');
        try appendUriPathSegment(alloc, &uri, target.table_name);
        return try uri.toOwnedSlice(alloc);
    }
    var uri = std.ArrayListUnmanaged(u8).empty;
    errdefer uri.deinit(alloc);
    try uri.appendSlice(alloc, routes.Routes.databases);
    try uri.append(alloc, '/');
    try appendUriPathSegment(alloc, &uri, target.database_name);
    try uri.appendSlice(alloc, "/namespaces/");
    try appendUriPathSegment(alloc, &uri, target.namespace_name);
    try uri.appendSlice(alloc, "/tables");
    try uri.append(alloc, '/');
    try appendUriPathSegment(alloc, &uri, target.table_name);
    return try uri.toOwnedSlice(alloc);
}

fn ensureDefaultCatalogArgs(args: std.json.Value) CatalogToolArgError!void {
    const target = catalogNamespaceTargetArg(args);
    if (!catalog_resources.namespaceIsDefaultPublic(target)) {
        return error.ExplicitCatalogTableRouteUnsupported;
    }
}

fn ensureDefaultCatalogQueryTargets(args: std.json.Value) CatalogToolArgError!void {
    try ensureDefaultCatalogArgs(args);
    if (args != .object) return;
    const queries = args.object.get("queries") orelse return;
    if (queries != .array) return;
    for (queries.array.items) |query| {
        try ensureDefaultCatalogArgs(query);
    }
}

fn a2aCatalogTableNameFromObjectAlloc(alloc: std.mem.Allocator, object: anytype) !?[]const u8 {
    const table_name = jsonStringObjectField(object, "table") orelse return null;
    const database_name = jsonStringObjectField(object, "database");
    const namespace_name = jsonStringObjectField(object, "namespace");
    if (database_name == null and namespace_name == null) return null;
    const target = catalog_resources.tableTargetFromOptional(
        database_name,
        namespace_name,
        table_name,
    ) catch return error.InvalidA2aCatalogTarget;
    return try catalog_resources.tableResourceNameAlloc(alloc, target.database_name, target.namespace_name, target.table_name);
}

fn a2aNormalizedQueriesAlloc(alloc: std.mem.Allocator, queries: std.json.Value) !std.json.Value {
    if (queries != .array) return queries;
    var out = std.json.Array.init(alloc);
    for (queries.array.items) |query| {
        var normalized = query;
        if (normalized == .object) {
            if (try a2aCatalogTableNameFromObjectAlloc(alloc, normalized.object)) |catalog_table_name| {
                try normalized.object.put(alloc, "table", .{ .string = catalog_table_name });
                _ = normalized.object.swapRemove("database");
                _ = normalized.object.swapRemove("namespace");
            }
        }
        try out.append(normalized);
    }
    return .{ .array = out };
}

fn catalogTableArgError(alloc: std.mem.Allocator, err: CatalogToolArgError) !mcp.CallToolResult {
    return switch (err) {
        error.MissingTableName => mcpError(alloc, "missing tableName"),
        error.ExplicitCatalogTableRouteUnsupported => mcpError(alloc, "invalid explicit database/namespace table target"),
    };
}

fn identityCanListTables(permissions: []const usermgr.Permission) bool {
    return identityHasPermission(permissions, .table, "*", .read) or
        identityHasAnyPermission(permissions, .namespace, .read);
}

fn extensionMcpToolVisibleForIdentity(installed: *const extension_domain.InstalledExtension, tool: *const ExtensionMcpTool, authenticated_identity: anytype) bool {
    const identity = authenticated_identity orelse return true;
    return extensionMcpToolAllowedForPermissions(installed.*, tool.*, identity.permissions);
}

fn extensionMcpToolAllowedForPermissions(
    installed: extension_domain.InstalledExtension,
    tool: ExtensionMcpTool,
    permissions: []const usermgr.Permission,
) bool {
    if (tool.required_capabilities.len == 0) {
        return extensionScopeVisibleForIdentity(installed, tool.member.*, permissions);
    }

    var checked_table_permission = false;
    for (tool.required_capabilities) |capability| {
        const permission_type = permissionTypeForExtensionCapability(capability.name) orelse continue;
        checked_table_permission = true;
        const table = tableResourceForExtensionCapability(installed, tool.member.*, capability);
        if (!identityHasPermission(permissions, .table, table, permission_type)) return false;
    }
    return checked_table_permission or extensionScopeVisibleForIdentity(installed, tool.member.*, permissions);
}

fn extensionScopeVisibleForIdentity(installed: extension_domain.InstalledExtension, member: extension_domain.ExtensionMember, permissions: []const usermgr.Permission) bool {
    if (member.scope.kind == .table) {
        return identityHasPermission(permissions, .table, member.scope.table_name, .read);
    }
    if (installed.scope.kind == .table) {
        return identityHasPermission(permissions, .table, installed.scope.table_name, .read);
    }
    return identityHasPermission(permissions, .@"*", "*", .admin);
}

fn permissionTypeForExtensionCapability(name: []const u8) ?usermgr.PermissionType {
    if (std.mem.eql(u8, name, "db:read") or std.mem.eql(u8, name, "read:table")) return .read;
    if (std.mem.eql(u8, name, "db:write") or std.mem.eql(u8, name, "write:table")) return .write;
    if (std.mem.eql(u8, name, "db:admin") or std.mem.eql(u8, name, "admin:table")) return .admin;
    return null;
}

fn tableResourceForExtensionCapability(installed: extension_domain.InstalledExtension, member: extension_domain.ExtensionMember, capability: extension_domain.Capability) []const u8 {
    if (capability.scope.len != 0 and !std.mem.eql(u8, capability.scope, installed.package_name)) return capability.scope;
    if (member.scope.kind == .table) return member.scope.table_name;
    if (installed.scope.kind == .table) return installed.scope.table_name;
    return "*";
}

fn identityHasAnyPermission(permissions: []const usermgr.Permission, resource_type: usermgr.ResourceType, permission_type: usermgr.PermissionType) bool {
    for (permissions) |permission| {
        const type_match = permission.resource_type == .@"*" or permission.resource_type == resource_type;
        if (!type_match) continue;
        if (permission.type == .admin or permission.type == permission_type) return true;
    }
    return false;
}

fn identityHasPermission(
    permissions: []const usermgr.Permission,
    resource_type: usermgr.ResourceType,
    resource: []const u8,
    permission_type: usermgr.PermissionType,
) bool {
    for (permissions) |permission| {
        const type_match = permission.resource_type == .@"*" or permission.resource_type == resource_type;
        const resource_match = std.mem.eql(u8, permission.resource, "*") or
            (if (resource_type == .table)
                catalog_resources.tableResourceMatches(permission.resource, resource)
            else
                std.mem.eql(u8, permission.resource, resource));
        if (!type_match or !resource_match) continue;
        if (permission.type == .admin or permission.type == permission_type) return true;
    }
    return false;
}

fn extensionMcpToolFromMemberAlloc(alloc: std.mem.Allocator, member: *const extension_domain.ExtensionMember, snapshot: anytype) !ExtensionMcpTool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, member.owner_metadata_json, .{}) catch null;
    defer if (parsed) |*value| value.deinit();

    var description: ?[]u8 = null;
    errdefer if (description) |value| alloc.free(value);
    var input_schema_json: ?[]u8 = null;
    errdefer if (input_schema_json) |value| alloc.free(value);
    var handler: ?[]u8 = null;
    errdefer if (handler) |value| alloc.free(value);
    var required_capabilities: []extension_domain.Capability = &.{};
    errdefer freeExtensionCapabilities(alloc, required_capabilities);

    if (parsed) |config| {
        if (config.value == .object) {
            if (jsonStringObjectField(config.value.object, "description")) |value| {
                description = try alloc.dupe(u8, value);
            }
            if (config.value.object.get("input_schema")) |schema| {
                if (schema == .object) input_schema_json = try stringifyJsonValue(alloc, schema);
            }
            if (input_schema_json == null) {
                if (jsonStringObjectField(config.value.object, "input_schema_json")) |schema_json| {
                    if (schema_json.len != 0) input_schema_json = try cloneValidMcpInputSchemaJson(alloc, schema_json);
                }
            }
            if (jsonStringObjectField(config.value.object, "handler")) |value| {
                handler = try alloc.dupe(u8, value);
            }
            if (config.value.object.get("required_capabilities")) |value| {
                required_capabilities = try parseExtensionCapabilitiesAlloc(alloc, value);
            }
        }
    }

    if (description == null) {
        description = try std.fmt.allocPrint(alloc, "Extension MCP tool {s}.{s}", .{ member.extension_name, member.object_name });
    }
    if (input_schema_json == null) {
        input_schema_json = try alloc.dupe(u8, "{\"type\":\"object\"}");
    }
    if (handler == null) {
        handler = try alloc.dupe(u8, "");
    }

    return .{
        .member = member,
        .description = description.?,
        .input_schema_json = input_schema_json.?,
        .handler = handler.?,
        .required_capabilities = required_capabilities,
        .runtime_binding = try extensionRuntimeBindingAlloc(alloc, member, handler.?, snapshot),
    };
}

fn parseExtensionCapabilitiesAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]extension_domain.Capability {
    if (value != .array) return try alloc.alloc(extension_domain.Capability, 0);
    var out = std.ArrayListUnmanaged(extension_domain.Capability).empty;
    errdefer {
        freeExtensionCapabilityValues(alloc, out.items);
        out.deinit(alloc);
    }
    for (value.array.items) |item| {
        switch (item) {
            .string => |name| {
                const owned_name = try alloc.dupe(u8, name);
                errdefer alloc.free(owned_name);
                try out.append(alloc, .{
                    .name = owned_name,
                    .scope = "",
                });
            },
            .object => |object| {
                const name = jsonStringObjectField(object, "name") orelse continue;
                const scope = jsonStringObjectField(object, "scope") orelse "";
                const owned_name = try alloc.dupe(u8, name);
                errdefer alloc.free(owned_name);
                const owned_scope = if (scope.len == 0) "" else try alloc.dupe(u8, scope);
                errdefer if (owned_scope.len > 0) alloc.free(owned_scope);
                try out.append(alloc, .{
                    .name = owned_name,
                    .scope = owned_scope,
                });
            },
            else => {},
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn freeExtensionCapabilityValues(alloc: std.mem.Allocator, capabilities: []const extension_domain.Capability) void {
    for (capabilities) |capability| {
        alloc.free(capability.name);
        if (capability.scope.len > 0) alloc.free(capability.scope);
    }
}

fn freeExtensionCapabilities(alloc: std.mem.Allocator, capabilities: []const extension_domain.Capability) void {
    freeExtensionCapabilityValues(alloc, capabilities);
    if (capabilities.len > 0) alloc.free(@constCast(capabilities));
}

fn extensionRuntimeBindingAlloc(alloc: std.mem.Allocator, member: *const extension_domain.ExtensionMember, handler: []const u8, snapshot: anytype) !?ExtensionRuntimeBinding {
    const parsed = parseWasmHandler(handler) orelse return null;
    const installed = findInstalledExtension(snapshot.installed_extensions, member.extension_name) orelse return null;
    const package = findExtensionPackage(snapshot.extension_packages, installed.package_name, installed.package_version, installed.package_digest) orelse return null;
    const runtime = findWasmRuntime(package.install.runtimes, parsed.runtime_name) orelse return null;

    var package_name: ?[]u8 = null;
    errdefer if (package_name) |value| alloc.free(value);
    var package_version: ?[]u8 = null;
    errdefer if (package_version) |value| alloc.free(value);
    var package_digest: ?[]u8 = null;
    errdefer if (package_digest) |value| alloc.free(value);
    var runtime_name: ?[]u8 = null;
    errdefer if (runtime_name) |value| alloc.free(value);
    var artifact: ?[]u8 = null;
    errdefer if (artifact) |value| alloc.free(value);
    var entrypoint: ?[]u8 = null;
    errdefer if (entrypoint) |value| alloc.free(value);

    package_name = try alloc.dupe(u8, installed.package_name);
    package_version = try alloc.dupe(u8, installed.package_version);
    package_digest = try alloc.dupe(u8, installed.package_digest);
    runtime_name = try alloc.dupe(u8, parsed.runtime_name);
    artifact = try alloc.dupe(u8, runtime.artifact);
    entrypoint = try runtimeEntrypointAlloc(alloc, runtime.config_json);

    return .{
        .package_name = package_name.?,
        .package_version = package_version.?,
        .package_digest = package_digest.?,
        .runtime_name = runtime_name.?,
        .artifact = artifact.?,
        .entrypoint = entrypoint.?,
    };
}

fn runtimeEntrypointAlloc(alloc: std.mem.Allocator, config_json: []const u8) ![]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, config_json, .{}) catch return try alloc.dupe(u8, "call-tool");
    defer parsed.deinit();
    if (parsed.value != .object) return try alloc.dupe(u8, "call-tool");
    return try alloc.dupe(u8, jsonStringObjectField(parsed.value.object, "entrypoint") orelse "call-tool");
}

const WasmHandler = struct {
    runtime_name: []const u8,
    tool_name: []const u8,
};

fn parseWasmHandler(handler: []const u8) ?WasmHandler {
    if (!std.mem.startsWith(u8, handler, "wasm:")) return null;
    const rest = handler["wasm:".len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return null;
    if (slash == 0 or slash + 1 >= rest.len) return null;
    return .{ .runtime_name = rest[0..slash], .tool_name = rest[slash + 1 ..] };
}

fn findInstalledExtension(installed_extensions: []const extension_domain.InstalledExtension, name: []const u8) ?extension_domain.InstalledExtension {
    for (installed_extensions) |installed| {
        if (std.mem.eql(u8, installed.name, name)) return installed;
    }
    return null;
}

fn findExtensionPackage(packages: []const extension_domain.PackageManifest, name: []const u8, version: []const u8, digest: []const u8) ?extension_domain.PackageManifest {
    for (packages) |package| {
        if (std.mem.eql(u8, package.name, name) and
            std.mem.eql(u8, package.version, version) and
            std.mem.eql(u8, package.digest, digest))
        {
            return package;
        }
    }
    return null;
}

pub fn testExtensionRuntimeBindingRequiresInstalledPackageDigestMatch() !void {
    if (!@import("builtin").is_test) @compileError("test-only helper");
    const alloc = std.testing.allocator;
    const runtimes = [_]extension_domain.RuntimeDecl{.{
        .name = "memoryaf_wasm",
        .mode = .wasm,
        .artifact = "target/wasm32-wasip2/release/memoryaf_extension.wasm",
    }};
    const packages = [_]extension_domain.PackageManifest{.{
        .name = "memoryaf",
        .version = "0.0.1",
        .digest = "sha256:projected",
        .install = .{
            .scopes_supported = &.{.cluster},
            .runtimes = &runtimes,
        },
    }};
    const installed = [_]extension_domain.InstalledExtension{.{
        .name = "memories",
        .package_name = "memoryaf",
        .package_version = "0.0.1",
        .package_digest = "sha256:installed",
        .scope = .{ .kind = .cluster },
        .status = .ready,
    }};
    const snapshot = .{
        .extension_packages = packages[0..],
        .installed_extensions = installed[0..],
    };
    const member = extension_domain.ExtensionMember{
        .extension_name = "memories",
        .scope = .{ .kind = .cluster },
        .object_kind = .mcp_tool,
        .object_name = "store_memory",
    };

    try std.testing.expectEqual(@as(?ExtensionRuntimeBinding, null), try extensionRuntimeBindingAlloc(alloc, &member, "wasm:memoryaf_wasm/store_memory", snapshot));
}

test "extension runtime binding requires installed package digest to match projected package" {
    try testExtensionRuntimeBindingRequiresInstalledPackageDigestMatch();
}

pub fn testExtensionRuntimeBindingCarriesMatchedInstalledPackageDigest() !void {
    if (!@import("builtin").is_test) @compileError("test-only helper");
    const alloc = std.testing.allocator;
    const runtimes = [_]extension_domain.RuntimeDecl{.{
        .name = "memoryaf_wasm",
        .mode = .wasm,
        .artifact = "target/wasm32-wasip2/release/memoryaf_extension.wasm",
        .config_json = "{\"entrypoint\":\"run-tool\"}",
    }};
    const packages = [_]extension_domain.PackageManifest{.{
        .name = "memoryaf",
        .version = "0.0.1",
        .digest = "sha256:projected",
        .install = .{
            .scopes_supported = &.{.cluster},
            .runtimes = &runtimes,
        },
    }};
    const installed = [_]extension_domain.InstalledExtension{.{
        .name = "memories",
        .package_name = "memoryaf",
        .package_version = "0.0.1",
        .package_digest = "sha256:projected",
        .scope = .{ .kind = .cluster },
        .status = .ready,
    }};
    const snapshot = .{
        .extension_packages = packages[0..],
        .installed_extensions = installed[0..],
    };
    const member = extension_domain.ExtensionMember{
        .extension_name = "memories",
        .scope = .{ .kind = .cluster },
        .object_kind = .mcp_tool,
        .object_name = "store_memory",
    };

    const binding = (try extensionRuntimeBindingAlloc(alloc, &member, "wasm:memoryaf_wasm/store_memory", snapshot)).?;
    defer binding.deinit(alloc);

    try std.testing.expectEqualStrings("memoryaf", binding.package_name);
    try std.testing.expectEqualStrings("0.0.1", binding.package_version);
    try std.testing.expectEqualStrings("sha256:projected", binding.package_digest);
    try std.testing.expectEqualStrings("memoryaf_wasm", binding.runtime_name);
    try std.testing.expectEqualStrings("target/wasm32-wasip2/release/memoryaf_extension.wasm", binding.artifact);
    try std.testing.expectEqualStrings("run-tool", binding.entrypoint);
}

test "extension runtime binding carries matched installed package digest" {
    try testExtensionRuntimeBindingCarriesMatchedInstalledPackageDigest();
}

fn findWasmRuntime(runtimes: []const extension_domain.RuntimeDecl, name: []const u8) ?extension_domain.RuntimeDecl {
    for (runtimes) |runtime| {
        if (runtime.mode == .wasm and std.mem.eql(u8, runtime.name, name)) return runtime;
    }
    return null;
}

fn findInstalledExtensionForRuntimeTool(installed_extensions: []const extension_domain.InstalledExtension, name: []const u8) ?*const extension_domain.InstalledExtension {
    for (installed_extensions) |*installed| {
        if (std.mem.eql(u8, installed.name, name) and installed.status == .ready) return installed;
    }
    return null;
}

fn callExtensionMcpTool(
    alloc: std.mem.Allocator,
    server: anytype,
    authorization: ?[]const u8,
    trusted_principal: ?[]const u8,
    authenticated_identity: ?DelegatedIdentity,
    installed: *const extension_domain.InstalledExtension,
    tool: ExtensionMcpTool,
    args: std.json.Value,
) !mcp.CallToolResult {
    if (parseWasmHandler(tool.handler)) |handler| {
        const tool_name = handler.tool_name;
        if (!std.mem.eql(u8, tool_name, tool.member.object_name)) {
            return try mcpError(alloc, "extension MCP handler does not match requested tool");
        }
        const binding = tool.runtime_binding orelse return try mcpError(alloc, "extension wasm runtime binding is unavailable");
        const request_json = try stringifyJsonValue(alloc, args);
        defer alloc.free(request_json);
        var host_context = ExtensionHostContext(@TypeOf(server)){
            .server = server,
            .authorization = authorization,
            .trusted_principal = trusted_principal,
            .authenticated_identity = authenticated_identity,
            .installed = installed,
        };
        if (wasmtime_runtime.invokeExtensionWithOptions(alloc, binding.runtime(), tool_name, request_json, .{
            .package_store_root = server.cfg.extension_package_store_dir,
            .host_imports = .{
                .ptr = &host_context,
                .db_query = ExtensionHostContext(@TypeOf(server)).dbQuery,
                .db_write = ExtensionHostContext(@TypeOf(server)).dbWrite,
                .ai_embed = ExtensionHostContext(@TypeOf(server)).aiEmbed,
            },
        })) |body| {
            return try mcpResultFromExtensionJson(alloc, body);
        } else |err| switch (err) {
            error.WasmtimeUnavailable,
            error.WasmtimeUnsupportedArtifact,
            error.WasmtimePackageStoreUnavailable,
            error.WasmtimeArtifactNotFound,
            error.WasmtimeSymbolMissing,
            => {
                std.log.warn("extension wasm runtime unavailable package={s} version={s} runtime={s} artifact={s} err={}", .{ binding.package_name, binding.package_version, binding.runtime_name, binding.artifact, err });
                return try mcpError(alloc, "extension wasm runtime is unavailable");
            },
            else => {
                std.log.warn("extension wasm runtime invocation failed package={s} version={s} runtime={s} artifact={s} err={}", .{ binding.package_name, binding.package_version, binding.runtime_name, binding.artifact, err });
                return try mcpError(alloc, "extension wasm runtime invocation failed");
            },
        }
    }

    const message = try std.fmt.allocPrint(alloc, "extension MCP tool '{s}' is registered but has no executable handler", .{tool.member.object_name});
    defer alloc.free(message);
    return mcpError(alloc, message);
}

fn ExtensionHostContext(comptime Server: type) type {
    return struct {
        server: Server,
        authorization: ?[]const u8,
        trusted_principal: ?[]const u8,
        authenticated_identity: ?DelegatedIdentity,
        installed: *const extension_domain.InstalledExtension,

        fn dbQuery(ptr: ?*anyopaque, alloc: std.mem.Allocator, table: []const u8, query_json: []const u8) anyerror![]u8 {
            const ctx = hostContext(ptr);
            const table_target = try ctx.resolveTableTarget(table);
            try requireExtensionTableAuthority(alloc, ctx.installed, ctx.authenticated_identity, table_target, "db:read", .read);
            const body = try extensionQueryBodyAlloc(alloc, query_json);
            defer alloc.free(body);
            return try ctx.dispatchJson(alloc, .POST, table_target, "query", body);
        }

        fn dbWrite(ptr: ?*anyopaque, alloc: std.mem.Allocator, table: []const u8, writes_json: []const u8) anyerror![]u8 {
            const ctx = hostContext(ptr);
            const table_target = try ctx.resolveTableTarget(table);
            try requireExtensionTableAuthority(alloc, ctx.installed, ctx.authenticated_identity, table_target, "db:write", .write);
            const body = try extensionBatchBodyAlloc(alloc, writes_json);
            defer alloc.free(body);
            return try ctx.dispatchJson(alloc, .POST, table_target, "batch", body);
        }

        fn aiEmbed(ptr: ?*anyopaque, alloc: std.mem.Allocator, _: []const u8, text: []const u8) anyerror![]f32 {
            const ctx = hostContext(ptr);
            try ctx.requireCapability("ai:embed");
            const out = try alloc.alloc(f32, 8);
            var hash = std.hash.Wyhash.init(0);
            hash.update(text);
            var state = hash.final();
            for (out, 0..) |*value, i| {
                state = std.hash.Wyhash.hash(state +% @as(u64, @intCast(i)), text);
                const raw: u16 = @truncate(state);
                value.* = @as(f32, @floatFromInt(raw)) / 65535.0;
            }
            return out;
        }

        fn hostContext(ptr: ?*anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr.?));
        }

        fn requireCapability(ctx: *@This(), name: []const u8) !void {
            for (ctx.installed.granted_capabilities) |capability| {
                if (!std.mem.eql(u8, capability.name, name)) continue;
                if (capability.scope.len == 0 or
                    std.mem.eql(u8, capability.scope, ctx.installed.package_name) or
                    (ctx.installed.scope.kind == .table and std.mem.eql(u8, capability.scope, ctx.installed.scope.table_name)))
                {
                    return;
                }
            }
            return error.ExtensionCapabilityDenied;
        }

        fn resolveTableTarget(ctx: *@This(), requested: []const u8) !catalog_resources.TableTarget {
            const table_name = switch (ctx.installed.scope.kind) {
                .table => ctx.installed.scope.table_name,
                .cluster => requested,
                .embedded_db => return error.UnsupportedExtensionScope,
            };
            return try extensionHostTableTarget(table_name);
        }

        fn dispatchJson(ctx: *@This(), alloc: std.mem.Allocator, method: http_common.Method, table_target: catalog_resources.TableTarget, route: []const u8, body: []const u8) ![]u8 {
            const table_uri = try catalogTableRouteAlloc(alloc, table_target.database_name, table_target.namespace_name, table_target.table_name);
            defer alloc.free(table_uri);
            const uri = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ table_uri, route });
            defer alloc.free(uri);
            const headers: []const http_common.RequestHeader = if (ctx.trusted_principal) |principal|
                &[_]http_common.RequestHeader{.{ .name = trusted_principal_header, .value = principal }}
            else
                &.{};
            var resp = try ctx.server.handle(.{
                .method = method,
                .uri = uri,
                .headers = headers,
                .authorization = ctx.authorization,
                .content_type = "application/json",
                .body = body,
            });
            defer resp.deinit(ctx.server.alloc);
            if (resp.status < 200 or resp.status >= 300) return error.ExtensionHostApiFailed;
            return try alloc.dupe(u8, resp.body);
        }
    };
}

fn extensionHostTableTarget(table_name: []const u8) !catalog_resources.TableTarget {
    const first_dot = std.mem.indexOfScalar(u8, table_name, '.');
    if (first_dot == null) return try catalog_resources.tableTargetFromOptional(null, null, table_name);
    const first = first_dot.?;
    const rest = table_name[first + 1 ..];
    const second_rel = std.mem.indexOfScalar(u8, rest, '.') orelse return error.UnsupportedExtensionTableTarget;
    const second = first + 1 + second_rel;
    if (std.mem.indexOfScalar(u8, table_name[second + 1 ..], '.') != null) return error.UnsupportedExtensionTableTarget;
    return try catalog_resources.tableTargetFromOptional(table_name[0..first], table_name[first + 1 .. second], table_name[second + 1 ..]);
}

fn requireExtensionTableAuthority(
    alloc: std.mem.Allocator,
    installed: *const extension_domain.InstalledExtension,
    authenticated_identity: ?DelegatedIdentity,
    table_target: catalog_resources.TableTarget,
    capability_name: []const u8,
    permission_type: usermgr.PermissionType,
) !void {
    if (!try extensionInstallAllowsTableCapability(alloc, installed, capability_name, table_target)) {
        return error.ExtensionCapabilityDenied;
    }
    const identity = authenticated_identity orelse return;
    const resource = try catalog_resources.tableResourceNameAlloc(alloc, table_target.database_name, table_target.namespace_name, table_target.table_name);
    defer alloc.free(resource);
    if (!identityHasPermission(identity.permissions, .table, resource, permission_type)) {
        return error.PermissionDenied;
    }
}

fn extensionInstallAllowsTableCapability(
    alloc: std.mem.Allocator,
    installed: *const extension_domain.InstalledExtension,
    capability_name: []const u8,
    table_target: catalog_resources.TableTarget,
) !bool {
    const resource = try catalog_resources.tableResourceNameAlloc(alloc, table_target.database_name, table_target.namespace_name, table_target.table_name);
    defer alloc.free(resource);
    for (installed.granted_capabilities) |capability| {
        if (!std.mem.eql(u8, capability.name, capability_name)) continue;
        if (capability.scope.len == 0 or std.mem.eql(u8, capability.scope, installed.package_name)) return true;
        if (std.mem.eql(u8, capability.scope, "*")) return true;
        if (catalog_resources.tableResourceMatches(capability.scope, resource)) return true;
    }
    return false;
}

fn extensionBatchBodyAlloc(alloc: std.mem.Allocator, writes_json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, writes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidExtensionWrite;
    const object = parsed.value.object;
    if (object.get("inserts") != null or object.get("deletes") != null or object.get("transforms") != null) {
        return try alloc.dupe(u8, writes_json);
    }
    const key = if (object.get("id")) |id_value| switch (id_value) {
        .string => |value| try alloc.dupe(u8, value),
        else => try extensionGeneratedKeyAlloc(alloc, writes_json),
    } else try extensionGeneratedKeyAlloc(alloc, writes_json);
    defer alloc.free(key);
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();
    try writer.writer.writeAll("{\"inserts\":{");
    try std.json.Stringify.value(key, .{}, &writer.writer);
    try writer.writer.writeByte(':');
    try std.json.Stringify.value(parsed.value, .{}, &writer.writer);
    try writer.writer.writeAll("},\"sync_level\":\"full_index\"}");
    return try writer.toOwnedSlice();
}

fn extensionGeneratedKeyAlloc(alloc: std.mem.Allocator, bytes: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "ext:{x}", .{std.hash.Wyhash.hash(0, bytes)});
}

fn extensionQueryBodyAlloc(alloc: std.mem.Allocator, query_json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, query_json, .{});
    defer parsed.deinit();
    const object = if (parsed.value == .object) parsed.value.object else return error.InvalidExtensionQuery;
    const limit = if (object.get("limit")) |value| switch (value) {
        .integer => |raw| @max(1, @min(raw, 100)),
        .float => |raw| @as(i64, @intFromFloat(@max(1, @min(raw, 100)))),
        else => 10,
    } else 10;
    var writer: std.Io.Writer.Allocating = .init(alloc);
    errdefer writer.deinit();
    try writer.writer.print("{{\"query\":{{\"match_all\":{{}}}},\"limit\":{d}}}", .{limit});
    return try writer.toOwnedSlice();
}

fn mcpResultFromExtensionJson(alloc: std.mem.Allocator, body: []u8) !mcp.CallToolResult {
    errdefer alloc.free(body);
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, alloc, body, .{});
    const is_error = parsed == .object and blk: {
        const ok = parsed.object.get("ok") orelse break :blk false;
        break :blk ok == .bool and !ok.bool;
    };
    return .{
        .is_error = is_error,
        .text = body,
        .structured = parsed,
    };
}

fn extensionRuntimeMemberVisible(installed_extensions: []const extension_domain.InstalledExtension, extension_name: []const u8) bool {
    for (installed_extensions) |installed| {
        if (!std.mem.eql(u8, installed.name, extension_name)) continue;
        return installed.status == .ready;
    }
    return false;
}

fn cloneValidMcpInputSchemaJson(alloc: std.mem.Allocator, schema_json: []const u8) !?[]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{}) catch return null;
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    return try alloc.dupe(u8, schema_json);
}

fn buildMcpInputSchema(alloc: std.mem.Allocator, spec: McpToolSpec) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"type\":\"object\"");

    var required_count: usize = 0;
    for (spec.fields) |field| {
        if (field.required) required_count += 1;
    }
    if (required_count > 0) {
        try out.appendSlice(alloc, ",\"required\":[");
        var emitted: usize = 0;
        for (spec.fields) |field| {
            if (!field.required) continue;
            if (emitted != 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, field.name);
            emitted += 1;
        }
        try out.append(alloc, ']');
    }

    try out.appendSlice(alloc, ",\"properties\":{");
    for (spec.fields, 0..) |field, i| {
        if (i != 0) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, field.name);
        try out.append(alloc, ':');
        if (field.schema_json) |schema_json| {
            try out.appendSlice(alloc, schema_json);
            continue;
        }
        try out.appendSlice(alloc, "{\"type\":");
        try appendJsonString(alloc, &out, @tagName(field.schema_type));
        if (field.items_json) |items_json| {
            try out.appendSlice(alloc, ",\"items\":");
            try out.appendSlice(alloc, items_json);
        }
        if (field.description) |description| {
            try out.appendSlice(alloc, ",\"description\":");
            try appendJsonString(alloc, &out, description);
        }
        if (field.default_json) |default_json| {
            try out.appendSlice(alloc, ",\"default\":");
            try out.appendSlice(alloc, default_json);
        }
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "}}");
    return try out.toOwnedSlice(alloc);
}

pub fn handleA2aRequest(
    server_ptr: anytype,
    req: http_common.HttpRequest,
    query_embedding_security_scope: anytype,
    authenticated_identity: anytype,
) !http_common.HttpResponse {
    var arena_impl = std.heap.ArenaAllocator.init(server_ptr.alloc);
    defer arena_impl.deinit();
    var dispatcher = try buildA2aDispatcher(
        server_ptr,
        arena_impl.allocator(),
        req.authorization,
        req.header(trusted_principal_header),
        query_embedding_security_scope,
        authenticated_identity,
    );
    if (isJsonRpcMethod(arena_impl.allocator(), req.body, "message/stream")) {
        var sink = A2aSseSink{};
        defer sink.out.deinit(server_ptr.alloc);
        try dispatcher.handleJsonRpcStream(server_ptr.alloc, req.body, sink.iface());
        try sink.out.appendSlice(server_ptr.alloc, "event: done\ndata: {}\n\n");
        return try eventStreamResponse(server_ptr.alloc, 200, sink.out.items);
    }
    const response_body = try dispatcher.handleJsonRpc(server_ptr.alloc, req.body);
    defer server_ptr.alloc.free(response_body);
    return try jsonBodyResponseWithStatus(server_ptr.alloc, 200, response_body);
}

pub fn isA2aStreamingRequest(alloc: std.mem.Allocator, req: http_common.HttpRequest) bool {
    return req.method == .POST and isJsonRpcMethod(alloc, req.body, "message/stream");
}

pub fn handleA2aStreamingRequest(
    server_ptr: anytype,
    req: http_common.HttpRequest,
    writer: http_common.StreamWriter,
    query_embedding_security_scope: anytype,
    authenticated_identity: anytype,
) !bool {
    var arena_impl = std.heap.ArenaAllocator.init(server_ptr.alloc);
    defer arena_impl.deinit();
    if (!isJsonRpcMethod(arena_impl.allocator(), req.body, "message/stream")) return false;

    try writer.start(server_ptr.alloc, .{
        .status = 200,
        .content_type = "text/event-stream",
    });

    var dispatcher = try buildA2aDispatcher(
        server_ptr,
        arena_impl.allocator(),
        req.authorization,
        req.header(trusted_principal_header),
        query_embedding_security_scope,
        authenticated_identity,
    );
    var sink = A2aLiveSseSink{ .writer = writer };
    try dispatcher.handleJsonRpcStream(server_ptr.alloc, req.body, sink.iface());
    try writer.writeAll("event: done\ndata: {}\n\n");
    try writer.flush();
    return true;
}

const A2aSseSink = struct {
    out: std.ArrayListUnmanaged(u8) = .empty,

    fn iface(self: *@This()) a2a.StreamSink {
        return .{ .ptr = self, .emit_fn = emit };
    }

    fn emit(ptr: *anyopaque, alloc: std.mem.Allocator, event: std.json.Value) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try appendSseJsonEvent(alloc, &self.out, "message", event);
    }
};

const A2aLiveSseSink = struct {
    writer: http_common.StreamWriter,

    fn iface(self: *@This()) a2a.StreamSink {
        return .{ .ptr = self, .emit_fn = emit };
    }

    fn emit(ptr: *anyopaque, alloc: std.mem.Allocator, event: std.json.Value) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try writeSseJsonEvent(alloc, self.writer, "message", event);
        try self.writer.flush();
    }
};

pub fn handleA2aCard(
    server_ptr: anytype,
    query_embedding_security_scope: anytype,
    authenticated_identity: anytype,
) !http_common.HttpResponse {
    var arena_impl = std.heap.ArenaAllocator.init(server_ptr.alloc);
    defer arena_impl.deinit();
    var dispatcher = try buildA2aDispatcher(
        server_ptr,
        arena_impl.allocator(),
        null,
        null,
        query_embedding_security_scope,
        authenticated_identity,
    );
    const card = try dispatcher.agentCard(arena_impl.allocator());
    const body = try stringifyJsonValue(server_ptr.alloc, card);
    defer server_ptr.alloc.free(body);
    return try jsonBodyResponseWithStatus(server_ptr.alloc, 200, body);
}

fn buildA2aDispatcher(
    server_ptr: anytype,
    dispatcher_alloc: std.mem.Allocator,
    authorization: ?[]const u8,
    trusted_principal: ?[]const u8,
    query_embedding_security_scope: anytype,
    authenticated_identity: anytype,
) !a2a.Dispatcher {
    const Server = @TypeOf(server_ptr);
    const HandlerKind = enum { query_builder, retrieval };
    const HandlerContext = struct {
        server: Server,
        authorization: ?[]const u8,
        trusted_principal: ?[]const u8,
        query_embedding_security_scope: @TypeOf(query_embedding_security_scope),
        authenticated_identity: @TypeOf(authenticated_identity),
        kind: HandlerKind,

        fn iface(ctx: *@This()) a2a.AgentHandler {
            return .{
                .ptr = ctx,
                .skill_id_fn = skillId,
                .skill_fn = skill,
                .execute_fn = @This().execute,
            };
        }

        fn skillId(ptr: *anyopaque) []const u8 {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            return switch (ctx.kind) {
                .query_builder => "query-builder",
                .retrieval => "retrieval",
            };
        }

        fn skill(ptr: *anyopaque, _: std.mem.Allocator) !a2a.Skill {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            return switch (ctx.kind) {
                .query_builder => .{ .id = "query-builder", .name = "Query Builder", .description = "Translate natural language into Antfly query requests", .tags = &.{ "antfly", "query" }, .scopes = &a2a_catalog_scopes },
                .retrieval => .{ .id = "retrieval", .name = "Retrieval", .description = "Run Antfly retrieval and generation workflows", .tags = &.{ "antfly", "retrieval" }, .scopes = &a2a_catalog_scopes },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, request_ctx: a2a.RequestContext, queue: *a2a.EventQueue) !void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            return switch (ctx.kind) {
                .query_builder => try ctx.executeQueryBuilder(alloc, request_ctx, queue),
                .retrieval => try ctx.executeRetrieval(alloc, request_ctx, queue),
            };
        }

        fn executeQueryBuilder(ctx: *@This(), alloc: std.mem.Allocator, request_ctx: a2a.RequestContext, queue: *a2a.EventQueue) !void {
            const text = try a2a.messageText(alloc, request_ctx.message);
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "intent", .{ .string = text });
            if (a2a.firstDataPart(request_ctx.message)) |data| {
                if (data == .object) {
                    const catalog_table_name = a2aCatalogTableNameFromObjectAlloc(alloc, data.object) catch |err| switch (err) {
                        error.InvalidA2aCatalogTarget => {
                            try queue.status(alloc, request_ctx.task_id, request_ctx.context_id, "failed", "invalid explicit database/namespace table target");
                            return;
                        },
                        else => return err,
                    };
                    if (catalog_table_name) |table| {
                        try body.put(alloc, "table", .{ .string = table });
                    } else if (jsonStringObjectField(data.object, "table")) |table| {
                        try body.put(alloc, "table", .{ .string = table });
                    }
                    if (data.object.get("context")) |context| try body.put(alloc, "context", context);
                }
            }
            const body_json = try stringifyJsonValue(alloc, .{ .object = body });
            const headers: []const http_common.RequestHeader = if (ctx.trusted_principal) |token|
                &[_]http_common.RequestHeader{.{ .name = trusted_principal_header, .value = token }}
            else
                &.{};
            var resp = try ctx.server.handle(.{
                .method = .POST,
                .uri = routes.Routes.agents_query_builder,
                .headers = headers,
                .authorization = ctx.authorization,
                .content_type = "application/json",
                .body = body_json,
            });
            defer resp.deinit(ctx.server.alloc);
            if (resp.status < 200 or resp.status >= 300) {
                const failed_body = try alloc.dupe(u8, resp.body);
                try queue.status(alloc, request_ctx.task_id, request_ctx.context_id, "failed", failed_body);
                return;
            }
            const parsed: std.json.Value = std.json.parseFromSliceLeaky(std.json.Value, alloc, resp.body, .{}) catch .{ .string = resp.body };
            try queue.artifact(alloc, request_ctx.task_id, request_ctx.context_id, "query", try a2a.dataPart(alloc, parsed));
            try queue.status(alloc, request_ctx.task_id, request_ctx.context_id, "completed", "query built");
        }

        fn executeRetrieval(ctx: *@This(), alloc: std.mem.Allocator, request_ctx: a2a.RequestContext, queue: *a2a.EventQueue) !void {
            const text = try a2a.messageText(alloc, request_ctx.message);
            var body = std.json.ObjectMap.empty;
            try body.put(alloc, "query", .{ .string = text });
            try body.put(alloc, "stream", .{ .bool = false });
            if (a2a.firstDataPart(request_ctx.message)) |data| {
                if (data == .object) {
                    if (data.object.get("queries")) |queries| {
                        const normalized_queries = a2aNormalizedQueriesAlloc(alloc, queries) catch |err| switch (err) {
                            error.InvalidA2aCatalogTarget => {
                                try queue.status(alloc, request_ctx.task_id, request_ctx.context_id, "failed", "invalid explicit database/namespace table target");
                                return;
                            },
                            else => return err,
                        };
                        try body.put(alloc, "queries", normalized_queries);
                    } else if (jsonStringObjectField(data.object, "table")) |table| {
                        var query_obj = std.json.ObjectMap.empty;
                        const catalog_table_name = (a2aCatalogTableNameFromObjectAlloc(alloc, data.object) catch |err| switch (err) {
                            error.InvalidA2aCatalogTarget => {
                                try queue.status(alloc, request_ctx.task_id, request_ctx.context_id, "failed", "invalid explicit database/namespace table target");
                                return;
                            },
                            else => return err,
                        }) orelse table;
                        try query_obj.put(alloc, "table", .{ .string = catalog_table_name });
                        var full_text = std.json.ObjectMap.empty;
                        try full_text.put(alloc, "query", .{ .string = text });
                        try query_obj.put(alloc, "full_text_search", .{ .object = full_text });
                        try query_obj.put(alloc, "limit", .{ .integer = jsonIntObjectField(data.object, "limit") orelse 5 });
                        var queries = std.json.Array.init(alloc);
                        try queries.append(.{ .object = query_obj });
                        try body.put(alloc, "queries", .{ .array = queries });
                    }
                    if (data.object.get("steps")) |steps| try body.put(alloc, "steps", steps);
                    if (jsonIntObjectField(data.object, "max_internal_iterations")) |max_iterations| try body.put(alloc, "max_internal_iterations", .{ .integer = max_iterations });
                }
            }
            const body_json = try stringifyJsonValue(alloc, .{ .object = body });
            try ctx.server.executeA2aRetrieval(
                alloc,
                body_json,
                request_ctx.task_id,
                request_ctx.context_id,
                queue,
                ctx.query_embedding_security_scope,
                ctx.authenticated_identity,
            );
        }
    };

    const task_authority = try std.fmt.allocPrint(
        dispatcher_alloc,
        "{s}:{d}:{s}",
        .{
            @tagName(query_embedding_security_scope.domain),
            query_embedding_security_scope.value.len,
            query_embedding_security_scope.value,
        },
    );
    var dispatcher = a2a.Dispatcher{
        .io = server_ptr.inferenceIo(),
        .name = "Antfly",
        .version = "1.0.0",
        .base_url = routes.Routes.a2a,
        .task_store = server_ptr.a2a_tasks.iface(),
        .task_authority = task_authority,
    };
    const contexts = try dispatcher_alloc.alloc(HandlerContext, 2);
    contexts[0] = .{
        .server = server_ptr,
        .authorization = authorization,
        .trusted_principal = trusted_principal,
        .query_embedding_security_scope = query_embedding_security_scope,
        .authenticated_identity = authenticated_identity,
        .kind = .query_builder,
    };
    contexts[1] = .{
        .server = server_ptr,
        .authorization = authorization,
        .trusted_principal = trusted_principal,
        .query_embedding_security_scope = query_embedding_security_scope,
        .authenticated_identity = authenticated_identity,
        .kind = .retrieval,
    };
    try dispatcher.addHandler(dispatcher_alloc, contexts[0].iface());
    try dispatcher.addHandler(dispatcher_alloc, contexts[1].iface());
    return dispatcher;
}

fn jsonBodyResponseWithStatus(alloc: std.mem.Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return try bodyResponseWithStatus(alloc, status, "application/json", body);
}

fn bodyResponseWithStatus(alloc: std.mem.Allocator, status: u16, content_type: []const u8, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, content_type),
        .body = try alloc.dupe(u8, body),
    };
}

fn mcpBodyResponseWithStatus(alloc: std.mem.Allocator, result: mcp.HttpResult) !http_common.HttpResponse {
    var headers = try alloc.alloc(http_common.Header, result.headers.len);
    var initialized: usize = 0;
    errdefer {
        for (headers[0..initialized]) |*header| header.deinit(alloc);
        alloc.free(headers);
    }
    for (headers, result.headers) |*out, header| {
        const name = try alloc.dupe(u8, header.name);
        const value = alloc.dupe(u8, header.value) catch |err| {
            alloc.free(name);
            return err;
        };
        out.* = .{
            .name = name,
            .value = value,
        };
        initialized += 1;
    }
    const content_type = try alloc.dupe(u8, result.content_type);
    errdefer alloc.free(content_type);
    const body = try alloc.dupe(u8, result.body);
    return .{
        .status = result.status,
        .content_type = content_type,
        .headers = headers,
        .body = body,
    };
}

fn textResponse(alloc: std.mem.Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/plain"),
        .body = try alloc.dupe(u8, body),
    };
}

fn eventStreamResponse(alloc: std.mem.Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/event-stream"),
        .body = try alloc.dupe(u8, body),
    };
}

fn isJsonRpcMethod(alloc: std.mem.Allocator, body: []const u8, method_name: []const u8) bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return false;
    defer parsed.deinit();
    const request = parsed.value;
    if (request != .object) return false;
    const method = jsonStringObjectField(request.object, "method") orelse return false;
    return std.mem.eql(u8, method, method_name);
}

fn appendSseJsonEvent(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), event_name: []const u8, value: std.json.Value) !void {
    const body = try stringifyJsonValue(alloc, value);
    defer alloc.free(body);
    try out.appendSlice(alloc, "event: ");
    try out.appendSlice(alloc, event_name);
    try out.appendSlice(alloc, "\n");
    try out.appendSlice(alloc, "data: ");
    try out.appendSlice(alloc, body);
    try out.appendSlice(alloc, "\n\n");
}

fn writeSseJsonEvent(alloc: std.mem.Allocator, writer: http_common.StreamWriter, event_name: []const u8, value: std.json.Value) !void {
    const body = try stringifyJsonValue(alloc, value);
    defer alloc.free(body);
    try writer.writeAll("event: ");
    try writer.writeAll(event_name);
    try writer.writeAll("\n");
    try writer.writeAll("data: ");
    try writer.writeAll(body);
    try writer.writeAll("\n\n");
}

fn jsonStringArg(value: std.json.Value, key: []const u8) ?[]const u8 {
    if (value != .object) return null;
    return jsonStringObjectField(value.object, key);
}

fn jsonStringObjectField(object: anytype, key: []const u8) ?[]const u8 {
    const value = object.get(key) orelse return null;
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

fn jsonIntArg(value: std.json.Value, key: []const u8) ?i64 {
    if (value != .object) return null;
    return jsonIntObjectField(value.object, key);
}

fn jsonIntObjectField(object: anytype, key: []const u8) ?i64 {
    const value = object.get(key) orelse return null;
    return switch (value) {
        .integer => |n| n,
        .float => |n| @intFromFloat(n),
        else => null,
    };
}

fn jsonBoolArg(value: std.json.Value, key: []const u8) ?bool {
    if (value != .object) return null;
    const raw = value.object.get(key) orelse return null;
    return switch (raw) {
        .bool => |flag| flag,
        else => null,
    };
}

fn jsonValueArg(value: std.json.Value, key: []const u8) ?std.json.Value {
    if (value != .object) return null;
    return value.object.get(key);
}

fn hasNonRawQueryArg(args: std.json.Value) bool {
    if (args != .object) return false;
    var it = args.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (std.mem.eql(u8, key, "tableName") or std.mem.eql(u8, key, "queryRequest")) continue;
        return true;
    }
    return false;
}

fn putFullTextSearch(
    alloc: std.mem.Allocator,
    body: *std.json.ObjectMap,
    match: []const u8,
    field: ?[]const u8,
) !void {
    var full_text_obj = std.json.ObjectMap.empty;
    if (field) |field_name| {
        if (field_name.len != 0) {
            try full_text_obj.put(alloc, "match", .{ .string = match });
            try full_text_obj.put(alloc, "field", .{ .string = field_name });
            try body.put(alloc, "full_text_search", .{ .object = full_text_obj });
            return;
        }
    }
    try full_text_obj.put(alloc, "query", .{ .string = match });
    try body.put(alloc, "full_text_search", .{ .object = full_text_obj });
}

fn putFullTextSearchArg(
    alloc: std.mem.Allocator,
    body: *std.json.ObjectMap,
    args: std.json.Value,
) !void {
    if (jsonValueArg(args, "full_text_search")) |full_text| {
        if (full_text != .null) try body.put(alloc, "full_text_search", full_text);
        return;
    }
    if (jsonValueArg(args, "fullTextSearch")) |full_text| {
        switch (full_text) {
            .string => |text| if (text.len != 0) try putFullTextSearch(alloc, body, text, jsonStringArg(args, "fullTextSearchField")),
            .object => try body.put(alloc, "full_text_search", full_text),
            .null => {},
            else => return error.InvalidParams,
        }
    }
}

fn stringifyJsonValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    const encoded = try stringifyJsonValue(alloc, .{ .string = text });
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn appendUriPathSegment(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    const hex = "0123456789ABCDEF";
    for (text) |ch| {
        if (isUriUnreserved(ch)) {
            try out.append(alloc, ch);
        } else {
            try out.append(alloc, '%');
            try out.append(alloc, hex[ch >> 4]);
            try out.append(alloc, hex[ch & 0x0f]);
        }
    }
}

fn isUriUnreserved(ch: u8) bool {
    return (ch >= 'A' and ch <= 'Z') or
        (ch >= 'a' and ch <= 'z') or
        (ch >= '0' and ch <= '9') or
        ch == '-' or ch == '.' or ch == '_' or ch == '~';
}

fn mcpError(alloc: std.mem.Allocator, text: []const u8) !mcp.CallToolResult {
    return .{
        .is_error = true,
        .text = try alloc.dupe(u8, text),
    };
}

fn mcpResultFromHttpResponse(alloc: std.mem.Allocator, resp: http_common.HttpResponse) !mcp.CallToolResult {
    if (resp.status < 200 or resp.status >= 300) {
        return .{
            .is_error = true,
            .text = try alloc.dupe(u8, resp.body),
        };
    }
    var structured: ?std.json.Value = null;
    if (resp.content_type) |content_type| {
        if (std.mem.indexOf(u8, content_type, "json") != null and resp.body.len != 0) {
            structured = std.json.parseFromSliceLeaky(std.json.Value, alloc, resp.body, .{}) catch null;
        }
    }
    return .{
        .text = try alloc.dupe(u8, if (resp.body.len == 0) "ok" else resp.body),
        .structured = structured,
    };
}

test "mcp table tools expose catalog fields and route supported catalog lifecycle targets" {
    const alloc = std.testing.allocator;
    const schema = try buildMcpInputSchema(alloc, mcp_tool_specs[0]);
    defer alloc.free(schema);
    try std.testing.expect(std.mem.indexOf(u8, schema, "\"database\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "\"namespace\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema, "\"tableName\"") != null);

    var default_args = try std.json.parseFromSlice(std.json.Value, alloc, "{\"database\":\"default\",\"namespace\":\"public\",\"tableName\":\"docs\"}", .{});
    defer default_args.deinit();
    try std.testing.expectEqualStrings("docs", try catalogTableNameArg(default_args.value));

    var non_default_args = try std.json.parseFromSlice(std.json.Value, alloc, "{\"database\":\"tenant_ops\",\"namespace\":\"analytics\",\"tableName\":\"docs\"}", .{});
    defer non_default_args.deinit();
    const non_default_target = try catalogLifecycleTableTargetArg(non_default_args.value);
    try std.testing.expectEqualStrings("tenant_ops", non_default_target.database_name);
    try std.testing.expectEqualStrings("analytics", non_default_target.namespace_name);
    try std.testing.expectEqualStrings("docs", non_default_target.table_name);
    const non_default_route = try catalogTableRouteAlloc(alloc, non_default_target.database_name, non_default_target.namespace_name, non_default_target.table_name);
    defer alloc.free(non_default_route);
    try std.testing.expectEqualStrings("/databases/tenant_ops/namespaces/analytics/tables/docs", non_default_route);

    const list_route = try catalogNamespaceTablesRouteAlloc(alloc, .{
        .database_name = non_default_target.database_name,
        .namespace_name = non_default_target.namespace_name,
    });
    defer alloc.free(list_route);
    try std.testing.expectEqualStrings("/databases/tenant_ops/namespaces/analytics/tables", list_route);

    const non_default_query_route = try catalogTableRouteAlloc(alloc, non_default_target.database_name, non_default_target.namespace_name, non_default_target.table_name);
    defer alloc.free(non_default_query_route);
    try std.testing.expectEqualStrings("/databases/tenant_ops/namespaces/analytics/tables/docs", non_default_query_route);

    var namespace_read = try usermgr.Permission.initOwned(alloc, .namespace, "tenant_ops.analytics", .read);
    defer namespace_read.deinit(alloc);
    try std.testing.expect(identityCanListTables(&.{namespace_read}));

    var unrelated_table_read = try usermgr.Permission.initOwned(alloc, .table, "docs", .read);
    defer unrelated_table_read.deinit(alloc);
    try std.testing.expect(!identityCanListTables(&.{unrelated_table_read}));

    var tenant_docs_read = try usermgr.Permission.initOwned(alloc, .table, "tenant_ops.analytics.docs", .read);
    defer tenant_docs_read.deinit(alloc);
    try std.testing.expect(try mcpToolInvocationAllowed(alloc, mcp_tool_specs[3], non_default_args.value, &.{tenant_docs_read}));
    try std.testing.expect(!try mcpToolInvocationAllowed(alloc, mcp_tool_specs[3], non_default_args.value, &.{unrelated_table_read}));
    try std.testing.expect(try mcpToolInvocationAllowed(alloc, mcp_tool_specs[3], default_args.value, &.{unrelated_table_read}));

    var tenant_namespace_read = try usermgr.Permission.initOwned(alloc, .namespace, "tenant_ops.analytics", .read);
    defer tenant_namespace_read.deinit(alloc);
    try std.testing.expect(try mcpToolInvocationAllowed(alloc, mcp_tool_specs[2], non_default_args.value, &.{tenant_namespace_read}));
}

test "extension table host imports require install capability and caller table permission" {
    const alloc = std.testing.allocator;
    const target = try extensionHostTableTarget("default.public.docs");
    try std.testing.expectEqualStrings("default", target.database_name);
    try std.testing.expectEqualStrings("public", target.namespace_name);
    try std.testing.expectEqualStrings("docs", target.table_name);
    try std.testing.expectError(error.UnsupportedExtensionTableTarget, extensionHostTableTarget("analytics.docs"));

    const granted = [_]extension_domain.Capability{
        .{ .name = "db:read", .scope = "default.public.docs" },
    };
    const installed = extension_domain.InstalledExtension{
        .name = "memoryaf",
        .package_name = "memoryaf",
        .package_version = "1.0.0",
        .package_digest = "sha256:abc",
        .scope = .{ .kind = .table, .table_name = "default.public.docs" },
        .granted_capabilities = &granted,
        .status = .ready,
    };

    var read_docs = try usermgr.Permission.initOwned(alloc, .table, "docs", .read);
    defer read_docs.deinit(alloc);
    var read_other = try usermgr.Permission.initOwned(alloc, .table, "other", .read);
    defer read_other.deinit(alloc);

    const allowed_identity = DelegatedIdentity{
        .username = "alice",
        .permissions = &.{read_docs},
    };
    const denied_identity = DelegatedIdentity{
        .username = "alice",
        .permissions = &.{read_other},
    };

    try requireExtensionTableAuthority(alloc, &installed, allowed_identity, target, "db:read", .read);
    try std.testing.expectError(error.PermissionDenied, requireExtensionTableAuthority(alloc, &installed, denied_identity, target, "db:read", .read));
    try std.testing.expectError(error.ExtensionCapabilityDenied, requireExtensionTableAuthority(alloc, &installed, allowed_identity, target, "db:write", .write));
}
