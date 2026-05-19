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

//! OpenAPI JSON parser.
//!
//! Parses a std.json.Value tree (from std.json.parseFromSlice) into
//! typed OpenAPI AST nodes.

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");

pub const ParseError = error{
    MissingField,
    InvalidType,
    InvalidValue,
    OutOfMemory,
};

pub const Parser = struct {
    arena: Allocator,

    pub fn init(arena: Allocator) Parser {
        return .{ .arena = arena };
    }

    /// Parse an OpenAPI document from a JSON string.
    pub fn parseDocument(self: *Parser, json_bytes: []const u8) !types.OpenApiDoc {
        const parsed = try std.json.parseFromSlice(std.json.Value, self.arena, json_bytes, .{});
        const root = parsed.value;
        return self.parseDocFromValue(root);
    }

    fn parseDocFromValue(self: *Parser, root: std.json.Value) !types.OpenApiDoc {
        const obj = switch (root) {
            .object => |o| o,
            else => return ParseError.InvalidType,
        };

        var doc = types.OpenApiDoc{
            .openapi = try self.getString(obj, "openapi") orelse return ParseError.MissingField,
            .info = try self.parseInfo(obj.get("info") orelse return ParseError.MissingField),
        };

        // servers
        if (obj.get("servers")) |servers_val| {
            doc.servers = try self.parseArray(types.Server, servers_val, parseServer);
        }

        // paths
        if (obj.get("paths")) |paths_val| {
            doc.paths = try self.parsePaths(paths_val);
        }

        // components
        if (obj.get("components")) |comp_val| {
            doc.components = try self.parseComponents(comp_val);
        }

        // security
        if (obj.get("security")) |sec_val| {
            doc.security = try self.parseSecurityRequirements(sec_val);
        }

        // 3.1+: webhooks
        if (obj.get("webhooks")) |webhooks_val| {
            doc.webhooks = try self.parsePaths(webhooks_val);
        }

        return doc;
    }

    fn parseInfo(self: *Parser, val: std.json.Value) !types.Info {
        const obj = try self.asObject(val);
        return types.Info{
            .title = try self.getString(obj, "title") orelse "Untitled",
            .version = try self.getString(obj, "version") orelse "0.0.0",
            .description = try self.getString(obj, "description"),
        };
    }

    fn parseServer(self: *Parser, val: std.json.Value) !types.Server {
        const obj = try self.asObject(val);
        return types.Server{
            .url = try self.getString(obj, "url") orelse return ParseError.MissingField,
            .description = try self.getString(obj, "description"),
        };
    }

    fn parsePaths(self: *Parser, val: std.json.Value) !std.StringArrayHashMapUnmanaged(types.PathItem) {
        const obj = try self.asObject(val);
        var result = std.StringArrayHashMapUnmanaged(types.PathItem){};
        for (obj.keys(), obj.values()) |key, item_val| {
            const path_item = try self.parsePathItem(item_val);
            try result.put(self.arena, key, path_item);
        }
        return result;
    }

    fn parsePathItem(self: *Parser, val: std.json.Value) !types.PathItem {
        const obj = try self.asObject(val);
        var item = types.PathItem{
            .summary = try self.getString(obj, "summary"),
            .description = try self.getString(obj, "description"),
        };

        if (obj.get("parameters")) |params_val| {
            item.parameters = try self.parseParameterOrRefs(params_val);
        }

        const methods = [_]struct { name: []const u8, field: *?types.Operation }{
            .{ .name = "get", .field = &item.get },
            .{ .name = "post", .field = &item.post },
            .{ .name = "put", .field = &item.put },
            .{ .name = "delete", .field = &item.delete },
            .{ .name = "patch", .field = &item.patch },
            .{ .name = "head", .field = &item.head },
            .{ .name = "options", .field = &item.options },
        };

        for (methods) |m| {
            if (obj.get(m.name)) |op_val| {
                m.field.* = try self.parseOperation(op_val);
            }
        }

        return item;
    }

    fn parseOperation(self: *Parser, val: std.json.Value) !types.Operation {
        const obj = try self.asObject(val);
        var op = types.Operation{
            .operation_id = try self.getString(obj, "operationId"),
            .summary = try self.getString(obj, "summary"),
            .description = try self.getString(obj, "description"),
            .deprecated = self.getBool(obj, "deprecated") orelse false,
        };

        if (obj.get("tags")) |tags_val| {
            op.tags = try self.parseStringArray(tags_val);
        }

        if (obj.get("parameters")) |params_val| {
            op.parameters = try self.parseParameterOrRefs(params_val);
        }

        if (obj.get("requestBody")) |rb_val| {
            op.request_body = try self.parseRequestBodyOrRef(rb_val);
        }

        if (obj.get("responses")) |resp_val| {
            op.responses = try self.parseResponseMap(resp_val);
        }

        if (obj.get("security")) |sec_val| {
            op.security = try self.parseSecurityRequirements(sec_val);
        }

        return op;
    }

    pub fn parseSchema(self: *Parser, val: std.json.Value) ParseError!types.Schema {
        const obj = switch (val) {
            .object => |o| o,
            else => return ParseError.InvalidType,
        };

        var schema = types.Schema{
            .schema_type = self.parseSchemaType(obj),
            .format = try self.getString(obj, "format"),
            .title = try self.getString(obj, "title"),
            .description = try self.getString(obj, "description"),
            .nullable = self.getBool(obj, "nullable") orelse false,
            .read_only = self.getBool(obj, "readOnly") orelse false,
            .write_only = self.getBool(obj, "writeOnly") orelse false,
            .default_value = try self.getString(obj, "default"),
            .const_value = try self.getString(obj, "const"),
            .pattern = try self.getString(obj, "pattern"),
            .content_encoding = try self.getString(obj, "contentEncoding"),
            .content_media_type = try self.getString(obj, "contentMediaType"),
        };

        // enum
        if (obj.get("enum")) |enum_val| {
            schema.enum_values = try self.parseStringArray(enum_val);
        }

        // properties
        if (obj.get("properties")) |props_val| {
            schema.properties = try self.parseSchemaMap(props_val);
        }

        // required
        if (obj.get("required")) |req_val| {
            schema.required = try self.parseStringArray(req_val);
        }

        // items (for arrays)
        if (obj.get("items")) |items_val| {
            const item = try self.arena.create(types.SchemaOrRef);
            item.* = try self.parseSchemaOrRef(items_val);
            schema.items = item;
        }

        // allOf, oneOf, anyOf
        if (obj.get("allOf")) |v| {
            schema.all_of = try self.parseSchemaOrRefArray(v);
        }
        if (obj.get("oneOf")) |v| {
            schema.one_of = try self.parseSchemaOrRefArray(v);
        }
        if (obj.get("anyOf")) |v| {
            schema.any_of = try self.parseSchemaOrRefArray(v);
        }

        // discriminator
        if (obj.get("discriminator")) |disc_val| {
            schema.discriminator = try self.parseDiscriminator(disc_val);
        }

        // additionalProperties
        if (obj.get("additionalProperties")) |ap_val| {
            schema.additional_properties = switch (ap_val) {
                .bool => |b| types.AdditionalProperties{ .boolean = b },
                .object => blk: {
                    const sor = try self.arena.create(types.SchemaOrRef);
                    sor.* = try self.parseSchemaOrRef(ap_val);
                    break :blk types.AdditionalProperties{ .schema = sor };
                },
                else => null,
            };
        }

        // Numeric bounds
        schema.minimum = self.getNumber(obj, "minimum");
        schema.maximum = self.getNumber(obj, "maximum");
        schema.min_items = self.getInteger(obj, "minItems");
        schema.max_items = self.getInteger(obj, "maxItems");

        // 3.1+: exclusive bounds (numbers, not booleans)
        schema.exclusive_minimum = self.getNumber(obj, "exclusiveMinimum");
        schema.exclusive_maximum = self.getNumber(obj, "exclusiveMaximum");

        // String validation
        schema.min_length = self.getInteger(obj, "minLength");
        schema.max_length = self.getInteger(obj, "maxLength");

        // Object validation
        schema.min_properties = self.getInteger(obj, "minProperties");
        schema.max_properties = self.getInteger(obj, "maxProperties");

        // 3.1+: tuple validation
        if (obj.get("prefixItems")) |v| {
            schema.prefix_items = try self.parseSchemaOrRefArray(v);
        }

        // x-* extensions
        for (obj.keys(), obj.values()) |key, ext_val| {
            if (std.mem.startsWith(u8, key, "x-")) {
                const s = switch (ext_val) {
                    .string => |s| try self.arena.dupe(u8, s),
                    else => blk: {
                        // Stringify to JSON
                        break :blk try std.json.Stringify.valueAlloc(self.arena, ext_val, .{});
                    },
                };
                try schema.extensions.put(self.arena, key, s);
            }
        }

        return schema;
    }

    pub fn parseSchemaOrRef(self: *Parser, val: std.json.Value) ParseError!types.SchemaOrRef {
        const obj = switch (val) {
            .object => |o| o,
            else => return ParseError.InvalidType,
        };

        // Check for $ref — in 3.1+ may have sibling description/summary
        if (obj.get("$ref")) |ref_val| {
            const ref_str = switch (ref_val) {
                .string => |s| s,
                else => return ParseError.InvalidType,
            };
            return types.SchemaOrRef{ .ref = .{
                .ref_string = ref_str,
                .description = try self.getString(obj, "description"),
                .summary = try self.getString(obj, "summary"),
            } };
        }

        return types.SchemaOrRef{ .schema = try self.parseSchema(val) };
    }

    fn parseSchemaOrRefArray(self: *Parser, val: std.json.Value) ![]const types.SchemaOrRef {
        const arr = switch (val) {
            .array => |a| a,
            else => return ParseError.InvalidType,
        };
        var result = try std.ArrayListUnmanaged(types.SchemaOrRef).initCapacity(self.arena, arr.items.len);
        for (arr.items) |item| {
            try result.append(self.arena, try self.parseSchemaOrRef(item));
        }
        return result.items;
    }

    fn parseSchemaMap(self: *Parser, val: std.json.Value) !std.StringArrayHashMapUnmanaged(types.SchemaOrRef) {
        const obj = try self.asObject(val);
        var result = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
        for (obj.keys(), obj.values()) |key, prop_val| {
            try result.put(self.arena, key, try self.parseSchemaOrRef(prop_val));
        }
        return result;
    }

    fn parseDiscriminator(self: *Parser, val: std.json.Value) !types.Discriminator {
        const obj = try self.asObject(val);
        var disc = types.Discriminator{
            .property_name = try self.getString(obj, "propertyName") orelse return ParseError.MissingField,
        };
        if (obj.get("mapping")) |mapping_val| {
            const mobj = try self.asObject(mapping_val);
            for (mobj.keys(), mobj.values()) |key, map_val| {
                const s = switch (map_val) {
                    .string => |s| s,
                    else => continue,
                };
                try disc.mapping.put(self.arena, key, s);
            }
        }
        return disc;
    }

    fn parseParameterOrRefs(self: *Parser, val: std.json.Value) ![]const types.ParameterOrRef {
        const arr = switch (val) {
            .array => |a| a,
            else => return ParseError.InvalidType,
        };
        var result = try std.ArrayListUnmanaged(types.ParameterOrRef).initCapacity(self.arena, arr.items.len);
        for (arr.items) |item| {
            try result.append(self.arena, try self.parseParameterOrRef(item));
        }
        return result.items;
    }

    fn parseParameterOrRef(self: *Parser, val: std.json.Value) ParseError!types.ParameterOrRef {
        const obj = try self.asObject(val);
        if (obj.get("$ref")) |ref_val| {
            const ref_str = switch (ref_val) {
                .string => |s| s,
                else => return ParseError.InvalidType,
            };
            return types.ParameterOrRef{ .ref = ref_str };
        }
        return types.ParameterOrRef{ .parameter = try self.parseParameter(val) };
    }

    fn parseParameter(self: *Parser, val: std.json.Value) !types.Parameter {
        const obj = try self.asObject(val);
        const in_str = try self.getString(obj, "in") orelse return ParseError.MissingField;
        var param = types.Parameter{
            .name = try self.getString(obj, "name") orelse return ParseError.MissingField,
            .in = types.ParameterLocation.fromString(in_str) orelse return ParseError.InvalidValue,
            .required = self.getBool(obj, "required") orelse false,
            .description = try self.getString(obj, "description"),
            .deprecated = self.getBool(obj, "deprecated") orelse false,
        };
        if (obj.get("schema")) |schema_val| {
            param.schema = try self.parseSchemaOrRef(schema_val);
        }
        return param;
    }

    fn parseRequestBodyOrRef(self: *Parser, val: std.json.Value) ParseError!types.RequestBodyOrRef {
        const obj = try self.asObject(val);
        if (obj.get("$ref")) |ref_val| {
            const ref_str = switch (ref_val) {
                .string => |s| s,
                else => return ParseError.InvalidType,
            };
            return types.RequestBodyOrRef{ .ref = ref_str };
        }
        return types.RequestBodyOrRef{ .request_body = try self.parseRequestBody(val) };
    }

    fn parseRequestBody(self: *Parser, val: std.json.Value) !types.RequestBody {
        const obj = try self.asObject(val);
        var rb = types.RequestBody{
            .description = try self.getString(obj, "description"),
            .required = self.getBool(obj, "required") orelse false,
        };
        if (obj.get("content")) |content_val| {
            rb.content = try self.parseContentMap(content_val);
        }
        return rb;
    }

    fn parseContentMap(self: *Parser, val: std.json.Value) !std.StringArrayHashMapUnmanaged(types.MediaType) {
        const obj = try self.asObject(val);
        var result = std.StringArrayHashMapUnmanaged(types.MediaType){};
        for (obj.keys(), obj.values()) |key, mt_val| {
            const mt_obj = try self.asObject(mt_val);
            var mt = types.MediaType{};
            if (mt_obj.get("schema")) |schema_val| {
                mt.schema = try self.parseSchemaOrRef(schema_val);
            }
            try result.put(self.arena, key, mt);
        }
        return result;
    }

    fn parseResponseMap(self: *Parser, val: std.json.Value) !std.StringArrayHashMapUnmanaged(types.ResponseOrRef) {
        const obj = try self.asObject(val);
        var result = std.StringArrayHashMapUnmanaged(types.ResponseOrRef){};
        for (obj.keys(), obj.values()) |key, resp_val| {
            try result.put(self.arena, key, try self.parseResponseOrRef(resp_val));
        }
        return result;
    }

    fn parseResponseOrRef(self: *Parser, val: std.json.Value) ParseError!types.ResponseOrRef {
        const obj = try self.asObject(val);
        if (obj.get("$ref")) |ref_val| {
            const ref_str = switch (ref_val) {
                .string => |s| s,
                else => return ParseError.InvalidType,
            };
            return types.ResponseOrRef{ .ref = ref_str };
        }
        return types.ResponseOrRef{ .response = try self.parseResponse(val) };
    }

    fn parseResponse(self: *Parser, val: std.json.Value) !types.Response {
        const obj = try self.asObject(val);
        var resp = types.Response{
            .description = try self.getString(obj, "description"),
        };
        if (obj.get("content")) |content_val| {
            resp.content = try self.parseContentMap(content_val);
        }
        return resp;
    }

    fn parseComponents(self: *Parser, val: std.json.Value) !types.Components {
        const obj = try self.asObject(val);
        var comp = types.Components{};

        if (obj.get("schemas")) |schemas_val| {
            comp.schemas = try self.parseSchemaMap(schemas_val);
        }

        if (obj.get("parameters")) |params_val| {
            const params_obj = try self.asObject(params_val);
            for (params_obj.keys(), params_obj.values()) |key, p_val| {
                try comp.parameters.put(self.arena, key, try self.parseParameterOrRef(p_val));
            }
        }

        if (obj.get("securitySchemes")) |ss_val| {
            const ss_obj = try self.asObject(ss_val);
            for (ss_obj.keys(), ss_obj.values()) |key, s_val| {
                try comp.security_schemes.put(self.arena, key, try self.parseSecurityScheme(s_val));
            }
        }

        if (obj.get("requestBodies")) |rb_val| {
            const rb_obj = try self.asObject(rb_val);
            for (rb_obj.keys(), rb_obj.values()) |key, r_val| {
                try comp.request_bodies.put(self.arena, key, try self.parseRequestBodyOrRef(r_val));
            }
        }

        if (obj.get("responses")) |resp_val| {
            const resp_obj = try self.asObject(resp_val);
            for (resp_obj.keys(), resp_obj.values()) |key, r_val| {
                try comp.responses.put(self.arena, key, try self.parseResponseOrRef(r_val));
            }
        }

        // 3.1+: pathItems
        if (obj.get("pathItems")) |pi_val| {
            const pi_obj = try self.asObject(pi_val);
            for (pi_obj.keys(), pi_obj.values()) |key, p_val| {
                try comp.path_items.put(self.arena, key, try self.parsePathItem(p_val));
            }
        }

        return comp;
    }

    fn parseSecurityScheme(self: *Parser, val: std.json.Value) !types.SecurityScheme {
        const obj = try self.asObject(val);
        return types.SecurityScheme{
            .type = try self.getString(obj, "type") orelse return ParseError.MissingField,
            .scheme = try self.getString(obj, "scheme"),
            .name = try self.getString(obj, "name"),
            .in = try self.getString(obj, "in"),
            .bearer_format = try self.getString(obj, "bearerFormat"),
        };
    }

    fn parseSecurityRequirements(self: *Parser, val: std.json.Value) ![]const types.SecurityRequirement {
        const arr = switch (val) {
            .array => |a| a,
            else => return ParseError.InvalidType,
        };
        var result = try std.ArrayListUnmanaged(types.SecurityRequirement).initCapacity(self.arena, arr.items.len);
        for (arr.items) |item| {
            const obj = switch (item) {
                .object => |o| o,
                else => continue,
            };
            for (obj.keys(), obj.values()) |key, scopes_val| {
                var scopes: []const []const u8 = &.{};
                if (scopes_val == .array) {
                    scopes = try self.parseStringArray(scopes_val);
                }
                try result.append(self.arena, types.SecurityRequirement{
                    .name = key,
                    .scopes = scopes,
                });
            }
        }
        return result.items;
    }

    // ---- Helpers ----

    /// Parse the "type" field which can be a string (3.0) or array of strings (3.1+).
    fn parseSchemaType(self: *Parser, obj: std.json.ObjectMap) ?types.SchemaType {
        const val = obj.get("type") orelse return null;
        switch (val) {
            .string => |s| return types.SchemaType{ .single = s },
            .array => |arr| {
                var result = std.ArrayListUnmanaged([]const u8).initCapacity(self.arena, arr.items.len) catch return null;
                for (arr.items) |item| {
                    switch (item) {
                        .string => |s| result.append(self.arena, s) catch return null,
                        else => {},
                    }
                }
                return types.SchemaType{ .array = result.items };
            },
            else => return null,
        }
    }

    fn asObject(_: *Parser, val: std.json.Value) ParseError!std.json.ObjectMap {
        return switch (val) {
            .object => |o| o,
            else => ParseError.InvalidType,
        };
    }

    fn getString(_: *Parser, obj: std.json.ObjectMap, key: []const u8) ParseError!?[]const u8 {
        const val = obj.get(key) orelse return null;
        return switch (val) {
            .string => |s| s,
            else => null,
        };
    }

    fn getBool(_: *Parser, obj: std.json.ObjectMap, key: []const u8) ?bool {
        const val = obj.get(key) orelse return null;
        return switch (val) {
            .bool => |b| b,
            else => null,
        };
    }

    fn getInteger(_: *Parser, obj: std.json.ObjectMap, key: []const u8) ?u64 {
        const val = obj.get(key) orelse return null;
        return switch (val) {
            .integer => |i| if (i >= 0) @intCast(i) else null,
            else => null,
        };
    }

    fn getNumber(_: *Parser, obj: std.json.ObjectMap, key: []const u8) ?f64 {
        const val = obj.get(key) orelse return null;
        return switch (val) {
            .integer => |i| @floatFromInt(i),
            .float => |f| f,
            else => null,
        };
    }

    fn parseStringArray(self: *Parser, val: std.json.Value) ![]const []const u8 {
        const arr = switch (val) {
            .array => |a| a,
            else => return ParseError.InvalidType,
        };
        var result = try std.ArrayListUnmanaged([]const u8).initCapacity(self.arena, arr.items.len);
        for (arr.items) |item| {
            switch (item) {
                .string => |s| try result.append(self.arena, s),
                else => {},
            }
        }
        return result.items;
    }

    fn parseArray(
        self: *Parser,
        comptime T: type,
        val: std.json.Value,
        comptime parseFn: fn (*Parser, std.json.Value) ParseError!T,
    ) ![]const T {
        const arr = switch (val) {
            .array => |a| a,
            else => return ParseError.InvalidType,
        };
        var result = try std.ArrayListUnmanaged(T).initCapacity(self.arena, arr.items.len);
        for (arr.items) |item| {
            try result.append(self.arena, try parseFn(self, item));
        }
        return result.items;
    }
};

test "parse minimal document" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const json_str =
        \\{
        \\  "openapi": "3.0.3",
        \\  "info": { "title": "Test API", "version": "1.0.0" },
        \\  "paths": {}
        \\}
    ;

    var p = Parser.init(arena);
    const doc = try p.parseDocument(json_str);

    try std.testing.expectEqualStrings("3.0.3", doc.openapi);
    try std.testing.expectEqualStrings("Test API", doc.info.title);
    try std.testing.expectEqual(@as(usize, 0), doc.paths.count());
}

test "parse schema with properties" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const json_str =
        \\{
        \\  "openapi": "3.0.3",
        \\  "info": { "title": "Test", "version": "1.0" },
        \\  "paths": {},
        \\  "components": {
        \\    "schemas": {
        \\      "User": {
        \\        "type": "object",
        \\        "required": ["id", "name"],
        \\        "properties": {
        \\          "id": { "type": "string" },
        \\          "name": { "type": "string" },
        \\          "age": { "type": "integer", "format": "int32" },
        \\          "active": { "type": "boolean" }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;

    var p = Parser.init(arena);
    const doc = try p.parseDocument(json_str);

    const schemas = doc.components.?.schemas;
    try std.testing.expectEqual(@as(usize, 1), schemas.count());

    const user_sor = schemas.get("User").?;
    const user = user_sor.schema;
    try std.testing.expectEqualStrings("object", user.primaryType().?);
    try std.testing.expectEqual(@as(usize, 4), user.properties.count());
    try std.testing.expectEqual(@as(usize, 2), user.required.len);
}

test "parse $ref" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const json_str =
        \\{
        \\  "openapi": "3.0.3",
        \\  "info": { "title": "Test", "version": "1.0" },
        \\  "paths": {
        \\    "/users": {
        \\      "get": {
        \\        "operationId": "listUsers",
        \\        "responses": {
        \\          "200": {
        \\            "description": "OK",
        \\            "content": {
        \\              "application/json": {
        \\                "schema": {
        \\                  "$ref": "#/components/schemas/UserList"
        \\                }
        \\              }
        \\            }
        \\          }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;

    var p = Parser.init(arena);
    const doc = try p.parseDocument(json_str);

    const path = doc.paths.get("/users").?;
    const op = path.get.?;
    try std.testing.expectEqualStrings("listUsers", op.operation_id.?);

    const resp = op.responses.get("200").?.response;
    const mt = resp.content.get("application/json").?;
    try std.testing.expectEqualStrings("#/components/schemas/UserList", mt.schema.?.ref.ref_string);
}

test "parse enum schema" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const json_str =
        \\{
        \\  "type": "string",
        \\  "enum": ["text", "keyword", "embedding"]
        \\}
    ;

    const parsed = try std.json.parseFromSlice(std.json.Value, arena, json_str, .{});
    var p = Parser.init(arena);
    const schema = try p.parseSchema(parsed.value);

    try std.testing.expectEqualStrings("string", schema.primaryType().?);
    try std.testing.expectEqual(@as(usize, 3), schema.enum_values.len);
    try std.testing.expectEqualStrings("text", schema.enum_values[0]);
}

test "parse 3.1 type array" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const json_str =
        \\{
        \\  "type": ["string", "null"],
        \\  "description": "nullable name"
        \\}
    ;

    const parsed = try std.json.parseFromSlice(std.json.Value, arena, json_str, .{});
    var p = Parser.init(arena);
    const schema = try p.parseSchema(parsed.value);

    try std.testing.expectEqualStrings("string", schema.primaryType().?);
    try std.testing.expect(schema.isNullable());
    try std.testing.expect(!schema.nullable); // 3.0 nullable is false
}

test "parse 3.1 $ref with description" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const json_str =
        \\{
        \\  "$ref": "#/components/schemas/Pet",
        \\  "description": "The pet to return"
        \\}
    ;

    const parsed = try std.json.parseFromSlice(std.json.Value, arena, json_str, .{});
    var p = Parser.init(arena);
    const sor = try p.parseSchemaOrRef(parsed.value);

    try std.testing.expectEqualStrings("#/components/schemas/Pet", sor.ref.ref_string);
    try std.testing.expectEqualStrings("The pet to return", sor.ref.description.?);
}

test "parse 3.1 new schema keywords" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const json_str =
        \\{
        \\  "type": "string",
        \\  "title": "User Name",
        \\  "const": "admin",
        \\  "minLength": 1,
        \\  "maxLength": 100,
        \\  "contentEncoding": "base64",
        \\  "contentMediaType": "image/png"
        \\}
    ;

    const parsed = try std.json.parseFromSlice(std.json.Value, arena, json_str, .{});
    var p = Parser.init(arena);
    const schema = try p.parseSchema(parsed.value);

    try std.testing.expectEqualStrings("User Name", schema.title.?);
    try std.testing.expectEqualStrings("admin", schema.const_value.?);
    try std.testing.expectEqual(@as(u64, 1), schema.min_length.?);
    try std.testing.expectEqual(@as(u64, 100), schema.max_length.?);
    try std.testing.expectEqualStrings("base64", schema.content_encoding.?);
    try std.testing.expectEqualStrings("image/png", schema.content_media_type.?);
}

test "parse 3.1 exclusive bounds" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const json_str =
        \\{
        \\  "type": "number",
        \\  "exclusiveMinimum": 0,
        \\  "exclusiveMaximum": 100.5
        \\}
    ;

    const parsed = try std.json.parseFromSlice(std.json.Value, arena, json_str, .{});
    var p = Parser.init(arena);
    const schema = try p.parseSchema(parsed.value);

    try std.testing.expectEqual(@as(f64, 0), schema.exclusive_minimum.?);
    try std.testing.expectEqual(@as(f64, 100.5), schema.exclusive_maximum.?);
}

test "parse 3.1 webhooks" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const json_str =
        \\{
        \\  "openapi": "3.1.0",
        \\  "info": { "title": "Test", "version": "1.0" },
        \\  "paths": {},
        \\  "webhooks": {
        \\    "newPet": {
        \\      "post": {
        \\        "operationId": "onNewPet",
        \\        "responses": {
        \\          "200": { "description": "OK" }
        \\        }
        \\      }
        \\    }
        \\  }
        \\}
    ;

    var p = Parser.init(arena);
    const doc = try p.parseDocument(json_str);

    try std.testing.expectEqual(@as(usize, 1), doc.webhooks.count());
    const webhook = doc.webhooks.get("newPet").?;
    try std.testing.expectEqualStrings("onNewPet", webhook.post.?.operation_id.?);
}
