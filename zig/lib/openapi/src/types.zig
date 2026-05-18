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

//! OpenAPI 3.0.x / 3.1.x / 3.2.x AST types
//!
//! These types represent the parsed structure of an OpenAPI document.
//! They are constructed by the parser from std.json.Value trees.
//! Supports OpenAPI 3.0, 3.1 (JSON Schema 2020-12), and 3.2 features.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Detected OpenAPI specification version.
pub const SpecVersion = enum {
    v3_0,
    v3_1,
    v3_2,

    pub fn parse(version_str: []const u8) SpecVersion {
        if (std.mem.startsWith(u8, version_str, "3.2")) return .v3_2;
        if (std.mem.startsWith(u8, version_str, "3.1")) return .v3_1;
        return .v3_0;
    }
};

pub const OpenApiDoc = struct {
    openapi: []const u8,
    info: Info,
    servers: []const Server = &.{},
    paths: std.StringArrayHashMapUnmanaged(PathItem) = .{},
    components: ?Components = null,
    security: []const SecurityRequirement = &.{},
    /// OpenAPI 3.1+: webhooks (operationId → PathItem)
    webhooks: std.StringArrayHashMapUnmanaged(PathItem) = .{},

    pub fn specVersion(self: *const OpenApiDoc) SpecVersion {
        return SpecVersion.parse(self.openapi);
    }
};

pub const Info = struct {
    title: []const u8,
    version: []const u8,
    description: ?[]const u8 = null,
};

pub const Server = struct {
    url: []const u8,
    description: ?[]const u8 = null,
};

pub const Components = struct {
    schemas: std.StringArrayHashMapUnmanaged(SchemaOrRef) = .{},
    parameters: std.StringArrayHashMapUnmanaged(ParameterOrRef) = .{},
    security_schemes: std.StringArrayHashMapUnmanaged(SecurityScheme) = .{},
    request_bodies: std.StringArrayHashMapUnmanaged(RequestBodyOrRef) = .{},
    responses: std.StringArrayHashMapUnmanaged(ResponseOrRef) = .{},
    /// OpenAPI 3.1+: reusable path items
    path_items: std.StringArrayHashMapUnmanaged(PathItem) = .{},
};

pub const PathItem = struct {
    summary: ?[]const u8 = null,
    description: ?[]const u8 = null,
    parameters: []const ParameterOrRef = &.{},
    get: ?Operation = null,
    post: ?Operation = null,
    put: ?Operation = null,
    delete: ?Operation = null,
    patch: ?Operation = null,
    head: ?Operation = null,
    options: ?Operation = null,
};

pub const Operation = struct {
    operation_id: ?[]const u8 = null,
    summary: ?[]const u8 = null,
    description: ?[]const u8 = null,
    tags: []const []const u8 = &.{},
    parameters: []const ParameterOrRef = &.{},
    request_body: ?RequestBodyOrRef = null,
    responses: std.StringArrayHashMapUnmanaged(ResponseOrRef) = .{},
    security: []const SecurityRequirement = &.{},
    deprecated: bool = false,
};

/// Schema type: in 3.0 a single string, in 3.1+ can be an array of strings.
/// e.g., "string" or ["string", "null"]
pub const SchemaType = union(enum) {
    single: []const u8,
    array: []const []const u8,

    /// Get the primary (non-null) type string.
    pub fn primaryType(self: SchemaType) ?[]const u8 {
        switch (self) {
            .single => |s| return s,
            .array => |arr| {
                for (arr) |t| {
                    if (!std.mem.eql(u8, t, "null")) return t;
                }
                return null;
            },
        }
    }

    /// Check if this type includes "null" (3.1 nullable style).
    pub fn includesNull(self: SchemaType) bool {
        switch (self) {
            .single => return false,
            .array => |arr| {
                for (arr) |t| {
                    if (std.mem.eql(u8, t, "null")) return true;
                }
                return false;
            },
        }
    }
};

pub const Schema = struct {
    schema_type: ?SchemaType = null,
    format: ?[]const u8 = null,
    title: ?[]const u8 = null,
    description: ?[]const u8 = null,
    enum_values: []const []const u8 = &.{},
    properties: std.StringArrayHashMapUnmanaged(SchemaOrRef) = .{},
    required: []const []const u8 = &.{},
    items: ?*const SchemaOrRef = null,
    all_of: []const SchemaOrRef = &.{},
    one_of: []const SchemaOrRef = &.{},
    any_of: []const SchemaOrRef = &.{},
    discriminator: ?Discriminator = null,
    /// 3.0 nullable flag. In 3.1+ this is expressed via type arrays.
    nullable: bool = false,
    additional_properties: ?AdditionalProperties = null,
    default_value: ?[]const u8 = null,
    /// 3.1+: const (single allowed value)
    const_value: ?[]const u8 = null,
    read_only: bool = false,
    write_only: bool = false,
    min_items: ?u64 = null,
    max_items: ?u64 = null,
    minimum: ?f64 = null,
    maximum: ?f64 = null,
    /// 3.1+: exclusive bounds are numbers, not booleans
    exclusive_minimum: ?f64 = null,
    exclusive_maximum: ?f64 = null,
    min_length: ?u64 = null,
    max_length: ?u64 = null,
    min_properties: ?u64 = null,
    max_properties: ?u64 = null,
    pattern: ?[]const u8 = null,
    /// 3.1+: content encoding for binary strings (e.g., "base64")
    content_encoding: ?[]const u8 = null,
    /// 3.1+: content media type (e.g., "image/png")
    content_media_type: ?[]const u8 = null,
    /// 3.1+: tuple validation (replaces items-as-array from JSON Schema)
    prefix_items: []const SchemaOrRef = &.{},
    // x-* extensions preserved as raw JSON strings
    extensions: std.StringArrayHashMapUnmanaged([]const u8) = .{},

    /// Returns true if this schema represents a nullable type, handling both
    /// 3.0 (`nullable: true`) and 3.1 (`type: ["string", "null"]`) styles.
    pub fn isNullable(self: *const Schema) bool {
        if (self.nullable) return true;
        if (self.schema_type) |st| return st.includesNull();
        return false;
    }

    /// Get the primary type string, handling both single and array forms.
    pub fn primaryType(self: *const Schema) ?[]const u8 {
        const st = self.schema_type orelse return null;
        return st.primaryType();
    }
};

pub const SchemaOrRef = union(enum) {
    schema: Schema,
    ref: Ref,
};

/// A $ref with optional sibling properties (3.1+ allows description/summary on $ref).
pub const Ref = struct {
    ref_string: []const u8,
    /// 3.1+: description override alongside $ref
    description: ?[]const u8 = null,
    /// 3.1+: summary override alongside $ref
    summary: ?[]const u8 = null,
};

pub const AdditionalProperties = union(enum) {
    boolean: bool,
    schema: *const SchemaOrRef,
};

pub const Discriminator = struct {
    property_name: []const u8,
    mapping: std.StringArrayHashMapUnmanaged([]const u8) = .{},
};

pub const Parameter = struct {
    name: []const u8,
    in: ParameterLocation,
    required: bool = false,
    description: ?[]const u8 = null,
    schema: ?SchemaOrRef = null,
    deprecated: bool = false,
};

pub const ParameterLocation = enum {
    path,
    query,
    header,
    cookie,

    pub fn fromString(s: []const u8) ?ParameterLocation {
        const map = std.StaticStringMap(ParameterLocation).initComptime(.{
            .{ "path", .path },
            .{ "query", .query },
            .{ "header", .header },
            .{ "cookie", .cookie },
        });
        return map.get(s);
    }
};

pub const ParameterOrRef = union(enum) {
    parameter: Parameter,
    ref: []const u8,
};

pub const RequestBody = struct {
    description: ?[]const u8 = null,
    required: bool = false,
    content: std.StringArrayHashMapUnmanaged(MediaType) = .{},
};

pub const RequestBodyOrRef = union(enum) {
    request_body: RequestBody,
    ref: []const u8,
};

pub const MediaType = struct {
    schema: ?SchemaOrRef = null,
};

pub const Response = struct {
    description: ?[]const u8 = null,
    content: std.StringArrayHashMapUnmanaged(MediaType) = .{},
    headers: std.StringArrayHashMapUnmanaged(SchemaOrRef) = .{},
};

pub const ResponseOrRef = union(enum) {
    response: Response,
    ref: []const u8,
};

pub const SecurityScheme = struct {
    type: []const u8,
    scheme: ?[]const u8 = null,
    name: ?[]const u8 = null,
    in: ?[]const u8 = null,
    bearer_format: ?[]const u8 = null,
};

pub const SecurityRequirement = struct {
    name: []const u8,
    scopes: []const []const u8 = &.{},
};

test "schema default" {
    const s = Schema{};
    try std.testing.expect(s.schema_type == null);
    try std.testing.expect(!s.nullable);
    try std.testing.expectEqual(@as(usize, 0), s.properties.count());
}

test "schema_or_ref ref" {
    const r = SchemaOrRef{ .ref = .{ .ref_string = "#/components/schemas/Foo" } };
    switch (r) {
        .ref => |ref| try std.testing.expectEqualStrings("#/components/schemas/Foo", ref.ref_string),
        .schema => unreachable,
    }
}

test "schema type array nullable" {
    const st = SchemaType{ .array = &.{ "string", "null" } };
    try std.testing.expect(st.includesNull());
    try std.testing.expectEqualStrings("string", st.primaryType().?);
}

test "schema isNullable 3.0 vs 3.1" {
    // 3.0 style
    const s30 = Schema{ .schema_type = .{ .single = "string" }, .nullable = true };
    try std.testing.expect(s30.isNullable());

    // 3.1 style
    const s31 = Schema{ .schema_type = .{ .array = &.{ "string", "null" } } };
    try std.testing.expect(s31.isNullable());

    // not nullable
    const s_nonnull = Schema{ .schema_type = .{ .single = "string" } };
    try std.testing.expect(!s_nonnull.isNullable());
}

test "spec version detection" {
    try std.testing.expectEqual(SpecVersion.v3_0, SpecVersion.parse("3.0.3"));
    try std.testing.expectEqual(SpecVersion.v3_1, SpecVersion.parse("3.1.0"));
    try std.testing.expectEqual(SpecVersion.v3_2, SpecVersion.parse("3.2.0"));
}
