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

//! Type code generation: OpenAPI schemas → Zig structs, enums, unions.

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const naming = @import("naming.zig");
const SourceWriter = @import("writer.zig").SourceWriter;
const Resolver = @import("resolver.zig").Resolver;

pub const TypeGenerator = struct {
    arena: Allocator,
    w: *SourceWriter,
    resolver: *Resolver,
    /// Maps external $ref file paths → Zig import module names.
    import_mapping: std.StringArrayHashMapUnmanaged([]const u8) = .{},
    /// Tracks which import modules were actually used during generation.
    used_imports: std.StringArrayHashMapUnmanaged(void) = .{},
    /// Extra top-level helper types emitted while generating named schema types.
    extra_type_reexports: std.ArrayListUnmanaged([]const u8) = .empty,
    /// All public Zig type identifiers reserved by components or generated
    /// helpers. OpenAPI component names are reserved before emission so helper
    /// naming is deterministic and independent of schema traversal order.
    reserved_type_names: std.StringHashMapUnmanaged(void) = .empty,
    /// Stable semantic helper key -> allocated public Zig type identifier.
    auxiliary_type_names: std.StringHashMapUnmanaged([]const u8) = .empty,
    optional_nullable_type_name: ?[]const u8 = null,
    raw_json_type_name: ?[]const u8 = null,
    uses_optional_nullable: bool = false,
    uses_raw_json: bool = false,
    uses_presence_aware_object: bool = false,
    type_names_initialized: bool = false,

    pub fn init(arena: Allocator, w: *SourceWriter, resolver: *Resolver) TypeGenerator {
        return .{ .arena = arena, .w = w, .resolver = resolver };
    }

    /// Generate all component schemas in stable lexical order.
    pub fn generateAll(self: *TypeGenerator, doc: *const types.OpenApiDoc) !void {
        const components = doc.components orelse return;
        const schemas = components.schemas;

        try self.initializeTypeNames(schemas.keys());
        const optional_nullable_type_name = try self.allocateAuxiliaryTypeName(
            "openapi-optional-nullable",
            "OpenApiOptionalNullable",
        );
        self.optional_nullable_type_name = optional_nullable_type_name;
        const raw_json_type_name = try self.allocateAuxiliaryTypeName(
            "openapi-raw-json",
            "OpenApiRawJson",
        );
        self.raw_json_type_name = raw_json_type_name;

        // Zig named declarations can reference later declarations, so schema
        // emission does not need dependency ordering. Keeping this lexical makes
        // checked-in generated files stable across runs and parser map layouts.
        const order = try sortedStringKeys(self.arena, schemas.keys());

        for (order) |name| {
            const sor = schemas.get(name) orelse continue;
            switch (sor) {
                .schema => |schema| {
                    try self.generateNamedType(name, schema);
                    try self.w.blank();
                },
                .ref => |ref| {
                    // Type alias for $ref at top level
                    const type_name = try naming.toTypeName(self.arena, name);
                    const target_name = try self.zigTypeForRef(ref.ref_string);
                    try self.w.line("pub const {s} = {s};", .{ type_name, target_name });
                    try self.w.blank();
                },
            }
        }
        if (self.uses_optional_nullable) {
            try self.emitOptionalNullableType(optional_nullable_type_name);
            try self.w.blank();
        }
        if (self.uses_raw_json) {
            try self.emitRawJsonType(raw_json_type_name);
            try self.w.blank();
        }
        if (self.uses_presence_aware_object) {
            try self.emitPresenceAwareObjectHelpers();
            try self.w.blank();
        }
    }

    /// A syntactically validated JSON subtree retained as compact owned bytes.
    /// Parsing copies tokens directly into the final buffer instead of building
    /// a std.json.Value DOM that callers must stringify before semantic parsing.
    fn emitRawJsonType(self: *TypeGenerator, type_name: []const u8) !void {
        try self.w.line("/// Syntactically validated JSON retained as compact owned bytes.", .{});
        try self.w.line("pub const {s} = struct {{", .{type_name});
        self.w.indent();
        try self.w.line("bytes: []const u8,", .{});
        try self.w.blank();
        try self.w.line("pub fn jsonParse(allocator: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {{", .{});
        self.w.indent();
        try self.w.line("var output: std.Io.Writer.Allocating = .init(allocator);", .{});
        try self.w.line("errdefer output.deinit();", .{});
        try self.w.line("var stringify: std.json.Stringify = .{{ .writer = &output.writer }};", .{});
        try self.w.line("copyValue(allocator, source, options, &stringify) catch |err| switch (err) {{", .{});
        self.w.indent();
        try self.w.line("error.WriteFailed => return error.OutOfMemory,", .{});
        try self.w.line("else => |parse_err| return parse_err,", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.line("return .{{ .bytes = try output.toOwnedSlice() }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();
        try self.w.line("pub fn jsonParseFromValue(allocator: std.mem.Allocator, source: std.json.Value, _: std.json.ParseOptions) !@This() {{", .{});
        self.w.indent();
        try self.w.line("return .{{ .bytes = try std.json.Stringify.valueAlloc(allocator, source, .{{}}) }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();
        try self.w.line("pub fn jsonStringify(self: @This(), jw: anytype) !void {{", .{});
        self.w.indent();
        try self.w.line("try jw.beginWriteRaw();", .{});
        try self.w.line("try jw.writer.writeAll(self.bytes);", .{});
        try self.w.line("jw.endWriteRaw();", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();
        try self.w.line("fn copyValue(allocator: std.mem.Allocator, source: anytype, options: std.json.ParseOptions, jw: *std.json.Stringify) !void {{", .{});
        self.w.indent();
        try self.w.line("switch (try source.peekNextTokenType()) {{", .{});
        self.w.indent();
        try self.w.line(".object_begin => {{", .{});
        self.w.indent();
        try self.w.line("_ = try source.next();", .{});
        try self.w.line("try jw.beginObject();", .{});
        try self.w.line("while (try source.peekNextTokenType() != .object_end) {{", .{});
        self.w.indent();
        try self.w.line("const token = try source.nextAllocMax(allocator, .alloc_if_needed, options.max_value_len.?);", .{});
        try self.w.line("switch (token) {{", .{});
        self.w.indent();
        try self.w.line(".string => |name| try jw.objectField(name),", .{});
        try self.w.line(".allocated_string => |name| {{ defer allocator.free(name); try jw.objectField(name); }},", .{});
        try self.w.line("else => return error.UnexpectedToken,", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.line("try copyValue(allocator, source, options, jw);", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.line("_ = try source.next();", .{});
        try self.w.line("try jw.endObject();", .{});
        self.w.dedent();
        try self.w.line("}},", .{});
        try self.w.line(".array_begin => {{", .{});
        self.w.indent();
        try self.w.line("_ = try source.next();", .{});
        try self.w.line("try jw.beginArray();", .{});
        try self.w.line("while (try source.peekNextTokenType() != .array_end) try copyValue(allocator, source, options, jw);", .{});
        try self.w.line("_ = try source.next();", .{});
        try self.w.line("try jw.endArray();", .{});
        self.w.dedent();
        try self.w.line("}},", .{});
        try self.w.line(".string => switch (try source.nextAllocMax(allocator, .alloc_if_needed, options.max_value_len.?)) {{", .{});
        self.w.indent();
        try self.w.line(".string => |value| try jw.write(value),", .{});
        try self.w.line(".allocated_string => |value| {{ defer allocator.free(value); try jw.write(value); }},", .{});
        try self.w.line("else => unreachable,", .{});
        self.w.dedent();
        try self.w.line("}},", .{});
        try self.w.line(".number => switch (try source.nextAllocMax(allocator, .alloc_if_needed, options.max_value_len.?)) {{", .{});
        self.w.indent();
        try self.w.line(".number => |value| try writeNumber(jw, value),", .{});
        try self.w.line(".allocated_number => |value| {{ defer allocator.free(value); try writeNumber(jw, value); }},", .{});
        try self.w.line("else => unreachable,", .{});
        self.w.dedent();
        try self.w.line("}},", .{});
        try self.w.line(".true => {{ _ = try source.next(); try jw.write(true); }},", .{});
        try self.w.line(".false => {{ _ = try source.next(); try jw.write(false); }},", .{});
        try self.w.line(".null => {{ _ = try source.next(); try jw.write(null); }},", .{});
        try self.w.line(".object_end, .array_end, .end_of_document => return error.UnexpectedToken,", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();
        try self.w.line("fn writeNumber(jw: *std.json.Stringify, value: []const u8) !void {{", .{});
        self.w.indent();
        try self.w.line("try jw.beginWriteRaw();", .{});
        try self.w.line("try jw.writer.writeAll(value);", .{});
        try self.w.line("jw.endWriteRaw();", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
    }

    fn emitOptionalNullableType(self: *TypeGenerator, type_name: []const u8) !void {
        try self.w.line("/// Presence-aware representation of an optional OpenAPI property that also permits JSON null.", .{});
        try self.w.line("pub fn {s}(comptime T: type) type {{", .{type_name});
        self.w.indent();
        try self.w.line("return union(enum) {{", .{});
        self.w.indent();
        try self.w.line("absent,", .{});
        try self.w.line("null_value,", .{});
        try self.w.line("value: T,", .{});
        try self.w.blank();
        try self.w.line("pub fn fromNullable(value: ?T) @This() {{", .{});
        self.w.indent();
        try self.w.line("return if (value) |item| .{{ .value = item }} else .null_value;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();
        try self.w.line("pub fn isPresent(self: @This()) bool {{", .{});
        self.w.indent();
        try self.w.line("return self != .absent;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();
        try self.w.line("pub fn valueOrNull(self: @This()) ?T {{", .{});
        self.w.indent();
        try self.w.line("return switch (self) {{", .{});
        self.w.indent();
        try self.w.line(".absent, .null_value => null,", .{});
        try self.w.line(".value => |item| item,", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();
        try self.w.line("pub fn jsonParse(allocator: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {{", .{});
        self.w.indent();
        try self.w.line("if (try source.peekNextTokenType() == .null) {{", .{});
        self.w.indent();
        try self.w.line("_ = try source.next();", .{});
        try self.w.line("return .null_value;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.line("return .{{ .value = try std.json.innerParse(T, allocator, source, options) }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();
        try self.w.line("pub fn jsonParseFromValue(allocator: std.mem.Allocator, source: std.json.Value, options: std.json.ParseOptions) !@This() {{", .{});
        self.w.indent();
        try self.w.line("if (source == .null) return .null_value;", .{});
        try self.w.line("return .{{ .value = try std.json.parseFromValueLeaky(T, allocator, source, options) }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();
        try self.w.line("pub fn jsonStringify(self: @This(), jw: anytype) !void {{", .{});
        self.w.indent();
        try self.w.line("switch (self) {{", .{});
        self.w.indent();
        try self.w.line(".absent => return error.OptionalNullablePropertyAbsent,", .{});
        try self.w.line(".null_value => try jw.write(@as(?u8, null)),", .{});
        try self.w.line(".value => |value| try jw.write(value),", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
    }

    fn emitPresenceAwareObjectHelpers(self: *TypeGenerator) !void {
        try self.w.line("/// Parse an OpenAPI object without materializing a second JSON tree while", .{});
        try self.w.line("/// rejecting explicit null for optional properties whose schemas are non-nullable.", .{});
        try self.w.line("fn openApiParseObject(", .{});
        self.w.indent();
        try self.w.line("comptime T: type,", .{});
        try self.w.line("comptime openapi_fields: anytype,", .{});
        try self.w.line("allocator: std.mem.Allocator,", .{});
        try self.w.line("source: anytype,", .{});
        try self.w.line("options: std.json.ParseOptions,", .{});
        self.w.dedent();
        try self.w.line(") !T {{", .{});
        self.w.indent();
        try self.w.line("@setEvalBranchQuota(100_000);", .{});
        try self.w.line("const struct_info = @typeInfo(T).@\"struct\";", .{});
        try self.w.line("if (struct_info.is_tuple) @compileError(\"OpenAPI object parser does not accept tuples\");", .{});
        try self.w.line("if (openapi_fields.len != struct_info.fields.len) @compileError(\"OpenAPI object field descriptors must match the generated struct\");", .{});
        try self.w.line("if (.object_begin != try source.next()) return error.UnexpectedToken;", .{});
        try self.w.blank();
        try self.w.line("var result: T = undefined;", .{});
        try self.w.line("var fields_seen = [_]bool{{false}} ** struct_info.fields.len;", .{});
        try self.w.line("while (true) {{", .{});
        self.w.indent();
        try self.w.line("var name_token: ?std.json.Token = try source.nextAllocMax(allocator, .alloc_if_needed, options.max_value_len.?);", .{});
        try self.w.line("const field_name = switch (name_token.?) {{", .{});
        self.w.indent();
        try self.w.line("inline .string, .allocated_string => |slice| slice,", .{});
        try self.w.line(".object_end => break,", .{});
        try self.w.line("else => return error.UnexpectedToken,", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.blank();
        try self.w.line("inline for (struct_info.fields, openapi_fields, 0..) |field, openapi_field, i| {{", .{});
        self.w.indent();
        try self.w.line("if (field.is_comptime) @compileError(\"comptime fields are not supported: \" ++ @typeName(T) ++ \".\" ++ field.name);", .{});
        try self.w.line("if (comptime !std.mem.eql(u8, field.name, openapi_field[1])) @compileError(\"OpenAPI object field descriptor order does not match the generated struct\");", .{});
        try self.w.line("if (std.mem.eql(u8, openapi_field[0], field_name)) {{", .{});
        self.w.indent();
        try self.w.line("openApiFreeAllocatedToken(allocator, name_token.?);", .{});
        try self.w.line("name_token = null;", .{});
        try self.w.line("if (openapi_field[2] and try source.peekNextTokenType() == .null) return error.UnexpectedToken;", .{});
        try self.w.line("if (fields_seen[i]) {{", .{});
        self.w.indent();
        try self.w.line("switch (options.duplicate_field_behavior) {{", .{});
        self.w.indent();
        try self.w.line(".use_first => {{", .{});
        self.w.indent();
        try self.w.line("_ = try std.json.innerParse(field.type, allocator, source, options);", .{});
        try self.w.line("break;", .{});
        self.w.dedent();
        try self.w.line("}},", .{});
        try self.w.line(".@\"error\" => return error.DuplicateField,", .{});
        try self.w.line(".use_last => {{}},", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.line("@field(result, field.name) = try std.json.innerParse(field.type, allocator, source, options);", .{});
        try self.w.line("fields_seen[i] = true;", .{});
        try self.w.line("break;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}} else {{", .{});
        self.w.indent();
        try self.w.line("openApiFreeAllocatedToken(allocator, name_token.?);", .{});
        try self.w.line("if (options.ignore_unknown_fields) try source.skipValue() else return error.UnknownField;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.line("try openApiFillDefaultStructValues(T, openapi_fields, &result, &fields_seen);", .{});
        try self.w.line("return result;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();

        try self.w.line("fn openApiParseObjectFromValue(", .{});
        self.w.indent();
        try self.w.line("comptime T: type,", .{});
        try self.w.line("comptime openapi_fields: anytype,", .{});
        try self.w.line("allocator: std.mem.Allocator,", .{});
        try self.w.line("source: std.json.Value,", .{});
        try self.w.line("options: std.json.ParseOptions,", .{});
        self.w.dedent();
        try self.w.line(") !T {{", .{});
        self.w.indent();
        try self.w.line("@setEvalBranchQuota(100_000);", .{});
        try self.w.line("const struct_info = @typeInfo(T).@\"struct\";", .{});
        try self.w.line("if (struct_info.is_tuple) @compileError(\"OpenAPI object parser does not accept tuples\");", .{});
        try self.w.line("if (openapi_fields.len != struct_info.fields.len) @compileError(\"OpenAPI object field descriptors must match the generated struct\");", .{});
        try self.w.line("if (source != .object) return error.UnexpectedToken;", .{});
        try self.w.line("var result: T = undefined;", .{});
        try self.w.line("var fields_seen = [_]bool{{false}} ** struct_info.fields.len;", .{});
        try self.w.line("var it = source.object.iterator();", .{});
        try self.w.line("while (it.next()) |entry| {{", .{});
        self.w.indent();
        try self.w.line("const field_name = entry.key_ptr.*;", .{});
        try self.w.line("inline for (struct_info.fields, openapi_fields, 0..) |field, openapi_field, i| {{", .{});
        self.w.indent();
        try self.w.line("if (field.is_comptime) @compileError(\"comptime fields are not supported: \" ++ @typeName(T) ++ \".\" ++ field.name);", .{});
        try self.w.line("if (comptime !std.mem.eql(u8, field.name, openapi_field[1])) @compileError(\"OpenAPI object field descriptor order does not match the generated struct\");", .{});
        try self.w.line("if (std.mem.eql(u8, openapi_field[0], field_name)) {{", .{});
        self.w.indent();
        try self.w.line("if (openapi_field[2] and entry.value_ptr.* == .null) return error.UnexpectedToken;", .{});
        try self.w.line("@field(result, field.name) = try std.json.innerParseFromValue(field.type, allocator, entry.value_ptr.*, options);", .{});
        try self.w.line("fields_seen[i] = true;", .{});
        try self.w.line("break;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}} else if (!options.ignore_unknown_fields) return error.UnknownField;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.line("try openApiFillDefaultStructValues(T, openapi_fields, &result, &fields_seen);", .{});
        try self.w.line("return result;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();

        try self.w.line("fn openApiFillDefaultStructValues(comptime T: type, comptime openapi_fields: anytype, result: *T, fields_seen: *[@typeInfo(T).@\"struct\".fields.len]bool) !void {{", .{});
        self.w.indent();
        try self.w.line("@setEvalBranchQuota(100_000);", .{});
        try self.w.line("inline for (@typeInfo(T).@\"struct\".fields, openapi_fields, 0..) |field, openapi_field, i| {{", .{});
        self.w.indent();
        try self.w.line("if (comptime !std.mem.eql(u8, field.name, openapi_field[1])) @compileError(\"OpenAPI object field descriptor order does not match the generated struct\");", .{});
        try self.w.line("if (!fields_seen[i]) {{", .{});
        self.w.indent();
        try self.w.line("if (field.defaultValue()) |default| @field(result, field.name) = default else return error.MissingField;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();
        try self.w.line("fn openApiFreeAllocatedToken(allocator: std.mem.Allocator, token: std.json.Token) void {{", .{});
        self.w.indent();
        try self.w.line("switch (token) {{", .{});
        self.w.indent();
        try self.w.line(".allocated_number, .allocated_string => |slice| allocator.free(slice),", .{});
        try self.w.line("else => {{}},", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
    }

    fn sortedStringKeys(arena: Allocator, keys: []const []const u8) ![]const []const u8 {
        const sorted = try arena.dupe([]const u8, keys);
        std.sort.pdq([]const u8, sorted, {}, stringLessThan);
        return sorted;
    }

    fn stringLessThan(_: void, left: []const u8, right: []const u8) bool {
        return std.mem.order(u8, left, right) == .lt;
    }

    /// Generate a named type from a schema.
    fn generateNamedType(self: *TypeGenerator, name: []const u8, schema: types.Schema) !void {
        const type_name = try naming.toTypeName(self.arena, name);

        // Some schemas deliberately retain arbitrary validated JSON instead of
        // exposing their referenced union as a generated Zig type. Honor that
        // intent before composition handling so a transparent oneOf wrapper
        // cannot accidentally turn a forward-compatible raw surface into a
        // best-effort structural union.
        if (try self.zigTypeOverride(schema)) |override| {
            if (schema.description) |desc| try self.w.docComment(desc);
            try self.w.line("pub const {s} = {s};", .{ type_name, override });
            return;
        }

        // A nullable oneOf already names its concrete payload, so keep the
        // compact alias instead of introducing a redundant FooValue wrapper.
        if (try self.nullableOneOfType(schema.one_of)) |inner_type| {
            if (schema.description) |desc| try self.w.docComment(desc);
            try self.w.line("pub const {s} = ?{s};", .{ type_name, inner_type });
            return;
        }

        // Component nullability is part of the named schema, not a property of
        // whichever document happens to reference it. Preserve that fact in
        // the generated Zig type so local aliases, array/map values, and mapped
        // external modules all agree. A separately named payload keeps enum and
        // object namespaces usable while the public component remains optional.
        if (schema.isNullable()) {
            const preferred_payload_name = try std.fmt.allocPrint(self.arena, "{s}Value", .{type_name});
            const payload_key = try std.fmt.allocPrint(self.arena, "nullable-payload:{s}", .{type_name});
            const payload_type_name = try self.allocateAuxiliaryTypeName(payload_key, preferred_payload_name);
            var payload_schema = try self.nonNullableSchema(schema);
            payload_schema.description = null;
            try self.generateNamedType(payload_type_name, payload_schema);
            try self.extra_type_reexports.append(self.arena, payload_type_name);
            try self.w.blank();
            if (schema.description) |desc| try self.w.docComment(desc);
            try self.w.line("pub const {s} = ?{s};", .{ type_name, payload_type_name });
            return;
        }

        // OpenAPI 3.1 type arrays can contain more than one non-null JSON type.
        // A single Zig primitive would silently narrow the accepted wire
        // domain, so retain the value losslessly until a first-class generated
        // union representation exists.
        if (schemaHasMultipleNonNullTypes(schema)) {
            if (schema.description) |desc| try self.w.docComment(desc);
            try self.w.line("pub const {s} = std.json.Value;", .{type_name});
            return;
        }

        // String enum
        if (schema.enum_values.len > 0) {
            try self.generateEnum(type_name, schema);
            return;
        }

        // allOf: merge into single struct
        if (schema.all_of.len > 0) {
            try self.generateAllOfStruct(type_name, schema);
            return;
        }

        // oneOf with discriminator: union(enum)
        if (schema.one_of.len > 0 and schema.discriminator != null) {
            try self.generateDiscriminatedUnion(type_name, schema);
            return;
        }

        // oneOf/anyOf without discriminator but with top-level properties:
        // emit one flattened object struct. These schemas model a single JSON
        // object with common fields plus variant-specific fields, for example
        // index configs keyed by a "type" field. Emitting only the common
        // fields would make request serialization lossy.
        if ((schema.one_of.len > 0 or schema.any_of.len > 0) and schema.properties.count() > 0) {
            try self.generateFlattenedUnionObjectStruct(type_name, schema);
            return;
        }

        // oneOf without discriminator but with object-like referenced variants:
        // emit a best-effort structural union(enum).
        if (schema.one_of.len > 0 and schema.any_of.len == 0 and try self.canGenerateStructuralUnion(schema)) {
            try self.generateStructuralUnion(type_name, schema);
            return;
        }

        // oneOf without discriminator or anyOf: opaque Value
        if (schema.one_of.len > 0 or schema.any_of.len > 0) {
            if (schema.description) |desc| try self.w.docComment(desc);
            try self.w.line("pub const {s} = std.json.Value;", .{type_name});
            return;
        }

        // A named free-form object must retain its arbitrary JSON fields while
        // preserving the schema's object-only wire domain. Raw std.json.Value
        // is reserved for deliberately untyped or multi-type schemas because
        // it would also accept arrays, scalars, and null here.
        if (schema.properties.count() == 0 and
            (schema.additional_properties != null or
                (schema.primaryType() != null and std.mem.eql(u8, schema.primaryType().?, "object"))))
        {
            if (schema.additional_properties) |additional_properties| switch (additional_properties) {
                .boolean => |allowed| if (allowed) {
                    if (schema.description) |desc| try self.w.docComment(desc);
                    try self.w.line("pub const {s} = std.json.ArrayHashMap(std.json.Value);", .{type_name});
                    return;
                },
                .schema => {
                    if (schema.description) |desc| try self.w.docComment(desc);
                    const zig_type = try self.zigTypeForSchema(schema);
                    try self.w.line("pub const {s} = {s};", .{ type_name, zig_type });
                    return;
                },
            } else {
                // JSON Schema and OpenAPI allow additional properties by
                // default. A property-free object without an explicit closed
                // marker is therefore an arbitrary-key object, not `{}` and
                // not an unconstrained JSON value.
                if (schema.description) |desc| try self.w.docComment(desc);
                try self.w.line("pub const {s} = std.json.ArrayHashMap(std.json.Value);", .{type_name});
                return;
            }
        }

        // Object with properties → struct
        const primary_type = schema.primaryType();
        if (schema.properties.count() > 0 or
            (primary_type != null and std.mem.eql(u8, primary_type.?, "object")))
        {
            try self.generateStruct(type_name, schema);
            return;
        }

        // Simple type alias
        const zig_type = try self.zigTypeForSchema(schema);
        if (schema.description) |desc| try self.w.docComment(desc);
        try self.w.line("pub const {s} = {s};", .{ type_name, zig_type });
    }

    fn nonNullableSchema(self: *TypeGenerator, schema: types.Schema) !types.Schema {
        var payload = schema;
        payload.nullable = false;
        payload.enum_has_null = false;
        if (schema.schema_type) |schema_type| switch (schema_type) {
            .single => {},
            .array => |members| {
                var non_null_count: usize = 0;
                for (members) |member| {
                    if (!std.mem.eql(u8, member, "null")) non_null_count += 1;
                }
                if (non_null_count == 0) return error.InvalidOpenApiSchema;
                const non_null_members = try self.arena.alloc([]const u8, non_null_count);
                var next: usize = 0;
                for (members) |member| {
                    if (std.mem.eql(u8, member, "null")) continue;
                    non_null_members[next] = member;
                    next += 1;
                }
                payload.schema_type = if (non_null_members.len == 1)
                    .{ .single = non_null_members[0] }
                else
                    .{ .array = non_null_members };
            },
        };
        return payload;
    }

    fn schemaHasMultipleNonNullTypes(schema: types.Schema) bool {
        const schema_type = schema.schema_type orelse return false;
        return switch (schema_type) {
            .single => false,
            .array => |members| blk: {
                var count: usize = 0;
                for (members) |member| {
                    if (std.mem.eql(u8, member, "null")) continue;
                    count += 1;
                    if (count > 1) break :blk true;
                }
                break :blk false;
            },
        };
    }

    fn zigTypeOverride(self: *TypeGenerator, schema: types.Schema) error{InvalidOpenApiSchema}!?[]const u8 {
        const override = schema.extensions.get("x-zig-type") orelse return null;
        if (std.mem.eql(u8, override, "std.json.Value")) return override;
        if (std.mem.eql(u8, override, "raw_json")) {
            self.uses_raw_json = true;
            return self.raw_json_type_name orelse return error.InvalidOpenApiSchema;
        }
        return error.InvalidOpenApiSchema;
    }

    fn initializeTypeNames(self: *TypeGenerator, schema_names: []const []const u8) !void {
        if (self.type_names_initialized) return;
        for (schema_names) |schema_name| {
            const type_name = try naming.toTypeName(self.arena, schema_name);
            const entry = try self.reserved_type_names.getOrPut(self.arena, type_name);
            if (entry.found_existing) return error.InvalidOpenApiSchema;
            entry.value_ptr.* = {};
        }
        self.type_names_initialized = true;
    }

    fn allocateAuxiliaryTypeName(
        self: *TypeGenerator,
        semantic_key: []const u8,
        preferred: []const u8,
    ) ![]const u8 {
        if (self.auxiliary_type_names.get(semantic_key)) |existing| return existing;

        var candidate = preferred;
        var suffix: usize = 2;
        while (self.reserved_type_names.contains(candidate)) : (suffix += 1) {
            candidate = try std.fmt.allocPrint(self.arena, "{s}{d}", .{ preferred, suffix });
        }
        try self.reserved_type_names.put(self.arena, candidate, {});
        try self.auxiliary_type_names.put(self.arena, semantic_key, candidate);
        return candidate;
    }

    /// Generate a Zig enum from a string enum schema.
    fn generateEnum(self: *TypeGenerator, type_name: []const u8, schema: types.Schema) !void {
        if (schema.description) |desc| try self.w.docComment(desc);
        try self.w.line("pub const {s} = enum {{", .{type_name});
        self.w.indent();

        // Pre-compute field names once for all three passes
        const fields = try self.arena.alloc([]u8, schema.enum_values.len);
        for (schema.enum_values, 0..) |val, i| {
            fields[i] = try naming.zigFieldName(self.arena, val);
        }

        for (fields) |field| {
            try self.w.line("{s},", .{field});
        }

        try self.w.blank();

        // jsonStringify: emit the original string value
        try self.w.line("pub fn jsonStringify(self: @This(), jw: anytype) !void {{", .{});
        self.w.indent();
        try self.w.line("const s = switch (self) {{", .{});
        self.w.indent();
        for (schema.enum_values, 0..) |val, i| {
            try self.w.line(".{s} => \"{f}\",", .{ fields[i], std.zig.fmtString(val) });
        }
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.line("try jw.write(s);", .{});
        self.w.dedent();
        try self.w.line("}}", .{});

        try self.w.blank();

        // jsonParse: parse from string
        try self.w.line("pub fn jsonParse(_: std.mem.Allocator, source: anytype, _: std.json.ParseOptions) !@This() {{", .{});
        self.w.indent();
        try self.w.line("const s = switch (try source.next()) {{", .{});
        self.w.indent();
        try self.w.line(".string => |v| v,", .{});
        try self.w.line("else => return error.UnexpectedToken,", .{});
        self.w.dedent();
        try self.w.line("}};", .{});

        // Use a StaticStringMap for lookup
        try self.w.line("const map = std.StaticStringMap(@This()).initComptime(.{{", .{});
        self.w.indent();
        for (schema.enum_values, 0..) |val, i| {
            try self.w.line(".{{ \"{f}\", .{s} }},", .{ std.zig.fmtString(val), fields[i] });
        }
        self.w.dedent();
        try self.w.line("}});", .{});
        try self.w.line("return map.get(s) orelse error.UnexpectedToken;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});

        self.w.dedent();
        try self.w.line("}};", .{});
    }

    /// Generate a struct from an object schema.
    fn generateStruct(self: *TypeGenerator, type_name: []const u8, schema: types.Schema) !void {
        try self.emitInlineEnumTypesForProperties(type_name, schema);

        if (schema.description) |desc| try self.w.docComment(desc);
        try self.w.line("pub const {s} = struct {{", .{type_name});
        self.w.indent();

        // Build required set once for O(1) lookups
        var required_set = std.StringArrayHashMapUnmanaged(void){};
        for (schema.required) |r| try required_set.put(self.arena, r, {});

        for (schema.properties.keys(), schema.properties.values()) |prop_name, prop_sor| {
            try self.emitStructField(type_name, prop_name, prop_sor, required_set.contains(prop_name));
        }

        var strict_optional_fields = std.StringArrayHashMapUnmanaged(void){};
        try self.collectNonNullableOptionalFields(schema, &required_set, true, &strict_optional_fields);
        try self.emitPresenceAwareObjectParsers(
            schema.properties.keys(),
            &strict_optional_fields,
            self.schemaNeedsRequiredFieldPresence(schema, &required_set, true),
        );

        // Request serialization normally omits null optional fields. Generate a
        // required-aware serializer when a required field permits null or its
        // nullability belongs to an external schema we cannot inspect. Presence
        // remains part of the containing object's wire contract either way.
        if (strict_optional_fields.count() > 0 or
            self.schemaNeedsRequiredFieldSerializer(schema, &required_set, true))
        {
            try self.w.blank();
            try self.w.line("pub fn jsonStringify(self: @This(), jw: anytype) !void {{", .{});
            self.w.indent();
            try self.w.line("try jw.beginObject();", .{});
            var emitted_json_props = std.StringArrayHashMapUnmanaged(void){};
            try self.emitFlattenedSchemaJsonStringifyFields(schema, &emitted_json_props, &required_set, true, true);
            try self.w.line("try jw.endObject();", .{});
            self.w.dedent();
            try self.w.line("}}", .{});
        }

        self.w.dedent();
        try self.w.line("}};", .{});
    }

    fn emitInlineEnumTypesForProperties(self: *TypeGenerator, owner_type_name: []const u8, schema: types.Schema) !void {
        for (schema.properties.keys(), schema.properties.values()) |prop_name, prop_sor| {
            const enum_schema = inlineEnumSchema(prop_name, prop_sor) orelse continue;
            const enum_type_name = try self.inlineEnumTypeName(owner_type_name, prop_name);
            try self.generateEnum(enum_type_name, enum_schema);
            try self.extra_type_reexports.append(self.arena, enum_type_name);
            try self.w.blank();
        }
    }

    fn emitFlattenedInlineEnumTypes(
        self: *TypeGenerator,
        owner_type_name: []const u8,
        schema: types.Schema,
        emitted_props: *std.StringArrayHashMapUnmanaged(void),
    ) !void {
        for (schema.properties.keys(), schema.properties.values()) |prop_name, prop_sor| {
            if (emitted_props.contains(prop_name)) continue;
            try emitted_props.put(self.arena, prop_name, {});

            const enum_schema = inlineEnumSchema(prop_name, prop_sor) orelse continue;
            const enum_type_name = try self.inlineEnumTypeName(owner_type_name, prop_name);
            try self.generateEnum(enum_type_name, enum_schema);
            try self.extra_type_reexports.append(self.arena, enum_type_name);
            try self.w.blank();
        }

        for (schema.all_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.emitFlattenedInlineEnumTypes(owner_type_name, resolved, emitted_props);
        }

        for (schema.one_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.emitFlattenedInlineEnumTypes(owner_type_name, resolved, emitted_props);
        }

        for (schema.any_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.emitFlattenedInlineEnumTypes(owner_type_name, resolved, emitted_props);
        }
    }

    fn inlineEnumSchema(prop_name: []const u8, prop_sor: types.SchemaOrRef) ?types.Schema {
        if (!std.mem.eql(u8, prop_name, "index_type")) return null;
        return switch (prop_sor) {
            .schema => |schema| if (schema.enum_values.len == 1) schema else null,
            .ref => null,
        };
    }

    fn inlineEnumTypeName(self: *TypeGenerator, owner_type_name: []const u8, prop_name: []const u8) ![]const u8 {
        const prop_type_name = try naming.toTypeName(self.arena, prop_name);
        const preferred = try std.fmt.allocPrint(self.arena, "{s}{s}", .{ owner_type_name, prop_type_name });
        const semantic_key = try std.fmt.allocPrint(
            self.arena,
            "inline-enum:{s}:{s}",
            .{ owner_type_name, prop_name },
        );
        return self.allocateAuxiliaryTypeName(semantic_key, preferred);
    }

    fn zigTypeForStructField(self: *TypeGenerator, owner_type_name: []const u8, prop_name: []const u8, prop_sor: types.SchemaOrRef) ![]const u8 {
        if (inlineEnumSchema(prop_name, prop_sor) != null) {
            return self.inlineEnumTypeName(owner_type_name, prop_name);
        }
        return self.zigTypeForSchemaOrRef(prop_sor);
    }

    /// Emit a single struct field with doc comment and optional/required handling.
    fn emitStructField(self: *TypeGenerator, owner_type_name: []const u8, prop_name: []const u8, prop_sor: types.SchemaOrRef, is_required: bool) !void {
        const field = try naming.zigFieldName(self.arena, prop_name);
        const zig_type = try self.zigTypeForStructField(owner_type_name, prop_name, prop_sor);

        // Add description as doc comment (3.1+ allows description on $ref too)
        switch (prop_sor) {
            .schema => |s| {
                if (s.description) |desc| try self.w.docComment(desc);
            },
            .ref => |ref| {
                if (ref.description) |desc| try self.w.docComment(desc);
            },
        }

        const representation = self.schemaOrRefRepresentation(prop_sor);

        if (is_required) {
            if (representation.nullability == .nullable and
                representation.null_representation == .none)
            {
                try self.w.line("{s}: ?{s},", .{ field, zig_type });
            } else {
                try self.w.line("{s}: {s},", .{ field, zig_type });
            }
        } else if (representation.nullability == .nullable) {
            const wrapper = self.optional_nullable_type_name orelse return error.InvalidOpenApiSchema;
            self.uses_optional_nullable = true;
            const value_type = if (representation.null_representation == .zig_optional)
                try std.fmt.allocPrint(self.arena, "std.meta.Child({s})", .{zig_type})
            else
                zig_type;
            try self.w.line("{s}: {s}({s}) = .absent,", .{ field, wrapper, value_type });
        } else if (representation.null_representation == .zig_optional) {
            try self.w.line("{s}: {s} = null,", .{ field, zig_type });
        } else {
            try self.w.line("{s}: ?{s} = null,", .{ field, zig_type });
        }
    }

    /// Generate a struct from allOf by merging all properties.
    fn generateAllOfStruct(self: *TypeGenerator, type_name: []const u8, schema: types.Schema) !void {
        var emitted_inline_enums = std.StringArrayHashMapUnmanaged(void){};
        for (schema.all_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.emitFlattenedInlineEnumTypes(type_name, resolved, &emitted_inline_enums);
        }
        try self.emitFlattenedInlineEnumTypes(type_name, schema, &emitted_inline_enums);

        if (schema.description) |desc| try self.w.docComment(desc);
        try self.w.line("pub const {s} = struct {{", .{type_name});
        self.w.indent();

        // Collect all properties from allOf members, deduplicating by name
        var all_required = std.StringArrayHashMapUnmanaged(void){};
        for (schema.required) |r| {
            try all_required.put(self.arena, r, {});
        }

        var emitted_props = std.StringArrayHashMapUnmanaged(void){};

        for (schema.all_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;

            for (resolved.required) |r| {
                try all_required.put(self.arena, r, {});
            }
            try self.emitFlattenedSchemaProperties(type_name, resolved, &emitted_props, &all_required, true);
        }

        // Also include any direct properties on the schema itself
        try self.emitFlattenedSchemaProperties(type_name, schema, &emitted_props, &all_required, true);

        var strict_optional_fields = std.StringArrayHashMapUnmanaged(void){};
        try self.collectNonNullableOptionalFields(schema, &all_required, true, &strict_optional_fields);
        try self.emitPresenceAwareObjectParsers(
            emitted_props.keys(),
            &strict_optional_fields,
            self.schemaNeedsRequiredFieldPresence(schema, &all_required, true),
        );

        if (strict_optional_fields.count() > 0 or
            self.schemaNeedsRequiredFieldSerializer(schema, &all_required, true))
        {
            try self.w.blank();
            try self.w.line("pub fn jsonStringify(self: @This(), jw: anytype) !void {{", .{});
            self.w.indent();
            try self.w.line("try jw.beginObject();", .{});
            var emitted_json_props = std.StringArrayHashMapUnmanaged(void){};
            try self.emitFlattenedSchemaJsonStringifyFields(schema, &emitted_json_props, &all_required, true, true);
            try self.w.line("try jw.endObject();", .{});
            self.w.dedent();
            try self.w.line("}}", .{});
        }

        self.w.dedent();
        try self.w.line("}};", .{});
    }

    /// Generate one struct from a JSON object schema that has common top-level
    /// properties plus oneOf/anyOf object variants. Variant properties are
    /// optional because only one variant is expected to be populated.
    fn generateFlattenedUnionObjectStruct(self: *TypeGenerator, type_name: []const u8, schema: types.Schema) !void {
        var emitted_inline_enums = std.StringArrayHashMapUnmanaged(void){};
        try self.emitFlattenedInlineEnumTypes(type_name, schema, &emitted_inline_enums);

        if (schema.description) |desc| try self.w.docComment(desc);
        try self.w.line("pub const {s} = struct {{", .{type_name});
        self.w.indent();

        var all_required = std.StringArrayHashMapUnmanaged(void){};
        for (schema.required) |r| {
            try all_required.put(self.arena, r, {});
        }

        var emitted_props = std.StringArrayHashMapUnmanaged(void){};
        try self.emitFlattenedSchemaProperties(type_name, schema, &emitted_props, &all_required, true);

        var strict_optional_fields = std.StringArrayHashMapUnmanaged(void){};
        try self.collectNonNullableOptionalFields(schema, &all_required, true, &strict_optional_fields);
        try self.emitPresenceAwareObjectParsers(
            emitted_props.keys(),
            &strict_optional_fields,
            self.schemaNeedsRequiredFieldPresence(schema, &all_required, true),
        );

        try self.w.blank();
        try self.w.line("pub fn jsonStringify(self: @This(), jw: anytype) !void {{", .{});
        self.w.indent();
        try self.w.line("try jw.beginObject();", .{});
        var emitted_json_props = std.StringArrayHashMapUnmanaged(void){};
        try self.emitFlattenedSchemaJsonStringifyFields(schema, &emitted_json_props, &all_required, true, false);
        try self.w.line("try jw.endObject();", .{});
        self.w.dedent();
        try self.w.line("}}", .{});

        self.w.dedent();
        try self.w.line("}};", .{});
    }

    fn collectNonNullableOptionalFields(
        self: *TypeGenerator,
        schema: types.Schema,
        required_fields: *const std.StringArrayHashMapUnmanaged(void),
        allow_required: bool,
        fields: *std.StringArrayHashMapUnmanaged(void),
    ) !void {
        for (schema.properties.keys(), schema.properties.values()) |prop_name, prop_sor| {
            const is_required = allow_required and required_fields.contains(prop_name);
            if (!is_required and self.schemaOrRefRepresentation(prop_sor).nullability == .non_nullable) {
                try fields.put(self.arena, prop_name, {});
            }
        }
        for (schema.all_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.collectNonNullableOptionalFields(resolved, required_fields, allow_required, fields);
        }
        for (schema.one_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.collectNonNullableOptionalFields(resolved, required_fields, false, fields);
        }
        for (schema.any_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.collectNonNullableOptionalFields(resolved, required_fields, false, fields);
        }
    }

    fn emitPresenceAwareObjectParsers(
        self: *TypeGenerator,
        property_names: []const []const u8,
        strict_optional_fields: *const std.StringArrayHashMapUnmanaged(void),
        has_required_presence_fields: bool,
    ) !void {
        if (strict_optional_fields.count() == 0 and !has_required_presence_fields) return;
        self.uses_presence_aware_object = true;

        try self.w.blank();
        try self.w.line("/// OpenAPI wire names and nullability consumed directly by antfly-json's typed parser.", .{});
        try self.w.line("pub const antflyOpenApiFieldMetadata = .{{", .{});
        self.w.indent();
        try self.emitOpenApiObjectFieldMetadata(property_names, strict_optional_fields);
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.blank();
        try self.w.line("pub fn jsonParse(allocator: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {{", .{});
        self.w.indent();
        try self.w.line("return try openApiParseObject(@This(), antflyOpenApiFieldMetadata, allocator, source, options);", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();
        try self.w.line("pub fn jsonParseFromValue(allocator: std.mem.Allocator, source: std.json.Value, options: std.json.ParseOptions) !@This() {{", .{});
        self.w.indent();
        try self.w.line("return try openApiParseObjectFromValue(@This(), antflyOpenApiFieldMetadata, allocator, source, options);", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
    }

    fn emitOpenApiObjectFieldMetadata(
        self: *TypeGenerator,
        property_names: []const []const u8,
        strict_optional_fields: *const std.StringArrayHashMapUnmanaged(void),
    ) !void {
        for (property_names) |json_name| {
            // @typeInfo reports the logical identifier without Zig's @"..."
            // source escaping. Keep the wire name, reflection name, and emitted
            // source spelling separate so reserved and otherwise escaped field
            // names retain the same compile-time ordering check as ordinary
            // identifiers.
            const reflection_name = try naming.toFieldName(self.arena, json_name);
            try self.w.line(
                ".{{ \"{f}\", \"{f}\", {s} }},",
                .{
                    std.zig.fmtString(json_name),
                    std.zig.fmtString(reflection_name),
                    if (strict_optional_fields.contains(json_name)) "true" else "false",
                },
            );
        }
    }

    fn emitFlattenedSchemaProperties(
        self: *TypeGenerator,
        owner_type_name: []const u8,
        schema: types.Schema,
        emitted_props: *std.StringArrayHashMapUnmanaged(void),
        required_fields: *const std.StringArrayHashMapUnmanaged(void),
        allow_required: bool,
    ) !void {
        for (schema.properties.keys(), schema.properties.values()) |prop_name, prop_sor| {
            if (emitted_props.contains(prop_name)) continue;
            try emitted_props.put(self.arena, prop_name, {});
            try self.emitStructField(owner_type_name, prop_name, prop_sor, allow_required and required_fields.contains(prop_name));
        }

        for (schema.all_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.emitFlattenedSchemaProperties(owner_type_name, resolved, emitted_props, required_fields, allow_required);
        }

        for (schema.one_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.emitFlattenedSchemaProperties(owner_type_name, resolved, emitted_props, required_fields, false);
        }

        for (schema.any_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.emitFlattenedSchemaProperties(owner_type_name, resolved, emitted_props, required_fields, false);
        }
    }

    fn emitFlattenedSchemaJsonStringifyFields(
        self: *TypeGenerator,
        schema: types.Schema,
        emitted_props: *std.StringArrayHashMapUnmanaged(void),
        required_fields: *const std.StringArrayHashMapUnmanaged(void),
        allow_required: bool,
        optional_nulls_follow_writer: bool,
    ) !void {
        for (schema.properties.keys(), schema.properties.values()) |prop_name, prop_sor| {
            if (emitted_props.contains(prop_name)) continue;
            try emitted_props.put(self.arena, prop_name, {});

            const field = try naming.zigFieldName(self.arena, prop_name);
            const is_required = allow_required and required_fields.contains(prop_name);
            const representation = self.schemaOrRefRepresentation(prop_sor);

            if (is_required) {
                try self.w.line("try jw.objectField(\"{s}\");", .{prop_name});
                try self.w.line("try jw.write(self.{s});", .{field});
            } else if (representation.nullability == .nullable) {
                try self.w.line("switch (self.{s}) {{", .{field});
                self.w.indent();
                try self.w.line(".absent => {{}},", .{});
                try self.w.line(".null_value => {{", .{});
                self.w.indent();
                try self.w.line("try jw.objectField(\"{s}\");", .{prop_name});
                try self.w.line("try jw.write(@as(?u8, null));", .{});
                self.w.dedent();
                try self.w.line("}},", .{});
                try self.w.line(".value => |value| {{", .{});
                self.w.indent();
                try self.w.line("try jw.objectField(\"{s}\");", .{prop_name});
                try self.w.line("try jw.write(value);", .{});
                self.w.dedent();
                try self.w.line("}},", .{});
                self.w.dedent();
                try self.w.line("}}", .{});
            } else {
                try self.w.line("if (self.{s}) |value| {{", .{field});
                self.w.indent();
                try self.w.line("try jw.objectField(\"{s}\");", .{prop_name});
                try self.w.line("try jw.write(value);", .{});
                self.w.dedent();
                // The writer option is a presentation choice, not permission
                // to violate a schema which distinguishes omission from JSON
                // null. Only fields whose nullability is unknown may inherit
                // that option; non-nullable optionals are always omitted.
                if (optional_nulls_follow_writer and representation.nullability == .unknown) {
                    try self.w.line("}} else if (jw.options.emit_null_optional_fields) {{", .{});
                    self.w.indent();
                    try self.w.line("try jw.objectField(\"{s}\");", .{prop_name});
                    try self.w.line("try jw.write(@as(?u8, null));", .{});
                    self.w.dedent();
                }
                try self.w.line("}}", .{});
            }
        }

        for (schema.all_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.emitFlattenedSchemaJsonStringifyFields(resolved, emitted_props, required_fields, allow_required, optional_nulls_follow_writer);
        }

        for (schema.one_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.emitFlattenedSchemaJsonStringifyFields(resolved, emitted_props, required_fields, false, optional_nulls_follow_writer);
        }

        for (schema.any_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            try self.emitFlattenedSchemaJsonStringifyFields(resolved, emitted_props, required_fields, false, optional_nulls_follow_writer);
        }
    }

    fn schemaNeedsRequiredFieldSerializer(
        self: *TypeGenerator,
        schema: types.Schema,
        required_fields: *const std.StringArrayHashMapUnmanaged(void),
        allow_required: bool,
    ) bool {
        if (allow_required) {
            for (schema.properties.keys(), schema.properties.values()) |prop_name, prop_sor| {
                const representation = self.schemaOrRefRepresentation(prop_sor);
                if (required_fields.contains(prop_name)) {
                    if (representation.nullability != .non_nullable) return true;
                } else if (representation.nullability == .nullable) {
                    return true;
                }
            }
        }

        for (schema.all_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            if (self.schemaNeedsRequiredFieldSerializer(resolved, required_fields, allow_required)) return true;
        }
        return false;
    }

    /// Whether parsing must distinguish an omitted required property from an
    /// explicit JSON null. Ordinary Zig fields already reject omission unless
    /// their representation is optional; nullable and unresolved external
    /// schemas can carry such a representation and therefore require OpenAPI
    /// field metadata on the SIMD path.
    fn schemaNeedsRequiredFieldPresence(
        self: *TypeGenerator,
        schema: types.Schema,
        required_fields: *const std.StringArrayHashMapUnmanaged(void),
        allow_required: bool,
    ) bool {
        if (allow_required) {
            for (schema.properties.keys(), schema.properties.values()) |prop_name, prop_sor| {
                if (required_fields.contains(prop_name) and
                    self.schemaOrRefRepresentation(prop_sor).nullability != .non_nullable)
                {
                    return true;
                }
            }
        }

        for (schema.all_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch continue;
            if (self.schemaNeedsRequiredFieldPresence(resolved, required_fields, allow_required)) return true;
        }
        return false;
    }

    const SchemaNullability = enum { non_nullable, nullable, unknown };
    const NullRepresentation = enum {
        /// The occurrence must add `?` when JSON null needs representation.
        none,
        /// The generated Zig type expression already carries `?`.
        zig_optional,
        /// The type represents JSON null internally (for example
        /// `std.json.Value`) but still needs an outer `?` when property absence
        /// must be represented.
        intrinsic,
        /// An external mapped type whose document is deliberately unavailable.
        unknown,
    };
    const SchemaRepresentation = struct {
        nullability: SchemaNullability,
        null_representation: NullRepresentation,
    };

    /// Whether a property schema permits JSON null. OpenAPI represents this
    /// through 3.0 `nullable`, a 3.1 type array, or a nullable composition.
    /// Local component aliases are resolved recursively. External schemas are
    /// unknown by construction; callers must preserve required-field presence
    /// conservatively instead of assuming that an unresolved type rejects null.
    fn schemaOrRefRepresentation(self: *TypeGenerator, schema_or_ref: types.SchemaOrRef) SchemaRepresentation {
        const schema = self.resolver.resolveSchema(schema_or_ref) catch return .{
            .nullability = .unknown,
            .null_representation = .unknown,
        };
        const nullability: SchemaNullability = if (schema.isNullable() or nullableOneOfInner(schema.one_of) != null)
            .nullable
        else
            .non_nullable;

        const null_representation: NullRepresentation = switch (schema_or_ref) {
            .schema => if (schema.extensions.get("x-zig-type") != null)
                .intrinsic
            else if (nullableOneOfInner(schema.one_of) != null)
                .zig_optional
            else if (schemaHasMultipleNonNullTypes(schema))
                .intrinsic
            else
                .none,
            .ref => if (schema.extensions.get("x-zig-type") != null or
                (schemaHasMultipleNonNullTypes(schema) and !schema.isNullable()))
                .intrinsic
            else if (nullability == .nullable)
                .zig_optional
            else
                .none,
        };
        return .{
            .nullability = nullability,
            .null_representation = null_representation,
        };
    }

    /// Generate a union(enum) from oneOf with discriminator.
    fn generateDiscriminatedUnion(self: *TypeGenerator, type_name: []const u8, schema: types.Schema) !void {
        const disc = schema.discriminator.?;
        if (schema.description) |desc| try self.w.docComment(desc);
        try self.w.line("pub const {s} = union(enum) {{", .{type_name});
        self.w.indent();

        const Variant = struct {
            field: []const u8,
            zig_type: []const u8,
            disc_value: []const u8,
        };
        // If exactly one variant's schema permits an omitted discriminator,
        // it is the only unambiguous fallback. Multiple such variants leave
        // the payload structurally ambiguous and deliberately disable it.
        var variants = std.ArrayListUnmanaged(Variant).empty;
        var fallback_variant: ?Variant = null;
        var fallback_ambiguous = false;

        // Generate variant for each oneOf member
        for (schema.one_of) |member| {
            switch (member) {
                .ref => |ref| {
                    const ref_name = naming.refToName(ref.ref_string) orelse continue;
                    const variant_name = try naming.zigFieldName(self.arena, ref_name);
                    const ref_type = try naming.toTypeName(self.arena, ref_name);
                    try self.w.line("{s}: {s},", .{ variant_name, ref_type });

                    // Determine discriminator value: reverse-lookup mapping, fall back to ref name
                    const disc_value = blk: {
                        for (disc.mapping.keys(), disc.mapping.values()) |map_key, map_ref| {
                            // mapping is { "disc_value": "#/components/schemas/TypeName" }
                            const mapped_name = naming.refToName(map_ref) orelse continue;
                            if (std.mem.eql(u8, mapped_name, ref_name)) break :blk map_key;
                        }
                        break :blk ref_name;
                    };
                    const variant = Variant{
                        .field = variant_name,
                        .zig_type = ref_type,
                        .disc_value = disc_value,
                    };
                    try variants.append(self.arena, variant);
                    const resolved = self.resolver.resolveSchema(member) catch null;
                    // Resolution failures and pathological allOf recursion fail
                    // closed: neither can safely establish an omitted-field
                    // fallback.
                    const discriminator_required = if (resolved) |resolved_schema|
                        self.schemaRequiresProperty(resolved_schema, disc.property_name, 0)
                    else
                        true;
                    if (!discriminator_required) {
                        if (fallback_variant == null) {
                            fallback_variant = variant;
                        } else {
                            fallback_ambiguous = true;
                        }
                    }
                },
                .schema => {
                    try self.w.line("// TODO: inline oneOf variant", .{});
                },
            }
        }
        if (fallback_ambiguous) fallback_variant = null;

        try self.w.blank();

        // jsonParseFromValue: parse from a pre-parsed std.json.Value tree
        if (variants.items.len == 0) {
            try self.generateEmptyUnionJsonStubs();
        } else {
            // Raw-subtree parsers can probe only the discriminator and then
            // parse the selected variant directly. This avoids retaining a
            // complete std.json.Value tree beside large typed response rows.
            const disc_field = try naming.zigFieldName(self.arena, disc.property_name);
            try self.w.line("pub fn jsonParseFromSliceLeaky(allocator: std.mem.Allocator, input: []const u8, options: std.json.ParseOptions) !@This() {{", .{});
            self.w.indent();
            try self.generateDiscriminatorProbe(disc_field);
            try self.w.line("var probe_options = options;", .{});
            try self.w.line("probe_options.ignore_unknown_fields = true;", .{});
            try self.w.line("const probe = try std.json.parseFromSliceLeaky(Probe, allocator, input, probe_options);", .{});
            try self.w.line("const disc_str = switch (probe.{s}) {{", .{disc_field});
            self.w.indent();
            try self.w.line(".value => |value| value,", .{});
            try self.w.line(".missing => {{", .{});
            self.w.indent();
            if (fallback_variant) |fallback| {
                try self.w.line("return .{{ .{s} = try std.json.parseFromSliceLeaky({s}, allocator, input, options) }};", .{ fallback.field, fallback.zig_type });
            } else {
                try self.w.line("return error.MissingField;", .{});
            }
            self.w.dedent();
            try self.w.line("}},", .{});
            self.w.dedent();
            try self.w.line("}};", .{});
            for (variants.items) |v| {
                try self.w.line("if (std.mem.eql(u8, disc_str, \"{f}\")) {{", .{std.zig.fmtString(v.disc_value)});
                self.w.indent();
                try self.w.line("return .{{ .{s} = try std.json.parseFromSliceLeaky({s}, allocator, input, options) }};", .{ v.field, v.zig_type });
                self.w.dedent();
                try self.w.line("}}", .{});
            }
            try self.w.line("return error.UnexpectedToken;", .{});
            self.w.dedent();
            try self.w.line("}}", .{});

            try self.w.blank();

            try self.w.line("pub fn jsonParse(allocator: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {{", .{});
            self.w.indent();
            try self.w.line("const value = try std.json.innerParse(std.json.Value, allocator, source, options);", .{});
            try self.w.line("return try jsonParseFromValue(allocator, value, options);", .{});
            self.w.dedent();
            try self.w.line("}}", .{});

            try self.w.blank();

            try self.w.line("pub fn jsonParseFromValue(allocator: std.mem.Allocator, source: std.json.Value, options: std.json.ParseOptions) !@This() {{", .{});
            self.w.indent();
            try self.w.line("if (source != .object) return error.UnexpectedToken;", .{});
            try self.w.line("const disc_val = source.object.get(\"{s}\") orelse {{", .{disc.property_name});
            self.w.indent();
            if (fallback_variant) |fallback| {
                try self.w.line("return .{{ .{s} = try std.json.parseFromValueLeaky({s}, allocator, source, options) }};", .{ fallback.field, fallback.zig_type });
            } else {
                try self.w.line("return error.MissingField;", .{});
            }
            self.w.dedent();
            try self.w.line("}};", .{});
            try self.w.line("const disc_str = switch (disc_val) {{", .{});
            self.w.indent();
            try self.w.line(".string => |s| s,", .{});
            try self.w.line("else => return error.UnexpectedToken,", .{});
            self.w.dedent();
            try self.w.line("}};", .{});

            // Match discriminator value to variant
            for (variants.items) |v| {
                try self.w.line("if (std.mem.eql(u8, disc_str, \"{s}\")) {{", .{v.disc_value});
                self.w.indent();
                try self.w.line("return .{{ .{s} = try std.json.parseFromValueLeaky({s}, allocator, source, options) }};", .{ v.field, v.zig_type });
                self.w.dedent();
                try self.w.line("}}", .{});
            }
            try self.w.line("return error.UnexpectedToken;", .{});
            self.w.dedent();
            try self.w.line("}}", .{});

            try self.w.blank();

            // jsonStringify: serialize the active variant
            try self.w.line("pub fn jsonStringify(self: @This(), jw: anytype) !void {{", .{});
            self.w.indent();
            try self.w.line("switch (self) {{", .{});
            self.w.indent();
            for (variants.items) |v| {
                try self.w.line(".{s} => |v| try jw.write(v),", .{v.field});
            }
            self.w.dedent();
            try self.w.line("}}", .{});
            self.w.dedent();
            try self.w.line("}}", .{});
        }

        self.w.dedent();
        try self.w.line("}};", .{});
    }

    fn schemaRequiresProperty(
        self: *TypeGenerator,
        schema: types.Schema,
        property_name: []const u8,
        depth: usize,
    ) bool {
        if (depth >= 32) return true;
        for (schema.required) |required_field| {
            if (std.mem.eql(u8, required_field, property_name)) return true;
        }
        for (schema.all_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch return true;
            if (self.schemaRequiresProperty(resolved, property_name, depth + 1)) return true;
        }
        return false;
    }

    fn canGenerateStructuralUnion(self: *TypeGenerator, schema: types.Schema) !bool {
        for (schema.one_of) |member| {
            if (!try self.canCollectStructuralVariants(member, 0)) return false;
        }
        return schema.one_of.len > 0;
    }

    /// A named oneOf is a transparent composition boundary for structural
    /// unions. Flattening it preserves concrete leaf types instead of reducing
    /// a perfectly typed nested union to std.json.Value. Only property-free
    /// oneOf wrappers are transparent; schemas with their own object surface
    /// remain ordinary variants.
    fn canCollectStructuralVariants(
        self: *TypeGenerator,
        member: types.SchemaOrRef,
        depth: usize,
    ) !bool {
        if (depth >= 32) return false;
        const resolved = self.resolver.resolveSchema(member) catch return false;
        if (isTransparentStructuralUnion(resolved)) {
            for (resolved.one_of) |nested| {
                if (!try self.canCollectStructuralVariants(nested, depth + 1)) return false;
            }
            return resolved.one_of.len > 0;
        }
        return try self.collectStructuralVariant(member) != null;
    }

    fn isTransparentStructuralUnion(schema: types.Schema) bool {
        return schema.one_of.len > 0 and
            schema.any_of.len == 0 and
            schema.all_of.len == 0 and
            schema.properties.count() == 0;
    }

    const StructuralVariant = struct {
        field: []const u8,
        zig_type: []const u8,
        ref_name: []const u8,
        selector_keys: []const []const u8,
        singleton_selectors: []const StructuralSingletonSelector,
        schema: types.Schema,
    };

    const StructuralSingletonSelector = struct {
        property_name: []const u8,
        value: []const u8,
    };

    const StructuralDiscriminator = struct {
        property_name: []const u8,
        values: []const []const u8,
        fallback_index: ?usize,
    };

    fn collectStructuralVariant(self: *TypeGenerator, member: types.SchemaOrRef) !?StructuralVariant {
        const ref = switch (member) {
            .ref => |ref| ref,
            .schema => return null,
        };
        if (naming.isExternalRef(ref.ref_string)) return null;
        const ref_name = naming.refToName(ref.ref_string) orelse return null;
        const ref_type = try naming.toTypeName(self.arena, ref_name);
        const resolved = self.resolver.resolveSchema(member) catch return null;
        if (resolved.primaryType()) |primary_type| {
            if (!std.mem.eql(u8, primary_type, "object")) return null;
        } else if (resolved.properties.count() == 0) {
            return null;
        }

        var selector_keys = std.ArrayListUnmanaged([]const u8).empty;
        var singleton_selectors = std.ArrayListUnmanaged(StructuralSingletonSelector).empty;
        for (resolved.properties.keys()) |prop_name| {
            if (std.mem.eql(u8, prop_name, "boost")) continue;
            if (std.mem.eql(u8, prop_name, "field")) continue;
            try selector_keys.append(self.arena, prop_name);
            if (!self.schemaRequiresProperty(resolved, prop_name, 0)) continue;
            const property = resolved.properties.get(prop_name) orelse continue;
            const property_schema = self.resolver.resolveSchema(property) catch continue;
            if (property_schema.enum_values.len != 1 or property_schema.enum_has_null) continue;
            try singleton_selectors.append(self.arena, .{
                .property_name = prop_name,
                .value = property_schema.enum_values[0],
            });
        }

        return .{
            .field = try naming.zigFieldName(self.arena, ref_name),
            .zig_type = ref_type,
            .ref_name = ref_name,
            .selector_keys = selector_keys.items,
            .singleton_selectors = singleton_selectors.items,
            .schema = resolved,
        };
    }

    fn collectStructuralVariants(
        self: *TypeGenerator,
        member: types.SchemaOrRef,
        variants: *std.ArrayListUnmanaged(StructuralVariant),
        depth: usize,
    ) !void {
        if (depth >= 32) return error.InvalidOpenApiSchema;
        const resolved = self.resolver.resolveSchema(member) catch return;
        if (isTransparentStructuralUnion(resolved)) {
            for (resolved.one_of) |nested| {
                try self.collectStructuralVariants(nested, variants, depth + 1);
            }
            return;
        }
        const variant = try self.collectStructuralVariant(member) orelse return;
        for (variants.items) |existing| {
            if (std.mem.eql(u8, existing.ref_name, variant.ref_name)) return;
        }
        try variants.append(self.arena, variant);
    }

    /// Infer a discriminator only when every structural variant declares the
    /// same singleton string-enum property with a unique value. One variant may
    /// omit that property; it becomes the compatibility fallback. This keeps
    /// the optimization schema-generic while preserving strict structural
    /// oneOf behavior for unions without such a proof.
    fn inferStructuralDiscriminator(
        self: *TypeGenerator,
        variants: []const StructuralVariant,
    ) !?StructuralDiscriminator {
        if (variants.len == 0) return null;
        var candidate_it = variants[0].schema.properties.iterator();
        candidate: while (candidate_it.next()) |candidate_entry| {
            const first_schema = self.resolver.resolveSchema(candidate_entry.value_ptr.*) catch continue;
            if (first_schema.enum_values.len != 1 or first_schema.enum_has_null) continue;

            const values = try self.arena.alloc([]const u8, variants.len);
            var fallback_index: ?usize = null;
            for (variants, 0..) |variant, i| {
                const property = variant.schema.properties.get(candidate_entry.key_ptr.*) orelse continue :candidate;
                const property_schema = self.resolver.resolveSchema(property) catch continue :candidate;
                if (property_schema.enum_values.len != 1 or property_schema.enum_has_null)
                    continue :candidate;
                const value = property_schema.enum_values[0];
                for (values[0..i]) |prior| {
                    if (std.mem.eql(u8, prior, value)) continue :candidate;
                }
                values[i] = value;
                if (!self.schemaRequiresProperty(variant.schema, candidate_entry.key_ptr.*, 0)) {
                    if (fallback_index != null) continue :candidate;
                    fallback_index = i;
                }
            }
            return .{
                .property_name = candidate_entry.key_ptr.*,
                .values = values,
                .fallback_index = fallback_index,
            };
        }
        return null;
    }

    /// Generate a recursive union(enum) from an undiscriminated oneOf of
    /// object-like refs. This is a targeted best-effort path for schemas like
    /// Bleve Query where variants are distinguished structurally.
    fn generateStructuralUnion(self: *TypeGenerator, type_name: []const u8, schema: types.Schema) !void {
        if (schema.description) |desc| try self.w.docComment(desc);
        try self.w.line("pub const {s} = union(enum) {{", .{type_name});
        self.w.indent();

        var variants = std.ArrayListUnmanaged(StructuralVariant).empty;
        for (schema.one_of) |member| {
            try self.collectStructuralVariants(member, &variants, 0);
        }

        // Sort: required singleton-enum selectors are strongest, followed by
        // the number of selector keys and an alphabetical tie-break. This
        // prevents a broad object variant from capturing a semantically
        // distinct literal variant that shares the same property name.
        // This order is used for both field layout and jsonParseFromValue dispatch.
        std.sort.pdq(StructuralVariant, variants.items, {}, struct {
            fn lessThan(_: void, a: StructuralVariant, b: StructuralVariant) bool {
                if (a.singleton_selectors.len != b.singleton_selectors.len)
                    return a.singleton_selectors.len > b.singleton_selectors.len;
                if (a.selector_keys.len != b.selector_keys.len) return a.selector_keys.len > b.selector_keys.len;
                return std.mem.order(u8, a.ref_name, b.ref_name) == .lt;
            }
        }.lessThan);
        const inferred_discriminator = try self.inferStructuralDiscriminator(variants.items);
        var needs_singleton_selector_helper = inferred_discriminator == null;
        if (needs_singleton_selector_helper) {
            needs_singleton_selector_helper = false;
            for (variants.items) |variant| {
                if (variant.singleton_selectors.len > 0) {
                    needs_singleton_selector_helper = true;
                    break;
                }
            }
        }

        for (variants.items) |variant| {
            try self.w.line("{s}: *{s},", .{ variant.field, variant.zig_type });
        }

        try self.w.blank();

        try self.w.line("fn parseStructuralVariant(comptime T: type, allocator: std.mem.Allocator, source: std.json.Value, options: std.json.ParseOptions) !?*T {{", .{});
        self.w.indent();
        try self.w.line("const parsed = std.json.parseFromValueLeaky(T, allocator, source, options) catch |err| switch (err) {{", .{});
        self.w.indent();
        try self.w.line("error.OutOfMemory => return err,", .{});
        try self.w.line("else => return null,", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.line("const value = try allocator.create(T);", .{});
        try self.w.line("value.* = parsed;", .{});
        try self.w.line("return value;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});

        try self.w.blank();

        if (needs_singleton_selector_helper) {
            try self.w.line("fn objectStringEquals(object: std.json.ObjectMap, comptime key: []const u8, comptime expected: []const u8) bool {{", .{});
            self.w.indent();
            try self.w.line("const value = object.get(key) orelse return false;", .{});
            try self.w.line("return value == .string and std.mem.eql(u8, value.string, expected);", .{});
            self.w.dedent();
            try self.w.line("}}", .{});

            try self.w.blank();
        }

        try self.w.line("fn objectHasAnyKey(object: std.json.ObjectMap, comptime keys: []const []const u8) bool {{", .{});
        self.w.indent();
        try self.w.line("inline for (keys) |key| {{", .{});
        self.w.indent();
        try self.w.line("if (object.contains(key)) return true;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.line("return false;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});

        try self.w.blank();

        if (variants.items.len == 0) {
            try self.generateEmptyUnionJsonStubs();
        } else {
            if (inferred_discriminator) |disc| {
                try self.w.line("fn parseStructuralVariantFromSlice(comptime T: type, allocator: std.mem.Allocator, input: []const u8, options: std.json.ParseOptions) !*T {{", .{});
                self.w.indent();
                try self.w.line("const parsed = try std.json.parseFromSliceLeaky(T, allocator, input, options);", .{});
                try self.w.line("const value = try allocator.create(T);", .{});
                try self.w.line("value.* = parsed;", .{});
                try self.w.line("return value;", .{});
                self.w.dedent();
                try self.w.line("}}", .{});
                try self.w.blank();

                const disc_field = try naming.zigFieldName(self.arena, disc.property_name);
                try self.w.line("pub fn jsonParseFromSliceLeaky(allocator: std.mem.Allocator, input: []const u8, options: std.json.ParseOptions) !@This() {{", .{});
                self.w.indent();
                try self.generateDiscriminatorProbe(disc_field);
                try self.w.line("var probe_options = options;", .{});
                try self.w.line("probe_options.ignore_unknown_fields = true;", .{});
                try self.w.line("const probe = try std.json.parseFromSliceLeaky(Probe, allocator, input, probe_options);", .{});
                try self.w.line("switch (probe.{s}) {{", .{disc_field});
                self.w.indent();
                try self.w.line(".value => |disc_str| {{", .{});
                self.w.indent();
                for (variants.items, disc.values) |variant, value| {
                    try self.w.line("if (std.mem.eql(u8, disc_str, \"{f}\")) return .{{ .{s} = try parseStructuralVariantFromSlice({s}, allocator, input, options) }};", .{ std.zig.fmtString(value), variant.field, variant.zig_type });
                }
                try self.w.line("return error.UnexpectedToken;", .{});
                self.w.dedent();
                try self.w.line("}},", .{});
                try self.w.line(".missing => {{", .{});
                self.w.indent();
                if (disc.fallback_index) |fallback_index| {
                    const fallback = variants.items[fallback_index];
                    try self.w.line("return .{{ .{s} = try parseStructuralVariantFromSlice({s}, allocator, input, options) }};", .{ fallback.field, fallback.zig_type });
                } else {
                    try self.w.line("return error.MissingField;", .{});
                }
                self.w.dedent();
                try self.w.line("}},", .{});
                self.w.dedent();
                try self.w.line("}}", .{});
                self.w.dedent();
                try self.w.line("}}", .{});
                try self.w.blank();
            }

            try self.w.line("pub fn jsonParse(allocator: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {{", .{});
            self.w.indent();
            try self.w.line("const value = try std.json.innerParse(std.json.Value, allocator, source, options);", .{});
            try self.w.line("return try jsonParseFromValue(allocator, value, options);", .{});
            self.w.dedent();
            try self.w.line("}}", .{});

            try self.w.blank();

            try self.w.line("pub fn jsonParseFromValue(allocator: std.mem.Allocator, source: std.json.Value, options: std.json.ParseOptions) !@This() {{", .{});
            self.w.indent();
            try self.w.line("if (source != .object) return error.UnexpectedToken;", .{});

            if (inferred_discriminator) |disc| {
                try self.w.line("const disc_val = source.object.get(\"{s}\") orelse {{", .{disc.property_name});
                self.w.indent();
                if (disc.fallback_index) |fallback_index| {
                    const fallback = variants.items[fallback_index];
                    try self.w.line("const parsed = try parseStructuralVariant({s}, allocator, source, options) orelse return error.UnexpectedToken;", .{fallback.zig_type});
                    try self.w.line("return .{{ .{s} = parsed }};", .{fallback.field});
                } else {
                    try self.w.line("return error.MissingField;", .{});
                }
                self.w.dedent();
                try self.w.line("}};", .{});
                try self.w.line("const disc_str = switch (disc_val) {{ .string => |value| value, else => return error.UnexpectedToken }};", .{});
                for (variants.items, disc.values) |variant, value| {
                    try self.w.line("if (std.mem.eql(u8, disc_str, \"{f}\")) {{", .{std.zig.fmtString(value)});
                    self.w.indent();
                    try self.w.line("const parsed = try parseStructuralVariant({s}, allocator, source, options) orelse return error.UnexpectedToken;", .{variant.zig_type});
                    try self.w.line("return .{{ .{s} = parsed }};", .{variant.field});
                    self.w.dedent();
                    try self.w.line("}}", .{});
                }
            } else {
                for (variants.items) |variant| {
                    if (variant.selector_keys.len == 0) continue;
                    try self.w.line("if (objectHasAnyKey(source.object, &.{{", .{});
                    self.w.indent();
                    for (variant.selector_keys) |selector_key| {
                        try self.w.line("\"{s}\",", .{selector_key});
                    }
                    self.w.dedent();
                    if (variant.singleton_selectors.len == 0) {
                        try self.w.line("}})) {{", .{});
                    } else {
                        try self.w.line("}}) and", .{});
                        self.w.indent();
                        for (variant.singleton_selectors, 0..) |selector, selector_index| {
                            try self.w.line("objectStringEquals(source.object, \"{f}\", \"{f}\"){s}", .{
                                std.zig.fmtString(selector.property_name),
                                std.zig.fmtString(selector.value),
                                if (selector_index + 1 == variant.singleton_selectors.len) ") {" else " and",
                            });
                        }
                        self.w.dedent();
                    }
                    self.w.indent();
                    try self.w.line("if (try parseStructuralVariant({s}, allocator, source, options)) |parsed| return .{{ .{s} = parsed }};", .{ variant.zig_type, variant.field });
                    self.w.dedent();
                    try self.w.line("}}", .{});
                }

                for (variants.items) |variant| {
                    if (variant.selector_keys.len > 0) continue;
                    try self.w.line("if (try parseStructuralVariant({s}, allocator, source, options)) |parsed| return .{{ .{s} = parsed }};", .{ variant.zig_type, variant.field });
                }
            }
            try self.w.line("return error.UnexpectedToken;", .{});
            self.w.dedent();
            try self.w.line("}}", .{});

            try self.w.blank();

            try self.w.line("pub fn jsonStringify(self: @This(), jw: anytype) !void {{", .{});
            self.w.indent();
            try self.w.line("switch (self) {{", .{});
            self.w.indent();
            for (variants.items) |variant| {
                try self.w.line(".{s} => |v| try jw.write(v.*),", .{variant.field});
            }
            self.w.dedent();
            try self.w.line("}}", .{});
            self.w.dedent();
            try self.w.line("}}", .{});
        }

        self.w.dedent();
        try self.w.line("}};", .{});
    }

    /// Emit a discriminator probe whose default represents only an omitted
    /// property. An explicit JSON null still invokes jsonParse and fails the
    /// string parse, so compatibility fallbacks cannot accidentally accept it.
    fn generateDiscriminatorProbe(self: *TypeGenerator, field_name: []const u8) !void {
        try self.w.line("const DiscriminatorProbe = union(enum) {{", .{});
        self.w.indent();
        try self.w.line("missing,", .{});
        try self.w.line("value: []const u8,", .{});
        try self.w.line("pub fn jsonParse(probe_allocator: std.mem.Allocator, probe_source: anytype, probe_options: std.json.ParseOptions) !@This() {{", .{});
        self.w.indent();
        try self.w.line("return .{{ .value = try std.json.innerParse([]const u8, probe_allocator, probe_source, probe_options) }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.line("const Probe = struct {{ {s}: DiscriminatorProbe = .missing }};", .{field_name});
    }

    /// Emit stub jsonParseFromValue/jsonStringify for unions with no resolved variants.
    fn generateEmptyUnionJsonStubs(self: *TypeGenerator) !void {
        try self.w.line("pub fn jsonParseFromValue(_: std.mem.Allocator, source: std.json.Value, _: std.json.ParseOptions) !@This() {{", .{});
        self.w.indent();
        try self.w.line("if (source != .object) return error.UnexpectedToken;", .{});
        try self.w.line("return error.UnexpectedToken;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});

        try self.w.blank();

        try self.w.line("pub fn jsonStringify(_: @This(), _: anytype) !void {{", .{});
        try self.w.line("}}", .{});
    }

    const GenError = error{ OutOfMemory, MissingExternalImportMapping, InvalidOpenApiSchema };

    /// Get the Zig type string for a SchemaOrRef.
    pub fn zigTypeForSchemaOrRef(self: *TypeGenerator, sor: types.SchemaOrRef) GenError![]const u8 {
        switch (sor) {
            .ref => |ref| return self.zigTypeForRef(ref.ref_string),
            .schema => |schema| return self.zigTypeForSchema(schema),
        }
    }

    fn zigTypeForRef(self: *TypeGenerator, ref: []const u8) GenError![]const u8 {
        const ref_name = naming.refToName(ref) orelse return "std.json.Value";
        const type_name = try naming.toTypeName(self.arena, ref_name);

        // Check if this is an external ref with an import mapping
        if (naming.isExternalRef(ref)) {
            if (naming.refToFilePath(ref)) |file_path| {
                if (self.import_mapping.get(file_path)) |module_name| {
                    try self.used_imports.put(self.arena, module_name, {});
                    return std.fmt.allocPrint(self.arena, "{s}.{s}", .{ module_name, type_name });
                }
            }
            return error.MissingExternalImportMapping;
        }

        return type_name;
    }

    test "external schema refs require an explicit import mapping" {
        const alloc = std.testing.allocator;
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const arena = arena_impl.allocator();

        var properties = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
        try properties.put(arena, "alias", .{
            .ref = .{ .ref_string = "generated/identifier.yaml#/components/schemas/Identifier" },
        });
        var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
        try schemas.put(arena, "Holder", .{ .schema = .{
            .schema_type = .{ .single = "object" },
            .properties = properties,
            .required = &.{"alias"},
        } });
        const doc = types.OpenApiDoc{
            .openapi = "3.0.3",
            .info = .{ .title = "Test", .version = "1.0" },
            .components = .{ .schemas = schemas },
        };

        var resolver = Resolver.init(arena, &doc);
        var unmapped_writer = SourceWriter.init(arena);
        var unmapped = TypeGenerator.init(arena, &unmapped_writer, &resolver);
        try std.testing.expectError(error.MissingExternalImportMapping, unmapped.generateAll(&doc));

        var mapped_writer = SourceWriter.init(arena);
        var mapped = TypeGenerator.init(arena, &mapped_writer, &resolver);
        try mapped.import_mapping.put(arena, "generated/identifier.yaml", "identifier_openapi");
        try mapped.generateAll(&doc);
        try std.testing.expect(std.mem.indexOf(u8, mapped_writer.toSlice(), "alias: identifier_openapi.Identifier,") != null);
        try std.testing.expect(mapped.used_imports.contains("identifier_openapi"));
        // The imported schema's nullability is not available in this document.
        // Conservatively emit the required field so an imported optional alias
        // cannot turn an explicit required null into an absent property.
        try std.testing.expect(std.mem.indexOf(u8, mapped_writer.toSlice(), "pub fn jsonStringify(self: @This(), jw: anytype) !void {") != null);
        try std.testing.expect(std.mem.indexOf(u8, mapped_writer.toSlice(), "try jw.objectField(\"alias\");") != null);
        try std.testing.expect(std.mem.indexOf(u8, mapped_writer.toSlice(), "try jw.write(self.alias);") != null);
    }

    /// Get the Zig type string for an inline schema.
    fn zigTypeForSchema(self: *TypeGenerator, schema: types.Schema) GenError![]const u8 {
        if (try self.zigTypeOverride(schema)) |override| return override;

        // OpenAPI 3.0 wraps a nullable $ref in a single-member allOf because
        // nullable is otherwise ignored next to $ref. Preserve the referenced
        // type instead of degrading the field to std.json.Value.
        if (schema.nullable and
            schema.schema_type == null and
            schema.all_of.len == 1 and
            schema.one_of.len == 0 and
            schema.any_of.len == 0 and
            schema.properties.count() == 0)
        {
            return self.zigTypeForSchemaOrRef(schema.all_of[0]);
        }

        if (try self.nullableOneOfType(schema.one_of)) |inner_type| {
            return std.fmt.allocPrint(self.arena, "?{s}", .{inner_type});
        }

        if (schemaHasMultipleNonNullTypes(schema)) return "std.json.Value";

        // String enum → will be a named type, but when used inline just emit Value
        if (schema.enum_values.len > 0) {
            return "[]const u8"; // unnamed enums default to string
        }

        const type_str = schema.primaryType() orelse return "std.json.Value";

        if (std.mem.eql(u8, type_str, "string")) {
            return "[]const u8";
        } else if (std.mem.eql(u8, type_str, "integer")) {
            if (schema.format) |fmt| {
                if (std.mem.eql(u8, fmt, "int32")) return "i32";
                if (std.mem.eql(u8, fmt, "int64")) return "i64";
            }
            return "i64";
        } else if (std.mem.eql(u8, type_str, "number")) {
            if (schema.format) |fmt| {
                if (std.mem.eql(u8, fmt, "float")) return "f32";
            }
            return "f64";
        } else if (std.mem.eql(u8, type_str, "boolean")) {
            return "bool";
        } else if (std.mem.eql(u8, type_str, "array")) {
            if (schema.items) |items| {
                const inner = try self.zigTypeForSchemaOrRef(items.*);
                return std.fmt.allocPrint(self.arena, "[]const {s}", .{inner});
            }
            return "[]const std.json.Value";
        } else if (std.mem.eql(u8, type_str, "object")) {
            if (schema.additional_properties) |ap| {
                switch (ap) {
                    .boolean => |allowed| {
                        if (allowed) return "std.json.ArrayHashMap(std.json.Value)";
                        if (schema.properties.count() == 0) return "struct {}";
                    },
                    .schema => |s| {
                        const inner = try self.zigTypeForSchemaOrRef(s.*);
                        return std.fmt.allocPrint(self.arena, "std.json.ArrayHashMap({s})", .{inner});
                    },
                }
            }
            if (schema.properties.count() > 0) {
                // Named inline object — would need anonymous struct generation
                return "std.json.Value";
            }
            return "std.json.ArrayHashMap(std.json.Value)";
        }

        return "std.json.Value";
    }

    fn nullableOneOfType(self: *TypeGenerator, members: []const types.SchemaOrRef) GenError!?[]const u8 {
        const inner = nullableOneOfInner(members) orelse return null;
        const inner_type = try self.zigTypeForSchemaOrRef(inner);
        return inner_type;
    }

    fn nullableOneOfInner(members: []const types.SchemaOrRef) ?types.SchemaOrRef {
        if (members.len != 2) return null;

        var saw_null = false;
        var inner: ?types.SchemaOrRef = null;
        for (members) |member| switch (member) {
            .schema => |schema| {
                if (isNullOnlySchema(schema)) {
                    if (saw_null) return null;
                    saw_null = true;
                } else {
                    if (inner != null) return null;
                    inner = member;
                }
            },
            .ref => {
                if (inner != null) return null;
                inner = member;
            },
        };
        if (!saw_null) return null;
        return inner;
    }

    fn isNullOnlySchema(schema: types.Schema) bool {
        return schema.enum_has_null and
            schema.enum_values.len == 0 and
            schema.schema_type == null and
            schema.all_of.len == 0 and
            schema.one_of.len == 0 and
            schema.any_of.len == 0 and
            schema.properties.count() == 0 and
            schema.items == null and
            schema.additional_properties == null;
    }
};

test "zigTypeForSchema primitives" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);

    try std.testing.expectEqualStrings("[]const u8", try gen.zigTypeForSchema(.{ .schema_type = .{ .single = "string" } }));
    try std.testing.expectEqualStrings("i64", try gen.zigTypeForSchema(.{ .schema_type = .{ .single = "integer" } }));
    try std.testing.expectEqualStrings("i32", try gen.zigTypeForSchema(.{ .schema_type = .{ .single = "integer" }, .format = "int32" }));
    try std.testing.expectEqualStrings("f64", try gen.zigTypeForSchema(.{ .schema_type = .{ .single = "number" } }));
    try std.testing.expectEqualStrings("f32", try gen.zigTypeForSchema(.{ .schema_type = .{ .single = "number" }, .format = "float" }));
    try std.testing.expectEqualStrings("bool", try gen.zigTypeForSchema(.{ .schema_type = .{ .single = "boolean" } }));
    try std.testing.expectEqualStrings("Status", try gen.zigTypeForSchema(.{
        .nullable = true,
        .all_of = &.{.{ .ref = .{ .ref_string = "#/components/schemas/Status" } }},
    }));
    try std.testing.expectEqualStrings("std.json.Value", try gen.zigTypeForSchema(.{
        .all_of = &.{.{ .ref = .{ .ref_string = "#/components/schemas/Status" } }},
    }));

    // 3.1 type arrays should also work
    try std.testing.expectEqualStrings("[]const u8", try gen.zigTypeForSchema(.{ .schema_type = .{ .array = &.{ "string", "null" } } }));
    try std.testing.expectEqualStrings("i64", try gen.zigTypeForSchema(.{ .schema_type = .{ .array = &.{ "integer", "null" } } }));
    try std.testing.expectEqualStrings("std.json.Value", try gen.zigTypeForSchema(.{
        .schema_type = .{ .array = &.{ "string", "integer", "null" } },
    }));
}

test "free-form objects preserve arbitrary fields and the object wire kind" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "DocumentQuery", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .additional_properties = .{ .boolean = true },
    } });
    try schemas.put(arena, "ImplicitDocument", .{ .schema = .{
        .schema_type = .{ .single = "object" },
    } });
    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);

    try std.testing.expectEqualStrings(
        "std.json.ArrayHashMap(std.json.Value)",
        try gen.zigTypeForSchema(.{
            .schema_type = .{ .single = "object" },
            .additional_properties = .{ .boolean = true },
        }),
    );
    try std.testing.expectEqualStrings(
        "struct {}",
        try gen.zigTypeForSchema(.{
            .schema_type = .{ .single = "object" },
            .additional_properties = .{ .boolean = false },
        }),
    );
    try std.testing.expect(std.mem.indexOf(
        u8,
        w.toSlice(),
        "pub const DocumentQuery = std.json.ArrayHashMap(std.json.Value);",
    ) != null);
    try std.testing.expect(std.mem.indexOf(
        u8,
        w.toSlice(),
        "pub const ImplicitDocument = std.json.ArrayHashMap(std.json.Value);",
    ) != null);
}

test "explicit Zig raw JSON override wins over nested structural unions" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var leaf_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try leaf_props.put(arena, "term", .{ .schema = .{ .schema_type = .{ .single = "string" } } });
    var override_extensions = std.StringArrayHashMapUnmanaged([]const u8){};
    try override_extensions.put(arena, "x-zig-type", "std.json.Value");

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Term", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = leaf_props,
        .required = &.{"term"},
    } });
    try schemas.put(arena, "Query", .{ .schema = .{
        .one_of = &.{.{ .ref = .{ .ref_string = "#/components/schemas/Term" } }},
    } });
    try schemas.put(arena, "RawQuery", .{ .schema = .{
        .one_of = &.{.{ .ref = .{ .ref_string = "#/components/schemas/Query" } }},
        .extensions = override_extensions,
    } });

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);

    try std.testing.expect(std.mem.indexOf(u8, w.toSlice(), "pub const RawQuery = std.json.Value;") != null);
    try std.testing.expect(std.mem.indexOf(u8, w.toSlice(), "pub const RawQuery = union(enum) {") == null);
}

test "raw_json override emits an owned streaming JSON type" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var override_extensions = std.StringArrayHashMapUnmanaged([]const u8){};
    try override_extensions.put(arena, "x-zig-type", "raw_json");
    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "RawQuery", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .extensions = override_extensions,
    } });

    const doc = types.OpenApiDoc{
        .openapi = "3.1.0",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();

    try std.testing.expect(std.mem.indexOf(u8, output, "pub const RawQuery = OpenApiRawJson;") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bytes: []const u8,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "copyValue(allocator, source, options, &stringify)") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.beginWriteRaw();") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.writer.writeAll(self.bytes);") != null);
}

test "nullable raw JSON override distinguishes required null from optional absence" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var override_extensions = std.StringArrayHashMapUnmanaged([]const u8){};
    try override_extensions.put(arena, "x-zig-type", "std.json.Value");
    var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try props.put(arena, "required_raw", .{
        .ref = .{ .ref_string = "#/components/schemas/NullableRaw" },
    });
    try props.put(arena, "optional_raw", .{
        .ref = .{ .ref_string = "#/components/schemas/NullableRaw" },
    });
    try props.put(arena, "required_inline_raw", .{ .schema = .{
        .nullable = true,
        .extensions = override_extensions,
    } });
    try props.put(arena, "optional_inline_raw", .{ .schema = .{
        .nullable = true,
        .extensions = override_extensions,
    } });
    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "NullableRaw", .{ .schema = .{
        .nullable = true,
        .extensions = override_extensions,
    } });
    try schemas.put(arena, "Envelope", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = props,
        .required = &.{ "required_raw", "required_inline_raw" },
    } });

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);

    const output = w.toSlice();
    try std.testing.expect(std.mem.indexOf(u8, output, "pub const NullableRaw = std.json.Value;") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "required_raw: NullableRaw,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "optional_raw: OpenApiOptionalNullable(NullableRaw) = .absent,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "required_inline_raw: std.json.Value,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "optional_inline_raw: OpenApiOptionalNullable(std.json.Value) = .absent,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.write(self.required_raw);") != null);
}

test "nullable helper allocation preserves colliding component names" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Flexible", .{ .schema = .{
        .schema_type = .{ .array = &.{ "string", "integer", "null" } },
    } });
    try schemas.put(arena, "FlexibleValue", .{ .schema = .{
        .schema_type = .{ .single = "integer" },
    } });

    const doc = types.OpenApiDoc{
        .openapi = "3.1.0",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);

    const output = w.toSlice();
    try std.testing.expect(std.mem.indexOf(u8, output, "pub const FlexibleValue = i64;") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "pub const FlexibleValue2 = std.json.Value;") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "pub const Flexible = ?FlexibleValue2;") != null);
    try std.testing.expectEqualStrings("FlexibleValue2", gen.extra_type_reexports.items[0]);
}

test "named typed map preserves additional property values" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const value_schema = try arena.create(types.SchemaOrRef);
    value_schema.* = .{ .ref = .{ .ref_string = "#/components/schemas/Node" } };
    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "NodeMap", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .additional_properties = .{ .schema = value_schema },
    } });
    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);

    try std.testing.expect(std.mem.indexOf(
        u8,
        w.toSlice(),
        "pub const NodeMap = std.json.ArrayHashMap(Node);",
    ) != null);
}

test "named nullable oneOf remains typed inside additional properties" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const nullable_members = try arena.alloc(types.SchemaOrRef, 2);
    nullable_members[0] = .{ .ref = .{ .ref_string = "#/components/schemas/Node" } };
    nullable_members[1] = .{ .schema = .{ .enum_has_null = true } };
    const map_value = try arena.create(types.SchemaOrRef);
    map_value.* = .{ .ref = .{ .ref_string = "#/components/schemas/NullableNode" } };

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "NullableNode", .{ .schema = .{ .one_of = nullable_members } });
    try schemas.put(arena, "NodeMap", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .additional_properties = .{ .schema = map_value },
    } });
    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);

    const output = w.toSlice();
    try std.testing.expect(std.mem.indexOf(u8, output, "pub const NullableNode = ?Node;") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "pub const NodeMap = std.json.ArrayHashMap(NullableNode);") != null);
}

test "required + nullable field codegen" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const nullable_string_members = try arena.alloc(types.SchemaOrRef, 2);
    nullable_string_members[0] = .{ .schema = .{ .schema_type = .{ .single = "string" } } };
    nullable_string_members[1] = .{ .schema = .{ .enum_has_null = true } };

    // Build a schema with required+nullable fields in every supported spelling
    // plus an ordinary optional field.
    var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try props.put(arena, "name", types.SchemaOrRef{
        .schema = types.Schema{ .schema_type = .{ .single = "string" } },
    });
    try props.put(arena, "tag", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .array = &.{ "string", "null" } },
            .description = "A nullable required tag",
        },
    });
    try props.put(arena, "note", types.SchemaOrRef{
        .schema = types.Schema{ .schema_type = .{ .single = "string" } },
    });
    // 3.0-style nullable + required
    try props.put(arena, "old_tag", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .single = "string" },
            .nullable = true,
        },
    });
    try props.put(arena, "one_of_tag", .{
        .schema = .{ .one_of = nullable_string_members },
    });
    try props.put(arena, "referenced_tag", .{
        .ref = .{ .ref_string = "#/components/schemas/NullableString" },
    });
    try props.put(arena, "aliased_referenced_tag", .{
        .ref = .{ .ref_string = "#/components/schemas/NullableStringAlias" },
    });
    try props.put(arena, "legacy_referenced_tag", .{
        .ref = .{ .ref_string = "#/components/schemas/LegacyNullableString" },
    });
    try props.put(arena, "modern_referenced_tag", .{
        .ref = .{ .ref_string = "#/components/schemas/ModernNullableString" },
    });
    try props.put(arena, "optional_legacy_referenced_tag", .{
        .ref = .{ .ref_string = "#/components/schemas/LegacyNullableString" },
    });

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "NullableString", .{
        .schema = .{ .one_of = nullable_string_members },
    });
    try schemas.put(arena, "NullableStringAlias", .{
        .ref = .{ .ref_string = "#/components/schemas/NullableString" },
    });
    try schemas.put(arena, "LegacyNullableString", .{
        .schema = .{ .schema_type = .{ .single = "string" }, .nullable = true },
    });
    try schemas.put(arena, "ModernNullableString", .{
        .schema = .{ .schema_type = .{ .array = &.{ "string", "null" } } },
    });
    try schemas.put(arena, "Item", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .single = "object" },
            .properties = props,
            .required = &.{ "name", "tag", "old_tag", "one_of_tag", "referenced_tag", "aliased_referenced_tag", "legacy_referenced_tag", "modern_referenced_tag" },
        },
    });

    const doc = types.OpenApiDoc{
        .openapi = "3.1.0",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = types.Components{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();

    // Required non-nullable: `name: []const u8,`
    try std.testing.expect(std.mem.indexOf(u8, output, "name: []const u8,") != null);
    // Required + nullable (3.1 type array): `tag: ?[]const u8,` (no default)
    try std.testing.expect(std.mem.indexOf(u8, output, "tag: ?[]const u8,") != null);
    // Required + nullable should NOT have `= null` default
    try std.testing.expect(std.mem.indexOf(u8, output, "tag: ?[]const u8 = null") == null);
    // Optional field: `note: ?[]const u8 = null,`
    try std.testing.expect(std.mem.indexOf(u8, output, "note: ?[]const u8 = null,") != null);
    // Required + nullable (3.0 style): `old_tag: ?[]const u8,` (no default)
    try std.testing.expect(std.mem.indexOf(u8, output, "old_tag: ?[]const u8,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "old_tag: ?[]const u8 = null") == null);
    // Nullable oneOf fields already carry their optional marker in the Zig
    // type, including through a named component reference.
    try std.testing.expect(std.mem.indexOf(u8, output, "one_of_tag: ?[]const u8,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "referenced_tag: NullableString,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "aliased_referenced_tag: NullableStringAlias,") != null);
    // Named 3.0 and 3.1 nullable components retain nullability in their public
    // type, so required refs can represent null and optional refs avoid ??T.
    try std.testing.expect(std.mem.indexOf(u8, output, "pub const LegacyNullableStringValue = []const u8;") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "pub const LegacyNullableString = ?LegacyNullableStringValue;") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "pub const ModernNullableStringValue = []const u8;") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "pub const ModernNullableString = ?ModernNullableStringValue;") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "legacy_referenced_tag: LegacyNullableString,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "modern_referenced_tag: ModernNullableString,") != null);
    try std.testing.expect(std.mem.indexOf(
        u8,
        output,
        "optional_legacy_referenced_tag: OpenApiOptionalNullable(std.meta.Child(LegacyNullableString)) = .absent,",
    ) != null);
    // Doc comment from nullable field description
    try std.testing.expect(std.mem.indexOf(u8, output, "/// A nullable required tag") != null);
    // Request serialization must preserve required nulls while omitting absent
    // optional fields.
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.objectField(\"tag\");") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.write(self.tag);") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.objectField(\"one_of_tag\");") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.write(self.one_of_tag);") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.objectField(\"referenced_tag\");") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.write(self.referenced_tag);") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.objectField(\"aliased_referenced_tag\");") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.write(self.aliased_referenced_tag);") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.objectField(\"legacy_referenced_tag\");") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.write(self.legacy_referenced_tag);") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.objectField(\"modern_referenced_tag\");") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.write(self.modern_referenced_tag);") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "if (self.note) |value|") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "switch (self.optional_legacy_referenced_tag)") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, ".null_value => {") != null);
    // `note` is non-nullable, so writer presentation options cannot turn its
    // absent state into an invalid explicit null on the wire.
    try std.testing.expect(std.mem.indexOf(u8, output, "else if (jw.options.emit_null_optional_fields)") == null);
}

test "component schema emission order is lexical" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var zeta_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try zeta_props.put(arena, "name", .{
        .schema = .{ .schema_type = .{ .single = "string" } },
    });

    var alpha_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try alpha_props.put(arena, "zeta", .{
        .ref = .{ .ref_string = "#/components/schemas/Zeta" },
    });

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Zeta", .{
        .schema = .{
            .schema_type = .{ .single = "object" },
            .properties = zeta_props,
        },
    });
    try schemas.put(arena, "Alpha", .{
        .schema = .{
            .schema_type = .{ .single = "object" },
            .properties = alpha_props,
        },
    });

    const doc = types.OpenApiDoc{
        .openapi = "3.1.0",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();

    const alpha_pos = std.mem.indexOf(u8, output, "pub const Alpha = struct {").?;
    const zeta_pos = std.mem.indexOf(u8, output, "pub const Zeta = struct {").?;
    try std.testing.expect(alpha_pos < zeta_pos);
}

test "inline index_type discriminator struct fields generate named enum types" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try props.put(arena, "index_type", .{
        .schema = .{
            .schema_type = .{ .single = "string" },
            .enum_values = &.{"full_text"},
            .description = "The index kind.",
        },
    });
    try props.put(arena, "total_indexed", .{
        .schema = .{ .schema_type = .{ .single = "integer" } },
    });

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "FullTextIndexStats", .{
        .schema = .{
            .schema_type = .{ .single = "object" },
            .properties = props,
            .required = &.{"index_type"},
        },
    });

    const doc = types.OpenApiDoc{
        .openapi = "3.1.0",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = types.Components{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();

    try std.testing.expect(std.mem.indexOf(u8, output, "pub const FullTextIndexStatsIndexType = enum {") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, ".full_text => \"full_text\",") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "index_type: FullTextIndexStatsIndexType,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "index_type: []const u8,") == null);
}

test "$ref with description sibling codegen" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try props.put(arena, "status", types.SchemaOrRef{
        .ref = .{
            .ref_string = "#/components/schemas/Status",
            .description = "Current status of the item",
        },
    });

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Status", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .single = "string" },
            .enum_values = &.{ "active", "inactive" },
        },
    });
    try schemas.put(arena, "Item", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .single = "object" },
            .properties = props,
        },
    });

    const doc = types.OpenApiDoc{
        .openapi = "3.1.0",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = types.Components{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();

    // $ref description sibling should appear as doc comment
    try std.testing.expect(std.mem.indexOf(u8, output, "/// Current status of the item") != null);
    // Field should reference the named type
    try std.testing.expect(std.mem.indexOf(u8, output, "status: ?Status = null,") != null);
}

test "discriminated union emits raw fast path and one optional-discriminator fallback" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var current_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try current_props.put(arena, "kind", .{ .schema = .{
        .schema_type = .{ .single = "string" },
        .enum_values = &.{"current"},
    } });
    var legacy_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try legacy_props.put(arena, "kind", .{ .schema = .{
        .schema_type = .{ .single = "string" },
        .enum_values = &.{"legacy"},
    } });
    try legacy_props.put(arena, "type", .{ .schema = .{ .schema_type = .{ .single = "string" } } });

    var mapping = std.StringArrayHashMapUnmanaged([]const u8){};
    try mapping.put(arena, "current", "#/components/schemas/Current");
    try mapping.put(arena, "legacy", "#/components/schemas/Legacy");
    try mapping.put(arena, "inherited", "#/components/schemas/Inherited");
    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Current", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = current_props,
        .required = &.{"kind"},
    } });
    try schemas.put(arena, "Legacy", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = legacy_props,
        .required = &.{"type"},
    } });
    try schemas.put(arena, "InheritedBase", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = current_props,
        .required = &.{"kind"},
    } });
    try schemas.put(arena, "Inherited", .{ .schema = .{
        .all_of = &.{.{ .ref = .{ .ref_string = "#/components/schemas/InheritedBase" } }},
    } });
    try schemas.put(arena, "Result", .{ .schema = .{
        .one_of = &.{
            .{ .ref = .{ .ref_string = "#/components/schemas/Current" } },
            .{ .ref = .{ .ref_string = "#/components/schemas/Legacy" } },
            .{ .ref = .{ .ref_string = "#/components/schemas/Inherited" } },
        },
        .discriminator = .{ .property_name = "kind", .mapping = mapping },
    } });
    const doc = types.OpenApiDoc{
        .openapi = "3.1.0",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();
    try std.testing.expect(std.mem.indexOf(u8, output, "pub fn jsonParseFromSliceLeaky") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "const DiscriminatorProbe = union(enum)") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "kind: DiscriminatorProbe = .missing") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, ".legacy = try std.json.parseFromSliceLeaky(Legacy") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, ".legacy = try std.json.parseFromValueLeaky(Legacy") != null);
}

test "undiscriminated recursive oneOf generates structural union" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "MatchQuery", .{
        .schema = .{
            .schema_type = .{ .single = "object" },
            .properties = blk: {
                var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
                try props.put(arena, "match", .{ .schema = .{ .schema_type = .{ .single = "string" } } });
                break :blk props;
            },
            .required = &.{"match"},
        },
    });
    try schemas.put(arena, "BooleanQuery", .{
        .schema = .{
            .schema_type = .{ .single = "object" },
            .properties = blk: {
                var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
                try props.put(arena, "filter", .{ .ref = .{ .ref_string = "#/components/schemas/Query" } });
                break :blk props;
            },
        },
    });
    try schemas.put(arena, "Query", .{
        .schema = .{
            .one_of = &.{
                .{ .ref = .{ .ref_string = "#/components/schemas/MatchQuery" } },
                .{ .ref = .{ .ref_string = "#/components/schemas/BooleanQuery" } },
            },
        },
    });

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();

    try std.testing.expect(std.mem.indexOf(u8, output, "pub const Query = union(enum) {") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "match_query: *MatchQuery,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "boolean_query: *BooleanQuery,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "pub fn jsonParse(allocator: std.mem.Allocator, source: anytype") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "if (objectHasAnyKey(source.object, &.{") != null);
}

test "structural union flattens transparent nested oneOf refs" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var nodes_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try nodes_props.put(arena, "kind", .{ .schema = .{
        .schema_type = .{ .single = "string" },
        .enum_values = &.{"nodes"},
    } });
    var bindings_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try bindings_props.put(arena, "kind", .{ .schema = .{
        .schema_type = .{ .single = "string" },
        .enum_values = &.{"bindings"},
    } });
    var legacy_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try legacy_props.put(arena, "kind", .{ .schema = .{
        .schema_type = .{ .single = "string" },
        .enum_values = &.{"legacy"},
    } });
    try legacy_props.put(arena, "type", .{ .schema = .{ .schema_type = .{ .single = "string" } } });

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Nodes", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = nodes_props,
        .required = &.{"kind"},
    } });
    try schemas.put(arena, "Bindings", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = bindings_props,
        .required = &.{"kind"},
    } });
    try schemas.put(arena, "Canonical", .{ .schema = .{
        .one_of = &.{
            .{ .ref = .{ .ref_string = "#/components/schemas/Nodes" } },
            .{ .ref = .{ .ref_string = "#/components/schemas/Bindings" } },
        },
    } });
    try schemas.put(arena, "Legacy", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = legacy_props,
        .required = &.{"type"},
    } });
    try schemas.put(arena, "Result", .{ .schema = .{
        .one_of = &.{
            .{ .ref = .{ .ref_string = "#/components/schemas/Canonical" } },
            .{ .ref = .{ .ref_string = "#/components/schemas/Legacy" } },
        },
    } });

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();

    try std.testing.expect(std.mem.indexOf(u8, output, "pub const Result = union(enum) {") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "nodes: *Nodes,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bindings: *Bindings,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "legacy: *Legacy,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "pub const Result = std.json.Value;") == null);
}

test "structural union infers allocation-light enum selector with legacy fallback" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var current_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try current_props.put(arena, "kind", .{ .schema = .{
        .schema_type = .{ .single = "string" },
        .enum_values = &.{"cur\"rent"},
    } });
    try current_props.put(arena, "rows", .{ .schema = .{ .schema_type = .{ .single = "array" } } });
    var legacy_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try legacy_props.put(arena, "kind", .{ .schema = .{
        .schema_type = .{ .single = "string" },
        .enum_values = &.{"legacy"},
    } });
    try legacy_props.put(arena, "type", .{ .schema = .{ .schema_type = .{ .single = "string" } } });

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Current", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = current_props,
        .required = &.{ "kind", "rows" },
    } });
    try schemas.put(arena, "Legacy", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = legacy_props,
        .required = &.{"type"},
    } });
    try schemas.put(arena, "Result", .{ .schema = .{
        .one_of = &.{
            .{ .ref = .{ .ref_string = "#/components/schemas/Current" } },
            .{ .ref = .{ .ref_string = "#/components/schemas/Legacy" } },
        },
    } });
    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();

    try std.testing.expect(std.mem.indexOf(u8, output, "pub fn jsonParseFromSliceLeaky") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "const DiscriminatorProbe = union(enum)") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "kind: DiscriminatorProbe = .missing") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "disc_str, \"cur\\\"rent\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, ".legacy = try parseStructuralVariantFromSlice(Legacy") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "const disc_val = source.object.get(\"kind\")") != null);
}

test "structural union prefers required singleton enum over broader object" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var row_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try row_props.put(arena, "count", .{ .schema = .{
        .schema_type = .{ .single = "string" },
        .enum_values = &.{"*"},
    } });
    var alias_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try alias_props.put(arena, "count", .{ .schema = .{ .schema_type = .{ .single = "string" } } });
    try alias_props.put(arena, "distinct", .{ .schema = .{ .schema_type = .{ .single = "boolean" } } });

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "RowCount", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = row_props,
        .required = &.{"count"},
    } });
    try schemas.put(arena, "AliasCount", .{ .schema = .{
        .schema_type = .{ .single = "object" },
        .properties = alias_props,
        .required = &.{"count"},
    } });
    try schemas.put(arena, "Count", .{ .schema = .{
        .one_of = &.{
            .{ .ref = .{ .ref_string = "#/components/schemas/AliasCount" } },
            .{ .ref = .{ .ref_string = "#/components/schemas/RowCount" } },
        },
    } });

    const doc = types.OpenApiDoc{
        .openapi = "3.1.0",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();

    const row_dispatch = std.mem.indexOf(u8, output, "parseStructuralVariant(RowCount").?;
    const alias_dispatch = std.mem.indexOf(u8, output, "parseStructuralVariant(AliasCount").?;
    try std.testing.expect(row_dispatch < alias_dispatch);
    try std.testing.expect(std.mem.indexOf(
        u8,
        output,
        "objectStringEquals(source.object, \"count\", \"*\")",
    ) != null);
}

test "allOf flattens nested oneOf member properties into struct" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "ChunkOptions", .{
        .schema = .{
            .schema_type = .{ .single = "object" },
            .properties = blk: {
                var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
                try props.put(arena, "max_chunks", .{ .schema = .{ .schema_type = .{ .single = "integer" } } });
                break :blk props;
            },
        },
    });
    try schemas.put(arena, "AntflyChunkerConfig", .{
        .schema = .{
            .all_of = &.{
                .{ .ref = .{ .ref_string = "#/components/schemas/ChunkOptions" } },
                .{ .schema = .{
                    .schema_type = .{ .single = "object" },
                    .properties = blk: {
                        var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
                        try props.put(arena, "api_url", .{ .schema = .{ .schema_type = .{ .single = "string" } } });
                        try props.put(arena, "model", .{ .schema = .{ .schema_type = .{ .single = "string" } } });
                        break :blk props;
                    },
                    .required = &.{"model"},
                } },
            },
        },
    });
    try schemas.put(arena, "ChunkerConfig", .{
        .schema = .{
            .all_of = &.{
                .{ .schema = .{
                    .one_of = &.{
                        .{ .ref = .{ .ref_string = "#/components/schemas/AntflyChunkerConfig" } },
                    },
                } },
                .{ .schema = .{
                    .schema_type = .{ .single = "object" },
                    .properties = blk: {
                        var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
                        try props.put(arena, "provider", .{ .schema = .{ .schema_type = .{ .single = "string" } } });
                        try props.put(arena, "store_chunks", .{ .schema = .{ .schema_type = .{ .single = "boolean" } } });
                        break :blk props;
                    },
                    .required = &.{"provider"},
                } },
            },
        },
    });

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();

    try std.testing.expect(std.mem.indexOf(u8, output, "pub const ChunkerConfig = struct {") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "provider: []const u8,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "store_chunks: ?bool = null,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "max_chunks: ?i64 = null,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "api_url: ?[]const u8 = null,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "model: ?[]const u8 = null,") != null);
}

test "object schema with base properties and oneOf variants flattens variant fields" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "FullTextIndexConfig", .{
        .schema = .{
            .schema_type = .{ .single = "object" },
            .properties = blk: {
                var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
                try props.put(arena, "mem_only", .{ .schema = .{ .schema_type = .{ .single = "boolean" } } });
                break :blk props;
            },
        },
    });
    try schemas.put(arena, "EmbeddingsIndexConfig", .{
        .schema = .{
            .schema_type = .{ .single = "object" },
            .properties = blk: {
                var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
                try props.put(arena, "template", .{ .schema = .{ .schema_type = .{ .single = "string" } } });
                try props.put(arena, "embedder", .{ .schema = .{ .schema_type = .{ .single = "object" } } });
                try props.put(arena, "chunker", .{ .schema = .{ .schema_type = .{ .single = "object" } } });
                break :blk props;
            },
        },
    });
    try schemas.put(arena, "IndexConfig", .{
        .schema = .{
            .schema_type = .{ .single = "object" },
            .one_of = &.{
                .{ .ref = .{ .ref_string = "#/components/schemas/FullTextIndexConfig" } },
                .{ .ref = .{ .ref_string = "#/components/schemas/EmbeddingsIndexConfig" } },
            },
            .required = &.{ "name", "type" },
            .properties = blk: {
                var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
                try props.put(arena, "name", .{ .schema = .{ .schema_type = .{ .single = "string" } } });
                try props.put(arena, "type", .{ .schema = .{ .schema_type = .{ .single = "string" } } });
                break :blk props;
            },
        },
    });

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };
    var resolver = Resolver.init(arena, &doc);
    var w = SourceWriter.init(arena);
    var gen = TypeGenerator.init(arena, &w, &resolver);
    try gen.generateAll(&doc);
    const output = w.toSlice();

    try std.testing.expect(std.mem.indexOf(u8, output, "pub const IndexConfig = struct {") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "name: []const u8,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "@\"type\": []const u8,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "mem_only: ?bool = null,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "template: ?[]const u8 = null,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "embedder: ?std.json.ArrayHashMap(std.json.Value) = null,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "chunker: ?std.json.ArrayHashMap(std.json.Value) = null,") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "pub fn jsonStringify(self: @This(), jw: anytype) !void {") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "try jw.write(self.@\"type\");") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "if (self.template) |value| {") != null);
}
