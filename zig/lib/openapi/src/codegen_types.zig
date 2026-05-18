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

    pub fn init(arena: Allocator, w: *SourceWriter, resolver: *Resolver) TypeGenerator {
        return .{ .arena = arena, .w = w, .resolver = resolver };
    }

    /// Generate all types from components/schemas in topological order.
    pub fn generateAll(self: *TypeGenerator, doc: *const types.OpenApiDoc) !void {
        const components = doc.components orelse return;
        const schemas = components.schemas;

        // Build dependency graph and emit in topological order
        const order = try self.topologicalSort(schemas);

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
    }

    /// Topological sort of schemas by dependency order.
    /// Schemas with no dependencies come first. Cycles are broken arbitrarily
    /// (Zig handles forward references to named types).
    fn topologicalSort(self: *TypeGenerator, schemas: std.StringArrayHashMapUnmanaged(types.SchemaOrRef)) ![]const []const u8 {
        const names = schemas.keys();

        // Build name → index map
        var name_index = std.StringArrayHashMapUnmanaged(usize){};
        for (names, 0..) |name, i| {
            try name_index.put(self.arena, name, i);
        }

        // Build in-degree counts
        var in_degree = try self.arena.alloc(usize, names.len);
        @memset(in_degree, 0);

        // For each schema, find which other schemas it references
        var reverse_deps = try self.arena.alloc(std.ArrayListUnmanaged(usize), names.len);
        for (0..names.len) |i| {
            reverse_deps[i] = .empty;
        }

        var refs = std.ArrayListUnmanaged([]const u8).empty;
        for (names, schemas.values(), 0..) |_, sor, i| {
            refs.clearRetainingCapacity();
            try collectRefs(sor, &refs, self.arena);
            for (refs.items) |ref_str| {
                // Skip external refs — they're imported, not defined locally
                if (naming.isExternalRef(ref_str)) continue;
                const ref_name = naming.refToName(ref_str) orelse continue;
                if (name_index.get(ref_name)) |dep_idx| {
                    if (dep_idx != i) {
                        in_degree[i] += 1;
                        try reverse_deps[dep_idx].append(self.arena, i);
                    }
                }
            }
        }

        // Kahn's algorithm with pre-allocated queue
        var queue = std.ArrayListUnmanaged(usize).empty;
        try queue.ensureTotalCapacity(self.arena, names.len);
        for (0..names.len) |i| {
            if (in_degree[i] == 0) queue.appendAssumeCapacity(i);
        }

        var order = std.ArrayListUnmanaged([]const u8).empty;
        try order.ensureTotalCapacity(self.arena, names.len);
        var head: usize = 0;
        while (head < queue.items.len) {
            const idx = queue.items[head];
            head += 1;
            try order.append(self.arena, names[idx]);
            for (reverse_deps[idx].items) |dependent| {
                in_degree[dependent] -= 1;
                if (in_degree[dependent] == 0) try queue.append(self.arena, dependent);
            }
        }

        // Append any remaining schemas (cycles) in original order.
        // Nodes still with in_degree > 0 are in cycles.
        if (order.items.len < names.len) {
            for (in_degree, 0..) |deg, i| {
                if (deg > 0) try order.append(self.arena, names[i]);
            }
        }

        return order.items;
    }

    /// Collect all $ref strings from a SchemaOrRef tree.
    fn collectRefs(sor: types.SchemaOrRef, refs: *std.ArrayListUnmanaged([]const u8), arena: Allocator) !void {
        switch (sor) {
            .ref => |ref| try refs.append(arena, ref.ref_string),
            .schema => |schema| {
                for (schema.properties.values()) |prop| try collectRefs(prop, refs, arena);
                if (schema.items) |items| try collectRefs(items.*, refs, arena);
                for (schema.all_of) |member| try collectRefs(member, refs, arena);
                for (schema.one_of) |member| try collectRefs(member, refs, arena);
                for (schema.any_of) |member| try collectRefs(member, refs, arena);
                if (schema.additional_properties) |ap| {
                    switch (ap) {
                        .schema => |s| try collectRefs(s.*, refs, arena),
                        .boolean => {},
                    }
                }
            },
        }
    }

    /// Generate a named type from a schema.
    fn generateNamedType(self: *TypeGenerator, name: []const u8, schema: types.Schema) !void {
        const type_name = try naming.toTypeName(self.arena, name);

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

        // oneOf/anyOf without discriminator but with top-level properties: emit struct
        // (the properties are the common/shared fields)
        if ((schema.one_of.len > 0 or schema.any_of.len > 0) and schema.properties.count() > 0) {
            try self.generateStruct(type_name, schema);
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
            try self.w.line(".{s} => \"{s}\",", .{ fields[i], val });
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
            try self.w.line(".{{ \"{s}\", .{s} }},", .{ val, fields[i] });
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
        return std.fmt.allocPrint(self.arena, "{s}{s}", .{ owner_type_name, prop_type_name });
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

        // Check if the schema itself declares nullability (3.0 nullable or 3.1 type array)
        const schema_nullable = switch (prop_sor) {
            .schema => |s| s.isNullable(),
            .ref => false,
        };

        if (is_required and !schema_nullable) {
            try self.w.line("{s}: {s},", .{ field, zig_type });
        } else if (is_required and schema_nullable) {
            // Required but nullable: field is present but can be null
            try self.w.line("{s}: ?{s},", .{ field, zig_type });
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

        self.w.dedent();
        try self.w.line("}};", .{});
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

    /// Generate a union(enum) from oneOf with discriminator.
    fn generateDiscriminatedUnion(self: *TypeGenerator, type_name: []const u8, schema: types.Schema) !void {
        const disc = schema.discriminator.?;
        if (schema.description) |desc| try self.w.docComment(desc);
        try self.w.line("pub const {s} = union(enum) {{", .{type_name});
        self.w.indent();

        // Collect variant info for jsonParse generation
        var variants = std.ArrayListUnmanaged(struct { field: []const u8, zig_type: []const u8, disc_value: []const u8 }).empty;

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
                    try variants.append(self.arena, .{
                        .field = variant_name,
                        .zig_type = ref_type,
                        .disc_value = disc_value,
                    });
                },
                .schema => {
                    try self.w.line("// TODO: inline oneOf variant", .{});
                },
            }
        }

        try self.w.blank();

        // jsonParseFromValue: parse from a pre-parsed std.json.Value tree
        if (variants.items.len == 0) {
            try self.generateEmptyUnionJsonStubs();
        } else {
            try self.w.line("pub fn jsonParseFromValue(allocator: std.mem.Allocator, source: std.json.Value, options: std.json.ParseOptions) !@This() {{", .{});
            self.w.indent();
            try self.w.line("if (source != .object) return error.UnexpectedToken;", .{});
            try self.w.line("const disc_val = source.object.get(\"{s}\") orelse return error.MissingField;", .{disc.property_name});
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
                try self.w.line("return .{{ .{s} = try std.json.parseFromValue({s}, allocator, source, options) }};", .{ v.field, v.zig_type });
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

    fn canGenerateStructuralUnion(self: *TypeGenerator, schema: types.Schema) !bool {
        for (schema.one_of) |member| {
            const resolved = self.resolver.resolveSchema(member) catch return false;
            if (resolved.primaryType()) |primary_type| {
                if (!std.mem.eql(u8, primary_type, "object")) return false;
            } else if (resolved.properties.count() == 0) {
                return false;
            }
        }
        return schema.one_of.len > 0;
    }

    const StructuralVariant = struct {
        field: []const u8,
        zig_type: []const u8,
        ref_name: []const u8,
        selector_keys: []const []const u8,
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
        for (resolved.properties.keys()) |prop_name| {
            if (std.mem.eql(u8, prop_name, "boost")) continue;
            if (std.mem.eql(u8, prop_name, "field")) continue;
            try selector_keys.append(self.arena, prop_name);
        }

        return .{
            .field = try naming.zigFieldName(self.arena, ref_name),
            .zig_type = ref_type,
            .ref_name = ref_name,
            .selector_keys = selector_keys.items,
        };
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
            const variant = try self.collectStructuralVariant(member) orelse continue;
            try variants.append(self.arena, variant);
        }

        // Sort: most-specific first (most selector keys), alphabetical tie-break.
        // This order is used for both field layout and jsonParseFromValue dispatch.
        std.sort.pdq(StructuralVariant, variants.items, {}, struct {
            fn lessThan(_: void, a: StructuralVariant, b: StructuralVariant) bool {
                if (a.selector_keys.len != b.selector_keys.len) return a.selector_keys.len > b.selector_keys.len;
                return std.mem.order(u8, a.ref_name, b.ref_name) == .lt;
            }
        }.lessThan);

        for (variants.items) |variant| {
            try self.w.line("{s}: *{s},", .{ variant.field, variant.zig_type });
        }

        try self.w.blank();

        try self.w.line("fn parseStructuralVariant(comptime T: type, allocator: std.mem.Allocator, source: std.json.Value, options: std.json.ParseOptions) !?*T {{", .{});
        self.w.indent();
        try self.w.line("const parsed = std.json.parseFromValue(T, allocator, source, options) catch |err| switch (err) {{", .{});
        self.w.indent();
        try self.w.line("error.OutOfMemory => return err,", .{});
        try self.w.line("else => return null,", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.line("const value = try allocator.create(T);", .{});
        try self.w.line("value.* = parsed.value;", .{});
        try self.w.line("return value;", .{});
        self.w.dedent();
        try self.w.line("}}", .{});

        try self.w.blank();

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
            try self.w.line("pub fn jsonParseFromValue(allocator: std.mem.Allocator, source: std.json.Value, options: std.json.ParseOptions) !@This() {{", .{});
            self.w.indent();
            try self.w.line("if (source != .object) return error.UnexpectedToken;", .{});

            for (variants.items) |variant| {
                if (variant.selector_keys.len == 0) continue;
                try self.w.line("if (objectHasAnyKey(source.object, &.{{", .{});
                self.w.indent();
                for (variant.selector_keys) |selector_key| {
                    try self.w.line("\"{s}\",", .{selector_key});
                }
                self.w.dedent();
                try self.w.line("}})) {{", .{});
                self.w.indent();
                try self.w.line("if (try parseStructuralVariant({s}, allocator, source, options)) |parsed| return .{{ .{s} = parsed }};", .{ variant.zig_type, variant.field });
                self.w.dedent();
                try self.w.line("}}", .{});
            }

            for (variants.items) |variant| {
                if (variant.selector_keys.len > 0) continue;
                try self.w.line("if (try parseStructuralVariant({s}, allocator, source, options)) |parsed| return .{{ .{s} = parsed }};", .{ variant.zig_type, variant.field });
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

    const GenError = error{OutOfMemory};

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
        }

        return type_name;
    }

    /// Get the Zig type string for an inline schema.
    fn zigTypeForSchema(self: *TypeGenerator, schema: types.Schema) GenError![]const u8 {
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
                    .boolean => return "std.json.Value",
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
            return "std.json.Value";
        }

        return "std.json.Value";
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

    // 3.1 type arrays should also work
    try std.testing.expectEqualStrings("[]const u8", try gen.zigTypeForSchema(.{ .schema_type = .{ .array = &.{ "string", "null" } } }));
    try std.testing.expectEqualStrings("i64", try gen.zigTypeForSchema(.{ .schema_type = .{ .array = &.{ "integer", "null" } } }));
}

test "required + nullable field codegen" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    // Build a schema with required+nullable (3.1 style) and optional fields
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

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Item", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .single = "object" },
            .properties = props,
            .required = &.{ "name", "tag", "old_tag" },
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
    // Doc comment from nullable field description
    try std.testing.expect(std.mem.indexOf(u8, output, "/// A nullable required tag") != null);
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
    try std.testing.expect(std.mem.indexOf(u8, output, "if (objectHasAnyKey(source.object, &.{") != null);
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
    try schemas.put(arena, "TermiteChunkerConfig", .{
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
    try schemas.put(arena, "AntflyChunkerConfig", .{
        .schema = .{
            .all_of = &.{
                .{ .ref = .{ .ref_string = "#/components/schemas/ChunkOptions" } },
            },
        },
    });
    try schemas.put(arena, "ChunkerConfig", .{
        .schema = .{
            .all_of = &.{
                .{ .schema = .{
                    .one_of = &.{
                        .{ .ref = .{ .ref_string = "#/components/schemas/TermiteChunkerConfig" } },
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
