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

//! Code generation orchestrator.
//!
//! Coordinates type, client, and server generation into output files.

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const naming = @import("naming.zig");
const SourceWriter = @import("writer.zig").SourceWriter;
const Resolver = @import("resolver.zig").Resolver;
const TypeGenerator = @import("codegen_types.zig").TypeGenerator;
const ClientGenerator = @import("codegen_client.zig").ClientGenerator;
const ServerGenerator = @import("codegen_server.zig").ServerGenerator;

pub const GenerateOptions = struct {
    package_name: []const u8 = "api",
    generate_types: bool = true,
    generate_client: bool = true,
    generate_server: bool = false,
    /// Generate only framework-agnostic extractors (param structs, body parsers, route table).
    /// Like server but without the httpx-specific ServerRouter layer.
    generate_extractors: bool = false,
    /// Maps external $ref file paths to Zig import module names.
    /// e.g., "../../lib/schema/openapi.yaml" → "schema"
    /// When a $ref points to a mapped file, the generated code will emit
    /// `@import("schema").TypeName` instead of generating the type inline.
    import_mapping: std.StringArrayHashMapUnmanaged([]const u8) = .{},
};

pub const GeneratedFiles = struct {
    root: []const u8,
    types: ?[]const u8 = null,
    client: ?[]const u8 = null,
    server: ?[]const u8 = null,
};

/// Generate all code from a parsed OpenAPI document.
pub fn generate(arena: Allocator, doc: *const types.OpenApiDoc, opts: GenerateOptions) !GeneratedFiles {
    var result = GeneratedFiles{ .root = undefined };

    var resolver = Resolver.init(arena, doc);
    var extra_type_reexports: []const []const u8 = &.{};

    // Generate types
    if (opts.generate_types) {
        var type_w = SourceWriter.init(arena);
        var type_gen = TypeGenerator.init(arena, &type_w, &resolver);
        type_gen.import_mapping = opts.import_mapping;
        try type_gen.generateAll(doc);
        extra_type_reexports = try type_gen.extra_type_reexports.toOwnedSlice(arena);
        result.types = try buildModule(arena, opts.package_name, &.{}, type_gen.used_imports, type_w.toSlice());
    }

    // Generate client
    if (opts.generate_client) {
        var body_w = SourceWriter.init(arena);
        var type_gen = TypeGenerator.init(arena, &body_w, &resolver);
        type_gen.import_mapping = opts.import_mapping;
        var client_gen = ClientGenerator.init(arena, &body_w, &resolver, &type_gen);
        try client_gen.generate(doc);
        result.client = try buildModule(arena, opts.package_name, &.{ .{ "httpx", "httpx" }, .{ "types", "types.zig" } }, type_gen.used_imports, body_w.toSlice());
    }

    // Generate server (full with httpx router) or extractors only (framework-agnostic)
    if (opts.generate_server or opts.generate_extractors) {
        var body_w = SourceWriter.init(arena);
        var type_gen = TypeGenerator.init(arena, &body_w, &resolver);
        type_gen.import_mapping = opts.import_mapping;
        var server_gen = ServerGenerator.init(arena, &body_w, &resolver, &type_gen);
        if (opts.generate_server) {
            try server_gen.generate(doc);
            result.server = try buildModule(arena, opts.package_name, &.{ .{ "httpx", "httpx" }, .{ "types", "types.zig" } }, type_gen.used_imports, body_w.toSlice());
        } else {
            try server_gen.generateExtractorsOnly(doc);
            result.server = try buildModule(arena, opts.package_name, &.{.{ "types", "types.zig" }}, type_gen.used_imports, body_w.toSlice());
        }
    }

    // Generate root module that re-exports everything
    {
        var w = SourceWriter.init(arena);
        try w.writeHeader(opts.package_name);

        if (opts.generate_types) {
            try w.line("pub const types = @import(\"types.zig\");", .{});
        }
        if (opts.generate_client) {
            try w.line("pub const client = @import(\"client.zig\");", .{});
            try w.line("pub const Client = client.Client;", .{});
            try w.line("pub const ApiResponse = client.ApiResponse;", .{});
        }
        if (opts.generate_server) {
            try w.line("pub const server = @import(\"server.zig\");", .{});
            try w.line("pub const ServerRouter = server.ServerRouter;", .{});
        } else if (opts.generate_extractors) {
            try w.line("pub const server = @import(\"server.zig\");", .{});
        }

        try w.blank();

        // Re-export all types at top level for convenience
        if (opts.generate_types) {
            if (doc.components) |components| {
                for (components.schemas.keys()) |schema_name| {
                    const type_name = try naming.toTypeName(arena, schema_name);
                    try w.line("pub const {s} = types.{s};", .{ type_name, type_name });
                }
                for (extra_type_reexports) |type_name| {
                    try w.line("pub const {s} = types.{s};", .{ type_name, type_name });
                }
            }
        }

        result.root = try w.toOwnedSlice();
    }

    return result;
}

/// Build a complete module by prepending a header (package name, fixed imports, used external
/// imports) to an already-generated body. Returns the concatenated output.
/// Each fixed import is a [2][]const u8: { const_name, import_path }.
fn buildModule(
    arena: Allocator,
    package_name: []const u8,
    fixed_imports: []const [2][]const u8,
    used_imports: std.StringArrayHashMapUnmanaged(void),
    body: []const u8,
) ![]const u8 {
    var hdr = SourceWriter.init(arena);
    try hdr.writeHeader(package_name);
    try hdr.line("const std = @import(\"std\");", .{});
    for (fixed_imports) |imp| {
        try hdr.line("const {s} = @import(\"{s}\");", .{ imp[0], imp[1] });
    }
    for (used_imports.keys()) |module_name| {
        try hdr.line("const {s} = @import(\"{s}\");", .{ module_name, module_name });
    }
    try hdr.blank();
    const header = hdr.toSlice();

    const combined = try arena.alloc(u8, header.len + body.len);
    @memcpy(combined[0..header.len], header);
    @memcpy(combined[header.len..], body);
    return combined;
}

test "generate minimal" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Status", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .single = "string" },
            .enum_values = &.{ "healthy", "degraded", "down" },
        },
    });

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = types.Components{
            .schemas = schemas,
        },
    };

    const result = try generate(arena, &doc, .{
        .package_name = "test_api",
        .generate_client = false,
    });

    // Verify types file contains the enum
    try std.testing.expect(result.types != null);
    try std.testing.expect(std.mem.indexOf(u8, result.types.?, "pub const Status = enum {") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.types.?, "healthy,") != null);
}

test "generate root reexports inline index_type discriminator enum field types" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try props.put(arena, "index_type", .{
        .schema = .{
            .schema_type = .{ .single = "string" },
            .enum_values = &.{"full_text"},
        },
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
        .components = .{ .schemas = schemas },
    };

    const result = try generate(arena, &doc, .{
        .package_name = "test_api",
        .generate_client = false,
    });

    try std.testing.expect(std.mem.indexOf(u8, result.types.?, "pub const FullTextIndexStatsIndexType = enum {") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.root, "pub const FullTextIndexStatsIndexType = types.FullTextIndexStatsIndexType;") != null);
}

test "generate 3.1 types with nullable and ref siblings" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    // Build a 3.1-style document: type arrays, $ref with description, required+nullable
    const pet_status_enum = types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .single = "string" },
            .enum_values = &.{ "available", "pending", "adopted" },
        },
    };

    var pet_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try pet_props.put(arena, "id", types.SchemaOrRef{
        .schema = types.Schema{ .schema_type = .{ .single = "integer" }, .format = "int64" },
    });
    try pet_props.put(arena, "name", types.SchemaOrRef{
        .schema = types.Schema{ .schema_type = .{ .single = "string" } },
    });
    // 3.1 type array: nullable optional
    try pet_props.put(arena, "tag", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .array = &.{ "string", "null" } },
            .description = "Optional tag for categorization",
        },
    });
    // $ref with 3.1 description sibling
    try pet_props.put(arena, "status", types.SchemaOrRef{
        .ref = .{
            .ref_string = "#/components/schemas/PetStatus",
            .description = "Current adoption status",
        },
    });

    var err_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try err_props.put(arena, "error", types.SchemaOrRef{
        .schema = types.Schema{ .schema_type = .{ .single = "string" } },
    });
    try err_props.put(arena, "message", types.SchemaOrRef{
        .schema = types.Schema{ .schema_type = .{ .single = "string" } },
    });
    // 3.1 type array: required + nullable
    try err_props.put(arena, "details", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .array = &.{ "object", "null" } },
            .description = "Error detail payload",
        },
    });

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "PetStatus", pet_status_enum);
    try schemas.put(arena, "Pet", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .single = "object" },
            .properties = pet_props,
            .required = &.{ "id", "name" },
        },
    });
    try schemas.put(arena, "Error", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .single = "object" },
            .properties = err_props,
            .required = &.{ "error", "message", "details" },
        },
    });

    const doc = types.OpenApiDoc{
        .openapi = "3.1.0",
        .info = .{ .title = "Petstore", .version = "1.0" },
        .components = types.Components{ .schemas = schemas },
    };

    try std.testing.expectEqual(types.SpecVersion.v3_1, doc.specVersion());

    const result = try generate(arena, &doc, .{
        .package_name = "petstore",
        .generate_client = false,
        .generate_server = false,
    });

    const out = result.types.?;

    // Required non-nullable fields: no `?`, no default
    try std.testing.expect(std.mem.indexOf(u8, out, "id: i64,") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "name: []const u8,") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "@\"error\": []const u8,") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "message: []const u8,") != null);

    // Optional (not required) + nullable type array: `?T = null`
    try std.testing.expect(std.mem.indexOf(u8, out, "tag: ?[]const u8 = null,") != null);

    // Required + nullable (3.1 type array): `?T` with no default
    try std.testing.expect(std.mem.indexOf(u8, out, "details: ?std.json.Value,") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "details: ?std.json.Value = null") == null);

    // $ref description sibling → doc comment on field
    try std.testing.expect(std.mem.indexOf(u8, out, "/// Current adoption status") != null);

    // Regular description on schema field → doc comment
    try std.testing.expect(std.mem.indexOf(u8, out, "/// Optional tag for categorization") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "/// Error detail payload") != null);

    // Enum generated
    try std.testing.expect(std.mem.indexOf(u8, out, "pub const PetStatus = enum {") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "adopted,") != null);

    // Optional $ref field: `?PetStatus = null`
    try std.testing.expect(std.mem.indexOf(u8, out, "status: ?PetStatus = null,") != null);
}

test "generate extractors only" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    // Build a doc with one path that has path params and a request body
    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    var pet_props = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try pet_props.put(arena, "name", types.SchemaOrRef{
        .schema = types.Schema{ .schema_type = .{ .single = "string" } },
    });
    try schemas.put(arena, "Pet", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .single = "object" },
            .properties = pet_props,
            .required = &.{"name"},
        },
    });

    var json_media = std.StringArrayHashMapUnmanaged(types.MediaType){};
    try json_media.put(arena, "application/json", types.MediaType{
        .schema = types.SchemaOrRef{ .ref = .{ .ref_string = "#/components/schemas/Pet" } },
    });

    var responses = std.StringArrayHashMapUnmanaged(types.ResponseOrRef){};
    try responses.put(arena, "200", types.ResponseOrRef{
        .response = types.Response{ .content = json_media },
    });

    var paths = std.StringArrayHashMapUnmanaged(types.PathItem){};
    try paths.put(arena, "/pets/{petId}", types.PathItem{
        .get = types.Operation{
            .operation_id = "getPet",
            .summary = "Get a pet by ID",
            .parameters = &.{types.ParameterOrRef{
                .parameter = types.Parameter{
                    .name = "petId",
                    .in = .path,
                    .required = true,
                },
            }},
            .responses = responses,
        },
        .put = types.Operation{
            .operation_id = "updatePet",
            .parameters = &.{types.ParameterOrRef{
                .parameter = types.Parameter{
                    .name = "petId",
                    .in = .path,
                    .required = true,
                },
            }},
            .request_body = types.RequestBodyOrRef{
                .request_body = types.RequestBody{
                    .required = true,
                    .content = json_media,
                },
            },
            .responses = responses,
        },
    });

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .paths = paths,
        .components = types.Components{ .schemas = schemas },
    };

    const result = try generate(arena, &doc, .{
        .package_name = "test_api",
        .generate_types = true,
        .generate_client = false,
        .generate_server = false,
        .generate_extractors = true,
    });

    // Types should still be generated
    try std.testing.expect(result.types != null);
    try std.testing.expect(std.mem.indexOf(u8, result.types.?, "pub const Pet = struct {") != null);

    // Server file should exist (extractors go in server.zig)
    const srv = result.server orelse return error.TestExpectedNonNull;

    // Should have path params struct
    try std.testing.expect(std.mem.indexOf(u8, srv, "pub const GetPetPathParams = struct {") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv, "pet_id: []const u8,") != null);

    // Should have body parser
    try std.testing.expect(std.mem.indexOf(u8, srv, "pub fn parseUpdatePetBody(") != null);

    // Should have route table
    try std.testing.expect(std.mem.indexOf(u8, srv, "pub const Route = struct {") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv, "pub const routes = [_]Route{") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv, "\"getPet\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv, "\"updatePet\"") != null);

    // Route methods should be uppercase
    try std.testing.expect(std.mem.indexOf(u8, srv, "\"GET\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv, "\"PUT\"") != null);

    // Should have handler interface (regular comments, not doc comments)
    try std.testing.expect(std.mem.indexOf(u8, srv, "// Handler interface") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv, "//   fn getPet(") != null);

    // Should NOT have httpx imports (extractors-only mode)
    try std.testing.expect(std.mem.indexOf(u8, srv, "const httpx = @import(\"httpx\");") == null);

    // Should NOT have ServerRouter
    try std.testing.expect(std.mem.indexOf(u8, srv, "ServerRouter") == null);

    // Root module should re-export server but not ServerRouter
    try std.testing.expect(std.mem.indexOf(u8, result.root, "pub const server = @import(\"server.zig\");") != null);
    try std.testing.expect(std.mem.indexOf(u8, result.root, "ServerRouter") == null);

    // Client should not be generated
    try std.testing.expect(result.client == null);
}
