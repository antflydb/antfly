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

//! $ref resolver for OpenAPI specs.
//!
//! Resolves local $ref pointers within a bundled OpenAPI document.
//! Cross-file resolution is not yet supported — use `redocly bundle` first.

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const naming = @import("naming.zig");

pub const ResolveError = error{
    CyclicRef,
    UnresolvedRef,
    OutOfMemory,
};

pub const Resolver = struct {
    doc: *const types.OpenApiDoc,
    arena: Allocator,

    pub fn init(arena: Allocator, doc: *const types.OpenApiDoc) Resolver {
        return .{ .arena = arena, .doc = doc };
    }

    /// Resolve a SchemaOrRef to its concrete Schema.
    /// For inline schemas, returns as-is. Local component aliases are followed
    /// to their concrete schema with cycle detection. External references are
    /// deliberately unresolved: their schemas belong to another document and
    /// must not be confused with a same-named component in this document.
    pub fn resolveSchema(self: *Resolver, sor: types.SchemaOrRef) ResolveError!types.Schema {
        // Schema aliases form a singly linked chain, so Floyd's algorithm gives
        // exact cycle detection without allocating a visited set for every
        // generator lookup.
        var slow = sor;
        var fast = sor;
        while (true) {
            slow = (try self.nextSchemaAlias(slow)) orelse break;
            fast = (try self.nextSchemaAlias(fast)) orelse break;
            fast = (try self.nextSchemaAlias(fast)) orelse break;
            if (sameComponentRef(slow, fast)) return ResolveError.CyclicRef;
        }

        var current = sor;
        while (true) {
            switch (current) {
                .schema => |schema| return schema,
                .ref => current = (try self.nextSchemaAlias(current)) orelse unreachable,
            }
        }
    }

    fn nextSchemaAlias(self: *Resolver, sor: types.SchemaOrRef) ResolveError!?types.SchemaOrRef {
        return switch (sor) {
            .schema => null,
            .ref => |ref| blk: {
                if (naming.isExternalRef(ref.ref_string)) return ResolveError.UnresolvedRef;
                const ref_name = naming.refToName(ref.ref_string) orelse return ResolveError.UnresolvedRef;
                const components = self.doc.components orelse return ResolveError.UnresolvedRef;
                break :blk components.schemas.get(ref_name) orelse return ResolveError.UnresolvedRef;
            },
        };
    }

    /// Resolve a ParameterOrRef to its concrete Parameter.
    pub fn resolveParameter(self: *Resolver, por: types.ParameterOrRef) ResolveError!types.Parameter {
        var slow = por;
        var fast = por;
        while (true) {
            slow = (try self.nextParameterAlias(slow)) orelse break;
            fast = (try self.nextParameterAlias(fast)) orelse break;
            fast = (try self.nextParameterAlias(fast)) orelse break;
            if (sameComponentRef(slow, fast)) return ResolveError.CyclicRef;
        }

        var current = por;
        while (true) switch (current) {
            .parameter => |parameter| return parameter,
            .ref => current = (try self.nextParameterAlias(current)) orelse unreachable,
        };
    }

    fn nextParameterAlias(self: *Resolver, por: types.ParameterOrRef) ResolveError!?types.ParameterOrRef {
        return switch (por) {
            .parameter => null,
            .ref => |ref_str| blk: {
                if (naming.isExternalRef(ref_str)) return ResolveError.UnresolvedRef;
                const ref_name = naming.refToName(ref_str) orelse return ResolveError.UnresolvedRef;
                const components = self.doc.components orelse return ResolveError.UnresolvedRef;
                break :blk components.parameters.get(ref_name) orelse return ResolveError.UnresolvedRef;
            },
        };
    }

    /// Resolve a RequestBodyOrRef to its concrete RequestBody.
    pub fn resolveRequestBody(self: *Resolver, rbor: types.RequestBodyOrRef) ResolveError!types.RequestBody {
        var slow = rbor;
        var fast = rbor;
        while (true) {
            slow = (try self.nextRequestBodyAlias(slow)) orelse break;
            fast = (try self.nextRequestBodyAlias(fast)) orelse break;
            fast = (try self.nextRequestBodyAlias(fast)) orelse break;
            if (sameComponentRef(slow, fast)) return ResolveError.CyclicRef;
        }

        var current = rbor;
        while (true) switch (current) {
            .request_body => |request_body| return request_body,
            .ref => current = (try self.nextRequestBodyAlias(current)) orelse unreachable,
        };
    }

    fn nextRequestBodyAlias(self: *Resolver, rbor: types.RequestBodyOrRef) ResolveError!?types.RequestBodyOrRef {
        return switch (rbor) {
            .request_body => null,
            .ref => |ref_str| blk: {
                if (naming.isExternalRef(ref_str)) return ResolveError.UnresolvedRef;
                const ref_name = naming.refToName(ref_str) orelse return ResolveError.UnresolvedRef;
                const components = self.doc.components orelse return ResolveError.UnresolvedRef;
                break :blk components.request_bodies.get(ref_name) orelse return ResolveError.UnresolvedRef;
            },
        };
    }

    /// Resolve a ResponseOrRef to its concrete Response.
    pub fn resolveResponse(self: *Resolver, ror: types.ResponseOrRef) ResolveError!types.Response {
        var slow = ror;
        var fast = ror;
        while (true) {
            slow = (try self.nextResponseAlias(slow)) orelse break;
            fast = (try self.nextResponseAlias(fast)) orelse break;
            fast = (try self.nextResponseAlias(fast)) orelse break;
            if (sameComponentRef(slow, fast)) return ResolveError.CyclicRef;
        }

        var current = ror;
        while (true) switch (current) {
            .response => |response| return response,
            .ref => current = (try self.nextResponseAlias(current)) orelse unreachable,
        };
    }

    fn nextResponseAlias(self: *Resolver, ror: types.ResponseOrRef) ResolveError!?types.ResponseOrRef {
        return switch (ror) {
            .response => null,
            .ref => |ref_str| blk: {
                if (naming.isExternalRef(ref_str)) return ResolveError.UnresolvedRef;
                const ref_name = naming.refToName(ref_str) orelse return ResolveError.UnresolvedRef;
                const components = self.doc.components orelse return ResolveError.UnresolvedRef;
                break :blk components.responses.get(ref_name) orelse return ResolveError.UnresolvedRef;
            },
        };
    }

    /// Get the Zig type name for a SchemaOrRef.
    /// For $ref, extracts and converts the name.
    /// For inline schemas, returns null (caller must generate anonymous type).
    pub fn typeName(self: *Resolver, sor: types.SchemaOrRef) !?[]const u8 {
        switch (sor) {
            .ref => |ref| {
                const ref_name = naming.refToName(ref.ref_string) orelse return null;
                return naming.toTypeName(self.arena, ref_name);
            },
            .schema => return null,
        }
    }
};

fn sameComponentRef(left: anytype, right: @TypeOf(left)) bool {
    const left_ref = componentRefString(left) orelse return false;
    const right_ref = componentRefString(right) orelse return false;
    return std.mem.eql(u8, left_ref, right_ref);
}

fn componentRefString(value: anytype) ?[]const u8 {
    return switch (value) {
        .ref => |ref| if (@TypeOf(ref) == []const u8) ref else ref.ref_string,
        else => null,
    };
}

test "resolve schema ref" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "User", types.SchemaOrRef{
        .schema = types.Schema{
            .schema_type = .{ .single = "object" },
        },
    });

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = types.Components{
            .schemas = schemas,
        },
    };

    var resolver = Resolver.init(arena, &doc);
    const resolved = try resolver.resolveSchema(types.SchemaOrRef{ .ref = .{ .ref_string = "#/components/schemas/User" } });
    try std.testing.expectEqualStrings("object", resolved.primaryType().?);
}

test "unresolved ref" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
    };

    var resolver = Resolver.init(arena, &doc);
    const result = resolver.resolveSchema(types.SchemaOrRef{ .ref = .{ .ref_string = "#/components/schemas/Missing" } });
    try std.testing.expectError(ResolveError.UnresolvedRef, result);
}

test "resolve schema ref alias chain" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Alias", .{ .ref = .{ .ref_string = "#/components/schemas/Target" } });
    try schemas.put(arena, "Target", .{ .schema = .{ .schema_type = .{ .single = "string" }, .nullable = true } });
    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };

    var resolver = Resolver.init(arena, &doc);
    const resolved = try resolver.resolveSchema(.{ .ref = .{ .ref_string = "#/components/schemas/Alias" } });
    try std.testing.expect(resolved.isNullable());
}

test "reject cyclic schema ref aliases" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Left", .{ .ref = .{ .ref_string = "#/components/schemas/Right" } });
    try schemas.put(arena, "Right", .{ .ref = .{ .ref_string = "#/components/schemas/Left" } });
    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };

    var resolver = Resolver.init(arena, &doc);
    const result = resolver.resolveSchema(.{ .ref = .{ .ref_string = "#/components/schemas/Left" } });
    try std.testing.expectError(ResolveError.CyclicRef, result);
}

test "external schema ref cannot resolve to same-named local component" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var schemas = std.StringArrayHashMapUnmanaged(types.SchemaOrRef){};
    try schemas.put(arena, "Identifier", .{ .schema = .{ .schema_type = .{ .single = "string" } } });
    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{ .schemas = schemas },
    };

    var resolver = Resolver.init(arena, &doc);
    const result = resolver.resolveSchema(.{
        .ref = .{ .ref_string = "generated/identifier.yaml#/components/schemas/Identifier" },
    });
    try std.testing.expectError(ResolveError.UnresolvedRef, result);
}

test "non-schema component aliases resolve locally and reject external collisions" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var parameters = std.StringArrayHashMapUnmanaged(types.ParameterOrRef){};
    try parameters.put(arena, "ParameterAlias", .{ .ref = "#/components/parameters/ParameterTarget" });
    try parameters.put(arena, "ParameterTarget", .{ .parameter = .{ .name = "limit", .in = .query } });

    var request_bodies = std.StringArrayHashMapUnmanaged(types.RequestBodyOrRef){};
    try request_bodies.put(arena, "BodyAlias", .{ .ref = "#/components/requestBodies/BodyTarget" });
    try request_bodies.put(arena, "BodyTarget", .{ .request_body = .{ .required = true } });

    var responses = std.StringArrayHashMapUnmanaged(types.ResponseOrRef){};
    try responses.put(arena, "ResponseAlias", .{ .ref = "#/components/responses/ResponseTarget" });
    try responses.put(arena, "ResponseTarget", .{ .response = .{ .description = "ok" } });

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{
            .parameters = parameters,
            .request_bodies = request_bodies,
            .responses = responses,
        },
    };
    var resolver = Resolver.init(arena, &doc);

    const parameter = try resolver.resolveParameter(.{ .ref = "#/components/parameters/ParameterAlias" });
    try std.testing.expectEqualStrings("limit", parameter.name);
    const request_body = try resolver.resolveRequestBody(.{ .ref = "#/components/requestBodies/BodyAlias" });
    try std.testing.expect(request_body.required);
    const response = try resolver.resolveResponse(.{ .ref = "#/components/responses/ResponseAlias" });
    try std.testing.expectEqualStrings("ok", response.description.?);

    try std.testing.expectError(
        ResolveError.UnresolvedRef,
        resolver.resolveParameter(.{ .ref = "shared.yaml#/components/parameters/ParameterTarget" }),
    );
    try std.testing.expectError(
        ResolveError.UnresolvedRef,
        resolver.resolveRequestBody(.{ .ref = "shared.yaml#/components/requestBodies/BodyTarget" }),
    );
    try std.testing.expectError(
        ResolveError.UnresolvedRef,
        resolver.resolveResponse(.{ .ref = "shared.yaml#/components/responses/ResponseTarget" }),
    );
}

test "non-schema component alias cycles fail explicitly" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var parameters = std.StringArrayHashMapUnmanaged(types.ParameterOrRef){};
    try parameters.put(arena, "Left", .{ .ref = "#/components/parameters/Right" });
    try parameters.put(arena, "Right", .{ .ref = "#/components/parameters/Left" });
    var request_bodies = std.StringArrayHashMapUnmanaged(types.RequestBodyOrRef){};
    try request_bodies.put(arena, "Left", .{ .ref = "#/components/requestBodies/Right" });
    try request_bodies.put(arena, "Right", .{ .ref = "#/components/requestBodies/Left" });
    var responses = std.StringArrayHashMapUnmanaged(types.ResponseOrRef){};
    try responses.put(arena, "Left", .{ .ref = "#/components/responses/Right" });
    try responses.put(arena, "Right", .{ .ref = "#/components/responses/Left" });

    const doc = types.OpenApiDoc{
        .openapi = "3.0.3",
        .info = .{ .title = "Test", .version = "1.0" },
        .components = .{
            .parameters = parameters,
            .request_bodies = request_bodies,
            .responses = responses,
        },
    };
    var resolver = Resolver.init(arena, &doc);

    try std.testing.expectError(ResolveError.CyclicRef, resolver.resolveParameter(.{ .ref = "#/components/parameters/Left" }));
    try std.testing.expectError(ResolveError.CyclicRef, resolver.resolveRequestBody(.{ .ref = "#/components/requestBodies/Left" }));
    try std.testing.expectError(ResolveError.CyclicRef, resolver.resolveResponse(.{ .ref = "#/components/responses/Left" }));
}
