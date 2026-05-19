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
    /// For inline schemas, returns as-is.
    /// For $ref, looks up in components/schemas.
    pub fn resolveSchema(self: *Resolver, sor: types.SchemaOrRef) ResolveError!types.Schema {
        switch (sor) {
            .schema => |s| return s,
            .ref => |ref| {
                const ref_name = naming.refToName(ref.ref_string) orelse return ResolveError.UnresolvedRef;
                const components = self.doc.components orelse return ResolveError.UnresolvedRef;
                const target = components.schemas.get(ref_name) orelse return ResolveError.UnresolvedRef;
                // Resolve one level (don't chase ref chains for now)
                return switch (target) {
                    .schema => |s| s,
                    .ref => return ResolveError.UnresolvedRef,
                };
            },
        }
    }

    /// Resolve a ParameterOrRef to its concrete Parameter.
    pub fn resolveParameter(self: *Resolver, por: types.ParameterOrRef) ResolveError!types.Parameter {
        switch (por) {
            .parameter => |p| return p,
            .ref => |ref_str| {
                const ref_name = naming.refToName(ref_str) orelse return ResolveError.UnresolvedRef;
                const components = self.doc.components orelse return ResolveError.UnresolvedRef;
                const target = components.parameters.get(ref_name) orelse return ResolveError.UnresolvedRef;
                return switch (target) {
                    .parameter => |p| p,
                    .ref => return ResolveError.UnresolvedRef,
                };
            },
        }
    }

    /// Resolve a RequestBodyOrRef to its concrete RequestBody.
    pub fn resolveRequestBody(self: *Resolver, rbor: types.RequestBodyOrRef) ResolveError!types.RequestBody {
        switch (rbor) {
            .request_body => |rb| return rb,
            .ref => |ref_str| {
                const ref_name = naming.refToName(ref_str) orelse return ResolveError.UnresolvedRef;
                const components = self.doc.components orelse return ResolveError.UnresolvedRef;
                const target = components.request_bodies.get(ref_name) orelse return ResolveError.UnresolvedRef;
                return switch (target) {
                    .request_body => |rb| rb,
                    .ref => return ResolveError.UnresolvedRef,
                };
            },
        }
    }

    /// Resolve a ResponseOrRef to its concrete Response.
    pub fn resolveResponse(self: *Resolver, ror: types.ResponseOrRef) ResolveError!types.Response {
        switch (ror) {
            .response => |r| return r,
            .ref => |ref_str| {
                const ref_name = naming.refToName(ref_str) orelse return ResolveError.UnresolvedRef;
                const components = self.doc.components orelse return ResolveError.UnresolvedRef;
                const target = components.responses.get(ref_name) orelse return ResolveError.UnresolvedRef;
                return switch (target) {
                    .response => |r| r,
                    .ref => return ResolveError.UnresolvedRef,
                };
            },
        }
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
