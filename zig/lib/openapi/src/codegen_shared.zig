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

//! Shared helpers for client and server code generation.

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const naming = @import("naming.zig");
const SourceWriter = @import("writer.zig").SourceWriter;
const Resolver = @import("resolver.zig").Resolver;
const TypeGenerator = @import("codegen_types.zig").TypeGenerator;

/// Success status codes to check for response types, in priority order.
pub const success_status_codes = [_][]const u8{ "200", "201", "202", "2XX" };

/// Derive the query-params struct name for an operation.
pub fn queryParamsTypeName(arena: Allocator, op_id: []const u8) ![]const u8 {
    return std.fmt.allocPrint(arena, "{s}Params", .{try naming.toTypeName(arena, op_id)});
}

/// Collect and classify parameters from path-level and operation-level sources.
pub const CollectedParams = struct {
    path: []const types.Parameter,
    query: []const types.Parameter,
};

pub fn collectParameters(
    arena: Allocator,
    resolver: *Resolver,
    path_level: []const types.ParameterOrRef,
    op_level: []const types.ParameterOrRef,
) !CollectedParams {
    var path_params = std.ArrayListUnmanaged(types.Parameter).empty;
    var query_params = std.ArrayListUnmanaged(types.Parameter).empty;

    for ([_][]const types.ParameterOrRef{ path_level, op_level }) |slice| {
        for (slice) |por| {
            const p = switch (por) {
                .parameter => |p| p,
                .ref => |ref| resolver.resolveParameter(.{ .ref = ref }) catch continue,
            };
            switch (p.in) {
                .path => try path_params.append(arena, p),
                .query => try query_params.append(arena, p),
                else => {},
            }
        }
    }

    return .{ .path = path_params.items, .query = query_params.items };
}

/// Extract the request body type from a RequestBodyOrRef.
pub fn getRequestBodyType(
    arena: Allocator,
    resolver: *Resolver,
    type_gen: *TypeGenerator,
    rb_or: types.RequestBodyOrRef,
) !?[]const u8 {
    const rb = switch (rb_or) {
        .request_body => |r| r,
        .ref => |ref| resolver.resolveRequestBody(.{ .ref = ref }) catch return @as(?[]const u8, "std.json.Value"),
    };
    if (rb.content.get("application/json")) |mt| {
        if (mt.schema) |schema_or| {
            return @as(?[]const u8, try qualifyType(arena, try type_gen.zigTypeForSchemaOrRef(schema_or)));
        }
    }
    return null;
}

/// Prefix generated type names with `types.` for use in client.zig / server.zig.
pub fn qualifyType(arena: Allocator, zig_type: []const u8) ![]const u8 {
    // Optional types: qualify the inner type
    if (std.mem.startsWith(u8, zig_type, "?")) {
        const inner = zig_type[1..];
        const q = try qualifyType(arena, inner);
        if (std.mem.eql(u8, inner, q)) return zig_type;
        return std.fmt.allocPrint(arena, "?{s}", .{q});
    }
    // Array types: qualify the inner type
    if (std.mem.startsWith(u8, zig_type, "[]const ")) {
        const inner = zig_type["[]const ".len..];
        const q = try qualifyType(arena, inner);
        if (std.mem.eql(u8, inner, q)) return zig_type;
        return std.fmt.allocPrint(arena, "[]const {s}", .{q});
    }
    // std types don't need qualification
    if (std.mem.startsWith(u8, zig_type, "std.")) return zig_type;
    // If starts with uppercase, it's a generated type
    if (zig_type.len > 0 and std.ascii.isUpper(zig_type[0])) {
        return std.fmt.allocPrint(arena, "types.{s}", .{zig_type});
    }
    return zig_type;
}

/// Detect if the success response is streaming (SSE/NDJSON) or binary.
pub fn isStreamingOrBinaryResponse(op: types.Operation) bool {
    for (success_status_codes) |code| {
        if (op.responses.get(code)) |resp_or| {
            const resp = switch (resp_or) {
                .response => |r| r,
                .ref => continue,
            };
            if (resp.content.get("text/event-stream") != null) return true;
            if (resp.content.get("application/x-ndjson") != null) return true;
            if (resp.content.get("application/octet-stream") != null) return true;
        }
    }
    return false;
}

/// Look for a success response type (200, 201, 202, 2XX).
pub fn getSuccessResponseType(arena: Allocator, resolver: *Resolver, type_gen: *TypeGenerator, op: types.Operation) ![]const u8 {
    for (success_status_codes) |code| {
        if (op.responses.get(code)) |resp_or| {
            const resp = switch (resp_or) {
                .response => |r| r,
                .ref => |ref| resolver.resolveResponse(.{ .ref = ref }) catch continue,
            };
            if (resp.content.get("application/json")) |mt| {
                if (mt.schema) |schema_or| {
                    return qualifyType(arena, try type_gen.zigTypeForSchemaOrRef(schema_or));
                }
            }
        }
    }
    return "std.json.Value";
}

/// Generate a query parameters struct for an operation.
pub fn generateQueryParamsStruct(arena: Allocator, w: *SourceWriter, op_id: []const u8, params: []const types.Parameter) !void {
    const struct_name = try queryParamsTypeName(arena, op_id);
    try w.line("pub const {s} = struct {{", .{struct_name});
    w.indent();
    for (params) |p| {
        const field_name = try naming.zigFieldName(arena, p.name);
        if (p.description) |desc| try w.docComment(desc);
        if (p.required) {
            try w.line("{s}: []const u8,", .{field_name});
        } else {
            try w.line("{s}: ?[]const u8 = null,", .{field_name});
        }
    }
    w.dedent();
    try w.line("}};", .{});
}

/// HTTP method + operation pair for iterating over a PathItem's operations.
pub const MethodOp = struct {
    method: []const u8,
    op: ?types.Operation,
};

/// Return all HTTP method/operation pairs for a PathItem.
pub fn methodOps(path_item: types.PathItem) [7]MethodOp {
    return .{
        .{ .method = "get", .op = path_item.get },
        .{ .method = "post", .op = path_item.post },
        .{ .method = "put", .op = path_item.put },
        .{ .method = "delete", .op = path_item.delete },
        .{ .method = "patch", .op = path_item.patch },
        .{ .method = "head", .op = path_item.head },
        .{ .method = "options", .op = path_item.options },
    };
}

/// Parsed segment from an OpenAPI path template like `/pets/{petId}/toys`.
pub const PathSegment = union(enum) {
    literal: u8,
    param: []const u8,
};

/// Iterate over an OpenAPI path template, yielding literal bytes and `{param}` names.
/// Malformed `{` without matching `}` is emitted as a literal.
pub fn iterPathTemplate(path: []const u8) PathTemplateIterator {
    return .{ .path = path, .pos = 0 };
}

pub const PathTemplateIterator = struct {
    path: []const u8,
    pos: usize,

    pub fn next(self: *PathTemplateIterator) ?PathSegment {
        if (self.pos >= self.path.len) return null;
        if (self.path[self.pos] == '{') {
            if (std.mem.indexOfScalarPos(u8, self.path, self.pos + 1, '}')) |end| {
                const name = self.path[self.pos + 1 .. end];
                self.pos = end + 1;
                return .{ .param = name };
            }
        }
        const c = self.path[self.pos];
        self.pos += 1;
        return .{ .literal = c };
    }
};

test "qualifyType" {
    const alloc = std.testing.allocator;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    try std.testing.expectEqualStrings("types.Pet", try qualifyType(arena, "Pet"));
    try std.testing.expectEqualStrings("std.json.Value", try qualifyType(arena, "std.json.Value"));
    try std.testing.expectEqualStrings("[]const u8", try qualifyType(arena, "[]const u8"));
    try std.testing.expectEqualStrings("[]const types.Pet", try qualifyType(arena, "[]const Pet"));
    try std.testing.expectEqualStrings("i64", try qualifyType(arena, "i64"));
    try std.testing.expectEqualStrings("?types.Pet", try qualifyType(arena, "?Pet"));
    try std.testing.expectEqualStrings("?[]const u8", try qualifyType(arena, "?[]const u8"));
}
