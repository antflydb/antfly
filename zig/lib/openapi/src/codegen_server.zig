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

//! Server code generation: OpenAPI operations → Zig server helpers.
//!
//! Generates three layers:
//! 1. **Extractors** (framework-agnostic): typed param structs and body parsers
//! 2. **Route table**: comptime list of method/path/operationId tuples
//! 3. **ServerRouter(Impl)**: httpx-specific sugar that wires extractors to an impl struct

const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const naming = @import("naming.zig");
const SourceWriter = @import("writer.zig").SourceWriter;
const Resolver = @import("resolver.zig").Resolver;
const TypeGenerator = @import("codegen_types.zig").TypeGenerator;
const shared = @import("codegen_shared.zig");

pub const ServerGenerator = struct {
    arena: Allocator,
    w: *SourceWriter,
    resolver: *Resolver,
    type_gen: *TypeGenerator,

    pub fn init(arena: Allocator, w: *SourceWriter, resolver: *Resolver, type_gen: *TypeGenerator) ServerGenerator {
        return .{ .arena = arena, .w = w, .resolver = resolver, .type_gen = type_gen };
    }

    /// Generate the full server module (extractors + route table + httpx ServerRouter).
    pub fn generate(self: *ServerGenerator, doc: *const types.OpenApiDoc) !void {
        const ops = try self.collectOps(doc);
        try self.generateExtractors(ops);
        try self.generateRouteTable(ops);
        try self.generateRouter(ops);
    }

    /// Generate only the framework-agnostic parts (extractors + route table, no httpx).
    pub fn generateExtractorsOnly(self: *ServerGenerator, doc: *const types.OpenApiDoc) !void {
        const ops = try self.collectOps(doc);
        try self.generateExtractors(ops);
        try self.generateRouteTable(ops);
        try self.generateHandlerInterface(ops);
    }

    fn collectOps(self: *ServerGenerator, doc: *const types.OpenApiDoc) ![]const OperationInfo {
        var ops = std.ArrayListUnmanaged(OperationInfo).empty;
        for (doc.paths.keys(), doc.paths.values()) |path, path_item| {
            for (shared.methodOps(path_item)) |mo| {
                const op = mo.op orelse continue;
                const op_id = op.operation_id orelse continue;
                const info = try self.collectOperationInfo(path, mo.method, op_id, op, path_item.parameters);
                try ops.append(self.arena, info);
            }
        }
        return ops.items;
    }

    const OperationInfo = struct {
        path: []const u8,
        http_method: []const u8,
        http_method_upper: []const u8,
        method_name: []const u8,
        op_id: []const u8,
        summary: ?[]const u8,
        path_params: []const types.Parameter,
        query_params: []const types.Parameter,
        request_body_type: ?[]const u8,
        response_type: []const u8,
        is_streaming: bool,
    };

    fn collectOperationInfo(
        self: *ServerGenerator,
        path: []const u8,
        http_method: []const u8,
        op_id: []const u8,
        op: types.Operation,
        path_level_params: []const types.ParameterOrRef,
    ) !OperationInfo {
        const method_name = try naming.toMethodName(self.arena, op_id);
        const params = try shared.collectParameters(self.arena, self.resolver, path_level_params, op.parameters);

        const request_body_type: ?[]const u8 = if (op.request_body) |rb_or|
            try shared.getRequestBodyType(self.arena, self.resolver, self.type_gen, rb_or)
        else
            null;

        const is_streaming = shared.isStreamingOrBinaryResponse(op);
        const response_type = if (is_streaming) "void" else try shared.getSuccessResponseType(self.arena, self.resolver, self.type_gen, op);

        return OperationInfo{
            .path = path,
            .http_method = http_method,
            .http_method_upper = try std.ascii.allocUpperString(self.arena, http_method),
            .method_name = method_name,
            .op_id = op_id,
            .summary = op.summary,
            .path_params = params.path,
            .query_params = params.query,
            .request_body_type = request_body_type,
            .response_type = response_type,
            .is_streaming = is_streaming,
        };
    }

    // ---- Layer 1: Extractors ----

    fn generateExtractors(self: *ServerGenerator, ops: []const OperationInfo) !void {
        try self.w.docComment("--- Extractors (framework-agnostic) ---");
        try self.w.blank();

        for (ops) |op| {
            // Path params struct
            if (op.path_params.len > 0) {
                try self.generatePathParamsStruct(op);
                try self.w.blank();
            }

            // Query params struct
            if (op.query_params.len > 0) {
                try shared.generateQueryParamsStruct(self.arena, self.w, op.op_id, op.query_params);
                try self.w.blank();
            }

            // Body parser
            if (op.request_body_type) |bt| {
                try self.generateBodyParser(op, bt);
                try self.w.blank();
            }
        }
    }

    fn generatePathParamsStruct(self: *ServerGenerator, op: OperationInfo) !void {
        const struct_name = try std.fmt.allocPrint(self.arena, "{s}PathParams", .{try naming.toTypeName(self.arena, op.op_id)});
        if (op.summary) |summary| try self.w.docComment(summary);
        try self.w.line("pub const {s} = struct {{", .{struct_name});
        self.w.indent();
        for (op.path_params) |p| {
            const field_name = try naming.zigFieldName(self.arena, p.name);
            if (p.description) |desc| try self.w.docComment(desc);
            try self.w.line("{s}: []const u8,", .{field_name});
        }
        self.w.dedent();
        try self.w.line("}};", .{});
    }

    fn generateBodyParser(self: *ServerGenerator, op: OperationInfo, body_type: []const u8) !void {
        const fn_name = try std.fmt.allocPrint(self.arena, "parse{s}Body", .{try naming.toTypeName(self.arena, op.op_id)});
        try self.w.docComment(try std.fmt.allocPrint(self.arena, "Parse the JSON request body for {s}.", .{op.op_id}));
        try self.w.line("pub fn {s}(allocator: std.mem.Allocator, body: []const u8) !std.json.Parsed({s}) {{", .{ fn_name, body_type });
        self.w.indent();
        try self.w.line("return std.json.parseFromSlice({s}, allocator, body, .{{ .ignore_unknown_fields = true }});", .{body_type});
        self.w.dedent();
        try self.w.line("}}", .{});
    }

    // ---- Layer 2: Route table ----

    fn generateRouteTable(self: *ServerGenerator, ops: []const OperationInfo) !void {
        try self.w.docComment("Route metadata for all operations.");
        try self.w.line("pub const Route = struct {{", .{});
        self.w.indent();
        try self.w.line("method: []const u8,", .{});
        try self.w.line("path: []const u8,", .{});
        try self.w.line("operation_id: []const u8,", .{});
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.blank();

        try self.w.line("pub const routes = [_]Route{{", .{});
        self.w.indent();
        for (ops) |op| {
            try self.w.line(".{{ .method = \"{s}\", .path = \"{s}\", .operation_id = \"{s}\" }},", .{ op.http_method_upper, op.path, op.op_id });
        }
        self.w.dedent();
        try self.w.line("}};", .{});
        try self.w.blank();
    }

    // ---- Layer 3: ServerRouter (httpx sugar) ----

    fn generateRouter(self: *ServerGenerator, ops: []const OperationInfo) !void {
        try self.w.docComment("Generated server router for httpx. Register routes on an httpx.Server");
        try self.w.docComment("by providing an implementation struct with handler methods.");
        try self.w.docComment("");
        try self.w.docComment("Example:");
        try self.w.docComment("  const MyImpl = struct {");
        try self.w.docComment("      pub fn getStatus(self: *@This(), ctx: *httpx.Context) !httpx.Response { ... }");
        try self.w.docComment("  };");
        try self.w.docComment("  var router = ServerRouter(MyImpl).init(&my_impl);");
        try self.w.docComment("  try router.register(&server);");
        try self.w.line("pub fn ServerRouter(comptime Impl: type) type {{", .{});
        self.w.indent();

        // Comptime validation
        try self.w.line("comptime {{", .{});
        self.w.indent();
        for (ops) |op| {
            try self.w.line("if (!@hasDecl(Impl, \"{s}\")) @compileError(\"ServerRouter: Impl missing required method '{s}'\");", .{ op.method_name, op.method_name });
        }
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();

        try self.w.line("return struct {{", .{});
        self.w.indent();

        // Global storage for the impl pointer, set during register().
        // This allows bare function-pointer handlers (httpx.Handler = *const fn)
        // to access the impl. Works across fibers/threads since register()
        // completes before any handler is invoked.
        try self.w.line("var active_impl: ?*Impl = null;", .{});
        try self.w.blank();

        try self.w.line("impl: *Impl,", .{});
        try self.w.blank();

        try self.w.line("pub fn init(impl: *Impl) @This() {{", .{});
        self.w.indent();
        try self.w.line("return .{{ .impl = impl }};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();

        // register method
        try self.w.docComment("Register all routes on the server and activate the impl.");
        try self.w.line("pub fn register(self: *const @This(), server: anytype) !void {{", .{});
        self.w.indent();
        try self.w.line("active_impl = self.impl;", .{});

        for (ops) |op| {
            const httpx_path = try self.openApiPathToHttpx(op.path);
            try self.w.line("try server.{s}(\"{s}\", {s});", .{
                op.http_method,
                httpx_path,
                op.method_name,
            });
        }

        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();

        // Generate a wrapper for each operation
        for (ops) |op| {
            try self.generateHandlerWrapper(op);
            try self.w.blank();
        }

        self.w.dedent();
        try self.w.line("}};", .{});
        self.w.dedent();
        try self.w.line("}}", .{});
        try self.w.blank();

        try self.generateHandlerInterface(ops);
    }

    fn generateHandlerInterface(self: *ServerGenerator, ops: []const OperationInfo) !void {
        try self.w.comment("Handler interface. Implement these methods on your Impl struct:");
        try self.w.comment("");
        for (ops) |op| {
            var sig = std.ArrayListUnmanaged(u8).empty;
            try sig.print(self.arena, "  fn {s}(self: *Impl, ctx: *httpx.Context", .{op.method_name});
            for (op.path_params) |p| {
                const pname = try naming.zigFieldName(self.arena, p.name);
                try sig.print(self.arena, ", {s}: []const u8", .{pname});
            }
            if (op.query_params.len > 0) {
                try sig.print(self.arena, ", params: {s}", .{try shared.queryParamsTypeName(self.arena, op.op_id)});
            }
            try sig.print(self.arena, ") !httpx.Response", .{});
            try self.w.comment(sig.items);
        }
    }

    fn generateHandlerWrapper(self: *ServerGenerator, op: OperationInfo) !void {
        if (op.summary) |summary| try self.w.docComment(summary);
        try self.w.docComment(try std.fmt.allocPrint(self.arena, "{s} {s}", .{ op.http_method_upper, op.path }));

        try self.w.line("fn {s}(ctx: *httpx.Context) anyerror!httpx.Response {{", .{op.method_name});
        self.w.indent();
        try self.w.line("const impl = active_impl orelse return ctx.status(503).json(.{{ .@\"error\" = \"not_initialized\", .message = \"server not initialized\" }});", .{});

        // Extract path parameters
        for (op.path_params) |p| {
            const field_name = try naming.zigFieldName(self.arena, p.name);
            try self.w.line("const {s} = ctx.param(\"{s}\") orelse return ctx.status(400).json(.{{ .@\"error\" = \"missing_path_param\", .message = \"Missing path parameter: {s}\" }});", .{
                field_name,
                p.name,
                p.name,
            });
        }

        // Extract query parameters into struct
        if (op.query_params.len > 0) {
            try self.w.line("const query_params = {s}{{", .{try shared.queryParamsTypeName(self.arena, op.op_id)});
            self.w.indent();
            for (op.query_params) |p| {
                const field_name = try naming.zigFieldName(self.arena, p.name);
                if (p.required) {
                    try self.w.line(".{s} = ctx.query(\"{s}\") orelse return ctx.status(400).json(.{{ .@\"error\" = \"missing_query_param\", .message = \"Missing required query parameter: {s}\" }}),", .{
                        field_name,
                        p.name,
                        p.name,
                    });
                } else {
                    try self.w.line(".{s} = ctx.query(\"{s}\"),", .{
                        field_name,
                        p.name,
                    });
                }
            }
            self.w.dedent();
            try self.w.line("}};", .{});
        }

        // Call impl method
        {
            var call = std.ArrayListUnmanaged(u8).empty;
            try call.print(self.arena, "return impl.{s}(ctx", .{op.method_name});
            for (op.path_params) |p| {
                const field_name = try naming.zigFieldName(self.arena, p.name);
                try call.print(self.arena, ", {s}", .{field_name});
            }
            if (op.query_params.len > 0) {
                try call.appendSlice(self.arena, ", query_params");
            }
            try call.appendSlice(self.arena, ");");
            try self.w.line("{s}", .{call.items});
        }

        self.w.dedent();
        try self.w.line("}}", .{});
    }

    // Wrap helper removed: handlers now read from threadlocal active_impl directly.

    /// Convert OpenAPI path template to httpx route pattern.
    /// `/pets/{petId}` → `/pets/:petId`
    fn openApiPathToHttpx(self: *ServerGenerator, path: []const u8) ![]const u8 {
        var result = std.ArrayListUnmanaged(u8).empty;
        var it = shared.iterPathTemplate(path);
        while (it.next()) |seg| {
            switch (seg) {
                .literal => |c| try result.append(self.arena, c),
                .param => |name| {
                    try result.append(self.arena, ':');
                    try result.appendSlice(self.arena, name);
                },
            }
        }
        return result.items;
    }
};

test "server generator smoke" {
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
    var type_gen = TypeGenerator.init(arena, &w, &resolver);
    _ = ServerGenerator.init(arena, &w, &resolver, &type_gen);
}
