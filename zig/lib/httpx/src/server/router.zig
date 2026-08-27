//! HTTP Router Implementation for httpx.zig
//!
//! Pattern-based routing with path parameter support:
//!
//! - Static path matching (/users, /api/posts)
//! - Dynamic parameters (/users/:id, /posts/:postId/comments/:commentId)
//! - Wildcard routes (/static/*)
//! - Route groups with prefixes
//! - Method-based routing

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const types = @import("../core/types.zig");

/// Route parameter extracted from the URL.
pub const RouteParam = struct {
    name: []const u8,
    value: []const u8,
};

/// Route match result containing the handler and extracted parameters.
pub const RouteMatch = struct {
    handler: Handler,
    data: ?*anyopaque,
    params: []const RouteParam,
    max_body_size: ?usize,
};

/// Handler function type — canonical definition lives in server.zig.
pub const Handler = @import("server.zig").Handler;

const Route = struct {
    method: types.Method,
    pattern: []const u8,
    pattern_owned: bool = false,
    segments: []const Segment,
    handler: Handler,
    data: ?*anyopaque = null,
    max_body_size: ?usize = null,
};

const Segment = union(enum) {
    literal: []const u8,
    param: []const u8,
    param_suffix: struct {
        name: []const u8,
        suffix: []const u8,
    },
    wildcard: void,
};

/// Number of method variants used for per-method route partitioning.
const method_count = @typeInfo(types.Method).@"enum".fields.len;

/// HTTP Router with path parameter support.
/// Routes are partitioned by HTTP method for O(R/M) lookup instead of O(R).
pub const Router = struct {
    allocator: Allocator,
    /// Per-method route lists indexed by @intFromEnum(method).
    method_routes: [method_count]std.ArrayListUnmanaged(Route) = [_]std.ArrayListUnmanaged(Route){.empty} ** method_count,
    body_limited_route_count: usize = 0,
    body_limited_route_counts: [method_count]usize = [_]usize{0} ** method_count,
    const Self = @This();

    /// Creates a new router.
    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    /// Releases all allocated resources.
    pub fn deinit(self: *Self) void {
        for (&self.method_routes) |*routes| {
            for (routes.items) |route| {
                self.allocator.free(route.segments);
                if (route.pattern_owned) {
                    self.allocator.free(route.pattern);
                }
            }
            routes.deinit(self.allocator);
        }
    }

    fn routesFor(self: *Self, method: types.Method) *std.ArrayListUnmanaged(Route) {
        return &self.method_routes[@intFromEnum(method)];
    }

    fn routesForConst(self: *const Self, method: types.Method) []const Route {
        return self.method_routes[@intFromEnum(method)].items;
    }

    /// Adds a route to the router.
    pub fn add(self: *Self, method: types.Method, pattern: []const u8, handler: anytype) !void {
        return self.addWithDataAndBodyLimit(method, pattern, handler, null, null);
    }

    /// Adds a route with borrowed opaque request data.
    pub fn addWithData(self: *Self, method: types.Method, pattern: []const u8, handler: anytype, data: ?*anyopaque) !void {
        return self.addWithDataAndBodyLimit(method, pattern, handler, data, null);
    }

    pub fn addWithBodyLimit(self: *Self, method: types.Method, pattern: []const u8, handler: anytype, max_body_size: usize) !void {
        return self.addWithDataAndBodyLimit(method, pattern, handler, null, max_body_size);
    }

    fn addWithDataAndBodyLimit(self: *Self, method: types.Method, pattern: []const u8, handler: anytype, data: ?*anyopaque, max_body_size: ?usize) !void {
        const segments = try self.parsePattern(pattern);
        errdefer self.allocator.free(segments);
        for (self.routesForConst(method)) |route| {
            if (sameRouteShape(route.segments, segments)) return error.DuplicateRoute;
        }
        try self.routesFor(method).append(self.allocator, .{
            .method = method,
            .pattern = pattern,
            .segments = segments,
            .handler = Handler.from(handler),
            .data = data,
            .max_body_size = max_body_size,
        });
        if (max_body_size != null) {
            self.body_limited_route_count += 1;
            self.body_limited_route_counts[@intFromEnum(method)] += 1;
        }
    }

    /// Adds a route with an owned (duped) pattern string.
    fn addOwned(self: *Self, method: types.Method, pattern: []const u8, handler: Handler) !void {
        const owned = try self.allocator.dupe(u8, pattern);
        errdefer self.allocator.free(owned);
        const segments = try self.parsePattern(owned);
        errdefer self.allocator.free(segments);
        for (self.routesForConst(method)) |route| {
            if (sameRouteShape(route.segments, segments)) return error.DuplicateRoute;
        }
        try self.routesFor(method).append(self.allocator, .{
            .method = method,
            .pattern = owned,
            .pattern_owned = true,
            .segments = segments,
            .handler = handler,
        });
    }

    fn sameRouteShape(left: []const Segment, right: []const Segment) bool {
        if (left.len != right.len) return false;
        for (left, right) |left_segment, right_segment| switch (left_segment) {
            .literal => |left_literal| switch (right_segment) {
                .literal => |right_literal| if (!mem.eql(u8, left_literal, right_literal)) return false,
                else => return false,
            },
            .param => switch (right_segment) {
                .param => {},
                else => return false,
            },
            .param_suffix => |left_param| switch (right_segment) {
                .param_suffix => |right_param| if (!mem.eql(u8, left_param.suffix, right_param.suffix)) return false,
                else => return false,
            },
            .wildcard => switch (right_segment) {
                .wildcard => {},
                else => return false,
            },
        };
        return true;
    }

    fn parsePattern(self: *Self, pattern: []const u8) ![]const Segment {
        var segments = std.ArrayListUnmanaged(Segment).empty;

        var iter = mem.splitScalar(u8, pattern, '/');
        while (iter.next()) |part| {
            if (part.len == 0) continue;

            if (part[0] == ':') {
                if (mem.indexOfScalar(u8, part[1..], ':')) |suffix_idx| {
                    const suffix_start = 1 + suffix_idx;
                    try segments.append(self.allocator, .{ .param_suffix = .{
                        .name = part[1..suffix_start],
                        .suffix = part[suffix_start..],
                    } });
                } else {
                    try segments.append(self.allocator, .{ .param = part[1..] });
                }
            } else if (mem.eql(u8, part, "*")) {
                try segments.append(self.allocator, .wildcard);
            } else {
                try segments.append(self.allocator, .{ .literal = part });
            }
        }

        return segments.toOwnedSlice(self.allocator);
    }

    /// Finds a matching route for the given method and path.
    /// `params_buf` is caller-owned storage for matched route parameters;
    /// the returned slice borrows from it, so it remains valid as long as
    /// the caller keeps the buffer alive.
    pub fn find(self: *Self, method: types.Method, path: []const u8, params_buf: *[16]RouteParam) ?RouteMatch {
        for (self.routesForConst(method)) |route| {
            if (self.matchRoute(route, path, params_buf)) |param_count| {
                return .{
                    .handler = route.handler,
                    .data = route.data,
                    .params = params_buf[0..param_count],
                    .max_body_size = route.max_body_size,
                };
            }
        }

        return null;
    }

    pub fn bodySizeLimit(self: *const Self, method: types.Method, path: []const u8) ?usize {
        if (!self.hasBodyLimitsForMethod(method)) return null;
        var params_buf: [16]RouteParam = undefined;
        for (self.routesForConst(method)) |route| {
            if (self.matchRoute(route, path, &params_buf) != null) return route.max_body_size;
        }
        return null;
    }

    pub fn hasBodyLimits(self: *const Self) bool {
        return self.body_limited_route_count != 0;
    }

    pub fn hasBodyLimitsForMethod(self: *const Self, method: types.Method) bool {
        return self.body_limited_route_counts[@intFromEnum(method)] != 0;
    }

    /// Returns the list of allowed methods for a given path.
    ///
    /// Scans all method partitions; the returned value is the number of
    /// methods written into `out_methods`.
    pub fn allowedMethods(self: *const Self, path: []const u8, out_methods: *[16]types.Method) usize {
        var params_buf: [16]RouteParam = undefined;
        var count: usize = 0;

        for (0..method_count) |mi| {
            const routes = self.method_routes[mi].items;
            for (routes) |route| {
                if (self.matchRoute(route, path, &params_buf) != null) {
                    if (count < out_methods.len) {
                        out_methods[count] = route.method;
                        count += 1;
                    }
                    break; // one match per method partition is enough
                }
            }
        }

        return count;
    }

    fn matchRoute(self: *const Self, route: Route, path: []const u8, params: *[16]RouteParam) ?usize {
        _ = self;
        var path_iter = mem.splitScalar(u8, path, '/');
        var param_idx: usize = 0;
        var seg_idx: usize = 0;

        while (path_iter.next()) |part| {
            if (part.len == 0) continue;

            if (seg_idx >= route.segments.len) return null;

            const segment = route.segments[seg_idx];
            switch (segment) {
                .literal => |lit| {
                    if (!mem.eql(u8, lit, part)) return null;
                },
                .param => |name| {
                    if (param_idx >= params.len) return null;
                    params[param_idx] = .{ .name = name, .value = part };
                    param_idx += 1;
                },
                .param_suffix => |param| {
                    if (!mem.endsWith(u8, part, param.suffix)) return null;
                    if (part.len <= param.suffix.len) return null;
                    if (param_idx >= params.len) return null;
                    params[param_idx] = .{ .name = param.name, .value = part[0 .. part.len - param.suffix.len] };
                    param_idx += 1;
                },
                .wildcard => {
                    return param_idx;
                },
            }
            seg_idx += 1;
        }

        return if (seg_idx == route.segments.len) param_idx else null;
    }

    /// Creates a route group with the given prefix.
    pub fn group(self: *Self, prefix: []const u8) RouteGroup {
        return RouteGroup.init(self, prefix);
    }
};

/// Route group for organizing routes with a common prefix.
pub const RouteGroup = struct {
    router: *Router,
    prefix: []const u8,

    const Self = @This();

    /// Creates a new route group.
    pub fn init(router: *Router, prefix: []const u8) Self {
        return .{ .router = router, .prefix = prefix };
    }

    /// Adds a route to the group.
    pub fn add(self: *Self, method: types.Method, path: []const u8, handler: anytype) !void {
        var full_path = std.ArrayListUnmanaged(u8).empty;
        defer full_path.deinit(self.router.allocator);

        try full_path.appendSlice(self.router.allocator, self.prefix);
        try full_path.appendSlice(self.router.allocator, path);

        try self.router.addOwned(method, full_path.items, Handler.from(handler));
    }

    /// Adds a GET route.
    pub fn get(self: *Self, path: []const u8, handler: anytype) !void {
        try self.add(.GET, path, handler);
    }

    /// Adds a POST route.
    pub fn post(self: *Self, path: []const u8, handler: anytype) !void {
        try self.add(.POST, path, handler);
    }

    /// Adds a PUT route.
    pub fn put(self: *Self, path: []const u8, handler: anytype) !void {
        try self.add(.PUT, path, handler);
    }

    /// Adds a DELETE route.
    pub fn delete(self: *Self, path: []const u8, handler: anytype) !void {
        try self.add(.DELETE, path, handler);
    }

    /// Adds a PATCH route.
    pub fn patch(self: *Self, path: []const u8, handler: anytype) !void {
        try self.add(.PATCH, path, handler);
    }

    /// Adds a HEAD route.
    pub fn head(self: *Self, path: []const u8, handler: anytype) !void {
        try self.add(.HEAD, path, handler);
    }

    /// Adds an OPTIONS route.
    pub fn options(self: *Self, path: []const u8, handler: anytype) !void {
        try self.add(.OPTIONS, path, handler);
    }
};

test "Router basic matching" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.GET, "/users", handler);
    try router.add(.GET, "/users/:id", handler);
    try router.add(.POST, "/users", handler);

    var route_state: u8 = 1;
    try router.addWithData(.PUT, "/users", handler, &route_state);

    const result1 = router.find(.GET, "/users", &pbuf);
    try std.testing.expect(result1 != null);
    try std.testing.expectEqual(@as(usize, 0), result1.?.params.len);

    const result2 = router.find(.GET, "/users/123", &pbuf);
    try std.testing.expect(result2 != null);
    try std.testing.expectEqual(@as(usize, 1), result2.?.params.len);
    try std.testing.expectEqualStrings("id", result2.?.params[0].name);
    try std.testing.expectEqualStrings("123", result2.?.params[0].value);

    const result3 = router.find(.DELETE, "/users", &pbuf);
    try std.testing.expect(result3 == null);

    const result4 = router.find(.PUT, "/users", &pbuf).?;
    try std.testing.expectEqual(@as(?*anyopaque, &route_state), result4.data);
}

test "Router exposes route-specific body limits" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.addWithBodyLimit(.POST, "/bounded/:id", handler, 64 * 1024);
    try router.add(.POST, "/unbounded", handler);
    try router.add(.PUT, "/overlap/:id", handler);
    try router.addWithBodyLimit(.PUT, "/overlap/*", handler, 1024);
    try std.testing.expect(router.hasBodyLimits());
    try std.testing.expect(router.hasBodyLimitsForMethod(.POST));
    try std.testing.expect(router.hasBodyLimitsForMethod(.PUT));
    try std.testing.expect(!router.hasBodyLimitsForMethod(.GET));
    try std.testing.expectEqual(@as(?usize, 64 * 1024), router.bodySizeLimit(.POST, "/bounded/7"));
    try std.testing.expectEqual(@as(?usize, null), router.bodySizeLimit(.POST, "/unbounded"));
    // Body-limit resolution must preserve normal route precedence: the first
    // matching route is unbounded even though a later limited route also matches.
    try std.testing.expectEqual(@as(?usize, null), router.bodySizeLimit(.PUT, "/overlap/7"));
    try std.testing.expectEqual(@as(?usize, null), router.bodySizeLimit(.GET, "/bounded/7"));
}

test "Router multiple parameters" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.GET, "/users/:userId/posts/:postId", handler);

    const result = router.find(.GET, "/users/42/posts/99", &pbuf);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 2), result.?.params.len);
    try std.testing.expectEqualStrings("userId", result.?.params[0].name);
    try std.testing.expectEqualStrings("42", result.?.params[0].value);
    try std.testing.expectEqualStrings("postId", result.?.params[1].name);
    try std.testing.expectEqualStrings("99", result.?.params[1].value);
}

test "Router parameter segment can have a literal suffix" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.POST, "/tables/:table/documents/:key/artifacts/:artifact:reprocess", handler);

    const result = router.find(.POST, "/tables/docs/documents/doc%2Fa/artifacts/document_units_v1:reprocess", &pbuf);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 3), result.?.params.len);
    try std.testing.expectEqualStrings("table", result.?.params[0].name);
    try std.testing.expectEqualStrings("docs", result.?.params[0].value);
    try std.testing.expectEqualStrings("key", result.?.params[1].name);
    try std.testing.expectEqualStrings("doc%2Fa", result.?.params[1].value);
    try std.testing.expectEqualStrings("artifact", result.?.params[2].name);
    try std.testing.expectEqualStrings("document_units_v1", result.?.params[2].value);

    try std.testing.expect(router.find(.POST, "/tables/docs/documents/doc%2Fa/artifacts/document_units_v1", &pbuf) == null);
    try std.testing.expect(router.find(.POST, "/tables/docs/documents/doc%2Fa/artifacts/:reprocess", &pbuf) == null);
}

test "Router wildcard route" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.GET, "/static/*", handler);

    const result1 = router.find(.GET, "/static/css/style.css", &pbuf);
    try std.testing.expect(result1 != null);

    const result2 = router.find(.GET, "/static/js/app.js", &pbuf);
    try std.testing.expect(result2 != null);

    const result3 = router.find(.GET, "/other/path", &pbuf);
    try std.testing.expect(result3 == null);
}

test "Router wildcard at root" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.GET, "/*", handler);

    const result1 = router.find(.GET, "/anything", &pbuf);
    try std.testing.expect(result1 != null);

    const result2 = router.find(.GET, "/deep/nested/path", &pbuf);
    try std.testing.expect(result2 != null);
}

test "Router no match" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.GET, "/users", handler);

    const result1 = router.find(.GET, "/posts", &pbuf);
    try std.testing.expect(result1 == null);

    const result2 = router.find(.GET, "/users/extra/segments", &pbuf);
    try std.testing.expect(result2 == null);
}

test "Router trailing slash" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.GET, "/users", handler);

    const result = router.find(.GET, "/users/", &pbuf);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?.params.len);
}

test "Router method filtering" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.GET, "/api", handler);
    try router.add(.POST, "/api", handler);

    const result1 = router.find(.GET, "/api", &pbuf);
    try std.testing.expect(result1 != null);

    const result2 = router.find(.POST, "/api", &pbuf);
    try std.testing.expect(result2 != null);

    const result3 = router.find(.DELETE, "/api", &pbuf);
    try std.testing.expect(result3 == null);
}

test "Router allowed methods" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.GET, "/items", handler);
    try router.add(.POST, "/items", handler);
    try router.add(.DELETE, "/items", handler);

    var methods_buf: [16]types.Method = undefined;
    const count = router.allowedMethods("/items", &methods_buf);
    try std.testing.expectEqual(@as(usize, 3), count);
}

test "Router route priority" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const literal_handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    const param_handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.GET, "/users/me", literal_handler);
    try router.add(.GET, "/users/:id", param_handler);

    const result1 = router.find(.GET, "/users/me", &pbuf);
    try std.testing.expect(result1 != null);
    try std.testing.expectEqual(literal_handler, result1.?.handler.function);
    try std.testing.expectEqual(@as(usize, 0), result1.?.params.len);

    const result2 = router.find(.GET, "/users/123", &pbuf);
    try std.testing.expect(result2 != null);
    try std.testing.expectEqual(param_handler, result2.?.handler.function);
    try std.testing.expectEqual(@as(usize, 1), result2.?.params.len);
    try std.testing.expectEqualStrings("id", result2.?.params[0].name);
    try std.testing.expectEqualStrings("123", result2.?.params[0].value);
}

test "Router empty path segments" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.GET, "/users", handler);

    const result = router.find(.GET, "//users", &pbuf);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?.params.len);
}

test "Router deep nesting" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            unreachable;
        }
    }.h;

    try router.add(.GET, "/a/:b/c/:d/e/:f", handler);

    const result = router.find(.GET, "/a/1/c/2/e/3", &pbuf);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 3), result.?.params.len);
    try std.testing.expectEqualStrings("b", result.?.params[0].name);
    try std.testing.expectEqualStrings("1", result.?.params[0].value);
    try std.testing.expectEqualStrings("d", result.?.params[1].name);
    try std.testing.expectEqualStrings("2", result.?.params[1].value);
    try std.testing.expectEqualStrings("f", result.?.params[2].name);
    try std.testing.expectEqualStrings("3", result.?.params[2].value);
}

test "Router no routes registered" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const result1 = router.find(.GET, "/anything", &pbuf);
    try std.testing.expect(result1 == null);

    const result2 = router.find(.POST, "/", &pbuf);
    try std.testing.expect(result2 == null);

    const result3 = router.find(.DELETE, "/some/deep/path", &pbuf);
    try std.testing.expect(result3 == null);
}

test "Router rejects duplicate method and pattern registrations" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            return error.TestUnexpectedResult;
        }
    }.h;

    try router.add(.GET, "/healthz", handler);
    try std.testing.expectError(error.DuplicateRoute, router.add(.GET, "/healthz", handler));
    try router.add(.POST, "/healthz", handler);

    try router.add(.GET, "/tables/:table_name/indexes/:index_name", handler);
    try std.testing.expectError(
        error.DuplicateRoute,
        router.add(.GET, "/tables/:table/indexes/:index", handler),
    );
}

test "RouteGroup routes use owned pattern strings" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            return error.TestUnexpectedResult;
        }
    }.h;

    // Create a group and add a route. The group's prefix+path concatenation
    // is built with a temporary ArrayList that is freed when add() returns.
    // The route must hold an owned copy so the pattern remains valid.
    var group = router.group("/api");
    try group.add(.GET, "/users", handler);

    // If the pattern were not owned, this lookup would read freed memory.
    const result = router.find(.GET, "/api/users", &pbuf);
    try std.testing.expect(result != null);
}

test "RouteGroup convenience methods" {
    const allocator = std.testing.allocator;
    var router = Router.init(allocator);
    defer router.deinit();
    var pbuf: [16]RouteParam = undefined;

    const handler = struct {
        fn h(_: *@import("server.zig").Context) anyerror!@import("../core/response.zig").Response {
            return error.TestUnexpectedResult;
        }
    }.h;

    var api = router.group("/api");
    try api.get("/get", handler);
    try api.post("/post", handler);
    try api.put("/put", handler);
    try api.delete("/delete", handler);
    try api.patch("/patch", handler);
    try api.head("/head", handler);
    try api.options("/options", handler);

    try std.testing.expect(router.find(.GET, "/api/get", &pbuf) != null);
    try std.testing.expect(router.find(.POST, "/api/post", &pbuf) != null);
    try std.testing.expect(router.find(.PUT, "/api/put", &pbuf) != null);
    try std.testing.expect(router.find(.DELETE, "/api/delete", &pbuf) != null);
    try std.testing.expect(router.find(.PATCH, "/api/patch", &pbuf) != null);
    try std.testing.expect(router.find(.HEAD, "/api/head", &pbuf) != null);
    try std.testing.expect(router.find(.OPTIONS, "/api/options", &pbuf) != null);
}
