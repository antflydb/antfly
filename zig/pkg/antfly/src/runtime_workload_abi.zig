// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Synchronous coordinator dispatch across independently compiled runtimes.
//! Requests and allocator descriptors are borrowed for the call. The producer
//! drains its response into the caller's sink before returning; no private
//! allocator, response, coordinator object, or Zig error crosses the boundary.
const std = @import("std");
const http = @import("runtime_http_abi.zig");
const errors = @import("runtime_error_abi.zig");
const memory = @import("runtime_memory_abi");
const common = @import("common/http/http_common.zig");

/// Exact version-1 prefix. A version-1 producer must never dereference the
/// version-2 control endpoint trailer.
pub const RequestV1 = extern struct {
    destination: u64,
    base_uri: http.Bytes,
    method: http.HttpMethod,
    uri: http.Bytes,
    headers: http.HeaderList = .{},
    authorization: http.OptionalBytes = .{},
    content_type: http.OptionalBytes = .{},
    body: http.Bytes,
    deadline_ns: u64,
    cancellation: http.CancellationView = .{},
};

pub const Request = extern struct {
    destination: u64,
    base_uri: http.Bytes,
    method: http.HttpMethod,
    uri: http.Bytes,
    headers: http.HeaderList = .{},
    authorization: http.OptionalBytes = .{},
    content_type: http.OptionalBytes = .{},
    body: http.Bytes,
    deadline_ns: u64,
    cancellation: http.CancellationView = .{},
    /// Only the coordinator's authenticated discovery/fence/status requests
    /// use this URL. Empty means the version-1 public endpoint fallback.
    control_base_uri: http.OptionalBytes = .{},
};

comptime {
    if (@offsetOf(Request, "control_base_uri") != @sizeOf(RequestV1))
        @compileError("workload coordinator v2 request must preserve the complete v1 prefix");
    for (.{ "destination", "base_uri", "method", "uri", "headers", "authorization", "content_type", "body", "deadline_ns", "cancellation" }) |field| {
        if (@offsetOf(Request, field) != @offsetOf(RequestV1, field))
            @compileError("workload coordinator v2 changed a v1 request field offset");
    }
}

pub const ResponseSink = extern struct {
    context: *anyopaque,
    receive: *const fn (*anyopaque, u16, http.OptionalBytes, http.HeaderList, http.Bytes) callconv(.c) errors.Status,
};

pub const OptionalCoordinatorPort = extern struct {
    present: u8 = 0,
    port: CoordinatorPort = undefined,
};

pub const CoordinatorPort = extern struct {
    version: u32 = 2,
    struct_size: u32 = @sizeOf(@This()),
    context: *anyopaque,
    dispatch: *const fn (*anyopaque, *const memory.Allocator, *const RequestV1, *const ResponseSink) callconv(.c) errors.Status,

    pub fn execute(self: CoordinatorPort, alloc: std.mem.Allocator, destination: u64, base_uri: []const u8, request: common.HttpRequest, deadline_ns: u64) !common.HttpResponse {
        return self.executeWithControlUri(alloc, destination, base_uri, null, request, deadline_ns);
    }

    pub fn executeWithControlUri(self: CoordinatorPort, alloc: std.mem.Allocator, destination: u64, base_uri: []const u8, control_base_uri: ?[]const u8, request: common.HttpRequest, deadline_ns: u64) !common.HttpResponse {
        if ((self.version != 1 and self.version != 2) or self.struct_size != @sizeOf(CoordinatorPort)) return error.UnsupportedVersion;
        const Capture = struct {
            alloc: std.mem.Allocator,
            result: ?common.HttpResponse = null,
            fn receive(raw: *anyopaque, status: u16, content_type: http.OptionalBytes, headers: http.HeaderList, body: http.Bytes) callconv(.c) errors.Status {
                const capture: *@This() = @ptrCast(@alignCast(raw));
                capture.copy(status, content_type, headers, body) catch |err| return errors.statusFromError(err);
                return .{};
            }
            fn copy(capture: *@This(), status: u16, content_type: http.OptionalBytes, headers: http.HeaderList, body: http.Bytes) !void {
                if (capture.result != null) return error.InvalidArgument;
                var result: common.HttpResponse = .{ .status = status };
                errdefer result.deinit(capture.alloc);
                result.body = try capture.alloc.dupe(u8, body.slice());
                if (content_type.slice()) |value| result.content_type = try capture.alloc.dupe(u8, value);
                const owned = try capture.alloc.alloc(common.Header, headers.len);
                var initialized: usize = 0;
                errdefer {
                    for (owned[0..initialized]) |*header| header.deinit(capture.alloc);
                    capture.alloc.free(owned);
                }
                for (headers.slice(), owned) |header, *target| {
                    const name = try capture.alloc.dupe(u8, header.name.slice());
                    errdefer capture.alloc.free(name);
                    target.* = .{ .name = name, .value = try capture.alloc.dupe(u8, header.value.slice()) };
                    initialized += 1;
                }
                result.headers = owned;
                capture.result = result;
            }
        };
        var capture: Capture = .{ .alloc = alloc };
        errdefer if (capture.result) |*result| result.deinit(alloc);
        const allocator = memory.Allocator.fromStd(&alloc);
        const headers = try alloc.alloc(http.HeaderView, request.headers.len);
        defer alloc.free(headers);
        for (request.headers, headers) |header, *view| view.* = .{ .name = .init(header.name), .value = .init(header.value) };
        if (request.delivery_tracker) |tracker| tracker.markUnknown();
        const now = @import("antfly_platform").time.monotonicNs();
        const effective_deadline = if (request.timeout_ms) |timeout| @min(deadline_ns, now +| @as(u64, timeout) * std.time.ns_per_ms) else deadline_ns;
        const wire_v1: RequestV1 = .{
            .destination = destination,
            .base_uri = .init(base_uri),
            .method = switch (request.method) {
                .GET => .get,
                .POST => .post,
                .PUT => .put,
                .DELETE => .delete,
            },
            .uri = .init(request.uri),
            .headers = .{ .ptr = headers.ptr, .len = headers.len },
            .authorization = .init(request.authorization),
            .content_type = .init(request.content_type),
            .body = .init(request.body),
            .deadline_ns = effective_deadline,
            .cancellation = if (request.cancellation) |cancellation| .{ .context = cancellation, .is_cancelled = struct {
                fn check(raw: ?*const anyopaque) callconv(.c) u8 {
                    const value: *const common.RequestCancellation = @ptrCast(@alignCast(raw.?));
                    return @intFromBool(value.isCancelled());
                }
            }.check } else .{},
        };
        const sink: ResponseSink = .{ .context = &capture, .receive = Capture.receive };
        const result = if (self.version == 1)
            self.dispatch(self.context, &allocator, &wire_v1, &sink)
        else blk: {
            const wire_v2: Request = .{
                .destination = wire_v1.destination,
                .base_uri = wire_v1.base_uri,
                .method = wire_v1.method,
                .uri = wire_v1.uri,
                .headers = wire_v1.headers,
                .authorization = wire_v1.authorization,
                .content_type = wire_v1.content_type,
                .body = wire_v1.body,
                .deadline_ns = wire_v1.deadline_ns,
                .cancellation = wire_v1.cancellation,
                .control_base_uri = .init(control_base_uri),
            };
            break :blk self.dispatch(self.context, &allocator, @ptrCast(&wire_v2), &sink);
        };
        if (!result.isOk()) return errors.errorFromStatus(result);
        return capture.result orelse error.InvalidArgument;
    }
};

test "workload admission coordinator ABI copies producer response and retains request cancellation" {
    const Producer = struct {
        calls: usize = 0,
        expected_control: ?[]const u8 = null,
        fn dispatch(raw: *anyopaque, allocator: *const memory.Allocator, request: *const RequestV1, sink: *const ResponseSink) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (!allocator.valid() or request.destination != 8) return errors.statusFromError(error.InvalidArgument);
            if (!std.mem.eql(u8, request.base_uri.slice(), "http://worker")) return errors.statusFromError(error.InvalidArgument);
            if (self.expected_control) |expected| {
                const extended: *const Request = @ptrCast(request);
                if (!std.mem.eql(u8, extended.control_base_uri.slice() orelse "", expected)) return errors.statusFromError(error.InvalidArgument);
            }
            if (request.cancellation.is_cancelled.?(request.cancellation.context) != 0) return errors.statusFromError(error.Canceled);
            const alloc = allocator.asStd();
            const body = alloc.dupe(u8, "owned response") catch |err| return errors.statusFromError(err);
            defer alloc.free(body);
            const headers = [_]http.HeaderView{.{ .name = .init("x-test"), .value = .init("temporary") }};
            return sink.receive(sink.context, 201, .init("text/plain"), .{ .ptr = &headers, .len = headers.len }, .init(body));
        }
    };
    var producer: Producer = .{};
    const port: CoordinatorPort = .{ .context = &producer, .dispatch = Producer.dispatch };
    var cancellation: common.RequestCancellation = .{};
    const request: common.HttpRequest = .{ .method = .GET, .uri = "http://worker/internal/v1/test", .cancellation = &cancellation };
    var response = try port.execute(std.testing.allocator, 8, "http://worker", request, 1234);
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("owned response", response.body);
    try std.testing.expectEqualStrings("temporary", response.header("x-test").?);
    cancellation.cancel();
    try std.testing.expectError(error.Canceled, port.execute(std.testing.allocator, 8, "http://worker", request, 1234));
    try std.testing.expectEqual(@as(usize, 2), producer.calls);
    cancellation = .{};
    producer.expected_control = "http://worker-control";
    var controlled = try port.executeWithControlUri(std.testing.allocator, 8, "http://worker", "http://worker-control", request, 1234);
    controlled.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), producer.calls);
    producer.expected_control = null;
    var legacy = port;
    legacy.version = 1;
    var old_response = try legacy.executeWithControlUri(std.testing.allocator, 8, "http://worker", "http://ignored-control", request, 1234);
    old_response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 4), producer.calls);
    var invalid = port;
    invalid.version += 1;
    try std.testing.expectError(error.UnsupportedVersion, invalid.execute(std.testing.allocator, 8, "http://worker", request, 1234));
}
