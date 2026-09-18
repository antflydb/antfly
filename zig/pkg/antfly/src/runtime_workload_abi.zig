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
};

pub const ResponseSink = extern struct {
    context: *anyopaque,
    receive: *const fn (*anyopaque, u16, http.OptionalBytes, http.HeaderList, http.Bytes) callconv(.c) errors.Status,
};

pub const OptionalCoordinatorPort = extern struct {
    present: u8 = 0,
    port: CoordinatorPort = undefined,
};

pub const CoordinatorPort = extern struct {
    version: u32 = 1,
    struct_size: u32 = @sizeOf(@This()),
    context: *anyopaque,
    dispatch: *const fn (*anyopaque, *const memory.Allocator, *const Request, *const ResponseSink) callconv(.c) errors.Status,

    pub fn execute(self: CoordinatorPort, alloc: std.mem.Allocator, destination: u64, base_uri: []const u8, request: common.HttpRequest, deadline_ns: u64) !common.HttpResponse {
        if (self.version != 1 or self.struct_size != @sizeOf(CoordinatorPort)) return error.UnsupportedVersion;
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
        const wire: Request = .{
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
        const result = self.dispatch(self.context, &allocator, &wire, &.{ .context = &capture, .receive = Capture.receive });
        if (!result.isOk()) return errors.errorFromStatus(result);
        return capture.result orelse error.InvalidArgument;
    }
};

test "workload admission coordinator ABI copies producer response and retains request cancellation" {
    const Producer = struct {
        calls: usize = 0,
        fn dispatch(raw: *anyopaque, allocator: *const memory.Allocator, request: *const Request, sink: *const ResponseSink) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (!allocator.valid() or request.destination != 8) return errors.statusFromError(error.InvalidArgument);
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
    var invalid = port;
    invalid.version += 1;
    try std.testing.expectError(error.UnsupportedVersion, invalid.execute(std.testing.allocator, 8, "http://worker", request, 1234));
}
