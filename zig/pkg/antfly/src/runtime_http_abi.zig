// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Versioned, C-layout HTTP values shared by independently generated runtime
//! units. All byte slices are borrowed for the duration of a call. Response
//! storage remains owned by the unit that produced it and is released through
//! that unit's explicit destroy export.

pub const HttpMethod = enum(c_int) {
    get,
    post,
    put,
    delete,
    patch,
};

pub const RequestBodyMode = enum(c_int) {
    none,
    buffered,
};

pub const CallbackStatus = enum(c_int) {
    ok,
    failed,
    canceled,
    timeout,
    body_too_large,
    body_capacity_exceeded,
    end_of_stream,
};

pub const Bytes = extern struct {
    ptr: [*]const u8,
    len: usize,

    pub fn init(value: []const u8) Bytes {
        return .{ .ptr = value.ptr, .len = value.len };
    }

    pub fn slice(self: Bytes) []const u8 {
        return self.ptr[0..self.len];
    }
};

pub const OptionalBytes = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,

    pub fn init(value: ?[]const u8) OptionalBytes {
        const present = value orelse return .{};
        return .{ .ptr = present.ptr, .len = present.len };
    }

    pub fn slice(self: OptionalBytes) ?[]const u8 {
        const present = self.ptr orelse return null;
        return present[0..self.len];
    }
};

pub const HeaderView = extern struct {
    name: Bytes,
    value: Bytes,
};

pub const RouteParamView = extern struct {
    name: Bytes,
    value: Bytes,
};

pub const HttpRequestView = extern struct {
    method: HttpMethod,
    path: Bytes,
    query: OptionalBytes = .{},
    headers_ptr: ?[*]const HeaderView = null,
    headers_len: usize = 0,
    params_ptr: ?[*]const RouteParamView = null,
    params_len: usize = 0,
    /// A body which was already available when dispatch crossed the runtime
    /// boundary. Streaming transports leave this absent and expose
    /// `RequestBodySource` on the call context instead.
    body: OptionalBytes = .{},
    authorization: OptionalBytes = .{},
    content_type: OptionalBytes = .{},
};

/// Transport-owned lazy request body. The returned bytes remain borrowed for
/// the lifetime of the surrounding handler call. `streaming` identifies body
/// sources which are still reading from the peer, allowing application-owned
/// admission to run before the transport buffers the upload.
pub const RequestBodySource = extern struct {
    context: ?*anyopaque = null,
    read_all: ?*const fn (?*anyopaque, *OptionalBytes) callconv(.c) CallbackStatus = null,
    streaming: u8 = 0,
};

/// Transport-owned request lifetime state. The callback is the complete
/// semantic contract across independently compiled runtime boundaries.
pub const CancellationView = extern struct {
    context: ?*const anyopaque = null,
    is_cancelled: ?*const fn (?*const anyopaque) callconv(.c) u8 = null,

    pub fn requested(self: CancellationView) bool {
        const callback = self.is_cancelled orelse return false;
        return callback(self.context) != 0;
    }
};

/// Transport-owned response sink used by independently generated handlers.
/// `start` publishes SSE response headers, `write` transfers one body chunk,
/// and `close` commits the terminating frame. Callbacks are request-scoped.
pub const StreamSink = extern struct {
    context: ?*anyopaque = null,
    start: ?*const fn (?*anyopaque, u16) callconv(.c) CallbackStatus = null,
    write: ?*const fn (?*anyopaque, Bytes) callconv(.c) CallbackStatus = null,
    close: ?*const fn (?*anyopaque) callconv(.c) CallbackStatus = null,
};

pub const HttpResponseView = extern struct {
    status: u16 = 500,
    content_type: OptionalBytes = .{},
    headers_ptr: ?[*]const HeaderView = null,
    headers_len: usize = 0,
    body: Bytes,
};

test "runtime HTTP values retain C layout" {
    const std = @import("std");
    try std.testing.expectEqual(.@"extern", @typeInfo(Bytes).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(HttpRequestView).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(HttpResponseView).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(CancellationView).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(RequestBodySource).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(StreamSink).@"struct".layout);
}
