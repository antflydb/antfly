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
    body: Bytes,
    authorization: OptionalBytes = .{},
    content_type: OptionalBytes = .{},
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
}
