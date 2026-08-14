// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-independent ingress contract for HA administration and internal
//! replication operations. Public HTTP adapters and in-process runtimes use
//! the same request shape without manufacturing a legacy HTTP request.

const std = @import("std");

pub const Method = enum {
    get,
    post,
    put,
    delete,
};

pub const Request = struct {
    method: Method,
    target: []const u8,
    authorization: ?[]const u8 = null,
    content_type: ?[]const u8 = null,
    body: []const u8 = "",
};

pub const OwnedResponse = struct {
    owner_allocator: std.mem.Allocator,
    status: u16 = 200,
    content_type: []u8,
    body: []u8,

    pub fn deinit(self: *@This()) void {
        self.owner_allocator.free(self.content_type);
        if (self.body.len > 0) self.owner_allocator.free(self.body);
        self.* = undefined;
    }
};

pub const Executor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        execute: *const fn (ptr: *anyopaque, request: Request) anyerror!OwnedResponse,
    };

    pub fn execute(self: Executor, request: Request) !OwnedResponse {
        return self.vtable.execute(self.ptr, request);
    }
};

test "storage.ha typed HTTP operation response owns its body" {
    var response = OwnedResponse{
        .owner_allocator = std.testing.allocator,
        .content_type = try std.testing.allocator.dupe(u8, "text/plain"),
        .body = try std.testing.allocator.dupe(u8, "ok"),
    };
    response.deinit();
}
