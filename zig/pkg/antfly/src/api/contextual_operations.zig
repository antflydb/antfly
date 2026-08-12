// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral result contracts for contextual protocol operations.

const std = @import("std");

pub const Method = enum {
    get,
    post,
    put,
    delete,
};

pub const OwnedResponse = struct {
    status: u16 = 200,
    content_type: []const u8,
    body: []u8,
    public_cors: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub fn json(body: []u8, public_cors: bool) OwnedResponse {
    return .{
        .content_type = "application/json",
        .body = body,
        .public_cors = public_cors,
    };
}

pub fn bytes(content_type: []const u8, body: []u8) OwnedResponse {
    return .{
        .content_type = content_type,
        .body = body,
    };
}

test "owned contextual response releases its body" {
    const alloc = std.testing.allocator;
    var response = json(try alloc.dupe(u8, "{}"), false);
    response.deinit(alloc);
}
