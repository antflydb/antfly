// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub const Method = enum {
    GET,
    POST,
    PUT,
    DELETE,
};

pub const HttpRequest = struct {
    method: Method,
    uri: []const u8,
    headers: []const RequestHeader = &.{},
    source_node_id: ?u64 = null,
    authorization: ?[]const u8 = null,
    content_type: ?[]const u8 = null,
    body: []const u8 = &.{},

    pub fn header(self: HttpRequest, name: []const u8) ?[]const u8 {
        for (self.headers) |entry| {
            if (std.ascii.eqlIgnoreCase(entry.name, name)) return entry.value;
        }
        return null;
    }
};

pub const RequestHeader = struct {
    name: []const u8,
    value: []const u8,
};

pub const Header = struct {
    name: []u8,
    value: []u8,

    pub fn deinit(self: *Header, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub const HttpResponse = struct {
    status: u16,
    content_type: ?[]u8 = null,
    headers: []Header = &.{},
    body: []u8 = &.{},

    pub fn deinit(self: *HttpResponse, alloc: std.mem.Allocator) void {
        if (self.content_type) |content_type| alloc.free(content_type);
        for (self.headers) |*header| header.deinit(alloc);
        if (self.headers.len > 0) alloc.free(self.headers);
        if (self.body.len > 0) alloc.free(self.body);
        self.* = undefined;
    }
};

pub const StreamingResponse = struct {
    status: u16,
    content_type: ?[]const u8 = null,
    headers: []const RequestHeader = &.{},
};

pub const StreamWriter = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        start: *const fn (*anyopaque, std.mem.Allocator, StreamingResponse) anyerror!void,
        write_all: *const fn (*anyopaque, []const u8) anyerror!void,
        flush: *const fn (*anyopaque) anyerror!void,
    };

    pub fn start(self: StreamWriter, alloc: std.mem.Allocator, response: StreamingResponse) !void {
        try self.vtable.start(self.ptr, alloc, response);
    }

    pub fn writeAll(self: StreamWriter, bytes: []const u8) !void {
        try self.vtable.write_all(self.ptr, bytes);
    }

    pub fn flush(self: StreamWriter) !void {
        try self.vtable.flush(self.ptr);
    }
};

pub const StreamingRequestExecutor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        execute: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, req: HttpRequest, writer: StreamWriter) anyerror!bool,
    };

    pub fn execute(self: StreamingRequestExecutor, alloc: std.mem.Allocator, req: HttpRequest, writer: StreamWriter) !bool {
        return try self.vtable.execute(self.ptr, alloc, req, writer);
    }
};

pub const RequestExecutor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        execute: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, req: HttpRequest) anyerror!HttpResponse,
    };

    pub fn execute(self: RequestExecutor, alloc: std.mem.Allocator, req: HttpRequest) !HttpResponse {
        return try self.vtable.execute(self.ptr, alloc, req);
    }
};

test "http common types compile" {
    _ = Method;
    _ = HttpRequest;
    _ = RequestHeader;
    _ = Header;
    _ = HttpResponse;
    _ = StreamingResponse;
    _ = StreamWriter;
    _ = StreamingRequestExecutor;
    _ = RequestExecutor;
}
