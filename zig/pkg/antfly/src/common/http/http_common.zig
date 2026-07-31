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

pub const metadata_not_leader_header = "X-Antfly-Metadata-Not-Leader";
pub const metadata_not_leader_value = "true";

pub const Method = enum {
    GET,
    POST,
    PUT,
    DELETE,
};

/// Listener-owned signal that remains valid for the lifetime of one request.
/// Executors must only borrow it synchronously; it is not serializable and
/// must never outlive the request that supplied it.
pub const RequestCancellation = struct {
    cancelled: std.atomic.Value(bool) = .init(false),

    pub fn cancel(self: *RequestCancellation) void {
        self.cancelled.store(true, .release);
    }

    pub fn isCancelled(self: *const RequestCancellation) bool {
        return self.cancelled.load(.acquire);
    }
};

pub const HttpRequest = struct {
    method: Method,
    uri: []const u8,
    headers: []const RequestHeader = &.{},
    source_node_id: ?u64 = null,
    authorization: ?[]const u8 = null,
    content_type: ?[]const u8 = null,
    timeout_ms: ?u32 = null,
    body: []const u8 = &.{},
    cancellation: ?*const RequestCancellation = null,

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
    /// Allocator that owns all response allocations. Executors that allocate
    /// independently of the allocator supplied by the caller must set this.
    owner_allocator: ?std.mem.Allocator = null,
    content_type: ?[]u8 = null,
    headers: []Header = &.{},
    body: []u8 = &.{},

    pub fn deinit(self: *HttpResponse, fallback_alloc: std.mem.Allocator) void {
        const alloc = self.owner_allocator orelse fallback_alloc;
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
    _ = RequestCancellation;
    _ = HttpRequest;
    _ = RequestHeader;
    _ = Header;
    _ = HttpResponse;
    _ = StreamingResponse;
    _ = StreamWriter;
    _ = StreamingRequestExecutor;
    _ = RequestExecutor;
}

test "http response uses its owning allocator" {
    var owner_gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(owner_gpa.deinit() == .ok);
    var fallback_gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(fallback_gpa.deinit() == .ok);

    const owner = owner_gpa.allocator();
    const headers = try owner.alloc(Header, 1);
    headers[0] = .{
        .name = try owner.dupe(u8, "X-Test"),
        .value = try owner.dupe(u8, "owned"),
    };
    var response = HttpResponse{
        .status = 200,
        .owner_allocator = owner,
        .content_type = try owner.dupe(u8, "text/plain"),
        .headers = headers,
        .body = try owner.dupe(u8, "ok"),
    };
    response.deinit(fallback_gpa.allocator());
}
