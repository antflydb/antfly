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
const runtime_callback_abi = @import("../../runtime_callback_abi.zig");
const CancellationToken = @import("../cancellation.zig").CancellationToken;

pub const metadata_not_leader_header = "X-Antfly-Metadata-Not-Leader";
pub const metadata_not_leader_value = "true";
/// Stronger than the authority-routing hint above: mutation clients may move
/// to another replica only when this response proves no proposal was admitted.
pub const metadata_mutation_not_admitted_header = "X-Antfly-Metadata-Mutation-Not-Admitted";
pub const metadata_mutation_not_admitted_value = "true";

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
    /// Optional listener-owned signal (for example an H2 RST_STREAM). It is
    /// borrowed for the request lifetime and complements local cancellation.
    borrowed: ?*const std.atomic.Value(bool) = null,
    borrowed_context: ?*const anyopaque = null,
    borrowed_is_cancelled: ?*const fn (*const anyopaque) bool = null,

    pub fn cancel(self: *RequestCancellation) void {
        self.cancelled.store(true, .release);
    }

    pub fn isCancelled(self: *const RequestCancellation) bool {
        return self.cancelled.load(.acquire) or
            (self.borrowed != null and self.borrowed.?.load(.acquire)) or
            self.isBorrowedCallbackCancelled();
    }

    fn isBorrowedCallbackCancelled(self: *const RequestCancellation) bool {
        const context = self.borrowed_context orelse return false;
        const callback = self.borrowed_is_cancelled orelse return false;
        return callback(context);
    }

    /// The listener installs at most one cancellation source per transport:
    /// H2 borrows the stream signal, while H1 uses the local socket watcher.
    pub fn signal(self: *const RequestCancellation) *const std.atomic.Value(bool) {
        return self.borrowed orelse &self.cancelled;
    }

    pub fn token(self: *const RequestCancellation) CancellationToken {
        return .{
            .ptr = self,
            .is_cancelled_fn = struct {
                fn call(raw: *const anyopaque) bool {
                    const cancellation: *const RequestCancellation = @ptrCast(@alignCast(raw));
                    return cancellation.isCancelled();
                }
            }.call,
        };
    }

    pub fn fromToken(token_value: CancellationToken) RequestCancellation {
        return .{
            .borrowed_context = token_value.ptr,
            .borrowed_is_cancelled = token_value.is_cancelled_fn,
        };
    }
};

test "RequestCancellation observes a borrowed listener signal" {
    var listener_signal = std.atomic.Value(bool).init(false);
    const cancellation = RequestCancellation{ .borrowed = &listener_signal };
    try std.testing.expect(!cancellation.isCancelled());
    listener_signal.store(true, .release);
    try std.testing.expect(cancellation.isCancelled());
}

test "RequestCancellation safely ignores incomplete semantic tokens" {
    var state = false;
    const cancellation = RequestCancellation.fromToken(.{ .ptr = &state });
    try std.testing.expect(!cancellation.isCancelled());
}

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
    delivery_tracker: ?*RequestDeliveryTracker = null,

    pub fn header(self: HttpRequest, name: []const u8) ?[]const u8 {
        for (self.headers) |entry| {
            if (std.ascii.eqlIgnoreCase(entry.name, name)) return entry.value;
        }
        return null;
    }
};

/// Tracks whether an HTTP request could have reached its peer. Callers that
/// need at-most-once retry semantics can use this to distinguish local setup
/// failures from failures observed after transmission began. Executors leave
/// the state unknown unless they can identify the send boundary precisely.
pub const RequestDeliveryTracker = struct {
    pub const State = enum(u8) {
        unknown,
        not_sent,
        may_have_been_sent,
    };

    state: std.atomic.Value(u8) = .init(@backingInt(State.unknown)),

    /// Enter an executor whose send boundary is not yet known. The executor
    /// may subsequently prove `not_sent` or advance to `may_have_been_sent`.
    pub fn markUnknown(self: *RequestDeliveryTracker) void {
        self.state.store(@backingInt(State.unknown), .release);
    }

    pub fn markNotSent(self: *RequestDeliveryTracker) void {
        self.state.store(@backingInt(State.not_sent), .release);
    }

    pub fn markMayHaveBeenSent(self: *RequestDeliveryTracker) void {
        self.state.store(@backingInt(State.may_have_been_sent), .release);
    }

    pub fn load(self: *const RequestDeliveryTracker) State {
        return @fromBackingInt(@intCast(self.state.load(.acquire)));
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

    pub fn header(self: HttpResponse, name: []const u8) ?[]const u8 {
        for (self.headers) |entry| {
            if (std.ascii.eqlIgnoreCase(entry.name, name)) return entry.value;
        }
        return null;
    }

    pub fn deinit(self: *HttpResponse, fallback_alloc: std.mem.Allocator) void {
        const alloc = self.owner_allocator orelse fallback_alloc;
        if (self.content_type) |content_type| alloc.free(content_type);
        for (self.headers) |*entry| entry.deinit(alloc);
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
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        start: *const fn (*anyopaque, std.mem.Allocator, StreamingResponse) anyerror!void,
        write_all: *const fn (*anyopaque, []const u8) anyerror!void,
        flush: *const fn (*anyopaque) anyerror!void,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn start(self: StreamWriter, alloc: std.mem.Allocator, response: StreamingResponse) !void {
        try BoundaryAbi.call("start", self.boundary_dispatch, self.vtable.start, .{ self.ptr, alloc, response });
    }

    pub fn writeAll(self: StreamWriter, bytes: []const u8) !void {
        try BoundaryAbi.call("write_all", self.boundary_dispatch, self.vtable.write_all, .{ self.ptr, bytes });
    }

    pub fn flush(self: StreamWriter) !void {
        try BoundaryAbi.call("flush", self.boundary_dispatch, self.vtable.flush, .{self.ptr});
    }
};

pub const StreamingRequestExecutor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        execute: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, req: HttpRequest, writer: StreamWriter) anyerror!bool,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn execute(self: StreamingRequestExecutor, alloc: std.mem.Allocator, req: HttpRequest, writer: StreamWriter) !bool {
        return try BoundaryAbi.call("execute", self.boundary_dispatch, self.vtable.execute, .{ self.ptr, alloc, req, writer });
    }
};

pub const RequestExecutor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        execute: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, req: HttpRequest) anyerror!HttpResponse,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn execute(self: RequestExecutor, alloc: std.mem.Allocator, req: HttpRequest) !HttpResponse {
        // Caller-side request construction may establish `not_sent`, but that
        // proof ends when control crosses an arbitrary executor boundary. A
        // tracking-aware executor can restore `not_sent` during its own local
        // setup and must advance the state before transmission. An executor
        // that does not implement tracking therefore remains safely unknown.
        if (req.delivery_tracker) |tracker| tracker.markUnknown();
        return try BoundaryAbi.call("execute", self.boundary_dispatch, self.vtable.execute, .{ self.ptr, alloc, req });
    }
};

test "request executor invalidates caller-side delivery proof at its boundary" {
    const ObservingExecutor = struct {
        observed: ?RequestDeliveryTracker.State = null,

        fn iface(self: *@This()) RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, req: HttpRequest) anyerror!HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
            self.observed = tracker.load();
            return error.Timeout;
        }
    };

    var tracker: RequestDeliveryTracker = .{};
    tracker.markNotSent();
    var observing = ObservingExecutor{};
    try std.testing.expectError(error.Timeout, observing.iface().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "http://127.0.0.1/internal",
        .delivery_tracker = &tracker,
    }));
    try std.testing.expectEqual(RequestDeliveryTracker.State.unknown, observing.observed.?);
    try std.testing.expectEqual(RequestDeliveryTracker.State.unknown, tracker.load());
}

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
