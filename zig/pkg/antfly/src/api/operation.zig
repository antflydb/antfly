// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral contracts shared by API-kernel operations.
//!
//! Nothing in this module depends on httpx or on the legacy HTTP request and
//! response types. HTTP, internal callers, and future compiled-boundary
//! adapters may all construct these values without manufacturing a wire
//! request.

const std = @import("std");
const platform_time = @import("antfly_platform").time;

pub const ApiError = error{
    Canceled,
    DeadlineExceeded,
    Unauthorized,
    Forbidden,
    NotFound,
    InvalidArgument,
    Conflict,
    Unsupported,
    CapacityExhausted,
    Unavailable,
    Internal,
};

/// Borrowed cancellation source. The owner must keep `ptr` alive for the
/// complete operation call. A callback keeps the kernel independent of the
/// transport's concrete cancellation representation.
pub const CancellationToken = struct {
    ptr: ?*const anyopaque = null,
    is_cancelled_fn: ?*const fn (*const anyopaque) bool = null,

    pub const none: CancellationToken = .{};

    pub fn fromAtomic(signal: *const std.atomic.Value(bool)) CancellationToken {
        return .{
            .ptr = signal,
            .is_cancelled_fn = struct {
                fn call(ptr: *const anyopaque) bool {
                    const value: *const std.atomic.Value(bool) = @ptrCast(@alignCast(ptr));
                    return value.load(.acquire);
                }
            }.call,
        };
    }

    pub fn isCancelled(self: CancellationToken) bool {
        const ptr = self.ptr orelse return false;
        return self.is_cancelled_fn.?(ptr);
    }

    pub fn check(self: CancellationToken) ApiError!void {
        if (self.isCancelled()) return error.Canceled;
    }
};

/// Authenticated identity projected into an operation. Permissions stay in
/// the authorization policy layer; operations receive only the identity they
/// need for ownership, auditing, and row-policy semantics.
pub const Principal = struct {
    kind: Kind,
    subject: []const u8,

    pub const Kind = enum {
        user,
        service,
        internal,
    };
};

/// An admission permit remains owned by the ingress adapter. The request
/// context borrows it so downstream code can preserve the reservation across
/// nested operations without releasing it accidentally.
pub const AdmissionReservation = struct {
    ptr: *anyopaque,
    release_fn: *const fn (*anyopaque) void,
    released: bool = false,

    pub fn release(self: *AdmissionReservation) void {
        if (self.released) return;
        self.released = true;
        self.release_fn(self.ptr);
    }
};

pub const RequestContext = struct {
    cancellation: CancellationToken = .none,
    /// Absolute monotonic deadline. This deliberately does not use a wall
    /// clock or a transport timeout duration.
    deadline_ns: ?u64 = null,
    /// Borrowed request identity used for correlation. An empty value means
    /// the caller did not supply one; adapters may generate one in middleware.
    request_id: []const u8 = "",
    principal: ?Principal = null,
    admission: ?*AdmissionReservation = null,

    pub fn ensureActive(self: RequestContext) ApiError!void {
        try self.cancellation.check();
        if (self.deadline_ns) |deadline| {
            if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        }
    }
};

/// A synchronous sink applies backpressure by not returning from `writeAll`
/// until the bytes have been accepted or the operation has failed. The sink
/// is transport-owned and borrowed for the duration of `produce`.
pub const StreamSink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        write_all: *const fn (*anyopaque, []const u8) anyerror!void,
        flush: *const fn (*anyopaque) anyerror!void,
        close: *const fn (*anyopaque) anyerror!void,
    };

    pub fn writeAll(self: StreamSink, bytes: []const u8) !void {
        try self.vtable.write_all(self.ptr, bytes);
    }

    pub fn flush(self: StreamSink) !void {
        try self.vtable.flush(self.ptr);
    }

    pub fn close(self: StreamSink) !void {
        try self.vtable.close(self.ptr);
    }
};

pub const StreamProducer = struct {
    ptr: *anyopaque,
    produce_fn: *const fn (*anyopaque, RequestContext, StreamSink) anyerror!void,

    pub fn produce(self: StreamProducer, request: RequestContext, sink: StreamSink) !void {
        try request.ensureActive();
        try self.produce_fn(self.ptr, request, sink);
    }
};

pub const JsonResult = struct {
    bytes: []const u8,
};

pub const BytesResult = struct {
    bytes: []const u8,
    content_type: []const u8 = "application/octet-stream",
};

pub const StatusResult = struct {
    status: u16 = 204,
};

pub const OperationResult = union(enum) {
    json: JsonResult,
    bytes: BytesResult,
    stream: StreamProducer,
    empty: StatusResult,
};

test "request context observes cancellation before deadline" {
    var signal = std.atomic.Value(bool).init(true);
    const request = RequestContext{
        .cancellation = CancellationToken.fromAtomic(&signal),
        .deadline_ns = 0,
    };
    try std.testing.expectError(error.Canceled, request.ensureActive());
}

test "admission reservation releases exactly once" {
    const Counter = struct {
        count: usize = 0,

        fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count += 1;
        }
    };
    var counter = Counter{};
    var reservation = AdmissionReservation{
        .ptr = &counter,
        .release_fn = Counter.release,
    };
    reservation.release();
    reservation.release();
    try std.testing.expectEqual(@as(usize, 1), counter.count);
}
