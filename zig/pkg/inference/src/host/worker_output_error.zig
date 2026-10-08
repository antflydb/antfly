// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Caller-owned failures stay local while layout-only callbacks signal failure.
const std = @import("std");
const http = @import("antfly_runtime_abi").http_abi;

pub const Output = struct {
    cause: ?anyerror = null,
    started: bool = false,

    pub fn record(self: *Output, err: anyerror) http.CallbackStatus {
        if (self.cause == null) self.cause = err;
        return switch (self.cause.?) {
            error.Canceled, error.Cancelled => .canceled,
            error.Timeout, error.DeadlineExceeded => .timeout,
            else => .failed,
        };
    }
    pub fn result(self: *const Output, status: http.CallbackStatus) !void {
        if (self.cause) |err| return err;
        return switch (status) {
            .ok => {},
            .canceled => error.Cancelled,
            .timeout => error.Timeout,
            .body_too_large => error.BodyTooLarge,
            .body_capacity_exceeded => error.BodyCapacityExceeded,
            .end_of_stream => error.EndOfStream,
            .failed => error.InferenceTransportFailed,
        };
    }
    pub fn start(self: *Output, sink: http.StreamSink, status: u16, content_type: http.Bytes, headers: http.HeaderList) !void {
        if (self.cause) |err| return err;
        if (self.started) return error.StreamAlreadyStarted;
        const callback = sink.start orelse return error.StreamingUnavailable;
        // The callback may publish headers and then fail weighted reentry.
        // Returning a second response after that attempt is unsafe.
        self.started = true;
        try self.result(callback(sink.context, status, content_type, headers));
    }
    pub fn write(self: *Output, sink: http.StreamSink, bytes: http.Bytes) !void {
        if (self.cause) |err| return err;
        try self.result((sink.write orelse return error.StreamingUnavailable)(sink.context, bytes));
    }
    pub fn close(self: *Output, sink: http.StreamSink) !void {
        if (self.cause) |err| return err;
        try self.result((sink.close orelse return error.StreamingUnavailable)(sink.context));
    }
};

test "worker output callbacks preserve the first local scheduler and original fence cause" {
    for ([_]anyerror{ error.Canceled, error.Cancelled, error.Timeout, error.DeadlineExceeded, error.RunnableWaitTimeout, error.SchedulerFull, error.SchedulerClosed, error.ResourceTemporarilyUnavailable }) |err| {
        var output: Output = .{};
        const status = output.record(err);
        try std.testing.expectError(err, output.result(status));
        try std.testing.expectError(err, output.result(.failed));
        _ = output.record(error.EndOfStream);
        try std.testing.expectError(err, output.result(.timeout));
        try std.testing.expectError(err, output.result(.ok));
    }
}

test "worker output callbacks retain existing transport-only status classes" {
    const output: Output = .{};
    try output.result(.ok);
    try std.testing.expectError(error.Cancelled, output.result(.canceled));
    try std.testing.expectError(error.Timeout, output.result(.timeout));
    try std.testing.expectError(error.BodyTooLarge, output.result(.body_too_large));
    try std.testing.expectError(error.BodyCapacityExceeded, output.result(.body_capacity_exceeded));
    try std.testing.expectError(error.EndOfStream, output.result(.end_of_stream));
    try std.testing.expectError(error.InferenceTransportFailed, output.result(.failed));
}

test "worker output header attempt prevents fallback after denied resume and blocks later callbacks" {
    const Probe = struct {
        output: *Output,
        calls: usize = 0,
        fn start(raw: ?*anyopaque, _: u16, _: http.Bytes, _: http.HeaderList) callconv(.c) http.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            return self.output.record(error.RunnableWaitTimeout);
        }
    };
    var output: Output = .{};
    var probe = Probe{ .output = &output };
    const sink: http.StreamSink = .{ .context = &probe, .start = Probe.start };
    try std.testing.expectError(error.RunnableWaitTimeout, output.start(sink, 200, .init("text/event-stream"), .{}));
    try std.testing.expect(output.started);
    try std.testing.expectError(error.RunnableWaitTimeout, output.start(sink, 503, .init("application/json"), .{}));
    try std.testing.expectError(error.RunnableWaitTimeout, output.write(sink, .init("next")));
    try std.testing.expectError(error.RunnableWaitTimeout, output.close(sink));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    var missing: Output = .{};
    try std.testing.expectError(error.StreamingUnavailable, missing.start(.{}, 200, .init("text/event-stream"), .{}));
    try std.testing.expect(!missing.started);
}
