// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Diagnostic-only live publication of canonical VOPR events.
//!
//! Publication is deliberately non-fallible at the runner boundary. The
//! built-in bounded NDJSON observer converts encoding overflow and sink failure
//! into diagnostic counters, so those conditions cannot perturb choices, time,
//! properties, replay artifacts, or the outcome of the history. Custom
//! observers are synchronous and must not block or panic.

const std = @import("std");
const trace = @import("trace.zig");

pub const format = "vopr-event-stream-v1";

pub const Observer = struct {
    ptr: *anyopaque,
    publish_fn: *const fn (*anyopaque, trace.EventRecord) void,

    /// `record.name` is borrowed only for this synchronous call. Observers
    /// that retain data must copy it into diagnostic-owned storage. Custom
    /// implementations must be non-blocking and non-panicking; use `Ndjson`
    /// when sink failures need to be isolated automatically.
    pub fn publish(self: Observer, record: trace.EventRecord) void {
        self.publish_fn(self.ptr, record);
    }
};

pub const WriteSink = struct {
    ptr: *anyopaque,
    write_fn: *const fn (*anyopaque, []const u8) anyerror!void,

    pub fn write(self: WriteSink, bytes: []const u8) !void {
        try self.write_fn(self.ptr, bytes);
    }
};

pub const Stats = struct {
    published: u64 = 0,
    dropped_oversize: u64 = 0,
    dropped_disabled: u64 = 0,
    write_failures: u64 = 0,
};

/// Allocation-free NDJSON observer. `scratch` is caller-owned and must remain
/// live for the stream. Each callback receives exactly one complete line.
pub const Ndjson = struct {
    run_id: []const u8,
    sink: WriteSink,
    scratch: []u8,
    stats: Stats = .{},
    disabled: bool = false,

    pub fn init(run_id: []const u8, sink: WriteSink, scratch: []u8) !Ndjson {
        if (run_id.len == 0) return error.EmptyEventStreamRunId;
        if (scratch.len < 128) return error.EventStreamBufferTooSmall;
        return .{ .run_id = run_id, .sink = sink, .scratch = scratch };
    }

    pub fn observer(self: *Ndjson) Observer {
        return .{ .ptr = self, .publish_fn = publish };
    }

    fn publish(ptr: *anyopaque, record: trace.EventRecord) void {
        const self: *Ndjson = @ptrCast(@alignCast(ptr));
        if (self.disabled) {
            self.stats.dropped_disabled +|= 1;
            return;
        }
        var writer = std.Io.Writer.fixed(self.scratch);
        std.json.Stringify.value(.{
            .format = format,
            .run_id = self.run_id,
            .event = record,
        }, .{}, &writer) catch {
            self.stats.dropped_oversize +|= 1;
            return;
        };
        writer.writeByte('\n') catch {
            self.stats.dropped_oversize +|= 1;
            return;
        };
        self.sink.write(writer.buffered()) catch {
            self.stats.write_failures +|= 1;
            self.disabled = true;
            return;
        };
        self.stats.published +|= 1;
    }
};

test "live NDJSON event publication is bounded and diagnostic only" {
    const Capture = struct {
        bytes: std.ArrayListUnmanaged(u8) = .empty,
        fail: bool = false,

        fn write(ptr: *anyopaque, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.InjectedObserverFailure;
            try self.bytes.appendSlice(std.testing.allocator, bytes);
        }
    };
    var capture: Capture = .{};
    defer capture.bytes.deinit(std.testing.allocator);
    var scratch: [512]u8 = undefined;
    var stream = try Ndjson.init("history-1", .{ .ptr = &capture, .write_fn = Capture.write }, &scratch);
    const observer = stream.observer();
    observer.publish(.{ .index = 1, .ordinal = 0, .id = 7, .name = "ready", .kind = .state_change, .actor_id = 2, .resource_id = 3, .payload_digest = 9 });
    try std.testing.expectEqual(@as(u64, 1), stream.stats.published);
    try std.testing.expect(std.mem.indexOf(u8, capture.bytes.items, "\"format\":\"vopr-event-stream-v1\"") != null);
    capture.fail = true;
    observer.publish(.{ .index = 2, .ordinal = 0, .id = 8, .name = "failed", .kind = .domain, .actor_id = null, .resource_id = null, .payload_digest = 0 });
    try std.testing.expectEqual(@as(u64, 1), stream.stats.write_failures);
}
