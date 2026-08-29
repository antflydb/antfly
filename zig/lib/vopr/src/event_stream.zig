// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Diagnostic-only live publication of canonical VOPR events.
//!
//! The built-in stream serializes events into a caller-owned bounded queue at
//! the runner boundary. Publication never calls external code, allocates, or
//! blocks. A diagnostic consumer drains complete NDJSON records separately;
//! sink failures retain the oldest queued record for a later retry. Queue
//! overflow uses an explicit drop-newest policy and cannot perturb choices,
//! time, properties, replay artifacts, or the outcome of a history.

const std = @import("std");
const trace = @import("trace.zig");

pub const format = "vopr-event-stream-v2";

pub const Observer = struct {
    ptr: *anyopaque,
    publish_fn: *const fn (*anyopaque, trace.EventRecord) void,

    /// `record.name` is borrowed only for this synchronous call. Implementors
    /// must not retain it. Prefer `BufferedNdjson`, whose publication path is
    /// allocation-free, bounded, non-blocking, and cannot call a sink.
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
    enqueued: u64 = 0,
    delivered: u64 = 0,
    dropped_full: u64 = 0,
    dropped_oversize: u64 = 0,
    dropped_closed: u64 = 0,
    write_failures: u64 = 0,
};

/// A caller-owned bounded stream. `max_line_bytes` includes the trailing
/// newline. Queue capacity is the number of slots supplied to `init`.
pub fn BufferedNdjson(comptime max_line_bytes: usize) type {
    if (max_line_bytes < 128) @compileError("VOPR event stream lines require at least 128 bytes");
    return struct {
        const Self = @This();

        pub const Slot = struct {
            bytes: [max_line_bytes]u8 = undefined,
            len: usize = 0,
        };

        run_id: []const u8,
        slots: []Slot,
        head: usize = 0,
        count: usize = 0,
        closed: bool = false,
        stats: Stats = .{},

        pub fn init(run_id: []const u8, slots: []Slot) !Self {
            if (run_id.len == 0) return error.EmptyEventStreamRunId;
            if (slots.len == 0) return error.EmptyEventStreamQueue;
            return .{ .run_id = run_id, .slots = slots };
        }

        pub fn observer(self: *Self) Observer {
            return .{ .ptr = self, .publish_fn = publish };
        }

        pub fn close(self: *Self) void {
            self.closed = true;
        }

        pub fn pending(self: *const Self) usize {
            return self.count;
        }

        pub fn isClosed(self: *const Self) bool {
            return self.closed;
        }

        /// Drains at most `maximum` complete records. A sink failure is
        /// diagnostic: the oldest record remains queued and draining stops so
        /// a caller may repair or replace the sink and retry without loss.
        pub fn drain(self: *Self, sink: WriteSink, maximum: usize) usize {
            var delivered: usize = 0;
            while (delivered < maximum and self.count != 0) {
                const slot = &self.slots[self.head];
                sink.write(slot.bytes[0..slot.len]) catch {
                    self.stats.write_failures +|= 1;
                    break;
                };
                slot.len = 0;
                self.head = (self.head + 1) % self.slots.len;
                self.count -= 1;
                delivered += 1;
                self.stats.delivered +|= 1;
            }
            return delivered;
        }

        fn publish(ptr: *anyopaque, record: trace.EventRecord) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.closed) {
                self.stats.dropped_closed +|= 1;
                return;
            }
            if (self.count == self.slots.len) {
                self.stats.dropped_full +|= 1;
                return;
            }
            const tail = (self.head + self.count) % self.slots.len;
            const slot = &self.slots[tail];
            var writer = std.Io.Writer.fixed(&slot.bytes);
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
            slot.len = writer.buffered().len;
            self.count += 1;
            self.stats.enqueued +|= 1;
        }
    };
}

test "bounded live NDJSON stream applies backpressure closes and retries drains" {
    const Capture = struct {
        bytes: std.ArrayListUnmanaged(u8) = .empty,
        fail: bool = false,

        fn write(ptr: *anyopaque, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.InjectedObserverFailure;
            try self.bytes.appendSlice(std.testing.allocator, bytes);
        }
    };
    const Stream = BufferedNdjson(512);
    var slots: [2]Stream.Slot = undefined;
    var stream = try Stream.init("history-1", &slots);
    const observer = stream.observer();
    observer.publish(.{ .index = 1, .ordinal = 0, .id = 7, .name = "ready", .kind = .state_change, .actor_id = 2, .resource_id = 3, .payload_digest = 9 });
    observer.publish(.{ .index = 2, .ordinal = 0, .id = 8, .name = "running", .kind = .domain, .actor_id = null, .resource_id = null, .payload_digest = 0 });
    observer.publish(.{ .index = 3, .ordinal = 0, .id = 9, .name = "backpressured", .kind = .domain, .actor_id = null, .resource_id = null, .payload_digest = 0 });
    try std.testing.expectEqual(@as(usize, 2), stream.pending());
    try std.testing.expectEqual(@as(u64, 2), stream.stats.enqueued);
    try std.testing.expectEqual(@as(u64, 1), stream.stats.dropped_full);

    var capture: Capture = .{ .fail = true };
    defer capture.bytes.deinit(std.testing.allocator);
    const sink: WriteSink = .{ .ptr = &capture, .write_fn = Capture.write };
    try std.testing.expectEqual(@as(usize, 0), stream.drain(sink, 1));
    try std.testing.expectEqual(@as(usize, 2), stream.pending());
    try std.testing.expectEqual(@as(u64, 1), stream.stats.write_failures);
    capture.fail = false;
    try std.testing.expectEqual(@as(usize, 1), stream.drain(sink, 1));
    try std.testing.expectEqual(@as(usize, 1), stream.pending());
    try std.testing.expect(std.mem.indexOf(u8, capture.bytes.items, "\"format\":\"vopr-event-stream-v2\"") != null);

    stream.close();
    observer.publish(.{ .index = 4, .ordinal = 0, .id = 10, .name = "closed", .kind = .domain, .actor_id = null, .resource_id = null, .payload_digest = 0 });
    try std.testing.expect(stream.isClosed());
    try std.testing.expectEqual(@as(u64, 1), stream.stats.dropped_closed);
    try std.testing.expectEqual(@as(usize, 1), stream.drain(sink, std.math.maxInt(usize)));
    try std.testing.expectEqual(@as(usize, 0), stream.pending());
    try std.testing.expectEqual(@as(u64, 2), stream.stats.delivered);
}
