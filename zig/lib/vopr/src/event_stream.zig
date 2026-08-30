// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Diagnostic-only live publication of canonical VOPR events.
//!
//! The built-in stream serializes events into a caller-owned bounded SPSC
//! queue at the runner boundary. Publication never calls external code,
//! allocates, or blocks. One diagnostic consumer may concurrently drain
//! complete NDJSON records; release/acquire publication prevents it from
//! observing partial records. Sink failures retain the oldest queued record
//! for a later retry. Queue overflow uses an explicit drop-newest policy and
//! cannot perturb choices, time, properties, replay artifacts, or the outcome
//! of a history.

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

const AtomicStats = struct {
    enqueued: std.atomic.Value(u64) = .init(0),
    delivered: std.atomic.Value(u64) = .init(0),
    dropped_full: std.atomic.Value(u64) = .init(0),
    dropped_oversize: std.atomic.Value(u64) = .init(0),
    dropped_closed: std.atomic.Value(u64) = .init(0),
    write_failures: std.atomic.Value(u64) = .init(0),

    fn snapshot(self: *const AtomicStats) Stats {
        return .{
            .enqueued = self.enqueued.load(.monotonic),
            .delivered = self.delivered.load(.monotonic),
            .dropped_full = self.dropped_full.load(.monotonic),
            .dropped_oversize = self.dropped_oversize.load(.monotonic),
            .dropped_closed = self.dropped_closed.load(.monotonic),
            .write_failures = self.write_failures.load(.monotonic),
        };
    }
};

fn incrementSaturating(counter: *std.atomic.Value(u64)) void {
    var current = counter.load(.monotonic);
    while (current != std.math.maxInt(u64)) {
        if (counter.cmpxchgWeak(current, current + 1, .monotonic, .monotonic)) |observed| {
            current = observed;
            continue;
        }
        return;
    }
}

/// A caller-owned bounded single-producer/single-consumer stream.
/// `max_line_bytes` includes the trailing newline. Queue capacity is the
/// power-of-two number of slots supplied to `init`; that bound makes wrapping
/// positions safe even after integer rollover. `publish` may race with one
/// `drain` caller; multiple concurrent publishers or consumers are unsupported.
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
        /// Monotonic wrapping positions. The producer owns `tail`; the
        /// consumer owns `head`. Their difference never exceeds capacity.
        head: std.atomic.Value(u64) = .init(0),
        tail: std.atomic.Value(u64) = .init(0),
        closed: std.atomic.Value(bool) = .init(false),
        counters: AtomicStats = .{},

        pub fn init(run_id: []const u8, slots: []Slot) !Self {
            if (run_id.len == 0) return error.EmptyEventStreamRunId;
            if (slots.len == 0) return error.EmptyEventStreamQueue;
            if (!std.math.isPowerOfTwo(slots.len)) return error.EventStreamQueueMustBePowerOfTwo;
            for (slots) |*slot| slot.len = 0;
            return .{ .run_id = run_id, .slots = slots };
        }

        pub fn observer(self: *Self) Observer {
            return .{ .ptr = self, .publish_fn = publish };
        }

        pub fn close(self: *Self) void {
            self.closed.store(true, .release);
        }

        pub fn pending(self: *const Self) usize {
            const head = self.head.load(.acquire);
            const tail = self.tail.load(.acquire);
            return @intCast(tail -% head);
        }

        pub fn isClosed(self: *const Self) bool {
            return self.closed.load(.acquire);
        }

        /// Best-effort atomic telemetry. A snapshot taken while publication or
        /// draining is active is safe but not a transactional cut across all
        /// counters.
        pub fn stats(self: *const Self) Stats {
            return self.counters.snapshot();
        }

        /// Drains at most `maximum` complete records. A sink failure is
        /// diagnostic: the oldest record remains queued and draining stops so
        /// a caller may repair or replace the sink and retry without loss.
        pub fn drain(self: *Self, sink: WriteSink, maximum: usize) usize {
            var delivered: usize = 0;
            var head = self.head.load(.monotonic);
            while (delivered < maximum) {
                const tail = self.tail.load(.acquire);
                if (head == tail) break;
                const slot = &self.slots[@intCast(head & @as(u64, @intCast(self.slots.len - 1)))];
                sink.write(slot.bytes[0..slot.len]) catch {
                    incrementSaturating(&self.counters.write_failures);
                    break;
                };
                slot.len = 0;
                head +%= 1;
                self.head.store(head, .release);
                delivered += 1;
                incrementSaturating(&self.counters.delivered);
            }
            return delivered;
        }

        fn publish(ptr: *anyopaque, record: trace.EventRecord) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.closed.load(.acquire)) {
                incrementSaturating(&self.counters.dropped_closed);
                return;
            }
            const tail = self.tail.load(.monotonic);
            const head = self.head.load(.acquire);
            if (tail -% head == self.slots.len) {
                incrementSaturating(&self.counters.dropped_full);
                return;
            }
            const slot = &self.slots[@intCast(tail & @as(u64, @intCast(self.slots.len - 1)))];
            var writer = std.Io.Writer.fixed(&slot.bytes);
            std.json.Stringify.value(.{
                .format = format,
                .run_id = self.run_id,
                .event = record,
            }, .{}, &writer) catch {
                incrementSaturating(&self.counters.dropped_oversize);
                return;
            };
            writer.writeByte('\n') catch {
                incrementSaturating(&self.counters.dropped_oversize);
                return;
            };
            slot.len = writer.buffered().len;
            self.tail.store(tail +% 1, .release);
            incrementSaturating(&self.counters.enqueued);
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
    var stats = stream.stats();
    try std.testing.expectEqual(@as(u64, 2), stats.enqueued);
    try std.testing.expectEqual(@as(u64, 1), stats.dropped_full);

    var capture: Capture = .{ .fail = true };
    defer capture.bytes.deinit(std.testing.allocator);
    const sink: WriteSink = .{ .ptr = &capture, .write_fn = Capture.write };
    try std.testing.expectEqual(@as(usize, 0), stream.drain(sink, 1));
    try std.testing.expectEqual(@as(usize, 2), stream.pending());
    stats = stream.stats();
    try std.testing.expectEqual(@as(u64, 1), stats.write_failures);
    capture.fail = false;
    try std.testing.expectEqual(@as(usize, 1), stream.drain(sink, 1));
    try std.testing.expectEqual(@as(usize, 1), stream.pending());
    try std.testing.expect(std.mem.indexOf(u8, capture.bytes.items, "\"format\":\"vopr-event-stream-v2\"") != null);

    stream.close();
    observer.publish(.{ .index = 4, .ordinal = 0, .id = 10, .name = "closed", .kind = .domain, .actor_id = null, .resource_id = null, .payload_digest = 0 });
    try std.testing.expect(stream.isClosed());
    stats = stream.stats();
    try std.testing.expectEqual(@as(u64, 1), stats.dropped_closed);
    try std.testing.expectEqual(@as(usize, 1), stream.drain(sink, std.math.maxInt(usize)));
    try std.testing.expectEqual(@as(usize, 0), stream.pending());
    stats = stream.stats();
    try std.testing.expectEqual(@as(u64, 2), stats.delivered);
}

test "bounded live NDJSON stream rejects oversize records without publication" {
    const Stream = BufferedNdjson(128);
    var slots: [1]Stream.Slot = undefined;
    var stream = try Stream.init("history-with-an-intentionally-long-run-identifier", &slots);
    stream.observer().publish(.{
        .index = 1,
        .ordinal = 0,
        .id = 7,
        .name = "an-intentionally-long-event-name-that-cannot-fit-in-the-minimum-stream-slot",
        .kind = .state_change,
        .actor_id = 2,
        .resource_id = 3,
        .payload_digest = 9,
    });
    const stats = stream.stats();
    try std.testing.expectEqual(@as(usize, 0), stream.pending());
    try std.testing.expectEqual(@as(u64, 0), stats.enqueued);
    try std.testing.expectEqual(@as(u64, 1), stats.dropped_oversize);
}

test "bounded live NDJSON stream rejects ambiguous ring capacities" {
    const Stream = BufferedNdjson(128);
    var slots: [3]Stream.Slot = undefined;
    try std.testing.expectError(error.EventStreamQueueMustBePowerOfTwo, Stream.init("history", &slots));
}

test "bounded live NDJSON stream supports a concurrent producer and consumer" {
    const Stream = BufferedNdjson(512);
    const attempts = 20_000;
    var slots: [32]Stream.Slot = undefined;
    var stream = try Stream.init("concurrent-history", &slots);

    const Producer = struct {
        fn run(target: *Stream) void {
            const observer = target.observer();
            for (0..attempts) |index| observer.publish(.{
                .index = index,
                .ordinal = 0,
                .id = 7,
                .name = "concurrent-event",
                .kind = .domain,
                .actor_id = 2,
                .resource_id = 3,
                .payload_digest = index,
            });
            target.close();
        }
    };
    const Capture = struct {
        records: usize = 0,

        fn write(ptr: *anyopaque, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(bytes.len > 1 and bytes[bytes.len - 1] == '\n');
            try std.testing.expect(std.mem.indexOf(u8, bytes, "\"format\":\"vopr-event-stream-v2\"") != null);
            self.records += 1;
        }
    };

    var capture: Capture = .{};
    const sink: WriteSink = .{ .ptr = &capture, .write_fn = Capture.write };
    const producer = try std.Thread.spawn(.{}, Producer.run, .{&stream});
    while (!stream.isClosed() or stream.pending() != 0) {
        _ = stream.drain(sink, 64);
        std.Thread.yield() catch {};
    }
    producer.join();
    _ = stream.drain(sink, std.math.maxInt(usize));

    const stats = stream.stats();
    try std.testing.expectEqual(@as(u64, attempts), stats.enqueued + stats.dropped_full);
    try std.testing.expectEqual(stats.enqueued, stats.delivered);
    try std.testing.expectEqual(@as(u64, 0), stats.dropped_oversize);
    try std.testing.expectEqual(@as(u64, 0), stats.dropped_closed);
    try std.testing.expectEqual(@as(u64, 0), stats.write_failures);
    try std.testing.expectEqual(@as(usize, @intCast(stats.delivered)), capture.records);
    try std.testing.expectEqual(@as(usize, 0), stream.pending());
}
