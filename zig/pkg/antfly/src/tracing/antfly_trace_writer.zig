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

/// Event emitted by instrumented Antfly transaction code for TLA+ trace validation.
pub const AntflyTracingEvent = struct {
    name: []const u8,
    txn_id: [16]u8,
    shard_id: []const u8, // "" for coordinator-level events
    /// Key-value state snapshot. Caller must ensure slices live until writeEvent returns.
    write_keys: []const []const u8 = &.{},
    delete_keys: []const []const u8 = &.{},
    predicate_keys: []const []const u8 = &.{},
    timestamp: ?u64 = null,
    reason: ?[]const u8 = null,
};

/// Vtable interface for receiving Antfly trace events.
pub const AntflyTraceWriter = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        trace_event: *const fn (ptr: *anyopaque, event: *const AntflyTracingEvent) void,
    };

    pub fn traceEvent(self: AntflyTraceWriter, event: *const AntflyTracingEvent) void {
        self.vtable.trace_event(self.ptr, event);
    }
};

/// Writes Antfly trace events as ndjson compatible with TraceAntflyTransaction.tla.
/// Each line: {"tag":"antfly-trace","event":{"name":"...","txnId":"...","shardId":"...","state":{...}}}
pub const AntflyNdjsonTraceWriter = struct {
    mutex: std.atomic.Mutex = .unlocked,
    writer: *std.Io.Writer,

    pub fn traceWriter(self: *AntflyNdjsonTraceWriter) AntflyTraceWriter {
        return .{
            .ptr = self,
            .vtable = &.{
                .trace_event = traceEvent,
            },
        };
    }

    fn traceEvent(ptr: *anyopaque, event: *const AntflyTracingEvent) void {
        const self: *AntflyNdjsonTraceWriter = @ptrCast(@alignCast(ptr));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        self.writeEvent(event) catch {};
        self.writer.flush() catch {};
    }

    fn writeEvent(self: *AntflyNdjsonTraceWriter, event: *const AntflyTracingEvent) !void {
        const w = self.writer;

        try w.writeAll("{\"tag\":\"antfly-trace\",\"event\":{");

        // name
        try w.print("\"name\":\"{s}\"", .{event.name});

        // txnId (hex)
        try w.writeAll(",\"txnId\":\"");
        try writeHex(w, &event.txn_id);
        try w.writeAll("\"");

        // shardId (always present, may be empty)
        try w.print(",\"shardId\":\"{s}\"", .{event.shard_id});

        // state object — always emit for write-intent events so TLA+ spec
        // can access fields unconditionally; for other events, only when non-empty
        const is_write_intent = std.mem.eql(u8, event.name, "WriteIntentOnShard") or
            std.mem.eql(u8, event.name, "WriteIntentFails");
        const has_state = is_write_intent or
            event.timestamp != null or
            event.reason != null;

        if (has_state) {
            try w.writeAll(",\"state\":{");
            var first = true;

            if (is_write_intent or event.write_keys.len > 0) {
                try w.writeAll("\"writeKeys\":");
                try writeStringArray(w, event.write_keys);
                first = false;
            }
            if (is_write_intent or event.delete_keys.len > 0) {
                if (!first) try w.writeAll(",");
                try w.writeAll("\"deleteKeys\":");
                try writeStringArray(w, event.delete_keys);
                first = false;
            }
            if (is_write_intent or event.predicate_keys.len > 0) {
                if (!first) try w.writeAll(",");
                try w.writeAll("\"predicateKeys\":");
                try writeStringArray(w, event.predicate_keys);
                first = false;
            }
            if (event.timestamp) |ts| {
                if (!first) try w.writeAll(",");
                try w.print("\"timestamp\":{d}", .{ts});
                first = false;
            }
            if (event.reason) |reason| {
                if (!first) try w.writeAll(",");
                try w.print("\"reason\":\"{s}\"", .{reason});
            }

            try w.writeAll("}");
        }

        try w.writeAll("}}\n");
    }
};

fn writeHex(w: *std.Io.Writer, bytes: []const u8) !void {
    for (bytes) |b| {
        try w.print("{x:0>2}", .{b});
    }
}

fn writeStringArray(w: *std.Io.Writer, items: []const []const u8) !void {
    try w.writeAll("[");
    for (items, 0..) |item, i| {
        if (i > 0) try w.writeAll(",");
        try w.writeAll("\"");
        // Escape JSON special characters
        for (item) |c| {
            switch (c) {
                '"' => try w.writeAll("\\\""),
                '\\' => try w.writeAll("\\\\"),
                '\n' => try w.writeAll("\\n"),
                else => try w.writeByte(c),
            }
        }
        try w.writeAll("\"");
    }
    try w.writeAll("]");
}

test "antfly trace writer emits valid ndjson" {
    var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer out.deinit();

    var trace_writer = AntflyNdjsonTraceWriter{
        .writer = &out.writer,
    };

    const keys = [_][]const u8{ "key1", "key2" };
    const event = AntflyTracingEvent{
        .name = "WriteIntentOnShard",
        .txn_id = [_]u8{0x55} ** 16,
        .shard_id = "42",
        .write_keys = keys[0..],
        .timestamp = 100,
    };

    trace_writer.traceWriter().traceEvent(&event);

    const output = out.written();
    try std.testing.expect(std.mem.indexOf(u8, output, "\"tag\":\"antfly-trace\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"name\":\"WriteIntentOnShard\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"shardId\":\"42\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"writeKeys\":[\"key1\",\"key2\"]") != null);
}
