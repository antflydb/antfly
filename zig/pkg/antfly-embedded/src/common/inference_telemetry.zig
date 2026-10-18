// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Per-attempt telemetry retains identity and actual provider usage only.
//! Prompts, tool arguments, media, credentials and endpoint URLs are excluded.
const std = @import("std");
const abi = @import("antfly_runtime_abi").http_abi;
const time = @import("antfly_platform").time;
const log = std.log.scoped(.inference_gateway);

pub const Observation = struct {
    allocator: std.mem.Allocator,
    request_id: []const u8,
    requested: []const u8,
    resolved: []const u8,
    backend_model: ?[]const u8 = null,
    executor: ?[]const u8 = null,
    connection: ?[]const u8 = null,
    task: []const u8,
    attempt: u8,
    started_ns: u64,
    first_output_ns: ?u64 = null,
    status: ?u16 = null,
    failure: ?[]const u8 = null,
    input_tokens: ?u64 = null,
    output_tokens: ?u64 = null,
    total_tokens: ?u64 = null,
    downstream: abi.StreamSink = .{},
    pending: std.ArrayList(u8) = .empty,
    dropping_event: bool = false,

    pub fn finish(self: *Observation) void {
        self.pending.deinit(self.allocator);
        log.info("invocation {f}", .{std.json.fmt(.{
            .request_id = self.request_id,
            .requested_alias = self.requested,
            .resolved_target = self.resolved,
            .backend_model = self.backend_model,
            .executor = self.executor,
            .connection = self.connection,
            .task = self.task,
            .attempt = self.attempt,
            .latency_ns = time.monotonicNs() -| self.started_ns,
            .time_to_first_output_ns = if (self.first_output_ns) |first| first -| self.started_ns else null,
            .status = self.status,
            .failure = self.failure,
            .input_tokens = self.input_tokens,
            .output_tokens = self.output_tokens,
            .total_tokens = self.total_tokens,
        }, .{})});
    }
    pub fn usage(self: *Observation, bytes: []const u8) void {
        if (bytes.len > 65536) return;
        const parsed = std.json.parseFromSlice(std.json.Value, self.allocator, bytes, .{}) catch return;
        defer parsed.deinit();
        if (parsed.value != .object) return;
        const value = parsed.value.object.get("usage") orelse return;
        if (value != .object) return;
        self.input_tokens = count(value, "input_tokens") orelse count(value, "prompt_tokens") orelse self.input_tokens;
        self.output_tokens = count(value, "output_tokens") orelse count(value, "completion_tokens") orelse self.output_tokens;
        self.total_tokens = count(value, "total_tokens") orelse self.total_tokens;
    }
    fn count(value: std.json.Value, key: []const u8) ?u64 {
        const number = value.object.get(key) orelse return null;
        return if (number == .integer and number.integer >= 0) @intCast(number.integer) else null;
    }
    pub fn stream(self: *Observation, downstream: abi.StreamSink) abi.StreamSink {
        self.downstream = downstream;
        return .{ .context = self, .start = start, .write = write, .close = close };
    }
    fn start(raw: ?*anyopaque, status: u16, content_type: abi.Bytes, headers: abi.HeaderList) callconv(.c) abi.CallbackStatus {
        const self: *Observation = @ptrCast(@alignCast(raw.?));
        self.status = status;
        return (self.downstream.start orelse return .failed)(self.downstream.context, status, content_type, headers);
    }
    fn write(raw: ?*anyopaque, bytes: abi.Bytes) callconv(.c) abi.CallbackStatus {
        const self: *Observation = @ptrCast(@alignCast(raw.?));
        if (bytes.len > 0 and self.first_output_ns == null) self.first_output_ns = time.monotonicNs();
        // Parse complete SSE data lines, bounded independently of the payload.
        // An oversized event is ignored for telemetry and still forwarded.
        for (bytes.slice()) |byte| {
            if (byte == '\n') {
                if (!self.dropping_event and std.mem.startsWith(u8, self.pending.items, "data:")) self.usage(std.mem.trim(u8, self.pending.items[5..], " \r"));
                self.pending.clearRetainingCapacity();
                self.dropping_event = false;
            } else if (!self.dropping_event) {
                if (self.pending.items.len >= 65536) {
                    self.pending.clearRetainingCapacity();
                    self.dropping_event = true;
                } else self.pending.append(self.allocator, byte) catch {
                    self.dropping_event = true;
                };
            }
        }
        return (self.downstream.write orelse return .failed)(self.downstream.context, bytes);
    }
    fn close(raw: ?*anyopaque) callconv(.c) abi.CallbackStatus {
        const self: *Observation = @ptrCast(@alignCast(raw.?));
        return (self.downstream.close orelse return .failed)(self.downstream.context);
    }
};

test "provider registry gateway telemetry parses fragmented actual usage without estimating" {
    const a = std.testing.allocator;
    var observation = Observation{
        .allocator = a,
        .request_id = "request",
        .requested = "chains/example",
        .resolved = "generators/example",
        .task = "generate",
        .attempt = 1,
        .started_ns = time.monotonicNs(),
    };
    defer observation.pending.deinit(a);
    const Discard = struct {
        fn write(_: ?*anyopaque, _: abi.Bytes) callconv(.c) abi.CallbackStatus {
            return .ok;
        }
    };
    const sink = observation.stream(.{ .write = Discard.write });
    try std.testing.expectEqual(abi.CallbackStatus.ok, sink.write.?(sink.context, .init("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")));
    try std.testing.expect(observation.first_output_ns != null);
    try std.testing.expectEqual(@as(?u64, null), observation.output_tokens);
    _ = sink.write.?(sink.context, .init("data: {\"usage\":{\"prompt_tokens\":2,"));
    try std.testing.expectEqual(@as(?u64, null), observation.input_tokens);
    _ = sink.write.?(sink.context, .init("\"completion_tokens\":1,\"total_tokens\":3}}\n\n"));
    try std.testing.expectEqual(@as(?u64, 2), observation.input_tokens);
    try std.testing.expectEqual(@as(?u64, 1), observation.output_tokens);
    try std.testing.expectEqual(@as(?u64, 3), observation.total_tokens);
    observation.usage("{\"usage\":{\"input_tokens\":-1},\"text\":\"long text cannot invent usage\"}");
    try std.testing.expectEqual(@as(?u64, 2), observation.input_tokens);
}
