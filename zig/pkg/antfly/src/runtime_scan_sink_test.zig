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
const Sink = @import("runtime_scan_sink.zig").ScanStreamSink;
const errors = @import("runtime_error_abi.zig");
extern fn scan_sink_test_consume(*const Sink, [*]const u8, usize) callconv(.c) errors.Status;
extern fn scan_sink_test_provider_sink(*Sink, *errors.Status) callconv(.c) void;

const Consumer = struct {
    failure: ?anyerror = null,
    fail_start: bool = false,
    starts: usize = 0,
    writes: usize = 0,
    expected: []const u8 = "borrowed bytes",

    fn sink(self: *@This()) Sink {
        return .{ .context = self, .start_fn = start, .write_fn = write };
    }
    fn start(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.starts += 1;
        if (self.fail_start) if (self.failure) |err| return err;
    }
    fn write(raw: ?*anyopaque, bytes: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.writes += 1;
        try std.testing.expectEqual(self.expected.ptr, bytes.ptr);
        try std.testing.expectEqualStrings(self.expected, bytes);
        if (self.failure) |err| return err;
    }
};

test "scan sink archive boundary preserves errors in both directions" {
    const failures = [_]anyerror{ error.Canceled, error.Timeout, error.OutOfMemory, error.ConnectionResetByPeer, error.InvalidArgument };
    for (failures) |failure| {
        for ([_]bool{ false, true }) |fail_start| {
            var consumer = Consumer{ .failure = failure, .fail_start = fail_start };
            const sink = consumer.sink();
            const result = scan_sink_test_consume(&sink, consumer.expected.ptr, consumer.expected.len);
            try std.testing.expect(!result.isOk());
            try std.testing.expectEqual(failure, errors.errorFromStatus(result));
            try std.testing.expectEqual(@as(usize, 1), consumer.starts);
            try std.testing.expectEqual(@as(usize, if (fail_start) 0 else 1), consumer.writes);
        }
        var status = errors.statusFromError(failure);
        var provider_sink: Sink = undefined;
        scan_sink_test_provider_sink(&provider_sink, &status);
        try provider_sink.start();
        try provider_sink.write("");
        try std.testing.expectError(failure, provider_sink.write("data"));
    }
}

test "scan sink archive boundary borrows bytes and skips empty writes" {
    var consumer = Consumer{};
    const sink = consumer.sink();
    try std.testing.expect(scan_sink_test_consume(&sink, consumer.expected.ptr, consumer.expected.len).isOk());
    try std.testing.expectEqual(@as(usize, 1), consumer.starts);
    try std.testing.expectEqual(@as(usize, 1), consumer.writes);
}

test "scan sink local calls retain private consumer errors" {
    var consumer = Consumer{ .failure = error.ConsumerStopped };
    const sink = consumer.sink();
    try sink.start();
    try std.testing.expectError(error.ConsumerStopped, sink.write(consumer.expected));
}
