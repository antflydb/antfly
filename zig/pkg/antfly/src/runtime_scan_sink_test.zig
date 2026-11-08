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

const std = @import("std");
const Sink = @import("antfly_local_sources").runtime_scan_sink.ScanStreamSink;
const errors = @import("antfly_runtime_abi").error_abi;
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

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

const CountDocument = @import("antfly_local_sources").storage_db_types.BorrowedScanDocument;
extern fn scan_sink_test_count_document(*CountDocument, *errors.Status) callconv(.c) void;
test "scan sink count projection preserves optional values and named failures across archives" {
    var status: errors.Status = .ok;
    var document: CountDocument = undefined;
    scan_sink_test_count_document(&document, &status);
    try std.testing.expect((try document.countProjection(std.testing.allocator, &.{"body"})).?.bool);
    try std.testing.expect(!(try document.countProjection(std.testing.allocator, &.{})).?.bool);
    for ([_]anyerror{ error.OutOfMemory, error.CypherNumericOverflow, error.CypherValueDepthExceeded, error.Forbidden, error.DeadlineExceeded }) |failure| {
        status = errors.statusFromError(failure);
        try std.testing.expectError(failure, document.countProjection(std.testing.allocator, &.{"body"}));
    }
    // Older producers retain the optional fallback without dispatching a
    // callback index that their independently compiled archive does not own.
    document.count_projection_fn = null;
    try std.testing.expect(try document.countProjection(std.testing.allocator, &.{"body"}) == null);
}

const ScanVisitor = @import("antfly_local_sources").storage_db_types.ScanVisitor;
extern fn scan_sink_test_report_progress(*const ScanVisitor, usize) callconv(.c) errors.Status;
test "scan sink physical progress preserves budget and authority failures across archives" {
    const Counter = struct {
        examined: usize = 0,
        failure: ?anyerror,
        fn visit(_: ?*anyopaque, _: @import("antfly_local_sources").storage_db_types.ScanVisitEntry) !void {}
        fn progress(ptr: ?*anyopaque, examined: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.examined += examined;
            if (self.failure) |failure| return failure;
        }
    };
    for ([_]?anyerror{ null, error.CypherAnchorBudgetExceeded, error.CypherStateBudgetExceeded, error.Forbidden, error.DeadlineExceeded }) |failure| {
        var counter: Counter = .{ .failure = failure };
        const visitor: ScanVisitor = .{ .context = &counter, .visit = Counter.visit, .progress = Counter.progress };
        const status = scan_sink_test_report_progress(&visitor, 17);
        try std.testing.expectEqual(@as(usize, 17), counter.examined);
        if (failure) |expected| try std.testing.expectEqual(expected, errors.errorFromStatus(status)) else try std.testing.expect(status.isOk());
    }
}
