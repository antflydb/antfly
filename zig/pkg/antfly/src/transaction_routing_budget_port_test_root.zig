// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

pub const antfly_sources = struct {};
test {
    @import("std").testing.refAllDecls(@import("antfly_local_sources").read_admission);
    @import("std").testing.refAllDecls(@import("storage/transaction_routing_budget_port.zig"));
    @import("std").testing.refAllDecls(@import("storage/transaction_routing_memory_port.zig"));
    @import("std").testing.refAllDecls(@import("antfly_local_sources").common_bounded_mutex);
}

test "transaction routing control port cursor admission never calls an unsupported legacy cursor" {
    const std = @import("std");
    const storage = @import("antfly_local_sources").storage_backend_erased;
    const admission = @import("antfly_local_sources").read_admission;
    const State = struct {
        legacy_calls: usize = 0,
        canceled: bool = false,
        fn close(_: std.mem.Allocator, _: *anyopaque) void {}
        fn row(ptr: *anyopaque) !?storage.Entry {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.legacy_calls += 1;
            return null;
        }
        fn seek(ptr: *anyopaque, _: []const u8) !?storage.Entry {
            return row(ptr);
        }
        pub fn checkpoint(self: *@This()) !void {
            if (self.canceled) return error.GenerationPublicationChanged;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedResult;
        }
    };
    var state = State{};
    var cursor = storage.Cursor{ .allocator = std.testing.allocator, .ptr = &state, .vtable = &.{
        .close = State.close,
        .first = State.row,
        .last = State.row,
        .next = State.row,
        .prev = State.row,
        .seek_at_or_after = State.seek,
        .seek_at_or_before = State.seek,
    } };
    var adapter = admission.Adapter(*State){ .budget = &state };
    const payload = adapter.payload();
    const control = admission.Port.init(&payload);
    try std.testing.expectError(error.ExecutionWaitUnsupported, cursor.nextWithAdmission(control));
    try std.testing.expectError(error.ExecutionWaitUnsupported, cursor.seekAtOrAfterWithAdmission("doc", control));
    state.canceled = true;
    try std.testing.expectError(error.GenerationPublicationChanged, cursor.nextWithAdmission(control));
    try std.testing.expectError(error.GenerationPublicationChanged, cursor.seekAtOrAfterWithAdmission("doc", control));
    try std.testing.expectEqual(@as(usize, 0), state.legacy_calls);
    cursor.close();
}
