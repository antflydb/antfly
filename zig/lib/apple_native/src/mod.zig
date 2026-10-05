// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");
const options = @import("apple_native_options");
const platform = @import("antfly_platform");
const httpx = @import("httpx");
pub const time = platform.time;
pub const CancellationToken = httpx.CancellationToken;

pub const enabled = builtin.os.tag == .macos and options.enabled;
pub const workspace_bytes: usize = 512 * 1024 * 1024;
pub const Control = struct {
    timeout_ms: ?u64 = null,
    cancellation: ?httpx.CancellationToken = null,
};
pub const Operation = enum(c_int) { generate = 1, transcribe = 2, availability = 3 };
pub fn checkAvailable() !void {
    if (!enabled) return error.AppleIntelligenceProviderUnavailable;
}

extern fn antfly_apple_invoke(
    operation: c_int,
    json: [*]const u8,
    json_len: usize,
    audio: ?[*]const u8,
    audio_len: usize,
    response_limit: usize,
    context: *anyopaque,
    cancel: *const fn (*anyopaque) callconv(.c) c_int,
    output: *const fn (*anyopaque, [*]const u8, usize) callconv(.c) c_int,
) c_int;

const Invocation = struct {
    alloc: std.mem.Allocator,
    control: Control,
    deadline: ?u64,
    limit: usize,
    result: ?[]u8 = null,
    failure: ?anyerror = null,

    fn check(self: *const Invocation) !void {
        if (self.control.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        if (self.deadline) |deadline| if (platform.time.monotonicNs() >= deadline) return error.Timeout;
    }
    fn cancel(ptr: *anyopaque) callconv(.c) c_int {
        const self: *Invocation = @ptrCast(@alignCast(ptr));
        self.check() catch return 1;
        return 0;
    }
    fn output(ptr: *anyopaque, bytes: [*]const u8, len: usize) callconv(.c) c_int {
        const self: *Invocation = @ptrCast(@alignCast(ptr));
        if (self.result != null or len > self.limit) {
            self.failure = error.ResponseTooLarge;
            return 1;
        }
        self.result = self.alloc.dupe(u8, bytes[0..len]) catch |err| {
            self.failure = err;
            return 1;
        };
        return 0;
    }
};

/// All buffers and callbacks remain borrowed until the native task has ended,
/// including after cancellation. No callback may outlive this stack frame.
pub fn invoke(alloc: std.mem.Allocator, operation: Operation, json: []const u8, audio: []const u8, limit: usize, control: Control) ![]u8 {
    if (!enabled) return error.AppleIntelligenceProviderUnavailable;
    if (json.len > 1024 * 1024 or audio.len > 128 * 1024 * 1024 or limit == 0)
        return error.AppleNativeInputTooLarge;
    var invocation = Invocation{
        .alloc = alloc,
        .control = control,
        .deadline = if (control.timeout_ms) |ms| if (ms > 0) platform.time.monotonicNs() +| (ms *| std.time.ns_per_ms) else null else null,
        .limit = limit,
    };
    errdefer if (invocation.result) |value| alloc.free(value);
    try invocation.check();
    const status = antfly_apple_invoke(@backingInt(operation), json.ptr, json.len, if (audio.len > 0) audio.ptr else null, audio.len, limit, &invocation, Invocation.cancel, Invocation.output);
    try invocation.check();
    if (invocation.failure) |err| return err;
    switch (status) {
        0 => {},
        1 => return error.InvalidAppleNativeRequest,
        2 => return error.AppleIntelligenceProviderUnavailable,
        3 => return error.AppleIntelligenceDisabled,
        4 => return error.AppleModelNotReady,
        5 => return error.UnsupportedAppleSpeechLocale,
        6 => return error.AppleSpeechAssetsMissing,
        7 => return error.Cancelled,
        8 => return error.AppleNativeBusy,
        9 => return error.ResponseTooLarge,
        10 => return error.AppleContextWindowExceeded,
        11 => return error.AppleGenerationRefused,
        else => return error.AppleNativeFailed,
    }
    return invocation.result orelse error.AppleNativeFailed;
}

test "Apple native disabled builds do not call Swift symbols" {
    if (enabled) return error.SkipZigTest;
    try std.testing.expectError(error.AppleIntelligenceProviderUnavailable, invoke(std.testing.allocator, .availability, "{}", &.{}, 1024, .{}));
}
