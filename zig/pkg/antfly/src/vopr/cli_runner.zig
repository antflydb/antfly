// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const builtin = @import("builtin");
const std = @import("std");

var process_init: std.process.Init.Minimal = undefined;

export fn antflyVoprProcessInit() *const anyopaque {
    return &process_init;
}

/// The Antfly scenarios intentionally use test-only facilities such as
/// `std.testing.tmpDir` and the leak-checking allocator. Compile the VOPR shell
/// as a test artifact, but dispatch its command line directly instead of
/// enumerating unit tests.
pub fn main(init: std.process.Init.Minimal) void {
    @disableInstrumentation();

    std.testing.allocator_instance = .{};
    defer if (std.testing.allocator_instance.deinit() == .leak) {
        std.debug.print("VOPR command leaked memory\n", .{});
        std.process.exit(1);
    };
    std.testing.io_instance = .init(std.testing.allocator, .{
        .argv0 = .init(init.args),
        .environ = init.environ,
    });
    defer std.testing.io_instance.deinit();
    std.testing.environ = init.environ;
    std.testing.log_level = .warn;

    process_init = init;
    for (builtin.test_functions) |test_fn| {
        if (!std.mem.endsWith(u8, test_fn.name, "VOPR command entrypoint")) continue;
        test_fn.func() catch |err| {
            if (@errorReturnTrace()) |trace| std.debug.dumpErrorReturnTrace(trace);
            std.debug.print("VOPR command failed: {t}\n", .{err});
            std.process.exit(1);
        };
        return;
    }
    std.debug.print("VOPR command entrypoint was not linked\n", .{});
    std.process.exit(1);
}
