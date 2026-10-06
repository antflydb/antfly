// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Standalone reproduction of the partitioned Linux executable's static TLS.
//! Run from the repository root with the real MemoryAF component and Wasmtime
//! library; see zig/FLAKES.md. A small unit executable otherwise misses this
//! failure because its static TLS fits beside Rayon's default compiler stack.
const std = @import("std");
const runtime = @import("wasmtime");

threadlocal var partition_tls: [2 * 1024 * 1024]u8 = undefined;

fn write(_: ?*anyopaque, alloc: std.mem.Allocator, table: []const u8, body: []const u8) ![]u8 {
    try std.testing.expectEqualStrings("memory_record", table);
    try std.testing.expect(std.mem.indexOf(u8, body, "stack regression") != null);
    return alloc.dupe(u8, "{}");
}

fn invoke() !void {
    // Keep the entire TLS symbol observable even in optimized builds.
    @as(*volatile u8, &partition_tls[0]).* = 1;
    std.mem.doNotOptimizeAway(&partition_tls);
    const alloc = std.testing.allocator;
    const result = try runtime.invokeExtensionWithOptions(alloc, .{
        .package_name = "memoryaf",
        .package_version = "0.0.1",
        .runtime_name = "memoryaf_wasm",
        .artifact = "target/wasm32-wasip2/release/memoryaf_extension.wasm",
    }, "store_memory", "{\"content\":\"stack regression\",\"project\":\"antfly\",\"visibility\":\"team\"}", .{
        .io = std.testing.io,
        .package_store_root = "extensions",
        .host_imports = .{ .db_write = write },
    });
    defer alloc.free(result);
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("ok").?.bool);
    try std.testing.expectEqualStrings("stored", parsed.value.object.get("status").?.string);
}

const Invocation = struct {
    failure: ?anyerror = null,

    fn run(self: *@This()) void {
        invoke() catch |err| {
            self.failure = err;
        };
    }
};

test "real component compiles with partitioned process TLS on a host worker" {
    // Use a host-sized worker; Wasmtime must not escape it onto a small-stack
    // global compiler pool. Join also exercises the invocation's TLS teardown.
    var invocation: Invocation = .{};
    const thread = try std.Thread.spawn(.{ .stack_size = 8 * 1024 * 1024 }, Invocation.run, .{&invocation});
    thread.join();
    if (invocation.failure) |err| return err;
}
