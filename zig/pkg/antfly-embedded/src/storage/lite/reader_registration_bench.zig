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

//! Standalone comparison of individual and admission-scoped epoch registration.
//! From zig/: zig build-exe --dep antfly_runtime_fs
//! -Mroot=pkg/antfly-embedded/src/storage/lite/reader_registration_bench.zig
//! --dep antfly_platform -Mantfly_runtime_fs=lib/runtime/src/fs.zig
//! -Mantfly_platform=lib/platform/src/root.zig -lc
//! --cache-dir .zig-cache --global-cache-dir .zig-cache/global
//! -femit-bin=zig-out/reader-registration-bench
//! Run zig-out/reader-registration-bench. Timings are observations, not assertions.
const std = @import("std");
const Coordinator = @import("reader_coordinator.zig").Coordinator;
const epochs = @import("reader_epochs.zig");

pub fn main(init: std.process.Init) !void {
    // Match C API admission rather than measuring DebugAllocator bookkeeping.
    const a = std.heap.c_allocator;
    const io = init.io;
    var nonce: [8]u8 = undefined;
    try io.randomSecure(&nonce);
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/reader-registration-{x}", .{nonce});
    defer a.free(directory);
    try std.Io.Dir.cwd().createDirPath(io, directory);
    defer std.Io.Dir.cwd().deleteTree(io, directory) catch {};
    const path = try std.fs.path.join(a, &.{ directory, "registration.aflite" });
    defer a.free(path);
    const data = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true });
    defer data.close(io);
    defer std.Io.Dir.cwd().deleteFile(io, path) catch {};
    const binding = try epochs.namespaceForPublication(a, io, path);
    const inode = (try data.stat(io)).inode;
    var owner = Coordinator.init(a, io);
    defer owner.deinit();
    const iterations = 2000;
    for (0..4) |round| {
        const order: [2]bool = if (round % 2 == 0) .{ false, true } else .{ true, false };
        for (order) |batched| {
            const began = std.Io.Clock.awake.now(io).nanoseconds;
            const batch: ?Coordinator.RegistrationBatch = if (batched) owner.beginRegistrationBatch() else null;
            for (0..iterations) |_| {
                const file = try std.Io.Dir.cwd().openFile(io, path, .{});
                defer file.close(io);
                try file.lock(io, .shared);
                const fence = try owner.registerEpoch(file, path, binding, inode, 1, @splat(0), false);
                owner.unregister(fence);
            }
            if (batch) |scope| scope.end();
            const elapsed = std.Io.Clock.awake.now(io).nanoseconds - began;
            const frontier = try epochs.scan(a, io, path, binding, inode);
            if (frontier.data != null or frontier.metadata != null) return error.LeakedReader;
            std.debug.print("{{\"round\":{d},\"batched\":{},\"iterations\":{d},\"nanoseconds\":{d}}}\n", .{ round, batched, iterations, elapsed });
        }
    }
}
