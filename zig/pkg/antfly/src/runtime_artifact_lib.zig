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

//! One independently code-generated server runtime, linked into the final
//! Antfly executable through a narrow C ABI entry point.

const builtin = @import("builtin");
const std = @import("std");
const platform = @import("antfly_platform");
const bridge = @import("runtime_bridge.zig");
const role_options = @import("runtime_artifact_options");

const runtime = switch (role_options.role) {
    .data => @import("data/runtime.zig"),
    .inference => @import("inference_runtime/runtime.zig"),
    .metadata => @import("metadata/runtime.zig"),
    .standalone => @import("standalone/runtime.zig"),
};

// The user-manager storage adapter deliberately imports these through the
// compilation root so it shares their exact Zig type identity.
pub const lsm_backend = @import("storage/lsm_backend/mod.zig");
pub const storage_backend_erased = @import("storage/backend_erased.zig");

fn runtimeEntry(context: *const bridge.Context) callconv(.c) c_int {
    const init: *const std.process.Init = @ptrCast(@alignCast(context.init));
    const args: *std.process.Args.Iterator = @ptrCast(@alignCast(context.args));
    const argv0 = "antfly " ++ @tagName(role_options.role);

    runtime.runFromIterator(runtimeInit(init.*), argv0, args) catch |err| {
        const message = switch (err) {
            error.FileNotFound => "required file was not found; check the configured path",
            error.AddressInUse => "listen address is already in use",
            error.InvalidCharacter, error.InvalidArguments => "invalid command-line value; run with --help",
            else => "startup failed; see the preceding diagnostic for details",
        };
        std.debug.print("antfly {s}: {s}\n", .{ @tagName(role_options.role), message });
        return 1;
    };
    return 0;
}

comptime {
    @export(&runtimeEntry, .{ .name = "antfly_runtime_" ++ @tagName(role_options.role) });
}

fn runtimeInit(init: std.process.Init) std.process.Init {
    return .{
        .minimal = init.minimal,
        .arena = init.arena,
        .gpa = runtimeAllocator(init),
        .io = init.io,
        .environ_map = init.environ_map,
        .preopens = init.preopens,
    };
}

fn runtimeAllocator(init: std.process.Init) std.mem.Allocator {
    const fallback = if (!builtin.single_threaded) std.heap.smp_allocator else init.gpa;
    return platform.allocator.processAllocator(fallback);
}
