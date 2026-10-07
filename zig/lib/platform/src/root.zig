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

pub const allocator = @import("allocator.zig");
pub const atomic = @import("atomic.zig");
pub const clock = @import("clock.zig");
pub const env = @import("env.zig");
pub const entropy = @import("entropy.zig");
pub const filesystem = @import("filesystem.zig");
pub const inference_process_supervisor = @import("inference_process_supervisor.zig");
pub const one_shot_process = @import("one_shot_process.zig");
pub const process = @import("process.zig");
pub const process_memory = @import("process_memory.zig");
pub const sync = @import("sync.zig");
pub const time = @import("time.zig");

/// Fiber backends adapted to the pinned Zig release Io interface.
pub const Evented = if (@import("std").Io.fiber.supported) switch (@import("builtin").os.tag) {
    .linux => @import("io_uring_compat.zig"),
    .macos => @import("dispatch_compat.zig"),
    else => @import("std").Io.Evented,
} else void;

/// Native owning executor; Windows retains repository-owned cancellation/TLS.
pub const Threaded = if (@import("builtin").os.tag == .windows) @import("threaded_windows.zig") else @import("std").Io.Threaded;
pub const c = @import("native_c.zig");
pub const DynLib = if (@import("builtin").os.tag == .windows) @import("windows_native.zig").WindowsDynLib else @import("std").DynLib;
pub const testing = @import("testing.zig");

/// Process-lifetime fallback for diagnostics and legacy optional I/O.
/// Runtime APIs should continue to use their caller-supplied executor.
var debug_threaded: Threaded = .init_single_threaded;
pub const debug_io: @import("std").Io = if (@import("builtin").os.tag == .windows) debug_threaded.io() else @import("std").Options.debug_io;
