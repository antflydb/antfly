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

//! Platform-owned I/O namespace. Backends return the shared Zig I/O interface.
const std = @import("std");
const builtin = @import("builtin");

/// Borrowed I/O context, compatible with APIs accepting std.Io.
/// A context dispatches through the vtable of the executor that created it.
pub const Context = std.Io;

/// Owning executor; Windows retains repository-owned cancellation and TLS.
pub const Threaded = if (builtin.os.tag == .windows) @import("threaded_windows.zig") else std.Io.Threaded;

/// Fiber backends adapted to the pinned Zig release I/O interface.
pub const Evented = if (std.Io.fiber.supported) switch (builtin.os.tag) {
    .linux => @import("io_uring_compat.zig"),
    .macos => @import("dispatch_compat.zig"),
    else => std.Io.Evented,
} else void;
