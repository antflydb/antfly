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

//! An inherited open-file lease, not a PID/health/timeout inference. It fences
//! generation overlap only for processes which retain the inherited descriptor.
const std = @import("std");
const builtin = @import("builtin");
pub const supported = builtin.os.tag == .linux or builtin.os.tag == .macos;
pub const Lease = struct {
    file: ?std.Io.File,

    pub fn acquire(io: std.Io, dir: std.Io.Dir, name: []const u8) !Lease {
        if (!supported) return error.UnsupportedProcessGenerationFence;
        const file = try dir.createFile(io, name, .{ .read = true, .truncate = false });
        errdefer file.close(io);
        if (!try file.tryLock(io, .exclusive)) return error.ProcessGenerationBusy;
        return .{ .file = file };
    }
    pub fn inheritedFile(self: *const Lease) std.Io.File {
        return self.file orelse unreachable;
    }
    /// Never explicitly unlock: that would revoke the shared open-file lock
    /// while an inherited descriptor still belongs to a living child.
    pub fn close(self: *Lease, io: std.Io) void {
        const file = self.file orelse return;
        self.file = null;
        file.close(io);
    }
};
