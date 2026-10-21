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

//! Loader for the monorepo inference library. Native Windows loader support
//! complements std.DynLib on the pinned Zig toolchain.
const std = @import("std");
pub const Library = if (@import("builtin").os.tag == .windows) Windows else std.DynLib;
const Windows = struct {
    handle: std.os.windows.PVOID,
    pub fn open(path: []const u8) !Windows {
        const w = std.os.windows;
        const wide = try std.unicode.utf8ToUtf16LeAllocZ(std.heap.page_allocator, path);
        defer std.heap.page_allocator.free(wide);
        if (wide.len > 32766) return error.NameTooLong;
        const name = w.UNICODE_STRING{ .Length = @intCast(wide.len * 2), .MaximumLength = @intCast((wide.len + 1) * 2), .Buffer = wide.ptr };
        var handle: w.PVOID = undefined;
        if (w.ntdll.LdrLoadDll(null, null, &name, &handle) != .SUCCESS) return error.FileNotFound;
        return .{ .handle = handle };
    }
    pub fn close(self: *Windows) void {
        _ = std.os.windows.ntdll.LdrUnloadDll(self.handle);
    }
    pub fn lookup(self: *Windows, comptime T: type, name: [:0]const u8) ?T {
        const w = std.os.windows;
        if (name.len > 65534) return null;
        const symbol = w.ANSI_STRING{ .Length = @intCast(name.len), .MaximumLength = @intCast(name.len + 1), .Buffer = @constCast(name.ptr) };
        var pointer: w.PVOID = undefined;
        if (w.ntdll.LdrGetProcedureAddress(self.handle, &symbol, 0, &pointer) != .SUCCESS) return null;
        return @ptrCast(pointer);
    }
};
