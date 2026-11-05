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

const abi = @import("../runtime_callback_abi.zig");
const Source = @import("../segment_source.zig").Source;

/// An unpublished, independently owned immutable artifact. Linking chooses a
/// fresh destination under final writer admission; it does not activate an index.
pub const Prepared = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct {
        link: *const fn (*anyopaque, []const u8) anyerror!Source,
        deinit: *const fn (*anyopaque) void,
    };
    const Boundary = abi.Boundary(VTable);
    pub fn link(self: Prepared, path: []const u8) !Source {
        return Boundary.call("link", self.dispatch, self.vtable.link, .{ self.ptr, path });
    }
    pub fn deinit(self: *Prepared) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }
};

/// Captured under catalog admission, used after releasing that admission.
/// Owns no mutable index, primary snapshot or borrowed DB/runtime pointer.
pub const Stager = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct {
        stage: *const fn (*anyopaque, []const u8) anyerror!Prepared,
        deinit: *const fn (*anyopaque) void,
    };
    const Boundary = abi.Boundary(VTable);
    pub fn stage(self: Stager, bytes: []const u8) !Prepared {
        return Boundary.call("stage", self.dispatch, self.vtable.stage, .{ self.ptr, bytes });
    }
    pub fn deinit(self: *Stager) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }
};
