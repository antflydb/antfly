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

const std = @import("std");
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
        prepare_link: ?*const fn (*anyopaque, []const u8) anyerror!Source = null,
        stage_link: ?*const fn (*anyopaque, @import("native_publication.zig").View) anyerror!void = null,
        deinit: *const fn (*anyopaque) void,
    };
    const Boundary = abi.Boundary(VTable);
    pub fn link(self: Prepared, path: []const u8) !Source {
        return Boundary.call("link", self.dispatch, self.vtable.link, .{ self.ptr, path });
    }
    pub fn prepareLink(self: Prepared, path: []const u8) !Source {
        const operation = self.vtable.prepare_link orelse return error.Unsupported;
        return Boundary.call("prepare_link", self.dispatch, operation, .{ self.ptr, path });
    }
    pub fn stageLink(self: Prepared, view: @import("native_publication.zig").View) !void {
        const operation = self.vtable.stage_link orelse return error.Unsupported;
        return Boundary.call("stage_link", self.dispatch, operation, .{ self.ptr, view });
    }
    pub fn deinit(self: *Prepared) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }
};

/// Independently retained preparation resources. This capability borrows no
/// DB, Store, schema/index entry, primary snapshot or publication admission.
pub const BuildResources = struct {
    ptr: *anyopaque,
    allocator: std.mem.Allocator,
    io: std.Io,
    resource_manager: ?*@import("resource_manager.zig").ResourceManager,
    release_fn: *const fn (*anyopaque) void,
    pub fn deinit(self: *BuildResources) void {
        self.release_fn(self.ptr);
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
        acquire_build_resources: ?*const fn (*anyopaque) anyerror!?BuildResources = null,
        deinit: *const fn (*anyopaque) void,
    };
    const Boundary = abi.Boundary(VTable);
    pub fn acquireBuildResources(self: Stager) !?BuildResources {
        const operation = self.vtable.acquire_build_resources orelse return null;
        return Boundary.call("acquire_build_resources", self.dispatch, operation, .{self.ptr});
    }
    pub fn stage(self: Stager, bytes: []const u8) !Prepared {
        return Boundary.call("stage", self.dispatch, self.vtable.stage, .{ self.ptr, bytes });
    }
    pub fn deinit(self: *Stager) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }
};
