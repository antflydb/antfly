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

/// A captured destination scope has no mutable runtime or broad page fence.
/// The native host independently rechecks namespace identity and collisions.
pub const AliasTarget = struct {
    inode: u64,
    file_namespace: [32]u8,
    prefix: []const u8,
    pub fn require(self: AliasTarget, inode: u64, file_namespace: [32]u8, path: []const u8) !void {
        if (self.inode != inode or !std.mem.eql(u8, &self.file_namespace, &file_namespace)) return error.StaleArtifactConstruction;
        try @import("native_publication.zig").requireDestination(self.prefix, path);
    }
};

/// An unpublished, independently owned immutable artifact. Linking chooses a
/// fresh destination under final writer admission; it does not activate an index.
pub const Prepared = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct {
        link: *const fn (*anyopaque, []const u8) anyerror!Source,
        prepare_link: ?*const fn (*anyopaque, []const u8) anyerror!Source = null,
        prepare_link_at: ?*const fn (*anyopaque, AliasTarget, []const u8) anyerror!Source = null,
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
    pub fn prepareLinkAt(self: Prepared, target: AliasTarget, path: []const u8) !Source {
        const operation = self.vtable.prepare_link_at orelse return error.Unsupported;
        return Boundary.call("prepare_link_at", self.dispatch, operation, .{ self.ptr, target, path });
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
    native_metadata_revision: u32 = 0,
    ptr: *anyopaque,
    allocator: std.mem.Allocator,
    io: std.Io,
    resource_manager: ?*@import("resource_manager.zig").ResourceManager,
    release_fn: *const fn (*anyopaque) void,
    retain_fn: *const fn (*anyopaque) void,
    pub fn retained(self: BuildResources) BuildResources {
        self.retain_fn(self.ptr);
        return self;
    }
    pub fn valueOwner(self: BuildResources) @import("native_publication_value.zig").Owner {
        return .{ .ptr = self.ptr, .retain_fn = self.retain_fn, .release_fn = self.release_fn };
    }
    /// Fixed publication metadata owns an independent resource reference and
    /// keeps its quota until the last reader releases the sealed value.
    pub fn createPublicationValue(self: BuildResources, length: usize) !@import("native_publication_value.zig").Builder {
        const Builder = @import("native_publication_value.zig").Builder;
        if (self.resource_manager) |manager| {
            const bytes = try Builder.budgetedBytes(@import("resource_manager.zig").Reservation, length);
            var credit = try manager.reserveWithoutReclaim(.lite_docstore_snapshot_cache, bytes);
            errdefer credit.release();
            return try Builder.createBudgeted(self.allocator, length, self.valueOwner(), credit);
        }
        return try Builder.createWithOwner(self.allocator, length, self.valueOwner());
    }
    pub fn deinit(self: *BuildResources) void {
        self.release_fn(self.ptr);
        self.* = undefined;
    }
};

/// One request's remaining contention budget. Active preparation/IO does not
/// consume it; every nested admission debits the same independently owned sink.
pub const AdmissionBudget = struct {
    remaining_ns: u64,
    waited_ns: u64 = 0,
    pub fn deadline(self: *const AdmissionBudget, io: std.Io) i96 {
        return std.Io.Clock.awake.now(io).nanoseconds + @as(i96, self.remaining_ns);
    }
    pub fn charge(self: *AdmissionBudget, elapsed_ns: u64) void {
        self.waited_ns +|= elapsed_ns;
        self.remaining_ns -|= elapsed_ns;
    }
    pub fn chargeElapsed(self: *AdmissionBudget, io: std.Io, started_ns: i96) void {
        self.charge(@intCast(@max(0, std.Io.Clock.awake.now(io).nanoseconds - started_ns)));
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
        /// Independently owned preparation. It borrows no live runtime or
        /// primary snapshot. Large artifacts may use bounded unpublished
        /// native commands through an independently retained publication gate.
        stage_detached: ?*const fn (*anyopaque, []const u8) anyerror!Prepared = null,
        stage_detached_with_budget: ?*const fn (*anyopaque, []const u8, *AdmissionBudget) anyerror!Prepared = null,
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
    pub fn stageDetached(self: Stager, bytes: []const u8) !Prepared {
        const operation = self.vtable.stage_detached orelse return error.Unsupported;
        return Boundary.call("stage_detached", self.dispatch, operation, .{ self.ptr, bytes });
    }
    pub fn stageDetachedWithBudget(self: Stager, bytes: []const u8, budget: *AdmissionBudget) !Prepared {
        const operation = self.vtable.stage_detached_with_budget orelse return error.Unsupported;
        return Boundary.call("stage_detached_with_budget", self.dispatch, operation, .{ self.ptr, bytes, budget });
    }
    pub fn deinit(self: *Stager) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }
};
