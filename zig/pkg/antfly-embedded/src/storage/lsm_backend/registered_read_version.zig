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

//! Funded directory-only reader epochs. The immutable directory and reader
//! header keep their actual allocator host until registered physical cleanup.
//! Projection construction is a separate funded operation, not implied here.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const Directory = @import("run_directory.zig").Directory;
const ReadVersion = @import("runtime.zig").ReadVersion;
const Root = @import("registered_directory_root.zig").Candidate;

pub const Owner = struct {
    const State = struct {
        directory: Directory = .{},
        cleanup: ?Directory.Reclaimer = null,
    };
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    host: resources.AllocatorHost,
    registration: resources.PayloadRetirement = .{},
    credit: ?resources.Reservation = null,
    version: ReadVersion,
    state: ?*State = null,
    state_offset: usize = 0,
    state_retiring: bool = false,
    taken: bool = false,
    creation_released: bool = false,
    returned: std.atomic.Value(bool) = .init(false),
    closing: std.atomic.Value(bool) = .init(false),
    complete: bool = false,

    /// Caller pins source through this call. Only canonical registered roots
    /// (or an empty root) are accepted; no Backend or admission borrow escapes.
    pub fn start(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Directory, scope: admission.Port) !*Owner {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        _ = try Root.validateSource(source, a, original);
        var host: ?resources.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(a, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, @sizeOf(Owner) + @sizeOf(State), original);
        defer construction.retire();
        const self = try a.create(Owner);
        var registered = false;
        errdefer if (!registered) a.destroy(self);
        self.* = .{ .allocator = a, .manager = manager, .host = host.?, .version = .{ .allocator = a, .prepared = false, .owned_retirement = .{ .context = self, .retire = returnVersion } } };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        errdefer self.requestRetirement();
        try original.checkpoint();
        self.state = try a.create(State);
        self.state.?.* = .{ .directory = source.forkContents() };
        self.version.directory = &self.state.?.directory;
        return self;
    }
    /// Transfers the one creation reference. A live epoch stays registered but
    /// does not request cleanup CPU until final release or explicit retirement.
    pub fn takeWithAdmission(self: *Owner, original: admission.Port) !*ReadVersion {
        try original.checkpoint();
        if (self.closing.load(.acquire) or self.taken or self.state == null) return error.InvalidArgument;
        self.taken = true;
        return &self.version;
    }
    pub fn requestRetirement(self: *Owner) void {
        if (self.closing.swap(true, .acq_rel)) return;
        self.registration.publish();
    }
    fn returnVersion(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.taken);
        std.debug.assert(self.version.references.load(.acquire) == 0 and !self.version.registered);
        // Registration still owns the frame while its receipt is false, even
        // if an explicit early retirement already made cleanup runnable.
        self.requestRetirement();
        // The last reader has finished every epoch/Backend handoff. Publishing
        // this receipt is its final access; a cleanup peer may now free self.
        self.returned.store(true, .release);
    }
    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        if (self.taken) {
            if (!self.returned.load(.acquire)) return false;
        } else if (!self.creation_released) {
            const previous = self.version.references.fetchSub(1, .acq_rel);
            std.debug.assert(previous == 1);
            self.creation_released = true;
        }
        std.debug.assert(self.version.references.load(.acquire) == 0);
        // This fixed grant owns headers only. No unreserved projection can be
        // attached through the ordinary Backend preparation path.
        std.debug.assert(!self.version.prepared and self.version.runs.len == 0 and self.version.l0_groups.len == 0 and self.version.levels.len == 0 and self.version.projection_bytes == 0);
        if (self.state) |s| {
            if (!self.state_retiring) {
                if (s.cleanup == null) s.cleanup = .init(&s.directory);
                var steps: usize = 1;
                if (!s.cleanup.?.step(self.allocator, &steps)) return false;
                if (original) |port| try port.checkpoint();
                s.cleanup.?.finishContents();
                s.cleanup = null;
                s.directory = .{};
                self.version.directory = null;
                self.state_retiring = true;
            }
            const bytes = std.mem.asBytes(s);
            const end = @min(bytes.len, self.state_offset + 16 * 1024);
            @memset(bytes[self.state_offset..end], undefined);
            self.state_offset = end;
            if (end != bytes.len) return false;
            self.state = null;
            self.allocator.rawFree(bytes, .of(State), @returnAddress());
            return false;
        }
        self.complete = true;
        return true;
    }
    fn destroy(raw: *anyopaque) void {
        comptime std.debug.assert(@sizeOf(Owner) <= 16 * 1024);
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.state == null);
        const a = self.allocator;
        const host = self.host;
        a.destroy(self);
        host.release();
    }
};
