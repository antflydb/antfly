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

//! Funded reader epochs. Directory/header and optional projection ownership
//! retain their actual allocator hosts through registered physical cleanup.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const Directory = @import("run_directory.zig").Directory;
const ReadVersion = @import("runtime.zig").ReadVersion;
const Root = @import("registered_directory_root.zig").Candidate;
const Projection = @import("registered_read_projection.zig").Owner;

pub const Owner = struct {
    const State = struct {
        directory: Directory = .{},
        cleanup: ?Directory.Reclaimer = null,
        projection: ?*Projection = null,
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
        const self = try prepare(a, manager, original);
        errdefer self.requestRetirement();
        try self.captureWithAdmission(source, original);
        return self;
    }
    /// Fund and register an empty frame off-lock. Its source is captured later
    /// in a fixed tail under the caller's generation/publication mutex.
    pub fn prepare(a: std.mem.Allocator, manager: *resources.ResourceManager, original: admission.Port) !*Owner {
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
        self.state.?.* = .{};
        return self;
    }
    /// Caller holds the source's publication mutex. Validation and six root
    /// retains are fixed work; lifetime checks never park under that mutex.
    pub fn captureWithAdmission(self: *Owner, source: *const Directory, original: admission.Port) !void {
        try original.checkLifetime();
        if (self.closing.load(.acquire) or self.taken or self.state == null or self.version.directory != null) return error.InvalidArgument;
        _ = try Root.validateSourceFixed(source, self.allocator);
        self.state.?.directory = source.forkContents();
        self.version.directory = &self.state.?.directory;
    }
    /// Capture and transfer share one lifetime check. After capture succeeds,
    /// the transfer cannot refuse and strand a frame between generations.
    pub fn captureAndTakeWithAdmission(self: *Owner, source: *const Directory, original: admission.Port) !*ReadVersion {
        try self.captureWithAdmission(source, original);
        self.taken = true;
        return &self.version;
    }
    /// Fixed ownership transfer for serialized Backend installation.
    pub fn takeCapturedWithAdmission(self: *Owner, original: admission.Port) !*ReadVersion {
        try original.checkLifetime();
        if (self.closing.load(.acquire) or self.taken or self.state == null or self.version.directory == null) return error.InvalidArgument;
        self.taken = true;
        return &self.version;
    }
    /// Transfers the one creation reference. A live epoch stays registered but
    /// does not request cleanup CPU until final release or explicit retirement.
    pub fn takeWithAdmission(self: *Owner, original: admission.Port) !*ReadVersion {
        try original.checkpoint();
        return self.takeCapturedWithAdmission(original);
    }
    fn fromVersion(version: *ReadVersion) !*Owner {
        const capability = version.owned_retirement orelse return error.ExecutionWaitUnsupported;
        if (capability.retire != returnVersion) return error.InvalidArgument;
        const self: *Owner = @ptrCast(@alignCast(capability.context));
        if (&self.version != version or self.complete or self.state == null or self.returned.load(.acquire) or version.references.load(.acquire) == 0 or (!self.taken and self.closing.load(.acquire)) or self.registration.credit == null or self.registration.context != @as(*anyopaque, @ptrCast(self)) or self.registration.step == null or self.registration.step.?.advance != advance or self.registration.credit.?.manager != self.manager or version.directory != &self.state.?.directory) return error.InvalidArgument;
        return self;
    }
    /// Caller owns an epoch reference and serializes preparation. All variable
    /// work occurs off service locks; refusal keeps this same registered builder.
    pub fn resumeProjectionForVersionWithAdmission(version: *ReadVersion, original: admission.Port) !void {
        try original.checkpoint();
        const self = try fromVersion(version);
        if (version.prepared) return;
        const s = self.state.?;
        if (s.projection == null) s.projection = try Projection.start(self.allocator, self.manager, &s.directory, original);
        try s.projection.?.resumeWithAdmission(original);
    }
    /// Fixed, serialized publication tail. Lifetime checks cannot cooperatively
    /// park while the caller holds its Backend publication mutex.
    pub fn publishProjectionForVersionWithAdmission(version: *ReadVersion, original: admission.Port) !bool {
        try original.checkLifetime();
        const self = try fromVersion(version);
        if (version.prepared) return false;
        const projection = self.state.?.projection orelse return error.InvalidArgument;
        const views = try projection.takePrepared();
        version.runs = views.runs;
        version.l0_groups = views.groups;
        version.levels = views.levels;
        version.projection_bytes = views.bytes;
        version.prepared = true;
        return true;
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
        if (self.state) |s| {
            if (s.projection) |projection| {
                s.projection = null;
                self.version.runs = &.{};
                self.version.l0_groups = &.{};
                self.version.levels = &.{};
                self.version.projection_bytes = 0;
                self.version.prepared = false;
                // The child owns its complete buffers and a separate directory
                // snapshot/host. No epoch or Backend pointer survives this drop.
                projection.requestRetirement();
                return false;
            }
            std.debug.assert(!self.version.prepared and self.version.runs.len == 0 and self.version.l0_groups.len == 0 and self.version.levels.len == 0 and self.version.projection_bytes == 0);
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
