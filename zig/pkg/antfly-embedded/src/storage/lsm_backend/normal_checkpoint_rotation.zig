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

//! The journal handoff retains its grant and exact accepted IO through joining.
const std = @import("std");
const io = @import("storage_io.zig");
const manifests = @import("manifest_set.zig");
const File = @import("owned_atomic_file.zig").File;
const admission = @import("../../common/read_admission.zig");
const Cleanup = @import("output_cleanup.zig");
pub var test_hook: ?*const fn (*Owner, admission.Port) anyerror!void = null;
pub const Owner = struct {
    file: File = undefined,
    grant: ?io.DurabilityResources = null,
    live: bool = false,
    stage: usize = 0,
    link_path: [std.fs.max_path_bytes]u8 = undefined,
    link_len: usize = 0,
    segment: [manifests.header_len]u8 = undefined,
    link: [manifests.header_len]u8 = undefined,
    queue: ?*Cleanup.Queue = null,
    creation: ?Cleanup.Creation = null,
    ticket: ?*Cleanup.Ticket = null,
    preserve: bool = false,
    retired: bool = false,
    pub fn prepare(self: *@This(), storage: io.Storage, queue: *Cleanup.Queue, root: []const u8, previous: u64, id: u64, sequence: u64, original: admission.Port) !void {
        if (id <= previous) return error.InvalidManifest;
        var path: [std.fs.max_path_bytes]u8 = undefined;
        const journal = try std.fmt.bufPrint(&path, "{s}/manifest-{d}.journal", .{ root, id });
        self.link_len = (try std.fmt.bufPrint(&self.link_path, "{s}/manifest-{d}.next", .{ root, previous })).len;
        self.segment = manifests.segmentHeader(id, try std.math.add(u64, sequence, 1));
        self.link = manifests.linkHeader(previous, id);
        self.grant = (try storage.tryPrepareDurabilityWithAdmission(original)) orelse return error.ResourceBudgetExceeded;
        self.file = try File.init(self.grant.?, journal);
        self.live = true;
        self.grant = null;
        try queue.retainProducerWithAdmission(original);
        self.queue = queue;
        self.creation = Cleanup.Creation.init(queue, journal);
        try self.creation.?.resumeWithAdmission(original);
        self.ticket = try self.creation.?.take();
        while (self.stage < 2) {
            if (comptime @import("builtin").is_test) if (test_hook) |hook| try hook(self, original);
            try self.file.openWithAdmission(original);
            try self.file.writeWithAdmission(0, if (self.stage == 0) &self.segment else &self.link, original);
            try self.file.finishWithAdmission(original);
            self.stage += 1;
            if (self.stage == 1) self.file = try File.init(try self.file.takeResources(), self.link_path[0..self.link_len]);
        }
        self.preserve = true;
    }
    pub fn linkMayHavePublished(self: *const @This()) bool {
        return self.preserve or self.stage == 2 or (self.stage == 1 and self.live and self.file.publication_attempted);
    }
    pub fn joinWithAdmission(self: *@This(), original: admission.Port) !void {
        self.preserve = self.preserve or self.linkMayHavePublished();
        if (self.live) try self.file.abortWithAdmission(original);
    }
    pub fn join(self: *@This()) void {
        self.preserve = self.preserve or self.linkMayHavePublished();
        if (self.live) {
            self.file.deinit();
            self.live = false;
        }
    }
    pub fn retireStep(self: *@This(), original: ?admission.Port) !bool {
        if (self.retired) return true;
        if (original) |port| try port.checkpoint();
        if (self.live) {
            if (original) |port| {
                try self.joinWithAdmission(port);
                try self.file.retireWithAdmission(port);
            }
            self.join();
        }
        if (self.grant) |*grant| {
            if (original) |port| try grant.retireWithAdmission(port) else grant.deinit();
            self.grant = null;
        }
        if (self.ticket) |ticket| {
            if (self.preserve) ticket.destroy() else if (original) |port| try ticket.abandonWithAdmission(port) else ticket.abandon();
            self.ticket = null;
        }
        if (self.creation) |*creation| try creation.retireWithAdmission(original);
        self.creation = null;
        if (self.queue) |queue| queue.releaseProducer();
        self.queue = null;
        self.retired = true;
        return true;
    }
};
