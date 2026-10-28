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

//! Normal flush adapter for the owned streaming checkpoint writer. The backend
//! retains this funded owner through physical join and independent cleanup.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const Directory = @import("run_directory.zig").Directory;
const Ledger = @import("obsolete_ledger.zig").Ledger;
const repository = @import("repository.zig");
const manifest = @import("../lsm/manifest.zig");
const io = @import("storage_io.zig");
const Cleanup = @import("output_cleanup.zig");
pub var test_hook: ?*const fn (admission.Port) anyerror!void = null;
pub var test_descriptor_hook: ?*const fn (admission.Port) anyerror!void = null;

pub const Source = struct {
    cursor: Directory.Cursor,
    ledger: Ledger.Cursor,
    count: usize,
    root: []const u8,
    pub fn runCount(self: *const @This()) usize {
        return self.cursor.directory.count();
    }
    pub fn obsoleteCount(self: *const @This()) usize {
        return self.count;
    }
    pub fn nextRun(self: *@This()) ?manifest.RunMeta {
        var meta = repository.runMeta((self.cursor.next() orelse return null).run.*);
        meta.path = repository.manifestRelativePath(self.root, meta.path);
        return meta;
    }
    pub fn nextObsolete(self: *@This()) ?manifest.ObsoletePathMeta {
        const path = self.ledger.next() orelse return null;
        return .{ .path = repository.manifestRelativePath(self.root, path.path), .delete_after_ns = path.delete_after_ns };
    }
};
pub const Physical = @import("owned_manifest_set.zig").Publication(Source);
pub const Owner = struct {
    physical: Physical = undefined,
    live: bool = false,
    grant: ?io.DurabilityResources = null,
    queue: ?*Cleanup.Queue = null,
    creations: [2]?Cleanup.Creation = @splat(null),
    tickets: [2]?*Cleanup.Ticket = @splat(null),
    ticket_index: usize = 0,
    cleanup_index: usize = 0,
    retired: bool = false,
    preserve_paths: bool = false,
    deadline: u64 = 0,

    pub fn prepare(self: *@This(), storage: io.Storage, queue: *Cleanup.Queue, root: []const u8, id: u64, next_id: u64, source: Source, original: admission.Port) !void {
        if (comptime @import("builtin").is_test) if (repository.publication_test_hook) |hook| try hook.run(hook.context);
        if (comptime @import("builtin").is_test) if (test_hook) |hook| try hook(original);
        self.grant = (try storage.tryPrepareDurabilityWithAdmission(original)) orelse return error.ResourceBudgetExceeded;
        try self.physical.initInPlace(self.grant.?, root, id, 0, next_id, source, repository.maxManifestReadBytes());
        self.live = true;
        self.grant = null;
        try queue.retainProducerWithAdmission(original);
        self.queue = queue;
        for (&self.creations, 0..) |*creation, i| creation.* = Cleanup.Creation.init(queue, self.physical.paths[i][0..self.physical.path_lens[i]]);
        while (self.ticket_index < 2) {
            const i = self.ticket_index;
            try self.creations[i].?.resumeWithAdmission(original);
            self.tickets[i] = try self.creations[i].?.take();
            self.ticket_index += 1;
        }
        try self.physical.prepareDescriptorWithAdmission(original);
    }
    pub fn finish(self: *@This(), original: admission.Port) !usize {
        if (comptime @import("builtin").is_test) if (test_descriptor_hook) |hook| try hook(original);
        self.physical.resumeWithAdmission(original) catch |cause| {
            self.preserve_paths = self.descriptorMayHavePublished();
            return cause;
        };
        self.preserve_paths = true;
        return self.physical.encoder.total;
    }
    pub fn descriptorMayHavePublished(self: *const @This()) bool {
        return self.live and (self.physical.phase == .complete or self.physical.attempted[2] or
            (self.physical.phase == .descriptor and self.physical.file.publication_attempted));
    }
    pub fn retireStep(self: *@This(), original: ?admission.Port) !bool {
        if (self.retired) return true;
        if (original) |port| try port.checkpoint();
        if (self.live) {
            if (original) |port| try self.physical.abortWithAdmission(port);
            self.physical.deinit();
            self.live = false;
        }
        if (self.grant) |*grant| {
            if (original) |port| try grant.retireWithAdmission(port) else grant.deinit();
            self.grant = null;
        }
        if (self.cleanup_index < 2) {
            const i = self.cleanup_index;
            if (self.tickets[i]) |ticket| {
                if (self.preserve_paths) ticket.destroy() else if (original) |port| try ticket.abandonWithAdmission(port) else ticket.abandon();
                self.tickets[i] = null;
            }
            if (self.creations[i]) |*creation| try creation.retireWithAdmission(original);
            self.creations[i] = null;
            self.cleanup_index += 1;
            return false;
        }
        if (self.queue) |queue| {
            self.queue = null;
            queue.releaseProducer();
        }
        self.retired = true;
        return true;
    }
};
