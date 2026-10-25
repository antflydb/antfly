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

//! Registered construction and shutdown ownership for a publication controller.
//! This frame retains the actual allocator host and quota anchor through all
//! entry/reader obligations. It never retains an execution or backend receiver.
const std = @import("std");
const resources = @import("../resource_manager.zig");
const admission = @import("../../common/read_admission.zig");
const quota = @import("pin_quota.zig");
const entry_owner = @import("directory_entry_owner.zig");
const Directory = @import("run_directory.zig").Directory;
const Run = @import("repository.zig").Run;

pub const Owner = struct {
    pub const EntryLease = Directory.EntryLease;
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    host: resources.AllocatorHost,
    registration: resources.PayloadRetirement = .{},
    credit: ?resources.Reservation = null,
    close: quota.Close = .{},
    creation: ?quota.Creation = null,
    quota_owner: ?*quota.Owner = null,
    entry: ?*entry_owner.Owner = null,
    closing: bool = false,
    complete: bool = false,

    fn pinHost(host: resources.AllocatorHost) quota.HostLifetime {
        return .{ .manager = host.manager, .parent = host.parent, .context = host.context, .release_joined = host.release_joined };
    }

    pub fn start(allocator: std.mem.Allocator, manager: *resources.ResourceManager, scope: admission.Port) !*Owner {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        var host: ?resources.AllocatorHost = try manager.acquireAllocatorHostWithAdmission(allocator, original);
        defer if (host) |held| held.release();
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, @sizeOf(Owner), original);
        defer construction.retire();
        const self = try allocator.create(Owner);
        var registered = false;
        errdefer if (!registered) allocator.destroy(self);
        self.* = .{ .allocator = allocator, .manager = manager, .host = host.? };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host = null;
        errdefer self.requestRetirement();
        // The quota close anchor owns a separate host retention. Its embedded
        // cursor lives at this stable address even after the producer exits.
        const quota_host = try manager.acquireAllocatorHostWithAdmission(allocator, original);
        errdefer quota_host.release();
        self.creation = try quota.Creation.initHosted(allocator, manager, &self.close, pinHost(quota_host));
        return self;
    }

    pub fn resumeWithAdmission(self: *Owner, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        if (self.quota_owner != null) return;
        const creation = if (self.creation) |*value| value else return error.InvalidArgument;
        try creation.resumeWithAdmission(original);
        self.quota_owner = try creation.takeWithAdmission(original);
        self.creation = null;
    }

    /// Source/ancestor authority is independently acquired by the controller.
    /// The in/out lifetime records transfer even on refusal after registration.
    /// A mismatched backend cannot replace this owner's original host or quota.
    pub fn beginEntryWithAdmission(self: *Owner, backend: anytype, directory: *Directory, source: Run, lifetime: *?entry_owner.Lifetime, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.entry != null) return error.InvalidArgument;
        if (backend.options.resource_manager != self.manager or backend.allocator.ptr != self.allocator.ptr or backend.allocator.vtable != self.allocator.vtable) return error.ResourceAccountingMismatch;
        const owner = self.quota_owner orelse return error.InvalidArgument;
        const retained_host = try self.manager.acquireAllocatorHostWithAdmission(self.allocator, original);
        var host: ?entry_owner.HostLifetime = .{ .allocator = retained_host.allocator, .lifetime = pinHost(retained_host) };
        defer if (host) |held| held.lifetime.release_joined(held.lifetime.context);
        self.entry = try backend.beginOwnedDirectoryEntryPreparation(directory, source, owner, &host, lifetime, original);
    }

    pub fn beginEntryFromRegisteredSourceWithAdmission(self: *Owner, backend: anytype, directory: *Directory, source: *const Directory.EntryLease, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.entry != null) return error.InvalidArgument;
        if (backend.options.resource_manager != self.manager or backend.allocator.ptr != self.allocator.ptr or backend.allocator.vtable != self.allocator.vtable) return error.ResourceAccountingMismatch;
        const routing = try source.registeredRouting(original);
        if (!routing.matches(.fromOptions(backend.options))) return error.CompactionPlanningStale;
        const owner = self.quota_owner orelse return error.InvalidArgument;
        const retained_host = try self.manager.acquireAllocatorHostWithAdmission(self.allocator, original);
        var host: ?entry_owner.HostLifetime = .{ .allocator = retained_host.allocator, .lifetime = pinHost(retained_host) };
        defer if (host) |held| held.lifetime.release_joined(held.lifetime.context);
        self.entry = try backend.beginOwnedDirectoryEntryFromRegisteredSource(directory, source, owner, &host, original);
    }

    pub fn beginEntryFromRegisteredHandleWithAdmission(self: *Owner, backend: anytype, directory: *Directory, source: Directory.Handle, original: admission.Port) !void {
        var captured = try source.retainRegisteredSourceWithAdmission(original);
        // The registered anchor makes this a fixed non-final drop, including
        // when the original admission already refused subsequent construction.
        defer captured.releaseWithAdmission(null) catch unreachable;
        try self.beginEntryFromRegisteredSourceWithAdmission(backend, directory, &captured.lease.?, original);
    }

    pub const RegisteredOutput = @import("registered_run_output.zig");
    pub fn beginRegisteredOutputWithAdmission(self: *Owner, original: admission.Port) !*RegisteredOutput.Owner {
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        return RegisteredOutput.Owner.start(self.allocator, self.manager, original);
    }
    pub fn beginEntryFromRegisteredOutputWithAdmission(self: *Owner, backend: anytype, directory: *Directory, source: *RegisteredOutput.Owner, metadata: RegisteredOutput.Metadata, scope: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing or self.entry != null) return error.InvalidArgument;
        if (backend.options.resource_manager != self.manager or backend.allocator.ptr != self.allocator.ptr or backend.allocator.vtable != self.allocator.vtable) return error.ResourceAccountingMismatch;
        const owner = self.quota_owner orelse return error.InvalidArgument;
        const retained_host = try self.manager.acquireAllocatorHostWithAdmission(self.allocator, original);
        var host: ?entry_owner.HostLifetime = .{ .allocator = retained_host.allocator, .lifetime = pinHost(retained_host) };
        defer if (host) |held| held.lifetime.release_joined(held.lifetime.context);
        self.entry = try backend.beginOwnedDirectoryEntryFromRegisteredOutput(directory, source, metadata, owner, &host, original);
    }

    pub const RegisteredWriter = @import("registered_writer.zig").Record;
    pub const RegisteredWriterRoot = @import("registered_writer_root.zig").Candidate;
    pub const WriterStore = @import("run_store.zig").Store;

    pub fn beginRegisteredWriterWithAdmission(self: *Owner, backend: anytype, source: *const Directory.EntryLease, revision: RegisteredWriter.Revision, scope: admission.Port) !*RegisteredWriter {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        if (backend.options.resource_manager != self.manager or backend.allocator.ptr != self.allocator.ptr or backend.allocator.vtable != self.allocator.vtable) return error.ResourceAccountingMismatch;
        const routing = try source.registeredRouting(original);
        if (!routing.matches(.fromOptions(backend.options))) return error.CompactionPlanningStale;
        return RegisteredWriter.start(self.allocator, self.manager, source, revision, original);
    }
    pub fn beginRegisteredWriterRootWithAdmission(self: *Owner, source: *const WriterStore, run: *const Run, replace: bool, original: admission.Port) !*RegisteredWriterRoot {
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        return RegisteredWriterRoot.start(self.allocator, self.manager, source, run, replace, original);
    }

    pub const RegisteredObsolete = @import("registered_obsolete.zig").Candidate;
    pub const ObsoleteLedger = @import("obsolete_ledger.zig").Ledger;
    pub fn beginObsoletePutWithAdmission(self: *Owner, source: *const ObsoleteLedger, run: *const Run, deadline: u64, original: admission.Port) !*RegisteredObsolete {
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        return RegisteredObsolete.startPut(self.allocator, self.manager, source, run, deadline, original);
    }
    pub fn beginObsoleteRemoveWithAdmission(self: *Owner, source: *const ObsoleteLedger, rank: usize, original: admission.Port) !*RegisteredObsolete {
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        return RegisteredObsolete.startRemove(self.allocator, self.manager, source, rank, original);
    }

    pub const DirectoryRoot = Directory;
    pub const RegisteredDirectory = @import("registered_directory_root.zig").Candidate;
    pub fn beginDirectoryPutWithAdmission(self: *Owner, source: *const Directory, lease: *const EntryLease, original: admission.Port) !*RegisteredDirectory {
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        return RegisteredDirectory.startPut(self.allocator, self.manager, source, lease, original);
    }
    pub fn beginDirectoryRemoveWithAdmission(self: *Owner, source: *const Directory, handle: Directory.Handle, original: admission.Port) !*RegisteredDirectory {
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        return RegisteredDirectory.startRemove(self.allocator, self.manager, source, handle, original);
    }

    /// Transfer a reader/candidate lease only after construction and final
    /// registration finish. Independent retirement immediately owns the anchor.
    pub fn takeEntryWithAdmission(self: *Owner, scope: admission.Port) !Directory.EntryLease {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope).?;
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        const entry = self.entry orelse return error.InvalidArgument;
        try entry.resumeWithAdmission(original);
        const lease = try entry.retainWithAdmission(original);
        entry.requestRetirement();
        self.entry = null;
        return lease;
    }

    /// Transfers the pointer to independent cleanup; never access it again.
    pub fn requestRetirement(self: *Owner) void {
        std.debug.assert(!self.closing);
        self.closing = true;
        self.registration.publish();
    }

    fn advance(raw: *anyopaque, scope: ?admission.Port) !bool {
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope);
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.closing);
        if (original) |port| try port.checkpoint();
        if (self.entry) |entry| {
            entry.requestRetirement();
            self.entry = null;
        }
        self.quota_owner = null;
        if (self.creation) |*creation| {
            if (original) |port| try creation.retireWithAdmission(port) else creation.deinitJoined();
            self.creation = null;
        } else if (self.close.manager != null and !self.close.complete) {
            // Joined draining must not wait on a peer registration that this
            // same registry still has to visit. Leave this root pending instead.
            if (original == null) {
                if (!self.close.tryDeinitJoined()) return false;
            } else {
                self.close.resumeWithAdmission(original.?) catch |cause| {
                    if (trace.failure) |first| return first;
                    if (cause == error.ParentMemoryBusy) return false;
                    return cause;
                };
            }
        }
        // Already completed: this is only a fixed host-retention drop, not a
        // variable joined destructor substituted for the original admission.
        self.close.deinitJoined();
        self.complete = true;
        return true;
    }
    fn destroy(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.creation == null and self.entry == null and self.close.owner == null);
        const allocator = self.allocator;
        const host = self.host;
        allocator.destroy(self);
        host.release();
    }
};
