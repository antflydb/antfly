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

//! Registered final anchor for one prepared directory payload. A publication
//! controller serializes its methods, transfers leases to private/live roots,
//! then requests retirement without joining readers or retaining execution.
const std = @import("std");
const Directory = @import("run_directory.zig").Directory;
const admission = @import("../../common/read_admission.zig");
const resources = @import("../resource_manager.zig");
const quota = @import("pin_quota.zig");
const Run = @import("repository.zig").Run;
const Backend = @import("../lsm_backend.zig").Backend;
const Account = @import("memory_account.zig").Account;
const LifetimeCallbacks = struct { release: *const fn (*anyopaque, ?admission.Port) anyerror!bool };
const LifetimeBoundary = @import("../../runtime_callback_abi.zig").Boundary(LifetimeCallbacks);

/// An acquired, independently stable source/ancestor retention. It preserves
/// shared-source accounting and partition-label storage through the last
/// output reference. Its registered cursor frees variable state incrementally;
/// false/error retains the same cursor. Null is explicit joined teardown and
/// must finish. It must not depend on a receiver destroyed before this owner.
/// This may release source allocator contexts; the separate host retention
/// keeps the exact owner/payload allocator alive through fixed-header free.
pub const Lifetime = struct {
    context: *anyopaque,
    dispatch: LifetimeBoundary.Dispatch = LifetimeBoundary.local_dispatch,
    release: *const fn (*anyopaque, ?admission.Port) anyerror!bool,
    fn advance(self: @This(), original: ?admission.Port) !bool {
        return LifetimeBoundary.call("release", self.dispatch, self.release, .{ self.context, original });
    }
};

/// Already acquired host retention for the exact payload/owner allocator,
/// manager and borrowed accounting ancestors. The quota's allocator may differ.
/// Release drops a retention only; variable host teardown belongs to its own
/// registered/joined owner. This capability transfers only after registration.
pub const HostLifetime = struct {
    allocator: std.mem.Allocator,
    lifetime: quota.HostLifetime,
    fn validate(self: @This(), allocator: std.mem.Allocator, manager: *resources.ResourceManager) !void {
        if (self.allocator.ptr != allocator.ptr or self.allocator.vtable != allocator.vtable or self.lifetime.manager != manager) return error.InvalidArgument;
        if (manager.memory_parent) |parent| {
            const retained = self.lifetime.parent orelse return error.ExecutionWaitUnsupported;
            if (!retained.valid() or !std.meta.eql(parent, retained)) return error.InvalidMemoryParent;
        } else if (self.lifetime.parent != null) return error.InvalidMemoryParent;
    }
};

pub const Owner = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    quota_owner: *quota.Owner,
    pin: Directory.EntryLease.PinRelease,
    host: HostLifetime,
    lifetime: ?Lifetime = null,
    credit: ?resources.Reservation = null,
    registration: resources.PayloadRetirement = .{},
    usage: u64 = 0,
    observer_prepared: bool = false,
    account: ?*Account = null,
    lease: ?Directory.EntryLease = null,
    preparation: ?Backend.DirectoryEntryPreparation = null,
    physical: ?Directory.EntryLease.Retirement = null,
    forgetting: ?@import("run_pin_registry.zig").Operation = null,
    forgetting_stale: bool = false,
    closing: bool = false,
    complete: bool = false,

    fn allocate(allocator: std.mem.Allocator, manager: *resources.ResourceManager, quota_owner: *quota.Owner, host: *?HostLifetime, pin: Directory.EntryLease.PinRelease, original: admission.Port) !*Owner {
        const retained_host = host.* orelse return error.InvalidArgument;
        try retained_host.validate(allocator, manager);
        if (quota_owner.manager != manager) return error.InvalidArgument;
        const retained = try quota_owner.acquireWithAdmission(original);
        var registered = false;
        errdefer if (!registered) retained.releaseBorrow();
        var construction = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, @sizeOf(Owner) + @sizeOf(Account), original);
        defer construction.retire();
        const self = try allocator.create(Owner);
        errdefer if (!registered) allocator.destroy(self);
        self.* = .{ .allocator = allocator, .manager = manager, .quota_owner = retained, .host = retained_host, .pin = pin };
        try manager.registerResumablePayloadRetirementWithAdmission(&self.registration, &self.credit, &construction, self, .{ .advance = advance }, destroy, original);
        registered = true;
        host.* = null;
        return self;
    }

    fn prepareResident(self: *Owner, original: admission.Port) !void {
        try original.checkpoint();
        if (self.account == null) self.account = try Account.createIndependentlyFunded(self.allocator);
        try self.manager.prepareUsageWithAdmission(.lsm_in_memory_state, &self.usage, original);
        self.observer_prepared = true;
    }

    /// Register before constructing any variable payload/pin state. A borrowed
    /// backend is used only in this call. Once registered, lifetime transfers
    /// even if a subsequent constructor checkpoint refuses: the null in/out
    /// capability tells the caller independent cleanup owns its source now.
    /// Earlier refusal leaves lifetime intact. No execution borrow is stored.
    pub fn start(backend: *Backend, directory: *Directory, source: Run, quota_owner: *quota.Owner, host: *?HostLifetime, lifetime: *?Lifetime, pin: Directory.EntryLease.PinRelease, original: admission.Port) !*Owner {
        try original.checkpoint();
        if (lifetime.* == null) return error.InvalidArgument;
        const manager = backend.options.resource_manager orelse return error.ExecutionWaitUnsupported;
        const self = try allocate(backend.allocator, manager, quota_owner, host, pin, original);
        self.lifetime = lifetime.*;
        lifetime.* = null;
        errdefer self.requestRetirement();
        try self.prepareResident(original);
        self.preparation = try backend.beginDirectoryEntryPreparation(directory, source, quota_owner, original);
        return self;
    }

    /// Retain progress at every refusal. The original scope is synchronous;
    /// later scheduler/cleanup admissions supply their own actual authority.
    /// Success leaves the final anchor registered before any lease can escape.
    pub fn resumeWithAdmission(self: *Owner, original: admission.Port) !void {
        try original.checkpoint();
        if (self.closing) return error.InvalidArgument;
        if (self.lease != null) return;
        const preparation = if (self.preparation) |*value| value else return error.InvalidArgument;
        try preparation.resumeWithAdmission(original);
        const lease = try preparation.takeWithIndependentAccountWithAdmission(.lsm_in_memory_state, &self.usage, self.account.?, original);
        self.lease = lease;
        self.preparation = null;
    }

    /// All variable preparation must already be ready. Prepay/register the
    /// fixed owner and its stable resident observer before taking the payload.
    /// Success consumes lifetime and the preparation output in one infallible
    /// tail. Refusal leaves both with the caller, and publishes only the empty
    /// registered frame for independent cleanup. Its separate host retention
    /// transfers at registration, including refusal. No backend receiver escapes.
    pub fn create(preparation: anytype, host: *?HostLifetime, lifetime: *?Lifetime, expected_pin: *const fn (*anyopaque, *Run) void, pin: Directory.EntryLease.PinRelease, original: admission.Port) !*Owner {
        try original.checkpoint();
        if (!preparation.ready or preparation.taken or preparation.retiring or preparation.retired or lifetime.* == null) return error.InvalidArgument;
        if (preparation.payload.?.release_pin != expected_pin) return error.ExecutionWaitUnsupported;
        const manager = preparation.manager;
        const self = try allocate(preparation.allocator, manager, preparation.quota_owner, host, pin, original);
        errdefer self.requestRetirement();
        try self.prepareResident(original);
        const lease = try preparation.takeWithIndependentAccountWithAdmission(.lsm_in_memory_state, &self.usage, self.account.?, original);
        // No refusal after taking the payload: its resident observer, final
        // anchor and source/ancestor capability now have the same stable owner.
        self.lease = lease;
        self.lifetime = lifetime.*;
        lifetime.* = null;
        return self;
    }

    /// Publication uses this additional lease; the registered anchor remains
    /// alive across every candidate, root and reader reference. The caller
    /// serializes this method against requestRetirement.
    pub fn retainWithAdmission(self: *Owner, original: admission.Port) !Directory.EntryLease {
        try original.checkpoint();
        if (self.closing or self.lease == null) return error.InvalidArgument;
        return self.lease.?.retainWithAdmission(original);
    }

    /// Transfers this pointer to the cleanup registry. Never access it again:
    /// an independent consumer may finish and free it before this returns.
    pub fn requestRetirement(self: *Owner) void {
        std.debug.assert(!self.closing);
        self.closing = true;
        self.registration.publish();
    }

    fn advance(raw: *anyopaque, original: ?admission.Port) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (original) |port| try port.checkpoint();
        std.debug.assert(self.closing);
        if (self.preparation) |*preparation| {
            if (original) |port| try preparation.retireWithAdmission(port) else preparation.deinit();
            self.preparation = null;
        }
        if (self.lease) |*lease| {
            self.physical = (try lease.beginFinalRetirement(.{ .context = self, .resume_fn = releasePin }, original)) orelse return false;
            self.lease = null;
        }
        if (self.physical) |*physical| {
            if (original) |port| try physical.retireWithAdmission(self.allocator, port) else physical.deinit(self.allocator);
            self.physical = null;
        }
        // Physical frees precede capacity return; a refused reconciliation
        // preserves the observer and full resident charge in this fixed frame.
        if (self.observer_prepared) {
            if (original) |port| try self.manager.adjustUsageWithAdmission(.lsm_in_memory_state, &self.usage, 0, port) else try self.manager.adjustUsage(.lsm_in_memory_state, &self.usage, 0);
            self.observer_prepared = false;
        }
        if (self.account) |account| {
            if (original) |port| try port.checkpoint();
            // Accounting tokens can outlive payloads. Their generic drops must
            // not become the last header free outside admitted cleanup either.
            if (account.refs.load(.acquire) != 1) return false;
            self.account = null;
            account.release();
        }
        if (self.lifetime) |lifetime| {
            if (!try lifetime.advance(original)) return false;
            self.lifetime = null;
        }
        if (original) |port| try self.quota_owner.reconcileWithAdmission(port) else self.quota_owner.reconcileJoined();
        self.complete = true;
        return true;
    }

    /// Logical release alone leaves a zero-count registry row funded by the
    /// original quota. Forget that row before destroying the payload path;
    /// another live pin makes forget a safe no-op and retains the original
    /// funder's host until its own final peer performs this same operation.
    fn releasePin(raw: *anyopaque, issuer: *anyopaque, run: *Run, scope: ?admission.Port) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        var trace: admission.RejectionTrace = .{};
        const original = trace.bind(scope);
        try self.pin.resume_fn(self.pin.context, issuer, run, original);
        const path = run.path orelse return;
        if (self.forgetting_stale) {
            try self.forgetting.?.retireWithAdmission(original);
            self.forgetting = null;
            self.forgetting_stale = false;
        }
        if (self.forgetting == null) self.forgetting = Backend.prepareRunSnapshotRefOperation(path, .forget);
        const operation = &self.forgetting.?;
        if (!operation.committed) {
            prepareForget(operation, original) catch |cause| {
                if (trace.failure == null and cause == error.CompactionPlanningStale) self.forgetting_stale = true;
                return cause;
            };
        }
        try operation.retireWithAdmission(original);
        self.forgetting = null;
    }
    fn prepareForget(operation: *@import("run_pin_registry.zig").Operation, original: ?admission.Port) !void {
        _ = try operation.planWithAdmission(original);
        try operation.resumeWithAdmission(original);
        try operation.publishWithAdmission(original);
    }

    fn destroy(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.complete and self.preparation == null and self.lease == null and self.physical == null and self.forgetting == null and self.account == null and self.lifetime == null and !self.observer_prepared and self.usage == 0);
        const allocator = self.allocator;
        const retained = self.quota_owner;
        const host = self.host.lifetime;
        allocator.destroy(self);
        // Neither metadata-quota retention nor source retirement substitutes
        // for the exact allocator/manager host retention across this free.
        retained.releaseBorrow();
        host.release_joined(host.context);
    }
};
