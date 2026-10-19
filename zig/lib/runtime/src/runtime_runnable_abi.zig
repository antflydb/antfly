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

//! Process-local execution ownership across independently compiled archives.
//! Tokens are single-worker capsules. I/O, cancellation and clock inputs remain
//! borrowed through finish; parallel children need independent tokens.
const std = @import("std");
const native = @import("runtime_native_abi.zig");
const errors = @import("runtime_error_abi.zig");
const http = @import("runtime_http_abi.zig");

pub const abi_version: u32 = 15;
/// Synchronous, non-yielding restriction on the original CPU-return wait.
/// Borrowed only until enterChecked returns; never retained by the owner.
/// Rejection stops that execution and does not retire physical ownership.
pub const Reentry = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    context: *anyopaque,
    check: *const fn (*anyopaque) callconv(.c) errors.Status,
    pub fn validate(self: *const Reentry) !void {
        if (self.version != abi_version or self.struct_size != @sizeOf(Reentry)) return error.UnsupportedVersion;
    }
    pub fn checkpoint(self: *const Reentry) !void {
        try self.validate();
        const status = self.check(self.context);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }
};

/// Borrowed trusted resource accounting, copied by value before queuing. The
/// context and its physical resources outlive owning finish and every join.
/// Callbacks hold no partial bundle on refusal and never wait, allocate,
/// reclaim, perform IO or reenter the executor. Probe retains no capacity.
/// Integer results are checked at the scheduler boundary before interpretation.
pub const ResourceBundle = extern struct {
    pub const version_number: u32 = 1;
    pub const Probe = enum(c_int) { ready, wait, unknown, too_large, unsupported, mismatch };
    pub const Acquire = enum(c_int) { acquired, wait, too_large, unsupported, mismatch };
    version: u32 = version_number,
    struct_size: u32 = @sizeOf(@This()),
    context: *anyopaque,
    probe: *const fn (*anyopaque) callconv(.c) c_int,
    acquire: *const fn (*anyopaque) callconv(.c) c_int,

    pub fn read(raw: *const ResourceBundle) !ResourceBundle {
        if (raw.version != version_number or raw.struct_size != @sizeOf(ResourceBundle)) return error.UnsupportedVersion;
        return raw.*;
    }
};

pub const Class = enum(c_int) { query, write, inference, background, control };
pub const Wait = enum(c_int) { resource, output };

pub const Start = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    io: *const native.IoBorrow,
    clock_io: ?*const native.IoBorrow = null,
    deadline_ns: u64 = 0,
    has_deadline: u8 = 0,
    native_deadline: u8 = 0,
    /// Initial acquisition only; published reentry keeps bounded fair waits.
    allow_wait: u8 = 1,
    /// Trusted transport cleanup, independent of the expired foreground root.
    cleanup: u8 = 0,
    reserved: [4]u8 = @splat(0),
    cancellation: http.CancellationView = .{},
    class: c_int,
    slots: usize = 1,
    retained_bytes: u64 = 0,
    out_handle: *?*anyopaque,
    resources: ?*const ResourceBundle = null,

    pub fn validate(self: *const Start) !void {
        if (self.version != abi_version or self.struct_size != @sizeOf(Start)) return error.UnsupportedVersion;
        if (self.has_deadline > 1 or self.native_deadline > 1 or self.allow_wait > 1 or self.cleanup > 1 or !std.mem.allEqual(u8, &self.reserved, 0)) return error.InvalidArgument;
        if (self.native_deadline != 0 and self.clock_io != null) return error.InvalidArgument;
        _ = std.enums.fromInt(Class, self.class) orelse return error.InvalidArgument;
        if (self.slots == 0) return error.InvalidRunnableDemand;
        if (self.resources) |resource| {
            if (self.class == @backingInt(Class.control) or self.cleanup != 0) return error.InvalidArgument;
            _ = try ResourceBundle.read(resource);
        }
        if (self.cleanup != 0 and (self.class != @backingInt(Class.control) or self.slots != 1 or self.allow_wait != 0 or
            self.has_deadline == 0 or self.cancellation.context != null or self.cancellation.is_cancelled != null)) return error.InvalidArgument;
    }
};

/// Trusted completion executor, borrowed through the owning finish. Its finite
/// deadline is independent of foreground cancellation. Reserve before any
/// potentially durable acceptance; activate only after the owner has arranged
/// task and resource lifetime through completion or authoritative transfer.
pub const Completion = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    io: *const native.IoBorrow,
    clock_io: ?*const native.IoBorrow = null,
    deadline_ns: u64,
    native_deadline: u8 = 0,
    reserved: [7]u8 = @splat(0),

    pub fn validate(self: *const Completion) !void {
        // Inspect the prefix before accessing fields from a foreign layout.
        if (self.version != abi_version or self.struct_size != @sizeOf(Completion)) return error.UnsupportedVersion;
        if (self.native_deadline > 1 or !std.mem.allEqual(u8, &self.reserved, 0)) return error.InvalidArgument;
        if (self.native_deadline != 0 and self.clock_io != null) return error.InvalidArgument;
    }
};

/// Trusted physical cleanup fence, reserved before resource publication.
/// It may activate after foreground expiry/close and conveys no write authority.
pub const Cleanup = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    io: *const native.IoBorrow,
    clock_io: ?*const native.IoBorrow = null,
    deadline_ns: u64,
    native_deadline: u8 = 0,
    /// When set, deadline_ns is a finite duration reserved before publication,
    /// started once on cleanup activation in the selected owner clock.
    activation_budget: u8 = 0,
    reserved: [6]u8 = @splat(0),

    pub fn validate(self: *const Cleanup) !void {
        // Inspect the prefix before accessing fields from a foreign layout.
        if (self.version != abi_version or self.struct_size != @sizeOf(Cleanup)) return error.UnsupportedVersion;
        if (self.native_deadline > 1 or self.activation_budget > 1 or !std.mem.allEqual(u8, &self.reserved, 0)) return error.InvalidArgument;
        if (self.native_deadline != 0 and self.clock_io != null) return error.InvalidArgument;
        if (self.activation_budget != 0 and self.deadline_ns == 0) return error.InvalidArgument;
    }
};

/// Prepublication child ownership. The independent worker IO is retained
/// through physical join and finish. Root clock/cancellation inputs remain
/// borrowed through every child finish; no new query authority is accepted.
pub const ChildStart = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    io: *const native.IoBorrow,
    retained_bytes: u64 = 0,
    out_child: *Child,
    pub fn validate(self: *const ChildStart) !void {
        if (self.version != abi_version or self.struct_size != @sizeOf(ChildStart)) return error.UnsupportedVersion;
    }
};

/// Trusted prepared-stage CPU eligibility. This carries no fresh clock,
/// cancellation, memory or durable authority. Disable uses a zero work bound.
pub const CpuBorrow = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    enabled: u8,
    reserved: [7]u8 = @splat(0),
    max_non_yield_ns: u64 = 0,
    pub fn validate(self: *const CpuBorrow) !void {
        if (self.version != abi_version or self.struct_size != @sizeOf(CpuBorrow)) return error.UnsupportedVersion;
        if (self.enabled > 1 or !std.mem.allEqual(u8, &self.reserved, 0) or
            (self.enabled == 0 and self.max_non_yield_ns != 0) or
            (self.enabled == 1 and self.max_non_yield_ns == 0)) return error.InvalidArgument;
    }
};

pub const Owner = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    context: *anyopaque,
    retain: *const fn (*anyopaque) callconv(.c) errors.Status,
    release: *const fn (*anyopaque) callconv(.c) void,
    start: *const fn (*anyopaque, *const Start) callconv(.c) errors.Status,
    checkpoint: *const fn (*anyopaque) callconv(.c) errors.Status,
    park: *const fn (*anyopaque, c_int) callconv(.c) errors.Status,
    enter: *const fn (*anyopaque, c_int) callconv(.c) errors.Status,
    finish: *const fn (*anyopaque) callconv(.c) void,
    /// Optional dynamic CPU team capability. All prior children must join
    /// before resizing. Absent support fails explicitly without new admission.
    resize_team: ?*const fn (*anyopaque, usize) callconv(.c) errors.Status = null,
    reserve_completion: ?*const fn (*anyopaque, *const Completion) callconv(.c) errors.Status = null,
    activate_completion: ?*const fn (*anyopaque) callconv(.c) errors.Status = null,
    reserve_cleanup: ?*const fn (*anyopaque, *const Cleanup) callconv(.c) errors.Status = null,
    activate_cleanup: ?*const fn (*anyopaque, *usize) callconv(.c) errors.Status = null,
    /// Remaining time in the already activated completion's original clock.
    /// Observing this budget conveys no admission or fresh deadline authority.
    completion_remaining_ns: ?*const fn (*anyopaque, *u64) callconv(.c) errors.Status = null,
    reserve_child: ?*const fn (*anyopaque, *const ChildStart) callconv(.c) errors.Status = null,
    activate_child: ?*const fn (*anyopaque) callconv(.c) errors.Status = null,
    /// May run on a physical borrower while this capsule is parked. It checks
    /// retained cancellation/deadline/shutdown state without acquiring CPU.
    check_lifetime: ?*const fn (*anyopaque) callconv(.c) errors.Status = null,
    /// Observe the retained current phase without acquiring CPU. The original
    /// parent class in a borrow remains the authority for returning a transfer.
    current_class: ?*const fn (*anyopaque, *c_int) callconv(.c) errors.Status = null,
    enter_checked: ?*const fn (*anyopaque, c_int, *const Reentry) callconv(.c) errors.Status = null,
    cpu_borrow: ?*const fn (*anyopaque, *const CpuBorrow) callconv(.c) errors.Status = null,
    request_resources: ?*const fn (*anyopaque, *const ResourceBundle) callconv(.c) errors.Status = null,

    /// Resource-capable owners advertise both initial and parked grants through
    /// request_resources. Require that capability before invoking start, so an
    /// owner that only supports CPU cannot silently ignore the resource field.
    pub fn startWithResources(self: Owner, options: *const Start) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        try options.validate();
        if (options.resources == null) return error.InvalidArgument;
        if (self.request_resources == null) return error.UnsupportedOperation;
        const status = self.start(self.context, options);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn requestResources(self: Owner, handle: *anyopaque, resources: *const ResourceBundle) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        const value = try ResourceBundle.read(resources);
        const callback = self.request_resources orelse return error.UnsupportedOperation;
        const status = callback(handle, &value);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn setCpuBorrow(self: Owner, handle: *anyopaque, options: *const CpuBorrow) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        try options.validate();
        const callback = self.cpu_borrow orelse return error.UnsupportedOperation;
        const status = callback(handle, options);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn checkLifetime(self: Owner, handle: *anyopaque) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        const callback = self.check_lifetime orelse return error.UnsupportedOperation;
        const status = callback(handle);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn reserveCompletion(self: Owner, handle: *anyopaque, options: *const Completion) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        try options.validate();
        const callback = self.reserve_completion orelse return error.UnsupportedOperation;
        const status = callback(handle, options);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn activateCompletion(self: Owner, handle: *anyopaque) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        const callback = self.activate_completion orelse return error.UnsupportedOperation;
        const status = callback(handle);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn completionRemainingNs(self: Owner, handle: *anyopaque) !u64 {
        if (!self.valid()) return error.UnsupportedVersion;
        const callback = self.completion_remaining_ns orelse return error.UnsupportedOperation;
        var remaining: u64 = 0;
        const status = callback(handle, &remaining);
        if (!status.isOk()) return errors.errorFromStatus(status);
        if (remaining == 0) return error.DeadlineExceeded;
        return remaining;
    }

    pub fn reserveCleanup(self: Owner, handle: *anyopaque, options: *const Cleanup) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        try options.validate();
        const callback = self.reserve_cleanup orelse return error.UnsupportedOperation;
        const status = callback(handle, options);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    /// Return the actual admitted cleanup demand after a successful transfer.
    /// The receiver writes it only after acquiring the cleanup CPU grant.
    pub fn activateCleanup(self: Owner, handle: *anyopaque) !usize {
        if (!self.valid()) return error.UnsupportedVersion;
        const callback = self.activate_cleanup orelse return error.UnsupportedOperation;
        var slots: usize = 0;
        const status = callback(handle, &slots);
        if (!status.isOk()) return errors.errorFromStatus(status);
        if (slots == 0) return error.InvalidRunnableDemand;
        return slots;
    }

    pub fn reserveChild(self: Owner, parent: *anyopaque, options: *const ChildStart) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        try options.validate();
        const callback = self.reserve_child orelse return error.UnsupportedOperation;
        const status = callback(parent, options);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }
    pub fn activateChild(self: Owner, child: *anyopaque) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        const callback = self.activate_child orelse return error.UnsupportedOperation;
        const status = callback(child);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn resizeTeam(self: Owner, handle: *anyopaque, slots: usize) !void {
        if (!self.valid()) return error.UnsupportedVersion;
        if (slots == 0) return error.InvalidRunnableDemand;
        const resize = self.resize_team orelse return error.UnsupportedOperation;
        const status = resize(handle, slots);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    /// Check the foreign prefix before copying any appended callbacks.
    pub fn read(raw: *const Owner) !Owner {
        if (raw.version != abi_version or raw.struct_size < @sizeOf(Owner)) return error.UnsupportedVersion;
        return raw.*;
    }

    pub fn valid(self: Owner) bool {
        return self.version == abi_version and self.struct_size >= @sizeOf(Owner);
    }
};

/// Stored consumer capability. The caller joins every consumer task before
/// releasing this lease; independently started capsules hold their own leases.
pub const Lease = struct {
    owner: Owner,

    pub fn initFrom(raw: *const Owner) !Lease {
        return init(try Owner.read(raw));
    }

    pub fn init(owner: Owner) !Lease {
        if (!owner.valid()) return error.UnsupportedVersion;
        const status = owner.retain(owner.context);
        if (!status.isOk()) return errors.errorFromStatus(status);
        return .{ .owner = owner };
    }

    pub fn deinit(self: *Lease) void {
        self.owner.release(self.owner.context);
        self.* = undefined;
    }
};

/// A synchronous child borrows the parent's single-worker grant. This value
/// cannot retire or retain its parent; it must not outlive the parent call or
/// be shared by parallel children. cpu_slots is the parent's admitted demand.
pub const Borrowed = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    context: *anyopaque,
    cpu_slots: usize,
    parent_class: c_int,
    checkpoint: *const fn (*anyopaque) callconv(.c) errors.Status,
    park: *const fn (*anyopaque, c_int) callconv(.c) errors.Status,
    /// The parent implementation validates its mandatory resumption fences
    /// before exposing child output to the parent phase.
    enter: *const fn (*anyopaque, c_int) callconv(.c) errors.Status,
    resize_team: ?*const fn (*anyopaque, usize) callconv(.c) errors.Status = null,
    reserve_completion: ?*const fn (*anyopaque, *const Completion) callconv(.c) errors.Status = null,
    activate_completion: ?*const fn (*anyopaque) callconv(.c) errors.Status = null,
    reserve_cleanup: ?*const fn (*anyopaque, *const Cleanup) callconv(.c) errors.Status = null,
    activate_cleanup: ?*const fn (*anyopaque, *usize) callconv(.c) errors.Status = null,
    /// Remaining time in the already activated completion's original clock.
    /// Observing this budget conveys no admission or fresh deadline authority.
    completion_remaining_ns: ?*const fn (*anyopaque, *u64) callconv(.c) errors.Status = null,
    reserve_child: ?*const fn (*anyopaque, *const ChildStart) callconv(.c) errors.Status = null,
    activate_child: ?*const fn (*anyopaque) callconv(.c) errors.Status = null,
    /// May run on a physical borrower while this capsule is parked. It checks
    /// retained cancellation/deadline/shutdown state without acquiring CPU.
    check_lifetime: ?*const fn (*anyopaque) callconv(.c) errors.Status = null,
    current_class: ?*const fn (*anyopaque, *c_int) callconv(.c) errors.Status = null,
    enter_checked: ?*const fn (*anyopaque, c_int, *const Reentry) callconv(.c) errors.Status = null,
    cpu_borrow: ?*const fn (*anyopaque, *const CpuBorrow) callconv(.c) errors.Status = null,
    request_resources: ?*const fn (*anyopaque, *const ResourceBundle) callconv(.c) errors.Status = null,

    pub fn requestResources(self: Borrowed, resources: *const ResourceBundle) !void {
        try self.validate();
        const value = try ResourceBundle.read(resources);
        const callback = self.request_resources orelse return error.UnsupportedOperation;
        const status = callback(self.context, &value);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn setCpuBorrow(self: Borrowed, options: *const CpuBorrow) !void {
        try self.validate();
        try options.validate();
        const callback = self.cpu_borrow orelse return error.UnsupportedOperation;
        const status = callback(self.context, options);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn enterChecked(self: Borrowed, class: Class, restriction: *const Reentry) !void {
        try self.validate();
        try restriction.validate();
        const callback = self.enter_checked orelse return error.ExecutionWaitUnsupported;
        const status = callback(self.context, @backingInt(class), restriction);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn currentClass(self: Borrowed) !Class {
        try self.validate();
        const callback = self.current_class orelse return error.UnsupportedOperation;
        var class: c_int = undefined;
        const status = callback(self.context, &class);
        if (!status.isOk()) return errors.errorFromStatus(status);
        return std.enums.fromInt(Class, class) orelse error.InvalidArgument;
    }

    pub fn checkLifetime(self: Borrowed) !void {
        try self.validate();
        const callback = self.check_lifetime orelse return error.UnsupportedOperation;
        const status = callback(self.context);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn reserveChild(self: Borrowed, options: *const ChildStart) !void {
        try self.validate();
        try options.validate();
        const callback = self.reserve_child orelse return error.UnsupportedOperation;
        const status = callback(self.context, options);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }
    pub fn activateChild(self: Borrowed) !void {
        try self.validate();
        const callback = self.activate_child orelse return error.UnsupportedOperation;
        const status = callback(self.context);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn reserveCompletion(self: Borrowed, options: *const Completion) !void {
        try self.validate();
        try options.validate();
        const callback = self.reserve_completion orelse return error.UnsupportedOperation;
        const status = callback(self.context, options);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn activateCompletion(self: Borrowed) !void {
        try self.validate();
        const callback = self.activate_completion orelse return error.UnsupportedOperation;
        const status = callback(self.context);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    pub fn completionRemainingNs(self: Borrowed) !u64 {
        try self.validate();
        const callback = self.completion_remaining_ns orelse return error.UnsupportedOperation;
        var remaining: u64 = 0;
        const status = callback(self.context, &remaining);
        if (!status.isOk()) return errors.errorFromStatus(status);
        if (remaining == 0) return error.DeadlineExceeded;
        return remaining;
    }

    pub fn reserveCleanup(self: Borrowed, options: *const Cleanup) !void {
        try self.validate();
        try options.validate();
        const callback = self.reserve_cleanup orelse return error.UnsupportedOperation;
        const status = callback(self.context, options);
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    /// Quiesce every previous borrower before transferring phases. Copies do
    /// not track this transfer; the activated cleanup callback receives this
    /// updated snapshot, including the receiver's actual admitted CPU demand.
    pub fn activateCleanup(self: *Borrowed) !void {
        try self.validate();
        const callback = self.activate_cleanup orelse return error.UnsupportedOperation;
        var slots: usize = 0;
        const status = callback(self.context, &slots);
        if (!status.isOk()) return errors.errorFromStatus(status);
        if (slots == 0) return error.InvalidRunnableDemand;
        self.cpu_slots = slots;
    }

    /// Update this snapshot only after the parent has admitted the new team.
    /// Quiesce every prior borrower first; copies do not track later resizing.
    pub fn resizeTeam(self: *Borrowed, slots: usize) !void {
        try self.validate();
        if (slots == 0) return error.InvalidRunnableDemand;
        const resize = self.resize_team orelse return error.UnsupportedOperation;
        const status = resize(self.context, slots);
        if (!status.isOk()) return errors.errorFromStatus(status);
        self.cpu_slots = slots;
    }

    pub fn read(raw: *const Borrowed) !Borrowed {
        if (raw.version != abi_version or raw.struct_size != @sizeOf(Borrowed)) return error.UnsupportedVersion;
        const value = raw.*;
        try value.validate();
        return value;
    }

    pub fn validate(self: Borrowed) !void {
        if (self.version != abi_version or self.struct_size != @sizeOf(Borrowed)) return error.UnsupportedVersion;
        if (self.cpu_slots == 0) return error.InvalidRunnableDemand;
        _ = std.enums.fromInt(Class, self.parent_class) orelse return error.InvalidArgument;
    }

    pub fn fromOwner(owner: Owner, handle: *anyopaque, slots: usize, class: Class) !Borrowed {
        if (!owner.valid()) return error.UnsupportedVersion;
        const borrow: Borrowed = .{
            .context = handle,
            .cpu_slots = slots,
            .parent_class = @backingInt(class),
            .checkpoint = owner.checkpoint,
            .park = owner.park,
            .enter = owner.enter,
            .resize_team = owner.resize_team,
            .reserve_completion = owner.reserve_completion,
            .activate_completion = owner.activate_completion,
            .reserve_cleanup = owner.reserve_cleanup,
            .activate_cleanup = owner.activate_cleanup,
            .completion_remaining_ns = owner.completion_remaining_ns,
            .reserve_child = owner.reserve_child,
            .activate_child = owner.activate_child,
            .check_lifetime = owner.check_lifetime,
            .current_class = owner.current_class,
            .enter_checked = owner.enter_checked,
            .cpu_borrow = owner.cpu_borrow,
            .request_resources = owner.request_resources,
        };
        try borrow.validate();
        return borrow;
    }
};

/// Move-only owned child token. The parent and its foreign IO/clock/cancel
/// inputs remain retained through this token's physical join and deinit.
pub const Child = extern struct {
    version: u32 = abi_version,
    struct_size: u32 = @sizeOf(@This()),
    borrow: Borrowed,
    finish: *const fn (*anyopaque) callconv(.c) void,
    active: u8 = 1,
    reserved: [7]u8 = @splat(0),
    pub fn validate(self: *const Child) !void {
        if (self.version != abi_version or self.struct_size != @sizeOf(Child)) return error.UnsupportedVersion;
        if (self.active != 1 or !std.mem.allEqual(u8, &self.reserved, 0)) return error.InvalidRunnableTransition;
        try self.borrow.validate();
    }
    pub fn activate(self: *Child) !void {
        try self.validate();
        try self.borrow.activateChild();
    }
    /// Call only after task publication has been abandoned or every physical
    /// consumer has joined. CPU stop/timeout is not proof of physical finish.
    pub fn deinit(self: *Child) void {
        if (self.active == 0) return;
        self.validate() catch @panic("invalid owned runnable child");
        self.active = 0;
        self.finish(self.borrow.context);
    }
};

/// Invocation-owned fanout under one retained execution. Demand includes the
/// coordinator plus every concurrently running child. The allocator must be a
/// thread-safe, bounded owner which remains alive through all joined children.
/// This is local trusted state, never a serialized or independently owned root.
pub const Team = struct {
    borrow: Borrowed,
    maximum_slots: usize,
    allocator: std.mem.Allocator,
    state: enum { fresh, active, joined } = .fresh,

    pub fn reserve(self: *Team, child_tasks: usize) !void {
        // A proven aborted attempt may retry after all its children joined.
        // The same parent fences and retained reservation still govern it.
        if (self.state == .active) return error.InvalidRunnableTransition;
        try self.borrow.validate();
        if (self.maximum_slots == 0 or self.borrow.cpu_slots > self.maximum_slots) return error.InvalidRunnableDemand;
        // Never shrink the caller's existing grant during admission. Accepted
        // work retains its admitted team until the execution's owning finish.
        const slots = @max(self.borrow.cpu_slots, @min(self.maximum_slots, child_tasks +| 1));
        try self.borrow.resizeTeam(slots);
        self.state = .active;
    }

    /// Reserve independently scheduled participant children rather than a
    /// simultaneous whole-team grant. Call only before publication or after
    /// every prior child has physically joined. The allocator and original
    /// execution authority remain borrowed through all child retirement.
    pub fn reserveIndependent(self: *Team) !void {
        if (self.state == .active) return error.InvalidRunnableTransition;
        try self.borrow.validate();
        if (self.maximum_slots == 0 or self.borrow.cpu_slots > self.maximum_slots) return error.InvalidRunnableDemand;
        if (self.borrow.reserve_child == null or self.borrow.check_lifetime == null or self.borrow.current_class == null)
            return error.UnsupportedOperation;
        try self.borrow.checkLifetime();
        // Protected terminal teams retain their single control grant; separate
        // control children would consume the health reserve independently.
        if (try self.borrow.currentClass() == .control) return error.InvalidRunnableDemand;
        try self.borrow.resizeTeam(1);
        self.state = .active;
    }

    pub fn childCapacity(self: *const Team) usize {
        std.debug.assert(self.state == .active);
        return self.borrow.cpu_slots - 1;
    }

    /// Every child must already be joined. Do not reacquire CPU under the
    /// expired foreground fence or discard an accepted completion obligation.
    /// The owning Execution retains the team grant until its actual finish.
    pub fn finish(self: *Team) void {
        std.debug.assert(self.state == .active);
        self.state = .joined;
    }
};

/// Transfer the existing operation to a synchronous child phase, then return
/// it through the parent's resumption callback. Failure stops the operation;
/// its owning caller still retires it. No fresh admission or deadline is made.
pub const Transfer = struct {
    borrow: Borrowed,
    active: bool = true,

    pub fn initFrom(raw: *const Borrowed, class: Class) !Transfer {
        return init(try Borrowed.read(raw), class);
    }

    pub fn init(borrow: Borrowed, class: Class) !Transfer {
        try borrow.validate();
        try checked(borrow.checkpoint(borrow.context));
        try checked(borrow.park(borrow.context, @backingInt(Wait.resource)));
        try checked(borrow.enter(borrow.context, @backingInt(class)));
        return .{ .borrow = borrow };
    }

    pub fn checkpoint(self: *const Transfer) !void {
        if (!self.active) return error.InvalidRunnableTransition;
        try checked(self.borrow.checkpoint(self.borrow.context));
    }

    pub fn returnToParent(self: *Transfer) !void {
        if (!self.active) return error.InvalidRunnableTransition;
        self.active = false;
        try checked(self.borrow.park(self.borrow.context, @backingInt(Wait.resource)));
        try checked(self.borrow.enter(self.borrow.context, self.borrow.parent_class));
    }

    fn checked(status: errors.Status) !void {
        if (!status.isOk()) return errors.errorFromStatus(status);
    }
};

test "runnable execution ABI rejects malformed inputs before borrowing executor state" {
    var handle: ?*anyopaque = null;
    var start: Start = .{ .io = @ptrFromInt(8), .class = @backingInt(Class.inference), .out_handle = &handle };
    try start.validate();
    start.version = 1;
    try std.testing.expectError(error.UnsupportedVersion, start.validate());
    start.version = abi_version;
    start.allow_wait = 2;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.allow_wait = 0;
    try start.validate();
    start.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, start.validate());
    start.struct_size = @sizeOf(Start);
    start.class = -1;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.class = @backingInt(Class.background);
    start.slots = 0;
    try std.testing.expectError(error.InvalidRunnableDemand, start.validate());
    start.slots = 1;
    start.has_deadline = 2;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.has_deadline = 1;
    start.native_deadline = 1;
    start.clock_io = @ptrFromInt(8);
    try std.testing.expectError(error.InvalidArgument, start.validate());
    try std.testing.expect(handle == null);
}

test "runnable execution ABI retains C layout and validates the owner prefix" {
    try std.testing.expectEqual(.@"extern", @typeInfo(Start).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(Owner).@"struct".layout);
    var owner: Owner = undefined;
    owner.version = abi_version;
    owner.struct_size = @sizeOf(Owner);
    try std.testing.expect(owner.valid());
    owner.struct_size -= 1;
    try std.testing.expect(!owner.valid());
    owner.struct_size = @sizeOf(Owner);
    owner.version += 1;
    try std.testing.expect(!owner.valid());
}

test "runnable execution borrowed ABI validates demand and headers before calling a parent" {
    try std.testing.expectEqual(.@"extern", @typeInfo(Borrowed).@"struct".layout);
    var loan: Borrowed = undefined;
    loan.version = abi_version;
    loan.struct_size = @sizeOf(Borrowed);
    loan.cpu_slots = 1;
    loan.parent_class = @backingInt(Class.query);
    try loan.validate();
    loan.version += 1;
    try std.testing.expectError(error.UnsupportedVersion, loan.validate());
    loan.version = abi_version;
    loan.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, loan.validate());
    loan.struct_size += 1;
    loan.cpu_slots = 0;
    try std.testing.expectError(error.InvalidRunnableDemand, loan.validate());
    loan.cpu_slots = 1;
    loan.parent_class = -1;
    try std.testing.expectError(error.InvalidArgument, loan.validate());
}

test "runnable cleanup ABI requires a bounded independent single-slot control root" {
    var handle: ?*anyopaque = null;
    var start: Start = .{ .io = @ptrFromInt(8), .class = @backingInt(Class.control), .cleanup = 1, .allow_wait = 0, .has_deadline = 1, .deadline_ns = 1, .out_handle = &handle };
    try start.validate();
    start.version = 2;
    try std.testing.expectError(error.UnsupportedVersion, start.validate());
    start.version = abi_version;
    start.cleanup = 2;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.cleanup = 1;
    start.allow_wait = 1;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.allow_wait = 0;
    start.has_deadline = 0;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.has_deadline = 1;
    start.slots = 2;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.slots = 1;
    start.class = @backingInt(Class.query);
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.class = @backingInt(Class.control);
    start.cancellation.context = @ptrFromInt(8);
    try std.testing.expectError(error.InvalidArgument, start.validate());
    start.cancellation = .{};
    start.reserved[0] = 1;
    try std.testing.expectError(error.InvalidArgument, start.validate());
    try std.testing.expect(handle == null);
}

test "runnable team ABI rejects legacy and malformed demand before callbacks and preserves failed snapshots" {
    const Fake = struct {
        calls: usize = 0,
        deny: bool = false,
        fn resize(raw: *anyopaque, slots: usize) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.deny) return errors.statusFromError(error.ResourceTemporarilyUnavailable);
            std.debug.assert(slots > 0);
            return .ok;
        }
        fn check(_: *anyopaque) callconv(.c) errors.Status {
            return .ok;
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) errors.Status {
            return .ok;
        }
    };
    var fake: Fake = .{};
    var loan: Borrowed = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(Class.write), .checkpoint = Fake.check, .park = Fake.transition, .enter = Fake.transition, .resize_team = Fake.resize };
    loan.version = 3;
    try std.testing.expectError(error.UnsupportedVersion, loan.resizeTeam(2));
    loan.version = abi_version;
    loan.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, loan.resizeTeam(2));
    loan.struct_size = @sizeOf(Borrowed);
    try std.testing.expectError(error.InvalidRunnableDemand, loan.resizeTeam(0));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try loan.resizeTeam(2);
    try std.testing.expectEqual(@as(usize, 2), loan.cpu_slots);
    fake.deny = true;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, loan.resizeTeam(3));
    try std.testing.expectEqual(@as(usize, 2), loan.cpu_slots);
    loan.resize_team = null;
    try std.testing.expectError(error.UnsupportedOperation, loan.resizeTeam(1));
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
}

test "runnable team ABI foreign readers reject header-only legacy and truncated values" {
    const Header = extern struct { version: u32, struct_size: u32 };
    var owner_header: Header align(@alignOf(Owner)) = .{ .version = 3, .struct_size = @sizeOf(Owner) - @sizeOf(usize) };
    const owner: *const Owner = @ptrCast(&owner_header);
    try std.testing.expectError(error.UnsupportedVersion, Owner.read(owner));
    try std.testing.expectError(error.UnsupportedVersion, Lease.initFrom(owner));
    owner_header.version = abi_version;
    owner_header.struct_size = @sizeOf(Header);
    try std.testing.expectError(error.UnsupportedVersion, Owner.read(owner));
    try std.testing.expectError(error.UnsupportedVersion, Lease.initFrom(owner));
    var borrow_header: Header align(@alignOf(Borrowed)) = .{ .version = 3, .struct_size = @sizeOf(Borrowed) - @sizeOf(usize) };
    const borrow: *const Borrowed = @ptrCast(&borrow_header);
    try std.testing.expectError(error.UnsupportedVersion, Borrowed.read(borrow));
    try std.testing.expectError(error.UnsupportedVersion, Transfer.initFrom(borrow, .inference));
    borrow_header.version = abi_version;
    borrow_header.struct_size = @sizeOf(Header);
    try std.testing.expectError(error.UnsupportedVersion, Borrowed.read(borrow));
    try std.testing.expectError(error.UnsupportedVersion, Transfer.initFrom(borrow, .inference));
}

test "completion ABI rejects old prefixes and malformed options before callbacks" {
    const Fake = struct {
        calls: usize = 0,
        fn check(_: *anyopaque) callconv(.c) errors.Status {
            return .ok;
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) errors.Status {
            return .ok;
        }
        fn reserve(raw: *anyopaque, _: *const Completion) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return errors.statusFromError(error.DeadlineExceeded);
        }
        fn activate(raw: *anyopaque) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return errors.statusFromError(error.Canceled);
        }
    };
    var fake: Fake = .{};
    var borrowed: Borrowed = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(Class.write), .checkpoint = Fake.check, .park = Fake.transition, .enter = Fake.transition };
    var options: Completion = .{ .io = @ptrFromInt(8), .deadline_ns = 10 };
    try std.testing.expectError(error.UnsupportedOperation, borrowed.reserveCompletion(&options));
    try std.testing.expectError(error.UnsupportedOperation, borrowed.activateCompletion());
    borrowed.reserve_completion = Fake.reserve;
    borrowed.activate_completion = Fake.activate;
    options.version = 4;
    try std.testing.expectError(error.UnsupportedVersion, borrowed.reserveCompletion(&options));
    options.version = abi_version;
    options.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrowed.reserveCompletion(&options));
    options.struct_size = @sizeOf(Completion);
    options.native_deadline = 2;
    try std.testing.expectError(error.InvalidArgument, borrowed.reserveCompletion(&options));
    options.native_deadline = 1;
    options.clock_io = @ptrFromInt(8);
    try std.testing.expectError(error.InvalidArgument, borrowed.reserveCompletion(&options));
    options.clock_io = null;
    options.reserved[0] = 1;
    try std.testing.expectError(error.InvalidArgument, borrowed.reserveCompletion(&options));
    options.reserved = @splat(0);
    borrowed.version = 4;
    try std.testing.expectError(error.UnsupportedVersion, borrowed.reserveCompletion(&options));
    try std.testing.expectError(error.UnsupportedVersion, borrowed.activateCompletion());
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    borrowed.version = abi_version;
    try std.testing.expectError(error.DeadlineExceeded, borrowed.reserveCompletion(&options));
    try std.testing.expectError(error.Canceled, borrowed.activateCompletion());
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    const Prefix = extern struct { version: u32, struct_size: u32 };
    var old: Prefix align(@alignOf(Owner)) = .{ .version = 4, .struct_size = @sizeOf(Prefix) };
    try std.testing.expectError(error.UnsupportedVersion, Owner.read(@ptrCast(&old)));
    try std.testing.expectError(error.UnsupportedVersion, Borrowed.read(@ptrCast(&old)));
    try std.testing.expectError(error.UnsupportedVersion, (@as(*const Completion, @ptrCast(&old))).validate());
}

test "completion ABI remaining budget rejects legacy headers and absent authority" {
    const Fake = struct {
        calls: usize = 0,
        remaining: u64 = 17,
        fn check(_: *anyopaque) callconv(.c) errors.Status {
            return .ok;
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) errors.Status {
            return .ok;
        }
        fn budget(raw: *anyopaque, out: *u64) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            out.* = self.remaining;
            return .ok;
        }
    };
    var fake: Fake = .{};
    var borrowed: Borrowed = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(Class.write), .checkpoint = Fake.check, .park = Fake.transition, .enter = Fake.transition };
    try std.testing.expectError(error.UnsupportedOperation, borrowed.completionRemainingNs());
    borrowed.completion_remaining_ns = Fake.budget;
    borrowed.version = 6;
    try std.testing.expectError(error.UnsupportedVersion, borrowed.completionRemainingNs());
    borrowed.version = abi_version;
    borrowed.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrowed.completionRemainingNs());
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    borrowed.struct_size = @sizeOf(Borrowed);
    try std.testing.expectEqual(@as(u64, 17), try borrowed.completionRemainingNs());
    fake.remaining = 0;
    try std.testing.expectError(error.DeadlineExceeded, borrowed.completionRemainingNs());
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
}

test "completion ABI cleanup activation budget rejects legacy semantics before dispatch" {
    const Fake = struct {
        calls: usize = 0,
        fn check(_: *anyopaque) callconv(.c) errors.Status {
            return .ok;
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) errors.Status {
            return .ok;
        }
        fn reserve(raw: *anyopaque, options: *const Cleanup) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(options.activation_budget == 1 and options.deadline_ns == 10);
            self.calls += 1;
            return .ok;
        }
    };
    var fake: Fake = .{};
    const borrowed: Borrowed = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(Class.query), .checkpoint = Fake.check, .park = Fake.transition, .enter = Fake.transition, .reserve_cleanup = Fake.reserve };
    var options: Cleanup = .{ .io = @ptrFromInt(8), .deadline_ns = 10, .activation_budget = 1 };
    options.version = 7;
    try std.testing.expectError(error.UnsupportedVersion, borrowed.reserveCleanup(&options));
    options.version = abi_version;
    options.activation_budget = 2;
    try std.testing.expectError(error.InvalidArgument, borrowed.reserveCleanup(&options));
    options.activation_budget = 1;
    options.deadline_ns = 0;
    try std.testing.expectError(error.InvalidArgument, borrowed.reserveCleanup(&options));
    options.deadline_ns = 10;
    options.reserved[0] = 1;
    try std.testing.expectError(error.InvalidArgument, borrowed.reserveCleanup(&options));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    options.reserved = @splat(0);
    try borrowed.reserveCleanup(&options);
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
}

test "runnable child ABI rejects legacy prefixes before borrowing worker IO or callbacks" {
    const Header = extern struct { version: u32, struct_size: u32 };
    var header: Header align(@alignOf(ChildStart)) = .{ .version = 8, .struct_size = @sizeOf(ChildStart) };
    const start: *const ChildStart = @ptrCast(&header);
    try std.testing.expectError(error.UnsupportedVersion, start.validate());
    header.version = abi_version;
    header.struct_size = @sizeOf(Header);
    try std.testing.expectError(error.UnsupportedVersion, start.validate());
    var child_header: Header align(@alignOf(Child)) = .{ .version = 8, .struct_size = @sizeOf(Child) };
    const child: *const Child = @ptrCast(&child_header);
    try std.testing.expectError(error.UnsupportedVersion, child.validate());
    child_header.version = abi_version;
    child_header.struct_size = @sizeOf(Header);
    try std.testing.expectError(error.UnsupportedVersion, child.validate());
}

test "runnable lifetime ABI never substitutes CPU checkpoint for parked lifetime" {
    const Fake = struct {
        calls: usize = 0,
        failure: ?anyerror = null,
        fn lifetime(raw: *anyopaque) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return if (self.failure) |failure| errors.statusFromError(failure) else .ok;
        }
        fn checkpoint(_: *anyopaque) callconv(.c) errors.Status {
            return errors.statusFromError(error.InvalidRunnableTransition);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) errors.Status {
            return errors.statusFromError(error.InvalidRunnableTransition);
        }
    };
    var fake: Fake = .{};
    var borrow: Borrowed = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(Class.query), .checkpoint = Fake.checkpoint, .park = Fake.phase, .enter = Fake.phase };
    try std.testing.expectError(error.UnsupportedOperation, borrow.checkLifetime());
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    borrow.check_lifetime = Fake.lifetime;
    try borrow.checkLifetime();
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |failure| {
        fake.failure = failure;
        try std.testing.expectError(failure, borrow.checkLifetime());
    }
    try std.testing.expectEqual(@as(usize, 4), fake.calls);
    borrow.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.checkLifetime());
    borrow.version = abi_version;
    borrow.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.checkLifetime());
    try std.testing.expectEqual(@as(usize, 4), fake.calls);
}

test "runnable lifetime ABI observes current phase without replacing parent authority" {
    const Fake = struct {
        phase: c_int = @backingInt(Class.inference),
        calls: usize = 0,
        fn check(_: *anyopaque) callconv(.c) errors.Status {
            return errors.statusFromError(error.InvalidRunnableTransition);
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) errors.Status {
            return errors.statusFromError(error.InvalidRunnableTransition);
        }
        fn current(raw: *anyopaque, out: *c_int) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            out.* = self.phase;
            return .ok;
        }
    };
    var fake: Fake = .{};
    var borrowed: Borrowed = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(Class.query), .checkpoint = Fake.check, .park = Fake.transition, .enter = Fake.transition };
    try std.testing.expectError(error.UnsupportedOperation, borrowed.currentClass());
    borrowed.current_class = Fake.current;
    try std.testing.expectEqual(Class.inference, try borrowed.currentClass());
    try std.testing.expectEqual(@backingInt(Class.query), borrowed.parent_class);
    fake.phase = 999;
    try std.testing.expectError(error.InvalidArgument, borrowed.currentClass());
    borrowed.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrowed.currentClass());
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
}

test "runnable lifetime ABI cleanup transfer updates demand only on checked success" {
    const Fake = struct {
        slots: usize = 1,
        calls: usize = 0,
        failure: ?anyerror = null,
        fn activate(raw: *anyopaque, out: *usize) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            out.* = self.slots;
            return if (self.failure) |failure| errors.statusFromError(failure) else .ok;
        }
        fn check(_: *anyopaque) callconv(.c) errors.Status {
            return .ok;
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) errors.Status {
            return .ok;
        }
    };
    var fake: Fake = .{};
    var borrow: Borrowed = .{ .context = &fake, .cpu_slots = 3, .parent_class = @backingInt(Class.write), .checkpoint = Fake.check, .park = Fake.phase, .enter = Fake.phase, .activate_cleanup = Fake.activate };
    // ABI 12 has the same layout but a different callback signature.
    borrow.version = 12;
    try std.testing.expectError(error.UnsupportedVersion, borrow.activateCleanup());
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    borrow.version = abi_version;
    borrow.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.activateCleanup());
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    borrow.struct_size = @sizeOf(Borrowed);
    for ([_]anyerror{ error.DeadlineExceeded, error.InvalidRunnableTransition, error.SchedulerClosed }) |failure| {
        fake.failure = failure;
        try std.testing.expectError(failure, borrow.activateCleanup());
        try std.testing.expectEqual(@as(usize, 3), borrow.cpu_slots);
    }
    fake.failure = null;
    fake.slots = 0;
    try std.testing.expectError(error.InvalidRunnableDemand, borrow.activateCleanup());
    try std.testing.expectEqual(@as(usize, 3), borrow.cpu_slots);
    // Both isolated serial cleanup and legacy admitted teams report actual demand.
    for ([_]usize{ 1, 3 }) |slots| {
        fake.slots = slots;
        try borrow.activateCleanup();
        try std.testing.expectEqual(slots, borrow.cpu_slots);
        try std.testing.expectEqual(@backingInt(Class.write), borrow.parent_class);
    }
    borrow.activate_cleanup = null;
    try std.testing.expectError(error.UnsupportedOperation, borrow.activateCleanup());
    try std.testing.expectEqual(@as(usize, 6), fake.calls);
}

test "runnable CPU borrow ABI rejects malformed prefixes before calling capability" {
    const Fake = struct {
        calls: usize = 0,
        fn set(raw: *anyopaque, options: *const CpuBorrow) callconv(.c) errors.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            options.validate() catch |err| return errors.statusFromError(err);
            return errors.statusFromError(error.ResourceTemporarilyUnavailable);
        }
        fn check(_: *anyopaque) callconv(.c) errors.Status {
            return .ok;
        }
        fn enter(_: *anyopaque, _: c_int) callconv(.c) errors.Status {
            return .ok;
        }
    };
    var fake: Fake = .{};
    var borrow: Borrowed = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(Class.query), .checkpoint = Fake.check, .park = Fake.enter, .enter = Fake.enter, .cpu_borrow = Fake.set };
    const enabled: CpuBorrow = .{ .enabled = 1, .max_non_yield_ns = 1000 };
    var bad = enabled;
    bad.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.setCpuBorrow(&bad));
    bad = enabled;
    bad.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.setCpuBorrow(&bad));
    bad = enabled;
    bad.reserved[0] = 1;
    try std.testing.expectError(error.InvalidArgument, borrow.setCpuBorrow(&bad));
    bad = enabled;
    bad.max_non_yield_ns = 0;
    try std.testing.expectError(error.InvalidArgument, borrow.setCpuBorrow(&bad));
    bad = enabled;
    bad.enabled = 2;
    try std.testing.expectError(error.InvalidArgument, borrow.setCpuBorrow(&bad));
    bad = enabled;
    bad.enabled = 0;
    try std.testing.expectError(error.InvalidArgument, borrow.setCpuBorrow(&bad));
    borrow.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.setCpuBorrow(&enabled));
    borrow.version = abi_version;
    borrow.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, borrow.setCpuBorrow(&enabled));
    borrow.struct_size = @sizeOf(Borrowed);
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, borrow.setCpuBorrow(&enabled));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, borrow.setCpuBorrow(&.{ .enabled = 0 }));
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    borrow.cpu_borrow = null;
    try std.testing.expectError(error.UnsupportedOperation, borrow.setCpuBorrow(&enabled));
}
