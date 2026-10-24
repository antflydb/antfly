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

//! Independently admitted synchronous producer work after service retirement.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;
const Context = @import("antfly_inference_execution_context").RequestContext;
const Holders = @import("mutation_admission.zig").SpeculativePreparation;

/// Keep this slot stable until provider work and every internal consumer join.
/// It never publishes the parent token to a provider or creates fresh authority.
pub const Stage = struct {
    holders: ?*Holders = null,
    parent: ?abi.Borrowed = null,
    parent_class: abi.Class = .write,
    child: ?abi.Child = null,
    original: ?Context = null,
    active: bool = false,

    pub fn begin(self: *@This(), holders: *Holders, parent: ?abi.Borrowed, context: ?Context) !void {
        if (self.active or self.child != null) return error.InvalidRunnableTransition;
        try holders.validateRetired();
        self.* = .{ .original = context };
        if (context) |original| {
            if (original.runnable != null) return error.InvalidRunnableTransition;
            try original.check();
        }
        const supplied = parent orelse return;
        const borrowed = try supplied.producerParent();
        try borrowed.validate();
        // Legacy capabilities and an already granted whole team retain their
        // sequential grant. Shrinking a copied team view would invalidate peers.
        if (borrowed.cpu_slots != 1 or borrowed.reserve_child == null or borrowed.enter_checked == null or borrowed.current_class == null or borrowed.check_lifetime == null) return;
        const class = try borrowed.currentClass();
        if (class == .control) return;
        const original = context orelse return error.InvalidRunnableTransition;
        const io = runtime.native_abi.IoBorrow.init(&original.io);
        var child: abi.Child = undefined;
        try borrowed.reserveChild(&.{ .io = &io, .out_child = &child });
        self.child = child;
        errdefer self.deinit();
        try self.child.?.validate();
        self.holders = holders;
        self.parent = borrowed;
        self.parent_class = class;
        holders.producer_active = true;
        self.active = true;
        try checked(borrowed.park(borrowed.context, @backingInt(abi.Wait.resource)));
        const restriction: abi.Reentry = .{ .context = self, .check = checkReentry };
        try self.child.?.activateChecked(&restriction);
        try original.check();
    }

    pub fn requestContext(self: *const @This()) !?Context {
        var context = self.original orelse return null;
        if (self.active) {
            const child = self.child orelse return error.InvalidRunnableTransition;
            try child.borrow.checkLifetime();
            context.runnable = child.borrow;
        }
        try context.check();
        return context;
    }

    /// Provider invocations and internal consumers have returned/joined before
    /// this call. Retire child ownership before queueing original parent CPU.
    pub fn complete(self: *@This()) !void {
        if (!self.active) {
            if (self.original) |context| try context.check();
            return;
        }
        _ = try self.requestContext();
        self.child.?.deinit();
        self.child = null;
        self.holders.?.producer_active = false;
        self.active = false;
        const restriction: abi.Reentry = .{ .context = self, .check = checkReentry };
        try self.parent.?.enterChecked(self.parent_class, &restriction);
    }

    /// Failure leaves the original parent parked/stopped for its owning finish.
    /// Never renew an expired request merely to unwind speculative work.
    pub fn deinit(self: *@This()) void {
        if (self.child) |*child| child.deinit();
        self.child = null;
        if (self.holders) |holders| holders.producer_active = false;
        self.active = false;
    }

    fn checkReentry(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.original) |context| context.check() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn checked(status: runtime.error_abi.Status) !void {
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
    }
};
