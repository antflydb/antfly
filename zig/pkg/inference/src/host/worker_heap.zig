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

//! Child handler heap admission. The reader remains free to route dedicated
//! resource replies; this allocator must never run under the endpoint mutex.
const std = @import("std");
pub const Bootstrap = @import("worker_bootstrap.zig").Owner;
const rpc = @import("antfly_inference_worker_rpc");
const wire = @import("antfly_inference_worker_wire");
const bridge = @import("antfly_inference_bridge");
const process = @import("antfly_platform").process;
const execution = @import("inference_server").execution_control;

/// The parent owns one stable ledger identity per child generation. Call under
/// its resource mutex; accepted credit remains owned until free or physical join.
pub fn adjustParent(current: *u64, budget: *const bridge.ResourceBudget, value: wire.Observation) !void {
    if (value.key != 1 or value.previous != current.*) return error.InvalidGenerationAdmission;
    const adjust = budget.adjust_transient_host orelse return error.ResourceOwnerNotConfigured;
    if (adjust(budget.context, @intFromPtr(current), value.previous, value.next) == 0)
        return error.ResourceTemporarilyUnavailable;
    current.* = value.next;
}

pub fn retireParentAfterJoin(current: *u64, budget: *const bridge.ResourceBudget) void {
    if (current.* == 0) return;
    adjustParent(current, budget, .{ .key = 1, .previous = current.*, .next = 0 }) catch
        @panic("worker child heap retirement refused after reap");
}

pub const Heap = struct {
    endpoint: *rpc.Endpoint,
    budget: *rpc.PayloadBudget,
    pub fn init(self: *Heap, alloc: std.mem.Allocator, endpoint: *rpc.Endpoint) !void {
        // Persistent model/cache memory is admitted by the parent node. RPC's
        // payload-size ceiling is a transport policy, not a heap budget. The
        // external callback is mandatory and runs before any backing allocation.
        const budget = try rpc.PayloadBudget.create(alloc, .{ .retained = std.math.maxInt(usize) });
        errdefer budget.releaseOwner();
        self.* = .{ .endpoint = endpoint, .budget = budget };
        try budget.configureExternal(.{ .context = self, .retain = retain, .release = release, .adjust = adjust });
    }
    pub fn allocator(self: *Heap) std.mem.Allocator {
        return self.budget.retainedAllocator();
    }
    pub fn deinit(self: *Heap) void {
        if (self.budget.references.load(.acquire) != 1) @panic("worker heap closed before its allocations retired");
        self.budget.releaseOwner();
    }
    fn retain(_: *anyopaque) callconv(.c) u8 {
        return 1;
    }
    fn release(_: *anyopaque) callconv(.c) void {}
    fn adjust(raw: *anyopaque, _: usize, previous: u64, next: u64) callconv(.c) u8 {
        const self: *Heap = @ptrCast(@alignCast(raw));
        var wait = if (next > previous) execution.ResourceWait.init() catch return 0 else execution.ResourceWait{};
        var value_buffer: [rpc.max_resource_bytes]u8 = undefined;
        var value = std.Io.Writer.fixed(&value_buffer);
        std.json.Stringify.value(wire.Observation{ .key = 1, .previous = previous, .next = next }, .{}, &value) catch process.exitImmediately(1);
        var request_buffer: [rpc.max_resource_bytes]u8 = undefined;
        var request = std.Io.Writer.fixed(&request_buffer);
        std.json.Stringify.value(wire.ResourceRequest{ .operation = .transient_host, .data = value.buffered() }, .{}, &request) catch process.exitImmediately(1);
        const reply = self.endpoint.callResource(request.buffered()) catch process.exitImmediately(1);
        wait.finish() catch process.exitImmediately(1);
        if (reply.code == 0 and reply.detail == 0 and reply.value == 0) return 1;
        if (next > previous and reply.value == 0) {
            for ([_]anyerror{ error.ResourceLimitExceeded, error.ResourceTemporarilyUnavailable }) |err| {
                const status = bridge.statusFromError(err);
                if (reply.code == status.code and reply.detail == status.detail) return 0;
            }
        }
        // An uncertain or refused release must not let the child keep using
        // the parent's capacity. Reap is the surviving owner's physical fence.
        process.exitImmediately(1);
    }
};
