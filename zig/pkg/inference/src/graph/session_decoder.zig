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

//! Immutable session-bound decoder metadata shared by executor/runtime owners.
//! The loaded session and shared expert cache outlive every derived runtime.
const std = @import("std");
const Session = @import("../backends/session.zig").Session;
const gpt = @import("../models/gpt.zig");
const runtime = @import("../runtime/root.zig");
const model = @import("model_runtime.zig");

pub const CreateRuntime = *const fn (*Definition, std.mem.Allocator) anyerror!model.ModelRuntime;

pub const Definition = struct {
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    session: Session,
    configuration: gpt.OwnedConfig,
    kv_dtype: ?runtime.kv.pool.KvDType,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,
    create_runtime_fn: CreateRuntime,

    fn init(allocator: std.mem.Allocator, session: Session, configuration: *const gpt.Config, kv_dtype: ?runtime.kv.pool.KvDType, shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache, create_runtime: CreateRuntime) !*Definition {
        var owned = try gpt.OwnedConfig.init(allocator, configuration);
        errdefer owned.deinit(allocator);
        const self = try allocator.create(Definition);
        self.* = .{ .allocator = allocator, .session = session, .configuration = owned, .kv_dtype = kv_dtype, .shared_moe_cache = shared_moe_cache, .create_runtime_fn = create_runtime };
        return self;
    }

    /// A runtime may outlive its executor and the executor's allocator wrapper.
    /// Share within one allocator lifetime; otherwise clone into the runtime's
    /// owner before retiring any request-scoped metadata or allocator state.
    pub fn acquireForRuntime(self: *Definition, allocator: std.mem.Allocator) !*Definition {
        if (allocator.ptr == self.allocator.ptr and allocator.vtable == self.allocator.vtable) {
            const previous = self.references.fetchAdd(1, .acq_rel);
            std.debug.assert(previous != 0);
            return self;
        }
        return init(allocator, self.session, &self.configuration.value, self.kv_dtype, self.shared_moe_cache, self.create_runtime_fn);
    }

    pub fn release(self: *Definition) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous != 1) return;
        self.configuration.deinit(self.allocator);
        self.allocator.destroy(self);
    }
};

pub fn createExecutor(allocator: std.mem.Allocator, session: Session, configuration: *const gpt.Config, kv_dtype: ?runtime.kv.pool.KvDType, shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache, create_runtime: CreateRuntime) !model.ModelExecutor {
    return .{ .ptr = try Definition.init(allocator, session, configuration, kv_dtype, shared_moe_cache, create_runtime), .vtable = &executor_vtable };
}

fn createRuntime(raw: *anyopaque, allocator: std.mem.Allocator) !model.ModelRuntime {
    const self: *Definition = @ptrCast(@alignCast(raw));
    return self.create_runtime_fn(self, allocator);
}

fn deinitExecutor(raw: *anyopaque) void {
    const self: *Definition = @ptrCast(@alignCast(raw));
    self.release();
}

const executor_vtable = model.ModelExecutor.VTable{ .create_runtime = createRuntime, .deinit = deinitExecutor };

const TestRuntime = struct {
    fn create(definition: *Definition, allocator: std.mem.Allocator) !model.ModelRuntime {
        return .{ .ptr = try definition.acquireForRuntime(allocator), .vtable = &vtable };
    }
    fn prefill(_: *anyopaque, _: std.mem.Allocator, _: model.PrefillRequest) !model.ModelOutput {
        return error.UnexpectedPhysicalExecution;
    }
    fn deinit(raw: *anyopaque) void {
        const definition: *Definition = @ptrCast(@alignCast(raw));
        definition.release();
    }
    const vtable = model.ModelRuntime.VTable{ .prefill = prefill, .deinit = deinit };
};

fn checkRuntimeMigration(allocator: std.mem.Allocator, executor: *const model.ModelExecutor) !void {
    var result = try executor.createRuntime(allocator);
    defer result.deinit();
    const definition: *Definition = @ptrCast(@alignCast(result.ptr));
    try std.testing.expectEqualStrings("model.", definition.configuration.value.weight_prefix);
    try std.testing.expect(definition.allocator.ptr == allocator.ptr);
    try std.testing.expect(definition.allocator.vtable == allocator.vtable);
}

test "session decoder definition shares metadata and survives executor retirement" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    const table: Session.VTable = undefined;
    const session = Session{ .ptr = &marker, .vtable = &table };
    var prefix = "model.".*;
    const config = gpt.Config{ .weight_prefix = &prefix };
    var executor = try createExecutor(a, session, &config, null, null, TestRuntime.create);
    var executor_live = true;
    defer if (executor_live) executor.deinit();
    @memset(&prefix, 'x');
    var result = try executor.createRuntime(a);
    defer result.deinit();
    try std.testing.expect(result.ptr == executor.ptr);
    executor.deinit();
    executor_live = false;
    const definition: *Definition = @ptrCast(@alignCast(result.ptr));
    try std.testing.expectEqualStrings("model.", definition.configuration.value.weight_prefix);
}

test "session decoder definition migrates metadata across allocator retirement" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    var arena_live = true;
    defer if (arena_live) arena.deinit();
    var marker: u8 = 0;
    const table: Session.VTable = undefined;
    const session = Session{ .ptr = &marker, .vtable = &table };
    const config = gpt.Config{ .weight_prefix = "model." };
    var executor = try createExecutor(arena.allocator(), session, &config, null, null, TestRuntime.create);
    var executor_live = true;
    defer if (executor_live) executor.deinit();
    var result = try executor.createRuntime(a);
    defer result.deinit();
    try std.testing.expect(result.ptr != executor.ptr);
    executor.deinit();
    executor_live = false;
    arena.deinit();
    arena_live = false;
    const definition: *Definition = @ptrCast(@alignCast(result.ptr));
    try std.testing.expectEqualStrings("model.", definition.configuration.value.weight_prefix);
}

test "session decoder definition migration is failure atomic at every allocation" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    const table: Session.VTable = undefined;
    const session = Session{ .ptr = &marker, .vtable = &table };
    const config = gpt.Config{ .weight_prefix = "model." };
    var executor = try createExecutor(a, session, &config, null, null, TestRuntime.create);
    defer executor.deinit();
    try std.testing.checkAllAllocationFailures(a, checkRuntimeMigration, .{&executor});
    try checkRuntimeMigration(a, &executor);
}
