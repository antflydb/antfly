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

const std = @import("std");

/// Shared operator policy for the database, embedded runtime and inference server.
/// All memory quantities are bytes. Zero memory budgets select automatic policy.
pub const Memory = struct {
    max_bytes: usize = 0,
};

pub const InferenceMemory = struct {
    max_host_bytes: usize = 0,
    max_device_bytes: usize = 0,
    max_combined_bytes: usize = 0,
    max_kv_bytes: usize = 0,
    max_scratch_bytes: usize = 0,

    pub fn limits(self: @This(), comptime T: type) T {
        return .{
            .host_limit_bytes = self.max_host_bytes,
            .backend_limit_bytes = self.max_device_bytes,
            .combined_limit_bytes = self.max_combined_bytes,
            .kv_limit_bytes = self.max_kv_bytes,
            .scratch_limit_bytes = self.max_scratch_bytes,
        };
    }
};

pub const Admission = struct {
    max_concurrent_requests: u32 = 32,
};
pub const InferenceAdmission = struct {
    max_concurrent_requests: u32 = 32,
    /// Null derives work capacity from concurrency; zero disables weighted accounting.
    max_work_units: ?u32 = null,

    pub fn workCapacity(self: @This()) ?usize {
        return if (self.max_work_units) |value| @as(usize, value) else null;
    }
};

pub const InferenceRequestLimits = struct {
    max_items_per_request: ?usize = null,
    max_input_tokens_per_item: ?usize = null,
    max_output_tokens_per_item: ?usize = null,
    max_decoded_pixels_per_batch: ?u64 = null,

    pub fn validateItemCount(self: @This(), count: usize) !void {
        if (self.max_items_per_request) |limit| if (count > limit) return error.InferenceBatchTooLarge;
    }

    pub fn validate(self: @This()) !void {
        inline for (comptime std.meta.fieldNames(@This())) |field| {
            if (@field(self, field)) |value| if (value == 0) return error.InvalidInferenceConfig;
        }
    }

    /// Native execution batching is independent of public request item limits.
    pub fn narrow(self: @This(), batch: anytype) @TypeOf(batch) {
        var out = batch;
        if (self.max_input_tokens_per_item) |limit| out.max_input_tokens_per_item = minimum(out.max_input_tokens_per_item, limit);
        if (self.max_output_tokens_per_item) |limit| out.max_output_tokens_per_item = minimum(out.max_output_tokens_per_item, limit);
        if (self.max_decoded_pixels_per_batch) |limit| if (out.max_decoded_pixels) |current| {
            out.max_decoded_pixels = @min(current, limit);
        };
        return out;
    }

    fn minimum(current: ?usize, limit: usize) ?usize {
        return if (current) |value| @min(value, limit) else limit;
    }
};

pub const GraphLimits = struct {
    max_explored_nodes: usize = 100_000,
    max_explored_edges: usize = 1_000_000,
    max_explored_edge_bytes: usize = 64 * 1024 * 1024,
    max_scanned_anchors: usize = 1_000_000,
    max_intermediate_states: usize = 100_000,
    max_retained_state_bytes: usize = 64 * 1024 * 1024,
    max_distinct_identities: usize = 100_000,
    max_distinct_state_bytes: usize = 16 * 1024 * 1024,

    pub fn validate(self: @This()) !void {
        inline for (comptime std.meta.fieldNames(@This())) |field| {
            if (@field(self, field) == 0) return error.InvalidGraphExecutionLimits;
        }
    }
};

pub const runnable = @import("runnable_policy.zig");
pub const Waiting = struct {
    max_queued_requests: u32 = 0,
    max_queued_bytes: u64 = 0,
    max_wait_ms: u32 = 0,
    pub fn validate(self: @This()) !void {
        if (self.max_wait_ms > 60_000) return error.InvalidConfig;
        if (self.max_wait_ms == 0) {
            if (self.max_queued_requests != 0 or self.max_queued_bytes != 0) return error.InvalidConfig;
        } else if (self.max_queued_requests == 0 or self.max_queued_bytes == 0) return error.InvalidConfig;
    }
};
pub const Execution = struct {
    runnable: ?runnable.Config = .{ .capacity_mode = .automatic, .max_bytes = 16 << 20, .control_bytes = 8 << 20 },
    max_request_duration_ms: u64 = 300_000,
    max_request_memory_bytes: u64 = 256 << 20,
    max_retained_memory_bytes: u64 = 256 << 20,
};
pub const Resources = struct {
    execution: Execution = .{},
    memory: Memory = .{},
    query: struct {
        admission: struct {
            max_concurrent_requests: u32 = 32,
            waiting: ?Waiting = null,
        } = .{},
        request_limits: struct { graph: GraphLimits = .{} } = .{},
    } = .{},
    write: struct {
        admission: Admission = .{ .max_concurrent_requests = 16 },
    } = .{},
    inference: struct {
        memory: InferenceMemory = .{},
        admission: InferenceAdmission = .{},
        request_limits: InferenceRequestLimits = .{},
    } = .{},

    pub fn validate(self: @This()) !void {
        if (self.execution.runnable) |policy| try policy.validateManagedCleanupCapacity();
        if (self.query.admission.waiting) |waiting| try waiting.validate();
        if (self.execution.max_request_duration_ms == 0 or self.execution.max_request_duration_ms > 86_400_000 or
            self.execution.max_request_memory_bytes < 1 << 20 or self.execution.max_retained_memory_bytes < 1 << 20 or
            self.execution.max_request_memory_bytes > std.math.maxInt(usize) or self.execution.max_retained_memory_bytes > std.math.maxInt(usize)) return error.InvalidConfig;
        try self.query.request_limits.graph.validate();
        try self.inference.request_limits.validate();
    }

    pub fn parse(allocator: std.mem.Allocator, value: std.json.Value) !Resources {
        try rejectNull(value);
        // No compatibility aliases or unknown policy fields: typos must fail closed.
        const parsed = try std.json.parseFromValue(Resources, allocator, value, .{ .ignore_unknown_fields = false });
        defer parsed.deinit();
        try parsed.value.validate();
        return parsed.value;
    }
};

fn rejectNull(value: std.json.Value) !void {
    if (value == .null) return error.InvalidInferenceConfig;
    if (value == .object) {
        var fields = value.object.iterator();
        while (fields.next()) |field| {
            // Explicit null disables node scheduling; other null policy values
            // retain the canonical contract's fail-closed validation.
            if (std.mem.eql(u8, field.key_ptr.*, "runnable")) continue;
            try rejectNull(field.value_ptr.*);
        }
    }
}

test "shared resources use bytes and reject misspelled policy and zero request ceilings" {
    const a = std.testing.allocator;
    var tree = try std.json.parseFromSlice(std.json.Value, a,
        \\{"memory":{"max_bytes":536870912},"inference":{"memory":{"max_device_bytes":123},"admission":{"max_concurrent_requests":1,"max_work_units":8},"request_limits":{"max_output_tokens_per_item":64}}}
    , .{});
    defer tree.deinit();
    const policy = try Resources.parse(a, tree.value);
    try std.testing.expectEqual(@as(usize, 536870912), policy.memory.max_bytes);
    try std.testing.expectEqual(@as(?u32, 8), policy.inference.admission.max_work_units);
    try std.testing.expectEqual(@as(?usize, 64), policy.inference.request_limits.max_output_tokens_per_item);
    inline for (.{ "{\"inference\":{\"request_limits\":{\"max_items\":1}}}", "{\"inference\":{\"request_limits\":{\"max_items_per_request\":0}}}" }) |raw| {
        var bad = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
        defer bad.deinit();
        if (Resources.parse(a, bad.value)) |_| return error.TestExpectedError else |_| {}
    }
}
