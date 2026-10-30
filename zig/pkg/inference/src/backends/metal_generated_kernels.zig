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

//! Neutral bounded generated-Metal kernel execution shared by context backends.
//! This module contains no model- or Qwen-specific kernels or routing policy.
const std = @import("std");
const options = @import("build_options");
const mr = @import("metal_runtime.zig");
const owner_alloc = @import("../runtime/kv/host_allocation_tracker.zig");

pub extern fn termite_metal_generated_alias(runtime: *mr.RawMetalDecodeRuntime, data: [*]u8, bytes: usize) ?*anyopaque;
pub extern fn termite_metal_buffer_release(handle: *anyopaque) void;
pub extern fn termite_metal_buffer_allocated_bytes(handle: *anyopaque) u64;

pub const Params = extern struct {
    op: u32,
    n: u32 = 0,
    dim: u32 = 0,
    groups: u32 = 0,
    position: u32 = 0,
    stride: u32 = 0,
    mode: u32 = 0,
    index: u32 = 0,
    epsilon: f32 = 1e-6,
    scale: f32 = 1,
    theta: f32 = 10000000,
    reserved: f32 = 0,
};
pub const Ref = struct {
    handle: *anyopaque,
    offset: usize = 0,
    bytes: usize,
    pub fn slice(self: Ref, offset: usize, bytes: usize) !Ref {
        if (offset > self.bytes or bytes > self.bytes - offset) return error.MetalBufferRange;
        return .{ .handle = self.handle, .offset = try std.math.add(usize, self.offset, offset), .bytes = bytes };
    }
};
pub const BufferAllocationTracker = owner_alloc.HostAllocationTracker;
pub const BufferAllocationSnapshot = owner_alloc.Snapshot;

pub const Buffer = struct {
    bytes: []u8,
    handle: *anyopaque,
    logical_bytes: usize,
    allocation_tracker: ?*owner_alloc.HostAllocationTracker = null,
    tracked_backing_bytes: usize = 0,
    pub fn init(runtime: *mr.RawMetalDecodeRuntime, len: usize) !Buffer {
        return initTracked(runtime, len, null);
    }
    pub fn initTracked(runtime: *mr.RawMetalDecodeRuntime, len: usize, tracker: ?*owner_alloc.HostAllocationTracker) !Buffer {
        if (len == 0) return error.EmptyMetalBuffer;
        if (tracker) |owner| owner.beginSharedMutation();
        defer if (tracker) |owner| owner.endSharedMutation();
        const page = std.heap.pageSize();
        const rounded = (try std.math.add(usize, len, page - 1)) & ~(page - 1);
        const bytes = try std.heap.page_allocator.alloc(u8, rounded);
        errdefer std.heap.page_allocator.free(bytes);
        const handle = termite_metal_generated_alias(runtime, bytes.ptr, bytes.len) orelse return error.MetalBufferAllocationFailed;
        errdefer termite_metal_buffer_release(handle);
        const allocated = if (tracker != null)
            std.math.cast(usize, termite_metal_buffer_allocated_bytes(handle)) orelse return error.MetalBufferAllocationSizeUnavailable
        else
            0;
        if (tracker) |owner| {
            if (allocated == 0 or allocated != bytes.len) return error.MetalBufferAllocationSizeMismatch;
            try owner.recordSharedAllocLocked(allocated);
        }
        return .{ .bytes = bytes, .handle = handle, .logical_bytes = len, .allocation_tracker = tracker, .tracked_backing_bytes = allocated };
    }
    pub fn deinit(self: *Buffer) void {
        const allocation_tracker = self.allocation_tracker;
        if (allocation_tracker) |owner| owner.beginSharedMutation();
        defer if (allocation_tracker) |owner| owner.endSharedMutation();
        termite_metal_buffer_release(self.handle);
        std.heap.page_allocator.free(self.bytes);
        if (allocation_tracker) |owner| owner.recordSharedFreeLocked(self.tracked_backing_bytes);
        self.* = undefined;
    }
    pub fn ref(self: Buffer) Ref {
        return .{ .handle = self.handle, .bytes = self.logical_bytes };
    }
    pub fn floats(self: Buffer) []f32 {
        return std.mem.bytesAsSlice(f32, @as([]align(4) u8, @alignCast(self.bytes[0..self.logical_bytes])));
    }
};
pub const BatchKind = enum { attention, replay, storage, fitting };

pub const Profile = struct {
    enabled: bool = false,
    // Host wall time: 0..27 opcodes, 28 quantized matvec, 29 paging, 30 batch wait.
    // 31/32 grouped projection/activation, 33/34 prefill gather/scatter.
    // Inside batches, opcode timings measure encoding, not GPU execution.
    nanoseconds: [35]u64 = @splat(0),
    calls: [35]u64 = @splat(0),
    batch_wall_ns: [4]u64 = @splat(0),
    batch_wait_ns: [4]u64 = @splat(0),
    batch_gpu_ns: [4]u64 = @splat(0),
    pub fn now() u64 {
        var ts: std.c.timespec = undefined;
        if (std.c.clock_gettime(.MONOTONIC, &ts) != 0) return 0;
        return @as(u64, @intCast(ts.sec)) * 1_000_000_000 + @as(u64, @intCast(ts.nsec));
    }
    pub fn record(self: *Profile, op: usize, start: u64) void {
        if (!self.enabled or op >= self.calls.len) return;
        self.nanoseconds[op] +|= now() -| start;
        self.calls[op] +|= 1;
    }
    pub fn since(self: Profile, before: Profile) Profile {
        var result = self;
        for (0..result.calls.len) |i| {
            result.nanoseconds[i] -|= before.nanoseconds[i];
            result.calls[i] -|= before.calls[i];
        }
        for (0..4) |i| {
            result.batch_wall_ns[i] -|= before.batch_wall_ns[i];
            result.batch_wait_ns[i] -|= before.batch_wait_ns[i];
            result.batch_gpu_ns[i] -|= before.batch_gpu_ns[i];
        }
        return result;
    }
};
pub const PairTiming = struct {
    wall_nanos: u64,
    wait_nanos: u64,
    gpu_nanos: u64,
};
pub const PairMetrics = struct {
    dispatches: u64 = 0,
    failures: u64 = 0,
    wall_nanos: u64 = 0,
    wait_nanos: u64 = 0,
    gpu_nanos: u64 = 0,
};
pub const BatchCounters = struct {
    submissions: u64 = 0,
    waits: u64 = 0,
    active: bool = false,
    submitted: bool = false,
};

fn profileIndex(op: u32) ?usize {
    const index: usize = if (op == 32) 28 else if (op >= 28) @as(usize, op) + 3 else op;
    return if (index < 35) index else null;
}

pub const Execution = struct {
    runtime: *mr.RawMetalDecodeRuntime,
    pipeline: *mr.RawMetalGeneratedPipeline,

    pub fn initSource(source: []const u8, name: [*:0]const u8) !Execution {
        if (comptime !options.enable_metal) return error.MetalNotEnabled;
        const runtime = mr.termite_metal_decode_runtime_create() orelse return error.MetalRuntimeUnavailable;
        errdefer mr.termite_metal_decode_runtime_destroy(runtime);
        var message: [4096]u8 = @splat(0);
        const pipeline = mr.termite_metal_generated_pipeline_create(source.ptr, source.len, name, 1, &message, message.len) orelse {
            std.log.err("Metal kernel {s}: {s}", .{ name, std.mem.sliceTo(&message, 0) });
            return error.MetalKernelCompilationFailed;
        };
        return .{ .runtime = runtime, .pipeline = pipeline };
    }

    pub fn deinit(self: *Execution) void {
        if (mr.hasActiveFrame(self.runtime) or mr.hasSubmittedFrame(self.runtime))
            @panic("destroying execution with retained Metal frame");
        mr.termite_metal_generated_pipeline_destroy(self.pipeline);
        mr.termite_metal_decode_runtime_destroy(self.runtime);
        self.* = undefined;
    }
};
pub const Kernels = struct {
    runtime: *mr.RawMetalDecodeRuntime,
    pipeline: *mr.RawMetalGeneratedPipeline,
    owns_execution: bool = true,
    dummy: Buffer,
    profile: Profile = .{},
    cancelled: ?*const std.atomic.Value(bool) = null,
    batch_active: bool = false,
    batch_submitted: bool = false,
    batching_enabled: bool = true,
    batch_submissions: u64 = 0,
    batch_waits: u64 = 0,
    pair_metrics: PairMetrics = .{},
    allocation_tracker: ?*owner_alloc.HostAllocationTracker = null,

    pub fn initSource(source: []const u8, name: [*:0]const u8) !Kernels {
        return initSourceTracked(source, name, null);
    }
    pub fn initSourceTracked(source: []const u8, name: [*:0]const u8, tracker: ?*owner_alloc.HostAllocationTracker) !Kernels {
        var execution = try Execution.initSource(source, name);
        errdefer execution.deinit();
        var result = try initBorrowedTracked(&execution, tracker);
        result.owns_execution = true;
        return result;
    }
    pub fn initBorrowedTracked(execution: *const Execution, tracker: ?*owner_alloc.HostAllocationTracker) !Kernels {
        return .{
            .runtime = execution.runtime,
            .pipeline = execution.pipeline,
            .owns_execution = false,
            .dummy = try Buffer.initTracked(execution.runtime, 4, tracker),
            .profile = .{ .enabled = std.c.getenv("ANTFLY_CONTEXT_PROFILE") != null },
            .allocation_tracker = tracker,
        };
    }
    pub fn buffer(self: *Kernels, len: usize) !Buffer {
        return Buffer.initTracked(self.runtime, len, self.allocation_tracker);
    }
    pub fn batchCounters(self: *const Kernels) BatchCounters {
        return .{ .submissions = self.batch_submissions, .waits = self.batch_waits, .active = self.batch_active, .submitted = self.batch_submitted };
    }
    pub fn requireOwnedBatchIdle(self: *const Kernels) !void {
        if (!self.batching_enabled) return error.GeneratedMetalBatchingDisabled;
        if (self.batch_active or self.batch_submitted)
            return error.ContextTransactionBusy;
    }
    pub fn requireNoBatchWork(self: *const Kernels) !void {
        if (self.batch_active or self.batch_submitted)
            return error.ContextTransactionBusy;
    }
    /// Pure Zig ABI/range preflight. Native dispatch repeats device ownership
    /// validation while encoding; callers cancel the unsubmitted frame if it
    /// unexpectedly fails there.
    pub fn validateRun(
        self: *const Kernels,
        params: Params,
        inputs: []const Ref,
        output: Ref,
    ) !void {
        _ = self;
        try validateDispatch(params, inputs, output);
    }
    pub fn beginBatch(self: *Kernels) !void {
        if (self.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (!self.batching_enabled) return;
        if (self.batch_active or self.batch_submitted) return error.ContextTransactionBusy;
        if (mr.termite_metal_decode_runtime_begin_frame(self.runtime) != 0) return error.MetalFrameBeginFailed;
        self.batch_active = true;
    }
    pub fn submitBatch(self: *Kernels) !void {
        if (!self.batch_active) return;
        if (self.batch_submitted) return error.ContextTransactionBusy;
        if (mr.termite_metal_decode_runtime_submit_frame(self.runtime) != 0) return error.MetalFrameSubmitFailed;
        self.batch_active = false;
        self.batch_submitted = true;
        self.batch_submissions +|= 1;
    }
    pub fn waitBatch(self: *Kernels, kind: ?BatchKind) !void {
        if (!self.batch_submitted) return;
        const start = if (self.profile.enabled) Profile.now() else 0;
        defer self.profile.record(30, start);
        // wait_frame drains submitted resources even when execution fails.
        self.batch_waits +|= 1;
        const status = mr.termite_metal_decode_runtime_wait_frame(self.runtime);
        self.batch_submitted = false;
        if (self.profile.enabled) if (kind) |k| {
            const i = @backingInt(k);
            self.profile.batch_wait_ns[i] +|= Profile.now() -| start;
            self.profile.batch_gpu_ns[i] +|= mr.termite_metal_decode_runtime_last_frame_gpu_nanos(self.runtime);
        };
        if (status != 0) return error.MetalFrameSyncFailed;
    }
    pub fn endBatch(self: *Kernels) !void {
        try self.submitBatch();
        try self.waitBatch(null);
    }
    pub fn endProfiledBatch(self: *Kernels, kind: BatchKind) !void {
        try self.submitBatch();
        try self.waitBatch(kind);
    }
    pub fn cancelBatch(self: *Kernels) void {
        if (self.batch_active) {
            _ = mr.termite_metal_decode_runtime_cancel_frame(self.runtime);
            self.batch_active = false;
        }
        self.waitBatch(null) catch {};
    }
    pub fn deinit(self: *Kernels) void {
        self.cancelBatch();
        self.dummy.deinit();
        if (self.owns_execution) {
            var execution = Execution{ .runtime = self.runtime, .pipeline = self.pipeline };
            execution.deinit();
        }
        self.* = undefined;
    }

    fn validateDispatch(params: Params, inputs: []const Ref, output: Ref) !void {
        if (params.n == 0 or inputs.len > 7 or output.bytes == 0) return error.InvalidGeneratedDispatch;
        // Some ABI variants intentionally bind an empty descriptor as an
        // unused placeholder. The native dispatcher still validates its
        // handle, offset, and backing-buffer range; a zero logical length is
        // therefore valid here, while an unaligned offset is not.
        for (inputs) |input| if (input.offset % 4 != 0) return error.InvalidGeneratedDispatch;
        if (output.offset % 4 != 0) return error.InvalidGeneratedDispatch;
    }

    const DispatchBuffers = struct { handles: [8]?*anyopaque, offsets: [8]usize, lengths: [8]usize };
    fn dispatchBuffers(self: *Kernels, inputs: []const Ref, output: Ref) DispatchBuffers {
        var result = DispatchBuffers{ .handles = @splat(self.dummy.handle), .offsets = @splat(0), .lengths = @splat(4) };
        for (inputs, 0..) |input, i| {
            result.handles[i] = input.handle;
            result.offsets[i] = input.offset;
            result.lengths[i] = input.bytes;
        }
        result.handles[7] = output.handle;
        result.offsets[7] = output.offset;
        result.lengths[7] = output.bytes;
        return result;
    }

    pub fn run(self: *Kernels, params: Params, inputs: []const Ref, output: Ref) !void {
        if (self.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        try validateDispatch(params, inputs, output);
        const start = if (self.profile.enabled) Profile.now() else 0;
        defer if (profileIndex(params.op)) |index| self.profile.record(index, start);
        var buffers = self.dispatchBuffers(inputs, output);
        if (mr.termite_metal_generated_dispatch(self.runtime, self.pipeline, &buffers.handles, &buffers.offsets, &buffers.lengths, 8, &params, @sizeOf(Params), params.n) != 0) return error.GeneratedMetalDispatchFailed;
    }

    pub fn runPair(
        self: *Kernels,
        first_params: Params,
        first_inputs: []const Ref,
        first_output: Ref,
        second_params: Params,
        second_inputs: []const Ref,
        second_output: Ref,
        kind: BatchKind,
    ) !PairTiming {
        if (self.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (self.batch_active or self.batch_submitted) return error.ContextTransactionBusy;
        // Validate both contracts before the owned command buffer is created.
        try validateDispatch(first_params, first_inputs, first_output);
        try validateDispatch(second_params, second_inputs, second_output);
        var first = self.dispatchBuffers(first_inputs, first_output);
        var second = self.dispatchBuffers(second_inputs, second_output);
        var wait_nanos: u64 = 0;
        var gpu_nanos: u64 = 0;
        const started = Profile.now();
        const status = mr.termite_metal_generated_dispatch_pair(
            self.runtime,
            self.pipeline,
            &first.handles,
            &first.offsets,
            &first.lengths,
            8,
            &first_params,
            @sizeOf(Params),
            first_params.n,
            &second.handles,
            &second.offsets,
            &second.lengths,
            8,
            &second_params,
            @sizeOf(Params),
            second_params.n,
            &wait_nanos,
            &gpu_nanos,
        );
        const wall_nanos = Profile.now() -| started;
        self.pair_metrics.wall_nanos +|= wall_nanos;
        self.pair_metrics.wait_nanos +|= wait_nanos;
        self.pair_metrics.gpu_nanos +|= gpu_nanos;
        if (status != 0) {
            self.pair_metrics.failures +|= 1;
            return error.GeneratedMetalDispatchFailed;
        }
        self.pair_metrics.dispatches +|= 1;
        if (self.profile.enabled) {
            const i = @backingInt(kind);
            self.profile.batch_wall_ns[i] +|= wall_nanos;
            self.profile.batch_wait_ns[i] +|= wait_nanos;
            self.profile.batch_gpu_ns[i] +|= gpu_nanos;
        }
        return .{ .wall_nanos = wall_nanos, .wait_nanos = wait_nanos, .gpu_nanos = gpu_nanos };
    }

    fn refsOverlap(a: Ref, b: Ref) bool {
        if (a.handle != b.handle or a.bytes == 0 or b.bytes == 0) return false;
        const a_end = std.math.add(usize, a.offset, a.bytes) catch return true;
        const b_end = std.math.add(usize, b.offset, b.bytes) catch return true;
        return a.offset < b_end and b.offset < a_end;
    }

    fn validateContextSplitPair(
        stage: Params,
        stage_inputs: []const Ref,
        scratch: Ref,
        reduce: Params,
        reduce_inputs: []const Ref,
        output: Ref,
    ) !void {
        try validateDispatch(stage, stage_inputs, scratch);
        try validateDispatch(reduce, reduce_inputs, output);
        if (stage_inputs.len != 7 or reduce_inputs.len != 7 or
            (stage.op != 3 and stage.op != 4) or reduce.op != stage.op + 2 or
            stage.dim == 0 or stage.groups == 0 or stage.stride == 0 or
            stage.stride > stage.groups or stage.groups % stage.stride != 0 or
            stage.dim != reduce.dim or stage.groups != reduce.groups or
            stage.position != reduce.position or stage.stride != reduce.stride or
            stage.mode != reduce.mode or stage.index != reduce.index or
            @as(u32, @bitCast(stage.epsilon)) != @as(u32, @bitCast(reduce.epsilon)) or
            @as(u32, @bitCast(stage.scale)) != @as(u32, @bitCast(reduce.scale)) or
            @as(u32, @bitCast(stage.theta)) != @as(u32, @bitCast(reduce.theta)) or
            @as(u32, @bitCast(stage.reserved)) != @as(u32, @bitCast(reduce.reserved)))
            return error.InvalidContextSplitDispatch;
        const split_count: u32 = @bitCast(stage.reserved);
        const output_bytes = try std.math.mul(usize, try std.math.mul(usize, stage.groups, stage.dim), @sizeOf(f32));
        var scratch_elements = try std.math.mul(usize, try std.math.mul(usize, stage.groups, stage.dim), split_count);
        scratch_elements = try std.math.add(usize, scratch_elements, try std.math.mul(usize, try std.math.mul(usize, stage.groups, 2), split_count));
        const scratch_bytes = try std.math.mul(usize, scratch_elements, @sizeOf(f32));
        const span_bytes = try std.math.mul(usize, stage.index, 6 * @sizeOf(u32));
        const head_bytes = try std.math.mul(usize, stage.stride, 6 * @sizeOf(u32));
        if (split_count == 0 or split_count > 32 or stage.dim > 512 or
            !std.math.isFinite(stage.scale) or !(stage.scale > 0) or
            !std.math.isFinite(stage.epsilon) or stage.epsilon < 0 or stage.index == 0 or
            stage.n != try std.math.mul(u32, try std.math.mul(u32, stage.groups, split_count), 32) or
            reduce.n != try std.math.mul(u32, stage.groups, 32) or
            stage_inputs[0].bytes < output_bytes or stage_inputs[1].bytes < @as(usize, stage.dim) * @sizeOf(f16) or
            stage_inputs[2].bytes < @as(usize, stage.dim) * @sizeOf(f16) or stage_inputs[4].bytes < span_bytes or
            stage_inputs[5].bytes < @sizeOf(u32) or (stage.op == 4 and stage_inputs[6].bytes < head_bytes) or
            scratch.bytes < scratch_bytes or reduce_inputs[0].bytes < scratch_bytes or
            reduce_inputs[5].bytes < @sizeOf(u32) or output.bytes < output_bytes or
            scratch.handle != reduce_inputs[0].handle or scratch.offset != reduce_inputs[0].offset or
            scratch.bytes != reduce_inputs[0].bytes or
            stage_inputs[5].handle != reduce_inputs[5].handle or
            stage_inputs[5].offset != reduce_inputs[5].offset or
            stage_inputs[5].bytes != reduce_inputs[5].bytes or refsOverlap(scratch, output))
            return error.InvalidContextSplitDispatch;
        for (stage_inputs, 0..) |input, index| {
            if (refsOverlap(scratch, input) or refsOverlap(output, input) or
                (index != 5 and refsOverlap(stage_inputs[5], input)))
                return error.InvalidContextSplitDispatch;
        }
    }

    pub fn runContextSplitPairOnActiveRuntime(
        self: *Kernels,
        owner_runtime: *mr.RawMetalDecodeRuntime,
        stage: Params,
        stage_inputs: []const Ref,
        scratch: Ref,
        reduce: Params,
        reduce_inputs: []const Ref,
        output: Ref,
    ) !void {
        if (self.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        try validateContextSplitPair(stage, stage_inputs, scratch, reduce, reduce_inputs, output);
        var stage_buffers = self.dispatchBuffers(stage_inputs, scratch);
        var reduce_buffers = self.dispatchBuffers(reduce_inputs, output);
        if (mr.termite_metal_context_split_dispatch_pair_on_active_frame(
            owner_runtime,
            self.pipeline,
            &stage_buffers.handles,
            &stage_buffers.offsets,
            &stage_buffers.lengths,
            8,
            &stage,
            @sizeOf(Params),
            stage.n,
            &reduce_buffers.handles,
            &reduce_buffers.offsets,
            &reduce_buffers.lengths,
            8,
            &reduce,
            @sizeOf(Params),
            reduce.n,
        ) != 0) return error.GeneratedMetalDispatchFailed;
    }

    fn validateReplaySplitPair(
        stage: Params,
        stage_inputs: []const Ref,
        scratch: Ref,
        reduce: Params,
        reduce_inputs: []const Ref,
        output: Ref,
    ) !void {
        try validateDispatch(stage, stage_inputs, scratch);
        try validateDispatch(reduce, reduce_inputs, output);
        const split_count: u32 = @bitCast(stage.reserved);
        if (stage_inputs.len != 7 or reduce_inputs.len != 7 or stage.op != 2 or reduce.op != 3 or
            stage.dim == 0 or stage.dim > 512 or stage.groups == 0 or stage.stride == 0 or
            stage.stride > stage.groups or stage.groups % stage.stride != 0 or
            stage.dim != reduce.dim or stage.groups != reduce.groups or stage.position != reduce.position or
            stage.stride != reduce.stride or stage.mode == 0 or stage.mode > 8 or stage.mode != reduce.mode or
            stage.index == 0 or stage.index != reduce.index or
            @as(u32, @bitCast(stage.epsilon)) != @as(u32, @bitCast(reduce.epsilon)) or
            @as(u32, @bitCast(stage.scale)) != @as(u32, @bitCast(reduce.scale)) or
            @as(u32, @bitCast(stage.reserved)) != @as(u32, @bitCast(reduce.reserved)) or
            !std.math.isFinite(stage.scale) or !(stage.scale > 0) or
            !std.math.isFinite(stage.epsilon) or stage.epsilon < 0 or
            split_count < 2 or split_count > 32)
            return error.InvalidReplaySplitDispatch;
        const vectors = try std.math.mul(u32, stage.mode, stage.groups);
        if (stage.n != try std.math.mul(u32, try std.math.mul(u32, vectors, split_count), 32) or
            reduce.n != try std.math.mul(u32, vectors, 32)) return error.InvalidReplaySplitDispatch;
        const query_bytes = try std.math.mul(usize, try std.math.mul(usize, vectors, stage.dim), @sizeOf(f32));
        var scratch_elements = try std.math.mul(usize, vectors, split_count);
        scratch_elements = try std.math.mul(usize, scratch_elements, try std.math.add(usize, stage.dim, 2));
        const scratch_bytes = try std.math.mul(usize, scratch_elements, @sizeOf(f32));
        const tail_end = try std.math.add(u32, stage.position, stage.mode);
        if (stage.position < stage.index or tail_end < stage.index) return error.InvalidReplaySplitDispatch;
        const tail_rows = tail_end - stage.index;
        const tail_bytes = try std.math.mul(usize, try std.math.mul(usize, tail_rows, stage.stride), try std.math.mul(usize, stage.dim, @sizeOf(f16)));
        if (stage_inputs[0].bytes < query_bytes or
            stage_inputs[1].bytes < @as(usize, stage.dim) * @sizeOf(f16) or
            stage_inputs[2].bytes < @as(usize, stage.dim) * @sizeOf(f16) or
            stage_inputs[3].bytes < tail_bytes or stage_inputs[4].bytes < tail_bytes or
            stage_inputs[5].bytes < @sizeOf(u32) or
            stage_inputs[6].bytes < @as(usize, stage.stride) * 6 * @sizeOf(u32) or
            scratch.bytes < scratch_bytes or reduce_inputs[0].bytes < scratch_bytes or
            reduce_inputs[5].bytes < @sizeOf(u32) or output.bytes < query_bytes or
            scratch.handle != reduce_inputs[0].handle or scratch.offset != reduce_inputs[0].offset or
            scratch.bytes != reduce_inputs[0].bytes or
            stage_inputs[5].handle != reduce_inputs[5].handle or
            stage_inputs[5].offset != reduce_inputs[5].offset or
            stage_inputs[5].bytes != reduce_inputs[5].bytes or refsOverlap(scratch, output))
            return error.InvalidReplaySplitDispatch;
        for (stage_inputs, 0..) |input, index| {
            if (refsOverlap(scratch, input) or refsOverlap(output, input) or
                (index != 5 and refsOverlap(stage_inputs[5], input)))
                return error.InvalidReplaySplitDispatch;
        }
    }

    pub fn runReplaySplitPairOnActiveRuntime(
        self: *Kernels,
        owner_runtime: *mr.RawMetalDecodeRuntime,
        stage: Params,
        stage_inputs: []const Ref,
        scratch: Ref,
        reduce: Params,
        reduce_inputs: []const Ref,
        output: Ref,
    ) !void {
        if (self.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        try validateReplaySplitPair(stage, stage_inputs, scratch, reduce, reduce_inputs, output);
        var stage_buffers = self.dispatchBuffers(stage_inputs, scratch);
        var reduce_buffers = self.dispatchBuffers(reduce_inputs, output);
        if (mr.termite_metal_replay_split_dispatch_pair_on_active_frame(
            owner_runtime,
            self.pipeline,
            &stage_buffers.handles,
            &stage_buffers.offsets,
            &stage_buffers.lengths,
            8,
            &stage,
            @sizeOf(Params),
            stage.n,
            &reduce_buffers.handles,
            &reduce_buffers.offsets,
            &reduce_buffers.lengths,
            8,
            &reduce,
            @sizeOf(Params),
            reduce.n,
        ) != 0) return error.GeneratedMetalDispatchFailed;
    }

    pub fn runOnActiveRuntime(
        self: *Kernels,
        owner_runtime: *mr.RawMetalDecodeRuntime,
        params: Params,
        inputs: []const Ref,
        output: Ref,
    ) !void {
        return self.runOnActiveRuntimeImpl(false, owner_runtime, params, inputs, output);
    }

    /// The caller owns the shader ABI: set each bit whose binding may write.
    /// Unset bits are read-only, including dummy bindings. Aliases retain the
    /// union of their roles through the existing runtime range hazard tracker.
    pub fn runCoalescedOnActiveRuntime(
        self: *Kernels,
        owner_runtime: *mr.RawMetalDecodeRuntime,
        params: Params,
        inputs: []const Ref,
        output: Ref,
        write_mask: u8,
    ) !void {
        if (self.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        try validateDispatch(params, inputs, output);
        var buffers = self.dispatchBuffers(inputs, output);
        if (mr.termite_metal_generated_dispatch_accesses_coalesced_on_active_frame(
            owner_runtime,
            self.pipeline,
            &buffers.handles,
            &buffers.offsets,
            &buffers.lengths,
            8,
            &params,
            @sizeOf(Params),
            params.n,
            write_mask,
        ) != 0) return error.GeneratedMetalDispatchFailed;
    }

    fn runOnActiveRuntimeImpl(
        self: *Kernels,
        comptime coalesced: bool,
        owner_runtime: *mr.RawMetalDecodeRuntime,
        params: Params,
        inputs: []const Ref,
        output: Ref,
    ) !void {
        if (self.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        try validateDispatch(params, inputs, output);
        var buffers = self.dispatchBuffers(inputs, output);
        const dispatch = if (coalesced) mr.termite_metal_generated_dispatch_coalesced_on_active_frame else mr.termite_metal_generated_dispatch_on_active_frame;
        if (dispatch(
            owner_runtime,
            self.pipeline,
            &buffers.handles,
            &buffers.offsets,
            &buffers.lengths,
            8,
            &params,
            @sizeOf(Params),
            params.n,
        ) != 0) return error.GeneratedMetalDispatchFailed;
    }

    pub fn runThreadgroupsPipeline(
        self: *Kernels,
        pipeline: *mr.RawMetalGeneratedPipeline,
        params: Params,
        inputs: []const Ref,
        output: Ref,
        grid: [3]usize,
        group: [3]usize,
    ) !PairTiming {
        if (self.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (self.batch_active or self.batch_submitted) return error.ContextTransactionBusy;
        if (inputs.len > 7) return error.InvalidGeneratedDispatch;
        var buffers = self.dispatchBuffers(inputs, output);
        var wait_nanos: u64 = 0;
        var gpu_nanos: u64 = 0;
        const started = Profile.now();
        const status = mr.termite_metal_generated_dispatch_threadgroups_owned(
            self.runtime,
            pipeline,
            &buffers.handles,
            &buffers.offsets,
            &buffers.lengths,
            8,
            &params,
            @sizeOf(Params),
            grid[0],
            grid[1],
            grid[2],
            group[0],
            group[1],
            group[2],
            &wait_nanos,
            &gpu_nanos,
        );
        const wall_nanos = Profile.now() -| started;
        if (status != 0) return error.GeneratedMetalDispatchFailed;
        return .{ .wall_nanos = wall_nanos, .wait_nanos = wait_nanos, .gpu_nanos = gpu_nanos };
    }

    pub fn runThreadgroupsPipelineOnActiveFrame(
        self: *Kernels,
        pipeline: *mr.RawMetalGeneratedPipeline,
        params: Params,
        inputs: []const Ref,
        output: Ref,
        grid: [3]usize,
        group: [3]usize,
    ) !void {
        return self.runThreadgroupsPipelineOnActiveRuntime(self.runtime, pipeline, params, inputs, output, grid, group);
    }

    pub fn runThreadgroupsPipelineOnActiveRuntime(
        self: *Kernels,
        owner_runtime: *mr.RawMetalDecodeRuntime,
        pipeline: *mr.RawMetalGeneratedPipeline,
        params: Params,
        inputs: []const Ref,
        output: Ref,
        grid: [3]usize,
        group: [3]usize,
    ) !void {
        return self.runThreadgroupsPipelineOnActiveRuntimeImpl(false, owner_runtime, pipeline, params, inputs, output, grid, group);
    }

    pub fn runThreadgroupsPipelineCoalescedOnActiveRuntime(
        self: *Kernels,
        owner_runtime: *mr.RawMetalDecodeRuntime,
        pipeline: *mr.RawMetalGeneratedPipeline,
        params: Params,
        inputs: []const Ref,
        output: Ref,
        grid: [3]usize,
        group: [3]usize,
        write_mask: u8,
    ) !void {
        if (self.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (self.batch_submitted or inputs.len > 7) return error.InvalidGeneratedDispatch;
        var buffers = self.dispatchBuffers(inputs, output);
        if (mr.termite_metal_generated_dispatch_threadgroups_accesses_coalesced_on_active_frame(
            owner_runtime,
            pipeline,
            &buffers.handles,
            &buffers.offsets,
            &buffers.lengths,
            8,
            &params,
            @sizeOf(Params),
            grid[0],
            grid[1],
            grid[2],
            group[0],
            group[1],
            group[2],
            write_mask,
        ) != 0) return error.GeneratedMetalDispatchFailed;
    }

    fn runThreadgroupsPipelineOnActiveRuntimeImpl(
        self: *Kernels,
        comptime coalesced: bool,
        owner_runtime: *mr.RawMetalDecodeRuntime,
        pipeline: *mr.RawMetalGeneratedPipeline,
        params: Params,
        inputs: []const Ref,
        output: Ref,
        grid: [3]usize,
        group: [3]usize,
    ) !void {
        if (self.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (self.batch_submitted or inputs.len > 7) return error.InvalidGeneratedDispatch;
        var buffers = self.dispatchBuffers(inputs, output);
        const dispatch = if (coalesced) mr.termite_metal_generated_dispatch_threadgroups_coalesced_on_active_frame else mr.termite_metal_generated_dispatch_threadgroups_on_active_frame;
        if (dispatch(
            owner_runtime,
            pipeline,
            &buffers.handles,
            &buffers.offsets,
            &buffers.lengths,
            8,
            &params,
            @sizeOf(Params),
            grid[0],
            grid[1],
            grid[2],
            group[0],
            group[1],
            group[2],
        ) != 0) return error.GeneratedMetalDispatchFailed;
    }
};

test "context generated profile indices remain inside fixed counters" {
    try std.testing.expectEqual(@as(?usize, 27), profileIndex(27));
    try std.testing.expectEqual(@as(?usize, 31), profileIndex(28));
    try std.testing.expectEqual(@as(?usize, 34), profileIndex(31));
    try std.testing.expectEqual(@as(?usize, 28), profileIndex(32));
    try std.testing.expectEqual(@as(?usize, null), profileIndex(33));
    try std.testing.expectEqual(@as(?usize, null), profileIndex(std.math.maxInt(u32)));

    var profile: Profile = .{ .enabled = true };
    profile.record(profile.calls.len, Profile.now());
    try std.testing.expectEqual(@as([35]u64, @splat(0)), profile.calls);
}

test "context generated owned batch admission and preflight fail closed" {
    var kernels: Kernels = undefined;
    kernels.batching_enabled = false;
    kernels.batch_active = false;
    kernels.batch_submitted = false;
    kernels.batch_submissions = 0;
    kernels.batch_waits = 0;
    try std.testing.expectError(
        error.GeneratedMetalBatchingDisabled,
        kernels.requireOwnedBatchIdle(),
    );
    kernels.batching_enabled = true;
    kernels.batch_active = true;
    try std.testing.expectError(
        error.ContextTransactionBusy,
        kernels.requireOwnedBatchIdle(),
    );
    kernels.batch_active = false;
    const handle: *anyopaque = @ptrFromInt(4096);
    const before = kernels.batchCounters();
    try std.testing.expectError(
        error.InvalidGeneratedDispatch,
        kernels.validateRun(.{ .op = 1, .n = 0 }, &.{.{ .handle = handle, .bytes = 4 }}, .{ .handle = handle, .bytes = 4 }),
    );
    try std.testing.expectEqual(before, kernels.batchCounters());
}

test "context generated dispatch accepts empty ABI placeholder inputs" {
    const handle: *anyopaque = @ptrFromInt(4096);
    const empty = Ref{ .handle = handle, .bytes = 0 };
    const output = Ref{ .handle = handle, .bytes = 4 };
    try Kernels.validateDispatch(.{ .op = 1, .n = 1 }, &.{empty}, output);
    try std.testing.expectError(
        error.InvalidGeneratedDispatch,
        Kernels.validateDispatch(.{ .op = 1, .n = 0 }, &.{empty}, output),
    );
    try std.testing.expectError(
        error.InvalidGeneratedDispatch,
        Kernels.validateDispatch(.{ .op = 1, .n = 1 }, &.{.{ .handle = handle, .offset = 2, .bytes = 0 }}, output),
    );
}

test "context generated split pair validates complete contract before active frame mutation" {
    const handles = [_]*anyopaque{
        @ptrFromInt(0x1000), @ptrFromInt(0x2000), @ptrFromInt(0x3000),
        @ptrFromInt(0x4000), @ptrFromInt(0x5000), @ptrFromInt(0x6000),
        @ptrFromInt(0x7000), @ptrFromInt(0x8000), @ptrFromInt(0x9000),
    };
    const stage_inputs = [_]Ref{
        .{ .handle = handles[0], .bytes = 2048 },
        .{ .handle = handles[1], .bytes = 1024 },
        .{ .handle = handles[2], .bytes = 1024 },
        .{ .handle = handles[3], .bytes = 1024 },
        .{ .handle = handles[4], .bytes = 1024 },
        .{ .handle = handles[5], .bytes = 4 },
        .{ .handle = handles[6], .bytes = 1024 },
    };
    const scratch = Ref{ .handle = handles[7], .bytes = 8192 };
    const output = Ref{ .handle = handles[8], .bytes = 2048 };
    const reduce_inputs = [_]Ref{
        scratch,
        .{ .handle = handles[0], .bytes = 4 },
        .{ .handle = handles[0], .bytes = 4 },
        .{ .handle = handles[0], .bytes = 4 },
        .{ .handle = handles[0], .bytes = 4 },
        stage_inputs[5],
        .{ .handle = handles[0], .bytes = 4 },
    };
    const stage = Params{
        .op = 4,
        .n = 2 * 2 * 32,
        .dim = 256,
        .groups = 2,
        .stride = 1,
        .index = 1,
        .reserved = @bitCast(@as(u32, 2)),
    };
    const reduce = Params{
        .op = 6,
        .n = 2 * 32,
        .dim = 256,
        .groups = 2,
        .stride = 1,
        .index = 1,
        .reserved = @bitCast(@as(u32, 2)),
    };
    try Kernels.validateContextSplitPair(stage, &stage_inputs, scratch, reduce, &reduce_inputs, output);

    var invalid_scale = stage;
    invalid_scale.scale = 0;
    try std.testing.expectError(error.InvalidContextSplitDispatch, Kernels.validateContextSplitPair(invalid_scale, &stage_inputs, scratch, reduce, &reduce_inputs, output));
    var short_queries = stage_inputs;
    short_queries[0].bytes = 2047;
    try std.testing.expectError(error.InvalidContextSplitDispatch, Kernels.validateContextSplitPair(stage, &short_queries, scratch, reduce, &reduce_inputs, output));
    var invalid_reduce = reduce;
    invalid_reduce.op = 5;
    try std.testing.expectError(error.InvalidContextSplitDispatch, Kernels.validateContextSplitPair(stage, &stage_inputs, scratch, invalid_reduce, &reduce_inputs, output));
    var wrong_scratch_inputs = reduce_inputs;
    wrong_scratch_inputs[0].offset = 4;
    try std.testing.expectError(error.InvalidContextSplitDispatch, Kernels.validateContextSplitPair(stage, &stage_inputs, scratch, reduce, &wrong_scratch_inputs, output));
    var wrong_status_inputs = reduce_inputs;
    wrong_status_inputs[5] = .{ .handle = handles[5], .offset = 4, .bytes = 4 };
    try std.testing.expectError(error.InvalidContextSplitDispatch, Kernels.validateContextSplitPair(stage, &stage_inputs, scratch, reduce, &wrong_status_inputs, output));
    const scratch_alias_output = Ref{ .handle = scratch.handle, .offset = 16, .bytes = 1024 };
    try std.testing.expectError(error.InvalidContextSplitDispatch, Kernels.validateContextSplitPair(stage, &stage_inputs, scratch, reduce, &reduce_inputs, scratch_alias_output));
    var aliased_stage_inputs = stage_inputs;
    aliased_stage_inputs[0] = .{ .handle = output.handle, .offset = 512, .bytes = 1024 };
    try std.testing.expectError(error.InvalidContextSplitDispatch, Kernels.validateContextSplitPair(stage, &aliased_stage_inputs, scratch, reduce, &reduce_inputs, output));
}

test "context replay split pair rejects stale geometry and aliases before frame mutation" {
    const handles = [_]*anyopaque{ @ptrFromInt(0x1000), @ptrFromInt(0x2000), @ptrFromInt(0x3000), @ptrFromInt(0x4000), @ptrFromInt(0x5000), @ptrFromInt(0x6000), @ptrFromInt(0x7000), @ptrFromInt(0x8000), @ptrFromInt(0x9000) };
    const dim: u32 = 256;
    const vectors: u32 = 16;
    const splits: u32 = 8;
    const scratch_bytes = @as(usize, vectors) * splits * (dim + 2) * @sizeOf(f32);
    const stage = Params{ .op = 2, .n = vectors * splits * 32, .dim = dim, .groups = 8, .position = 512, .stride = 2, .mode = 2, .index = 512, .epsilon = 2, .scale = 0.0625, .reserved = @bitCast(splits) };
    const reduce = Params{ .op = 3, .n = vectors * 32, .dim = dim, .groups = 8, .position = 512, .stride = 2, .mode = 2, .index = 512, .epsilon = 2, .scale = 0.0625, .reserved = @bitCast(splits) };
    const scratch = Ref{ .handle = handles[7], .bytes = scratch_bytes };
    const stage_inputs = [_]Ref{
        .{ .handle = handles[0], .bytes = @as(usize, vectors) * dim * 4 },
        .{ .handle = handles[1], .bytes = 512 * 2 * dim * 2 },
        .{ .handle = handles[2], .bytes = 512 * 2 * dim * 2 },
        .{ .handle = handles[3], .bytes = 2 * 2 * dim * 2 },
        .{ .handle = handles[4], .bytes = 2 * 2 * dim * 2 },
        .{ .handle = handles[5], .bytes = 4 },
        .{ .handle = handles[6], .bytes = 2 * 6 * 4 },
    };
    const reduce_inputs = [_]Ref{ scratch, .{ .handle = handles[8], .bytes = 4 }, .{ .handle = handles[8], .bytes = 4 }, .{ .handle = handles[8], .bytes = 4 }, .{ .handle = handles[8], .bytes = 4 }, stage_inputs[5], .{ .handle = handles[8], .bytes = 4 } };
    const output = Ref{ .handle = handles[8], .offset = 64, .bytes = @as(usize, vectors) * dim * 4 };
    try Kernels.validateReplaySplitPair(stage, &stage_inputs, scratch, reduce, &reduce_inputs, output);
    var changed = reduce;
    changed.position += 1;
    try std.testing.expectError(error.InvalidReplaySplitDispatch, Kernels.validateReplaySplitPair(stage, &stage_inputs, scratch, changed, &reduce_inputs, output));
    var before_base = stage;
    before_base.position = stage.index - 1;
    try std.testing.expectError(error.InvalidReplaySplitDispatch, Kernels.validateReplaySplitPair(before_base, &stage_inputs, scratch, reduce, &reduce_inputs, output));
    var wrong_threads = stage;
    wrong_threads.n -= 32;
    try std.testing.expectError(error.InvalidReplaySplitDispatch, Kernels.validateReplaySplitPair(wrong_threads, &stage_inputs, scratch, reduce, &reduce_inputs, output));
    var short_tail = stage_inputs;
    short_tail[3].bytes -= 2;
    try std.testing.expectError(error.InvalidReplaySplitDispatch, Kernels.validateReplaySplitPair(stage, &short_tail, scratch, reduce, &reduce_inputs, output));
    const aliased_output = Ref{ .handle = scratch.handle, .bytes = output.bytes };
    try std.testing.expectError(error.InvalidReplaySplitDispatch, Kernels.validateReplaySplitPair(stage, &stage_inputs, scratch, reduce, &reduce_inputs, aliased_output));
}

test "attention_match Metal explicit generated access roles preserve RAW WAR WAW and read fanout across scoped policies" {
    if (comptime !options.enable_metal) return error.SkipZigTest;
    const source =
        \\#include <metal_stdlib>
        \\using namespace metal;
        \\struct Params {uint op,n,dim,groups,position,stride,mode,index;float epsilon,scale,theta,reserved;};
        \\kernel void access_chain(device const float* input [[buffer(0)]],device float* output [[buffer(7)]],constant Params&p [[buffer(8)]],uint tid [[thread_position_in_grid]]) {if(tid<p.n)output[tid]=input[tid]*p.scale;}
    ;
    var kernels = try Kernels.initSource(source, "access_chain");
    defer kernels.deinit();
    const count = 256;
    var a = try kernels.buffer(count * 4);
    defer a.deinit();
    var b = try kernels.buffer(count * 4);
    defer b.deinit();
    var c = try kernels.buffer(count * 4);
    defer c.deinit();
    var d = try kernels.buffer(count * 4);
    defer d.deinit();
    const initial_policy = mr.termite_metal_decode_runtime_native_frame_policy_state(kernels.runtime);
    // Exercise restoration of both an existing request and the default policy.
    for ([_]c_int{ 0, 1 }) |previous_requested| {
        try std.testing.expectEqual(@as(c_int, 0), mr.termite_metal_decode_runtime_set_concurrent_planned_dispatch(kernels.runtime, previous_requested));
        const previous_policy = mr.termite_metal_decode_runtime_native_frame_policy_state(kernels.runtime);
        for (0..3) |policy| {
            var conservative_barriers: u64 = 0;
            var conservative_encoders: u64 = 0;
            for ([_]u8{ 0xff, 0x80 }) |mask| {
                if (policy != 0) {
                    try std.testing.expectEqual(@as(c_int, 0), mr.termite_metal_decode_runtime_begin_native_frame_policy(kernels.runtime, @intFromBool(policy == 2), 1));
                    try std.testing.expectEqual(@as(c_int, -3), mr.termite_metal_decode_runtime_begin_native_frame_policy(kernels.runtime, 0, 0));
                    try std.testing.expectEqual(@as(c_int, -3), mr.termite_metal_decode_runtime_set_concurrent_planned_dispatch(kernels.runtime, 0));
                }
                errdefer {
                    if (mr.hasActiveFrame(kernels.runtime)) mr.cancelFrame(kernels.runtime) catch {};
                    if (mr.hasSubmittedFrame(kernels.runtime)) mr.waitFrame(kernels.runtime) catch {};
                    if (policy != 0) _ = mr.termite_metal_decode_runtime_end_native_frame_policy(kernels.runtime);
                }
                @memset(a.floats(), 1);
                @memset(b.floats(), -1);
                @memset(c.floats(), -1);
                @memset(d.floats(), -1);
                try mr.beginFrame(kernels.runtime);
                try std.testing.expectEqual(@as(c_int, -2), mr.termite_metal_decode_runtime_begin_native_frame_policy(kernels.runtime, 1, 1));
                if (policy != 0) try std.testing.expectEqual(@as(c_int, -2), mr.termite_metal_decode_runtime_end_native_frame_policy(kernels.runtime));
                errdefer {
                    if (mr.hasActiveFrame(kernels.runtime)) mr.cancelFrame(kernels.runtime) catch {};
                    if (mr.hasSubmittedFrame(kernels.runtime)) mr.waitFrame(kernels.runtime) catch {};
                }
                try kernels.runCoalescedOnActiveRuntime(kernels.runtime, .{ .op = 1, .n = count, .scale = 2 }, &.{a.ref()}, b.ref(), mask);
                // Two independent consumers share a read-only input.
                try kernels.runCoalescedOnActiveRuntime(kernels.runtime, .{ .op = 1, .n = count, .scale = 3 }, &.{a.ref()}, c.ref(), mask);
                try kernels.runThreadgroupsPipelineCoalescedOnActiveRuntime(kernels.runtime, kernels.pipeline, .{ .op = 1, .n = count, .scale = 5 }, &.{b.ref()}, d.ref(), .{ 4, 1, 1 }, .{ 64, 1, 1 }, mask);
                // Reuse the earlier input as an output, then overwrite b twice.
                try kernels.runCoalescedOnActiveRuntime(kernels.runtime, .{ .op = 1, .n = count, .scale = 7 }, &.{c.ref()}, a.ref(), mask);
                try kernels.runCoalescedOnActiveRuntime(kernels.runtime, .{ .op = 1, .n = count, .scale = 11 }, &.{a.ref()}, b.ref(), mask);
                try kernels.runCoalescedOnActiveRuntime(kernels.runtime, .{ .op = 1, .n = count, .scale = 13 }, &.{c.ref()}, b.ref(), mask);
                try mr.submitFrame(kernels.runtime);
                try mr.waitFrame(kernels.runtime);
                if (policy != 0) try std.testing.expectEqual(@as(c_int, 0), mr.termite_metal_decode_runtime_end_native_frame_policy(kernels.runtime));
                try std.testing.expectEqual(previous_policy, mr.termite_metal_decode_runtime_native_frame_policy_state(kernels.runtime));
                for (a.floats(), b.floats(), c.floats(), d.floats()) |av, bv, cv, dv| {
                    try std.testing.expectEqual(@as(f32, 21), av);
                    try std.testing.expectEqual(@as(f32, 39), bv);
                    try std.testing.expectEqual(@as(f32, 3), cv);
                    try std.testing.expectEqual(@as(f32, 10), dv);
                }
                const snapshot = mr.runtimeMemorySnapshot(kernels.runtime);
                if (mask == 0xff) {
                    conservative_barriers = snapshot.last_frame_planned_barrier_count;
                    conservative_encoders = snapshot.last_frame_compute_encoder_count;
                } else if (policy == 0 and conservative_barriers != 0 and conservative_encoders == 1 and snapshot.last_frame_compute_encoder_count == 1) {
                    try std.testing.expect(snapshot.last_frame_planned_barrier_count < conservative_barriers);
                    try std.testing.expect(snapshot.last_frame_planned_barrier_count > 0);
                }
            }
        }
        try std.testing.expectEqual(@as(c_int, 0), mr.termite_metal_decode_runtime_begin_native_frame_policy(kernels.runtime, 1, 1));
        try mr.beginFrame(kernels.runtime);
        try kernels.runCoalescedOnActiveRuntime(kernels.runtime, .{ .op = 1, .n = count, .scale = 17 }, &.{a.ref()}, b.ref(), 0x80);
        try mr.cancelFrame(kernels.runtime);
        try std.testing.expectEqual(@as(c_int, 0), mr.termite_metal_decode_runtime_end_native_frame_policy(kernels.runtime));
        try std.testing.expectEqual(previous_policy, mr.termite_metal_decode_runtime_native_frame_policy_state(kernels.runtime));
    }
    try std.testing.expectEqual(@as(c_int, 0), mr.termite_metal_decode_runtime_set_concurrent_planned_dispatch(kernels.runtime, @intCast(initial_policy & 1)));
    try std.testing.expectEqual(initial_policy, mr.termite_metal_decode_runtime_native_frame_policy_state(kernels.runtime));
}
