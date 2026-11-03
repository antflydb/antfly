// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Optional measurements on the submitting thread. Nested observations include
//! work in child scopes. They exclude independent worker threads and never
//! pretend that an opaque vendor encode reveals its internal kernel launches.
const build_options = @import("build_options");

/// Actual successful/declined/failed backend callback returns. These entry
/// families do not claim vendor-internal kernel algorithms or completion.
pub const Entry = enum { linear, linear_no_bias, layer_norm, gelu, add, encoder_attention, causal_attention, cross_attention, convolution_1d, convolution_2d, prepared_linear, prepared_linear_pair, prepared_qkv, prepared_linear_into, prepared_qkv_into };
pub const EntryBackend = enum { native, metal, cuda };
pub const EntryOutcome = enum { selected, declined, failed };
pub const FailureText = struct {
    bytes: [64]u8 = @splat(0),
    len: u8 = 0,
    truncated: bool = false,
    pub fn init(value: []const u8) FailureText {
        var result = FailureText{};
        const count = @min(value.len, result.bytes.len);
        @memcpy(result.bytes[0..count], value[0..count]);
        result.len = @intCast(count);
        result.truncated = value.len != count;
        return result;
    }
};
const entry_count = @typeInfo(Entry).@"enum".field_names.len;
const entry_slots = entry_count * 3;
const EntryCounts = struct { selected: u64 = 0, declined: u64 = 0, failed: u64 = 0, identity_changes: u64 = 0 };
const EntryState = struct { counters: EntryCounts = .{}, implementation: usize = 0, failure: FailureText = .{} };
threadlocal var entry_states: [entry_slots]EntryState = @splat(.{});
pub const EntryReport = struct {
    backend: EntryBackend = .native,
    family: Entry = .linear,
    successful_returns: u64 = 0,
    declined_returns: u64 = 0,
    failed_returns: u64 = 0,
    /// Process-local actual selected callback identity, never a callable owner.
    last_implementation_identity: usize = 0,
    implementation_changes: u64 = 0,
    last_failure: FailureText = .{},
};
pub const Entries = struct {
    scope: enum { submitting_thread_instrumented_backend_entries } = .submitting_thread_instrumented_backend_entries,
    unit: enum { callback_returns_not_device_kernels } = .callback_returns_not_device_kernels,
    coverage: enum { instrumented_entries_only } = .instrumented_entries_only,
    vendor_internal_algorithms: enum { opaque_vendor } = .opaque_vendor,
    records: [8]EntryReport = @splat(.{}),
    count: u8 = 0,
    truncated: bool = false,
    observed_families: usize = 0,
};
pub inline fn enabled() bool {
    return native_depth != 0;
}
pub fn backendEntry(backend: anytype, family: Entry, implementation: usize, outcome: EntryOutcome, failure: ?[]const u8) void {
    if (!enabled()) return;
    const selected: usize = switch (backend) {
        .native => 0,
        .metal => 1,
        .cuda => 2,
        else => return,
    };
    const state = &entry_states[selected * entry_count + @backingInt(family)];
    if (state.implementation != 0 and state.implementation != implementation) state.counters.identity_changes +|= 1;
    state.implementation = implementation;
    switch (outcome) {
        .selected => state.counters.selected +|= 1,
        .declined => state.counters.declined +|= 1,
        .failed => state.counters.failed +|= 1,
    }
    if (failure) |value| state.failure = FailureText.init(value);
}
fn entryStarts() [entry_slots]EntryCounts {
    var result: [entry_slots]EntryCounts = undefined;
    for (&result, entry_states) |*item, state| item.* = state.counters;
    return result;
}
fn entriesSince(before: [entry_slots]EntryCounts) Entries {
    var result = Entries{};
    for (entry_states, before, 0..) |state, start, index| {
        const selected = state.counters.selected -| start.selected;
        const declined = state.counters.declined -| start.declined;
        const failed = state.counters.failed -| start.failed;
        if (selected == 0 and declined == 0 and failed == 0) continue;
        result.observed_families += 1;
        if (result.count == result.records.len) {
            result.truncated = true;
            continue;
        }
        result.records[result.count] = .{ .backend = @fromBackingInt(@intCast(index / entry_count)), .family = @fromBackingInt(@intCast(index % entry_count)), .successful_returns = selected, .declined_returns = declined, .failed_returns = failed, .last_implementation_identity = state.implementation, .implementation_changes = state.counters.identity_changes -| start.identity_changes, .last_failure = if (failed != 0) state.failure else .{} };
        result.count += 1;
    }
    return result;
}

/// Exact project-owned CUDA launch arguments observed only after driver success.
/// Function identities are resolved CUfunction handles, not planner names.
pub const KernelRecord = struct {
    context_identity: usize = 0,
    function_identity: usize = 0,
    stream_identity: usize = 0,
    cooperative: bool = false,
    grid: [3]u32 = @splat(0),
    block: [3]u32 = @splat(0),
    shared_bytes: u32 = 0,
    accepted_direct_calls: u64 = 0,
    capture_recording_calls: u64 = 0,
    foreign_stream_calls: u64 = 0,
    launch_geometry_changes: u64 = 0,
};
pub const Kernels = struct {
    scope: enum { submitting_thread_project_cuda_launchers } = .submitting_thread_project_cuda_launchers,
    unit: enum { accepted_driver_calls_not_completed_kernels } = .accepted_driver_calls_not_completed_kernels,
    device_dynamic_and_vendor_kernels: enum { opaque_vendor } = .opaque_vendor,
    symbol_names: enum { unavailable } = .unavailable,
    records: [8]KernelRecord = @splat(.{}),
    count: u8 = 0,
    total_calls: u64 = 0,
    truncated: bool = false,
    unrecorded_calls: u64 = 0,
};
threadlocal var active_kernels: ?*KernelScope = null;
/// Resolved MTLComputePipelineState numerical identities, not shader names or
/// vendor-internal algorithms. Unknown binding and truncation stay explicit.
pub const MetalDispatch = struct {
    encoder_identity: usize = 0,
    pipeline_identity: usize = 0,
    kind: enum(u32) { threads, threadgroups } = .threads,
    grid: [3]u64 = @splat(0),
    threads: [3]u64 = @splat(0),
    encoded_messages: u64 = 0,
    geometry_changes: u64 = 0,
};
pub const MetalDispatches = struct {
    scope: enum { submitting_thread_project_bind_and_dispatch_wrappers } = .submitting_thread_project_bind_and_dispatch_wrappers,
    unit: enum { dispatch_method_returns_not_completed_kernels } = .dispatch_method_returns_not_completed_kernels,
    vendor_internals: enum { opaque_vendor } = .opaque_vendor,
    identities: enum { observed_object_addresses_not_lifetime_authority } = .observed_object_addresses_not_lifetime_authority,
    pipeline_labels_and_specialization_constants: enum { unavailable } = .unavailable,
    records: [8]MetalDispatch = @splat(.{}),
    count: u8 = 0,
    total_messages: u64 = 0,
    unknown_pipeline_messages: u64 = 0,
    truncated: bool = false,
    unrecorded_messages: u64 = 0,
    association_table_overflowed_during_enclosing_scope: bool = false,
};
pub const NativeBody = struct {
    family: enum { activation_simd_cube_first_scalar_left } = .activation_simd_cube_first_scalar_left,
    activation: []const u8 = "",
    vector_width: usize = 0,
    completed_calls: u64 = 0,
    vector_iterations: u64 = 0,
    scalar_elements: u64 = 0,
};
pub const NativeBodies = struct {
    scope: enum { submitting_thread_instrumented_terminal_activation_bodies } = .submitting_thread_instrumented_terminal_activation_bodies,
    coverage: enum { excludes_uninstrumented_bodies_and_independent_workers } = .excludes_uninstrumented_bodies_and_independent_workers,
    records: [8]NativeBody = @splat(.{}),
    count: u8 = 0,
    total_calls: u64 = 0,
    truncated: bool = false,
    unrecorded_calls: u64 = 0,
};
pub fn antfly_runtime_observe_metal_dispatch(encoder: usize, pipeline: usize, kind: u32, gx: u64, gy: u64, gz: u64, tx: u64, ty: u64, tz: u64, overflow: bool) callconv(.c) void {
    var active = active_kernels;
    while (active) |scope| : (active = scope.previous) {
        const values = &scope.metal;
        values.total_messages +|= 1;
        values.association_table_overflowed_during_enclosing_scope = values.association_table_overflowed_during_enclosing_scope or overflow;
        if (pipeline == 0 or encoder == 0 or kind > 1) {
            values.unknown_pipeline_messages +|= 1;
            continue;
        }
        var found: ?*MetalDispatch = null;
        for (values.records[0..values.count]) |*record| if (record.encoder_identity == encoder and record.pipeline_identity == pipeline and @backingInt(record.kind) == kind) {
            found = record;
            break;
        };
        const grid = [3]u64{ gx, gy, gz };
        const threads = [3]u64{ tx, ty, tz };
        if (found == null) {
            if (values.count == values.records.len) {
                values.truncated = true;
                values.unrecorded_messages +|= 1;
                continue;
            }
            found = &values.records[values.count];
            values.count += 1;
            found.?.* = .{ .encoder_identity = encoder, .pipeline_identity = pipeline, .kind = @fromBackingInt(@intCast(kind)), .grid = grid, .threads = threads };
        }
        const record = found.?;
        if (!@import("std").meta.eql(record.grid, grid) or !@import("std").meta.eql(record.threads, threads)) record.geometry_changes +|= 1;
        record.grid = grid;
        record.threads = threads;
        record.encoded_messages +|= 1;
    }
}
pub inline fn nativeActivationBody(comptime activation: []const u8, vector_width: usize, vector_iterations: usize, scalar_elements: usize) void {
    if (active_kernels == null) return;
    nativeActivationBodyEnabled(activation, vector_width, vector_iterations, scalar_elements);
}
noinline fn nativeActivationBodyEnabled(activation: []const u8, vector_width: usize, vector_iterations: usize, scalar_elements: usize) void {
    var active = active_kernels;
    while (active) |scope| : (active = scope.previous) {
        const values = &scope.native_bodies;
        values.total_calls +|= 1;
        var found: ?*NativeBody = null;
        for (values.records[0..values.count]) |*record| if (record.vector_width == vector_width and @import("std").mem.eql(u8, record.activation, activation)) {
            found = record;
            break;
        };
        if (found == null) {
            if (values.count == values.records.len) {
                values.truncated = true;
                values.unrecorded_calls +|= 1;
                continue;
            }
            found = &values.records[values.count];
            values.count += 1;
            found.?.* = .{ .activation = activation, .vector_width = vector_width };
        }
        found.?.completed_calls +|= 1;
        found.?.vector_iterations +|= vector_iterations;
        found.?.scalar_elements +|= scalar_elements;
    }
}

const MetalDispatchSink = *const fn (usize, usize, u32, u64, u64, u64, u64, u64, u64, bool) callconv(.c) void;
extern fn termite_metal_observation_exchange_dispatch_sink(sink: ?MetalDispatchSink) ?MetalDispatchSink;

pub const KernelScope = struct {
    previous: ?*KernelScope = null,
    previous_metal_sink: ?MetalDispatchSink = null,
    values: Kernels = .{},
    metal: MetalDispatches = .{},
    native_bodies: NativeBodies = .{},
    attached: bool = false,
    /// Must be called only after the owner reaches final address-stable storage.
    pub fn begin(self: *KernelScope) void {
        self.* = .{ .previous = active_kernels, .attached = true };
        active_kernels = self;
        // Referencing the callback here forces only observing link roots to
        // emit it. C numerical entrypoints have no required Zig export.
        if (comptime build_options.enable_metal)
            self.previous_metal_sink = termite_metal_observation_exchange_dispatch_sink(antfly_runtime_observe_metal_dispatch);
    }
    pub fn finish(self: *KernelScope) Kernels {
        @import("std").debug.assert(self.attached and active_kernels == self);
        if (comptime build_options.enable_metal)
            _ = termite_metal_observation_exchange_dispatch_sink(self.previous_metal_sink);
        active_kernels = self.previous;
        self.attached = false;
        return self.values;
    }
};
pub inline fn cudaKernel(context: usize, function: usize, stream: usize, cooperative: bool, grid: [3]u32, block: [3]u32, shared: u32, captured: bool, foreign: bool) void {
    if (active_kernels == null) return;
    cudaKernelEnabled(context, function, stream, cooperative, grid, block, shared, captured, foreign);
}
noinline fn cudaKernelEnabled(context: usize, function: usize, stream: usize, cooperative: bool, grid: [3]u32, block: [3]u32, shared: u32, captured: bool, foreign: bool) void {
    var active = active_kernels;
    while (active) |scope| : (active = scope.previous) {
        scope.values.total_calls +|= 1;
        var selected: ?*KernelRecord = null;
        for (scope.values.records[0..scope.values.count]) |*record| if (record.context_identity == context and record.function_identity == function and record.stream_identity == stream and record.cooperative == cooperative) {
            selected = record;
            break;
        };
        if (selected == null) {
            if (scope.values.count == scope.values.records.len) {
                scope.values.truncated = true;
                scope.values.unrecorded_calls +|= 1;
                continue;
            }
            const record = &scope.values.records[scope.values.count];
            scope.values.count += 1;
            record.* = .{ .context_identity = context, .function_identity = function, .stream_identity = stream, .cooperative = cooperative, .grid = grid, .block = block, .shared_bytes = shared };
            selected = record;
        }
        const record = selected.?;
        if (!@import("std").meta.eql(record.grid, grid) or !@import("std").meta.eql(record.block, block) or record.shared_bytes != shared) record.launch_geometry_changes +|= 1;
        record.grid = grid;
        record.block = block;
        record.shared_bytes = shared;
        if (captured) record.capture_recording_calls +|= 1 else record.accepted_direct_calls +|= 1;
        if (foreign) record.foreign_stream_calls +|= 1;
    }
}

pub const Counters = struct {
    native_assigned_dispatches: u64 = 0,
    backend_entries: Entries = .{},
    metal_encoded_dispatches: ?u64 = null,
    metal_command_submissions: ?u64 = null,
    metal_command_waits: ?u64 = null,
    metal_vendor_encodes: ?u64 = null,
};

threadlocal var native_depth: usize = 0;
threadlocal var native_dispatches: u64 = 0;

extern fn termite_metal_observation_begin() void;
extern fn termite_metal_observation_end() void;
extern fn termite_metal_observation_snapshot(values: [*]u64) void;

/// Record only completed execution, never a descriptor eligibility query.
pub inline fn nativeAssignedDispatch() void {
    if (native_depth != 0) native_dispatches +|= 1;
}

fn metalCounters() [4]u64 {
    var values: [4]u64 = @splat(0);
    if (comptime build_options.enable_metal) termite_metal_observation_snapshot(&values);
    return values;
}

pub const Scope = struct {
    active: bool = false,
    native_start: u64 = 0,
    entry_start: [entry_slots]EntryCounts = @splat(.{}),
    metal_start: [4]u64 = @splat(0),

    /// Counters live in thread-local storage, so this returned owner can move.
    /// Its lifetime must remain on this submitting thread.
    pub fn init() Scope {
        native_depth += 1;
        if (comptime build_options.enable_metal) termite_metal_observation_begin();
        return .{ .active = true, .native_start = native_dispatches, .entry_start = entryStarts(), .metal_start = metalCounters() };
    }

    pub fn snapshot(self: Scope) Counters {
        var result = Counters{ .native_assigned_dispatches = native_dispatches -| self.native_start, .backend_entries = entriesSince(self.entry_start) };
        if (comptime build_options.enable_metal) {
            const after = metalCounters();
            result.metal_encoded_dispatches = after[0] -| self.metal_start[0];
            result.metal_command_waits = after[1] -| self.metal_start[1];
            result.metal_command_submissions = after[2] -| self.metal_start[2];
            result.metal_vendor_encodes = after[3] -| self.metal_start[3];
        }
        return result;
    }

    pub fn deinit(self: *Scope) void {
        if (!self.active) return;
        self.active = false;
        native_depth -= 1;
        if (comptime build_options.enable_metal) termite_metal_observation_end();
    }
};

test "Metal dispatch bridge links without SDK work and restores nested and foreign callbacks" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const std = @import("std");
    const Foreign = struct {
        threadlocal var calls: usize = 0;
        fn observe(_: usize, _: usize, _: u32, _: u64, _: u64, _: u64, _: u64, _: u64, _: u64, _: bool) callconv(.c) void {
            calls += 1;
        }
    };
    const original = termite_metal_observation_exchange_dispatch_sink(Foreign.observe);
    defer _ = termite_metal_observation_exchange_dispatch_sink(original);
    Foreign.calls = 0;
    var parent = KernelScope{};
    parent.begin();
    var child = KernelScope{};
    child.begin();
    // Read the actual C TLS slot, then restore it before emitting evidence.
    const nested_sink = termite_metal_observation_exchange_dispatch_sink(null);
    _ = termite_metal_observation_exchange_dispatch_sink(nested_sink);
    nested_sink.?(1, 2, 0, 32, 1, 1, 32, 1, 1, false);
    _ = child.finish();
    const parent_sink = termite_metal_observation_exchange_dispatch_sink(null);
    _ = termite_metal_observation_exchange_dispatch_sink(parent_sink);
    parent_sink.?(1, 2, 0, 64, 1, 1, 32, 1, 1, false);
    _ = parent.finish();
    const restored = termite_metal_observation_exchange_dispatch_sink(null);
    try std.testing.expectEqual(@intFromPtr(&Foreign.observe), @intFromPtr(restored.?));
    restored.?(1, 2, 0, 1, 1, 1, 1, 1, 1, false);
    try std.testing.expectEqual(@as(usize, 1), Foreign.calls);
    try std.testing.expectEqual(@as(u64, 1), child.metal.total_messages);
    try std.testing.expectEqual(@as(u64, 2), parent.metal.total_messages);
    try std.testing.expectEqual(@as(u64, 1), parent.metal.records[0].geometry_changes);
    // The previously empty root remains empty after its owner retires.
    var root = KernelScope{};
    root.begin();
    _ = root.finish();
    try std.testing.expect(termite_metal_observation_exchange_dispatch_sink(null) == null);
}
