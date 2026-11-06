// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Optional measurements on the submitting thread. Nested observations include
//! work in child scopes. Joined worker values are explicit; unreported workers
//! remain excluded. These observations never
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
    native_dispatches: NativeDispatches = .{},
    native_workers: NativeWorkers = .{},
    quant_worker_callbacks: NativeQuantWorkerCallbacks = .{},
    attention_programs: NativeAttentionPrograms = .{},
    activation_programs: NativeActivationPrograms = .{},
    native_fallbacks: NativeFallbacks = .{},
    native_programs: NativePrograms = .{},
    native_program_bank: ?NativeProgramBank = null,
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

/// Existing outer dispatch note sites have explicit phase units. A note before
/// a fallible call is not a completed body, and a successful outer return does
/// not enumerate subordinate vendor kernels or independently submitting workers.
pub const NativeDispatchPhase = enum { before_dispatch, successful_return };
pub const NativeDispatch = struct {
    /// Compiler-owned enum tag text, never a borrow from caller data.
    family: []const u8 = "",
    phase: NativeDispatchPhase = .before_dispatch,
    events: u64 = 0,
};
pub const NativeDispatches = struct {
    scope: enum { submitting_thread_outer_quantized_dispatch_notes } = .submitting_thread_outer_quantized_dispatch_notes,
    unit: enum { phased_note_events_not_terminal_body_or_worker_counts } = .phased_note_events_not_terminal_body_or_worker_counts,
    coverage: enum { excludes_unnoted_branches_and_inner_microkernel_counters } = .excludes_unnoted_branches_and_inner_microkernel_counters,
    label_source: enum { native_outer_dispatch_note_tags } = .native_outer_dispatch_note_tags,
    resolved_terminal_schedule_identity: enum { unavailable } = .unavailable,
    independent_worker_dispatches: enum { unobserved } = .unobserved,
    vendor_internals: enum { opaque_vendor } = .opaque_vendor,
    records: [8]NativeDispatch = @splat(.{}),
    count: u8 = 0,
    total_events: u64 = 0,
    before_dispatch_events: u64 = 0,
    successful_return_events: u64 = 0,
    truncated: bool = false,
    unrecorded_events: u64 = 0,
};

pub inline fn nativeDispatchFamily(kind: anytype, phase: NativeDispatchPhase) void {
    if (active_kernels == null) return;
    nativeDispatchFamilyEnabled(@tagName(kind), phase);
}
noinline fn nativeDispatchFamilyEnabled(family: []const u8, phase: NativeDispatchPhase) void {
    var active = active_kernels;
    while (active) |scope| : (active = scope.previous) {
        const values = &scope.native_dispatches;
        values.total_events +|= 1;
        switch (phase) {
            .before_dispatch => values.before_dispatch_events +|= 1,
            .successful_return => values.successful_return_events +|= 1,
        }
        var found: ?*NativeDispatch = null;
        for (values.records[0..values.count]) |*record| if (record.phase == phase and @import("std").mem.eql(u8, record.family, family)) {
            found = record;
            break;
        };
        if (found == null) {
            if (values.count == values.records.len) {
                values.truncated = true;
                values.unrecorded_events +|= 1;
                continue;
            }
            found = &values.records[values.count];
            values.count += 1;
            found.?.* = .{ .family = family, .phase = phase };
        }
        found.?.events +|= 1;
    }
}

test "Native outer dispatch note scopes preserve phases nesting truncation and detached path" {
    const std = @import("std");
    const Family = enum { same_family, family_a, family_b, family_c, family_d, family_e, family_f, family_g };
    var outer = KernelScope{};
    outer.begin();
    defer if (outer.attached) {
        _ = outer.finish();
    };
    nativeDispatchFamily(Family.same_family, .before_dispatch);
    var inner = KernelScope{};
    inner.begin();
    defer if (inner.attached) {
        _ = inner.finish();
    };
    nativeDispatchFamily(Family.same_family, .successful_return);
    _ = inner.finish();
    try std.testing.expectEqual(@as(u64, 1), inner.native_dispatches.total_events);
    try std.testing.expectEqual(@as(u64, 2), outer.native_dispatches.total_events);
    try std.testing.expectEqual(@as(u8, 2), outer.native_dispatches.count);
    inline for (.{ Family.family_a, Family.family_b, Family.family_c, Family.family_d, Family.family_e, Family.family_f, Family.family_g }) |kind| nativeDispatchFamily(kind, .before_dispatch);
    _ = outer.finish();
    try std.testing.expectEqual(@as(u64, 9), outer.native_dispatches.total_events);
    try std.testing.expectEqual(@as(u64, 8), outer.native_dispatches.before_dispatch_events);
    try std.testing.expectEqual(@as(u64, 1), outer.native_dispatches.successful_return_events);
    try std.testing.expect(outer.native_dispatches.truncated);
    try std.testing.expectEqual(@as(u64, 1), outer.native_dispatches.unrecorded_events);
    nativeDispatchFamily(Family.same_family, .successful_return);
    try std.testing.expect(active_kernels == null);
    try std.testing.expectEqual(@as(u64, 9), outer.native_dispatches.total_events);
}

/// Returned values from actual joined project-owned prepared/dense GEMM workers.
/// Each worker entry is a leaf invocation, not an MR tile or CPU instruction.
pub const NativeWorkers = struct {
    scope: enum { authored_dense_gemm_body_returns_and_joined_worker_values } = .authored_dense_gemm_body_returns_and_joined_worker_values,
    unit: enum { authored_body_entries_and_successful_returns } = .authored_body_entries_and_successful_returns,
    coverage: enum { prepared_and_dense_gemm_other_schedules_unobserved_vendor_opaque } = .prepared_and_dense_gemm_other_schedules_unobserved_vendor_opaque,
    portable_packed_rows_entries: u64 = 0,
    portable_packed_rows_returns: u64 = 0,
    x86_avx2_prepared_entries: u64 = 0,
    x86_avx2_prepared_returns: u64 = 0,
    dispatched_workers: u64 = 0,
    not_entered_workers: u64 = 0,
    /// Order is lower-library SelectedBody: portable normal, AVX2 normal,
    /// portable transposed F32, AVX2 transposed F32, portable transposed F16,
    /// AVX2 transposed F16. These are actual successful leaf returns, separate
    /// from worker counts (a column worker can invoke several row leaves).
    dense_successful_leaf_returns: [6]u64 = @splat(0),
    dense_dispatched_workers: u64 = 0,
    dense_entered_workers: u64 = 0,
    dense_returned_workers: u64 = 0,
    dense_opaque_vendor_returns: u64 = 0,
    dense_unobserved_callback_returns: u64 = 0,
    /// Actual direct authored compatibility body, separate from dense worker
    /// units and BLAS API returns. This does not enumerate vendor internals.
    transposed_a_body_identity: usize = 0,
    transposed_a_successful_body_returns: u64 = 0,
    transposed_a_vendor_api_returns: u64 = 0,
};
pub inline fn kernelEnabled() bool {
    return active_kernels != null;
}
pub fn nativePreparedWorkers(value: anytype) void {
    var cursor = active_kernels;
    while (cursor) |scope| : (cursor = scope.previous) {
        const result = &scope.native_workers;
        result.portable_packed_rows_entries +|= value.entries[0];
        result.portable_packed_rows_returns +|= value.successful_returns[0];
        result.x86_avx2_prepared_entries +|= value.entries[1];
        result.x86_avx2_prepared_returns +|= value.successful_returns[1];
        result.dispatched_workers +|= value.dispatched_workers;
        result.not_entered_workers +|= value.not_entered_workers;
    }
}

test "Native joined worker observations preserve nested partial entry and return units" {
    const std = @import("std");
    const Value = struct { entries: [2]u64, successful_returns: [2]u64, dispatched_workers: u64, not_entered_workers: u64 };
    var outer: KernelScope = .{};
    outer.begin();
    var inner: KernelScope = .{};
    inner.begin();
    nativePreparedWorkers(Value{ .entries = .{ 2, 1 }, .successful_returns = .{ 2, 0 }, .dispatched_workers = 4, .not_entered_workers = 1 });
    _ = inner.finish();
    nativePreparedWorkers(Value{ .entries = .{ 1, 0 }, .successful_returns = .{ 1, 0 }, .dispatched_workers = 1, .not_entered_workers = 0 });
    _ = outer.finish();
    try std.testing.expectEqual(@as(u64, 3), outer.native_workers.portable_packed_rows_entries);
    try std.testing.expectEqual(@as(u64, 2), inner.native_workers.portable_packed_rows_returns);
    try std.testing.expectEqual(@as(u64, 0), outer.native_workers.x86_avx2_prepared_returns);
    try std.testing.expectEqual(@as(u64, 1), outer.native_workers.not_entered_workers);
}

/// Actual declined optional authored-route attempts, not invocation failures.
pub const NativeFallbackFamily = enum { prepared_dense_projection, canonical_panel_projection };
pub const NativeFallbackReason = enum { projection_eligibility_policy_or_shape, incompatible_weight_owner_or_layout, required_cached_panels_unavailable, panel_preparation_out_of_memory, panel_memory_budget_exceeded, panel_slice_alignment_or_extent };
pub const NativeFallback = struct { family: NativeFallbackFamily = .prepared_dense_projection, reason: NativeFallbackReason = .projection_eligibility_policy_or_shape, attempts: u64 = 0 };
pub const NativeFallbacks = struct {
    scope: enum { instrumented_optional_projection_attempts_on_submitting_thread } = .instrumented_optional_projection_attempts_on_submitting_thread,
    records: [8]NativeFallback = @splat(.{}),
    count: u8 = 0,
    total_attempts: u64 = 0,
    truncated_attempts: u64 = 0,
};
pub inline fn nativeFallback(family: NativeFallbackFamily, reason: NativeFallbackReason) void {
    if (active_kernels == null) return;
    nativeFallbackEnabled(family, reason);
}
noinline fn nativeFallbackEnabled(family: NativeFallbackFamily, reason: NativeFallbackReason) void {
    var cursor = active_kernels;
    while (cursor) |scope| : (cursor = scope.previous) {
        const result = &scope.native_fallbacks;
        result.total_attempts +|= 1;
        var found: ?*NativeFallback = null;
        for (result.records[0..result.count]) |*item| if (item.family == family and item.reason == reason) {
            found = item;
            break;
        };
        if (found == null) {
            if (result.count == result.records.len) {
                result.truncated_attempts +|= 1;
                continue;
            }
            found = &result.records[result.count];
            result.count += 1;
            found.?.* = .{ .family = family, .reason = reason };
        }
        found.?.attempts +|= 1;
    }
}

pub fn nativeDenseWorkers(value: anytype) void {
    var cursor = active_kernels;
    while (cursor) |scope| : (cursor = scope.previous) {
        const result = &scope.native_workers;
        for (&result.dense_successful_leaf_returns, value.successful_leaf_returns) |*count, observed| count.* +|= observed;
        result.dense_dispatched_workers +|= value.dispatched_workers;
        result.dense_entered_workers +|= value.entered_workers;
        result.dense_returned_workers +|= value.returned_workers;
        if (comptime @hasField(@TypeOf(value), "opaque_vendor_returns")) result.dense_opaque_vendor_returns +|= value.opaque_vendor_returns;
        if (comptime @hasField(@TypeOf(value), "unobserved_callback_returns")) result.dense_unobserved_callback_returns +|= value.unobserved_callback_returns;
    }
}

/// Actual original worker callback identities invoked by the authored quantized
/// scheduler, including activation preparation. These are not inferred terminal
/// dot/MR kernel counts, vendor kernels or generic observation-proxy identities.
pub const NativeQuantWorkerCallback = struct {
    function_identity: usize = 0,
    configured_dot_function_identities: [2]usize = @splat(0),
    configured_vectorized: ?bool = null,
    entered_workers: u64 = 0,
    returned_workers: u64 = 0,
    context_error_returns: u64 = 0,
    not_entered_workers: u64 = 0,
    last_context_error: FailureText = .{},
};
pub const NativeQuantWorkerCallbacks = struct {
    scope: enum { joined_authored_quant_scheduler_original_worker_callbacks } = .joined_authored_quant_scheduler_original_worker_callbacks,
    unit: enum { worker_callback_entries_returns_and_recorded_context_errors } = .worker_callback_entries_returns_and_recorded_context_errors,
    configured_dot_scope: enum { immutable_context_bindings_not_dot_invocation_counts } = .immutable_context_bindings_not_dot_invocation_counts,
    inner_MR_and_vendor_terminal_coverage: enum { unavailable } = .unavailable,
    records: [8]NativeQuantWorkerCallback = @splat(.{}),
    count: u8 = 0,
    submitted_workers: u64 = 0,
    entered_workers: u64 = 0,
    returned_workers: u64 = 0,
    context_error_returns: u64 = 0,
    not_entered_workers: u64 = 0,
    unrepresented_worker_records: u64 = 0,
    unobserved_capacity_workers: u64 = 0,
};
pub fn nativeQuantWorkerBatchOmitted(workers: usize) void {
    var cursor = active_kernels;
    while (cursor) |scope| : (cursor = scope.previous) {
        scope.quant_worker_callbacks.submitted_workers +|= workers;
        scope.quant_worker_callbacks.unobserved_capacity_workers +|= workers;
    }
}
pub fn nativeQuantWorkerReturn(identity: usize, dots: [2]usize, vectorized: ?bool, entered: bool, returned: bool, failure: ?[]const u8) void {
    const std = @import("std");
    var cursor = active_kernels;
    while (cursor) |scope| : (cursor = scope.previous) {
        const result = &scope.quant_worker_callbacks;
        result.submitted_workers +|= 1;
        result.entered_workers +|= @intFromBool(entered);
        result.returned_workers +|= @intFromBool(returned);
        result.context_error_returns +|= @intFromBool(returned and failure != null);
        result.not_entered_workers +|= @intFromBool(!entered);
        var found: ?*NativeQuantWorkerCallback = null;
        for (result.records[0..result.count]) |*item| if (item.function_identity == identity and std.meta.eql(item.configured_dot_function_identities, dots) and item.configured_vectorized == vectorized) {
            found = item;
            break;
        };
        if (found == null) {
            if (result.count == result.records.len) {
                result.unrepresented_worker_records +|= 1;
                continue;
            }
            found = &result.records[result.count];
            result.count += 1;
            found.?.* = .{ .function_identity = identity, .configured_dot_function_identities = dots, .configured_vectorized = vectorized };
        }
        found.?.entered_workers +|= @intFromBool(entered);
        found.?.returned_workers +|= @intFromBool(returned);
        found.?.not_entered_workers +|= @intFromBool(!entered);
        if (returned) if (failure) |message| {
            found.?.context_error_returns +|= 1;
            found.?.last_context_error = FailureText.init(message);
        };
    }
}

test "Native quant worker value records preserve nested partial error ownership and bounded identities" {
    const std = @import("std");
    var outer: KernelScope = .{};
    outer.begin();
    var inner: KernelScope = .{};
    inner.begin();
    var failure: [80]u8 = @splat('x');
    nativeQuantWorkerReturn(1, .{ 11, 0 }, false, true, true, &failure);
    @memset(&failure, 'z');
    nativeQuantWorkerReturn(2, .{ 0, 0 }, null, false, false, null);
    _ = inner.finish();
    for (3..10) |identity| nativeQuantWorkerReturn(identity, .{ 0, 0 }, null, true, true, null);
    _ = outer.finish();
    const retained = outer.quant_worker_callbacks;
    try std.testing.expectEqual(@as(u64, 9), retained.submitted_workers);
    try std.testing.expectEqual(@as(u64, 8), retained.entered_workers);
    try std.testing.expectEqual(@as(u64, 1), retained.context_error_returns);
    try std.testing.expectEqual(@as(u64, 1), retained.not_entered_workers);
    try std.testing.expectEqual(@as(u8, 8), retained.count);
    try std.testing.expectEqual(@as(u64, 1), retained.unrepresented_worker_records);
    try std.testing.expect(retained.records[0].last_context_error.truncated);
    for (retained.records[0].last_context_error.bytes) |byte| try std.testing.expectEqual(@as(u8, 'x'), byte);
    try std.testing.expectEqual(@as(u64, 2), inner.quant_worker_callbacks.submitted_workers);
}

/// Actual authored attention program callbacks/direct heads, returned after the
/// original scheduler drains. Inner GEMM leaf returns use NativeWorkers units;
/// these records never claim MR/dot counts, vendor internals or other families.
pub const NativeAttentionProgram = struct {
    family: []const u8 = "",
    program_identity: usize = 0,
    original_callback_identity: usize = 0,
    entries: u64 = 0,
    returns: u64 = 0,
    error_returns: u64 = 0,
    not_entered: u64 = 0,
    last_error: FailureText = .{},
};
pub const NativeAttentionPrograms = struct {
    scope: enum { authored_attention_program_values_after_join_or_direct_return } = .authored_attention_program_values_after_join_or_direct_return,
    coverage: enum { flash_segment_relative_dense_rows_other_families_unobserved } = .flash_segment_relative_dense_rows_other_families_unobserved,
    records: [8]NativeAttentionProgram = @splat(.{}),
    count: u8 = 0,
    submitted: u64 = 0,
    entries: u64 = 0,
    returns: u64 = 0,
    error_returns: u64 = 0,
    not_entered: u64 = 0,
    unrecorded_events: u64 = 0,
    unobserved_capacity_jobs: u64 = 0,
};
pub fn nativeAttentionPrograms(value: anytype) void {
    nativeDenseWorkers(value.gemm_values);
    var cursor = active_kernels;
    while (cursor) |scope| : (cursor = scope.previous) {
        const out = &scope.attention_programs;
        out.submitted +|= value.submitted;
        out.entries +|= value.entries;
        out.returns +|= value.returns;
        out.error_returns +|= value.error_returns;
        out.not_entered +|= value.not_entered;
        out.unrecorded_events +|= value.unrecorded_events;
        out.unobserved_capacity_jobs +|= value.unobserved_capacity_jobs;
        for (value.records[0..value.count]) |item| {
            const family = @tagName(item.family); // enum tag storage has static lifetime
            var found: ?*NativeAttentionProgram = null;
            for (out.records[0..out.count]) |*record| if (@import("std").mem.eql(u8, record.family, family) and record.program_identity == item.program_identity and record.original_callback_identity == item.original_callback_identity) {
                found = record;
                break;
            };
            if (found == null) {
                if (out.count == out.records.len) {
                    out.unrecorded_events +|= item.entries +| item.not_entered;
                    continue;
                }
                found = &out.records[out.count];
                out.count += 1;
                found.?.* = .{ .family = family, .program_identity = item.program_identity, .original_callback_identity = item.original_callback_identity };
            }
            found.?.entries +|= item.entries;
            found.?.returns +|= item.returns;
            found.?.error_returns +|= item.error_returns;
            found.?.not_entered +|= item.not_entered;
            if (item.last_error) |err| found.?.last_error = FailureText.init(@errorName(err));
        }
    }
}

test "Native attention returned error values survive nested observation and bounded projection" {
    const Family = enum { authored_test };
    const Record = struct { family: Family = .authored_test, program_identity: usize = 1, original_callback_identity: usize = 2, entries: u64 = 1, returns: u64 = 1, error_returns: u64 = 1, not_entered: u64 = 0, last_error: ?anyerror = error.ActualAttentionBodyCanceled };
    const Value = struct { records: [1]Record = .{.{}}, count: u8 = 1, submitted: u64 = 1, entries: u64 = 1, returns: u64 = 1, error_returns: u64 = 1, not_entered: u64 = 0, unrecorded_events: u64 = 0, unobserved_capacity_jobs: u64 = 0, gemm_values: struct { successful_leaf_returns: [6]u64 = @splat(0), dispatched_workers: u64 = 0, entered_workers: u64 = 0, returned_workers: u64 = 0 } = .{} };
    var parent: KernelScope = .{};
    parent.begin();
    var child: KernelScope = .{};
    child.begin();
    nativeAttentionPrograms(Value{});
    _ = child.finish();
    _ = parent.finish();
    const copy = child.attention_programs;
    child.attention_programs = .{};
    try @import("std").testing.expectEqual(@as(u64, 1), parent.attention_programs.error_returns);
    try @import("std").testing.expectEqualStrings("ActualAttentionBodyCanceled", copy.records[0].last_error.bytes[0..copy.records[0].last_error.len]);
}

pub inline fn nativeTransposedA(body_identity: usize, vendor_api: bool) void {
    if (active_kernels == null) return;
    nativeTransposedAEnabled(body_identity, vendor_api);
}
noinline fn nativeTransposedAEnabled(body_identity: usize, vendor_api: bool) void {
    var cursor = active_kernels;
    while (cursor) |scope| : (cursor = scope.previous) {
        const out = &scope.native_workers;
        if (vendor_api) out.transposed_a_vendor_api_returns +|= 1 else {
            out.transposed_a_body_identity = body_identity;
            out.transposed_a_successful_body_returns +|= 1;
        }
    }
}

pub const NativeActivationProgram = struct {
    family: []const u8 = "",
    /// Actual selected whole-program callable; terminal math is the separate
    /// terminal_body_identity field, never inferred from a generic wrapper.
    program_identity: usize = 0,
    entries: u64 = 0,
    returns: u64 = 0,
    error_returns: u64 = 0,
    not_entered: u64 = 0,
    last_error: FailureText = .{},
};
pub const NativeActivationPrograms = struct {
    scope: enum { joined_or_direct_authored_gelu_program_values } = .joined_or_direct_authored_gelu_program_values,
    /// Program values count callbacks/chunks. Terminal GELU fields measure the
    /// shared actual completed body only, including independent workers.
    records: [4]NativeActivationProgram = @splat(.{}),
    count: u8 = 0,
    unrecorded_events: u64 = 0,
    terminal_body_identity: usize = 0,
    terminal_gelu_calls: u64 = 0,
    vector_width: usize = 0,
    vector_iterations: u64 = 0,
    scalar_elements: u64 = 0,
    unobserved_custom_callbacks: u64 = 0,
};
pub fn nativeActivationPrograms(value: anytype) void {
    var cursor = active_kernels;
    while (cursor) |scope| : (cursor = scope.previous) {
        const out = &scope.activation_programs;
        if (value.terminal_body_identity != 0) out.terminal_body_identity = value.terminal_body_identity;
        out.terminal_gelu_calls +|= value.terminal_calls;
        out.vector_width = value.vector_width;
        out.vector_iterations +|= value.vector_iterations;
        out.scalar_elements +|= value.scalar_elements;
        out.unobserved_custom_callbacks +|= value.unobserved_custom_callbacks;
        out.unrecorded_events +|= value.unrecorded_events;
        for (value.records[0..value.count]) |item| {
            const family = @tagName(item.family);
            var found: ?*NativeActivationProgram = null;
            for (out.records[0..out.count]) |*record| if (@import("std").mem.eql(u8, record.family, family) and record.program_identity == item.program_identity) {
                found = record;
                break;
            };
            if (found == null) {
                if (out.count == out.records.len) {
                    out.unrecorded_events +|= item.entries +| item.not_entered;
                    continue;
                }
                found = &out.records[out.count];
                out.count += 1;
                found.?.* = .{ .family = family, .program_identity = item.program_identity };
            }
            found.?.entries +|= item.entries;
            found.?.returns +|= item.returns;
            found.?.error_returns +|= item.error_returns;
            found.?.not_entered +|= item.not_entered;
            if (item.last_error) |err| found.?.last_error = FailureText.init(@errorName(err));
        }
    }
}

/// Whole authored Call units. Terminal math/worker units stay in their own
/// existing records; static selected steps/pairs never claim actual launches.
pub const NativeProgramRecord = struct {
    identity: usize = 0,
    selected_steps: usize = 0,
    selected_pairs: usize = 0,
    entries: u64 = 0,
    returns: u64 = 0,
    error_returns: u64 = 0,
    not_entered: u64 = 0,
    assigned_dispatches: u64 = 0,
    last_error: FailureText = .{},
};
pub const NativePrograms = struct {
    scope: enum { actual_authored_call_and_legacy_selection_values_in_invocation } = .actual_authored_call_and_legacy_selection_values_in_invocation,
    grouping: enum { observed_numerical_address_and_selection_not_distinct_program_lifetime } = .observed_numerical_address_and_selection_not_distinct_program_lifetime,
    coverage: enum { selected_program_and_observed_legacy_fallthrough_only_other_routes_unavailable } = .selected_program_and_observed_legacy_fallthrough_only_other_routes_unavailable,
    records: [8]NativeProgramRecord = @splat(.{}),
    count: u8 = 0,
    unrecorded_calls: u64 = 0,
    fallback_no_prepared_native_program: u64 = 0,
    fallback_unsupported_call: u64 = 0,
    fallback_begin_call_declined: u64 = 0,
    last_begin_call_error: FailureText = .{},
};
/// One copied sample from the latest actually entered authored Call. Frame
/// counters include its acquisition/cold preparation and run, before close.
/// The optional ledger covers shared prepared-owner physical Leases; its
/// peaks and birth/death counters are owner-lifetime values, never request
/// peaks. Model weights, KV, Call host output, metadata and vendor heaps are
/// outside this ledger. Unknown backing bytes stay null.
pub const NativeProgramBank = struct {
    /// Copied only from the actual entered Call, sampled before close.
    /// It describes prepared admission policy, not numerical success.
    prepared_payload_coverage: ?@import("owned_coverage.zig").Coverage = null,
    scope: enum { latest_entered_call_acquire_prepare_run_before_close } = .latest_entered_call_acquire_prepare_run_before_close,
    ledger_scope: enum { prepared_owner_lease_lifetime_sampled_before_call_close } = .prepared_owner_lease_lifetime_sampled_before_call_close,
    coverage: enum { program_owned_leases_excluding_model_kv_host_output_metadata_vendor } = .program_owned_leases_excluding_model_kv_host_output_metadata_vendor,
    frame_allocations: u64,
    frame_requested_allocation_bytes: u64,
    frame_reuse_accesses: u64,
    owned_ledger: ?@import("storage_accounting.zig").Snapshot,
};
pub inline fn nativeProgramBank(value: NativeProgramBank) void {
    if (active_kernels == null) return;
    nativeProgramBankEnabled(value);
}
noinline fn nativeProgramBankEnabled(value: NativeProgramBank) void {
    var cursor = active_kernels;
    while (cursor) |scope| : (cursor = scope.previous) scope.native_program_bank = value;
}

pub const NativeProgramEvent = struct {
    identity: usize,
    entered: bool,
    returned: bool = false,
    failure: ?anyerror = null,
    selected_steps: usize,
    selected_pairs: usize,
    assigned_dispatches: u64,
};
pub inline fn nativeProgram(value: NativeProgramEvent) void {
    if (active_kernels == null) return;
    nativeProgramEnabled(value);
}
noinline fn nativeProgramEnabled(value: NativeProgramEvent) void {
    var cursor = active_kernels;
    while (cursor) |scope| : (cursor = scope.previous) {
        const out = &scope.native_programs;
        var found: ?*NativeProgramRecord = null;
        for (out.records[0..out.count]) |*record| if (record.identity == value.identity and record.selected_steps == value.selected_steps and record.selected_pairs == value.selected_pairs) {
            found = record;
            break;
        };
        if (found == null) {
            if (out.count == out.records.len) {
                out.unrecorded_calls +|= 1;
                continue;
            }
            found = &out.records[out.count];
            out.count += 1;
            found.?.* = .{ .identity = value.identity, .selected_steps = value.selected_steps, .selected_pairs = value.selected_pairs };
        }
        const record = found.?;
        if (value.entered) record.entries +|= 1 else record.not_entered +|= 1;
        if (value.returned) record.returns +|= 1;
        if (value.failure) |err| {
            if (value.entered) record.error_returns +|= 1;
            record.last_error = FailureText.init(@errorName(err));
        }
        record.assigned_dispatches +|= value.assigned_dispatches;
    }
}

test "Native authored Call records are bounded copied invocation values and disabled recording is inert" {
    const a = @import("std").testing;
    const previous = active_kernels;
    try a.expect(previous == null);
    nativeProgram(.{ .identity = 91, .entered = true, .returned = true, .selected_steps = 4, .selected_pairs = 1, .assigned_dispatches = 4 });
    try a.expect(active_kernels == null);
    var outer = KernelScope{};
    outer.begin();
    defer _ = outer.finish();
    var inner = KernelScope{};
    inner.begin();
    nativeProgram(.{ .identity = 91, .entered = false, .failure = error.AuthoredResidentWeightChanged, .selected_steps = 4, .selected_pairs = 1, .assigned_dispatches = 0 });
    nativeProgram(.{ .identity = 91, .entered = true, .failure = error.Cancelled, .selected_steps = 4, .selected_pairs = 1, .assigned_dispatches = 2 });
    nativeProgram(.{ .identity = 91, .entered = true, .returned = true, .selected_steps = 4, .selected_pairs = 1, .assigned_dispatches = 4 });
    const copy = inner.native_programs;
    _ = inner.finish();
    inner.native_programs = .{};
    try a.expectEqual(@as(u8, 1), copy.count);
    try a.expectEqual(@as(u64, 2), copy.records[0].entries);
    try a.expectEqual(@as(u64, 1), copy.records[0].not_entered);
    try a.expectEqual(@as(u64, 1), copy.records[0].error_returns);
    try a.expectEqual(@as(u64, 1), copy.records[0].returns);
    try a.expectEqual(@as(u64, 6), copy.records[0].assigned_dispatches);
    try a.expectEqualDeep(copy, outer.native_programs);
    for (0..12) |i| nativeProgram(.{ .identity = i + 100, .entered = true, .returned = true, .selected_steps = 1, .selected_pairs = 0, .assigned_dispatches = 1 });
    try a.expectEqual(@as(u8, 8), outer.native_programs.count);
    try a.expect(outer.native_programs.unrecorded_calls > 0);
}

pub const NativeProgramFallback = enum { no_prepared_native_program, unsupported_call, begin_call_declined };
pub inline fn nativeProgramFallback(reason: NativeProgramFallback, failure: ?anyerror) void {
    if (active_kernels == null) return;
    nativeProgramFallbackEnabled(reason, failure);
}
noinline fn nativeProgramFallbackEnabled(reason: NativeProgramFallback, failure: ?anyerror) void {
    var cursor = active_kernels;
    while (cursor) |scope| : (cursor = scope.previous) {
        const out = &scope.native_programs;
        switch (reason) {
            .no_prepared_native_program => out.fallback_no_prepared_native_program +|= 1,
            .unsupported_call => out.fallback_unsupported_call +|= 1,
            .begin_call_declined => {
                out.fallback_begin_call_declined +|= 1;
                if (failure) |err| out.last_begin_call_error = FailureText.init(@errorName(err));
            },
        }
    }
}

test "Native legacy selection reasons stay factual copied and disabled without constructor attribution" {
    const t = @import("std").testing;
    try t.expect(!kernelEnabled());
    nativeProgramFallback(.no_prepared_native_program, null);
    var scope = KernelScope{};
    scope.begin();
    defer _ = scope.finish();
    nativeProgramFallback(.no_prepared_native_program, null);
    nativeProgramFallback(.unsupported_call, null);
    nativeProgramFallback(.begin_call_declined, error.AuthoredPagedWriteRequiresExclusiveBlock);
    const copied = scope.native_programs;
    scope.native_programs = .{};
    try t.expectEqual(@as(u64, 1), copied.fallback_no_prepared_native_program);
    try t.expectEqual(@as(u64, 1), copied.fallback_unsupported_call);
    try t.expectEqual(@as(u64, 1), copied.fallback_begin_call_declined);
    try t.expectEqual(@as(u8, 0), copied.count);
}

test "Native Call address reuse never combines different selected static program metadata" {
    const t = @import("std").testing;
    var scope = KernelScope{};
    scope.begin();
    defer _ = scope.finish();
    nativeProgram(.{ .identity = 91, .entered = true, .returned = true, .selected_steps = 4, .selected_pairs = 1, .assigned_dispatches = 4 });
    nativeProgram(.{ .identity = 91, .entered = true, .returned = true, .selected_steps = 7, .selected_pairs = 2, .assigned_dispatches = 7 });
    try t.expectEqual(@as(u8, 2), scope.native_programs.count);
    try t.expectEqual(@as(usize, 4), scope.native_programs.records[0].selected_steps);
    try t.expectEqual(@as(usize, 7), scope.native_programs.records[1].selected_steps);
    for (scope.native_programs.records[0..2]) |record| try t.expectEqual(@as(u64, 1), record.returns);
}

test "Native Call bank observations copy optional lifetime gauges through nested scopes" {
    const a = @import("std").testing;
    const ledger = try @import("storage_accounting.zig").Ledger.create(a.allocator);
    defer ledger.release();
    ledger.allocated(.workspace, 32, null);
    var live = true;
    defer if (live) ledger.freed(.workspace, 32, null);
    const sample = NativeProgramBank{ .frame_allocations = 1, .frame_requested_allocation_bytes = 32, .frame_reuse_accesses = 2, .owned_ledger = ledger.snapshot() };
    nativeProgramBank(sample); // Disabled: no collector and no producer access.
    var outer = KernelScope{};
    outer.begin();
    defer {
        if (outer.attached) _ = outer.finish();
    }
    try a.expect(outer.native_program_bank == null);
    var inner = KernelScope{};
    inner.begin();
    defer {
        if (inner.attached) _ = inner.finish();
    }
    nativeProgramBank(sample);
    _ = inner.finish();
    ledger.freed(.workspace, 32, null);
    live = false;
    try a.expectEqualDeep(sample, inner.native_program_bank.?);
    try a.expectEqualDeep(sample, outer.native_program_bank.?);
    try a.expectEqual(@as(?u64, null), inner.native_program_bank.?.owned_ledger.?.workspace.backing_live_bytes);
    try a.expectEqual(@as(?u64, null), inner.native_program_bank.?.owned_ledger.?.workspace.backing_peak_bytes);
    try a.expectEqual(@as(u64, 32), inner.native_program_bank.?.owned_ledger.?.workspace.requested_live_bytes);
    try a.expectEqual(@as(u64, 0), ledger.snapshot().workspace.requested_live_bytes);
    _ = outer.finish();
    var fresh = KernelScope{};
    fresh.begin();
    defer _ = fresh.finish();
    try a.expect(fresh.native_program_bank == null);
}
