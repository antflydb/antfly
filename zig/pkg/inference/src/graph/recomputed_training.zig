// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit, bounded activation recomputation over immutable graph regions.
//! The encoder caller supplies semantic boundaries AFTER its single PEFT
//! rewrite. Initial execution retains only those boundaries. Backward replays
//! one region, consumes its local seeded tape, and releases it before moving
//! to the preceding region. Detached head decisions are never recomputed.
//!
//! This module owns no optimizer, checkpoint file, model loader or RNG. The
//! enclosing parameter binding lease must outlive Tape. A Plan cannot execute
//! until sealAdmission includes the separately compiled head and complete
//! optimizer transaction. Default seeded/multi-stage execution is unchanged.
const std = @import("std");
const ml = @import("ml").graph;
const seeded = @import("seeded_training.zig");
const interpreter = @import("interpreter.zig");
const resident = @import("resident_training_execution.zig");
const program = @import("resident_training_program.zig");
const ops = @import("../ops/ops.zig");
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const Id = ml.NodeId;
const nil = ml.null_node;

pub const profile = "encoder_layer_recompute_v1";
pub const RegionSpec = struct {
    /// Semantic ordinal, never derived from graph allocation or lowering IDs.
    key: u64,
    /// Earlier region outputs which cut this region's incoming dependencies.
    inputs: []const Id,
    /// Independently retained snapshots, each declared exactly once.
    outputs: []const Id,
};
pub const ReplayInput = struct {
    node: Id,
    /// Caller binds the exact site, occurrence, shape, probability and RNG ABI.
    key: [32]u8,
};
pub const BindingPolicy = struct {
    /// Named model tensors already included in the enclosing managed owner.
    /// All other fixed leaves are charged once as immutable runtime bindings.
    managed_parameters: []const Id = &.{},
    replay_inputs: []const ReplayInput = &.{},
};
pub const ReplaySource = struct {
    /// Includes the step's counter tuple and any immutable supplied-mask pins.
    fingerprint: [32]u8 = @splat(0),
    context: ?*const anyopaque = null,
    /// Must reject changes to the borrowed recipe owner during a live tape.
    validate: ?*const fn (?*const anyopaque, [32]u8) anyerror!void = null,
    /// Returns an owned, exactly shaped tensor for ONE logical input. The
    /// caller sees the same key/identity on initial and replay execution.
    materialize: ?*const fn (?*const anyopaque, Allocator, *const ops.ComputeBackend, ReplayInput, ml.Shape, seeded.StepIdentity, ?Control) anyerror!ops.CT = null,
};
pub const Limits = struct {
    max_regions: usize = 64,
    max_source_nodes: usize = 1_000_000,
    /// One reclaiming owner for compiled graphs and runtime host metadata.
    max_plan_host_bytes: usize = 256 * 1024 * 1024,
    max_compile_bytes: usize = 1024 * 1024 * 1024,
    max_checkpoint_bytes: usize = 256 * 1024 * 1024,
    max_gradient_bytes: usize = 1024 * 1024 * 1024,
    max_backend_bytes: usize = 8 * 1024 * 1024 * 1024,
    max_host_bytes: usize = 1024 * 1024 * 1024,
    max_total_work: u64 = 1 << 42,
};
pub const Options = struct {
    session: seeded.Options = .{},
    limits: Limits = .{},
};
pub const EnclosingAdmission = struct {
    /// Existing model weights and optimizer state, each physical owner once.
    fixed_backend_bytes: usize = 0,
    fixed_host_bytes: usize = 0,
    head_tape_bytes: usize = 0,
    head_local_bytes: usize = 0,
    head_gradient_bytes: usize = 0,
    head_compile_bytes: usize = 0,
    head_host_metadata_bytes: usize = 0,
    head_work: u64 = 0,
    optimizer_transaction_backend_bytes: usize = 0,
    optimizer_transaction_host_bytes: usize = 0,
    optimizer_transaction_work: u64 = 0,
};
pub const Admission = struct {
    regions: usize = 0,
    compile_upper_bound_bytes: usize = 0,
    managed_parameter_bytes: usize = 0,
    immutable_binding_bytes: usize = 0,
    checkpoint_bytes: usize = 0,
    gradient_accumulator_bytes: usize = 0,
    largest_region_tape_bytes: usize = 0,
    largest_local_bytes: usize = 0,
    largest_replay_input_bytes: usize = 0,
    local_host_metadata_bytes: usize = 0,
    instruction_control_readback_upper_bound_bytes: usize = 0,
    control_readback_upper_bound_bytes: usize = 0,
    work: u64 = 0,
    backend_upper_bound_bytes: usize = 0,
    host_upper_bound_bytes: usize = 0,
};
pub const ReplaySummary = struct {
    initial_regions: usize = 0,
    replay_regions: usize = 0,
    initial_calls: usize = 0,
    replay_calls: usize = 0,
    initial_upload_bytes: usize = 0,
    replay_upload_bytes: usize = 0,
    largest_input_bytes: usize = 0,
    largest_live_region_bytes: usize = 0,
    /// Calls to ReplaySource.validate only. A materializer may perform its
    /// own validations and must add those costs separately. Teardown never
    /// calls user validation, allocates or replays an input.
    source_validation_calls_upper_bound: usize = 0,
};

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.RecomputeLimitExceeded;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.RecomputeLimitExceeded;
}
fn addWork(a: u64, b: u64) !u64 {
    return std.math.add(u64, a, b) catch error.RecomputeLimitExceeded;
}
fn bytes(shape: ml.Shape) !usize {
    if (shape.rank_ > 8 or (shape.dtype != .f32 and shape.dtype != .i32)) return error.InvalidRecomputeShape;
    for (shape.dims[0..shape.rank_]) |dim| if (dim <= 0 or dim > std.math.maxInt(i32)) return error.InvalidRecomputeShape;
    return seeded.shapeBytes(shape);
}
fn shape32(shape: ml.Shape, storage: *[8]i32) []const i32 {
    for (shape.dims[0..shape.rank_], 0..) |dim, index| storage[index] = @intCast(dim);
    return storage[0..shape.rank_];
}
fn contains(ids: []const Id, id: Id) bool {
    return std.mem.indexOfScalar(Id, ids, id) != null;
}
fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}
const CombinedControl = struct {
    original: ?Control,
    request: ?Control,
    fn apply(raw: ?*anyopaque) !void {
        const self: *const CombinedControl = @ptrCast(@alignCast(raw.?));
        try check(self.original);
        try check(self.request);
    }
    fn control(self: *@This()) Control {
        // Preserve the process supervisor/IO carrier while composing both
        // cancellation sources. A callback-only replacement would lose the
        // caller's hard interruption boundary for driver operations.
        var result = self.request orelse self.original orelse Control{};
        if (self.original) |original| {
            if (result.io == null) result.io = original.io;
            if (result.hard_cancellation == null) result.hard_cancellation = original.hard_cancellation;
            if (original.deadline_ns) |deadline| result.deadline_ns = if (result.deadline_ns) |other| @min(deadline, other) else deadline;
        }
        result.ptr = self;
        result.check_fn = apply;
        return result;
    }
};
fn localProgramBytes(value: program.Admission) !usize {
    return add(try add(value.constant_bytes, value.working_upper_bound_bytes), value.upload_staging_bytes);
}

const Checkpoint = struct { node: Id, shape: ml.Shape, producer: usize, needs_gradient: bool = false };
pub const InputDescriptor = struct { node: Id, shape: ml.Shape, managed: bool, replay: ?ReplayInput };
const Input = InputDescriptor;
const HostOwner = struct {
    budget: Budget,
    failure: ?Budget.AllocationFailure = null,
    fn failed(raw: ?*anyopaque, value: Budget.AllocationFailure) void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.failure = value;
    }
    fn translate(self: *const @This(), err: anyerror) anyerror {
        if (err == error.OutOfMemory) if (self.failure) |failure| {
            if (failure.kind == .declared_limit) return error.RecomputeLimitExceeded;
        };
        return err;
    }
};
const Destination = union(enum) { checkpoint: usize, parameter: usize };
const GradientRoute = struct { local: Id, destination: Destination };
const InputRoute = struct { source: usize, local: Id };
const Entry = struct { checkpoint: usize, local: Id };
const Region = struct {
    key: u64,
    cut: ml.lower.LowerResult,
    initial: program.Program,
    initial_analysis: interpreter.CachedAnalysis,
    session: ?seeded.Session,
    outputs: []usize,
    entries: []Entry,
    inputs: []InputRoute,
    gradients: []GradientRoute,
    duplicate_gradients: []?usize,

    fn deinit(self: *Region, a: Allocator) void {
        if (self.session) |*session| session.deinit();
        self.initial_analysis.deinit(a);
        self.initial.deinit();
        self.cut.deinit();
        a.free(self.outputs);
        a.free(self.entries);
        a.free(self.inputs);
        a.free(self.gradients);
        a.free(self.duplicate_gradients);
    }
};

fn compileRegion(a: Allocator, source: *const ml.Graph, spec: RegionSpec, checkpoints: []Checkpoint, checkpoint_map: *const std.AutoHashMapUnmanaged(Id, usize), inputs: []const Input, parameters: []const Id, options: Options, admission: *Admission) !Region {
    var cut = try seeded.cutProgram(a, source, spec.inputs, spec.outputs);
    errdefer cut.deinit();
    var program_limits = options.session.resident.program;
    program_limits.max_compile_bytes = @min(program_limits.max_compile_bytes, options.limits.max_compile_bytes -| admission.compile_upper_bound_bytes);
    var initial = try program.Program.init(a, &cut.graph, cut.graph.outputs.items, program_limits);
    errdefer initial.deinit();
    var analysis = try interpreter.CachedAnalysis.compute(a, &initial.lowered.graph);
    errdefer analysis.deinit(a);
    const output_slots = try a.alloc(usize, spec.outputs.len);
    errdefer a.free(output_slots);
    for (spec.outputs, output_slots) |id, *slot| slot.* = checkpoint_map.get(id) orelse return error.InvalidRecomputeBoundary;
    const entries = try a.alloc(Entry, spec.inputs.len);
    errdefer a.free(entries);
    for (spec.inputs, entries) |id, *entry| entry.* = .{ .checkpoint = checkpoint_map.get(id) orelse return error.InvalidRecomputeBoundary, .local = cut.id_map[id] };
    var input_routes = std.ArrayListUnmanaged(InputRoute).empty;
    defer input_routes.deinit(a);
    var replay_bytes: usize = 0;
    var output_seed_bytes: usize = 0;
    for (output_slots) |slot| output_seed_bytes = try add(output_seed_bytes, try bytes(checkpoints[slot].shape));
    for (inputs, 0..) |input, index| {
        const id = cut.id_map[input.node];
        if (id == nil or !initial.usesParameter(id)) continue;
        try input_routes.append(a, .{ .source = index, .local = id });
        if (input.replay != null) replay_bytes = try add(replay_bytes, try bytes(input.shape));
    }
    const owned_inputs = try input_routes.toOwnedSlice(a);
    errdefer a.free(owned_inputs);
    const forward_count = cut.graph.nodeCount();
    const needed = try a.alloc(bool, forward_count);
    defer a.free(needed);
    @memset(needed, false);
    var wrt = std.ArrayListUnmanaged(Id).empty;
    defer wrt.deinit(a);
    var destinations = std.AutoHashMapUnmanaged(Id, Destination).empty;
    defer destinations.deinit(a);
    for (parameters, 0..) |id, index| {
        const local = cut.id_map[id];
        if (local == nil) continue;
        try wrt.append(a, local);
        try destinations.put(a, local, .{ .parameter = index });
        needed[local] = true;
    }
    for (entries) |entry| {
        if (entry.local == nil or !checkpoints[entry.checkpoint].needs_gradient) continue;
        try wrt.append(a, entry.local);
        try destinations.put(a, entry.local, .{ .checkpoint = entry.checkpoint });
        needed[entry.local] = true;
    }
    for (cut.graph.nodes.items, 0..) |node, index| {
        if (node.op == .stop_gradient) continue;
        for (node.getInputs()) |input| needed[index] = needed[index] or needed[input];
    }
    for (output_slots, cut.graph.outputs.items) |slot, id| checkpoints[slot].needs_gradient = needed[id];
    var session: ?seeded.Session = null;
    errdefer if (session) |*value| value.deinit();
    var gradient_routes = std.ArrayListUnmanaged(GradientRoute).empty;
    defer gradient_routes.deinit(a);
    var duplicates = std.ArrayListUnmanaged(?usize).empty;
    defer duplicates.deinit(a);
    var local_peak = try add(try localProgramBytes(initial.admission), initial.admission.capture_bytes);
    var local_metadata = initial.admission.host_metadata_upper_bound_bytes;
    var compilation = initial.admission.compile_upper_bound_bytes;
    var work = initial.admission.total_work;
    var instruction_control_bytes = if (options.session.execution == .resident_metal) initial.admission.instruction_control_readback_upper_bound_bytes else 0;
    var control_bytes = instruction_control_bytes;
    if (wrt.items.len != 0) {
        const seeds = try a.alloc(ml.autodiff.Seed, output_slots.len);
        defer a.free(seeds);
        var builder = ml.Builder.init(&cut.graph);
        // The output list is stable while appending detached seed leaves.
        for (cut.graph.outputs.items, seeds, 0..) |id, *seed, index| {
            var buffer: [96]u8 = undefined;
            const name = try std.fmt.bufPrint(&buffer, "__antfly_recompute_seed.{d}.{d}", .{ spec.key, index });
            seed.* = .{ .output = id, .cotangent = try builder.parameter(name, cut.graph.node(id).output_shape) };
        }
        var local_options = options.session;
        local_options.allow_no_gradients = true;
        local_options.gradient.require_all_gradients = false;
        local_options.resident.max_compile_bytes = @min(local_options.resident.max_compile_bytes, options.limits.max_compile_bytes -| try add(admission.compile_upper_bound_bytes, compilation));
        session = try seeded.Session.init(a, &cut.graph, seeds, wrt.items, local_options);
        const value = &session.?;
        for (value.gradient_parameters) |id| try gradient_routes.append(a, .{ .local = id, .destination = destinations.get(id) orelse return error.InvalidRecomputeGradient });
        var duplicate_copy_bytes: usize = 0;
        for (value.backward.graph.outputs.items, 0..) |id, index| {
            const prior = std.mem.indexOfScalar(Id, value.backward.graph.outputs.items[0..index], id);
            try duplicates.append(a, prior);
            if (prior != null) duplicate_copy_bytes = try add(duplicate_copy_bytes, try bytes(value.backward.graph.node(id).output_shape));
        }
        admission.largest_region_tape_bytes = @max(admission.largest_region_tape_bytes, value.tape_bytes);
        if (value.resident) |*compiled| {
            compilation = try add(compilation, compiled.admission.compile_upper_bound_bytes);
            local_metadata = try add(local_metadata, compiled.admission.host_metadata_upper_bound_bytes);
            // Binding bytes are charged ONCE by the coordinator. A local
            // session's retained captures, scratch and outputs are additional.
            local_peak = @max(local_peak, try add(compiled.admission.device_upper_bound_bytes - compiled.admission.persistent_binding_bytes, duplicate_copy_bytes));
            work = try addWork(work, compiled.admission.total_work);
            instruction_control_bytes = try add(instruction_control_bytes, compiled.admission.instruction_control_readback_upper_bound_bytes);
            control_bytes = try add(control_bytes, try add(compiled.admission.finite_check_readback_bytes, compiled.admission.instruction_control_readback_upper_bound_bytes));
        } else {
            // The strict descriptor compiler also validates the native region
            // geometry. Native execution still uses its checked CPU backend;
            // no Metal callback or resident fallback executes here.
            program_limits.max_compile_bytes = @min(program_limits.max_compile_bytes, options.limits.max_compile_bytes -| try add(admission.compile_upper_bound_bytes, compilation));
            var replay = try program.Program.init(a, &value.differentiated.graph, value.captures, program_limits);
            defer replay.deinit();
            compilation = try add(compilation, replay.admission.compile_upper_bound_bytes);
            work = try addWork(work, replay.admission.total_work);
            local_metadata = try add(local_metadata, replay.admission.host_metadata_upper_bound_bytes);
            local_peak = @max(local_peak, try add(value.tape_bytes, try nativeLocalBytes(&replay)));
            if (value.backward.graph.outputs.items.len != 0) {
                program_limits.max_compile_bytes = @min(program_limits.max_compile_bytes, options.limits.max_compile_bytes -| try add(admission.compile_upper_bound_bytes, compilation));
                var reverse = try program.Program.init(a, &value.backward.graph, value.backward.graph.outputs.items, program_limits);
                defer reverse.deinit();
                compilation = try add(compilation, reverse.admission.compile_upper_bound_bytes);
                work = try addWork(work, reverse.admission.total_work);
                local_metadata = try add(local_metadata, reverse.admission.host_metadata_upper_bound_bytes);
                local_peak = @max(local_peak, try add(value.tape_bytes, try add(try nativeLocalBytes(&reverse), try add(reverse.admission.capture_bytes, duplicate_copy_bytes))));
            }
        }
    }
    if (options.session.execution == .native) local_peak = @max(local_peak, try add(try nativeLocalBytes(&initial), initial.admission.capture_bytes));
    // Region-generated masks, their upload staging and independent initial
    // snapshot overlap must coexist with the selected local program.
    // Missing output cotangents require explicit zero buffers. During a
    // reduction the old accumulator, incoming gradient and new sum coexist;
    // incoming gradients are already included in program capture bytes.
    var largest_sum: usize = 0;
    for (gradient_routes.items) |route| {
        const n = try bytes(cut.graph.node(route.local).output_shape);
        largest_sum = @max(largest_sum, n);
        work = try addWork(work, n / 4); // deterministic accumulator addition
    }
    work = try addWork(work, try add(try mul(replay_bytes / 4, 2), output_seed_bytes / 4));
    local_peak = try add(local_peak, try add(try mul(replay_bytes, 2), try add(try mul(output_seed_bytes, 2), largest_sum)));
    const routes = try gradient_routes.toOwnedSlice(a);
    errdefer a.free(routes);
    const duplicate_routes = try duplicates.toOwnedSlice(a);
    errdefer a.free(duplicate_routes);
    admission.compile_upper_bound_bytes = try add(admission.compile_upper_bound_bytes, compilation);
    admission.work = try addWork(admission.work, work);
    admission.control_readback_upper_bound_bytes = try add(admission.control_readback_upper_bound_bytes, control_bytes);
    admission.instruction_control_readback_upper_bound_bytes = try add(admission.instruction_control_readback_upper_bound_bytes, instruction_control_bytes);
    admission.largest_local_bytes = @max(admission.largest_local_bytes, local_peak);
    admission.largest_replay_input_bytes = @max(admission.largest_replay_input_bytes, replay_bytes);
    admission.local_host_metadata_bytes = try add(admission.local_host_metadata_bytes, local_metadata);
    if (admission.compile_upper_bound_bytes > options.limits.max_compile_bytes or admission.work > options.limits.max_total_work) return error.RecomputeLimitExceeded;
    return .{ .key = spec.key, .cut = cut, .initial = initial, .initial_analysis = analysis, .session = session, .outputs = output_slots, .entries = entries, .inputs = owned_inputs, .gradients = routes, .duplicate_gradients = duplicate_routes };
}

/// Conservative native logical-data envelope: all program values can coexist
/// with two operand materializations. Include the CPU attention tile workspace
/// independently of the resident kernel's different scratch geometry. Opaque
/// BLAS/driver workspace remains subject to the enclosing process reservation.
pub fn nativeLocalBytes(compiled: *const program.Program) !usize {
    var values: usize = 0;
    var largest_operand: usize = 0;
    var scratch: usize = 0;
    for (compiled.lowered.graph.nodes.items) |node| {
        const n = try bytes(node.output_shape);
        largest_operand = @max(largest_operand, n);
        if (node.op != .parameter) values = try add(values, n);
        switch (node.op) {
            .fused_deberta_training_attention_v1, .fused_deberta_training_attention_backward_v1 => |attrs| {
                const plan = try @import("../ops/deberta_training_attention.zig").plan(attrs, .{});
                scratch = @max(scratch, plan.scratch_bytes);
            },
            else => {},
        }
    }
    return add(try add(values, try mul(largest_operand, 2)), scratch);
}

pub const Plan = struct {
    backing: Allocator,
    host_owner: *HostOwner,
    budget: *Budget,
    allocator: Allocator,
    /// Borrowed immutable source; the graph owner outlives Plan and every tape.
    source: *const ml.Graph,
    options: Options,
    regions: []Region,
    checkpoints: []Checkpoint,
    inputs: []Input,
    parameters: []Id,
    parameter_shapes: []ml.Shape,
    regional_nodes: []bool,
    final_slot: usize,
    regional_admission: Admission,
    admission: ?Admission = null,
    parameter_epoch: u64 = 0,
    active_tape: bool = false,

    pub fn init(backing: Allocator, source: *const ml.Graph, specs: []const RegionSpec, final_output: Id, selected: []const Id, policy: BindingPolicy, options: Options) !Plan {
        try validateRequest(source, specs, selected, policy, options);
        const owner = try backing.create(HostOwner);
        errdefer backing.destroy(owner);
        owner.* = .{ .budget = .{ .backing = backing, .limit = options.limits.max_plan_host_bytes, .failure_context = owner, .allocation_failed = HostOwner.failed } };
        const budget = &owner.budget;
        const a = budget.allocator();
        return initialize(backing, owner, a, source, specs, final_output, selected, policy, options) catch |err| {
            std.debug.assert(budget.live == 0);
            return owner.translate(err);
        };
    }

    fn initialize(backing: Allocator, host_owner: *HostOwner, a: Allocator, source: *const ml.Graph, specs: []const RegionSpec, final_output: Id, selected: []const Id, policy: BindingPolicy, options: Options) !Plan {
        var scratch_arena = std.heap.ArenaAllocator.init(a);
        defer scratch_arena.deinit();
        const scratch = scratch_arena.allocator();
        const owner = try scratch.alloc(?usize, source.nodeCount());
        @memset(owner, null);
        const marks = try scratch.alloc(usize, source.nodeCount());
        @memset(marks, 0);
        var checkpoint_list = std.ArrayListUnmanaged(Checkpoint).empty;
        var checkpoint_map = std.AutoHashMapUnmanaged(Id, usize).empty;
        var input_list = std.ArrayListUnmanaged(Input).empty;
        var input_map = std.AutoHashMapUnmanaged(Id, usize).empty;
        var stack = std.ArrayListUnmanaged(Id).empty;
        for (specs, 0..) |spec, index| {
            for (spec.outputs) |id| {
                if (checkpoint_map.contains(id)) return error.DuplicateRecomputeBoundary;
                if (source.node(id).op == .parameter or source.node(id).op == .constant or source.node(id).output_shape.dtype != .f32) return error.InvalidRecomputeBoundary;
                try checkpoint_map.put(scratch, id, checkpoint_list.items.len);
                try checkpoint_list.append(scratch, .{ .node = id, .shape = source.node(id).output_shape, .producer = index });
            }
        }
        const final_slot = checkpoint_map.get(final_output) orelse return error.InvalidRecomputeBoundary;
        if (checkpoint_list.items[final_slot].producer != specs.len - 1) return error.InvalidRecomputeBoundary;
        // Explicit graph closure, including alternates, catches hidden edges
        // across a checkpoint. Node allocation order is deliberately irrelevant.
        for (specs, 0..) |spec, index| {
            for (spec.inputs, 0..) |id, ordinal| {
                const slot = checkpoint_map.get(id) orelse return error.InvalidRecomputeBoundary;
                if (checkpoint_list.items[slot].producer >= index or contains(spec.inputs[0..ordinal], id)) return error.InvalidRecomputeBoundary;
            }
            stack.clearRetainingCapacity();
            try stack.appendSlice(scratch, spec.outputs);
            while (stack.pop()) |id| {
                if (marks[id] == index + 1) continue;
                marks[id] = index + 1;
                if (contains(spec.inputs, id)) continue;
                if (checkpoint_map.get(id)) |slot| if (checkpoint_list.items[slot].producer != index) return error.UndeclaredRecomputeDependency;
                const node = source.node(id);
                if (node.op == .parameter) {
                    if (!input_map.contains(id)) {
                        var replay: ?ReplayInput = null;
                        for (policy.replay_inputs) |candidate| if (candidate.node == id) {
                            replay = candidate;
                            break;
                        };
                        try input_map.put(scratch, id, input_list.items.len);
                        try input_list.append(scratch, .{ .node = id, .shape = node.output_shape, .managed = contains(policy.managed_parameters, id), .replay = replay });
                    }
                    continue;
                }
                if (node.op == .constant) continue;
                if (owner[id]) |prior| if (prior != index) return error.UndeclaredRecomputeDependency;
                owner[id] = index;
                try stack.appendSlice(scratch, node.getInputs());
                if (node.vjp_alternate != nil) try stack.append(scratch, node.vjp_alternate);
            }
        }
        for (policy.replay_inputs) |value| if (!input_map.contains(value.node)) return error.UnusedRecomputeReplayInput;
        const active_parameters = try differentiableReachability(a, source, final_output);
        defer a.free(active_parameters);
        var parameters = std.ArrayListUnmanaged(Id).empty;
        var parameter_shapes = std.ArrayListUnmanaged(ml.Shape).empty;
        for (selected) |id| if (input_map.contains(id) and active_parameters[id]) {
            try parameters.append(scratch, id);
            try parameter_shapes.append(scratch, source.node(id).output_shape);
        };
        const regions = try a.alloc(Region, specs.len);
        errdefer a.free(regions);
        var initialized: usize = 0;
        errdefer for (regions[0..initialized]) |*region| region.deinit(a);
        var admission = Admission{ .regions = specs.len };
        for (checkpoint_list.items) |checkpoint| admission.checkpoint_bytes = try add(admission.checkpoint_bytes, try bytes(checkpoint.shape));
        if (admission.checkpoint_bytes > options.limits.max_checkpoint_bytes) return error.RecomputeLimitExceeded;
        for (input_list.items) |input| {
            const n = try bytes(input.shape);
            if (input.managed) admission.managed_parameter_bytes = try add(admission.managed_parameter_bytes, n) else if (input.replay == null) admission.immutable_binding_bytes = try add(admission.immutable_binding_bytes, n);
        }
        for (parameter_shapes.items) |shape| admission.gradient_accumulator_bytes = try add(admission.gradient_accumulator_bytes, try bytes(shape));
        for (specs, regions, 0..) |spec, *region, index| {
            region.* = try compileRegion(a, source, spec, checkpoint_list.items, &checkpoint_map, input_list.items, parameters.items, options, &admission);
            initialized += 1;
            _ = index;
        }
        // Cotangents of shared checkpoints (Rnorm) accumulate in reverse region
        // order. Count every possible boundary accumulator, even when pruned.
        for (checkpoint_list.items) |checkpoint| if (checkpoint.needs_gradient) {
            admission.gradient_accumulator_bytes = try add(admission.gradient_accumulator_bytes, try bytes(checkpoint.shape));
        };
        if (admission.gradient_accumulator_bytes > options.limits.max_gradient_bytes) return error.RecomputeLimitExceeded;
        const owned_checkpoints = try a.dupe(Checkpoint, checkpoint_list.items);
        errdefer a.free(owned_checkpoints);
        const owned_inputs = try a.dupe(Input, input_list.items);
        errdefer a.free(owned_inputs);
        const owned_parameters = try a.dupe(Id, parameters.items);
        errdefer a.free(owned_parameters);
        const owned_shapes = try a.dupe(ml.Shape, parameter_shapes.items);
        errdefer a.free(owned_shapes);
        const regional_nodes = try a.alloc(bool, owner.len);
        for (owner, regional_nodes) |value, *out| out.* = value != null;
        return .{ .backing = backing, .host_owner = host_owner, .budget = &host_owner.budget, .allocator = a, .source = source, .options = options, .regions = regions, .checkpoints = owned_checkpoints, .inputs = owned_inputs, .parameters = owned_parameters, .parameter_shapes = owned_shapes, .regional_nodes = regional_nodes, .final_slot = final_slot, .regional_admission = admission };
    }

    pub fn deinit(self: *Plan) void {
        std.debug.assert(!self.active_tape);
        for (self.regions) |*region| region.deinit(self.allocator);
        self.allocator.free(self.regions);
        self.allocator.free(self.checkpoints);
        self.allocator.free(self.inputs);
        self.allocator.free(self.parameters);
        self.allocator.free(self.parameter_shapes);
        self.allocator.free(self.regional_nodes);
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self.host_owner);
        self.* = undefined;
    }

    pub fn needsBackward(self: *const Plan) bool {
        return self.checkpoints[self.final_slot].needs_gradient;
    }
    /// Original graph IDs, including frozen named weights. All non-replay
    /// leaves must be supplied explicitly; normal named-store lookup is not
    /// a recomputation binding or an immutable lifetime lease.
    pub fn inputDescriptors(self: *const Plan) []const InputDescriptor {
        return self.inputs;
    }
    pub fn requiresInput(self: *const Plan, original_id: Id) bool {
        for (self.inputs) |input| if (input.node == original_id) return input.replay == null;
        return false;
    }
    /// Allocation-free occurrence accounting. A shared logical recipe used by
    /// two regions is uploaded twice on the initial pass and, when needed,
    /// twice on replay. Unique-recipe bytes are not a transfer/work bound.
    pub fn replaySummary(self: *const Plan) !ReplaySummary {
        var result = ReplaySummary{ .initial_regions = self.regions.len };
        const reverse = self.needsBackward();
        for (self.regions) |region| {
            const replayed = reverse and region.session != null;
            if (replayed) result.replay_regions = try add(result.replay_regions, 1);
            var live: usize = 0;
            for (region.inputs) |route| {
                const input = self.inputs[route.source];
                if (input.replay == null) continue;
                const n = try bytes(input.shape);
                result.initial_calls = try add(result.initial_calls, 1);
                result.initial_upload_bytes = try add(result.initial_upload_bytes, n);
                if (replayed) {
                    result.replay_calls = try add(result.replay_calls, 1);
                    result.replay_upload_bytes = try add(result.replay_upload_bytes, n);
                }
                result.largest_input_bytes = @max(result.largest_input_bytes, n);
                live = try add(live, n);
            }
            result.largest_live_region_bytes = @max(result.largest_live_region_bytes, live);
        }
        // Two validations per initial/replayed bindRegion; one after initial
        // forward, one before backward, and one after a non-empty backward.
        result.source_validation_calls_upper_bound = try add(try mul(try add(result.initial_regions, result.replay_regions), 2), if (reverse) @as(usize, 3) else 2);
        return result;
    }
    pub fn remainingCompileBytes(self: *const Plan) usize {
        return self.options.limits.max_compile_bytes -| self.regional_admission.compile_upper_bound_bytes;
    }

    /// Caller owns the result, including its original-source-to-head ID map.
    /// Add loss seed leaves and build existing head sessions on THIS graph.
    pub fn cutHead(self: *const Plan, a: Allocator, outputs: []const Id) !ml.lower.LowerResult {
        if (outputs.len == 0) return error.InvalidRecomputeBoundary;
        var result = try seeded.cutProgram(a, self.source, &.{self.checkpoints[self.final_slot].node}, outputs);
        errdefer result.deinit();
        const final = self.checkpoints[self.final_slot].node;
        for (self.regional_nodes, 0..) |regional, id| {
            if (regional and id != final and result.id_map[id] != nil) return error.UndeclaredRecomputeDependency;
        }
        if (result.id_map[final] == nil) return error.UnusedRecomputeBoundary;
        return result;
    }

    pub fn sealAdmission(self: *Plan, enclosing: EnclosingAdmission) !Admission {
        if (self.active_tape or self.admission != null) return error.RecomputeAdmissionAlreadySealed;
        var result = self.regional_admission;
        if (try add(enclosing.fixed_backend_bytes, enclosing.fixed_host_bytes) < result.managed_parameter_bytes) return error.InvalidRecomputeAdmission;
        result.compile_upper_bound_bytes = try add(result.compile_upper_bound_bytes, enclosing.head_compile_bytes);
        if (result.compile_upper_bound_bytes > self.options.limits.max_compile_bytes) return error.RecomputeLimitExceeded;
        const retained = try add(try add(result.checkpoint_bytes, enclosing.head_tape_bytes), try add(result.gradient_accumulator_bytes, enclosing.head_gradient_bytes));
        const local = @max(result.largest_local_bytes, enclosing.head_local_bytes);
        result.backend_upper_bound_bytes = try add(enclosing.fixed_backend_bytes, try add(result.immutable_binding_bytes, try add(retained, try add(local, enclosing.optimizer_transaction_backend_bytes))));
        // The Plan's fixed host reservation includes its compiled programs,
        // runtime vectors and CPU-only checked copy staging. Backend-owned
        // grouping metadata remains a separate, explicitly reported bound.
        result.host_upper_bound_bytes = try add(enclosing.fixed_host_bytes, try add(self.options.limits.max_plan_host_bytes, try add(enclosing.head_host_metadata_bytes, try add(result.local_host_metadata_bytes, enclosing.optimizer_transaction_host_bytes))));
        result.work = try addWork(result.work, try addWork(enclosing.head_work, enclosing.optimizer_transaction_work));
        if (result.backend_upper_bound_bytes > self.options.limits.max_backend_bytes or result.host_upper_bound_bytes > self.options.limits.max_host_bytes or result.work > self.options.limits.max_total_work) return error.RecomputeLimitExceeded;
        self.admission = result;
        return result;
    }

    pub fn advanceParameterEpoch(self: *Plan) !void {
        if (self.active_tape) return error.TrainingTapeStillLive;
        const next = std.math.add(u64, self.parameter_epoch, 1) catch return error.TrainingEpochOverflow;
        for (self.regions) |*region| if (region.session) |*session| {
            if (session.active_tape) return error.TrainingTapeStillLive;
            if (session.parameter_epoch != self.parameter_epoch) return error.TrainingTapeIdentityMismatch;
        };
        // All validation precedes this infallible all-region publication.
        for (self.regions) |*region| {
            if (region.session) |*session| session.parameter_epoch = next;
        }
        self.parameter_epoch = next;
    }

    fn validateRegionEpochs(self: *const Plan) !void {
        for (self.regions) |*region| if (region.session) |*session| {
            if (session.active_tape) return error.TrainingTapeStillLive;
            if (session.parameter_epoch != self.parameter_epoch) return error.TrainingTapeIdentityMismatch;
        };
    }

    /// The enclosing owner freezes parameters and supplied fixed inputs until
    /// Tape teardown. Resident inputs acquire independent reference leases;
    /// native payloads remain borrowed from that same enclosing owner.
    pub fn forward(self: *Plan, cb: *const ops.ComputeBackend, fixed: []const interpreter.RuntimeInput, replay: ReplaySource, identity: seeded.StepIdentity, control: ?Control) !Tape {
        if (self.active_tape) return error.TrainingTapeStillLive;
        if (self.admission == null) return error.RecomputeAdmissionNotSealed;
        try self.validateRegionEpochs();
        self.host_owner.failure = null;
        return Tape.initialize(self, cb, fixed, replay, identity, control) catch |err| return self.host_owner.translate(err);
    }
};

/// Follow the actual lowered forward semantics, without constructing a global
/// VJP/tape. A selected leaf used only by an auxiliary retained output must
/// stay absent, even when that output shares a local multi-output session with
/// a live branch. All other supported F32 primitive operands have VJPs; routing
/// integers, comparisons, predicates and stop_gradient never contribute one.
fn differentiableReachability(a: Allocator, source: *const ml.Graph, output: Id) ![]bool {
    var lowered = try seeded.cutProgram(a, source, &.{}, &.{output});
    defer lowered.deinit();
    const graph = &lowered.graph;
    const reachable = try a.alloc(bool, graph.nodeCount());
    defer a.free(reachable);
    @memset(reachable, false);
    var stack = std.ArrayListUnmanaged(Id).empty;
    defer stack.deinit(a);
    try stack.appendSlice(a, graph.outputs.items);
    while (stack.pop()) |id| {
        if (reachable[id]) continue;
        const node = graph.node(id);
        if (node.output_shape.dtype != .f32) continue;
        reachable[id] = true;
        switch (node.op) {
            .parameter, .constant, .stop_gradient, .less_than => {},
            .where_select => try stack.appendSlice(a, node.getInputs()[1..]),
            else => try stack.appendSlice(a, node.getInputs()),
        }
    }
    const original = try a.alloc(bool, source.nodeCount());
    for (lowered.id_map, original) |local, *value| value.* = local != nil and reachable[local];
    return original;
}

const Bindings = struct {
    values: []interpreter.RuntimeInput,
    generated: []ops.CT,
    allocator: Allocator,
    fn deinit(self: *@This(), cb: *const ops.ComputeBackend) void {
        for (self.generated) |value| cb.free(value);
        self.allocator.free(self.generated);
        self.allocator.free(self.values);
    }
};

pub const Tape = struct {
    plan: *Plan,
    /// This is the caller's stable backend address, never a stack-local
    /// controlled copy. All operation-specific controls stay within the call.
    backend: *const ops.ComputeBackend,
    fixed: []?ops.CT,
    checkpoints: []?ops.CT,
    replay: ReplaySource,
    identity: seeded.StepIdentity,
    epoch: u64,
    decisions: ?[32]u8 = null,
    consumed: bool = false,

    fn initialize(plan: *Plan, backend: *const ops.ComputeBackend, inputs: []const interpreter.RuntimeInput, replay: ReplaySource, identity: seeded.StepIdentity, control: ?Control) !Tape {
        var checks = CombinedControl{ .original = backend.execution_control, .request = control };
        const combined = checks.control();
        try combined.check();
        try validateBackend(plan, backend);
        const a = plan.allocator;
        const fixed = try a.alloc(?ops.CT, plan.inputs.len);
        errdefer a.free(fixed);
        @memset(fixed, null);
        var owned = false;
        errdefer if (owned and plan.options.session.execution == .resident_metal) for (fixed) |value| {
            if (value) |tensor| backend.free(tensor);
        };
        var required: usize = 0;
        var generated: usize = 0;
        for (plan.inputs) |input| {
            if (input.replay == null) required += 1 else generated += 1;
        }
        if (inputs.len != required) return error.MissingRecomputeBinding;
        if (generated != 0 and (replay.validate == null or replay.materialize == null)) return error.MissingRecomputeReplaySource;
        for (inputs) |input| {
            var position: ?usize = null;
            for (plan.inputs, 0..) |descriptor, index| if (descriptor.node == input.node_id and descriptor.replay == null) {
                position = index;
                break;
            };
            const index = position orelse return error.InvalidRecomputeBinding;
            if (fixed[index] != null) return error.DuplicateTrainingBinding;
            try seeded.validateTensor(a, backend, input.value, plan.inputs[index].shape);
            fixed[index] = input.value;
        }
        // Stage resident reference leases independently. No partial failure
        // may free one of the caller's original handles.
        if (plan.options.session.execution == .resident_metal) {
            for (fixed) |*value| value.* = null;
            owned = true;
            var cb = backend.*;
            cb.execution_control = combined;
            for (inputs) |input| {
                for (plan.inputs, 0..) |descriptor, index| if (descriptor.node == input.node_id) {
                    fixed[index] = try resident.lease(&cb, input.value, descriptor.shape, plan.options.session.resident);
                    break;
                };
            }
        }
        const checkpoints = try a.alloc(?ops.CT, plan.checkpoints.len);
        errdefer a.free(checkpoints);
        @memset(checkpoints, null);
        var result = Tape{ .plan = plan, .backend = backend, .fixed = fixed, .checkpoints = checkpoints, .replay = replay, .identity = identity, .epoch = plan.parameter_epoch };
        // The tape now owns metadata/leases. Retain outer metadata errdefers,
        // but release only checkpoint payloads on an initial-forward failure.
        plan.active_tape = true;
        errdefer {
            for (checkpoints) |value| if (value) |tensor| backend.free(tensor);
            plan.active_tape = false;
        }
        var cb = backend.*;
        cb.execution_control = combined;
        for (plan.regions) |*region| {
            try combined.check();
            var bindings = try result.bindRegion(region, &cb, combined);
            defer bindings.deinit(&cb);
            const captured = if (plan.options.session.execution == .resident_metal) resident_capture: {
                const mapped = try a.alloc(program.Binding, bindings.values.len);
                defer a.free(mapped);
                for (bindings.values, mapped) |input, *out| out.* = .{ .node_id = input.node_id, .value = input.value };
                const executed = try region.initial.execute(a, &cb, mapped, combined);
                break :resident_capture interpreter.CapturedValuesResult{ .values = executed.outputs, .allocator = executed.allocator };
            } else native_capture: {
                const mapped = try a.alloc(interpreter.RuntimeInput, bindings.values.len);
                defer a.free(mapped);
                for (bindings.values, mapped) |input, *out| out.* = .{ .node_id = region.initial.lowered.id_map[input.node_id], .value = input.value };
                break :native_capture try interpreter.captureNodeValues(a, &region.initial.lowered.graph, &cb, .{ .runtime_inputs = mapped, .cached_analysis = region.initial_analysis, .execution_control = combined, .strict_integer_constants = true }, region.initial.lowered.graph.outputs.items);
            };
            std.debug.assert(captured.values.len == region.outputs.len);
            // Independent snapshots transfer without a second data copy.
            for (captured.values, region.outputs) |value, index| {
                std.debug.assert(checkpoints[index] == null);
                checkpoints[index] = value;
            }
            captured.allocator.free(captured.values);
        }
        try result.validateReplay();
        try combined.check();
        return result;
    }

    fn validateReplay(self: *const Tape) !void {
        if (self.replay.validate) |validate| try validate(self.replay.context, self.replay.fingerprint);
    }

    fn bindRegion(self: *const Tape, region: *const Region, cb: *const ops.ComputeBackend, control: ?Control) !Bindings {
        try self.validateReplay();
        try check(control);
        const a = self.plan.allocator;
        var values = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        defer values.deinit(a);
        var generated = std.ArrayListUnmanaged(ops.CT).empty;
        defer generated.deinit(a);
        errdefer for (generated.items) |value| cb.free(value);
        for (region.entries) |entry| {
            if (entry.local == nil) continue;
            try values.append(a, .{ .node_id = entry.local, .value = self.checkpoints[entry.checkpoint] orelse return error.MissingRecomputeCheckpoint });
        }
        for (region.inputs) |route| {
            const input = self.plan.inputs[route.source];
            const value = if (input.replay) |recipe| replay_value: {
                try check(control);
                const owned = try self.replay.materialize.?(self.replay.context, a, cb, recipe, input.shape, self.identity, control);
                generated.append(a, owned) catch |err| {
                    cb.free(owned);
                    return err;
                };
                try seeded.validateTensor(a, cb, owned, input.shape);
                break :replay_value owned;
            } else self.fixed[route.source] orelse return error.MissingRecomputeBinding;
            try values.append(a, .{ .node_id = route.local, .value = value });
        }
        try self.validateReplay();
        const owned_values = try values.toOwnedSlice(a);
        errdefer a.free(owned_values);
        return .{ .values = owned_values, .generated = try generated.toOwnedSlice(a), .allocator = a };
    }

    /// Borrowed independent final checkpoint. The caller may use it as a head
    /// runtime binding until this tape is consumed; it must not mutate it.
    pub fn finalOutput(self: *const Tape) !ops.CT {
        if (self.consumed) return error.InvalidTrainingTape;
        return self.checkpoints[self.plan.final_slot] orelse error.MissingRecomputeCheckpoint;
    }

    pub fn sealDecisions(self: *Tape, fingerprint: [32]u8) !void {
        if (self.consumed) return error.InvalidTrainingTape;
        if (self.decisions != null) return error.TrainingDecisionsAlreadySealed;
        self.decisions = fingerprint;
    }

    pub fn deinit(self: *Tape) void {
        if (self.consumed) return;
        self.consumed = true;
        for (self.checkpoints) |value| if (value) |tensor| self.backend.free(tensor);
        if (self.plan.options.session.execution == .resident_metal) for (self.fixed) |value| {
            if (value) |tensor| self.backend.free(tensor);
        };
        self.plan.allocator.free(self.checkpoints);
        self.plan.allocator.free(self.fixed);
        self.plan.active_tape = false;
    }

    /// Null dFinal represents an absent live encoder path, not an explicit
    /// zero gradient. The owning trainer retains its established zero-touch
    /// policy. A non-null cotangent replays regions in a fixed reverse order.
    /// Every failure consumes this tape and publishes no parameter gradient.
    pub fn backward(self: *Tape, identity: seeded.StepIdentity, decisions: [32]u8, loss: f32, d_final: ?ops.CT, control: ?Control) !seeded.BackwardResult {
        if (self.consumed) return error.InvalidTrainingTape;
        defer self.deinit();
        self.plan.host_owner.failure = null;
        return self.backwardInternal(identity, decisions, loss, d_final, control) catch |err| return self.plan.host_owner.translate(err);
    }

    fn backwardInternal(self: *Tape, identity: seeded.StepIdentity, decisions: [32]u8, loss: f32, d_final: ?ops.CT, control: ?Control) !seeded.BackwardResult {
        const plan = self.plan;
        const a = plan.allocator;
        if (!std.meta.eql(self.identity, identity) or self.epoch != plan.parameter_epoch) return error.TrainingTapeIdentityMismatch;
        if (self.decisions == null or !std.mem.eql(u8, &self.decisions.?, &decisions)) return error.TrainingDecisionIdentityMismatch;
        if (!std.math.isFinite(loss)) return error.InvalidTrainingCotangent;
        try plan.validateRegionEpochs();
        var checks = CombinedControl{ .original = self.backend.execution_control, .request = control };
        const combined = checks.control();
        var cb = self.backend.*;
        cb.execution_control = combined;
        try combined.check();
        try self.validateReplay();
        const incoming = d_final orelse return emptyGradients(a, loss);
        const final_shape = plan.checkpoints[plan.final_slot].shape;
        try seeded.validateTensor(a, &cb, incoming, final_shape);
        if (!plan.needsBackward()) return error.UnexpectedRecomputeCotangent;
        const boundary = try a.alloc(?ops.CT, plan.checkpoints.len);
        defer a.free(boundary);
        @memset(boundary, null);
        // dFinal is owned by the separately consumed head result. Keep it
        // borrowed; every other boundary accumulator is owned by this call.
        boundary[plan.final_slot] = incoming;
        defer for (boundary, 0..) |value, index| {
            if (index != plan.final_slot) if (value) |tensor| cb.free(tensor);
        };
        const gradients = try a.alloc(?ops.CT, plan.parameters.len);
        defer a.free(gradients);
        @memset(gradients, null);
        defer for (gradients) |value| if (value) |tensor| cb.free(tensor);
        var control_bytes: usize = 0;
        var ordinal = plan.regions.len;
        while (ordinal != 0) {
            ordinal -= 1;
            const region = &plan.regions[ordinal];
            const session = if (region.session) |*value| value else continue;
            var present = false;
            for (region.outputs) |slot| present = present or boundary[slot] != null;
            if (!present) continue;
            try combined.check();
            var bindings = try self.bindRegion(region, &cb, combined);
            defer bindings.deinit(&cb);
            const local_identity = regionIdentity(identity, self.replay.fingerprint, region.key);
            var local_tape = try session.forward(&cb, bindings.values, local_identity, combined);
            defer local_tape.deinit();
            const cotangents = try a.alloc(ops.CT, region.outputs.len);
            defer a.free(cotangents);
            var made: usize = 0;
            defer for (cotangents[0..made], region.outputs[0..made]) |value, slot| {
                if (boundary[slot] == null) cb.free(value);
            };
            for (region.outputs, cotangents) |slot, *value| {
                value.* = boundary[slot] orelse try zeroTensor(plan, &cb, plan.checkpoints[slot].shape);
                made += 1;
            }
            try local_tape.sealDecisions(decisions);
            var result = try local_tape.backward(local_identity, decisions, loss, cotangents, combined);
            var result_owned = true;
            errdefer if (result_owned) result.deinit(&cb);
            control_bytes = try add(control_bytes, result.control_readback_bytes);
            const outputs = try ownGradientOutputs(plan, region, &cb, &result);
            result_owned = false;
            defer a.free(outputs);
            defer for (outputs) |value| if (value) |tensor| cb.free(tensor);
            for (region.gradients, outputs) |route, *value| {
                const tensor = value.* orelse unreachable;
                value.* = null; // accumulate consumes tensor even on failure
                switch (route.destination) {
                    .checkpoint => |slot| try accumulate(plan, &cb, &boundary[slot], tensor, plan.checkpoints[slot].shape),
                    .parameter => |slot| try accumulate(plan, &cb, &gradients[slot], tensor, plan.parameter_shapes[slot]),
                }
            }
            // These checkpoints cannot be read by an earlier region. Shared
            // inputs remain live until their single producing-region replay.
            for (region.outputs) |slot| {
                if (self.checkpoints[slot]) |value| cb.free(value);
                self.checkpoints[slot] = null;
                // Keep cotangent buffers through the scoped zero-buffer
                // cleanup above. Boundary payloads are freed at call teardown.
            }
        }
        try combined.check();
        try self.validateReplay();
        var count: usize = 0;
        for (gradients) |value| if (value != null) {
            count += 1;
        };
        const ids = try a.alloc(Id, count);
        errdefer a.free(ids);
        const outputs = try a.alloc(ops.CT, count);
        var index: usize = 0;
        for (gradients, plan.parameters) |*value, id| if (value.*) |tensor| {
            ids[index] = id;
            outputs[index] = tensor;
            value.* = null;
            index += 1;
        };
        return .{ .loss = loss, .gradients = .{ .outputs = outputs, .allocator = a }, .parameter_ids = ids, .allocator = a, .control_readback_bytes = control_bytes };
    }
};

fn validateBackend(plan: *const Plan, cb: *const ops.ComputeBackend) !void {
    switch (plan.options.session.execution) {
        .native => if (cb.kind() != .native) return error.UnsupportedSeededTrainingBackend,
        .resident_metal => if (cb.kind() != .metal or cb.vtable.residentTrainingInstruction == null or cb.vtable.residentTrainingPrimitive == null or cb.vtable.residentTrainingNorm == null or cb.vtable.snapshotTensorShape == null) return error.UnsupportedSeededTrainingBackend,
    }
}

fn regionIdentity(identity: seeded.StepIdentity, fingerprint: [32]u8, key: u64) seeded.StepIdentity {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(profile);
    hash.update(&identity.binding);
    hash.update(&fingerprint);
    var buffer: [8]u8 = undefined;
    std.mem.writeInt(u64, &buffer, key, .little);
    hash.update(&buffer);
    var result = identity;
    hash.final(&result.binding);
    return result;
}

fn emptyGradients(a: Allocator, loss: f32) !seeded.BackwardResult {
    const ids = try a.alloc(Id, 0);
    errdefer a.free(ids);
    return .{ .loss = loss, .gradients = .{ .outputs = try a.alloc(ops.CT, 0), .allocator = a }, .parameter_ids = ids, .allocator = a };
}

fn zeroTensor(plan: *const Plan, cb: *const ops.ComputeBackend, shape: ml.Shape) !ops.CT {
    var dimensions: [8]i32 = undefined;
    const dims = shape32(shape, &dimensions);
    if (plan.options.session.execution == .resident_metal) {
        const scalar = try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = &.{0}, .shape = &.{} } }, plan.options.session.resident.program.instruction.primitive);
        defer cb.free(scalar);
        const instruction = ops.resident_program.Instruction{ .op = .{ .broadcast_in_dim = .{ .target_shape = shape, .num_axes = 0 } }, .output = shape, .inputs = .{ ml.Shape.init(.f32, &.{}), .{}, .{}, .{} }, .num_inputs = 1 };
        return cb.residentTrainingInstruction(&instruction, &.{scalar}, plan.options.session.resident.program.instruction);
    }
    const data = try plan.allocator.alloc(f32, (try bytes(shape)) / 4);
    defer plan.allocator.free(data);
    @memset(data, 0);
    return cb.fromFloat32Shape(data, dims);
}

/// Transfer unique gradient handles without copying large embedding gradients.
/// Duplicate output IDs receive an explicitly admitted independent native
/// copy (or a resident immutable lease). Unexpected aliases fail closed.
fn ownGradientOutputs(plan: *const Plan, region: *const Region, cb: *const ops.ComputeBackend, result: *seeded.BackwardResult) ![]?ops.CT {
    const a = plan.allocator;
    if (result.parameter_ids.len != region.gradients.len or result.gradients.outputs.len != region.gradients.len) return error.InvalidRecomputeGradient;
    for (result.parameter_ids, region.gradients) |id, route| if (id != route.local) return error.InvalidRecomputeGradient;
    const outputs = try a.alloc(?ops.CT, region.gradients.len);
    errdefer a.free(outputs);
    @memset(outputs, null);
    errdefer for (outputs, region.duplicate_gradients) |value, duplicate| {
        if (duplicate != null) if (value) |tensor| cb.free(tensor);
    };
    for (result.gradients.outputs, region.duplicate_gradients, outputs, 0..) |tensor, duplicate, *output, index| {
        if (duplicate) |_| {
            const shape = region.cut.graph.node(region.gradients[index].local).output_shape;
            if (plan.options.session.execution == .resident_metal) {
                output.* = try resident.lease(cb, tensor, shape, plan.options.session.resident);
            } else {
                const data = try cb.toFloat32(tensor, a);
                defer a.free(data);
                var dimensions: [8]i32 = undefined;
                output.* = try cb.fromFloat32Shape(data, shape32(shape, &dimensions));
            }
        } else {
            for (result.gradients.outputs[0..index]) |prior| if (prior == tensor) return error.UnexpectedRecomputeGradientAlias;
            output.* = tensor;
        }
    }
    // Resident result snapshots are independent even for repeated graph IDs;
    // the duplicate replacement's original is owned only in that case.
    for (result.gradients.outputs, region.duplicate_gradients, 0..) |tensor, duplicate, index| {
        if (duplicate != null) {
            var repeated = false;
            for (result.gradients.outputs[0..index]) |prior| repeated = repeated or prior == tensor;
            if (!repeated) cb.free(tensor);
        }
    }
    result.gradients.allocator.free(result.gradients.outputs);
    result.allocator.free(result.parameter_ids);
    return outputs;
}

/// Takes ownership of incoming on every path. The fixed order is region
/// reverse order, then local selected-parameter order; there are no atomics.
fn accumulate(plan: *const Plan, cb: *const ops.ComputeBackend, destination: *?ops.CT, incoming: ops.CT, shape: ml.Shape) !void {
    errdefer cb.free(incoming);
    try seeded.validateTensor(plan.allocator, cb, incoming, shape);
    const old = destination.* orelse {
        destination.* = incoming;
        return;
    };
    const sum = if (plan.options.session.execution == .resident_metal) resident_sum: {
        const instruction = ops.resident_program.Instruction{ .op = .add, .output = shape, .inputs = .{ shape, shape, .{}, .{} }, .num_inputs = 2 };
        break :resident_sum try cb.residentTrainingInstruction(&instruction, &.{ old, incoming }, plan.options.session.resident.program.instruction);
    } else try cb.add(old, incoming);
    errdefer cb.free(sum);
    try seeded.validateTensor(plan.allocator, cb, sum, shape);
    cb.free(old);
    cb.free(incoming);
    destination.* = sum;
}

fn validateRequest(source: *const ml.Graph, specs: []const RegionSpec, selected: []const Id, policy: BindingPolicy, options: Options) !void {
    const limits = options.limits;
    if (limits.max_regions == 0 or limits.max_regions > 256 or specs.len == 0 or specs.len > limits.max_regions or
        source.nodeCount() == 0 or source.nodeCount() > limits.max_source_nodes or limits.max_source_nodes > 1_000_000 or
        limits.max_plan_host_bytes == 0 or limits.max_compile_bytes == 0 or limits.max_checkpoint_bytes == 0 or limits.max_gradient_bytes == 0 or
        limits.max_backend_bytes == 0 or limits.max_host_bytes == 0 or limits.max_total_work == 0 or selected.len > 65536 or
        policy.managed_parameters.len > 65536 or policy.replay_inputs.len > 65536) return error.RecomputeLimitExceeded;
    for (source.nodes.items) |node| {
        if (node.num_inputs > 4 or node.output_shape.rank_ > 8) return error.InvalidRecomputeGraph;
        _ = try bytes(node.output_shape);
        for (node.getInputs()) |input| if (input == nil or input >= source.nodeCount()) return error.InvalidRecomputeGraph;
        if (node.vjp_alternate != nil and node.vjp_alternate >= source.nodeCount()) return error.InvalidRecomputeGraph;
        if (node.op == .parameter) {
            const attrs = node.op.parameter;
            if (attrs.name_offset > source.string_table.items.len or attrs.name_len > source.string_table.items.len - attrs.name_offset) return error.InvalidRecomputeGraph;
            if (std.mem.startsWith(u8, source.parameterName(&node), "__antfly_recompute_seed.")) return error.InvalidRecomputeGraph;
        }
    }
    for (specs, 0..) |spec, index| {
        if (spec.outputs.len == 0 or spec.outputs.len > 32 or spec.inputs.len > 64) return error.InvalidRecomputeBoundary;
        for (specs[0..index]) |prior| if (prior.key == spec.key) return error.DuplicateRecomputeRegion;
        for (spec.outputs) |id| if (id >= source.nodeCount()) return error.InvalidRecomputeBoundary;
        for (spec.inputs) |id| if (id >= source.nodeCount()) return error.InvalidRecomputeBoundary;
    }
    for (selected, 0..) |id, index| {
        if (id >= source.nodeCount() or source.node(id).op != .parameter or source.node(id).output_shape.dtype != .f32) return error.InvalidRecomputeParameter;
        if (contains(selected[0..index], id)) return error.DuplicateGradientParameter;
    }
    for (policy.managed_parameters, 0..) |id, index| {
        if (id >= source.nodeCount() or source.node(id).op != .parameter or source.node(id).output_shape.dtype != .f32 or contains(policy.managed_parameters[0..index], id)) return error.InvalidRecomputeParameter;
    }
    for (policy.replay_inputs, 0..) |input, index| {
        if (input.node >= source.nodeCount() or source.node(input.node).op != .parameter or contains(selected, input.node) or contains(policy.managed_parameters, input.node)) return error.InvalidRecomputeReplayInput;
        for (policy.replay_inputs[0..index]) |prior| if (prior.node == input.node) return error.InvalidRecomputeReplayInput;
    }
}
