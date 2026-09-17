// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit cotangent training with retained forward activations. The backward
//! program replaces captured values with input leaves, so binding a tape never
//! replays their encoder/dropout ancestors. Model parameters are borrowed under
//! a caller-owned managed session and immutable parameter epoch.
const std = @import("std");
const ml = @import("ml").graph;
const ops = @import("../ops/ops.zig");
const interpreter = @import("interpreter.zig");
const resident_execution = @import("resident_training_execution.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const Id = ml.NodeId;

pub const Execution = enum { native, resident_metal };
pub const Options = struct {
    gradient: ml.autodiff.SeedOptions = .{},
    max_tape_bytes: usize = 256 * 1024 * 1024,
    max_cotangent_bytes: usize = 64 * 1024 * 1024,
    execution: Execution = .native,
    resident: resident_execution.Limits = .{},
    /// Explicit forward-only tape for a selected set with no live gradient
    /// path. The owning trainer decides zero-touch versus absent slot updates;
    /// this session never invents a gradient or silently weakens strict VJPs.
    allow_no_gradients: bool = false,
};

/// The caller fingerprints sample/schema/candidate inputs and dropout masks
/// together. Matching decisions are sealed after forward. Another microbatch or
/// parameter epoch must never be applied to a retained forward computation.
pub const StepIdentity = struct { binding: [32]u8, optimizer_step: u64, microbatch: u64 };

pub const BackwardResult = struct {
    /// The actual reduced objective supplied alongside its cotangents.
    loss: f32,
    gradients: interpreter.ExecutionResult,
    /// Original graph parameter IDs aligned with gradients.outputs. Inactive
    /// heads are omitted explicitly; callers never infer an optimizer slot
    /// from a shortened positional gradient list.
    parameter_ids: []Id,
    allocator: Allocator,
    /// Resident finite-check scalar summaries; activation/gradient tensors stay
    /// on device. Native execution leaves this at zero.
    control_readback_bytes: usize = 0,
    pub fn deinit(self: *BackwardResult, cb: *const ops.ComputeBackend) void {
        self.gradients.deinit(cb);
        self.allocator.free(self.parameter_ids);
        self.* = undefined;
    }
};

pub fn shapeBytes(shape: ml.Shape) !usize {
    if (shape.rank_ > ml.shape.max_rank) return error.InvalidTrainingShape;
    const elements = shape.numElements() orelse return error.InvalidTrainingShape;
    if (elements < 0) return error.InvalidTrainingShape;
    return std.math.mul(usize, @intCast(elements), shape.dtype.byteSize()) catch error.TrainingTapeLimitExceeded;
}

pub fn validateTensor(a: Allocator, cb: *const ops.ComputeBackend, tensor: ops.CT, shape: ml.Shape) !void {
    if (!std.mem.eql(u8, @tagName(try cb.tensorDType(tensor)), @tagName(shape.dtype))) return error.TrainingBindingDTypeMismatch;
    const dimensions = try cb.tensorShape(tensor, a);
    defer a.free(dimensions);
    if (!std.mem.eql(i64, dimensions, shape.dims[0..shape.rank_])) return error.TrainingBindingShapeMismatch;
}

/// Make independent graph ownership through the ordinary lowering pass, using
/// a temporary read-only view whose cut nodes are explicit runtime parameters.
pub fn cutProgram(a: Allocator, graph: *const ml.Graph, cuts: []const Id, outputs: []const Id) !ml.lower.LowerResult {
    const nodes = try a.dupe(ml.Node, graph.nodes.items);
    defer a.free(nodes);
    var strings = std.ArrayListUnmanaged(u8).empty;
    defer strings.deinit(a);
    try strings.appendSlice(a, graph.string_table.items);
    var parameters = std.ArrayListUnmanaged(Id).empty;
    defer parameters.deinit(a);
    try parameters.appendSlice(a, graph.parameters.items);
    for (cuts) |id| {
        if (id >= nodes.len) return error.InvalidTrainingCut;
        var buffer: [64]u8 = undefined;
        const name = try std.fmt.bufPrint(&buffer, "__antfly_training_tape_{d}", .{id});
        const offset = std.math.cast(u32, strings.items.len) orelse return error.TrainingTapeLimitExceeded;
        try strings.appendSlice(a, name);
        nodes[id].op = .{ .parameter = .{ .name_offset = offset, .name_len = @intCast(name.len) } };
        nodes[id].inputs = .{ml.null_node} ** 4;
        nodes[id].num_inputs = 0;
        nodes[id].vjp_alternate = ml.null_node;
        try parameters.append(a, id);
    }
    var view = graph.*;
    view.nodes = .{ .items = nodes, .capacity = nodes.len };
    view.string_table = strings;
    view.parameters = parameters;
    view.outputs = .{ .items = @constCast(outputs), .capacity = outputs.len };
    return ml.lower.lower(a, &view);
}

pub const Session = struct {
    allocator: Allocator,
    differentiated: ml.autodiff.GradientResult,
    backward: ml.lower.LowerResult,
    forward_analysis: interpreter.CachedAnalysis,
    backward_analysis: interpreter.CachedAnalysis,
    /// Lowered forward IDs. Includes seed logits for loss computation.
    captures: []Id,
    seeds: []ml.autodiff.Seed,
    original_parameters: []bool,
    gradient_parameters: []Id,
    tape_bytes: usize,
    options: Options,
    resident: ?resident_execution.Plans = null,
    parameter_epoch: u64 = 0,
    active_tape: bool = false,

    pub fn init(a: Allocator, graph: *const ml.Graph, seeds: []const ml.autodiff.Seed, wrt: []const Id, options: Options) !Session {
        return initInternal(a, graph, seeds, wrt, options, true);
    }

    /// Stage constructors supply their exact cut-forward programs after the
    /// shared differentiation is built. No resident forward can execute until
    /// configureResident has admitted every stage and the final backward.
    pub fn initForStages(a: Allocator, graph: *const ml.Graph, seeds: []const ml.autodiff.Seed, wrt: []const Id, options: Options) !Session {
        return initInternal(a, graph, seeds, wrt, options, false);
    }

    fn initInternal(a: Allocator, graph: *const ml.Graph, seeds: []const ml.autodiff.Seed, wrt: []const Id, options: Options, direct: bool) !Session {
        var differentiated = try ml.autodiff.gradientWithSeeds(a, graph, seeds, wrt, options.gradient);
        errdefer differentiated.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var outputs = std.ArrayListUnmanaged(Id).empty;
        var gradient_parameters = std.ArrayListUnmanaged(Id).empty;
        for (differentiated.param_grads, wrt) |id, parameter| if (id != ml.null_node) {
            try outputs.append(scratch, id);
            try gradient_parameters.append(scratch, parameter);
        };
        if (outputs.items.len == 0 and !options.allow_no_gradients) return error.DisconnectedGradientParameter;
        const seen = try scratch.alloc(bool, differentiated.graph.nodeCount());
        @memset(seen, false);
        var stack = std.ArrayListUnmanaged(Id).empty;
        try stack.appendSlice(scratch, outputs.items);
        var captures = std.ArrayListUnmanaged(Id).empty;
        while (stack.pop()) |id| {
            if (seen[id]) continue;
            seen[id] = true;
            const node = differentiated.graph.node(id);
            if (id < differentiated.forward_node_count) {
                if (node.op != .parameter and node.op != .constant) try captures.append(scratch, id);
                continue;
            }
            for (node.getInputs()) |input| if (input != ml.null_node) try stack.append(scratch, input);
        }
        // Backward leaves need only the retained dependency boundary. Logits
        // are also retained for the external, independently tested losses.
        var backward = if (outputs.items.len == 0) empty: {
            const mapping = try a.alloc(Id, differentiated.graph.nodeCount());
            @memset(mapping, ml.null_node);
            break :empty ml.lower.LowerResult{ .graph = ml.Graph.init(a), .id_map = mapping };
        } else try cutProgram(a, &differentiated.graph, captures.items, outputs.items);
        errdefer backward.deinit();
        for (seeds) |seed| {
            const mapped = differentiated.id_map[seed.output];
            if (std.mem.indexOfScalar(Id, captures.items, mapped) == null) try captures.append(scratch, mapped);
        }
        std.mem.sort(Id, captures.items, {}, std.sort.asc(Id));
        var tape_bytes: usize = 0;
        for (captures.items) |id| tape_bytes = std.math.add(usize, tape_bytes, try shapeBytes(differentiated.graph.node(id).output_shape)) catch return error.TrainingTapeLimitExceeded;
        if (tape_bytes > options.max_tape_bytes or options.max_cotangent_bytes == 0) return error.TrainingTapeLimitExceeded;
        var forward_analysis = try interpreter.CachedAnalysis.computeForTargets(a, &differentiated.graph, captures.items);
        errdefer forward_analysis.deinit(a);
        var backward_analysis = try interpreter.CachedAnalysis.compute(a, &backward.graph);
        errdefer backward_analysis.deinit(a);
        const capture_ids = try a.dupe(Id, captures.items);
        errdefer a.free(capture_ids);
        const owned_seeds = try a.dupe(ml.autodiff.Seed, seeds);
        errdefer a.free(owned_seeds);
        const original_parameters = try a.alloc(bool, graph.nodeCount());
        errdefer a.free(original_parameters);
        @memset(original_parameters, false);
        for (graph.parameters.items) |id| original_parameters[id] = true;
        const owned_parameters = try a.dupe(Id, gradient_parameters.items);
        errdefer a.free(owned_parameters);
        var result = Session{ .allocator = a, .differentiated = differentiated, .backward = backward, .forward_analysis = forward_analysis, .backward_analysis = backward_analysis, .captures = capture_ids, .seeds = owned_seeds, .original_parameters = original_parameters, .gradient_parameters = owned_parameters, .tape_bytes = tape_bytes, .options = options };
        if (direct) try result.configureResident(&.{.{ .graph = &result.differentiated.graph, .targets = result.captures }}, tape_bytes);
        return result;
    }

    pub fn configureResident(self: *Session, forwards: []const resident_execution.Spec, retained_bytes: usize) !void {
        if (self.options.execution == .native) return;
        if (self.active_tape or self.resident != null) return error.InvalidTrainingStage;
        var binding_bytes: usize = 0;
        for (self.differentiated.graph.parameters.items) |id| {
            binding_bytes = std.math.add(usize, binding_bytes, try shapeBytes(self.differentiated.graph.node(id).output_shape)) catch return error.TrainingTapeLimitExceeded;
        }
        if (self.seeds.len > 16384) return error.TrainingTapeLimitExceeded;
        const shapes = try self.allocator.alloc(ml.Shape, self.seeds.len);
        defer self.allocator.free(shapes);
        for (self.seeds, shapes) |seed, *shape| shape.* = self.differentiated.graph.node(self.differentiated.id_map[seed.cotangent]).output_shape;
        const cotangents = try resident_execution.planCotangents(shapes, self.options.max_cotangent_bytes, self.options.resident);
        self.resident = try resident_execution.Plans.init(self.allocator, forwards, .{ .graph = &self.backward.graph, .targets = self.backward.graph.outputs.items }, binding_bytes, retained_bytes, cotangents, self.options.resident);
    }

    pub fn executionAdmission(self: *const Session) ?resident_execution.Admission {
        return if (self.resident) |*compiled| compiled.admission else null;
    }

    pub fn validateBackend(self: *const Session, cb: *const ops.ComputeBackend) !void {
        switch (self.options.execution) {
            .native => if (cb.kind() != .native) return error.UnsupportedSeededTrainingBackend,
            .resident_metal => if (cb.kind() != .metal or self.resident == null or cb.vtable.residentTrainingInstruction == null or cb.vtable.residentTrainingPrimitive == null or cb.vtable.residentTrainingNorm == null or cb.vtable.snapshotTensorShape == null) return error.UnsupportedSeededTrainingBackend,
        }
    }

    pub fn leaseInput(self: *const Session, cb: *const ops.ComputeBackend, input: interpreter.RuntimeInput) !interpreter.RuntimeInput {
        if (self.options.execution == .native) return input;
        if (input.node_id >= self.differentiated.graph.nodeCount()) return error.InvalidTrainingBinding;
        return .{ .node_id = input.node_id, .value = try resident_execution.lease(cb, input.value, self.differentiated.graph.node(input.node_id).output_shape, self.options.resident) };
    }

    pub fn releaseInputs(self: *const Session, cb: *const ops.ComputeBackend, inputs: []const interpreter.RuntimeInput) void {
        if (self.options.execution == .resident_metal) for (inputs) |input| cb.free(input.value);
    }

    pub fn deinit(self: *Session) void {
        std.debug.assert(!self.active_tape);
        if (self.resident) |*compiled| compiled.deinit();
        self.backward_analysis.deinit(self.allocator);
        self.forward_analysis.deinit(self.allocator);
        self.backward.deinit();
        self.differentiated.deinit();
        self.allocator.free(self.captures);
        self.allocator.free(self.seeds);
        self.allocator.free(self.original_parameters);
        self.allocator.free(self.gradient_parameters);
        self.* = undefined;
    }

    pub fn advanceParameterEpoch(self: *Session) !void {
        if (self.active_tape) return error.TrainingTapeStillLive;
        self.parameter_epoch = std.math.add(u64, self.parameter_epoch, 1) catch return error.TrainingEpochOverflow;
    }

    /// Inputs use original graph parameter IDs and must remain immutable until
    /// tape teardown. Weights may use the managed backend's normal named store;
    /// explicit dynamic inputs are type/shape validated before execution.
    pub fn forward(self: *Session, cb: *const ops.ComputeBackend, inputs: []const interpreter.RuntimeInput, identity: StepIdentity, control: ?Control) !Tape {
        if (control) |active| try active.check();
        try self.validateBackend(cb);
        if (self.active_tape) return error.TrainingTapeStillLive;
        const a = self.allocator;
        const mapped = try a.alloc(interpreter.RuntimeInput, inputs.len);
        errdefer a.free(mapped);
        var leased: usize = 0;
        errdefer self.releaseInputs(cb, mapped[0..leased]);
        for (inputs, mapped, 0..) |input, *out, index| {
            if (input.node_id >= self.original_parameters.len or !self.original_parameters[input.node_id]) return error.InvalidTrainingBinding;
            for (inputs[0..index]) |prior| if (prior.node_id == input.node_id) return error.DuplicateTrainingBinding;
            for (self.seeds) |seed| if (seed.cotangent == input.node_id) return error.GradientSeedUsedByForward;
            const id = self.differentiated.id_map[input.node_id];
            if (id == ml.null_node) return error.UnusedTrainingBinding;
            try validateTensor(a, cb, input.value, self.differentiated.graph.node(id).output_shape);
            out.* = .{ .node_id = id, .value = input.value };
        }
        for (mapped) |*input| {
            input.* = try self.leaseInput(cb, input.*);
            leased += 1;
        }
        var captured = if (self.resident) |*compiled|
            try compiled.capture(0, cb, mapped, control)
        else
            try interpreter.captureNodeValues(a, &self.differentiated.graph, cb, .{ .runtime_inputs = mapped, .cached_analysis = self.forward_analysis, .execution_control = control, .strict_integer_constants = true }, self.captures);
        errdefer captured.deinit(cb);
        if (control) |active| try active.check();
        self.active_tape = true;
        return .{ .session = self, .cb = cb, .inputs = mapped, .captured = captured, .identity = identity, .epoch = self.parameter_epoch };
    }
};

pub const Tape = struct {
    session: *Session,
    cb: *const ops.ComputeBackend,
    inputs: []interpreter.RuntimeInput,
    captured: interpreter.CapturedValuesResult,
    identity: StepIdentity,
    epoch: u64,
    consumed: bool = false,
    decisions: ?[32]u8 = null,

    /// Seal gold targets, hard-negative membership and assignment identities
    /// once they have been computed from this tape's live logits. Subsequent
    /// replacement is rejected; the loss/cotangent producer supplies this same
    /// fingerprint when consuming the tape.
    pub fn sealDecisions(self: *Tape, fingerprint: [32]u8) !void {
        if (self.consumed) return error.InvalidTrainingTape;
        if (self.decisions != null) return error.TrainingDecisionsAlreadySealed;
        self.decisions = fingerprint;
    }

    pub fn deinit(self: *Tape) void {
        if (self.consumed) return;
        self.consumed = true;
        self.captured.deinit(self.cb);
        self.session.releaseInputs(self.cb, self.inputs);
        self.session.allocator.free(self.inputs);
        self.session.active_tape = false;
    }

    pub fn logits(self: *const Tape, seed_index: usize) !ops.CT {
        if (self.consumed or seed_index >= self.session.seeds.len) return error.InvalidTrainingTape;
        const node = self.session.differentiated.id_map[self.session.seeds[seed_index].output];
        const index = std.mem.indexOfScalar(Id, self.session.captures, node) orelse return error.InvalidTrainingTape;
        return self.captured.values[index];
    }

    /// Consumes captures on success and failure. Each cotangent corresponds to
    /// one declared seed; duplicate seed parameters require identical CTs.
    /// Parameter epoch changes are prohibited until this method or deinit ends.
    pub fn backward(self: *Tape, identity: StepIdentity, decisions: [32]u8, loss: f32, cotangents: []const ops.CT, control: ?Control) !BackwardResult {
        if (self.consumed) return error.InvalidTrainingTape;
        defer self.deinit();
        const session = self.session;
        if (!std.meta.eql(identity, self.identity) or self.epoch != session.parameter_epoch) return error.TrainingTapeIdentityMismatch;
        if (self.decisions == null or !std.mem.eql(u8, &self.decisions.?, &decisions)) return error.TrainingDecisionIdentityMismatch;
        if (!std.math.isFinite(loss) or cotangents.len != session.seeds.len) return error.InvalidTrainingCotangent;
        if (control) |active| try active.check();
        const a = session.allocator;
        var inputs = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        defer inputs.deinit(a);
        for (self.inputs) |input| {
            const mapped = session.backward.id_map[input.node_id];
            if (mapped != ml.null_node) try inputs.append(a, .{ .node_id = mapped, .value = input.value });
        }
        for (session.captures, self.captured.values) |id, value| {
            const mapped = session.backward.id_map[id];
            if (mapped == ml.null_node) continue;
            // An exposed output may itself be an immutable input parameter.
            // Its retained snapshot and leased input refer to the same graph
            // leaf; bind it once for the strict resident program.
            var present = false;
            for (inputs.items) |input| if (input.node_id == mapped) {
                present = true;
                break;
            };
            if (!present) try inputs.append(a, .{ .node_id = mapped, .value = value });
        }
        var bytes: usize = 0;
        const seed_shapes: []ml.Shape = if (session.options.execution == .resident_metal) try a.alloc(ml.Shape, cotangents.len) else &.{};
        defer if (session.options.execution == .resident_metal) a.free(seed_shapes);
        for (session.seeds, cotangents, 0..) |seed, value, seed_index| {
            const id = session.differentiated.id_map[seed.cotangent];
            const shape = session.differentiated.graph.node(id).output_shape;
            bytes = std.math.add(usize, bytes, try shapeBytes(shape)) catch return error.TrainingTapeLimitExceeded;
            if (bytes > session.options.max_cotangent_bytes) return error.TrainingTapeLimitExceeded;
            try validateTensor(a, self.cb, value, shape);
            if (session.options.execution == .resident_metal) {
                seed_shapes[seed_index] = shape;
            } else {
                const checked = try self.cb.toFloat32(value, a);
                defer a.free(checked);
                for (checked) |element| if (!std.math.isFinite(element)) return error.InvalidTrainingCotangent;
            }
            const mapped = session.backward.id_map[id];
            if (mapped == ml.null_node) continue;
            var duplicate = false;
            for (inputs.items) |input| if (input.node_id == mapped) {
                if (input.value != value) return error.ConflictingTrainingCotangent;
                duplicate = true;
            };
            if (!duplicate) try inputs.append(a, .{ .node_id = mapped, .value = value });
        }
        const control_readback_bytes = if (session.options.execution == .resident_metal) try resident_execution.finiteCotangents(a, self.cb, cotangents, seed_shapes, session.options.max_cotangent_bytes, session.options.resident, control) else 0;
        var gradients = if (session.gradient_parameters.len == 0)
            interpreter.ExecutionResult{ .outputs = try a.alloc(ops.CT, 0), .allocator = a }
        else if (session.resident) |*compiled|
            try compiled.gradients(self.cb, inputs.items, control)
        else
            try interpreter.execute(a, &session.backward.graph, self.cb, .{ .runtime_inputs = inputs.items, .cached_analysis = session.backward_analysis, .execution_control = control, .strict_integer_constants = true });
        errdefer gradients.deinit(self.cb);
        if (control) |active| try active.check();
        const parameter_ids = try a.dupe(Id, session.gradient_parameters);
        return .{ .loss = loss, .gradients = gradients, .parameter_ids = parameter_ids, .allocator = a, .control_readback_bytes = control_readback_bytes };
    }
};
