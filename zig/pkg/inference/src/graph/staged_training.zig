// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A fixed-capacity training graph with a detached decision boundary. Prefix
//! logits select candidate indices; the suffix reads those indices and the
//! retained prefix activations. Neither suffix nor backward can replay a
//! prefix encoder or dropout operation. All weights belong to one epoch.
const std = @import("std");
const ml = @import("ml").graph;
const seeded = @import("seeded_training.zig");
const interpreter = @import("interpreter.zig");
const ops = @import("../ops/ops.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const Id = ml.NodeId;

pub const Boundary = struct {
    /// Original parameter IDs for detached candidate indices, masks, geometry
    /// and any suffix-only dropout masks. They must be bound exactly once.
    deferred_parameters: []const Id,
    /// Original output IDs needed to make the detached candidate decisions.
    outputs: []const Id,
};

pub const Session = struct {
    base: seeded.Session,
    prefix_analysis: interpreter.CachedAnalysis,
    suffix: ml.lower.LowerResult,
    suffix_analysis: interpreter.CachedAnalysis,
    prefix_captures: []Id,
    suffix_captures: []Id,
    deferred: []Id,
    exposed: []Id,

    pub fn init(a: Allocator, graph: *const ml.Graph, seeds: []const ml.autodiff.Seed, wrt: []const Id, boundary: Boundary, options: seeded.Options) !Session {
        if (boundary.deferred_parameters.len == 0 or boundary.outputs.len == 0) return error.InvalidTrainingStage;
        var base = try seeded.Session.initForStages(a, graph, seeds, wrt, options);
        errdefer base.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const forward_count = base.differentiated.forward_node_count;
        const g = &base.differentiated.graph;
        const late = try scratch.alloc(bool, forward_count);
        @memset(late, false);
        const deferred = try a.alloc(Id, boundary.deferred_parameters.len);
        errdefer a.free(deferred);
        for (boundary.deferred_parameters, deferred, 0..) |original, *mapped, i| {
            if (original >= base.original_parameters.len or !base.original_parameters[original]) return error.InvalidTrainingStage;
            for (boundary.deferred_parameters[0..i]) |prior| if (prior == original) return error.DuplicateTrainingBinding;
            for (seeds) |seed| if (seed.cotangent == original) return error.InvalidTrainingStage;
            mapped.* = base.differentiated.id_map[original];
            if (mapped.* == ml.null_node or mapped.* >= forward_count) return error.UnusedTrainingBinding;
            late[mapped.*] = true;
        }
        // The differentiated forward prefix has already been topologically
        // lowered, including injected adapters whose original IDs may differ.
        for (0..forward_count) |i| for (g.node(@intCast(i)).getInputs()) |input| {
            if (input != ml.null_node and late[input]) late[i] = true;
        };
        const prefix_set = try scratch.alloc(bool, forward_count);
        @memset(prefix_set, false);
        for (0..forward_count) |i| if (late[i]) {
            for (g.node(@intCast(i)).getInputs()) |input| {
                if (input == ml.null_node or late[input]) continue;
                const node = g.node(input);
                if (node.op != .parameter and node.op != .constant) prefix_set[input] = true;
            }
        };
        var suffix_targets = std.ArrayListUnmanaged(Id).empty;
        for (base.captures) |id| {
            if (late[id]) try suffix_targets.append(scratch, id) else prefix_set[id] = true;
        }
        const exposed = try a.alloc(Id, boundary.outputs.len);
        errdefer a.free(exposed);
        for (boundary.outputs, exposed) |original, *mapped| {
            if (original >= base.differentiated.id_map.len) return error.InvalidTrainingStage;
            mapped.* = base.differentiated.id_map[original];
            if (mapped.* == ml.null_node or mapped.* >= forward_count or late[mapped.*]) return error.InvalidTrainingStage;
            prefix_set[mapped.*] = true;
        }
        var prefix_targets = std.ArrayListUnmanaged(Id).empty;
        for (prefix_set, 0..) |needed, id| if (needed) try prefix_targets.append(scratch, @intCast(id));
        var bytes: usize = 0;
        for (prefix_targets.items) |id| bytes = std.math.add(usize, bytes, try seeded.shapeBytes(g.node(id).output_shape)) catch return error.TrainingTapeLimitExceeded;
        for (suffix_targets.items) |id| bytes = std.math.add(usize, bytes, try seeded.shapeBytes(g.node(id).output_shape)) catch return error.TrainingTapeLimitExceeded;
        if (bytes > options.max_tape_bytes) return error.TrainingTapeLimitExceeded;
        var prefix_analysis = try interpreter.CachedAnalysis.computeForTargets(a, g, prefix_targets.items);
        errdefer prefix_analysis.deinit(a);
        var suffix = try seeded.cutProgram(a, g, prefix_targets.items, suffix_targets.items);
        errdefer suffix.deinit();
        var suffix_analysis = try interpreter.CachedAnalysis.compute(a, &suffix.graph);
        errdefer suffix_analysis.deinit(a);
        const prefix_captures = try a.dupe(Id, prefix_targets.items);
        errdefer a.free(prefix_captures);
        const suffix_captures = try a.alloc(Id, suffix_targets.items.len);
        errdefer a.free(suffix_captures);
        for (suffix_targets.items, suffix_captures) |id, *out| out.* = suffix.id_map[id];
        try base.configureResident(&.{
            .{ .graph = &base.differentiated.graph, .targets = prefix_captures },
            .{ .graph = &suffix.graph, .targets = suffix_captures },
        }, bytes);
        return .{ .base = base, .prefix_analysis = prefix_analysis, .suffix = suffix, .suffix_analysis = suffix_analysis, .prefix_captures = prefix_captures, .suffix_captures = suffix_captures, .deferred = deferred, .exposed = exposed };
    }

    pub fn deinit(self: *Session) void {
        const a = self.base.allocator;
        self.prefix_analysis.deinit(a);
        self.suffix_analysis.deinit(a);
        self.suffix.deinit();
        a.free(self.prefix_captures);
        a.free(self.suffix_captures);
        a.free(self.deferred);
        a.free(self.exposed);
        self.base.deinit();
        self.* = undefined;
    }

    fn mapInput(self: *Session, cb: *const ops.ComputeBackend, input: interpreter.RuntimeInput) !interpreter.RuntimeInput {
        if (input.node_id >= self.base.original_parameters.len or !self.base.original_parameters[input.node_id]) return error.InvalidTrainingBinding;
        for (self.base.seeds) |seed| if (seed.cotangent == input.node_id) return error.GradientSeedUsedByForward;
        const mapped = self.base.differentiated.id_map[input.node_id];
        if (mapped == ml.null_node) return error.UnusedTrainingBinding;
        try seeded.validateTensor(self.base.allocator, cb, input.value, self.base.differentiated.graph.node(mapped).output_shape);
        return .{ .node_id = mapped, .value = input.value };
    }

    pub fn forwardPrefix(self: *Session, cb: *const ops.ComputeBackend, inputs: []const interpreter.RuntimeInput, identity: seeded.StepIdentity, control: ?Control) !PrefixTape {
        if (control) |active| try active.check();
        try self.base.validateBackend(cb);
        if (self.base.active_tape) return error.TrainingTapeStillLive;
        const a = self.base.allocator;
        const mapped = try a.alloc(interpreter.RuntimeInput, inputs.len);
        errdefer a.free(mapped);
        var leased: usize = 0;
        errdefer self.base.releaseInputs(cb, mapped[0..leased]);
        for (inputs, mapped, 0..) |input, *out, i| {
            out.* = try self.mapInput(cb, input);
            if (std.mem.indexOfScalar(Id, self.deferred, out.node_id) != null) return error.PrematureTrainingBinding;
            for (mapped[0..i]) |prior| if (prior.node_id == out.node_id) return error.DuplicateTrainingBinding;
        }
        for (mapped) |*input| {
            input.* = try self.base.leaseInput(cb, input.*);
            leased += 1;
        }
        var captured = if (self.base.resident) |*compiled|
            try compiled.capture(0, cb, mapped, control)
        else
            try interpreter.captureNodeValues(a, &self.base.differentiated.graph, cb, .{ .runtime_inputs = mapped, .cached_analysis = self.prefix_analysis, .execution_control = control, .strict_integer_constants = true }, self.prefix_captures);
        errdefer captured.deinit(cb);
        if (control) |active| try active.check();
        self.base.active_tape = true;
        return .{ .session = self, .cb = cb, .inputs = mapped, .captured = captured, .identity = identity, .epoch = self.base.parameter_epoch };
    }
};

pub const PrefixTape = struct {
    session: *Session,
    cb: *const ops.ComputeBackend,
    inputs: []interpreter.RuntimeInput,
    captured: interpreter.CapturedValuesResult,
    identity: seeded.StepIdentity,
    epoch: u64,
    consumed: bool = false,

    pub fn deinit(self: *PrefixTape) void {
        if (self.consumed) return;
        self.consumed = true;
        self.captured.deinit(self.cb);
        self.session.base.releaseInputs(self.cb, self.inputs);
        self.session.base.allocator.free(self.inputs);
        self.session.base.active_tape = false;
    }

    pub fn logits(self: *const PrefixTape, index: usize) !ops.CT {
        if (self.consumed or index >= self.session.exposed.len) return error.InvalidTrainingTape;
        const position = std.mem.indexOfScalar(Id, self.session.prefix_captures, self.session.exposed[index]) orelse return error.InvalidTrainingStage;
        return self.captured.values[position];
    }

    /// Consumes the prefix even when a binding or execution fails. The caller
    /// fingerprints the actual selected indices/masks; the returned tape's
    /// identity includes both the initial binding and this candidate decision.
    pub fn forwardSuffix(self: *PrefixTape, identity: seeded.StepIdentity, candidate_fingerprint: [32]u8, deferred_inputs: []const interpreter.RuntimeInput, control: ?Control) !seeded.Tape {
        if (self.consumed) return error.InvalidTrainingTape;
        errdefer self.deinit();
        const session = self.session;
        const a = session.base.allocator;
        if (!std.meta.eql(identity, self.identity) or self.epoch != session.base.parameter_epoch) return error.TrainingTapeIdentityMismatch;
        if (control) |active| try active.check();
        if (deferred_inputs.len != session.deferred.len) return error.MissingTrainingBinding;
        var all_inputs = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        defer {
            session.base.releaseInputs(self.cb, all_inputs.items);
            all_inputs.deinit(a);
        }
        for (self.inputs) |input| {
            const leased = try session.base.leaseInput(self.cb, input);
            all_inputs.append(a, leased) catch |err| {
                session.base.releaseInputs(self.cb, &.{leased});
                return err;
            };
        }
        for (deferred_inputs) |input| {
            const mapped = try session.mapInput(self.cb, input);
            if (std.mem.indexOfScalar(Id, session.deferred, mapped.node_id) == null) return error.InvalidTrainingStage;
            for (all_inputs.items) |prior| if (prior.node_id == mapped.node_id) return error.DuplicateTrainingBinding;
            const leased = try session.base.leaseInput(self.cb, mapped);
            all_inputs.append(a, leased) catch |err| {
                session.base.releaseInputs(self.cb, &.{leased});
                return err;
            };
        }
        var suffix_inputs = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        defer suffix_inputs.deinit(a);
        for (all_inputs.items) |input| {
            const mapped = session.suffix.id_map[input.node_id];
            if (mapped != ml.null_node) try suffix_inputs.append(a, .{ .node_id = mapped, .value = input.value });
        }
        for (session.prefix_captures, self.captured.values) |id, value| {
            const mapped = session.suffix.id_map[id];
            if (mapped == ml.null_node) continue;
            var present = false;
            for (suffix_inputs.items) |input| if (input.node_id == mapped) {
                present = true;
                break;
            };
            if (!present) try suffix_inputs.append(a, .{ .node_id = mapped, .value = value });
        }
        var suffix_values = if (session.base.resident) |*compiled|
            try compiled.capture(1, self.cb, suffix_inputs.items, control)
        else
            try interpreter.captureNodeValues(a, &session.suffix.graph, self.cb, .{ .runtime_inputs = suffix_inputs.items, .cached_analysis = session.suffix_analysis, .execution_control = control, .strict_integer_constants = true }, session.suffix_captures);
        errdefer suffix_values.deinit(self.cb);
        const values = try a.alloc(ops.CT, session.base.captures.len);
        errdefer a.free(values);
        for (session.base.captures, values) |id, *out| {
            if (std.mem.indexOfScalar(Id, session.prefix_captures, id)) |position| {
                out.* = self.captured.values[position];
            } else {
                const mapped = session.suffix.id_map[id];
                const position = std.mem.indexOfScalar(Id, session.suffix_captures, mapped) orelse return error.InvalidTrainingStage;
                out.* = suffix_values.values[position];
            }
        }
        const owned_inputs = try all_inputs.toOwnedSlice(a);
        errdefer {
            session.base.releaseInputs(self.cb, owned_inputs);
            a.free(owned_inputs);
        }
        if (control) |active| try active.check();
        var final_identity = self.identity;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.staged-training.binding.v1");
        hash.update(&self.identity.binding);
        hash.update(&candidate_fingerprint);
        hash.final(&final_identity.binding);
        // Transfer each retained CT exactly once. Prefix-only decision outputs
        // are no longer needed; suffix results are all backward/logit captures.
        for (session.prefix_captures, self.captured.values) |id, value| {
            if (std.mem.indexOfScalar(Id, session.base.captures, id) == null) self.cb.free(value);
        }
        a.free(self.captured.values);
        a.free(suffix_values.values);
        session.base.releaseInputs(self.cb, self.inputs);
        a.free(self.inputs);
        self.consumed = true;
        return .{ .session = &session.base, .cb = self.cb, .inputs = owned_inputs, .captured = .{ .values = values, .allocator = a }, .identity = final_identity, .epoch = self.epoch };
    }
};
