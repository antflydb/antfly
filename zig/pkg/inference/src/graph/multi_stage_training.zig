// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Ordered detached decisions within one retained differentiable computation.
//! Each forward node executes in its earliest stage; later programs cut all
//! crossing activations into leaves. Candidate and relation selection therefore
//! observe current logits without replaying an encoder, adapter or dropout site.
const std = @import("std");
const ml = @import("ml").graph;
const seeded = @import("seeded_training.zig");
const interpreter = @import("interpreter.zig");
const resident_execution = @import("resident_training_execution.zig");
const ops = @import("../ops/ops.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const Id = ml.NodeId;

pub const max_stages = 8;
pub const Boundary = struct {
    /// Original parameter IDs supplied exactly once to the following stage.
    deferred_parameters: []const Id,
    /// Original output IDs exposed after this stage for detached decisions.
    outputs: []const Id,
};

pub const Stage = struct {
    program: ml.lower.LowerResult,
    analysis: interpreter.CachedAnalysis,
    /// Program IDs and global retained slots are positionally aligned.
    captures: []Id,
    slots: []usize,
    /// Differentiated IDs supplied on entry; empty for the initial stage.
    deferred: []Id,
    /// Global slots exposed after this stage; empty for the final stage.
    exposed: []usize,

    fn deinit(self: *Stage, a: Allocator) void {
        self.analysis.deinit(a);
        self.program.deinit();
        a.free(self.captures);
        a.free(self.slots);
        a.free(self.deferred);
        a.free(self.exposed);
    }
};

pub const Session = struct {
    base: seeded.Session,
    stages: []Stage,
    /// Differentiated forward IDs for every retained activation, stored once.
    captures: []Id,
    tape_bytes: usize,

    pub fn init(a: Allocator, graph: *const ml.Graph, seeds: []const ml.autodiff.Seed, wrt: []const Id, boundaries: []const Boundary, options: seeded.Options) !Session {
        if (boundaries.len == 0 or boundaries.len >= max_stages) return error.InvalidTrainingStage;
        var base = try seeded.Session.initForStages(a, graph, seeds, wrt, options);
        errdefer base.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const forward_count = base.differentiated.forward_node_count;
        const g = &base.differentiated.graph;
        const birth = try scratch.alloc(u8, forward_count);
        @memset(birth, 0);
        const deferred_sets = try scratch.alloc([]Id, boundaries.len);
        for (boundaries, deferred_sets, 0..) |boundary, *set, stage| {
            if (boundary.deferred_parameters.len == 0 or boundary.outputs.len == 0) return error.InvalidTrainingStage;
            set.* = try scratch.alloc(Id, boundary.deferred_parameters.len);
            for (boundary.deferred_parameters, set.*) |original, *mapped| {
                if (original >= base.original_parameters.len or !base.original_parameters[original]) return error.InvalidTrainingStage;
                for (seeds) |seed| if (seed.cotangent == original) return error.InvalidTrainingStage;
                mapped.* = base.differentiated.id_map[original];
                if (mapped.* == ml.null_node or mapped.* >= forward_count) return error.UnusedTrainingBinding;
                if (birth[mapped.*] != 0) return error.DuplicateTrainingBinding;
                birth[mapped.*] = @intCast(stage + 1);
            }
        }
        // Lowering is topological even after PEFT graph rewrites.
        for (0..forward_count) |i| for (g.node(@intCast(i)).getInputs()) |input| {
            if (input != ml.null_node) birth[i] = @max(birth[i], birth[input]);
        };
        const capture_set = try scratch.alloc(bool, forward_count);
        @memset(capture_set, false);
        for (base.captures) |id| capture_set[id] = true;
        for (0..forward_count) |i| for (g.node(@intCast(i)).getInputs()) |input| {
            if (input == ml.null_node or birth[input] == birth[i]) continue;
            const node = g.node(input);
            if (node.op != .parameter and node.op != .constant) capture_set[input] = true;
        };
        const exposed_sets = try scratch.alloc([]Id, boundaries.len);
        for (boundaries, exposed_sets, 0..) |boundary, *set, stage| {
            set.* = try scratch.alloc(Id, boundary.outputs.len);
            for (boundary.outputs, set.*) |original, *mapped| {
                if (original >= base.differentiated.id_map.len) return error.InvalidTrainingStage;
                mapped.* = base.differentiated.id_map[original];
                if (mapped.* == ml.null_node or mapped.* >= forward_count or birth[mapped.*] > stage) return error.InvalidTrainingStage;
                capture_set[mapped.*] = true;
            }
        }
        var targets = std.ArrayListUnmanaged(Id).empty;
        var bytes: usize = 0;
        for (capture_set, 0..) |needed, id| if (needed) {
            bytes = std.math.add(usize, bytes, try seeded.shapeBytes(g.node(@intCast(id)).output_shape)) catch return error.TrainingTapeLimitExceeded;
            if (bytes > options.max_tape_bytes) return error.TrainingTapeLimitExceeded;
            try targets.append(scratch, @intCast(id));
        };
        const captures = try a.dupe(Id, targets.items);
        errdefer a.free(captures);
        const stages = try a.alloc(Stage, boundaries.len + 1);
        errdefer a.free(stages);
        var initialized: usize = 0;
        errdefer for (stages[0..initialized]) |*stage| stage.deinit(a);
        for (stages, 0..) |*stage, number| {
            var cuts = std.ArrayListUnmanaged(Id).empty;
            var outputs = std.ArrayListUnmanaged(Id).empty;
            var positions = std.ArrayListUnmanaged(usize).empty;
            for (captures, 0..) |id, slot| {
                if (birth[id] < number) try cuts.append(scratch, id);
                if (birth[id] == number) {
                    try outputs.append(scratch, id);
                    try positions.append(scratch, slot);
                }
            }
            if (outputs.items.len == 0) return error.InvalidTrainingStage;
            var program = try seeded.cutProgram(a, g, cuts.items, outputs.items);
            errdefer program.deinit();
            var analysis = try interpreter.CachedAnalysis.compute(a, &program.graph);
            errdefer analysis.deinit(a);
            const ids = try a.alloc(Id, outputs.items.len);
            errdefer a.free(ids);
            for (outputs.items, ids) |id, *out| out.* = program.id_map[id];
            const slots = try a.dupe(usize, positions.items);
            errdefer a.free(slots);
            const deferred = try a.dupe(Id, if (number == 0) &.{} else deferred_sets[number - 1]);
            errdefer a.free(deferred);
            const exposed = try a.alloc(usize, if (number < boundaries.len) exposed_sets[number].len else 0);
            errdefer a.free(exposed);
            if (number < boundaries.len) for (exposed_sets[number], exposed) |id, *slot| {
                slot.* = std.mem.indexOfScalar(Id, captures, id) orelse return error.InvalidTrainingStage;
            };
            stage.* = .{ .program = program, .analysis = analysis, .captures = ids, .slots = slots, .deferred = deferred, .exposed = exposed };
            initialized += 1;
        }
        var resident_stages: [max_stages]resident_execution.Spec = undefined;
        for (stages, resident_stages[0..stages.len]) |*stage, *spec| spec.* = .{ .graph = &stage.program.graph, .targets = stage.captures };
        try base.configureResident(resident_stages[0..stages.len], bytes);
        return .{ .base = base, .stages = stages, .captures = captures, .tape_bytes = bytes };
    }

    pub fn deinit(self: *Session) void {
        std.debug.assert(!self.base.active_tape);
        const a = self.base.allocator;
        for (self.stages) |*stage| stage.deinit(a);
        a.free(self.stages);
        a.free(self.captures);
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

    pub fn forward(self: *Session, cb: *const ops.ComputeBackend, inputs: []const interpreter.RuntimeInput, identity: seeded.StepIdentity, control: ?Control) !StageTape {
        if (control) |active| try active.check();
        try self.base.validateBackend(cb);
        if (self.base.active_tape) return error.TrainingTapeStillLive;
        const a = self.base.allocator;
        const values = try a.alloc(?ops.CT, self.captures.len);
        @memset(values, null);
        var tape = StageTape{ .session = self, .cb = cb, .values = values, .identity = identity, .final_identity = identity, .epoch = self.base.parameter_epoch };
        self.base.active_tape = true;
        errdefer tape.deinit();
        for (inputs) |input| {
            const mapped = try self.mapInput(cb, input);
            for (self.stages[1..]) |stage| if (std.mem.indexOfScalar(Id, stage.deferred, mapped.node_id) != null) return error.PrematureTrainingBinding;
            for (tape.inputs.items) |prior| if (prior.node_id == mapped.node_id) return error.DuplicateTrainingBinding;
            const leased = try self.base.leaseInput(cb, mapped);
            tape.inputs.append(a, leased) catch |err| {
                self.base.releaseInputs(cb, &.{leased});
                return err;
            };
        }
        try tape.executeStage(control);
        return tape;
    }
};

pub const StageTape = struct {
    session: *Session,
    cb: *const ops.ComputeBackend,
    inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty,
    values: []?ops.CT,
    identity: seeded.StepIdentity,
    final_identity: seeded.StepIdentity,
    epoch: u64,
    stage: usize = 0,
    consumed: bool = false,

    pub fn deinit(self: *StageTape) void {
        if (self.consumed) return;
        self.consumed = true;
        const a = self.session.base.allocator;
        for (self.values) |value| if (value) |owned| self.cb.free(owned);
        a.free(self.values);
        self.session.base.releaseInputs(self.cb, self.inputs.items);
        self.inputs.deinit(a);
        self.session.base.active_tape = false;
    }

    fn executeStage(self: *StageTape, control: ?Control) !void {
        const a = self.session.base.allocator;
        const stage = &self.session.stages[self.stage];
        var inputs = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        defer inputs.deinit(a);
        for (self.inputs.items) |input| {
            const mapped = stage.program.id_map[input.node_id];
            if (mapped != ml.null_node) try inputs.append(a, .{ .node_id = mapped, .value = input.value });
        }
        for (self.session.captures, self.values) |id, value| if (value) |retained| {
            const mapped = stage.program.id_map[id];
            if (mapped == ml.null_node) continue;
            var duplicate = false;
            for (inputs.items) |input| if (input.node_id == mapped) {
                duplicate = true;
                break;
            };
            if (!duplicate) try inputs.append(a, .{ .node_id = mapped, .value = retained });
        };
        var captured = if (self.session.base.resident) |*compiled|
            try compiled.capture(self.stage, self.cb, inputs.items, control)
        else
            try interpreter.captureNodeValues(a, &stage.program.graph, self.cb, .{ .runtime_inputs = inputs.items, .cached_analysis = stage.analysis, .execution_control = control, .strict_integer_constants = true }, stage.captures);
        errdefer captured.deinit(self.cb);
        if (control) |active| try active.check();
        for (stage.slots) |slot| if (self.values[slot] != null) return error.InvalidTrainingStage;
        for (stage.slots, captured.values) |slot, value| self.values[slot] = value;
        a.free(captured.values);
    }

    pub fn logits(self: *const StageTape, index: usize) !ops.CT {
        if (self.consumed) return error.InvalidTrainingTape;
        const exposed = self.session.stages[self.stage].exposed;
        if (index >= exposed.len) return error.InvalidTrainingStage;
        return self.values[exposed[index]] orelse error.InvalidTrainingTape;
    }

    /// Advance once. Any error consumes the complete tape and releases its
    /// epoch lock, including malformed, stale, cancelled or duplicate inputs.
    pub fn advance(self: *StageTape, identity: seeded.StepIdentity, decision_fingerprint: [32]u8, deferred_inputs: []const interpreter.RuntimeInput, control: ?Control) !void {
        if (self.consumed) return error.InvalidTrainingTape;
        errdefer self.deinit();
        if (!std.meta.eql(identity, self.identity) or self.epoch != self.session.base.parameter_epoch) return error.TrainingTapeIdentityMismatch;
        if (self.stage + 1 >= self.session.stages.len) return error.InvalidTrainingStage;
        if (control) |active| try active.check();
        const next = &self.session.stages[self.stage + 1];
        if (deferred_inputs.len != next.deferred.len) return error.MissingTrainingBinding;
        for (deferred_inputs) |input| {
            const mapped = try self.session.mapInput(self.cb, input);
            if (std.mem.indexOfScalar(Id, next.deferred, mapped.node_id) == null) return error.InvalidTrainingStage;
            for (self.inputs.items) |prior| if (prior.node_id == mapped.node_id) return error.DuplicateTrainingBinding;
            const leased = try self.session.base.leaseInput(self.cb, mapped);
            self.inputs.append(self.session.base.allocator, leased) catch |err| {
                self.session.base.releaseInputs(self.cb, &.{leased});
                return err;
            };
        }
        self.stage += 1;
        try self.executeStage(control);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.multi-stage-training.binding.v1");
        hash.update(&self.final_identity.binding);
        hash.update(&.{@as(u8, @intCast(self.stage))});
        hash.update(&decision_fingerprint);
        hash.final(&self.final_identity.binding);
    }

    /// Transfer the final backward captures to the ordinary seeded tape. Input
    /// tensors remain caller-owned until that tape has been consumed.
    pub fn finish(self: *StageTape) !seeded.Tape {
        if (self.consumed) return error.InvalidTrainingTape;
        errdefer self.deinit();
        if (self.stage + 1 != self.session.stages.len) return error.IncompleteTrainingStages;
        if (self.epoch != self.session.base.parameter_epoch) return error.TrainingTapeIdentityMismatch;
        const a = self.session.base.allocator;
        const values = try a.alloc(ops.CT, self.session.base.captures.len);
        errdefer a.free(values);
        for (self.session.base.captures, values) |id, *out| {
            const slot = std.mem.indexOfScalar(Id, self.session.captures, id) orelse return error.InvalidTrainingStage;
            out.* = self.values[slot] orelse return error.InvalidTrainingTape;
        }
        const inputs = try self.inputs.toOwnedSlice(a);
        // Everything below is infallible: transfer retained CTs once, free
        // decision-only captures, and leave the base epoch locked for backward.
        for (self.session.captures, self.values) |id, value| if (std.mem.indexOfScalar(Id, self.session.base.captures, id) == null) {
            if (value) |owned| self.cb.free(owned);
        };
        a.free(self.values);
        self.consumed = true;
        return .{ .session = &self.session.base, .cb = self.cb, .inputs = inputs, .captured = .{ .values = values, .allocator = a }, .identity = self.final_identity, .epoch = self.epoch };
    }
};
