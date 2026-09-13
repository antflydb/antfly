// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded GLiNER2.5 training steps over one immutable differentiable graph.
//! Encoder/proposal activations survive detached candidate selection, and the
//! candidate scores survive detached relation selection. The managed caller
//! retains every parameter lease until backward completes; this module neither
//! updates parameters nor owns an optimizer/checkpoint copy.
const std = @import("std");
const ml = @import("ml").graph;
const model = @import("../models/gliner_boundary.zig");
const ops = @import("../ops/ops.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const schema_mod = @import("../pipelines/extraction_schema.zig");
const relation_proposals = @import("../pipelines/gliner_boundary_relations.zig");
const boundary_ops = @import("../architectures/gliner_boundary_ops.zig");
const graph_mod = @import("../architectures/gliner_boundary_graph.zig");
const candidate_graph = @import("../architectures/gliner_boundary_graph_candidates.zig");
const task_graph = @import("../architectures/gliner_boundary_graph_tasks.zig");
const encoder_graph = @import("gliner_boundary_encoder_graph.zig");
const peft_graph = @import("gliner_boundary_peft_graph.zig");
const recomputed_graph = @import("gliner_boundary_recomputed_graph.zig");
const recomputed = @import("../graph/recomputed_training.zig");
const replay_bindings = @import("gliner_boundary_replay_bindings.zig");
const recomputed_admission = @import("gliner_boundary_recomputed_admission.zig");
const recomputed_execution = @import("gliner_boundary_recomputed_execution.zig");
const targets_mod = @import("gliner_boundary_targets.zig");
const selection = @import("gliner_boundary_selection.zig");
const decisions = @import("gliner_boundary_train_decisions.zig");
const objectives = @import("gliner_boundary_train_objectives.zig");
const primitive = @import("gliner_boundary_losses.zig");
const matching = @import("gliner_boundary_matching.zig");
const record_loss = @import("gliner_boundary_record_loss.zig");
const seeded = @import("../graph/seeded_training.zig");
const staged = @import("../graph/multi_stage_training.zig");
const interpreter = @import("../graph/interpreter.zig");
const transfer = @import("gliner_boundary_training_transfer.zig");
const decision_events = @import("gliner_boundary_training_decisions.zig");
const BoundedAllocator = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const Id = ml.NodeId;
const Shape = ml.Shape;
const nil = ml.null_node;

pub const Mode = graph_mod.Mode;
pub const AttentionProfile = encoder_graph.AttentionProfile;
pub const ActivationProfile = encoder_graph.ActivationProfile;
pub const Capacities = struct {
    /// Explicit overrides must agree with the serialized training head. No
    /// capacity shrinks or silent annotation truncation happen per microbatch.
    pool: ?u32 = null,
    gold_per_query: ?u32 = null,
};
pub const Limits = struct {
    // This includes model/source and optimizer owners, beyond the generic
    // regional executor's isolated host envelope. Trainer clamps it again to
    // the explicitly configured parent reservation before building a plan.
    recomputation: recomputed.Limits = .{ .max_host_bytes = 6 * 1024 * 1024 * 1024 },
    replay: replay_bindings.Limits = .{},
    encoder: encoder_graph.Limits = .{},
    graph: graph_mod.Limits = .{},
    targets: targets_mod.Options = .{},
    selection: selection.Limits = .{},
    losses: primitive.Limits = .{},
    matching: matching.Options = .{},
    record_loss: record_loss.Options = .{},
    relation: relation_proposals.Options = .{},
    max_outputs: usize = 8192,
    max_dropout_sites: usize = 16384,
    max_record_groups: usize = 4096,
    max_relation_pairs: usize = 65536,
    /// Reclaiming host allocation ceiling for targets, decisions, logit copies
    /// and cotangent assembly. Backend/tape/optimizer admission is additional.
    max_step_host_bytes: usize = 512 * 1024 * 1024,
    max_step_work: usize = 500 * 1024 * 1024,
    transfers: transfer.Limits = .{},
    /// Aggregate graph, lowering/cut-program copies, retained host tape
    /// metadata, and transient host steps. The stable owner outlives every
    /// returned StepResult; backend buffers require caller admission too.
    max_total_host_bytes: usize = 2 * 1024 * 1024 * 1024,
};
pub const DropoutMask = struct { name: []const u8, values: []const f32 };
pub const DropoutDescriptor = struct { node: Id, name: []const u8, shape: Shape, probability: f32 };
pub const StepContext = struct {
    identity: seeded.StepIdentity,
    replay: encoder_graph.Replay,
    progress: objectives.Progress,
    /// Mirrors explicit BoundaryHead.set_* controls for low-level reference
    /// runs. Ordinary training leaves this null and uses the pinned schedules.
    /// An enclosing durable run contract must include any explicit override.
    scales_override: ?objectives.Scales = null,
    /// Optional complete replacement for every required dropout site. Values
    /// are borrowed until run returns, must be exactly 0 or 1/(1-p), and are
    /// bound into the retained step identity. Null uses native Replay masks.
    dropout_masks: ?[]const DropoutMask = null,
    weights: objectives.Weights = .{},
    injection_draws: ?[]const f32 = null,
    negative_query_draws: ?[]const f32 = null,
    /// The pinned relation generator caps/thresholds before labelling and does
    /// not inject gold pairs. Coverage is always reported; strict jobs can
    /// reject any missing positive without changing the proposal distribution.
    require_gold_relation_coverage: bool = false,
    /// Optional borrowed diagnostics; all decisions and replay seals retain
    /// their ordinary semantics. Observer errors abort and release the tape.
    decision_observer: ?decision_events.Observer = null,
};
pub const GradientPresence = enum { computed, computed_zero, absent };
pub const Presence = struct { parameter: Id, kind: GradientPresence };
pub const Coverage = struct { gold_mentions: usize = 0, proposed_gold_mentions: usize = 0, gold_relations: usize = 0, proposed_gold_relations: usize = 0, matched_records: usize = 0 };
pub const StepResult = struct {
    allocator: Allocator,
    terms: objectives.Terms,
    backward: seeded.BackwardResult,
    presence: []Presence,
    plan_fingerprint: [32]u8,
    target_fingerprint: [32]u8,
    decision_fingerprint: [32]u8,
    dropout_fingerprint: ?[32]u8,
    coverage: Coverage,
    host_peak_bytes: usize,
    /// Request-local explicit device cuts. Native execution reports zero.
    transfers: transfer.Diagnostics = .{},
    pub fn deinit(self: *StepResult, cb: *const ops.ComputeBackend) void {
        self.backward.deinit(cb);
        self.allocator.free(self.presence);
        self.* = undefined;
    }
};
const Kind = enum { start, end, inside, pair, proposal, abstention, count, classification, record_object, record_assignment, relation };
const Output = struct { kind: Kind, node: Id, group: usize = 0 };
const RecordGroup = struct {
    sample: usize,
    structure: usize,
    group: usize,
    mode: schema_mod.RecordMode,
    fields: []matching.Field,
    anchor_query: ?usize,
    instances: usize,
};
const RelationRoute = struct { sample: usize, local: usize, schema_index: usize, head_query: usize, tail_query: usize, allow_self: bool };
const Session = union(enum) {
    direct: seeded.Session,
    stages: staged.Session,
    fn base(self: *Session) *seeded.Session {
        return switch (self.*) {
            .direct => |*value| value,
            .stages => |*value| &value.base,
        };
    }
    fn deinit(self: *Session) void {
        switch (self.*) {
            inline else => |*value| value.deinit(),
        }
    }
};
const Recomputed = struct {
    graph: recomputed_graph.Built,
    recipes: replay_bindings.Recipes,
    head_admission: ?recomputed.EnclosingAdmission = null,

    fn deinit(self: *Recomputed) void {
        self.graph.deinit();
        self.recipes.deinit();
        self.* = undefined;
    }
};

/// The observer belongs to the allocator owner, never to a movable Plan or a
/// previous request. Only terminal allocation failures classify an OOM;
/// resize/remap misses may recover and leave budget.denied set indefinitely.
const HostOwner = struct {
    budget: BoundedAllocator,
    failure: ?BoundedAllocator.AllocationFailure = null,

    fn init(self: *@This(), backing: Allocator, limit: usize) void {
        self.* = .{ .budget = .{
            .backing = backing,
            .limit = limit,
            .failure_context = self,
            .allocation_failed = failed,
        } };
    }

    fn failed(raw: ?*anyopaque, failure: BoundedAllocator.AllocationFailure) void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.failure = failure;
    }

    fn resetFailure(self: *@This()) void {
        self.failure = null;
    }

    fn translate(self: *const @This(), err: anyerror) anyerror {
        if (err == error.OutOfMemory) if (self.failure) |failure| {
            if (failure.kind == .declared_limit) return error.BoundaryTrainingStepLimitExceeded;
        };
        return err;
    }
};

pub const Plan = struct {
    backing: Allocator,
    host_owner: *HostOwner,
    /// Stable alias retained for the enclosing trainer's live admission.
    budget: *BoundedAllocator,
    allocator: Allocator,
    graph: *ml.Graph,
    config: model.Config,
    mode: Mode,
    limits: Limits,
    pool_capacity: u32,
    gold_capacity: u32,
    relation_capacity: u32,
    encoder: encoder_graph.Built,
    bindings: []graph_mod.Binding,
    pool_input: ?candidate_graph.PoolInput,
    relation_input: ?task_graph.RelationInput,
    records: []RecordGroup,
    relations: []RelationRoute,
    outputs: []Output,
    /// Five proposal views in this stable order: start, end, inside,
    /// projected pool starts and projected pool ends.
    proposal_views: []Id,
    schema_fingerprints: [][32]u8,
    fingerprint: [32]u8,
    peft: ?peft_graph.Result = null,
    session: ?Session = null,
    recomputation: ?Recomputed = null,
    selected_parameters: []Id = &.{},
    construction_failed: bool = false,

    pub fn deinit(self: *Plan) void {
        if (self.session) |*session| session.deinit();
        if (self.recomputation) |*regional| regional.deinit();
        if (self.peft) |*peft| peft.deinit();
        self.allocator.free(self.selected_parameters);
        for (self.records) |record| self.allocator.free(record.fields);
        self.allocator.free(self.records);
        self.allocator.free(self.relations);
        for (self.bindings) |binding| self.allocator.free(binding.name);
        self.allocator.free(self.bindings);
        self.allocator.free(self.outputs);
        self.allocator.free(self.proposal_views);
        self.allocator.free(self.schema_fingerprints);
        self.encoder.deinit();
        self.graph.deinit();
        self.allocator.destroy(self.graph);
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self.host_owner);
        self.* = undefined;
    }

    /// Rewrite live linear calls before finalization. Adapter parameter and
    /// per-call dropout metadata remain owned by this Plan. Initial adapter
    /// values and the frozen base continue to belong to the managed trainer.
    pub fn applyPeft(self: *Plan, config: peft_graph.Config, limits: peft_graph.Limits) !void {
        self.host_owner.resetFailure();
        return self.applyPeftOwned(config, limits) catch |err| return self.host_owner.translate(err);
    }

    fn applyPeftOwned(self: *Plan, config: peft_graph.Config, limits: peft_graph.Limits) !void {
        if (self.construction_failed) return error.BoundaryTrainingPlanInvalidated;
        if (self.session != null or self.peft != null) return error.BoundaryTrainingPlanAlreadyFinalized;
        if ((config.mode == .train) != (self.mode == .training)) return error.InvalidBoundaryTrainingStepOptions;
        // A construction failure can leave remapped descriptors or appended
        // graph parameters. The owner must discard the Plan before retrying.
        errdefer self.construction_failed = true;
        const config_bytes = try std.json.Stringify.valueAlloc(self.allocator, config, .{});
        defer self.allocator.free(config_bytes);
        var rewritten = try peft_graph.inject(self.allocator, self.graph, config, limits);
        errdefer rewritten.deinit();
        // Any remap error invalidates construction; no forward has occurred.
        try self.encoder.remap(rewritten.rewrites);
        for (self.bindings) |*binding| binding.node = try rewritten.remap(binding.node);
        for (self.outputs) |*output| output.node = try rewritten.remap(output.node);
        for (self.proposal_views) |*node| node.* = try rewritten.remap(node.*);
        if (self.pool_input) |*pool| inline for (std.meta.fields(candidate_graph.PoolInput)) |field| {
            if (field.type == Id and !std.mem.eql(u8, field.name, "capacity")) @field(pool, field.name) = try rewritten.remap(@field(pool, field.name));
            if (field.type == ?Id) if (@field(pool, field.name)) |node| {
                @field(pool, field.name) = try rewritten.remap(node);
            };
        };
        if (self.relation_input) |*relation| {
            inline for (std.meta.fields(task_graph.RelationInput)) |field| {
                if (field.type == Id and !std.mem.eql(u8, field.name, "pairs") and !std.mem.eql(u8, field.name, "relations")) @field(relation, field.name) = try rewritten.remap(@field(relation, field.name));
            }
            for (&relation.text_indices) |*node| node.* = try rewritten.remap(node.*);
        }
        self.graph.deinit();
        self.graph.* = rewritten.graph;
        rewritten.graph = ml.Graph.init(self.allocator);
        self.peft = rewritten;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.gliner25.training.peft-plan.v1");
        hash.update(&self.fingerprint);
        hash.update(config_bytes);
        hash.final(&self.fingerprint);
    }

    /// `wrt` names the caller's selected parameter slots. Disconnected task
    /// parameters may remain absent; strict VJP coverage applies to every
    /// differentiable path that actually reaches a requested parameter.
    pub fn finalize(self: *Plan, wrt: []const Id, options: seeded.Options) !void {
        return self.finalizeWithActivationProfile(wrt, options, .retained_v1);
    }

    /// Regional execution also requires sealRecomputedAdmission, after the
    /// enclosing model and optimizer owners have supplied their reservations.
    pub fn finalizeWithActivationProfile(self: *Plan, wrt: []const Id, options: seeded.Options, profile: ActivationProfile) !void {
        self.host_owner.resetFailure();
        return self.finalizeOwned(wrt, options, profile) catch |err| return self.host_owner.translate(err);
    }

    fn finalizeOwned(self: *Plan, wrt: []const Id, options: seeded.Options, profile: ActivationProfile) !void {
        if (self.construction_failed) return error.BoundaryTrainingPlanInvalidated;
        if (self.session != null) return error.BoundaryTrainingPlanAlreadyFinalized;
        for (wrt, 0..) |node, i| {
            if (node >= self.graph.nodes.items.len or self.graph.node(node).op != .parameter or self.graph.node(node).output_shape.dtype != .f32 or
                std.mem.startsWith(u8, self.graph.parameterName(self.graph.node(node)), "__")) return error.InvalidBoundaryTrainingParameter;
            if (std.mem.indexOfScalar(Id, wrt[0..i], node) != null) return error.DuplicateTrainingBinding;
        }
        errdefer self.construction_failed = true;
        if (self.encoder.activation_profile != profile) {
            if (profile != .layer_recompute_v1) return error.InvalidBoundaryTrainingStepOptions;
            self.encoder.admission = try encoder_graph.planWithProfiles(&self.config, self.encoder.layout, self.encoder.mode, self.encoder.attention_profile, profile, self.encoder.limits);
            self.encoder.activation_profile = profile;
        }
        const selected = try self.allocator.dupe(Id, wrt);
        errdefer self.allocator.free(selected);
        var pool_parameters = std.ArrayListUnmanaged(Id).empty;
        defer pool_parameters.deinit(self.allocator);
        var relation_parameters = std.ArrayListUnmanaged(Id).empty;
        defer relation_parameters.deinit(self.allocator);
        const used = try reachable(self.allocator, self.graph, self.outputs);
        defer self.allocator.free(used);
        for (self.bindings) |binding| {
            if (!used[binding.node]) continue;
            if (std.mem.startsWith(u8, binding.name, "__gliner25.pool.")) try pool_parameters.append(self.allocator, binding.node);
            if (std.mem.startsWith(u8, binding.name, "__gliner25.relations.")) try relation_parameters.append(self.allocator, binding.node);
        }
        if (profile == .layer_recompute_v1) {
            // Cut semantic regions before differentiating the head. A retained
            // whole-encoder Session would already defeat this memory profile.
            var recipes = try replay_bindings.Recipes.init(self.allocator, self.graph, &.{self.encoder.regions.output}, self.encoder.dropouts, if (self.peft) |peft| peft.uses else &.{}, .{ .limits = self.limits.replay });
            errdefer recipes.deinit();
            const outputs = try self.allocator.alloc(Id, self.outputs.len);
            defer self.allocator.free(outputs);
            for (self.outputs, outputs) |output, *id| id.* = output.node;
            var pair_view: [1]Id = undefined;
            var boundaries: [2]staged.Boundary = undefined;
            var count: usize = 0;
            if (self.pool_input != null) {
                boundaries[0] = .{ .outputs = self.proposal_views, .deferred_parameters = pool_parameters.items };
                count = 1;
                if (self.relation_input != null) {
                    pair_view = .{self.outputs[try self.outputIndex(.pair, 0)].node};
                    boundaries[1] = .{ .outputs = &pair_view, .deferred_parameters = relation_parameters.items };
                    count = 2;
                }
            }
            var regions = try recomputed_graph.build(self.allocator, self.graph, self.encoder.regions, wrt, recipes.replay_inputs, .{ .outputs = outputs, .boundaries = boundaries[0..count] }, .{ .session = options, .limits = self.limits.recomputation });
            errdefer regions.deinit();
            for (recipes.entries) |entry| if (regions.headId(entry.node) != nil) return error.UndeclaredRecomputeDependency;
            var head_options = options;
            head_options.resident.max_compile_bytes = @min(head_options.resident.max_compile_bytes, regions.regional.remainingCompileBytes());
            const session = if (count == 0)
                Session{ .direct = try seeded.Session.init(self.allocator, &regions.head.graph, regions.seeds, regions.parameters, head_options) }
            else
                Session{ .stages = try staged.Session.init(self.allocator, &regions.head.graph, regions.seeds, regions.parameters, regions.boundaries, head_options) };
            self.recomputation = .{ .graph = regions, .recipes = recipes };
            self.session = session;
            self.selected_parameters = selected;
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update("antfly.gliner25.training.activation-profile.v1\x00");
            hash.update(&self.fingerprint);
            hash.update(@tagName(profile));
            hash.update(&recipes.fingerprint);
            hash.final(&self.fingerprint);
            return;
        }
        var builder = ml.Builder.init(self.graph);
        const seeds = try self.allocator.alloc(ml.autodiff.Seed, self.outputs.len);
        defer self.allocator.free(seeds);
        for (self.outputs, seeds, 0..) |output, *seed, index| {
            var name: [96]u8 = undefined;
            seed.* = .{ .output = output.node, .cotangent = try builder.parameter(try std.fmt.bufPrint(&name, "__gliner25.seed.{s}.{d}", .{ @tagName(output.kind), index }), self.graph.node(output.node).output_shape) };
        }
        const session = if (self.pool_input == null)
            Session{ .direct = try seeded.Session.init(self.allocator, self.graph, seeds, wrt, options) }
        else blk: {
            const pair_view = [_]Id{self.outputs[try self.outputIndex(.pair, 0)].node};
            var boundaries: [2]staged.Boundary = undefined;
            boundaries[0] = .{ .outputs = self.proposal_views, .deferred_parameters = pool_parameters.items };
            if (self.relation_input != null) boundaries[1] = .{ .outputs = &pair_view, .deferred_parameters = relation_parameters.items };
            break :blk Session{ .stages = try staged.Session.init(self.allocator, self.graph, seeds, wrt, boundaries[0..if (self.relation_input != null) @as(usize, 2) else 1], options) };
        };
        self.session = session;
        self.selected_parameters = selected;
    }

    pub fn advanceParameterEpoch(self: *Plan) !void {
        if (self.construction_failed) return error.BoundaryTrainingPlanInvalidated;
        const session = if (self.session) |*value| value else return error.BoundaryTrainingPlanNotFinalized;
        const base = session.base();
        if (self.recomputation) |*regional| {
            if (base.active_tape) return error.TrainingTapeStillLive;
            if (base.parameter_epoch != regional.graph.regional.parameter_epoch) return error.TrainingTapeIdentityMismatch;
            const next = std.math.add(u64, base.parameter_epoch, 1) catch return error.TrainingEpochOverflow;
            // All child checks precede the infallible head publication.
            try regional.graph.regional.advanceParameterEpoch();
            base.parameter_epoch = next;
        } else try base.advanceParameterEpoch();
    }

    /// Includes every direct/staged head program and the final canonical
    /// gradient merge. Managed weights, batch scratch and optimizer staging
    /// belong to the enclosing owner supplied to sealRecomputedAdmission.
    pub fn recomputedHeadAdmission(self: *Plan) !recomputed.EnclosingAdmission {
        self.host_owner.resetFailure();
        return self.recomputedHeadAdmissionOwned() catch |err| return self.host_owner.translate(err);
    }

    fn recomputedHeadAdmissionOwned(self: *Plan) !recomputed.EnclosingAdmission {
        const regional = if (self.recomputation) |*value| value else return error.InvalidBoundaryTrainingStepOptions;
        if (regional.head_admission) |value| return value;
        const session = if (self.session) |*value| value else return error.BoundaryTrainingPlanNotFinalized;
        const base = session.base();
        const stages: ?*const staged.Session = switch (session.*) {
            .direct => null,
            .stages => |*value| value,
        };
        var head = (try recomputed_admission.headAdmission(self.allocator, base, stages, regional.graph.regional.remainingCompileBytes())).enclosing;
        // Shared parameters retain both input results while independently
        // owned canonical outputs are assembled; the merge cost is additional.
        const merging = try recomputed_execution.admission(self.allocator, &regional.graph, base.options.execution, base.options.resident.program.instruction.primitive, null);
        head.head_local_bytes = try std.math.add(usize, head.head_local_bytes, merging.backend_upper_bound_bytes);
        head.head_host_metadata_bytes = try std.math.add(usize, head.head_host_metadata_bytes, merging.host_upper_bound_bytes);
        head.head_work = try std.math.add(u64, head.head_work, merging.total_work);
        const transfers = try self.transferAdmission();
        head.head_local_bytes = try std.math.add(usize, head.head_local_bytes, transfers.largest_upload_bytes);
        regional.head_admission = head;
        return head;
    }

    pub fn sealRecomputedAdmission(self: *Plan, owner: recomputed.EnclosingAdmission) !recomputed.Admission {
        var enclosing = try self.recomputedHeadAdmission();
        inline for (std.meta.fields(recomputed.EnclosingAdmission)) |field| {
            @field(enclosing, field.name) = try std.math.add(field.type, @field(enclosing, field.name), @field(owner, field.name));
        }
        return self.recomputation.?.graph.regional.sealAdmission(enclosing);
    }

    /// Caller owns the descriptor slice; names borrow this finalized Plan.
    /// Only reachable sites are returned, in stable encoder/head/PEFT order.
    pub fn dropoutDescriptors(self: *Plan, a: Allocator) ![]DropoutDescriptor {
        if (self.construction_failed) return error.BoundaryTrainingPlanInvalidated;
        if (self.session == null) return error.BoundaryTrainingPlanNotFinalized;
        var result = std.ArrayListUnmanaged(DropoutDescriptor).empty;
        errdefer result.deinit(a);
        for (self.encoder.dropouts) |descriptor| if (required(self, descriptor.node)) {
            if (result.items.len >= self.limits.max_dropout_sites) return error.BoundaryTrainingStepLimitExceeded;
            try result.append(a, .{ .node = descriptor.node, .name = self.graph.parameterName(self.graph.node(descriptor.node)), .shape = descriptor.shape, .probability = descriptor.probability });
        };
        for (self.bindings) |binding| if (binding.kind == .inverted_dropout and required(self, binding.node)) {
            if (result.items.len >= self.limits.max_dropout_sites) return error.BoundaryTrainingStepLimitExceeded;
            try result.append(a, .{ .node = binding.node, .name = binding.name, .shape = binding.shape, .probability = binding.probability });
        };
        if (self.peft) |peft| for (peft.uses) |use| if (use.mask != nil and required(self, use.mask)) {
            if (result.items.len >= self.limits.max_dropout_sites) return error.BoundaryTrainingStepLimitExceeded;
            try result.append(a, .{ .node = use.mask, .name = use.mask_name.?, .shape = use.mask_shape, .probability = use.probability });
        };
        return result.toOwnedSlice(a);
    }
    pub fn run(self: *Plan, cb: *const ops.ComputeBackend, parameters: []const interpreter.RuntimeInput, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, annotations: []const targets_mod.Annotations, context: StepContext, control: ?Control) !StepResult {
        if (self.construction_failed) return error.BoundaryTrainingPlanInvalidated;
        if (self.session == null) return error.BoundaryTrainingPlanNotFinalized;
        if (self.recomputation) |*regional| {
            if (regional.graph.regional.admission == null) return error.RecomputeAdmissionNotSealed;
            if (regional.graph.regional.parameter_epoch != self.session.?.base().parameter_epoch) return error.TrainingTapeIdentityMismatch;
        }
        // Every backend guard and host loop borrows this same composition.
        // runStep joins/releases all tapes and guards before this frame ends;
        // neither the cached Plan nor StepResult retains its callback context.
        var checks = CombinedControl{ .original = cb.execution_control, .request = control };
        const active = checks.value();
        try active.check();
        try self.session.?.base().validateBackend(cb);
        const transfer_admission = try self.transferAdmission();
        if (self.session.?.base().options.execution == .resident_metal and cb.vtable.glinerBoundaryDownload == null) return error.UnsupportedSeededTrainingBackend;
        var controlled = cb.*;
        controlled.execution_control = active;
        self.host_owner.resetFailure();
        var owner: HostOwner = undefined;
        owner.init(self.allocator, self.limits.max_step_host_bytes);
        defer std.debug.assert(owner.budget.live == 0);
        var result = runStep(self, owner.budget.allocator(), &controlled, parameters, prepared, schemas, annotations, context, active, transfer_admission) catch |err| {
            // The request may fit its local cap but exhaust the aggregate
            // Plan owner. A backing allocator denial in either owner remains
            // OutOfMemory, independent of earlier recoverable resize misses.
            return self.host_owner.translate(owner.translate(err));
        };
        result.host_peak_bytes = owner.budget.peak;
        return result;
    }
    /// Validate every allowed device transfer before allocating step inputs.
    /// Parameter uploads remain the enclosing immutable model owner's cost.
    pub fn instructionControlReadbackUpperBound(self: *Plan) !usize {
        if (self.session == null) return error.BoundaryTrainingPlanNotFinalized;
        const admission = self.session.?.base().executionAdmission() orelse return 0;
        return std.math.add(usize, admission.instruction_control_readback_upper_bound_bytes, if (self.recomputation) |*regional| regional.graph.regional.regional_admission.instruction_control_readback_upper_bound_bytes else 0);
    }

    pub fn transferAdmission(self: *Plan) !transfer.Admission {
        if (self.session == null) return error.BoundaryTrainingPlanNotFinalized;
        const base = self.session.?.base();
        var result = transfer.Admission{};
        if (base.options.execution == .native) return result;
        for (self.graph.parameters.items) |id| {
            const name = self.graph.parameterName(self.graph.node(id));
            if (!std.mem.startsWith(u8, name, "__") or std.mem.startsWith(u8, name, "__gliner25.seed.") or !required(self, id) or replayInput(self, id)) continue;
            try result.upload(self.graph.node(id).output_shape);
        }
        if (self.recomputation) |*regional| {
            const replay = try regional.graph.regional.replaySummary();
            result.upper.upload_bytes = try std.math.add(usize, result.upper.upload_bytes, try std.math.add(usize, replay.initial_upload_bytes, replay.replay_upload_bytes));
            result.largest_upload_bytes = @max(result.largest_upload_bytes, replay.largest_input_bytes);
            try result.readback(.finite_control, regional.graph.regional.regional_admission.control_readback_upper_bound_bytes);
        }
        for (self.outputs) |output| {
            const shape = self.graph.node(output.node).output_shape;
            try result.upload(shape);
            try result.readback(.loss_logits, try transfer.shapeBytes(shape));
        }
        if (self.pool_input != null) for (self.proposal_views, 0..) |id, i| {
            try result.readback(if (i < 3) .proposal_logits else .proposal_features, try transfer.shapeBytes(self.graph.node(id).output_shape));
        };
        if (self.relation_input != null) {
            const pair = self.outputs[try self.outputIndex(.pair, 0)].node;
            try result.readback(.proposal_logits, try transfer.shapeBytes(self.graph.node(pair).output_shape));
        }
        const execution = base.executionAdmission().?;
        try result.readback(.finite_control, execution.finite_check_readback_bytes);
        try result.readback(.finite_control, execution.instruction_control_readback_upper_bound_bytes);
        result.device_upper_bound_bytes = if (self.recomputation) |*regional|
            if (regional.graph.regional.admission) |aggregate| aggregate.backend_upper_bound_bytes else 0
        else
            std.math.add(usize, execution.device_upper_bound_bytes, result.largest_upload_bytes) catch return error.BoundaryTrainingTransferLimitExceeded;
        if (result.device_upper_bound_bytes > base.options.resident.max_device_bytes) return error.BoundaryTrainingTransferLimitExceeded;
        try result.validate(self.limits.transfers);
        return result;
    }
    fn outputIndex(self: *const Plan, kind: Kind, group: usize) !usize {
        for (self.outputs, 0..) |output, i| if (output.kind == kind and output.group == group) return i;
        return error.MissingBoundaryTrainingOutput;
    }
};

fn reachable(a: Allocator, graph: *const ml.Graph, outputs: []const Output) ![]bool {
    const used = try a.alloc(bool, graph.nodes.items.len);
    errdefer a.free(used);
    @memset(used, false);
    var queue = std.ArrayListUnmanaged(Id).empty;
    defer queue.deinit(a);
    for (outputs) |output| try queue.append(a, output.node);
    while (queue.pop()) |node| {
        if (node >= used.len) return error.InvalidBoundaryTrainingGraphShape;
        if (used[node]) continue;
        used[node] = true;
        for (graph.node(node).getInputs()) |input| if (input != nil) try queue.append(a, input);
    }
    return used;
}

fn product(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.BoundaryTrainingStepLimitExceeded;
}
fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}

const CombinedControl = struct {
    original: ?Control,
    request: ?Control,

    fn apply(raw: ?*anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try check(self.original);
        try check(self.request);
    }

    fn value(self: *@This()) Control {
        var result = self.request orelse self.original orelse Control{};
        if (self.original) |original| {
            if (result.io == null) result.io = original.io;
            if (result.hard_cancellation == null) result.hard_cancellation = original.hard_cancellation;
            if (result.progress == null) result.progress = original.progress;
            if (original.deadline_ns) |deadline| result.deadline_ns = if (result.deadline_ns) |other| @min(deadline, other) else deadline;
        }
        result.ptr = self;
        result.check_fn = apply;
        return result;
    }
};

fn appendHash(hash: *std.crypto.hash.sha2.Sha256, number: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, number, .little);
    hash.update(&bytes);
}
fn route(sample: processor.Sample, kind: processor.QueryKind, index: usize, field: ?usize) !usize {
    var found: ?usize = null;
    for (sample.queries, 0..) |query, q| if (query.kind == kind and query.schema_index == index and (field == null or query.label_index == field.?)) {
        if (found != null) return error.InvalidBoundaryTrainingRouting;
        found = q;
    };
    return found orelse error.InvalidBoundaryTrainingRouting;
}
fn indices(g: *graph_mod.GraphBuilder, values: []const usize, bound: usize) !Id {
    const bytes = try product(values.len, @sizeOf(i32));
    if (bytes > g.limits.max_constant_bytes -| g.builder.graph.constant_pool.items.len) return error.BoundaryTrainingStepLimitExceeded;
    const typed = try g.allocator.alloc(i32, values.len);
    defer g.allocator.free(typed);
    for (values, typed) |value, *out| {
        if (value >= bound) return error.InvalidBoundaryTrainingRouting;
        out.* = std.math.cast(i32, value) orelse return error.BoundaryTrainingStepLimitExceeded;
    }
    return g.builder.tensorConstBytes(std.mem.sliceAsBytes(typed), Shape.init(.i32, &.{@intCast(typed.len)}));
}
fn consecutive(g: *graph_mod.GraphBuilder, start: usize, count: usize, bound: usize) !Id {
    const values = try g.allocator.alloc(usize, count);
    defer g.allocator.free(values);
    for (values, 0..) |*value, i| value.* = start + i;
    return indices(g, values, bound);
}
fn outputAppend(a: Allocator, list: *std.ArrayListUnmanaged(Output), limits: Limits, kind: Kind, node: Id, group: usize) !void {
    if (list.items.len >= limits.max_outputs) return error.BoundaryTrainingStepLimitExceeded;
    try list.append(a, .{ .kind = kind, .node = node, .group = group });
}
fn touchWeights(g: *graph_mod.GraphBuilder) !void {
    const h = g.config.encoder.hidden_size;
    const d = g.config.head.record_dim;
    if (g.config.head.enable_records) {
        _ = try g.weight("record_decoder.instance_embed", &.{ g.config.head.record_instance_queries, h });
        _ = try g.weight("record_decoder.null_embed", &.{d});
        inline for (.{ "q_proj", "k_proj", "v_proj", "object_head", "latent_seed_head", "inst_proj", "field_proj", "cand_proj" }) |name| {
            const out = if (comptime std.mem.eql(u8, name, "v_proj")) h else if (comptime std.mem.eql(u8, name, "object_head") or std.mem.eql(u8, name, "latent_seed_head")) 1 else d;
            _ = try g.weight("record_decoder." ++ name ++ ".weight", &.{ out, h });
            _ = try g.weight("record_decoder." ++ name ++ ".bias", &.{out});
        }
    }
    if (g.config.head.enable_relations) {
        const query: u32 = if (g.config.head.directional_relation_states) 2 * h else h;
        _ = try g.weight("relation_scorer.mlp.0.weight", &.{ h, 4 * h + query + 2 });
        _ = try g.weight("relation_scorer.mlp.0.bias", &.{h});
        _ = try g.weight("relation_scorer.mlp.3.weight", &.{ 1, h });
        _ = try g.weight("relation_scorer.mlp.3.bias", &.{1});
        if (g.config.head.relation_biaffine_content) {
            inline for (.{ "head_content_projection", "tail_content_projection" }) |name| {
                _ = try g.weight("relation_scorer." ++ name ++ ".weight", &.{ h, h });
                _ = try g.weight("relation_scorer." ++ name ++ ".bias", &.{h});
            }
            _ = try g.weight("relation_scorer.relation_content_gate.weight", &.{ h, query });
            _ = try g.weight("relation_scorer.relation_content_gate.bias", &.{h});
            _ = try g.weight("relation_scorer.content_linear.weight", &.{ 1, 2 * h + query });
            _ = try g.weight("relation_scorer.content_linear.bias", &.{1});
        }
    }
}

pub fn build(backing: Allocator, config: model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, capacities: Capacities, mode: Mode, limits: Limits) !Plan {
    return buildWithAttentionProfile(backing, config, prepared, schemas, capacities, mode, .materialized_v1, limits);
}

pub fn buildWithAttentionProfile(backing: Allocator, config: model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, capacities: Capacities, mode: Mode, attention_profile: AttentionProfile, limits: Limits) !Plan {
    return buildWithProfiles(backing, config, prepared, schemas, capacities, mode, attention_profile, .retained_v1, limits);
}

pub fn buildWithProfiles(backing: Allocator, config: model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, capacities: Capacities, mode: Mode, attention_profile: AttentionProfile, activation_profile: ActivationProfile, limits: Limits) !Plan {
    if (limits.max_total_host_bytes == 0) return error.InvalidBoundaryTrainingStepOptions;
    const owner = try backing.create(HostOwner);
    errdefer backing.destroy(owner);
    owner.init(backing, limits.max_total_host_bytes);
    return buildOwned(backing, owner, config, prepared, schemas, capacities, mode, attention_profile, activation_profile, limits) catch |err| {
        std.debug.assert(owner.budget.live == 0);
        return owner.translate(err);
    };
}
fn buildOwned(backing: Allocator, owner: *HostOwner, config: model.Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, capacities: Capacities, mode: Mode, attention_profile: AttentionProfile, activation_profile: ActivationProfile, limits: Limits) !Plan {
    const budget = &owner.budget;
    const a = budget.allocator();
    try check(limits.graph.control);
    try config.head.validate();
    if (config.head.candidate_pool != .shared or config.head.candidate_attention_layers != 0 or config.head.query_attention_layers != 0 or config.head.content_soft_max_pool)
        return error.UnsupportedBoundaryTrainingGraphOption;
    const pool_capacity = capacities.pool orelse config.head.pool_size;
    const gold_capacity = capacities.gold_per_query orelse config.head.max_gold_per_query;
    if (pool_capacity != config.head.pool_size or gold_capacity == 0 or gold_capacity > config.head.max_gold_per_query or schemas.len != prepared.samples.len or
        limits.max_outputs == 0 or limits.max_step_host_bytes == 0) return error.InvalidBoundaryTrainingStepOptions;
    const layout = try encoder_graph.layoutFromPrepared(&config, prepared, limits.encoder);
    const schema_fingerprints = try a.alloc([32]u8, schemas.len);
    errdefer a.free(schema_fingerprints);
    for (schemas, prepared.samples, schema_fingerprints) |schema, sample, *fingerprint| {
        if (!std.mem.eql(u8, &schema.fingerprint, &sample.schema_fingerprint)) return error.BoundaryTrainingSchemaMismatch;
        fingerprint.* = schema.fingerprint;
    }
    const graph = try a.create(ml.Graph);
    errdefer a.destroy(graph);
    graph.* = ml.Graph.init(a);
    errdefer graph.deinit();
    var builder = ml.Builder.init(graph);
    var encoder = try encoder_graph.buildWithProfiles(&builder, &config, layout, if (mode == .training) .train else .eval, attention_profile, activation_profile, limits.encoder);
    errdefer encoder.deinit();
    var g = try graph_mod.GraphBuilder.init(a, &builder, config, .{ .batch = layout.batch, .words = layout.words, .queries = layout.queries, .classifications = layout.classifications }, mode, limits.graph);
    defer g.deinit();
    var outputs = std.ArrayListUnmanaged(Output).empty;
    errdefer outputs.deinit(a);
    var records = std.ArrayListUnmanaged(RecordGroup).empty;
    errdefer {
        for (records.items) |record| a.free(record.fields);
        records.deinit(a);
    }
    var relations = std.ArrayListUnmanaged(RelationRoute).empty;
    errdefer relations.deinit(a);
    var proposal_views = std.ArrayListUnmanaged(Id).empty;
    errdefer proposal_views.deinit(a);
    var pool_input: ?candidate_graph.PoolInput = null;
    var relation_input: ?task_graph.RelationInput = null;
    var relation_capacity: u32 = 0;
    if (layout.classifications != 0) {
        const count = try std.math.mul(u32, layout.batch, layout.classifications);
        try outputAppend(a, &outputs, limits, .classification, try g.buildClassification(encoder.nodes.classifications, count), 0);
    }
    if (layout.queries != 0) {
        const input = graph_mod.Input{ .text = encoder.nodes.text, .queries = encoder.nodes.queries, .text_mask = try g.reshape(encoder.inputs.routes[@intFromEnum(encoder_graph.RouteKind.text)].valid, &.{ layout.batch, layout.words }), .query_mask = try g.reshape(encoder.inputs.routes[@intFromEnum(encoder_graph.RouteKind.queries)].valid, &.{ layout.batch, layout.queries }) };
        const proposals = try g.buildProposals(input);
        try proposal_views.appendSlice(a, &.{ proposals.start_logits, proposals.end_logits, proposals.inside_logits, proposals.pool_start, proposals.pool_end });
        try outputAppend(a, &outputs, limits, .start, proposals.start_logits, 0);
        try outputAppend(a, &outputs, limits, .end, proposals.end_logits, 0);
        try outputAppend(a, &outputs, limits, .inside, proposals.inside_logits, 0);
        if (proposals.null_logits) |node| if (config.head.abstention_loss_weight > 0) try outputAppend(a, &outputs, limits, .abstention, node, 0);
        if (proposals.count_logits) |node| if (config.head.count_loss_weight > 0) try outputAppend(a, &outputs, limits, .count, node, 0);
        pool_input = try candidate_graph.poolInputs(&g, pool_capacity);
        const scored = try candidate_graph.buildSharedPool(&g, proposals, pool_input.?);
        try outputAppend(a, &outputs, limits, .pair, scored.pair_logits, 0);
        try outputAppend(a, &outputs, limits, .proposal, scored.proposal_logits, 0);
        const h = config.encoder.hidden_size;
        const bc = try std.math.mul(u32, layout.batch, pool_capacity);
        const bq = try std.math.mul(u32, layout.batch, layout.queries);
        for (schemas, prepared.samples, 0..) |compiled, sample, b| {
            if (mode == .training) for (compiled.schema.structures, 0..) |structure, s| {
                const record_mode = structure.mode orelse continue;
                if (!config.head.enable_records) return error.UnsupportedBoundaryTrainingRecordTask;
                if (records.items.len >= limits.max_record_groups or structure.fields.len > limits.matching.max_fields) return error.BoundaryTrainingStepLimitExceeded;
                const fields = try a.alloc(matching.Field, structure.fields.len);
                errdefer a.free(fields);
                const field_indices = try a.alloc(usize, structure.fields.len);
                defer a.free(field_indices);
                for (structure.fields, fields, field_indices, 0..) |definition, *field, *index, f| {
                    const q = try route(sample, .field, s, f);
                    field.* = .{ .query = q, .cardinality = targets_mod.fieldCardinality(definition, structure.anchor == f) };
                    index.* = b * layout.queries + q;
                }
                const pool_indices = try consecutive(&g, b * pool_capacity, pool_capacity, bc);
                const mask = try g.reshape(try g.gather(try g.reshape(pool_input.?.valid, &.{ bc, 1 }), pool_indices, pool_capacity, 1), &.{pool_capacity});
                const instances = @max(pool_capacity, config.head.record_instance_queries);
                const active = if (record_mode == .anchorless) try g.fill(&.{config.head.record_instance_queries}, 1) else mask;
                const active_count = if (record_mode == .anchorless) config.head.record_instance_queries else pool_capacity;
                const instance_mask = if (active_count == instances) active else try builder.concat(active, try g.fill(&.{instances - active_count}, 0), 0);
                const anchor = if (structure.anchor) |f| fields[f].query else null;
                const natural: ?Id = if (anchor) |q| try g.reshape(try g.gather(try g.reshape(scored.pair_logits, &.{ try std.math.mul(u32, bq, pool_capacity), 1 }), try consecutive(&g, (b * layout.queries + q) * pool_capacity, pool_capacity, try product(bq, pool_capacity)), pool_capacity, 1), &.{pool_capacity}) else null;
                const out = try task_graph.buildRecordGroupDense(&g, .{ .mode = record_mode, .candidates = pool_capacity, .fields = @intCast(fields.len), .candidate_states = try g.gather(scored.candidate_states orelse return error.UnsupportedBoundaryTrainingRecordTask, pool_indices, pool_capacity, h), .field_queries = try g.gather(encoder.nodes.queries, try indices(&g, field_indices, bq), @intCast(fields.len), h), .candidate_mask = mask, .field_membership = try g.expand(mask, &.{ @intCast(fields.len), pool_capacity }, &.{1}), .instance_mask = instance_mask, .natural_object_logits = natural });
                const group = records.items.len;
                try outputAppend(a, &outputs, limits, .record_object, out.object_logits, group);
                try outputAppend(a, &outputs, limits, .record_assignment, out.assignment_logits, group);
                try records.append(a, .{ .sample = b, .structure = s, .group = sample.queries[fields[0].query].group_index, .mode = record_mode, .fields = fields, .anchor_query = anchor, .instances = instances });
            };
            if (mode == .training) {
                var local: usize = 0;
                for (sample.groups) |group| {
                    if (group.kind != .relation) continue;
                    if (!config.head.enable_relations) return error.UnsupportedBoundaryTrainingRelationTask;
                    if (relations.items.len >= limits.relation.max_routes) return error.BoundaryTrainingStepLimitExceeded;
                    try relations.append(a, .{ .sample = b, .local = local, .schema_index = group.schema_index, .head_query = try route(sample, .relation_head, group.schema_index, null), .tail_query = try route(sample, .relation_tail, group.schema_index, null), .allow_self = if (compiled.schema.joint_ie) |joint| joint.relations[group.schema_index].allow_self else false });
                    local += 1;
                }
            }
        }
        if (relations.items.len != 0) {
            const count = try product(relations.items.len, config.head.relation_pair_cap);
            if (count > limits.max_relation_pairs or count > limits.relation.max_output_pairs) return error.BoundaryTrainingStepLimitExceeded;
            relation_capacity = std.math.cast(u32, count) orelse return error.BoundaryTrainingStepLimitExceeded;
            relation_input = try relationInputs(&g, encoder.nodes, layout.relations, relation_capacity);
            const relations_out = try task_graph.buildRelations(&g, relation_input.?);
            try outputAppend(a, &outputs, limits, .relation, relations_out.logits, 0);
        }
    }
    if (outputs.items.len == 0) return error.MissingBoundaryTrainingSupervision;
    try touchWeights(&g);
    try g.check();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.gliner25.training.plan.v1");
    if (attention_profile != .materialized_v1) {
        hash.update("\x00attention_profile\x00");
        hash.update(@tagName(attention_profile));
        hash.update("\x00");
    }
    // Canonical JSON includes every numeric/semantic model switch, avoiding
    // undefined struct padding and platform-dependent object layouts.
    const config_bytes = try std.json.Stringify.valueAlloc(a, config, .{});
    defer a.free(config_bytes);
    hash.update(config_bytes);
    hash.update(@tagName(mode));
    inline for (std.meta.fields(encoder_graph.Layout)) |field| appendHash(&hash, @field(layout, field.name));
    appendHash(&hash, pool_capacity);
    appendHash(&hash, gold_capacity);
    for (schema_fingerprints) |fingerprint| hash.update(&fingerprint);
    const owned_bindings = try g.bindings.toOwnedSlice(a);
    errdefer {
        for (owned_bindings) |binding| a.free(binding.name);
        a.free(owned_bindings);
    }
    const owned_outputs = try outputs.toOwnedSlice(a);
    errdefer a.free(owned_outputs);
    const owned_records = try records.toOwnedSlice(a);
    errdefer {
        for (owned_records) |record| a.free(record.fields);
        a.free(owned_records);
    }
    const owned_relations = try relations.toOwnedSlice(a);
    errdefer a.free(owned_relations);
    const owned_views = try proposal_views.toOwnedSlice(a);
    return .{ .backing = backing, .host_owner = owner, .budget = budget, .allocator = a, .graph = graph, .config = config, .mode = mode, .limits = limits, .pool_capacity = pool_capacity, .gold_capacity = gold_capacity, .relation_capacity = relation_capacity, .encoder = encoder, .bindings = owned_bindings, .pool_input = pool_input, .relation_input = relation_input, .records = owned_records, .relations = owned_relations, .outputs = owned_outputs, .proposal_views = owned_views, .schema_fingerprints = schema_fingerprints, .fingerprint = hash.finalResult() };
}

fn relationInputs(g: *graph_mod.GraphBuilder, nodes: encoder_graph.RoutedNodes, relations: u32, pairs: u32) !task_graph.RelationInput {
    const bw = try std.math.mul(u32, g.layout.batch, g.layout.words);
    const bn = try std.math.mul(u32, g.layout.batch, g.layout.words + 1);
    var endpoints: [4]Id = undefined;
    for (&endpoints, 0..) |*node, index| {
        var name: [96]u8 = undefined;
        node.* = try g.input(try std.fmt.bufPrint(&name, "__gliner25.relations.endpoint.{d}", .{index}), Shape.init(.i32, &.{pairs}), .indices, .candidates, bw);
    }
    return .{
        .pairs = pairs,
        .relations = relations,
        .text = nodes.text,
        .relation_queries = nodes.relation_queries,
        .query_indices = try g.input("__gliner25.relations.query_indices", Shape.init(.i32, &.{pairs}), .indices, .candidates, try std.math.mul(u32, g.layout.batch, relations)),
        .text_indices = endpoints,
        .head_prefix_start = try g.input("__gliner25.relations.head_prefix_start", Shape.init(.i32, &.{pairs}), .indices, .candidates, bn),
        .head_prefix_end = try g.input("__gliner25.relations.head_prefix_end", Shape.init(.i32, &.{pairs}), .indices, .candidates, bn),
        .tail_prefix_start = try g.input("__gliner25.relations.tail_prefix_start", Shape.init(.i32, &.{pairs}), .indices, .candidates, bn),
        .tail_prefix_end = try g.input("__gliner25.relations.tail_prefix_end", Shape.init(.i32, &.{pairs}), .indices, .candidates, bn),
        .head_length = try g.input("__gliner25.relations.head_length", Shape.init(.f32, &.{ pairs, 1 }), .values, .candidates, 0),
        .tail_length = try g.input("__gliner25.relations.tail_length", Shape.init(.f32, &.{ pairs, 1 }), .values, .candidates, 0),
        .geometry = try g.input("__gliner25.relations.geometry", Shape.init(.f32, &.{ pairs, 2 }), .values, .candidates, 0),
        .valid = try g.input("__gliner25.relations.valid", Shape.init(.f32, &.{pairs}), .binary_mask, .candidates, 0),
    };
}

const StepWork = struct {
    limit: usize,
    control: ?Control,
    count: usize = 0,
    fn charge(self: *StepWork, count: usize) !void {
        if (count > self.limit -| self.count) return error.BoundaryTrainingStepLimitExceeded;
        self.count += count;
        try check(self.control);
    }
    fn available(self: StepWork, requested: usize) usize {
        return @min(requested, self.limit -| self.count);
    }
};
const Uploads = struct {
    allocator: Allocator,
    cb: *const ops.ComputeBackend,
    io: *transfer.IO,
    values: std.ArrayListUnmanaged(ops.CT) = .empty,
    fn deinit(self: *Uploads) void {
        for (self.values.items) |value| self.cb.free(value);
        self.values.deinit(self.allocator);
    }
    fn upload(self: *Uploads, shape: Shape, values: encoder_graph.Values) !ops.CT {
        const tensor = try self.io.upload(shape, values);
        errdefer self.cb.free(tensor);
        try self.values.append(self.allocator, tensor);
        return tensor;
    }
};
fn required(plan: *Plan, id: Id) bool {
    if (plan.recomputation) |*regional| {
        for (regional.graph.regional.inputDescriptors()) |input| if (input.node == id) return true;
    }
    return headRequired(plan, id);
}
fn headRequired(plan: *Plan, original: Id) bool {
    const base = plan.session.?.base();
    const id = if (plan.recomputation) |*regional| regional.graph.headId(original) else original;
    return id < base.differentiated.id_map.len and base.differentiated.id_map[id] != nil;
}
fn replayInput(plan: *const Plan, id: Id) bool {
    if (plan.recomputation) |*regional| for (regional.recipes.entries) |entry| {
        if (entry.node == id) return true;
    };
    return false;
}
/// Convert original parameter IDs only at the independently compiled head
/// boundary. Optimizer bindings and regional replay keep their original IDs.
fn mapHeadInputs(plan: *Plan, list: *std.ArrayListUnmanaged(interpreter.RuntimeInput)) void {
    const regional = if (plan.recomputation) |*value| value else return;
    var count: usize = 0;
    for (list.items) |input| {
        if (!headRequired(plan, input.node_id)) continue;
        list.items[count] = .{ .node_id = regional.graph.headId(input.node_id), .value = input.value };
        count += 1;
    }
    list.shrinkRetainingCapacity(count);
}
fn addUpload(plan: *Plan, owner: *Uploads, list: *std.ArrayListUnmanaged(interpreter.RuntimeInput), id: Id, shape: Shape, values: encoder_graph.Values) !void {
    if (!required(plan, id)) return;
    try list.append(owner.allocator, .{ .node_id = id, .value = try owner.upload(shape, values) });
}
fn fillHeadDropout(binding: graph_mod.Binding, replay: encoder_graph.Replay, optimizer_step: u64, output: []f32) !void {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.gliner25.training.head-dropout.v1");
    hash.update(binding.name);
    appendHash(&hash, replay.seed);
    appendHash(&hash, replay.micro_batch);
    appendHash(&hash, replay.replica);
    appendHash(&hash, optimizer_step);
    const digest = hash.finalResult();
    const stream = std.mem.readInt(u64, digest[0..8], .little);
    const threshold: u64 = @intFromFloat(@as(f64, binding.probability) * 4294967296.0);
    const scale: f32 = 1 / (1 - binding.probability);
    for (output, 0..) |*value, index| {
        var x = (stream ^ @as(u64, @intCast(index))) +% 0x9e3779b97f4a7c15;
        x = (x ^ (x >> 30)) *% 0xbf58476d1ce4e5b9;
        x = (x ^ (x >> 27)) *% 0x94d049bb133111eb;
        x ^= x >> 31;
        value.* = if (x >> 32 < threshold) 0 else scale;
    }
    try graph_mod.validateFloatBinding(binding, output);
}
fn hashFloat(hash: *std.crypto.hash.sha2.Sha256, value: f32) void {
    appendHash(hash, @as(u32, @bitCast(value)));
}
fn targetFingerprint(a: Allocator, targets: *const targets_mod.Targets) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.gliner25.training.targets.v1");
    const bytes = try std.json.Stringify.valueAlloc(a, targets.samples, .{});
    defer a.free(bytes);
    hash.update(bytes);
    appendHash(&hash, targets.gold_capacity);
    for (targets.classification_targets) |value| hashFloat(&hash, value);
    for (targets.classification_mask) |value| hash.update(&.{@intFromBool(value)});
    return hash.finalResult();
}
fn validateDraws(draws: ?[]const f32, count: usize) !void {
    if (draws) |values| {
        if (values.len != count) return error.InvalidBoundaryTrainingShape;
        for (values) |value| if (!std.math.isFinite(value) or value < 0 or value >= 1) return error.InvalidBoundaryTrainingInjectionDraw;
    }
}
const DropoutOverrides = struct {
    values: std.StringHashMapUnmanaged([]const f32) = .empty,
    fingerprint: ?[32]u8 = null,
    fn deinit(self: *DropoutOverrides, a: Allocator) void {
        self.values.deinit(a);
    }
};
fn dropoutOverrides(plan: *Plan, a: Allocator, masks: ?[]const DropoutMask, work: *StepWork) !DropoutOverrides {
    // A dense explicit probability mask cannot be represented by the compact
    // counter leaf. Fail before binding instead of silently mixing protocols.
    if (plan.encoder.attention_profile == .replay_tiled_v1 and masks != null)
        return error.UnsupportedBoundaryTrainingAttentionDropoutOverride;
    var result = DropoutOverrides{};
    errdefer result.deinit(a);
    const supplied = masks orelse return result;
    if (supplied.len > plan.limits.max_dropout_sites) return error.BoundaryTrainingStepLimitExceeded;
    const descriptors = try plan.dropoutDescriptors(a);
    defer a.free(descriptors);
    if (supplied.len < descriptors.len) return error.MissingBoundaryTrainingDropoutMask;
    if (supplied.len > descriptors.len) return error.UnexpectedBoundaryTrainingDropoutMask;
    for (supplied) |mask| {
        if (mask.name.len == 0 or mask.name.len > 1024) return error.InvalidBoundaryTrainingDropoutMask;
        try work.charge(mask.name.len + 1);
        const entry = try result.values.getOrPut(a, mask.name);
        if (entry.found_existing) return error.DuplicateBoundaryTrainingDropoutMask;
        entry.value_ptr.* = mask.values;
    }
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.gliner25.training.explicit-dropout.v1");
    for (descriptors) |descriptor| {
        const values = result.values.get(descriptor.name) orelse return error.UnexpectedBoundaryTrainingDropoutMask;
        const count = std.math.cast(usize, descriptor.shape.numElements() orelse return error.InvalidBoundaryTrainingDropoutMask) orelse return error.InvalidBoundaryTrainingDropoutMask;
        if (values.len != count or !std.math.isFinite(descriptor.probability) or descriptor.probability < 0 or descriptor.probability >= 1) return error.InvalidBoundaryTrainingDropoutMask;
        try work.charge(count + descriptor.name.len);
        const scale: f32 = 1 / (1 - descriptor.probability);
        appendHash(&hash, descriptor.name.len);
        hash.update(descriptor.name);
        hashFloat(&hash, descriptor.probability);
        appendHash(&hash, count);
        for (values) |value| {
            if (value != 0 and value != scale) return error.InvalidBoundaryTrainingDropoutMask;
            hashFloat(&hash, value);
        }
    }
    result.fingerprint = hash.finalResult();
    return result;
}

fn preparedIdentity(plan: *Plan, targets: [32]u8, prepared: *const processor.PreparedBatch, context: StepContext, dropout_fingerprint: ?[32]u8) seeded.StepIdentity {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.gliner25.training.step.v1");
    hash.update(&context.identity.binding);
    hash.update(&plan.fingerprint);
    hash.update(&targets);
    appendHash(&hash, context.replay.seed);
    appendHash(&hash, context.replay.micro_batch);
    appendHash(&hash, context.replay.replica);
    appendHash(&hash, context.progress.total_optimizer_steps);
    hash.update(&.{@intFromBool(dropout_fingerprint != null)});
    if (dropout_fingerprint) |fingerprint| hash.update(&fingerprint);
    hash.update(&.{@intFromBool(context.scales_override != null)});
    if (context.scales_override) |scales| inline for (std.meta.fields(objectives.Scales)) |field| hashFloat(&hash, @field(scales, field.name));
    inline for (std.meta.fields(objectives.Weights)) |field| hashFloat(&hash, @field(context.weights, field.name));
    for ([_]f32{ context.progress.gold_start, context.progress.gold_end, context.progress.gold_hold_fraction }) |value| hashFloat(&hash, value);
    for (prepared.input_ids) |id| appendHash(&hash, @bitCast(id));
    for (prepared.attention_mask) |mask| appendHash(&hash, @bitCast(mask));
    for ([_]?[]const f32{ context.injection_draws, context.negative_query_draws }) |draws| {
        hash.update(&.{@intFromBool(draws != null)});
        if (draws) |values| for (values) |value| hashFloat(&hash, value);
    }
    var identity = context.identity;
    hash.final(&identity.binding);
    return identity;
}
fn poolBinding(pool: *const decisions.Pool, binding: graph_mod.Binding) !encoder_graph.Values {
    const name = binding.name;
    if (std.mem.eql(u8, name, "__gliner25.pool.starts")) return .{ .i32 = pool.starts };
    if (std.mem.eql(u8, name, "__gliner25.pool.ends")) return .{ .i32 = pool.ends };
    if (std.mem.eql(u8, name, "__gliner25.pool.valid")) return .{ .f32 = pool.valid };
    if (std.mem.eql(u8, name, "__gliner25.pool.lengths")) return .{ .f32 = pool.lengths };
    if (std.mem.eql(u8, name, "__gliner25.pool.length_features")) return .{ .f32 = pool.length_features };
    if (std.mem.eql(u8, name, "__gliner25.pool.inside_mean")) return .{ .f32 = pool.inside_mean };
    return error.InvalidBoundaryTrainingBinding;
}
fn validateBinding(binding: graph_mod.Binding, values: encoder_graph.Values) !void {
    switch (values) {
        .f32 => |items| try graph_mod.validateFloatBinding(binding, items),
        .i32 => |items| try graph_mod.validateIndexBinding(binding, items),
    }
}
const RelationGeometry = struct {
    query_indices: []i32,
    text_indices: [4][]i32,
    head_prefix_start: []i32,
    head_prefix_end: []i32,
    tail_prefix_start: []i32,
    tail_prefix_end: []i32,
    head_length: []f32,
    tail_length: []f32,
    geometry: []f32,
    valid: []f32,
    mask: []bool,
    labels: []f32,
    gold: usize,
    covered: usize,
    fingerprint: [32]u8,
    fn binding(self: RelationGeometry, descriptor: graph_mod.Binding) !encoder_graph.Values {
        inline for (.{ "query_indices", "head_prefix_start", "head_prefix_end", "tail_prefix_start", "tail_prefix_end" }) |name|
            if (std.mem.eql(u8, descriptor.name, "__gliner25.relations." ++ name)) return .{ .i32 = @field(self, name) };
        inline for (.{ "head_length", "tail_length", "geometry", "valid" }) |name|
            if (std.mem.eql(u8, descriptor.name, "__gliner25.relations." ++ name)) return .{ .f32 = @field(self, name) };
        for (self.text_indices, 0..) |values, i| {
            var name: [96]u8 = undefined;
            if (std.mem.eql(u8, descriptor.name, try std.fmt.bufPrint(&name, "__gliner25.relations.endpoint.{d}", .{i}))) return .{ .i32 = values };
        }
        return error.InvalidBoundaryTrainingBinding;
    }
};
fn zeros(comptime T: type, a: Allocator, count: usize, value: T) ![]T {
    const values = try a.alloc(T, count);
    @memset(values, value);
    return values;
}
/// Storage belongs to the caller's bounded per-step arena.
fn selectRelations(plan: *Plan, a: Allocator, pool: *const decisions.Pool, pairs: []const f32, targets: *const targets_mod.Targets, work: *StepWork) !RelationGeometry {
    const b = plan.encoder.layout.batch;
    const q = plan.encoder.layout.queries;
    const w = plan.encoder.layout.words;
    const c = plan.pool_capacity;
    const count = plan.relation_capacity;
    const spans = try a.alloc(boundary_ops.Span, pool.selection.spans.len);
    for (spans, pool.selection.spans) |*out, span| out.* = .{ .start = @intCast(span.start), .end = @intCast(span.end) };
    const routes = try a.alloc(relation_proposals.Route, plan.relations.len);
    const endpoints = try a.alloc(usize, try product(plan.relations.len, 2));
    for (plan.relations, routes, 0..) |route_info, *out, i| {
        endpoints[2 * i] = route_info.head_query;
        endpoints[2 * i + 1] = route_info.tail_query;
        out.* = .{ .batch_index = route_info.sample, .relation_index = route_info.local, .head_queries = endpoints[2 * i ..][0..1], .tail_queries = endpoints[2 * i + 1 ..][0..1], .allow_self = route_info.allow_self };
    }
    var options = plan.limits.relation;
    options.control = work.control;
    options.heads_per_relation = plan.config.head.relation_heads_per_type;
    options.tails_per_relation = plan.config.head.relation_tails_per_type;
    options.pair_cap = plan.config.head.relation_pair_cap;
    options.argument_threshold = plan.config.head.relation_argument_proposal_threshold;
    options.max_output_pairs = @min(options.max_output_pairs, count);
    const route_work = try product(plan.relations.len, try product(@min(c, options.heads_per_relation), @min(c, options.tails_per_relation)));
    try work.charge(route_work + try product(plan.relations.len, try product(q, c)));
    const proposed = try relation_proposals.generate(a, .{ .batch = b, .queries = q, .capacity = c, .spans = spans, .valid = pool.selection.valid, .query_mask = targets.query_mask, .logits = pairs }, routes, options);
    var result = RelationGeometry{ .query_indices = try zeros(i32, a, count, 0), .text_indices = undefined, .head_prefix_start = try zeros(i32, a, count, 0), .head_prefix_end = try zeros(i32, a, count, 0), .tail_prefix_start = try zeros(i32, a, count, 0), .tail_prefix_end = try zeros(i32, a, count, 0), .head_length = try zeros(f32, a, count, 1), .tail_length = try zeros(f32, a, count, 1), .geometry = try zeros(f32, a, try product(count, 2), 0), .valid = try zeros(f32, a, count, 0), .mask = try zeros(bool, a, count, false), .labels = try zeros(f32, a, count, 0), .gold = 0, .covered = 0, .fingerprint = undefined };
    for (&result.text_indices) |*values| values.* = try zeros(i32, a, count, 0);
    const covered = try a.alloc([]bool, targets.samples.len);
    for (targets.samples, covered) |sample, *mask| {
        mask.* = try zeros(bool, a, sample.relations.len, false);
        result.gold += sample.relations.len;
    }
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.gliner25.training.relations.v1");
    for (proposed, 0..) |proposal, i| {
        try work.charge(targets.samples[proposal.batch_index].relations.len + 1);
        const batch = proposal.batch_index;
        const head = proposal.head_span;
        const tail = proposal.tail_span;
        if (head.end > targets.samples[batch].word_count or tail.end > targets.samples[batch].word_count) return error.InvalidBoundaryTrainingRouting;
        const relation = for (plan.relations) |route_info| {
            if (route_info.sample == batch and route_info.local == proposal.relation_index) break route_info;
        } else return error.InvalidBoundaryTrainingRouting;
        result.query_indices[i] = @intCast(batch * plan.encoder.layout.relations + proposal.relation_index);
        result.text_indices[0][i] = @intCast(batch * w + head.start);
        result.text_indices[1][i] = @intCast(batch * w + head.end - 1);
        result.text_indices[2][i] = @intCast(batch * w + tail.start);
        result.text_indices[3][i] = @intCast(batch * w + tail.end - 1);
        result.head_prefix_start[i] = @intCast(batch * (w + 1) + head.start);
        result.head_prefix_end[i] = @intCast(batch * (w + 1) + head.end);
        result.tail_prefix_start[i] = @intCast(batch * (w + 1) + tail.start);
        result.tail_prefix_end[i] = @intCast(batch * (w + 1) + tail.end);
        result.head_length[i] = @floatFromInt(head.end - head.start);
        result.tail_length[i] = @floatFromInt(tail.end - tail.start);
        const delta = @as(i64, @intCast(tail.start)) - @as(i64, @intCast(head.start));
        result.geometry[2 * i] = if (delta > 0) 1 else if (delta < 0) -1 else 0;
        result.geometry[2 * i + 1] = @as(f32, @floatFromInt(@abs(delta))) / @as(f32, @floatFromInt(@max(w, 1)));
        result.valid[i] = 1;
        result.mask[i] = true;
        for (targets.samples[batch].relations, covered[batch]) |gold, *present| {
            if (gold.relation_type == relation.schema_index and gold.head.start == head.start and gold.head.end == head.end and gold.tail.start == tail.start and gold.tail.end == tail.end) {
                result.labels[i] = 1;
                present.* = true;
            }
        }
        appendHash(&hash, batch);
        appendHash(&hash, relation.schema_index);
        appendHash(&hash, head.start);
        appendHash(&hash, head.end);
        appendHash(&hash, tail.start);
        appendHash(&hash, tail.end);
        hashFloat(&hash, result.labels[i]);
    }
    for (covered) |mask| for (mask) |present| {
        result.covered += @intFromBool(present);
    };
    hash.final(&result.fingerprint);
    return result;
}

fn recordObjectives(plan: *Plan, a: Allocator, pool: *const decisions.Pool, targets: *const targets_mod.Targets, logits: []const []const f32, work: *StepWork, observer: ?decision_events.Observer) !record_loss.Result {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const maps = try scratch.alloc(matching.TargetMap, plan.records.len);
    var map_count: usize = 0;
    defer for (maps[0..map_count]) |*map| map.deinit();
    const matches = try scratch.alloc(matching.Matches, plan.records.len);
    var match_count: usize = 0;
    defer for (matches[0..match_count]) |*item| item.deinit();
    const groups = try scratch.alloc(record_loss.Group, plan.records.len);
    for (plan.records, 0..) |record, group_index| {
        const c = plan.pool_capacity;
        const spans = pool.selection.spans[record.sample * c ..][0..c];
        const valid = pool.selection.valid[record.sample * c ..][0..c];
        const membership = try scratch.alloc(bool, try product(record.fields.len, c));
        for (0..record.fields.len) |field| @memcpy(membership[field * c ..][0..c], valid);
        const instance_mask = try zeros(bool, scratch, record.instances, false);
        if (record.mode == .anchorless) @memset(instance_mask[0..plan.config.head.record_instance_queries], true) else @memcpy(instance_mask[0..c], valid);
        const anchors: []?usize = if (record.mode == .natural) try scratch.alloc(?usize, record.instances) else &.{};
        for (anchors, 0..) |*anchor, index| anchor.* = if (index < c and valid[index]) index else null;
        var options = plan.limits.matching;
        options.control = work.control;
        options.max_work = work.available(options.max_work);
        maps[group_index] = try matching.compile(a, .{ .structure = record.structure, .group = record.group, .mode = record.mode, .word_count = targets.samples[record.sample].word_count, .fields = record.fields, .candidate_spans = spans, .candidate_valid = valid, .field_membership = membership, .instance_mask = instance_mask, .anchor_query = record.anchor_query, .anchor_candidates = anchors }, targets.samples[record.sample].records, options);
        map_count += 1;
        try work.charge(maps[group_index].work);
        const scores = matching.Logits{ .objects = logits[try plan.outputIndex(.record_object, group_index)], .assignments = logits[try plan.outputIndex(.record_assignment, group_index)] };
        options.max_work = work.available(plan.limits.matching.max_work);
        var costs = try matching.buildFloat32Costs(a, &maps[group_index], scores, options);
        defer costs.deinit();
        try work.charge(costs.work);
        options.max_work = work.available(plan.limits.matching.max_work);
        matches[group_index] = try matching.match(a, &maps[group_index], &costs, options);
        match_count += 1;
        try work.charge(matches[group_index].work);
        try decision_events.Observer.emit(observer, .{ .record = .{ .group = group_index, .sample = record.sample, .schema_group = record.group, .target = &maps[group_index], .matches = &matches[group_index] } });
        groups[group_index] = .{ .target = &maps[group_index], .matches = &matches[group_index], .logits = scores };
    }
    var options = plan.limits.record_loss;
    options.control = work.control;
    options.max_work = work.available(options.max_work);
    options.object_weight = 1;
    options.field_weight = 1;
    var result = try record_loss.compute(a, groups, options);
    errdefer result.deinit();
    try work.charge(result.work);
    return result;
}
pub fn canonicalParameterName(name: []const u8) []const u8 {
    return if (std.mem.startsWith(u8, name, "base_model.model.")) name["base_model.model.".len..] else name;
}
pub fn isTouchParameter(parameter_name: []const u8, head: model.HeadConfig) bool {
    // Includes PEFT parameters below these optional modules. Other dormant
    // boundary/proposer/classifier parameters keep grad=None semantics.
    const name = canonicalParameterName(parameter_name);
    return (head.enable_records and std.mem.startsWith(u8, name, "record_decoder.")) or
        (head.enable_relations and (std.mem.startsWith(u8, name, "relation_scorer.") or std.mem.startsWith(u8, name, "relation_pair_generator.")));
}
fn runStep(plan: *Plan, a: Allocator, cb: *const ops.ComputeBackend, parameters: []const interpreter.RuntimeInput, prepared: *const processor.PreparedBatch, schemas: []const *const schema_mod.CompiledSchema, annotations: []const targets_mod.Annotations, context: StepContext, control: ?Control, transfer_admission: transfer.Admission) !StepResult {
    var work = StepWork{ .limit = plan.limits.max_step_work, .control = control };
    try work.charge(0);
    if (context.identity.optimizer_step != context.progress.optimizer_step or context.identity.microbatch != context.replay.micro_batch) return error.TrainingTapeIdentityMismatch;
    if (schemas.len != plan.schema_fingerprints.len or annotations.len != schemas.len or prepared.samples.len != schemas.len) return error.BoundaryTrainingSchemaMismatch;
    for (schemas, plan.schema_fingerprints, prepared.samples) |schema, fingerprint, sample| {
        if (!std.mem.eql(u8, &schema.fingerprint, &fingerprint) or !std.mem.eql(u8, &sample.schema_fingerprint, &fingerprint)) return error.BoundaryTrainingSchemaMismatch;
    }
    const scheduled_scales = try objectives.scales(plan.config.head, context.progress);
    const active_scales = context.scales_override orelse scheduled_scales;
    inline for (std.meta.fields(objectives.Scales)) |field| {
        const value = @field(active_scales, field.name);
        if (!std.math.isFinite(value) or value < 0) return error.InvalidBoundaryTrainingSchedule;
    }
    if (active_scales.gold_injection > 1) return error.InvalidBoundaryTrainingSchedule;
    var target_options = plan.limits.targets;
    target_options.control = control;
    target_options.gold_capacity = plan.gold_capacity;
    target_options.max_gold_per_query = @min(target_options.max_gold_per_query, plan.config.head.max_gold_per_query);
    target_options.max_work = work.available(target_options.max_work);
    var targets = try targets_mod.compileBatch(a, prepared.samples, schemas, annotations, target_options);
    defer targets.deinit();
    try work.charge(targets.work);
    const target_fingerprint = try targetFingerprint(a, &targets);
    const b = plan.encoder.layout.batch;
    const q = plan.encoder.layout.queries;
    const w = plan.encoder.layout.words;
    const c = plan.pool_capacity;
    const bq = try product(b, q);
    if (targets.query_width != q or targets.word_width != w or targets.classification_width != plan.encoder.layout.classifications) return error.BoundaryGraphShapeMismatch;
    try validateDraws(context.injection_draws, try product(bq, plan.gold_capacity));
    try validateDraws(context.negative_query_draws, bq);
    if (plan.mode == .training and q > 0) {
        if (active_scales.gold_injection > 0 and active_scales.gold_injection < 1 and context.injection_draws == null) return error.MissingBoundaryTrainingInjectionDraws;
        if (plan.config.head.negative_query_ratio > 0 and context.negative_query_draws == null) return error.MissingBoundaryTrainingQueryDraws;
    }
    var masks = try dropoutOverrides(plan, a, context.dropout_masks, &work);
    defer masks.deinit(a);
    if (plan.recomputation) |*regional| {
        const replay = try regional.graph.regional.replaySummary();
        const calls = try std.math.add(usize, replay.initial_calls, replay.replay_calls);
        // Every Context validation hashes its recipe metadata and, for an
        // explicit override, all pinned regional values. Account the complete
        // repeated scans before any forward or dropout upload starts.
        const validations = try std.math.add(usize, replay.source_validation_calls_upper_bound, try std.math.add(usize, try product(calls, 2), 1));
        var scan = regional.recipes.metadataBytes();
        if (context.dropout_masks) |supplied| {
            var explicit_bytes: usize = 0;
            for (supplied) |mask| explicit_bytes = try std.math.add(usize, explicit_bytes, try product(mask.values.len, @sizeOf(f32)));
            if (explicit_bytes > plan.limits.replay.max_explicit_bytes) return error.BoundaryReplayLimitExceeded;
            const aggregate = regional.graph.regional.admission.?;
            if (explicit_bytes > regional.graph.regional.options.limits.max_host_bytes -| aggregate.host_upper_bound_bytes) return error.RecomputeLimitExceeded;
            scan = try std.math.add(usize, scan, regional.recipes.all_mask_bytes / @sizeOf(f32));
        }
        try work.charge(try product(validations, scan));
        try work.charge((try std.math.add(usize, replay.initial_upload_bytes, replay.replay_upload_bytes)) / @sizeOf(f32));
    }
    var host = std.heap.ArenaAllocator.init(a);
    defer host.deinit();
    const scratch = host.allocator();
    const base_options = plan.session.?.base().options;
    var io = transfer.IO{ .allocator = a, .cb = cb, .execution = base_options.execution, .primitive = base_options.resident.program.instruction.primitive, .admission = transfer_admission, .control = control };
    var uploads = Uploads{ .allocator = a, .cb = cb, .io = &io };
    defer uploads.deinit();
    var initial = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
    defer initial.deinit(a);
    // The managed lease may include unchanged optimizer slots for pruned
    // parameters. Verify every caller binding, then omit only proven pruned
    // graph IDs before entering the stricter retained executor.
    for (parameters, 0..) |input, i| {
        try work.charge(1);
        if (input.node_id >= plan.graph.nodes.items.len or plan.graph.node(input.node_id).op != .parameter or
            std.mem.startsWith(u8, plan.graph.parameterName(plan.graph.node(input.node_id)), "__")) return error.InvalidTrainingBinding;
        for (parameters[0..i]) |prior| if (prior.node_id == input.node_id) return error.DuplicateTrainingBinding;
        try seeded.validateTensor(a, cb, input.value, plan.graph.node(input.node_id).output_shape);
        if (required(plan, input.node_id)) try initial.append(a, input);
    }
    var bound = if (plan.recomputation != null)
        try encoder_graph.bindFixedPrepared(a, &plan.encoder, &plan.config, prepared, context.replay)
    else
        try encoder_graph.bindPrepared(a, &plan.encoder, &plan.config, prepared, context.replay);
    defer bound.deinit();
    for (bound.bindings) |binding| {
        try work.charge(1);
        const values: encoder_graph.Values = if (masks.values.get(plan.graph.parameterName(plan.graph.node(binding.node)))) |replacement| .{ .f32 = replacement } else binding.values;
        try addUpload(plan, &uploads, &initial, binding.node, binding.shape, values);
    }
    for (plan.bindings) |binding| {
        if (binding.kind != .inverted_dropout or !required(plan, binding.node)) continue;
        const count = std.math.cast(usize, binding.shape.numElements() orelse return error.InvalidBoundaryTrainingBinding) orelse return error.InvalidBoundaryTrainingBinding;
        try work.charge(count);
        const values = masks.values.get(binding.name) orelse blk: {
            const generated = try scratch.alloc(f32, count);
            try fillHeadDropout(binding, context.replay, context.identity.optimizer_step, generated);
            break :blk generated;
        };
        try addUpload(plan, &uploads, &initial, binding.node, binding.shape, .{ .f32 = values });
    }
    if (plan.peft) |peft| for (peft.uses) |use| {
        if (use.mask == nil or !required(plan, use.mask) or replayInput(plan, use.mask)) continue;
        const count = std.math.cast(usize, use.mask_shape.numElements() orelse return error.InvalidBoundaryTrainingBinding) orelse return error.InvalidBoundaryTrainingBinding;
        try work.charge(count);
        const values = masks.values.get(use.mask_name.?) orelse blk: {
            const generated = try scratch.alloc(f32, count);
            try peft_graph.fillDropout(use, .{ .seed = context.replay.seed, .optimizer_step = context.identity.optimizer_step, .micro_batch = context.replay.micro_batch, .replica = context.replay.replica }, generated);
            break :blk generated;
        };
        try addUpload(plan, &uploads, &initial, use.mask, use.mask_shape, .{ .f32 = values });
    };
    const identity = preparedIdentity(plan, target_fingerprint, prepared, context, masks.fingerprint);
    // This address remains stable until both head and encoder tapes have been
    // consumed. Only one regional mask is materialized per callback.
    var replay_context: ?replay_bindings.Context = null;
    defer if (replay_context) |*value| value.deinit();
    var encoder_tape: ?recomputed.Tape = null;
    defer if (encoder_tape) |*value| value.deinit();
    if (plan.recomputation) |*regional| {
        const supplied: ?[]const replay_bindings.Mask = if (context.dropout_masks != null) explicit: {
            const subset = try scratch.alloc(replay_bindings.Mask, regional.recipes.entries.len);
            for (regional.recipes.entries, subset) |entry, *out| {
                out.* = .{ .name = entry.name, .values = masks.values.get(entry.name) orelse return error.MissingBoundaryTrainingDropoutMask };
            }
            break :explicit subset;
        } else null;
        replay_context = try replay_bindings.Context.init(a, &regional.recipes, context.replay, identity, supplied, &io);
        var fixed = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        defer fixed.deinit(a);
        for (initial.items) |input| if (regional.graph.regional.requiresInput(input.node_id)) try fixed.append(a, input);
        for (regional.graph.regional.inputDescriptors()) |descriptor| {
            if (descriptor.replay != null) continue;
            var found = false;
            for (fixed.items) |input| if (input.node_id == descriptor.node) {
                found = true;
                break;
            };
            if (found) continue;
            if (!descriptor.managed or cb.kind() != .native) return error.MissingTrainingBinding;
            // Native frozen tensors normally resolve through the named store.
            // Recompute requires an explicit lifetime: acquireWeight returns a
            // distinct owned handle borrowing the immutable source payload.
            const tensor = try cb.acquireWeight(plan.graph.parameterName(plan.graph.node(descriptor.node)));
            uploads.values.append(a, tensor) catch |err| {
                cb.free(tensor);
                return err;
            };
            try fixed.append(a, .{ .node_id = descriptor.node, .value = tensor });
        }
        encoder_tape = try regional.graph.regional.forward(cb, fixed.items, try replay_context.?.source(), identity, control);
        mapHeadInputs(plan, &initial);
        try initial.append(a, .{ .node_id = regional.graph.final_hidden, .value = try encoder_tape.?.finalOutput() });
    }
    var pool: ?decisions.Pool = null;
    defer if (pool) |*value| value.deinit();
    var relation_geometry: ?RelationGeometry = null;
    var tape = switch (plan.session.?) {
        .direct => |*session| try session.forward(cb, initial.items, identity, control),
        .stages => |*session| blk: {
            var prefix = try session.forward(cb, initial.items, identity, control);
            defer prefix.deinit();
            var captured: [5][]f32 = undefined;
            var captured_count: usize = 0;
            defer for (captured[0..captured_count]) |values| a.free(values);
            for (&captured, 0..) |*values, i| {
                values.* = try io.download(try prefix.logits(i), plan.graph.node(plan.proposal_views[i]).output_shape, if (i < 3) .proposal_logits else .proposal_features);
                captured_count += 1;
            }
            const word_counts = try scratch.alloc(usize, b);
            for (targets.samples, word_counts) |sample, *count| count.* = sample.word_count;
            var options = selection.Options{ .phase = if (plan.mode == .training) .training else .evaluation, .capacity = c, .gold_injection_probability = if (plan.mode == .training) active_scales.gold_injection else 0, .injection_draws = context.injection_draws, .limits = plan.limits.selection };
            options.limits.control = control;
            options.limits.max_work = work.available(options.limits.max_work);
            pool = try decisions.selectPool(a, plan.config.head, .{ .batch = b, .queries = q, .words = w, .dimension = plan.config.head.boundary_dim, .starts = captured[0], .ends = captured[1], .inside = captured[2], .projected_starts = captured[3], .projected_ends = captured[4], .query_mask = targets.query_mask, .word_counts = word_counts }, targets.gold(), options);
            try work.charge(pool.?.work);
            try decision_events.Observer.emit(context.decision_observer, .{ .pool = &pool.? });
            var late = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
            defer late.deinit(a);
            for (plan.bindings) |binding| {
                if (!std.mem.startsWith(u8, binding.name, "__gliner25.pool.") or !required(plan, binding.node)) continue;
                const values = try poolBinding(&pool.?, binding);
                try validateBinding(binding, values);
                try addUpload(plan, &uploads, &late, binding.node, binding.shape, values);
            }
            mapHeadInputs(plan, &late);
            try prefix.advance(identity, pool.?.fingerprint, late.items, control);
            if (plan.relation_input != null) {
                const pair_node = plan.outputs[try plan.outputIndex(.pair, 0)].node;
                const pair_scores = try io.download(try prefix.logits(0), plan.graph.node(pair_node).output_shape, .proposal_logits);
                defer a.free(pair_scores);
                relation_geometry = try selectRelations(plan, scratch, &pool.?, pair_scores, &targets, &work);
                const geometry = relation_geometry.?;
                try decision_events.Observer.emit(context.decision_observer, .{ .relations = .{ .query_indices = geometry.query_indices, .text_indices = .{ geometry.text_indices[0], geometry.text_indices[1], geometry.text_indices[2], geometry.text_indices[3] }, .head_prefix_start = geometry.head_prefix_start, .head_prefix_end = geometry.head_prefix_end, .tail_prefix_start = geometry.tail_prefix_start, .tail_prefix_end = geometry.tail_prefix_end, .mask = geometry.mask, .labels = geometry.labels } });
                if (context.require_gold_relation_coverage and relation_geometry.?.covered != relation_geometry.?.gold) return error.MissingBoundaryTrainingGoldRelation;
                late.clearRetainingCapacity();
                for (plan.bindings) |binding| {
                    if (!std.mem.startsWith(u8, binding.name, "__gliner25.relations.") or !required(plan, binding.node)) continue;
                    const values = try relation_geometry.?.binding(binding);
                    try validateBinding(binding, values);
                    try addUpload(plan, &uploads, &late, binding.node, binding.shape, values);
                }
                mapHeadInputs(plan, &late);
                try prefix.advance(identity, relation_geometry.?.fingerprint, late.items, control);
            }
            break :blk try prefix.finish();
        },
    };
    defer tape.deinit();
    const logits = try a.alloc([]const f32, plan.outputs.len);
    defer a.free(logits);
    var logit_count: usize = 0;
    defer for (logits[0..logit_count]) |values| a.free(values);
    for (logits, 0..) |*values, index| {
        values.* = try io.download(try tape.logits(index), plan.graph.node(plan.outputs[index].node).output_shape, .loss_logits);
        logit_count += 1;
        try work.charge(values.len);
        for (values.*) |value| if (!std.math.isFinite(value)) return error.NonFiniteBoundaryTraining;
    }
    var span_loss: ?objectives.Result = null;
    defer if (span_loss) |*value| value.deinit();
    var class_loss: ?primitive.Loss = null;
    defer if (class_loss) |*value| value.deinit();
    var records_loss: ?record_loss.Result = null;
    defer if (records_loss) |*value| value.deinit();
    var relations_loss: ?primitive.Loss = null;
    defer if (relations_loss) |*value| value.deinit();
    var terms = objectives.Terms{};
    var decision_hash = std.crypto.hash.sha2.Sha256.init(.{});
    decision_hash.update("antfly.gliner25.training.backward-decisions.v1");
    decision_hash.update(&identity.binding);
    decision_hash.update(&target_fingerprint);
    var coverage = Coverage{};
    if (pool) |*selected| {
        const text_mask = try scratch.alloc(bool, try product(b, w));
        const boundary_mask = try scratch.alloc(bool, try product(b, w + 1));
        for (targets.samples, 0..) |sample, batch| {
            for (0..w) |word| text_mask[batch * w + word] = word < sample.word_count;
            for (0..w + 1) |word| boundary_mask[batch * (w + 1) + word] = word <= sample.word_count;
        }
        var options = objectives.Options{ .training = plan.mode == .training, .weights = context.weights, .scales = active_scales, .negative_query_draws = context.negative_query_draws, .limits = plan.limits.losses };
        options.limits.control = control;
        options.limits.max_work = work.available(options.limits.max_work);
        span_loss = try objectives.boundary(a, plan.config.head, .{ .batch = b, .queries = q, .words = w, .capacity = c, .starts = logits[try plan.outputIndex(.start, 0)], .ends = logits[try plan.outputIndex(.end, 0)], .inside = logits[try plan.outputIndex(.inside, 0)], .pairs = logits[try plan.outputIndex(.pair, 0)], .proposals = logits[try plan.outputIndex(.proposal, 0)], .nulls = if (plan.config.head.enable_abstention and plan.config.head.abstention_loss_weight > 0) logits[try plan.outputIndex(.abstention, 0)] else null, .counts = if (plan.config.head.enable_count_head and plan.config.head.count_loss_weight > 0) logits[try plan.outputIndex(.count, 0)] else null, .spans = selected.selection.spans, .pool_mask = selected.selection.valid, .query_mask = targets.query_mask, .text_mask = text_mask, .boundary_mask = boundary_mask, .gold = targets.gold() }, options);
        try work.charge(span_loss.?.work);
        try decision_events.Observer.emit(context.decision_observer, .{ .boundary_loss = &span_loss.? });
        terms = span_loss.?.terms;
        coverage.gold_mentions = selected.selection.gold_total;
        coverage.proposed_gold_mentions = selected.selection.gold_hits_before_injection;
        decision_hash.update(&selected.fingerprint);
        decision_hash.update(&span_loss.?.decision_fingerprint);
    }
    var loss_limits = plan.limits.losses;
    loss_limits.control = control;
    if (plan.encoder.layout.classifications != 0) {
        loss_limits.max_work = work.available(plan.limits.losses.max_work);
        class_loss = try objectives.supervisedBce(a, logits[try plan.outputIndex(.classification, 0)], targets.classification_targets, targets.classification_mask, plan.config.head.classification_loss_weight, loss_limits);
        try work.charge(class_loss.?.work);
        terms.classification = @floatCast(class_loss.?.value);
        terms.total += terms.classification;
    }
    if (plan.records.len != 0) {
        records_loss = try recordObjectives(plan, a, &pool.?, &targets, logits, &work, context.decision_observer);
        terms.record_object = records_loss.?.object_loss;
        terms.record_field = records_loss.?.field_loss;
        terms.total += records_loss.?.value * plan.config.head.record_loss_weight;
        coverage.matched_records = records_loss.?.matched_record_count;
        decision_hash.update(&records_loss.?.decisions_fingerprint);
    }
    if (relation_geometry) |geometry| {
        loss_limits.max_work = work.available(plan.limits.losses.max_work);
        relations_loss = try objectives.supervisedBce(a, logits[try plan.outputIndex(.relation, 0)], geometry.labels, geometry.mask, plan.config.head.relation_loss_weight, loss_limits);
        try work.charge(relations_loss.?.work);
        terms.relation = @floatCast(relations_loss.?.value);
        terms.total += terms.relation;
        coverage.gold_relations = geometry.gold;
        coverage.proposed_gold_relations = geometry.covered;
        decision_hash.update(&geometry.fingerprint);
    }
    if (!std.math.isFinite(terms.total)) return error.NonFiniteBoundaryTraining;
    const cotangents = try scratch.alloc(ops.CT, plan.outputs.len);
    for (plan.outputs, cotangents) |output, *cotangent| {
        const gradient = switch (output.kind) {
            .start => span_loss.?.gradients.starts,
            .end => span_loss.?.gradients.ends,
            .inside => span_loss.?.gradients.inside,
            .pair => span_loss.?.gradients.pairs,
            .proposal => span_loss.?.gradients.proposals,
            .abstention => span_loss.?.gradients.nulls.?,
            .count => span_loss.?.gradients.counts.?,
            .classification => class_loss.?.gradient,
            .record_object => records_loss.?.gradients[output.group].objects,
            .record_assignment => records_loss.?.gradients[output.group].assignments,
            .relation => relations_loss.?.gradient,
        };
        if (output.kind == .record_object or output.kind == .record_assignment) for (gradient) |*value| {
            value.* *= plan.config.head.record_loss_weight;
        };
        try work.charge(gradient.len);
        cotangent.* = try uploads.upload(plan.graph.node(output.node).output_shape, .{ .f32 = gradient });
    }
    const decision_fingerprint = decision_hash.finalResult();
    try tape.sealDecisions(decision_fingerprint);
    var backward = if (plan.recomputation) |*regional| combined: {
        try encoder_tape.?.sealDecisions(decision_fingerprint);
        var head = try tape.backward(tape.identity, decision_fingerprint, terms.total, cotangents, control);
        defer head.deinit(cb);
        var encoder = try encoder_tape.?.backward(identity, decision_fingerprint, terms.total, try recomputed_execution.hiddenCotangent(&regional.graph, &head), control);
        defer encoder.deinit(cb);
        break :combined try recomputed_execution.merge(plan.allocator, cb, &regional.graph, &head, &encoder, base_options.execution, base_options.resident.program.instruction.primitive, control);
    } else try tape.backward(tape.identity, decision_fingerprint, terms.total, cotangents, control);
    errdefer backward.deinit(cb);
    try io.recordControl(backward.control_readback_bytes);
    const presence = try plan.allocator.alloc(Presence, plan.selected_parameters.len);
    errdefer plan.allocator.free(presence);
    const class_supervision = std.mem.indexOfScalar(bool, targets.classification_mask, true) != null;
    for (plan.selected_parameters, presence) |parameter, *out| {
        const name = canonicalParameterName(plan.graph.parameterName(plan.graph.node(parameter)));
        const reached = std.mem.indexOfScalar(Id, backward.parameter_ids, parameter) != null;
        var kind: GradientPresence = if (reached) .computed else .absent;
        if (!class_supervision and std.mem.startsWith(u8, name, "classifier.")) kind = .absent;
        if (q == 0 and !class_supervision) kind = .absent;
        if (kind == .absent and isTouchParameter(name, plan.config.head)) kind = .computed_zero;
        out.* = .{ .parameter = parameter, .kind = kind };
    }
    try work.charge(0);
    return .{ .allocator = plan.allocator, .terms = terms, .backward = backward, .presence = presence, .plan_fingerprint = plan.fingerprint, .target_fingerprint = target_fingerprint, .decision_fingerprint = decision_fingerprint, .dropout_fingerprint = masks.fingerprint, .coverage = coverage, .host_peak_bytes = 0, .transfers = io.diagnostics };
}

test "boundary training step control preserves independent callbacks cancellation deadlines and worker carriers" {
    const execution = @import("../execution_control.zig");
    const Probe = struct {
        calls: usize = 0,
        cancelled: bool = false,
        failure: ?anyerror = null,
        monitor: ?execution.MonitorControl = null,
        arms: usize = 0,
        disarms: usize = 0,
        progress: usize = 0,

        fn checkControl(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.failure) |err| return err;
        }
        fn isCancelled(raw: ?*anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.cancelled;
        }
        fn arm(raw: *anyopaque, monitor: execution.MonitorControl) anyerror!u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.monitor = monitor;
            self.arms += 1;
            return 7;
        }
        fn disarm(raw: *anyopaque, token: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(token == 7 and self.monitor != null);
            self.monitor = null;
            self.disarms += 1;
        }
        fn update(raw: ?*anyopaque, _: execution.Progress) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.progress += 1;
        }
    };
    var original = Probe{};
    var request = Probe{};
    const future = std.math.maxInt(u64);
    var checks = CombinedControl{
        .original = .{
            .io = std.testing.io,
            .deadline_ns = future - 2,
            .ptr = &original,
            .check_fn = Probe.checkControl,
            .cancellation = .{ .ptr = &original, .is_cancelled_fn = Probe.isCancelled },
            .progress = .{ .ptr = &original, .update_fn = Probe.update },
            .hard_cancellation = .{ .ptr = &original, .arm_fn = Probe.arm, .disarm_fn = Probe.disarm },
        },
        .request = .{
            .deadline_ns = future - 1,
            .ptr = &request,
            .check_fn = Probe.checkControl,
            .cancellation = .{ .ptr = &request, .is_cancelled_fn = Probe.isCancelled },
        },
    };
    var active = checks.value();
    try std.testing.expectEqual(std.testing.io, active.io.?);
    try std.testing.expectEqual(future - 2, active.deadline_ns.?);
    try active.update(.executing, 0, 1);
    try std.testing.expectEqual(@as(usize, 1), original.calls);
    try std.testing.expectEqual(@as(usize, 1), request.calls);
    try std.testing.expectEqual(@as(usize, 1), original.progress);
    {
        // Use the real guard/monitor API without a thread or process. The
        // monitor must retain both full controls until its guard is disarmed.
        var guard = try active.enterUninterruptible(.process_required);
        defer guard.deinit();
        const monitor = original.monitor.?;
        try std.testing.expectEqual(future - 2, monitor.deadline_ns.?);
        try monitor.check();
        original.failure = error.OriginalStepControlStopped;
        try std.testing.expectError(error.OriginalStepControlStopped, monitor.check());
        original.failure = null;
        request.failure = error.RequestStepControlStopped;
        try std.testing.expectError(error.RequestStepControlStopped, monitor.check());
        request.failure = null;
        original.cancelled = true;
        try std.testing.expectError(error.Cancelled, monitor.check());
        original.cancelled = false;
        request.cancelled = true;
        try std.testing.expectError(error.Cancelled, monitor.check());
        request.cancelled = false;
    }
    try std.testing.expectEqual(@as(usize, 1), original.arms);
    try std.testing.expectEqual(original.arms, original.disarms);

    // Distinct request carriers take precedence. This IO value is compared
    // only; no operation is dispatched through its deliberately distinct tag.
    checks.request.?.io = .{ .userdata = &request, .vtable = std.testing.io.vtable };
    checks.request.?.hard_cancellation = .{ .ptr = &request, .arm_fn = Probe.arm, .disarm_fn = Probe.disarm };
    checks.request.?.progress = .{ .ptr = &request, .update_fn = Probe.update };
    checks.request.?.deadline_ns = future - 3;
    active = checks.value();
    try std.testing.expectEqual(checks.request.?.io.?, active.io.?);
    try std.testing.expectEqual(future - 3, active.deadline_ns.?);
    try active.update(.executing, 1, 1);
    try std.testing.expectEqual(@as(usize, 1), request.progress);
    {
        var guard = try active.enterUninterruptible(.process_required);
        defer guard.deinit();
        try request.monitor.?.check();
    }
    try std.testing.expectEqual(@as(usize, 1), request.arms);
    try std.testing.expectEqual(request.arms, request.disarms);
    checks.original.?.deadline_ns = 0;
    active = checks.value();
    try std.testing.expectEqual(@as(u64, 0), active.deadline_ns.?);
    try std.testing.expectError(error.Timeout, active.check());

    checks.original = null;
    checks.request = null;
    active = checks.value();
    try active.check();
    try std.testing.expectError(error.ProcessIsolationRequired, active.enterUninterruptible(.process_required));
}

test "boundary training step terminal allocation cause survives resize misses and nested owner limits" {
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var aggregate: HostOwner = undefined;
    aggregate.init(backing.allocator(), 64);
    defer std.debug.assert(aggregate.budget.live == 0);
    var request: HostOwner = undefined;
    request.init(aggregate.budget.allocator(), 32);
    defer std.debug.assert(request.budget.live == 0);
    const a = request.budget.allocator();
    const bytes = try a.alloc(u8, 8);
    defer a.free(bytes);
    try std.testing.expect(!a.resize(bytes, 33));
    try std.testing.expect(a.remap(bytes, 33) == null);
    try std.testing.expect(!aggregate.budget.allocator().resize(bytes, 65));
    try std.testing.expect(request.budget.denied and aggregate.budget.denied);
    try std.testing.expect(request.failure == null and aggregate.failure == null);

    backing.fail_index = backing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 1));
    try std.testing.expectEqual(.backing_allocator, request.failure.?.kind);
    try std.testing.expectEqual(.backing_allocator, aggregate.failure.?.kind);
    try std.testing.expectEqual(error.OutOfMemory, aggregate.translate(request.translate(error.OutOfMemory)));

    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 33));
    try std.testing.expectEqual(.declared_limit, request.failure.?.kind);
    try std.testing.expectEqual(error.BoundaryTrainingStepLimitExceeded, aggregate.translate(request.translate(error.OutOfMemory)));
    // A later terminal backing failure replaces a prior declared failure.
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 1));
    try std.testing.expectEqual(error.OutOfMemory, aggregate.translate(request.translate(error.OutOfMemory)));

    aggregate.budget.limit = aggregate.budget.live;
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 1));
    try std.testing.expectEqual(.backing_allocator, request.failure.?.kind);
    try std.testing.expectEqual(.declared_limit, aggregate.failure.?.kind);
    try std.testing.expectEqual(error.BoundaryTrainingStepLimitExceeded, aggregate.translate(request.translate(error.OutOfMemory)));
    try std.testing.expectEqual(error.Cancelled, aggregate.translate(request.translate(error.Cancelled)));

    aggregate.budget.limit = 64;
    aggregate.resetFailure();
    request.resetFailure();
    try std.testing.expect(request.budget.denied and aggregate.budget.denied);
    try std.testing.expect(request.failure == null and aggregate.failure == null);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 1));
    try std.testing.expectEqual(error.OutOfMemory, aggregate.translate(request.translate(error.OutOfMemory)));
}

test "boundary training step cached plan runs compose host checks and reset terminal allocation history" {
    const fixture = @import("gliner_boundary_train_step_test.zig");
    const native = @import("../ops/native_compute.zig");
    const Probe = struct {
        calls: usize = 0,
        failure: ?anyerror = null,
        fn checkControl(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls >= 2) if (self.failure) |err| return err;
        }
    };
    const a = std.testing.allocator;
    var schema = try schema_mod.compile(a, "{\"classifications\":[{\"name\":\"intent\",\"labels\":[\"yes\",\"no\"]}]}", .{});
    defer schema.deinit();
    var tokenizer = fixture.TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "Ada", .schema = &schema }}, .{});
    defer prepared.deinit();
    // Both the owner allocation and its first child allocation unwind cleanly.
    for (0..2) |fail_index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
        try std.testing.expectError(error.OutOfMemory, build(failing.allocator(), fixture.config(), &prepared, &.{&schema}, .{}, .training, .{}));
    }
    var backing = std.testing.FailingAllocator.init(a, .{});
    var plan = try build(backing.allocator(), fixture.config(), &prepared, &.{&schema}, .{}, .training, .{});
    defer plan.deinit();
    try std.testing.expectEqual(&plan.host_owner.budget, plan.budget);
    try std.testing.expectEqual(@as(?*anyopaque, @ptrCast(plan.host_owner)), plan.budget.failure_context);
    const selected = for (plan.graph.parameters.items) |id| {
        if (std.mem.startsWith(u8, plan.graph.parameterName(plan.graph.node(id)), "classifier.")) break id;
    } else return error.TestExpectedClassifierParameter;
    try plan.finalize(&.{selected}, .{});
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    var original = Probe{ .failure = error.OriginalStepControlStopped };
    var requested = Probe{};
    cb.execution_control = .{ .ptr = &original, .check_fn = Probe.checkControl };
    const request = Control{ .ptr = &requested, .check_fn = Probe.checkControl };
    const context = StepContext{ .identity = .{ .binding = @splat(1), .optimizer_step = 0, .microbatch = 0 }, .replay = .{ .seed = 1, .micro_batch = 0 }, .progress = .{ .optimizer_step = 0, .total_optimizer_steps = 1 } };
    const annotations = [_]targets_mod.Annotations{.{ .schema_fingerprint = schema.fingerprint, .classifications = &.{.{ .task = 0, .labels = &.{0} }} }};
    const live = plan.budget.live;
    // The first check passes. Cancellation must also reach runStep's host
    // work before any tensor binding or encoder execution is attempted.
    try std.testing.expectError(error.OriginalStepControlStopped, plan.run(&cb, &.{}, &prepared, &.{&schema}, &annotations, context, request));
    try std.testing.expectEqual(@as(usize, 2), original.calls);
    try std.testing.expectEqual(@as(usize, 1), requested.calls);
    try std.testing.expectEqual(live, plan.budget.live);
    original = .{};
    requested = .{ .failure = error.RequestStepControlStopped };
    try std.testing.expectError(error.RequestStepControlStopped, plan.run(&cb, &.{}, &prepared, &.{&schema}, &annotations, context, request));
    try std.testing.expectEqual(@as(usize, 2), original.calls);
    try std.testing.expectEqual(@as(usize, 2), requested.calls);
    try std.testing.expectEqual(live, plan.budget.live);
    // The backend itself remains unchanged and no cached callback points into
    // either returned run frame.
    try std.testing.expectEqual(@as(?*anyopaque, @ptrCast(&original)), cb.execution_control.?.ptr);

    cb.execution_control = null;
    const total_limit = plan.budget.limit;
    plan.budget.limit = live;
    try std.testing.expectError(error.BoundaryTrainingStepLimitExceeded, plan.run(&cb, &.{}, &prepared, &.{&schema}, &annotations, context, null));
    try std.testing.expectEqual(.declared_limit, plan.host_owner.failure.?.kind);
    try std.testing.expectEqual(live, plan.budget.live);
    plan.budget.limit = total_limit;
    backing.fail_index = backing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, plan.run(&cb, &.{}, &prepared, &.{&schema}, &annotations, context, null));
    try std.testing.expectEqual(.backing_allocator, plan.host_owner.failure.?.kind);
    try std.testing.expect(plan.budget.denied);
    try std.testing.expectEqual(live, plan.budget.live);

    const step_limit = plan.limits.max_step_host_bytes;
    plan.limits.max_step_host_bytes = 1;
    try std.testing.expectError(error.BoundaryTrainingStepLimitExceeded, plan.run(&cb, &.{}, &prepared, &.{&schema}, &annotations, context, null));
    try std.testing.expect(plan.host_owner.failure == null);
    plan.limits.max_step_host_bytes = step_limit;
    try std.testing.expectError(error.OutOfMemory, plan.run(&cb, &.{}, &prepared, &.{&schema}, &annotations, context, null));
    try std.testing.expectEqual(live, plan.budget.live);
}

test "boundary training step construction methods classify terminal causes and preserve invalidation" {
    const fixture = @import("gliner_boundary_train_step_test.zig");
    const a = std.testing.allocator;
    var schema = try schema_mod.compile(a, "{\"classifications\":[{\"name\":\"intent\",\"labels\":[\"yes\",\"no\"]}]}", .{});
    defer schema.deinit();
    var tokenizer = fixture.TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "Ada", .schema = &schema }}, .{});
    defer prepared.deinit();
    for ([_]bool{ false, true }) |peft| for ([_]bool{ false, true }) |declared| {
        var backing = std.testing.FailingAllocator.init(a, .{});
        var plan = try build(backing.allocator(), fixture.config(), &prepared, &.{&schema}, .{}, .training, .{});
        defer plan.deinit();
        const selected = for (plan.graph.parameters.items) |id| {
            if (std.mem.startsWith(u8, plan.graph.parameterName(plan.graph.node(id)), "classifier.")) break id;
        } else return error.TestExpectedClassifierParameter;
        const live = plan.budget.live;
        try std.testing.expectError(error.OutOfMemory, plan.allocator.alloc(u8, plan.budget.limit + 1));
        try std.testing.expectEqual(.declared_limit, plan.host_owner.failure.?.kind);
        if (declared) plan.budget.limit = live else backing.fail_index = backing.alloc_index;
        const expected = if (declared) error.BoundaryTrainingStepLimitExceeded else error.OutOfMemory;
        if (peft) {
            try std.testing.expectError(expected, plan.applyPeft(.{ .rank = 2, .alpha = 3, .dropout = 0 }, .{}));
        } else {
            try std.testing.expectError(expected, plan.finalize(&.{selected}, .{}));
        }
        try std.testing.expect(plan.construction_failed);
        try std.testing.expectEqual(live, plan.budget.live);
        try std.testing.expectError(error.BoundaryTrainingPlanInvalidated, plan.finalize(&.{selected}, .{}));
        try std.testing.expect(plan.host_owner.failure == null);
    };
}

test {
    _ = @import("gliner_boundary_train_step_test.zig");
    _ = @import("gliner_boundary_train_step_oracle_test.zig");
    _ = @import("gliner_boundary_train_step_device_test.zig");
    _ = objectives;
    _ = decisions;
}
