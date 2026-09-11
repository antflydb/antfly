// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Complete mixed-task resident Step proof against both the pinned Python
//! fixture and the native Step. Frozen and updated weights are fixture inputs;
//! GPU optimizer execution and durable GPU resume are separate qualifications.
const std = @import("std");
const ml = @import("ml").graph;
const build_options = @import("build_options");
const step = @import("gliner_boundary_train_step.zig");
const objective = @import("gliner_boundary_train_objectives.zig");
const decision_events = @import("gliner_boundary_training_decisions.zig");
const decision_trace = @import("gliner_boundary_training_decisions_test.zig");
const reference_mod = @import("gliner_boundary_train_step_oracle_test.zig");
const seeded = @import("../graph/seeded_training.zig");
const interpreter = @import("../graph/interpreter.zig");
const resident_fixture = @import("../graph/resident_training_fixture.zig");
const fixture = @import("../architectures/gliner_boundary_parity_test.zig");
const ops = @import("../ops/ops.zig");
const native = @import("../ops/native_compute.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Allocator = std.mem.Allocator;

const Profiles = struct {
    attention: step.AttentionProfile = .materialized_v1,
    activation: step.ActivationProfile = .retained_v1,
};

/// Test-owned fixtures and diagnostics remain host values. Native comparison
/// tensors and physical Metal buffers are separate backend payload owners.
/// No optimizer is constructed: both initial and post-update weights are
/// immutable source-fixture inputs, as in the original retained tests.
const RegionalOwners = struct {
    host: Budget,
    native_backend: Budget,
    metal_host: Budget,

    const caller_bytes = 32 * 1024 * 1024;
    const metal_host_bytes = 128 * 1024 * 1024;
    const Reservation = struct { future_host_bytes: usize, future_backend_bytes: usize };

    fn init(self: *@This()) void {
        const limits = step.Limits{};
        self.* = .{
            .host = .{ .backing = std.testing.allocator, .limit = limits.recomputation.max_host_bytes },
            .native_backend = .{ .backing = std.testing.allocator, .limit = limits.recomputation.max_backend_bytes },
            .metal_host = undefined,
        };
        self.metal_host = .{ .backing = self.host.allocator(), .limit = metal_host_bytes };
    }

    fn fixedHost(self: *@This(), caller: *const Budget) !usize {
        if (caller.live > self.host.live or self.metal_host.live > self.host.live - caller.live) return error.InvalidRecomputeAdmission;
        return self.host.live - caller.live - self.metal_host.live;
    }

    fn backendLive(self: *const @This()) !usize {
        const device = std.math.cast(usize, metal_tensor.memoryStatsSnapshot().device_owned_live_bytes) orelse return error.InvalidRecomputeAdmission;
        return std.math.add(usize, self.native_backend.live, device);
    }

    fn seal(self: *@This(), plan: *step.Plan, caller: *const Budget) !Reservation {
        const regional = &plan.recomputation.?.graph.regional;
        _ = try plan.recomputedHeadAdmission();
        const host_live = try self.fixedHost(caller);
        if (regional.budget.live > host_live) return error.InvalidRecomputeAdmission;
        const backend_live = try self.backendLive();
        // Replace only this region's current compiler allocation with its
        // complete cap. Reserve the full caller/Metal metadata owners, not
        // their early live sizes. All other compiled plans, loaded weights,
        // and an already-completed native result remain in the live census.
        const fixed_host = try std.math.add(usize, host_live - regional.budget.live, try std.math.add(usize, caller.limit, self.metal_host.limit));
        const admitted = try plan.sealRecomputedAdmission(.{
            .fixed_host_bytes = fixed_host,
            .fixed_backend_bytes = backend_live,
            .head_host_metadata_bytes = plan.limits.max_step_host_bytes,
            .head_work = plan.limits.max_step_work,
        });
        try std.testing.expect(admitted.host_upper_bound_bytes >= host_live);
        try std.testing.expect(admitted.backend_upper_bound_bytes >= backend_live);
        try std.testing.expect(admitted.work <= plan.limits.recomputation.max_total_work);
        const result = Reservation{
            .future_host_bytes = admitted.host_upper_bound_bytes - host_live,
            .future_backend_bytes = admitted.backend_upper_bound_bytes - backend_live,
        };
        try self.check(caller, result);
        return result;
    }

    fn check(self: *@This(), caller: *const Budget, reservation: Reservation) !void {
        const host = try std.math.add(usize, try self.fixedHost(caller), reservation.future_host_bytes);
        const backend = try std.math.add(usize, try self.backendLive(), reservation.future_backend_bytes);
        if (host > self.host.limit or backend > self.native_backend.limit) return error.RecomputeLimitExceeded;
    }
};

const ObservedDecisions = struct {
    capture: *decision_trace.Capture,
    pool: usize = 0,
    relations: usize = 0,
    boundary_loss: usize = 0,
    records: usize = 0,

    fn observer(self: *@This()) decision_events.Observer {
        return .{ .context = self, .observe = observe };
    }
    fn observe(raw: *anyopaque, event: decision_events.Event) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        switch (event) {
            .pool => self.pool += 1,
            .relations => self.relations += 1,
            .boundary_loss => self.boundary_loss += 1,
            .record => self.records += 1,
        }
        const capture = self.capture.observer();
        try capture.observe(capture.context, event);
    }
    fn finish(self: *const @This(), plan: *const step.Plan) !void {
        try std.testing.expectEqual(@as(usize, @intFromBool(plan.pool_input != null)), self.pool);
        try std.testing.expectEqual(@as(usize, @intFromBool(plan.relation_input != null)), self.relations);
        try std.testing.expectEqual(@as(usize, @intFromBool(plan.pool_input != null)), self.boundary_loss);
        try std.testing.expectEqual(plan.records.len, self.records);
        try self.capture.finish();
    }
};

fn preparePlan(a: Allocator, config: @import("../models/gliner_boundary.zig").Config, prepared: *const reference_mod.Prepared, mode: reference_mod.Mode, mapped: []const reference_mod.MappedParameter, dropout: f32, execution: seeded.Execution, profiles: Profiles) !step.Plan {
    var plan = try step.buildWithProfiles(a, config, &prepared.batch, prepared.pointers, .{}, .training, profiles.attention, profiles.activation, .{});
    errdefer plan.deinit();
    if (mode == .lora or mode == .dora) try plan.applyPeft(.{ .kind = if (mode == .dora) .dora else .lora, .rank = 2, .alpha = 3, .dropout = dropout, .targets = &.{"encoder"} }, .{});
    var wrt = std.ArrayListUnmanaged(ml.NodeId).empty;
    defer wrt.deinit(a);
    for (plan.graph.parameters.items) |id| {
        const name = plan.graph.parameterName(plan.graph.node(id));
        if (std.mem.startsWith(u8, name, "__")) continue;
        const parameter = try reference_mod.findParameter(mapped, name);
        const shape = plan.graph.node(id).output_shape;
        try std.testing.expectEqual(parameter.source.shape.len, shape.rank());
        for (parameter.source.shape, shape.dims[0..shape.rank()]) |expected, actual| try std.testing.expectEqual(@as(i64, expected), actual);
        if (parameter.source.trainable) try wrt.append(a, id);
    }
    // Apply PEFT once, before regional or head differentiation/remapping.
    try plan.finalizeWithActivationProfile(wrt.items, .{ .execution = execution }, profiles.activation);
    try std.testing.expectEqual(profiles.attention, plan.encoder.attention_profile);
    try std.testing.expectEqual(profiles.activation, plan.encoder.activation_profile);
    if (profiles.activation == .layer_recompute_v1) {
        const regional = &plan.recomputation.?.graph.regional;
        try std.testing.expectEqual(@as(usize, config.encoder.num_hidden_layers) + 1, regional.regions.len);
        try std.testing.expectEqual(mode != .heads, regional.needsBackward());
        const replay = try regional.replaySummary();
        try std.testing.expectEqual(mode != .heads, replay.replay_regions != 0);
    } else try std.testing.expect(plan.recomputation == null);
    return plan;
}

fn weightKey(profile: reference_mod.Profile, parameter: reference_mod.MappedParameter, optimizer_step: u64) ![]const u8 {
    if (!parameter.source.trainable or optimizer_step == 0) return parameter.initial;
    for (profile.flushes) |flush| if (flush.optimizer_step == optimizer_step) {
        return (flush.parameters.map.get(parameter.source.name) orelse return error.MissingFixtureParameter).weight;
    };
    return error.MissingFixtureOptimizerStep;
}

const Parameters = struct {
    allocator: Allocator,
    cb: *const ops.ComputeBackend,
    inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty,
    upload_bytes: usize = 0,

    fn init(a: Allocator, cb: *const ops.ComputeBackend, plan: *const step.Plan, profile: reference_mod.Profile, mapped: []const reference_mod.MappedParameter, reference: *const fixture.TensorFixture, optimizer_step: u64) !Parameters {
        var result = Parameters{ .allocator = a, .cb = cb };
        errdefer result.deinit();
        for (plan.graph.parameters.items) |id| {
            const name = plan.graph.parameterName(plan.graph.node(id));
            if (std.mem.startsWith(u8, name, "__")) continue;
            const parameter = try reference_mod.findParameter(mapped, name);
            const values = try reference.floats(try weightKey(profile, parameter, optimizer_step));
            const value = if (cb.kind() == .metal)
                try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = parameter.source.shape } }, .{})
            else
                try cb.fromFloat32Shape(values, parameter.source.shape);
            errdefer cb.free(value);
            try result.inputs.append(a, .{ .node_id = id, .value = value });
            result.upload_bytes += values.len * @sizeOf(f32);
        }
        return result;
    }
    fn deinit(self: *Parameters) void {
        for (self.inputs.items) |input| self.cb.free(input.value);
        self.inputs.deinit(self.allocator);
    }
};

/// This is an explicit diagnostic gradient download after Step has finished.
/// The production Step only returns resident gradients and presence metadata.
fn gradientFor(a: Allocator, cb: *const ops.ComputeBackend, plan: *const step.Plan, result: *const step.StepResult, name: []const u8) !?[]f32 {
    for (result.presence) |presence| {
        if (!std.mem.eql(u8, name, plan.graph.parameterName(plan.graph.node(presence.parameter)))) continue;
        if (presence.kind == .absent) return null;
        const count: usize = @intCast(plan.graph.node(presence.parameter).output_shape.numElements().?);
        for (result.backward.parameter_ids, result.backward.gradients.outputs) |id, value| if (id == presence.parameter) {
            if (cb.kind() != .metal) return try cb.toFloat32(value, a);
            const output = try a.alloc(f32, count);
            errdefer a.free(output);
            try cb.glinerBoundaryDownload(value, output);
            return output;
        };
        if (presence.kind != .computed_zero) return error.MissingTrainingGradient;
        const output = try a.alloc(f32, count);
        @memset(output, 0);
        return output;
    }
    return null;
}

fn compareResults(a: Allocator, cb: *const ops.ComputeBackend, cpu: *const ops.ComputeBackend, plan: *const step.Plan, cpu_plan: *const step.Plan, device: *const step.StepResult, host: *const step.StepResult, mapped: []const reference_mod.MappedParameter, reference: *const fixture.TensorFixture, micro: reference_mod.Microbatch, mode: reference_mod.Mode, index: usize, transported_relations: bool) !void {
    try reference_mod.checkTerms(a, device.terms, micro);
    try reference_mod.checkTerms(a, host.terms, micro);
    inline for (std.meta.fields(objective.Terms)) |field| {
        errdefer std.debug.print("resident/native composed term {s}\n", .{field.name});
        try fixture.expectFloats(&.{@field(host.terms, field.name)}, &.{@field(device.terms, field.name)}, 1e-3, 2e-5);
    }
    try std.testing.expectEqual(host.coverage, device.coverage);
    try std.testing.expectEqualSlices(u8, &host.plan_fingerprint, &device.plan_fingerprint);
    try std.testing.expectEqualSlices(u8, &host.target_fingerprint, &device.target_fingerprint);
    // Replay seals include raw detached inside-mean bits. Actual discrete
    // choices are compared through the borrowed observer below instead.
    // Relation masks are identical per semantic pair. Their physical row
    // permutation is part of each backend's own retained input fingerprint.
    if (!transported_relations) try std.testing.expectEqual(host.dropout_fingerprint, device.dropout_fingerprint);
    try std.testing.expectEqual(host.presence.len, device.presence.len);
    for (host.presence, device.presence) |want, actual| {
        try std.testing.expectEqual(want.parameter, actual.parameter);
        try std.testing.expectEqual(want.kind, actual.kind);
    }
    var absent: usize = 0;
    var trainable: usize = 0;
    for (mapped) |parameter| {
        if (!parameter.source.trainable) continue;
        trainable += 1;
        errdefer std.debug.print("resident composed gradient {s}\n", .{parameter.source.name});
        const expected = micro.gradients.map.get(parameter.source.name) orelse return error.MissingFixtureParameter;
        const actual = try gradientFor(a, cb, plan, device, parameter.name);
        defer if (actual) |value| a.free(value);
        const native_value = try gradientFor(a, cpu, cpu_plan, host, parameter.name);
        defer if (native_value) |value| a.free(value);
        try std.testing.expectEqual(expected != null, actual != null);
        try std.testing.expectEqual(expected != null, native_value != null);
        if (expected) |key| {
            const pinned = try reference.floats(key);
            try fixture.expectFloats(pinned, actual.?, 5e-4, 8e-4);
            try fixture.expectFloats(pinned, native_value.?, 5e-4, 8e-4);
            try fixture.expectFloats(native_value.?, actual.?, 5e-4, 8e-4);
            // A connected zero stays a zero tensor, separate from grad=None.
            if (std.mem.allEqual(f32, pinned, 0)) for (actual.?) |value| try std.testing.expectEqual(@as(f32, 0), value);
        } else absent += 1;
    }
    try std.testing.expectEqual(micro.gradients.map.count(), trainable);
    if (index != 0 and (mode == .full or mode == .heads)) try std.testing.expectEqual(@as(usize, 34), absent);
}

const TransportedMasks = struct {
    allocator: Allocator,
    masks: []step.DropoutMask,
    values: []f32,
    fn init(a: Allocator, original: []const step.DropoutMask, captured: *const decision_trace.Capture) !TransportedMasks {
        const source_rows = captured.relation_source_rows orelse return error.MissingFixtureRelationPermutation;
        if (source_rows.len == 0) return error.InvalidFixtureRelationPermutation;
        const index = for (original, 0..) |mask, i| {
            if (std.mem.endsWith(u8, mask.name, ".relations.hidden")) break i;
        } else return error.MissingFixtureRelationDropout;
        const source = original[index].values;
        if (source.len == 0 or source.len % source_rows.len != 0) return error.InvalidFixtureRelationDropout;
        const width = source.len / source_rows.len;
        const masks = try a.dupe(step.DropoutMask, original);
        errdefer a.free(masks);
        const values = try a.dupe(f32, source);
        errdefer a.free(values);
        for (source_rows, 0..) |row, native_row| {
            if (row == std.math.maxInt(usize)) continue;
            if (row >= source_rows.len) return error.InvalidFixtureRelationPermutation;
            @memcpy(values[native_row * width ..][0..width], source[row * width ..][0..width]);
        }
        masks[index].values = values;
        // Every other site retains the original captured tensor, unchanged.
        for (masks, original, 0..) |actual, expected, i| if (i != index) try std.testing.expectEqual(expected.values.ptr, actual.values.ptr);
        return .{ .allocator = a, .masks = masks, .values = values };
    }
    fn deinit(self: *TransportedMasks) void {
        self.allocator.free(self.masks);
        self.allocator.free(self.values);
    }
};

fn runOracle(mode: reference_mod.Mode, directory: []const u8) !void {
    return runOracleWithProfiles(mode, directory, .{});
}

fn runOracleWithProfiles(mode: reference_mod.Mode, directory: []const u8, profiles: Profiles) !void {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    var owners: RegionalOwners = undefined;
    owners.init();
    defer std.debug.assert(owners.host.live == 0 and owners.native_backend.live == 0 and owners.metal_host.live == 0);
    const regional = profiles.activation == .layer_recompute_v1;
    const a = if (regional) owners.host.allocator() else std.testing.allocator;
    const native_allocator = if (regional) owners.native_backend.allocator() else a;
    const metal_allocator = if (regional) owners.metal_host.allocator() else a;
    const capture_path = try std.fmt.allocPrint(a, "{s}/capture.json", .{directory});
    defer a.free(capture_path);
    const bytes = try fixture.fixtureBytes(a, capture_path);
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(reference_mod.Oracle, a, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    var decisions = try std.json.parseFromSlice(struct { profiles: []struct { mode: reference_mod.Mode, microbatches: []decision_trace.Source } }, a, bytes, .{ .ignore_unknown_fields = true });
    defer decisions.deinit();
    const oracle = parsed.value;
    try std.testing.expectEqual(@as(u32, 1), oracle.format_version);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", oracle.source_commit);
    try std.testing.expect(!oracle.qualification);
    const tensor_path = try std.fmt.allocPrint(a, "{s}/tensors.safetensors", .{directory});
    defer a.free(tensor_path);
    var reference = try fixture.TensorFixture.init(a, tensor_path);
    defer reference.deinit();
    try std.testing.expectEqual(oracle.tensors.size_bytes, reference.reader.file_bytes.len);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(reference.reader.file_bytes, &hash, .{});
    try std.testing.expectEqualStrings(oracle.tensors.sha256, &std.fmt.bytesToHex(hash, .lower));
    const profile = for (oracle.profiles) |value| {
        if (value.mode == mode) break value;
    } else return error.MissingFixtureProfile;
    const decision_profile = for (decisions.value.profiles) |value| {
        if (value.mode == mode) break value;
    } else return error.MissingFixtureProfile;
    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 1 }, oracle.sequence);
    try std.testing.expectEqual(@as(usize, 3), profile.microbatches.len);
    try std.testing.expectEqual(profile.microbatches.len, decision_profile.microbatches.len);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const mapped = try reference_mod.mappedParameters(arena.allocator(), profile);
    const config = try reference_mod.configure(&reference, oracle, profile, mapped);
    var tokenizer = reference_mod.FragmentTokenizer{ .fragments = &oracle.tokenizer_fragments };
    var prepared: [2]?reference_mod.Prepared = .{ null, null };
    defer for (&prepared) |*value| if (value.*) |*item| item.deinit();
    var plans: [2]?step.Plan = .{ null, null };
    defer for (&plans) |*value| if (value.*) |*item| item.deinit();
    var cpu_plans: [2]?step.Plan = .{ null, null };
    defer for (&cpu_plans) |*value| if (value.*) |*item| item.deinit();
    var reservations: [2]?RegionalOwners.Reservation = .{ null, null };
    var cpu_reservations: [2]?RegionalOwners.Reservation = .{ null, null };
    for (oracle.batches, 0..) |batch, i| {
        prepared[i] = try reference_mod.Prepared.init(a, batch, &tokenizer);
        const dropout = if (oracle.dropout) |value| value.peft_probability else 0;
        plans[i] = try preparePlan(a, config, &prepared[i].?, mode, mapped, dropout, .resident_metal, profiles);
        cpu_plans[i] = try preparePlan(a, config, &prepared[i].?, mode, mapped, dropout, .native, profiles);
    }
    // Compile all four heads before taking any live admission census.
    if (regional) {
        for (&plans) |*value| _ = try value.*.?.recomputedHeadAdmission();
        for (&cpu_plans) |*value| _ = try value.*.?.recomputedHeadAdmission();
    }
    var device = try resident_fixture.Device.init(metal_allocator);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    var store = native.WeightStore{ .allocator = native_allocator, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(native_allocator, &store, null);
    defer compute.deinit();
    const cpu = compute.computeBackend();
    for (profile.microbatches, oracle.sequence, 0..) |micro, batch_index, index| {
        errdefer std.debug.print("resident composed profile {s} fixture {s} microbatch{d} ({s}) attention={s} activation={s}\n", .{ @tagName(mode), directory, index, micro.id, @tagName(profiles.attention), @tagName(profiles.activation) });
        var caller = Budget{ .backing = a, .limit = RegionalOwners.caller_bytes };
        defer std.debug.assert(caller.live == 0);
        const scratch = if (regional) caller.allocator() else a;
        if (index == 2) {
            for (&plans) |*value| try value.*.?.advanceParameterEpoch();
            for (&cpu_plans) |*value| try value.*.?.advanceParameterEpoch();
        }
        const batch = &prepared[batch_index].?;
        const plan = &plans[batch_index].?;
        const cpu_plan = &cpu_plans[batch_index].?;
        try reference_mod.checkPrepared(scratch, &reference, batch, micro);
        const masks = try reference_mod.controlledMasks(scratch, plan, &reference, micro);
        defer if (masks) |owned| scratch.free(owned);
        const context = step.StepContext{
            .identity = .{ .binding = @splat(0x25), .optimizer_step = micro.optimizer_step_before, .microbatch = micro.microbatch },
            .replay = .{ .seed = 253955, .micro_batch = micro.microbatch },
            .progress = .{ .optimizer_step = micro.optimizer_step_before, .total_optimizer_steps = 2 },
            .scales_override = .{ .gold_injection = oracle.optimizer.gold_injection_probability, .consistency = oracle.optimizer.consistency_scale, .soft_iou = oracle.optimizer.soft_iou_scale },
            .negative_query_draws = try reference.floats(micro.outputs.map.get("negative_uniform").?),
            .require_gold_relation_coverage = true,
            .dropout_masks = masks,
        };
        var parameters = try Parameters.init(scratch, &cb, plan, profile, mapped, &reference, micro.optimizer_step_before);
        defer parameters.deinit();
        var cpu_parameters = try Parameters.init(scratch, &cpu, cpu_plan, profile, mapped, &reference, micro.optimizer_step_before);
        defer cpu_parameters.deinit();
        var cpu_decisions = decision_trace.Capture.init(scratch);
        defer cpu_decisions.deinit();
        var cpu_observed = ObservedDecisions{ .capture = &cpu_decisions };
        const active_routes = try scratch.alloc(usize, plan.relations.len);
        defer scratch.free(active_routes);
        for (plan.relations, active_routes) |route, *out| out.* = route.sample * plan.encoder.layout.relations + route.local;
        cpu_decisions.expected = .{ .reference = &reference, .outputs = &micro.outputs, .source = decision_profile.microbatches[index], .words = plan.encoder.layout.words, .batch = plan.encoder.layout.batch, .relations = plan.encoder.layout.relations, .pair_cap = plan.config.head.relation_pair_cap, .active_routes = active_routes };
        var device_decisions = decision_trace.Capture.init(scratch);
        defer device_decisions.deinit();
        var device_observed = ObservedDecisions{ .capture = &device_decisions };
        device_decisions.expected = cpu_decisions.expected;
        var cpu_context = context;
        cpu_context.decision_observer = cpu_observed.observer();
        var device_context = context;
        device_context.decision_observer = device_observed.observer();
        if (regional) {
            if (cpu_reservations[batch_index] == null) cpu_reservations[batch_index] = try owners.seal(cpu_plan, &caller);
            try owners.check(&caller, cpu_reservations[batch_index].?);
        }
        var native_result = try cpu_plan.run(&cpu, cpu_parameters.inputs.items, &batch.batch, batch.pointers, batch.annotations, cpu_context, null);
        var native_result_owned = true;
        defer if (native_result_owned) native_result.deinit(&cpu);
        var cpu_replay_decisions = decision_trace.Capture.init(scratch);
        defer cpu_replay_decisions.deinit();
        var cpu_replay_observed = ObservedDecisions{ .capture = &cpu_replay_decisions };
        cpu_replay_decisions.expected = cpu_decisions.expected;
        const transported_relations = masks != null and plan.relations.len > 0;
        if (transported_relations) {
            // Discover ordering without prescribing a backend's near-tie
            // ranking, then bind exactly the source mask for each pair.
            var transported = try TransportedMasks.init(scratch, masks.?, &cpu_decisions);
            defer transported.deinit();
            native_result.deinit(&cpu);
            native_result_owned = false;
            cpu_context.dropout_masks = transported.masks;
            cpu_context.decision_observer = cpu_replay_observed.observer();
            if (regional) try owners.check(&caller, cpu_reservations[batch_index].?);
            native_result = try cpu_plan.run(&cpu, cpu_parameters.inputs.items, &batch.batch, batch.pointers, batch.annotations, cpu_context, null);
            native_result_owned = true;
            try decision_trace.expectReplay(&cpu_decisions, &cpu_replay_decisions);
            try cpu_replay_observed.finish(cpu_plan);
        }
        if (regional) {
            if (reservations[batch_index] == null) reservations[batch_index] = try owners.seal(plan, &caller);
            try owners.check(&caller, reservations[batch_index].?);
        }
        if (index == 0) {
            try std.testing.expectError(error.UnsupportedSeededTrainingBackend, plan.run(&cpu, cpu_parameters.inputs.items, &batch.batch, batch.pointers, batch.annotations, context, null));
            try std.testing.expectError(error.UnsupportedSeededTrainingBackend, cpu_plan.run(&cb, parameters.inputs.items, &batch.batch, batch.pointers, batch.annotations, context, null));
            const saved_limits = plan.limits.transfers;
            defer plan.limits.transfers = saved_limits;
            const before_admission = metal_tensor.memoryStatsSnapshot();
            plan.limits.transfers.max_readback_bytes = 1;
            try std.testing.expectError(error.BoundaryTrainingTransferLimitExceeded, plan.run(&cb, parameters.inputs.items, &batch.batch, batch.pointers, batch.annotations, context, null));
            plan.limits.transfers = saved_limits;
            plan.limits.transfers.max_upload_bytes = 1;
            try std.testing.expectError(error.BoundaryTrainingTransferLimitExceeded, plan.run(&cb, parameters.inputs.items, &batch.batch, batch.pointers, batch.annotations, context, null));
            const after_admission = metal_tensor.memoryStatsSnapshot();
            try std.testing.expectEqual(before_admission.device_owned_buffers_created, after_admission.device_owned_buffers_created);
            try std.testing.expectEqual(before_admission.to_host_device_calls, after_admission.to_host_device_calls);
        }
        if (mode == .full and index == 0 and std.mem.eql(u8, directory, "training_step")) {
            for ([_]decision_trace.EventKind{ .pool, .record }) |event| {
                var failure = decision_trace.Failure{ .event = event };
                var failed_context = context;
                failed_context.decision_observer = failure.observer();
                const before_failure = metal_tensor.memoryStatsSnapshot();
                if (regional) try owners.check(&caller, reservations[batch_index].?);
                try std.testing.expectError(error.DecisionObserverStopped, plan.run(&cb, parameters.inputs.items, &batch.batch, batch.pointers, batch.annotations, failed_context, null));
                try std.testing.expect(failure.reached);
                const after_failure = metal_tensor.memoryStatsSnapshot();
                try std.testing.expectEqual(before_failure.device_owned_live_bytes, after_failure.device_owned_live_bytes);
            }
        }
        if (regional) try owners.check(&caller, reservations[batch_index].?);
        const before = metal_tensor.memoryStatsSnapshot();
        var result = try plan.run(&cb, parameters.inputs.items, &batch.batch, batch.pointers, batch.annotations, device_context, null);
        var result_owned = true;
        defer if (result_owned) result.deinit(&cb);
        var device_replay_decisions = decision_trace.Capture.init(scratch);
        defer device_replay_decisions.deinit();
        var device_replay_observed = ObservedDecisions{ .capture = &device_replay_decisions };
        device_replay_decisions.expected = device_decisions.expected;
        if (transported_relations) {
            var transported = try TransportedMasks.init(scratch, masks.?, &device_decisions);
            defer transported.deinit();
            result.deinit(&cb);
            result_owned = false;
            device_context.dropout_masks = transported.masks;
            device_context.decision_observer = device_replay_observed.observer();
            if (regional) try owners.check(&caller, reservations[batch_index].?);
            result = try plan.run(&cb, parameters.inputs.items, &batch.batch, batch.pointers, batch.annotations, device_context, null);
            result_owned = true;
            try decision_trace.expectReplay(&device_decisions, &device_replay_decisions);
            try device_replay_observed.finish(plan);
        }
        const after = metal_tensor.memoryStatsSnapshot();
        // Explicit raw head downloads do not create generic host mirrors.
        // No readback of an encoder hidden state or gradient occurs in Step.
        try std.testing.expectEqual(before.to_host_device_calls, after.to_host_device_calls);
        try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
        const admitted = try plan.transferAdmission();
        try std.testing.expect(result.transfers.bytes.upload_bytes > 0);
        try std.testing.expect(result.transfers.bytes.upload_bytes <= admitted.upper.upload_bytes);
        inline for (.{ "proposal_logits", "proposal_features", "loss_logits" }) |field| try std.testing.expectEqual(@field(admitted.upper, field), @field(result.transfers.bytes, field));
        if (regional) {
            // Cotangent summaries are measured. Replay-attention instruction
            // controls have a separately reported upper bound, not a measured
            // byte count. Preserve exact accounting for both categories.
            const instruction_upper = try plan.instructionControlReadbackUpperBound();
            try std.testing.expect(result.transfers.bytes.finite_control > 0);
            try std.testing.expectEqual(admitted.upper.finite_control, try std.math.add(usize, result.transfers.bytes.finite_control, instruction_upper));
            try std.testing.expectEqual(result.backward.control_readback_bytes, result.transfers.bytes.finite_control);
            try std.testing.expect(!plan.recomputation.?.graph.regional.active_tape);
            try std.testing.expect(!cpu_plan.recomputation.?.graph.regional.active_tape);
        } else try std.testing.expectEqual(admitted.upper.finite_control, result.transfers.bytes.finite_control);
        try std.testing.expect(result.transfers.bytes.proposal_features > 0);
        try std.testing.expectEqual(@as(usize, 0), native_result.transfers.bytes.upload_bytes);
        try std.testing.expectEqual(@as(usize, 0), try native_result.transfers.bytes.readbackBytes());
        try cpu_observed.finish(cpu_plan);
        try device_observed.finish(plan);
        try decision_trace.expectEqual(&cpu_decisions, &device_decisions);
        try compareResults(scratch, &cb, &cpu, plan, cpu_plan, &result, &native_result, mapped, &reference, micro, mode, index, transported_relations);
        std.debug.print("resident composed {s}/{s} micro{d}: parameter_upload={d} step_upload={d} proposal_logits={d} proposal_features={d} loss_logits={d} finite_control={d} attention={s} activation={s}\n", .{ directory, @tagName(mode), index, parameters.upload_bytes, result.transfers.bytes.upload_bytes, result.transfers.bytes.proposal_logits, result.transfers.bytes.proposal_features, result.transfers.bytes.loss_logits, result.transfers.bytes.finite_control, @tagName(profiles.attention), @tagName(profiles.activation) });
    }
}

test "resident composed step Metal full pinned and native losses gradients and absent semantics" {
    try runOracle(.full, "training_step");
}
test "resident composed step Metal heads pinned and native losses gradients and absent semantics" {
    try runOracle(.heads, "training_step");
}
test "resident composed step Metal LoRA pinned and native losses gradients and absent semantics" {
    try runOracle(.lora, "training_step");
}
test "resident composed step Metal DoRA pinned and native losses gradients and absent semantics" {
    try runOracle(.dora, "training_step");
}
test "resident composed step Metal controlled dropout full pinned and native gradients" {
    try runOracle(.full, "training_step_dropout");
}
test "resident composed step Metal controlled dropout heads pinned and native gradients" {
    try runOracle(.heads, "training_step_dropout");
}
test "resident composed step Metal controlled dropout LoRA pinned and native gradients" {
    try runOracle(.lora, "training_step_dropout");
}
test "resident composed step Metal controlled dropout DoRA pinned and native gradients" {
    try runOracle(.dora, "training_step_dropout");
}

test "resident regional composed step Metal replay tiled full pinned losses gradients and single head decisions" {
    try runOracleWithProfiles(.full, "training_step", .{ .attention = .replay_tiled_v1, .activation = .layer_recompute_v1 });
}
test "resident regional composed step Metal replay tiled heads pinned losses gradients and single head decisions" {
    try runOracleWithProfiles(.heads, "training_step", .{ .attention = .replay_tiled_v1, .activation = .layer_recompute_v1 });
}
test "resident regional composed step Metal replay tiled LoRA pinned losses gradients and single head decisions" {
    try runOracleWithProfiles(.lora, "training_step", .{ .attention = .replay_tiled_v1, .activation = .layer_recompute_v1 });
}
test "resident regional composed step Metal replay tiled DoRA pinned losses gradients and single head decisions" {
    try runOracleWithProfiles(.dora, "training_step", .{ .attention = .replay_tiled_v1, .activation = .layer_recompute_v1 });
}
test "resident regional composed step Metal controlled dropout full pinned gradients and single head decisions" {
    try runOracleWithProfiles(.full, "training_step_dropout", .{ .activation = .layer_recompute_v1 });
}
test "resident regional composed step Metal controlled dropout heads pinned gradients and single head decisions" {
    try runOracleWithProfiles(.heads, "training_step_dropout", .{ .activation = .layer_recompute_v1 });
}
test "resident regional composed step Metal controlled dropout LoRA pinned gradients and single head decisions" {
    try runOracleWithProfiles(.lora, "training_step_dropout", .{ .activation = .layer_recompute_v1 });
}
test "resident regional composed step Metal controlled dropout DoRA pinned gradients and single head decisions" {
    try runOracleWithProfiles(.dora, "training_step_dropout", .{ .activation = .layer_recompute_v1 });
}
