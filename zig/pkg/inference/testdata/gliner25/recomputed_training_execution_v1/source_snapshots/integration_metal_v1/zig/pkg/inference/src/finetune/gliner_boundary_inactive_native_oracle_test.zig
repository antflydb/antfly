// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Actual NativeTrainer proof against a tiny pinned source epoch. Only the
//! captured step-zero PEFT initialization is injected, through a constructor
//! compiled exclusively for tests. Subsequent weights, VJPs, accumulation and
//! durable restores are produced by the ordinary managed execution path.
const std = @import("std");
const trainer = @import("gliner_boundary_native_trainer.zig");
const synthetic = @import("gliner_boundary_synthetic_adapter_fixture.zig");
const control_oracle = @import("gliner_boundary_inactive_oracle_test.zig");
const previous = @import("gliner_boundary_native_trainer_test.zig");
const controller = @import("seeded_gradient_trainer.zig");
const step = @import("gliner_boundary_train_step.zig");
const data = @import("gliner_boundary_dataset.zig");
const run = @import("gliner_boundary_run.zig");
const objectives = @import("gliner_boundary_train_objectives.zig");
const parity = @import("../architectures/gliner_boundary_parity_test.zig");
const native = @import("../ops/native_compute.zig");
const model = @import("../models/gliner_boundary.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const metal = @import("../backends/metal_runtime.zig");
const tokenizer_mod = @import("inference_tokenizer");
const Allocator = std.mem.Allocator;
const mib = 1024 * 1024;
const directory = "training_inactive_native_epoch/";
const Names = std.json.ArrayHashMap([]const u8);
const Oracle = struct {
    version: u32,
    qualification: bool,
    scope: []const u8,
    source_commit: []const u8,
    config: struct { boundary_head: model.HeadConfig },
    encoder_config: model.EncoderConfig,
    base_parameters: Names,
    tokenizer_fragments: std.json.ArrayHashMap([]const i32),
    optimizer: control_oracle.Optimizer,
    profiles: []const control_oracle.Profile,
};

const Tokens = struct {
    const Added = struct { content: []const u8, id: i32, special: bool };
    fragments: *const std.json.ArrayHashMap([]const i32),
    added: []const Added,
    special: tokenizer_mod.SpecialTokens,
    vocabulary: usize,

    fn declaredId(added: []const Added, name: []const u8) !i32 {
        for (added) |token| if (token.special and std.mem.eql(u8, token.content, name)) return token.id;
        return error.UncapturedTrainingTokenization;
    }
    fn tokenizer(self: *Tokens) tokenizer_mod.Tokenizer {
        return .{ .ptr = self, .vtable = &.{ .encode = encode, .encodeInto = encodeInto, .encodeForModel = undefined, .encodeGeneration = undefined, .decode = undefined, .specialTokens = specialTokens, .allSpecialTokenIds = specialIds, .vocabSize = vocabSize, .deinit = undefined } };
    }
    fn encode(raw: *anyopaque, a: Allocator, text: []const u8) ![]i32 {
        const self: *Tokens = @ptrCast(@alignCast(raw));
        // Marker resolution asks for every declared boundary token, including
        // markers unused by these eight cases. Their exact IDs come from the
        // pinned baseline tokenizer metadata, not a guessed fallback.
        for (self.added) |token| if (token.special and std.mem.eql(u8, token.content, text)) {
            if (self.fragments.map.get(text)) |captured| {
                if (captured.len != 1 or captured[0] != token.id) return error.InvalidInactiveTrainingFixture;
            }
            return a.dupe(i32, &.{token.id});
        };
        const ids = self.fragments.map.get(text) orelse return error.UncapturedTrainingTokenization;
        return a.dupe(i32, ids);
    }
    fn encodeInto(raw: *anyopaque, a: Allocator, text: []const u8, out: *std.ArrayListUnmanaged(i32)) !void {
        const ids = try encode(raw, a, text);
        defer a.free(ids);
        try out.appendSlice(a, ids);
    }
    fn specialTokens(raw: *anyopaque) tokenizer_mod.SpecialTokens {
        const self: *Tokens = @ptrCast(@alignCast(raw));
        return self.special;
    }
    fn specialIds(raw: *anyopaque, a: Allocator) ![]u32 {
        const self: *Tokens = @ptrCast(@alignCast(raw));
        var result = std.ArrayListUnmanaged(u32).empty;
        errdefer result.deinit(a);
        for (self.added) |token| {
            if (!token.special) continue;
            if (token.id < 0 or @as(usize, @intCast(token.id)) >= self.vocabulary) return error.InvalidInactiveTrainingFixture;
            try result.append(a, @intCast(token.id));
        }
        return result.toOwnedSlice(a);
    }
    fn vocabSize(raw: *anyopaque) usize {
        const self: *Tokens = @ptrCast(@alignCast(raw));
        return self.vocabulary;
    }
};

fn integers(reference: *const parity.TensorFixture, names: control_oracle.Names, key: []const u8, actual: []const i64) !void {
    const tensor = try reference.tensor((names.map.get(key) orelse return error.InvalidInactiveTrainingFixture).?);
    if (tensor.dtype != .i64 or tensor.data.len != actual.len * 8) return error.InvalidInactiveTrainingFixture;
    for (actual, 0..) |value, index| try std.testing.expectEqual(std.mem.readInt(i64, tensor.data[index * 8 ..][0..8], .little), value);
}
fn booleans(reference: *const parity.TensorFixture, names: control_oracle.Names, key: []const u8, actual: []const bool) !void {
    const tensor = try reference.tensor((names.map.get(key) orelse return error.InvalidInactiveTrainingFixture).?);
    if ((tensor.dtype != .u8 and tensor.dtype != .bool_) or tensor.data.len != actual.len) return error.InvalidInactiveTrainingFixture;
    for (actual, tensor.data) |value, byte| {
        if (byte > 1) return error.InvalidInactiveTrainingFixture;
        try std.testing.expectEqual(byte != 0, value);
    }
}

const Observation = struct {
    allocator: Allocator,
    reference: *const parity.TensorFixture,
    micro: ?control_oracle.Microbatch = null,
    calls: usize = 0,
    explicit_gradient_readback_bytes: usize = 0,
    fail_once: bool = false,

    fn observe(raw: ?*anyopaque, event: synthetic.Observation) !void {
        const self: *Observation = @ptrCast(@alignCast(raw.?));
        const micro = self.micro orelse return error.InvalidInactiveTrainingFixture;
        const prepared = event.prepared;
        try integers(self.reference, micro.inputs, "input_ids", prepared.input_ids);
        try integers(self.reference, micro.inputs, "attention_mask", prepared.attention_mask);
        try integers(self.reference, micro.inputs, "text_word_indices", prepared.text_word_indices);
        try integers(self.reference, micro.inputs, "query_marker_indices", prepared.query_marker_indices);
        try integers(self.reference, micro.inputs, "cls_marker_indices", prepared.cls_marker_indices);
        try booleans(self.reference, micro.inputs, "text_word_mask", prepared.text_word_mask);
        try booleans(self.reference, micro.inputs, "query_marker_mask", prepared.query_marker_mask);
        try booleans(self.reference, micro.inputs, "cls_marker_mask", prepared.cls_marker_mask);
        try std.testing.expectEqual(micro.fallback, event.zero_loss_fallback);
        if (micro.fallback) try std.testing.expectEqual(@as(f32, 0), event.optimizer_loss) else try parity.expectFloats(&.{micro.reported_loss}, &.{event.optimizer_loss}, 1e-3, 2e-5);
        // Same complete-model loss/VJP tolerances as the existing mixed Step
        // source fixture; inactive zero and missing gradients stay exact.
        inline for (std.meta.fields(objectives.Terms)) |field| {
            const expected = if (comptime std.mem.eql(u8, field.name, "total")) micro.model_loss else micro.losses.map.get(field.name ++ "_loss") orelse 0;
            try parity.expectFloats(&.{expected}, &.{@field(event.result.terms, field.name)}, 1e-3, 2e-5);
        }
        if (self.fail_once) {
            self.fail_once = false;
            return error.Cancelled;
        }
        try std.testing.expectEqual(micro.gradients_unscaled.map.count(), event.gradients.len);
        var gradient_bytes: usize = 0;
        for (event.gradients) |gradient| gradient_bytes = try std.math.add(usize, gradient_bytes, try std.math.mul(usize, gradient.elements, 4));
        if (gradient_bytes > mib) return error.InvalidInactiveTrainingFixture;
        for (event.gradients) |gradient| {
            errdefer std.debug.print("inactive native gradient {s}\n", .{gradient.name});
            const expected = try control_oracle.optionalTensor(self.reference, micro.gradients_unscaled, gradient.name);
            try std.testing.expectEqual(expected == null, gradient.kind == .absent);
            if (expected) |want| {
                try std.testing.expectEqual(want.len, gradient.elements);
                const actual = if (gradient.tensor) |tensor| read: {
                    const result = try self.allocator.alloc(f32, gradient.elements);
                    errdefer self.allocator.free(result);
                    if (event.backend.kind() == .metal) {
                        try event.backend.glinerBoundaryDownload(tensor, result);
                        self.explicit_gradient_readback_bytes += result.len * 4;
                    } else {
                        const owned = try event.backend.toFloat32(tensor, self.allocator);
                        defer self.allocator.free(owned);
                        try std.testing.expectEqual(result.len, owned.len);
                        @memcpy(result, owned);
                    }
                    break :read result;
                } else zero: {
                    try std.testing.expectEqual(@import("gliner_boundary_train_step.zig").GradientPresence.computed_zero, gradient.kind);
                    const result = try self.allocator.alloc(f32, gradient.elements);
                    @memset(result, 0);
                    break :zero result;
                };
                defer self.allocator.free(actual);
                const exact_zero = micro.fallback or gradient.kind == .computed_zero;
                if (exact_zero) {
                    for (want, actual) |expected_value, actual_value| {
                        try std.testing.expectEqual(@as(f32, 0), expected_value);
                        try std.testing.expectEqual(@as(f32, 0), actual_value);
                    }
                } else try parity.expectFloats(want, actual, 5e-4, 8e-4);
            } else try std.testing.expect(gradient.tensor == null);
        }
        self.calls += 1;
    }
};

fn state(reference: *const parity.TensorFixture, owner: *trainer.Trainer, expected: control_oracle.Flush, result: controller.Result) !void {
    try std.testing.expect(result.optimizer_stepped);
    try std.testing.expectEqual(expected.global_step, result.identity.optimizer_step);
    try std.testing.expectEqual(expected.after_microbatch, result.identity.microbatch_step);
    try std.testing.expectEqual(expected.global_step, expected.scheduler_last_epoch);
    try std.testing.expectApproxEqAbs(expected.grad_norm, result.grad_norm, 2e-3 + @abs(expected.grad_norm) * 5e-4);
    try owner.optimizer.ensureHostState(null);
    for (owner.optimizer.owner.regular_params.items) |slot| {
        const want = expected.parameters.map.get(slot.name) orelse return error.InvalidInactiveTrainingFixture;
        const moment = owner.optimizer.owner.optimizer_state.param_states.get(slot.name).?;
        try std.testing.expectEqual(want.step, slot.adam_step_count);
        try std.testing.expectEqual(want.step, moment.step_count);
        try std.testing.expectEqual(want.state_present, moment.step_count > 0);
        try parity.expectFloats(try reference.floats(want.weight), slot.weights, 2e-5, 2e-5);
        if (want.exp_avg) |key| try parity.expectFloats(try reference.floats(key), moment.m, 2e-5, 1e-3) else for (moment.m) |value| try std.testing.expectEqual(@as(f32, 0), value);
        if (want.exp_avg_sq) |key| try parity.expectFloats(try reference.floats(key), moment.v, 1e-7, 2e-3) else for (moment.v) |value| try std.testing.expectEqual(@as(f32, 0), value);
    }
}

fn replay(a: Allocator, store: *native.WeightStore, originals: []const run.Parameter, tokenizer: tokenizer_mod.Tokenizer, source: bundle.Identity, config: model.Config, samples: *const data.Dataset, options: trainer.Options, fixture: *const synthetic.Fixture, observation: *Observation, profile: control_oracle.Profile, path: []const u8, resume_partial: bool) ![32]u8 {
    var owner = try trainer.Trainer.initWithSyntheticAdapterFixtureForTest(a, store, tokenizer, source, config, samples, originals, options, null, fixture);
    defer owner.deinit();
    // Initialization is copied only into a fresh zero-counter owner. Every
    // subsequent state comparison uses native-updated values.
    for (owner.optimizer.owner.regular_params.items) |slot| {
        const initial = (try control_oracle.optionalTensor(observation.reference, profile.initial, slot.name)).?;
        try std.testing.expectEqualSlices(f32, initial, slot.weights);
        try std.testing.expectEqual(@as(u32, 0), slot.adam_step_count);
        const moment = owner.optimizer.owner.optimizer_state.param_states.get(slot.name).?;
        for (moment.m) |value| try std.testing.expectEqual(@as(f32, 0), value);
        for (moment.v) |value| try std.testing.expectEqual(@as(f32, 0), value);
    }
    var flush_index: usize = 0;
    for (profile.microbatches, 0..) |micro, index| {
        observation.micro = micro;
        if (resume_partial and index == 1) {
            const before = try owner.optimizer.stateFingerprint(owner.fingerprint, null);
            observation.fail_once = true;
            try std.testing.expectError(error.Cancelled, owner.next(null));
            try std.testing.expectEqual(before, try owner.optimizer.stateFingerprint(owner.fingerprint, null));
            try std.testing.expect(!owner.busy.load(.acquire));
            if (options.activation_profile == .layer_recompute_v1) {
                try std.testing.expect(!owner.plan.?.recomputation.?.graph.regional.active_tape);
                // A declared caller-arena failure must preserve the already
                // restored partial window and never reach the source observer.
                const limit = owner.options.limits.max_recomputed_batch_scratch_bytes;
                const observed = observation.calls;
                {
                    owner.options.limits.max_recomputed_batch_scratch_bytes = 1;
                    defer owner.options.limits.max_recomputed_batch_scratch_bytes = limit;
                    try std.testing.expectError(error.BoundaryTrainingHostMemoryLimitExceeded, owner.next(null));
                }
                try std.testing.expectEqual(observed, observation.calls);
                try std.testing.expectEqual(before, try owner.optimizer.stateFingerprint(owner.fingerprint, null));
                try std.testing.expect(!owner.busy.load(.acquire));
                try std.testing.expect(!owner.plan.?.recomputation.?.graph.regional.active_tape);
            }
        }
        const calls = observation.calls;
        var report = (try previous.nextObserved(owner)) orelse return error.InvalidInactiveTrainingFixture;
        try std.testing.expectEqual(calls + 1, observation.calls);
        if (options.activation_profile == .layer_recompute_v1) {
            const plan = &owner.plan.?;
            const regional = &plan.recomputation.?.graph.regional;
            try std.testing.expectEqual(step.AttentionProfile.replay_tiled_v1, plan.encoder.attention_profile);
            try std.testing.expectEqual(step.ActivationProfile.layer_recompute_v1, plan.encoder.activation_profile);
            try std.testing.expectEqual(@as(usize, config.encoder.num_hidden_layers) + 1, regional.regions.len);
            // Classifier/optional-head adapters do not request encoder VJPs;
            // the mixed encoder+classifier profile must actually replay them.
            try std.testing.expectEqual(std.mem.endsWith(u8, profile.id, "encoder_classifier"), regional.needsBackward());
            try std.testing.expect(!regional.active_tape);
            const admitted = regional.admission orelse return error.RecomputeAdmissionNotSealed;
            try std.testing.expect(admitted.backend_upper_bound_bytes <= options.limits.max_backend_bytes);
            try std.testing.expect(admitted.host_upper_bound_bytes <= options.source_reserved_bytes + options.limits.max_host_bytes);
            try std.testing.expect(owner.host_budget.live + owner.recomputed_future_host_bytes <= options.limits.max_host_bytes);
        }
        try std.testing.expect(report.terms != null);
        try std.testing.expectEqual(micro.fallback, report.zero_loss_fallback);
        if (micro.fallback) try std.testing.expectEqual(@as(f32, 0), report.optimizer.loss.?) else try parity.expectFloats(&.{micro.reported_loss}, &.{report.optimizer.loss.?}, 1e-3, 2e-5);
        if (!report.optimizer.optimizer_stepped) {
            try owner.optimizer.ensureHostState(null);
            for (owner.optimizer.owner.regular_params.items, owner.optimizer.present) |slot, present| {
                const expected = try control_oracle.optionalTensor(observation.reference, micro.accumulated_gradients, slot.name);
                try std.testing.expectEqual(expected != null, present);
                if (expected) |values| try parity.expectFloats(values, slot.grad_accum, 5e-4, 8e-4) else for (slot.grad_accum) |value| try std.testing.expectEqual(@as(f32, 0), value);
            }
            if (resume_partial and index == 0) {
                const digest = try owner.optimizer.stateFingerprint(owner.fingerprint, null);
                try owner.save(path, null);
                const restored = try trainer.Trainer.initWithSyntheticAdapterFixtureForTest(a, store, tokenizer, source, config, samples, originals, options, null, fixture);
                var owned = true;
                errdefer if (owned) restored.deinit();
                _ = try restored.restorePinned(path, digest, null);
                try previous.expectSameState(owner, restored);
                owner.deinit();
                owner = restored;
                owned = false;
            }
        }
        const flush = std.mem.indexOfScalar(u64, profile.flush_after, index + 1) != null;
        if (flush and !report.optimizer.optimizer_stepped) {
            const before = observation.calls;
            report = (try previous.nextObserved(owner)) orelse return error.InvalidInactiveTrainingFixture;
            try std.testing.expect(report.terms == null);
            try std.testing.expectEqual(before, observation.calls);
        }
        if (flush) {
            try state(observation.reference, owner, profile.flushes[flush_index], report.optimizer);
            flush_index += 1;
        } else try std.testing.expect(!report.optimizer.optimizer_stepped);
    }
    try std.testing.expect((try owner.next(null)) == null);
    try std.testing.expectEqual(profile.flushes.len, flush_index);
    return owner.optimizer.stateFingerprint(owner.fingerprint, null);
}

fn exercise(a: Allocator, execution: controller.Execution) !void {
    return exerciseWithProfiles(a, execution, .materialized_v1, .retained_v1);
}

fn exerciseWithProfiles(a: Allocator, execution: controller.Execution, attention_profile: step.AttentionProfile, activation_profile: step.ActivationProfile) !void {
    const capture_bytes = try parity.fixtureBytes(a, directory ++ "capture.json");
    defer a.free(capture_bytes);
    if (capture_bytes.len > mib) return error.InvalidInactiveTrainingFixture;
    try control_oracle.expectDigest(capture_bytes, "9947cc37d6adc8b209c7769b2cd61f239738c3583646747444e655bb1afa44f1");
    const parsed = try std.json.parseFromSlice(Oracle, a, capture_bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();
    const oracle = parsed.value;
    try std.testing.expectEqual(@as(u32, 1), oracle.version);
    try std.testing.expect(!oracle.qualification);
    try std.testing.expectEqualStrings("gliner25_inactive_adapter_native_epoch/v1", oracle.scope);
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", oracle.source_commit);
    try std.testing.expectEqual(@as(usize, 8), oracle.profiles.len);
    var reference = try parity.TensorFixture.init(a, directory ++ "tensors.safetensors");
    defer reference.deinit();
    try control_oracle.expectDigest(reference.reader.file_bytes, "374658ee67127b2f81ec597a65eabe72ee52263d88a718ebf4ef7e9b246741e9");
    const settings = try parity.fixtureBytes(a, directory ++ "native_settings.json");
    defer a.free(settings);
    try control_oracle.expectDigest(settings, "0c041c7f9123e507cebc7d85860b03380a7f413a54427c1bcc068c9241443fd6");
    const tensor_digest = bundle.Digest.of(reference.reader.file_bytes);
    var config = model.Config{ .version = 3, .architecture_version = 1, .backbone = .small, .max_len = 4096, .head = oracle.config.boundary_head, .encoder = oracle.encoder_config };
    const relative = try reference.tensor(oracle.base_parameters.map.get("encoder.encoder.rel_embeddings.weight").?);
    config.encoder.max_position_embeddings = @intCast(relative.shape[0]);
    if (activation_profile == .layer_recompute_v1) {
        // This immutable source fixture uses p=0. Tiled attention therefore
        // preserves its source randomness contract without supplied masks.
        try std.testing.expectEqual(step.AttentionProfile.replay_tiled_v1, attention_profile);
        try std.testing.expectEqual(@as(f32, 0), config.head.dropout);
        try std.testing.expectEqual(@as(f32, 0), config.encoder.hidden_dropout_prob);
        try std.testing.expectEqual(@as(f32, 0), config.encoder.attention_probs_dropout_prob);
    }
    const tokenizer_bytes = try parity.fixtureBytes(a, "training_step/tokenizer.json");
    defer a.free(tokenizer_bytes);
    try control_oracle.expectDigest(tokenizer_bytes, "ee7f0d2f277c34191fe76227371cc95d7b55e0f7a10f1a153eec36b5d5865cc8");
    const tokenizer_config_bytes = try parity.fixtureBytes(a, "training_step/tokenizer_config.json");
    defer a.free(tokenizer_config_bytes);
    try control_oracle.expectDigest(tokenizer_config_bytes, "2ea7f1b38011a9f3f7b4c895bd876160e8672cfe62f26d8303fa875e3eaa5f80");
    const declared = try std.json.parseFromSlice(struct { added_tokens: []const Tokens.Added }, a, tokenizer_bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer declared.deinit();
    const added = declared.value.added_tokens;
    var tokens = Tokens{ .fragments = &oracle.tokenizer_fragments, .added = added, .vocabulary = config.encoder.vocab_size, .special = .{
        .cls_id = try Tokens.declaredId(added, "[CLS]"),
        .sep_id = try Tokens.declaredId(added, "[SEP]"),
        .pad_id = try Tokens.declaredId(added, "[PAD]"),
        .unk_id = try Tokens.declaredId(added, "[UNK]"),
        .mask_id = try Tokens.declaredId(added, "[MASK]"),
    } };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var original = std.ArrayListUnmanaged(run.Parameter).empty;
    for (oracle.base_parameters.map.keys(), oracle.base_parameters.map.values()) |canonical, key| {
        const source = try reference.tensor(key);
        const borrowed = if (std.mem.startsWith(u8, canonical, "encoder.")) canonical["encoder.".len..] else canonical;
        const name = try a.dupe(u8, borrowed);
        var owned = true;
        errdefer if (owned) a.free(name);
        var tensor = try Tensor.initFloat32(a, name, source.shape, try reference.floats(key));
        errdefer if (owned) tensor.deinit();
        try store.resident_weights.put(a, name, .{ .tensor = tensor });
        owned = false;
        const dimensions = try scratch.alloc(i32, source.shape.len);
        for (dimensions, source.shape) |*out, value| out.* = @intCast(value);
        try original.append(scratch, .{ .name = name, .canonical_name = canonical, .values = tensor.asFloat32(), .dimensions = dimensions, .kind = .original });
    }
    const identity = bundle.Identity{ .backbone = .small, .precision = .fp32, .weight = tensor_digest, .sidecars = .{ bundle.Digest.of(capture_bytes), bundle.Digest.of("synthetic encoder"), bundle.Digest.of("captured tokenizer fragments"), bundle.Digest.of("test-only source initial state") } };
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const checkpoint = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/native-inactive.safetensors", .{temporary.sub_path});
    defer a.free(checkpoint);
    for (oracle.profiles) |profile| {
        errdefer std.debug.print("actual inactive NativeTrainer source profile {s}/{s}/{s}/{s}\n", .{ profile.id, @tagName(execution), @tagName(attention_profile), @tagName(activation_profile) });
        const separator = std.mem.indexOfScalar(u8, profile.id, '.') orelse return error.InvalidInactiveTrainingFixture;
        const name = try std.fmt.allocPrint(scratch, directory ++ "{s}.jsonl", .{profile.id[separator + 1 ..]});
        const rows = try parity.fixtureBytes(a, name);
        defer a.free(rows);
        const row_digest = if (std.mem.endsWith(u8, name, "/classifier_only.jsonl"))
            "377177ee889c4e426eeeccc4dc551437999aa702b8a5055c233aaee8936c54c7"
        else if (std.mem.endsWith(u8, name, "/encoder_classifier.jsonl"))
            "9bdaab9a4f11555e50ec40ec02b068a086754b57c30d6fe0c0cb4a66c6e53f3e"
        else if (std.mem.endsWith(u8, name, "/record_only.jsonl"))
            "81243bcde2488b7560b15c1d5bd29efa2f963a1f707792b6b6681387bfdaa37d"
        else if (std.mem.endsWith(u8, name, "/relation_only.jsonl"))
            "629d2d22484654cb41e169bf744b019229109dcfd5d4869907140a0034d36a4f"
        else
            return error.InvalidInactiveTrainingFixture;
        try control_oracle.expectDigest(rows, row_digest);
        var samples = try data.Dataset.fromBytes(a, rows, .{ .limits = .{ .max_host_bytes = 16 * mib } }, null, null);
        defer samples.deinit();
        try std.testing.expectEqual(profile.sequence.len, samples.index.len);
        const initial = try scratch.alloc(run.Parameter, profile.parameters.len);
        for (profile.parameters, initial) |parameter, *out| out.* = .{ .name = parameter.name, .canonical_name = parameter.name, .dimensions = parameter.shape, .values = (try control_oracle.optionalTensor(&reference, profile.initial, parameter.name)).?, .kind = .adapter };
        std.mem.sort(run.Parameter, initial, {}, struct {
            fn less(_: void, lhs: run.Parameter, rhs: run.Parameter) bool {
                return std.mem.order(u8, lhs.name, rhs.name) == .lt;
            }
        }.less);
        var observation = Observation{ .allocator = a, .reference = &reference };
        const fixture = synthetic.Fixture{ .capture = bundle.Digest.of(capture_bytes), .tensors = tensor_digest, .initial = initial, .observer_context = &observation, .observe = Observation.observe };
        const dora = std.mem.startsWith(u8, profile.id, "dora.");
        var options = trainer.Options{
            .execution = execution,
            .attention_profile = attention_profile,
            .activation_profile = activation_profile,
            .run = .{ .mode = if (dora) .dora else .lora, .epochs = 1, .batch_size = 1, .accumulation = oracle.optimizer.accumulation_steps, .seed = 257713, .task_lr = oracle.optimizer.task_lr, .weight_decay = oracle.optimizer.weight_decay, .beta1 = oracle.optimizer.betas[0], .beta2 = oracle.optimizer.betas[1], .epsilon = oracle.optimizer.eps, .max_grad_norm = oracle.optimizer.max_grad_norm, .scheduler = .constant, .warmup_steps = 0, .shuffle = false },
            .source_reserved_bytes = 4 * mib,
            .peft = .{ .kind = if (dora) .dora else .lora, .rank = 2, .alpha = 3, .dropout = 0, .targets = profile.targets },
            .gold_start = 1,
            .gold_end = 1,
            .gold_hold_fraction = 0,
            .limits = .{ .max_host_bytes = 128 * mib, .max_backend_bytes = 256 * mib, .max_backend_host_bytes = 16 * mib, .max_combined_bytes = 512 * mib },
        };
        if (activation_profile == .layer_recompute_v1) {
            // Same finite enclosing source/host/device owners as the retained
            // fixture. Explicit inner caps cover this tiny complete epoch.
            options.limits.step.recomputation.max_plan_host_bytes = 32 * mib;
            options.limits.step.max_step_host_bytes = 32 * mib;
            options.limits.max_recomputed_batch_scratch_bytes = 16 * mib;
        }
        const uninterrupted = try replay(a, &store, original.items, tokens.tokenizer(), identity, config, &samples, options, &fixture, &observation, profile, checkpoint, false);
        const resumed = try replay(a, &store, original.items, tokens.tokenizer(), identity, config, &samples, options, &fixture, &observation, profile, checkpoint, true);
        try std.testing.expectEqual(uninterrupted, resumed);
        try std.testing.expectEqual(profile.microbatches.len * 2, observation.calls);
        if (execution == .resident_metal and !std.mem.endsWith(u8, profile.id, "record_only") and !std.mem.endsWith(u8, profile.id, "relation_only")) try std.testing.expect(observation.explicit_gradient_readback_bytes > 0);
    }
}

test "boundary inactive NativeTrainer CPU actual source tokens fallback VJPs optimizer and durable resume" {
    try exercise(std.testing.allocator, .native);
}

test "boundary inactive NativeTrainer Metal actual source tokens fallback VJPs optimizer and durable resume" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!metal.metalDeviceAvailable()) return error.SkipZigTest;
    try exercise(std.testing.allocator, .resident_metal);
}

test "boundary inactive NativeTrainer regional recomputation CPU source fallback VJPs optimizer and durable resume" {
    try exerciseWithProfiles(std.testing.allocator, .native, .replay_tiled_v1, .layer_recompute_v1);
}

test "boundary inactive NativeTrainer regional recomputation Metal source fallback VJPs optimizer and durable resume" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!metal.metalDeviceAvailable()) return error.SkipZigTest;
    try exerciseWithProfiles(std.testing.allocator, .resident_metal, .replay_tiled_v1, .layer_recompute_v1);
}
