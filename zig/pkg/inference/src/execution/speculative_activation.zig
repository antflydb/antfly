// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Request-owned predictor activation provenance and retained embeddings.
//! Task policy controls drafting/acceptance; actual tensor/cache state lives here.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const gpt_mod = @import("../models/gpt.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const decoder_forward = @import("decoder_forward.zig");
const state_mod = @import("../graph/decode_state_runtime.zig");
const NativeDecodeState = state_mod.NativeDecodeState;
const BorrowedDecodeStateRuntime = state_mod.BorrowedDecodeStateRuntime;

pub const State = struct {
    /// One physical drafting round. Retained deferred slots and logits own
    /// their actual producer allocator/backend; ordinary chains own only the
    /// latest activation. No task/sampler/tokenizer callback is retained.
    pub const Round = struct {
        owner: *State,
        target: decoder_forward.Runtime,
        draft: decoder_forward.Runtime,
        deferred: bool,
        host_chain: ?[]f32 = null,
        device_chain: ?ops.CT = null,
        host_steps: [16]?[]f32 = @splat(null),
        device_steps: [16]?ops.CT = @splat(null),
        logits: [16]?[]f32 = @splat(null),
        step_written: [16]bool = @splat(false),
        next_host: ?[]f32 = null,
        next_device: ?ops.CT = null,

        pub const Step = struct {
            token: usize,
            logit_source: @import("../architectures/gemma4_mtp.zig").DraftLogitSource,
            profile: @import("../architectures/gemma4_mtp.zig").DraftStageProfile,
        };
        pub const Request = struct {
            index: usize,
            token_id: i64,
            decode_context: *const gpt_arch.DecodeContext,
            resident: bool,
            target_embedding_on_draft: ?ops.CT = null,
            debug_top_k: usize,
            profile_enabled: bool,
            profile_sync: bool,
        };

        pub fn deinit(self: *@This()) void {
            if (self.next_device) |tensor| self.draft.context.cb.free(tensor);
            if (self.next_host) |activation| self.owner.allocator.free(activation);
            for (self.host_steps) |slot| if (slot) |activation| self.owner.allocator.free(activation);
            for (self.device_steps) |slot| if (slot) |tensor| self.draft.context.cb.free(tensor);
            if (!self.deferred) {
                if (self.device_chain) |tensor| self.draft.context.cb.free(tensor);
                if (self.host_chain) |activation| self.owner.allocator.free(activation);
            }
            for (self.logits) |slot| if (slot) |values| self.owner.allocator.free(values);
            self.* = undefined;
        }

        pub fn step(self: *@This(), request: Request) !Step {
            const mtp = @import("../architectures/gemma4_mtp.zig");
            if (request.index >= self.logits.len) return error.InvalidSpeculativeK;
            if (self.step_written[request.index]) return error.InvalidSpeculativeState;
            const result: Step = if (request.resident) blk: {
                const activation = self.device_chain orelse self.owner.device orelse return error.MissingGemma4MtpActivation;
                const value = try mtp.draftTokenDevice(.{
                    .allocator = self.owner.allocator,
                    .target_cb = self.target.context.cb,
                    .draft_cb = self.draft.context.cb,
                    .target_config = self.target.context.gpt_config.*,
                    .draft_config = self.draft.context.gpt_config.*,
                    .token_id = request.token_id,
                    .activation = activation,
                    .target_embedding_on_draft = request.target_embedding_on_draft,
                    .decode_context = request.decode_context,
                    .debug_top_k = request.debug_top_k,
                    .profile_enabled = request.profile_enabled,
                    .profile_sync = request.profile_sync,
                });
                if (request.profile_sync) self.draft.context.cb.evalTensor(value.projected_activation) catch {};
                if (self.deferred) self.device_steps[request.index] = value.projected_activation else if (self.device_chain) |old| self.draft.context.cb.free(old);
                self.device_chain = value.projected_activation;
                self.logits[request.index] = value.logits;
                break :blk .{ .token = value.token, .logit_source = value.logit_source, .profile = value.profile };
            } else blk: {
                const activation = self.host_chain orelse self.owner.host orelse return error.MissingGemma4MtpActivation;
                const value = try mtp.draftToken(.{
                    .allocator = self.owner.allocator,
                    .target_cb = self.target.context.cb,
                    .draft_cb = self.draft.context.cb,
                    .target_config = self.target.context.gpt_config.*,
                    .draft_config = self.draft.context.gpt_config.*,
                    .token_id = request.token_id,
                    .activation = activation,
                    .decode_context = request.decode_context,
                    .debug_top_k = request.debug_top_k,
                    .profile_enabled = request.profile_enabled,
                    .profile_sync = request.profile_sync,
                });
                if (self.deferred) self.host_steps[request.index] = value.projected_activation else if (self.host_chain) |old| self.owner.allocator.free(old);
                self.host_chain = value.projected_activation;
                self.logits[request.index] = value.logits;
                break :blk .{ .token = value.token, .logit_source = value.logit_source, .profile = value.profile };
            };
            self.step_written[request.index] = true;
            return result;
        }

        /// The task selects an accepted logical step; this owner transfers its
        /// already-produced physical activation once, without another forward.
        pub fn retainStep(self: *@This(), index: usize, resident: bool) !void {
            if (index >= self.logits.len) return error.InvalidSpeculativeK;
            if (resident) {
                self.next_device = self.device_steps[index] orelse return error.MissingGemma4MtpActivation;
                self.device_steps[index] = null;
            } else {
                self.next_host = self.host_steps[index] orelse return error.MissingGemma4MtpActivation;
                self.host_steps[index] = null;
            }
        }

        pub fn captureHidden(self: *@This(), value: *const decoder_forward.ForwardHiddenDevice, row: usize, source: decoder_forward.HiddenSource, resident: bool, sync: bool) !void {
            if (resident) {
                self.next_device = (try self.target.copyTargetHiddenRowFromHiddenToBackend(value, row, source, self.draft.context.cb)) orelse return error.UnsupportedBackend;
                if (sync) self.draft.context.cb.evalTensor(self.next_device.?) catch {};
            } else {
                var target = self.target;
                target.context.allocator = self.owner.allocator;
                self.next_host = try target.dupeTargetHiddenRowFromHiddenDevice(value, row, source);
            }
        }
        pub fn captureAllDevice(self: *@This(), value: *const decoder_forward.ForwardAllWithHiddenDevice, row: usize, source: decoder_forward.HiddenSource, resident: bool, sync: bool) !void {
            if (resident) {
                self.next_device = (try self.target.copyTargetHiddenRowToBackend(value, row, source, self.draft.context.cb)) orelse return error.UnsupportedBackend;
                if (sync) self.draft.context.cb.evalTensor(self.next_device.?) catch {};
            } else {
                var target = self.target;
                target.context.allocator = self.owner.allocator;
                self.next_host = try target.dupeTargetHiddenRowFromDevice(value, row, source);
            }
        }
        pub fn captureAllHost(self: *@This(), value: *const decoder_forward.ForwardAllWithHiddenHost, row: usize, source: decoder_forward.HiddenSource) !void {
            var target = self.target;
            target.context.allocator = self.owner.allocator;
            self.next_host = try target.dupeTargetHiddenRow(value, row, source);
        }
        pub fn replaceNextDevice(self: *@This(), activation: ops.CT) void {
            if (self.next_device) |old| self.draft.context.cb.free(old);
            self.next_device = activation;
            if (self.next_host) |old| self.owner.allocator.free(old);
            self.next_host = null;
        }
        pub fn replaceNextHost(self: *@This(), activation: []f32) void {
            if (self.next_host) |old| self.owner.allocator.free(old);
            self.next_host = activation;
        }
        pub fn materializeNext(self: *@This(), token_ids: []const i64, sequence_length: usize, state: *NativeDecodeState, source: decoder_forward.HiddenSource, resident: bool, sync: bool) !?u32 {
            if (resident) {
                const materialized = try self.target.materializeAcceptedTokenKvAndCopyHiddenToBackend(token_ids, sequence_length, state, source, self.draft.context.cb);
                const activation = materialized.activation orelse return error.UnsupportedBackend;
                if (sync) self.draft.context.cb.evalTensor(activation) catch {};
                self.replaceNextDevice(activation);
                return materialized.next_target_choice;
            }
            var target = self.target;
            target.context.allocator = self.owner.allocator;
            const materialized = try target.materializeAcceptedTokenKvAndReturnHidden(token_ids, sequence_length, state, source);
            self.replaceNextHost(materialized.activation);
            return materialized.next_target_choice;
        }
        pub fn publishActivation(self: *@This()) void {
            if (self.next_device) |activation| {
                self.owner.replaceDevice(activation);
                self.next_device = null;
            } else if (self.next_host) |activation| {
                self.owner.replaceHost(activation);
                self.next_host = null;
            }
        }
    };

    pub fn beginRound(self: *State, target: decoder_forward.Runtime, draft: decoder_forward.Runtime, deferred: bool) Round {
        return .{ .owner = self, .target = target, .draft = draft, .deferred = deferred };
    }

    const DraftEmbeddingCacheEntry = struct {
        token_id: i64,
        tensor: ops.CT,
    };

    allocator: std.mem.Allocator,
    draft_cb: *const ComputeBackend,
    host: ?[]f32 = null,
    device: ?ops.CT = null,
    cached_target_choice: ?u32 = null,
    /// True only when the retained activation produced cached_target_choice.
    /// Predictor activations and the initial prompt activation do not qualify.
    cached_choice_matches_activation: bool = false,
    /// Defer-materialize fold: a committed token (already counted by
    /// seq_len and present in token_ids) whose target forward/KV write is
    /// deferred to the next verify round. Invariant at every round
    /// boundary: decode_state.total_tokens + pendingCount() == seq_len.
    pending_unmaterialized: bool = false,
    cache_event: CacheEvent = .{},
    draft_embedding_cache: std.ArrayListUnmanaged(DraftEmbeddingCacheEntry) = .empty,

    pub fn deinit(self: *State) void {
        if (self.host) |activation| self.allocator.free(activation);
        if (self.device) |tensor| self.draft_cb.free(tensor);
        for (self.draft_embedding_cache.items) |entry| self.draft_cb.free(entry.tensor);
        self.draft_embedding_cache.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn replaceHost(self: *State, activation: []f32) void {
        if (self.host) |old| self.allocator.free(old);
        if (self.device) |old| self.draft_cb.free(old);
        self.host = activation;
        self.device = null;
        self.cached_target_choice = null;
        self.cached_choice_matches_activation = false;
    }

    pub fn replaceDevice(self: *State, tensor: ops.CT) void {
        if (self.host) |old| self.allocator.free(old);
        if (self.device) |old| self.draft_cb.free(old);
        self.host = null;
        self.device = tensor;
        self.cached_target_choice = null;
        self.cached_choice_matches_activation = false;
    }

    pub fn residentReady(self: *const State) bool {
        return self.device != null;
    }

    pub fn pendingCount(self: *const State) usize {
        return @intFromBool(self.pending_unmaterialized);
    }

    /// Every committed token except the pending one must have KV written.
    /// This is a production invariant: a mismatch shifts the verify suffix
    /// write onto the wrong KV rows, so fail closed in ReleaseFast too.
    pub fn validateKvTokensInSync(self: *const State, decode_state: *const NativeDecodeState, seq_len: usize) !void {
        const accounted_tokens = std.math.add(usize, decode_state.total_tokens, self.pendingCount()) catch
            return error.InvalidSpeculativeState;
        if (accounted_tokens != seq_len) return error.InvalidSpeculativeState;
    }

    pub fn notePendingUnmaterialized(self: *State) !void {
        if (self.pending_unmaterialized) return error.InvalidSpeculativeState;
        self.pending_unmaterialized = true;
    }

    pub fn notePendingKvWritten(self: *State) void {
        self.pending_unmaterialized = false;
    }

    fn cachedDraftEmbedding(self: *const State, token_id: i64) ?ops.CT {
        for (self.draft_embedding_cache.items) |entry| {
            if (entry.token_id == token_id) return entry.tensor;
        }
        return null;
    }

    fn rememberDraftEmbedding(self: *State, token_id: i64, tensor: ops.CT, limit: usize) !usize {
        if (limit == 0) {
            self.draft_cb.free(tensor);
            return 0;
        }
        if (self.cachedDraftEmbedding(token_id) != null) {
            self.draft_cb.free(tensor);
            return 0;
        }
        var evictions: usize = 0;
        while (self.draft_embedding_cache.items.len >= limit) {
            const old = self.draft_embedding_cache.orderedRemove(0);
            self.draft_cb.free(old.tensor);
            evictions += 1;
        }
        try self.draft_embedding_cache.append(self.allocator, .{
            .token_id = token_id,
            .tensor = tensor,
        });
        return evictions;
    }
    pub fn replaceFromPrompt(
        self: *State,
        target_runtime: decoder_forward.Runtime,
        prompt_ids: []const i64,
        seq_len: usize,
        hidden_source: decoder_forward.HiddenSource,
        use_resident_draft: bool,
        draft_cb: *const ComputeBackend,
        replay_label: []const u8,
    ) !void {
        var target = target_runtime;
        const allocator = target.context.allocator;
        if (seq_len == 0 or prompt_ids.len < seq_len) return error.InvalidTensorShape;
        var seed_state = NativeDecodeState.initContiguous(allocator);
        defer seed_state.deinit();
        seed_state.configureForGptConfig(target.context.gpt_config.*);
        var seed_runtime = BorrowedDecodeStateRuntime.init(&seed_state);
        const seed_ctx = seed_runtime.makeDecodeContext(seq_len, seq_len);
        var target_hidden = try target.forwardTargetHiddenDevice(
            prompt_ids[0..seq_len],
            1,
            seq_len,
            &seed_ctx,
            hidden_source,
            replay_label,
        );
        defer target_hidden.deinit();

        const row = seq_len - 1;
        if (use_resident_draft) {
            if (try target.copyTargetHiddenRowFromHiddenToBackend(&target_hidden, row, hidden_source, draft_cb)) |activation| {
                self.replaceDevice(activation);
                return;
            }
        }
        self.replaceHost(try self.copyHostRow(target, &target_hidden, row, hidden_source));
    }

    pub fn replaceFromPrefillHidden(
        self: *State,
        target_runtime: decoder_forward.Runtime,
        hidden: ops.CT,
        rows: usize,
        hidden_source: decoder_forward.HiddenSource,
        use_resident_draft: bool,
        draft_cb: *const ComputeBackend,
    ) !bool {
        var target = target_runtime;
        if (rows == 0) return error.InvalidTensorShape;
        if (hidden_source != .final) return false;
        const target_hidden = decoder_forward.ForwardHiddenDevice{
            .cb = target.context.cb,
            .hidden = hidden,
            .pre_norm_hidden = null,
            .rows = rows,
        };
        const row = rows - 1;
        if (use_resident_draft) {
            if (try target.copyTargetHiddenRowFromHiddenToBackend(&target_hidden, row, hidden_source, draft_cb)) |activation| {
                self.replaceDevice(activation);
                return true;
            }
        }
        self.replaceHost(try self.copyHostRow(target, &target_hidden, row, hidden_source));
        return true;
    }

    fn copyHostRow(self: *State, target_runtime: decoder_forward.Runtime, hidden: *const decoder_forward.ForwardHiddenDevice, row: usize, source: decoder_forward.HiddenSource) ![]f32 {
        var target = target_runtime;
        // Host activations belong to this owner, even when its allocator is
        // different from the source runtime's temporary allocation domain.
        target.context.allocator = self.allocator;
        return target.dupeTargetHiddenRowFromHiddenDevice(hidden, row, source);
    }

    pub const CacheEvent = struct {
        status: enum { none, disabled, hit, miss, inserted } = .none,
        evictions: usize = 0,
    };

    /// The returned tensor is borrowed from this owner. Cache storage uses the
    /// draft backend; source model scaling and layout remain canonical.
    pub fn cachedTargetEmbedding(self: *State, target_runtime: decoder_forward.Runtime, draft_model: *const gpt_mod.Config, token_id: i64, limit: usize) !?ops.CT {
        self.cache_event = .{};
        if (limit == 0) {
            self.cache_event.status = .disabled;
            return null;
        }
        if (self.cachedDraftEmbedding(token_id)) |tensor| {
            self.cache_event.status = .hit;
            return tensor;
        }
        self.cache_event.status = .miss;
        const target = target_runtime.context;
        const width: usize = @intCast(draft_model.mtp_backbone_hidden_size);
        if (width == 0 or target.gpt_config.hidden_size != width) return null;
        const weight = try gpt_arch.getEmbeddingWeight(target.cb, target.gpt_config.*);
        defer target.cb.free(weight);
        const embedded = try target.cb.embeddingLookup(weight, &.{token_id}, 1, width);
        const scaled = try gpt_arch.maybeScaleTokenEmbeddings(target.cb, target.allocator, target.gpt_config.*, embedded, 1, width);
        defer target.cb.free(scaled);
        const copied = (try self.draft_cb.copyTensorFromBackend(target.cb, scaled)) orelse return null;
        errdefer self.draft_cb.free(copied);
        self.cache_event.evictions = try self.rememberDraftEmbedding(token_id, copied, limit);
        self.cache_event.status = .inserted;
        return copied;
    }
};

fn populateActivationWeights(store: *@import("../ops/native_compute.zig").WeightStore) !void {
    const fixture = @import("../ops/media_residency_test.zig");
    try fixture.putWeight(store, "model.embed_tokens.weight", &.{ 4, 4 }, null);
    try fixture.putWeight(store, "model.norm.weight", &.{4}, 1);
}

fn checkActualActivationOwner(draft_value: ComputeBackend, reference_value: ComputeBackend) !void {
    const a = std.testing.allocator;
    const draft = &draft_value;
    const target_cb = &draft_value;
    const reference_cb = &reference_value;
    const config = gpt_mod.Config{ .family = .llama, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .vocab_size = 4, .weight_tying = true, .norm_type = .rms_norm, .position_encoding = .rope };
    const draft_config = gpt_mod.Config{ .hidden_size = 4, .mtp_backbone_hidden_size = 4 };
    const target = decoder_forward.Runtime{ .context = .{ .allocator = a, .cb = target_cb, .gpt_config = &config, .weight_semantics = .traced_decoder } };
    var state = State{ .allocator = a, .draft_cb = draft };
    defer state.deinit();
    state.replaceHost(try a.dupe(f32, &.{ 1, 2, 3, 4 }));
    state.cached_target_choice = 3;
    state.cached_choice_matches_activation = true;
    const device = try draft.fromFloat32Shape(&.{ 5, 6, 7, 8 }, &.{ 1, 4 });
    state.replaceDevice(device);
    try std.testing.expect(state.host == null and state.residentReady());
    try std.testing.expect(state.cached_target_choice == null and !state.cached_choice_matches_activation);
    state.cached_target_choice = 2;
    state.cached_choice_matches_activation = true;
    state.replaceHost(try a.dupe(f32, &.{ 9, 10, 11, 12 }));
    try std.testing.expect(!state.residentReady());
    try std.testing.expect(state.cached_target_choice == null and !state.cached_choice_matches_activation);

    const embedding_weight = try reference_cb.getWeight("model.embed_tokens.weight");
    defer reference_cb.free(embedding_weight);
    const raw_embedding = try reference_cb.embeddingLookup(embedding_weight, &.{1}, 1, 4);
    defer reference_cb.free(raw_embedding);
    const norm_weight = try reference_cb.getWeight("model.norm.weight");
    defer reference_cb.free(norm_weight);
    const normalized = try reference_cb.rmsNorm(raw_embedding, norm_weight, 4, config.norm_eps);
    defer reference_cb.free(normalized);
    const expected_seed = try reference_cb.toFloat32(normalized, a);
    defer a.free(expected_seed);
    try state.replaceFromPrompt(target, &.{ 0, 1 }, 2, .final, false, draft, "decoder.test_seed");
    for (expected_seed, state.host.?) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 1e-5);

    const hidden = try target_cb.fromFloat32Shape(&.{ 1, 2, 3, 4, -2, 4, 6, 8 }, &.{ 2, 4 });
    defer target_cb.free(hidden);
    try std.testing.expect(try state.replaceFromPrefillHidden(target, hidden, 2, .final, false, draft));
    try std.testing.expectEqualSlices(f32, &.{ -2, 4, 6, 8 }, state.host.?);
    // Source ownership stays with the caller even after selected-row copy.
    const retained_source = try target_cb.toFloat32(hidden, a);
    defer a.free(retained_source);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, -2, 4, 6, 8 }, retained_source);
    try std.testing.expect(!try state.replaceFromPrefillHidden(target, hidden, 2, .pre_norm, false, draft));
    try std.testing.expectEqualSlices(f32, &.{ -2, 4, 6, 8 }, state.host.?);

    try std.testing.expect((try state.cachedTargetEmbedding(target, &draft_config, 1, 0)) == null);
    try std.testing.expectEqual(.disabled, state.cache_event.status);
    const copied = try state.cachedTargetEmbedding(target, &draft_config, 1, 2);
    if (draft.kind() == .metal) try std.testing.expect(copied != null);
    if (copied) |tensor| {
        try std.testing.expectEqual(.inserted, state.cache_event.status);
        const hit = try state.cachedTargetEmbedding(target, &draft_config, 1, 2);
        try std.testing.expectEqual(.hit, state.cache_event.status);
        try std.testing.expectEqual(tensor, hit.?);
        const source_weight = try gpt_arch.getEmbeddingWeight(reference_cb, config);
        defer reference_cb.free(source_weight);
        const source_embedding = try reference_cb.embeddingLookup(source_weight, &.{1}, 1, 4);
        defer reference_cb.free(source_embedding);
        const expected = try reference_cb.toFloat32(source_embedding, a);
        defer a.free(expected);
        const actual = try draft.toFloat32(tensor, a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, expected, actual);
    } else {
        try std.testing.expectEqual(.miss, state.cache_event.status);
    }
    // Exercise actual retained backend tensors even when that backend has no
    // direct inter-provider copy capability (the portable Native path).
    const token2 = try draft.fromFloat32Shape(&.{ 2, 4, 6, 8 }, &.{ 1, 4 });
    _ = try state.rememberDraftEmbedding(2, token2, 2);
    const token3 = try draft.fromFloat32Shape(&.{ 3, 6, 9, 12 }, &.{ 1, 4 });
    _ = try state.rememberDraftEmbedding(3, token3, 2);
    const token0 = try draft.fromFloat32Shape(&.{ 0, 0, 0, 0 }, &.{ 1, 4 });
    try std.testing.expectEqual(@as(usize, 1), try state.rememberDraftEmbedding(0, token0, 2));
    try std.testing.expect(state.cachedDraftEmbedding(2) == null);
    const retained = try draft.toFloat32(state.cachedDraftEmbedding(3).?, a);
    defer a.free(retained);
    try std.testing.expectEqualSlices(f32, &.{ 3, 6, 9, 12 }, retained);

    const CopyOom = struct {
        fn run(allocator: std.mem.Allocator, source: decoder_forward.Runtime, source_hidden: ops.CT, destination: *const ComputeBackend) !void {
            var owned = State{ .allocator = allocator, .draft_cb = destination };
            defer owned.deinit();
            _ = try owned.replaceFromPrefillHidden(source, source_hidden, 2, .final, false, destination);
            try std.testing.expectEqualSlices(f32, &.{ -2, 4, 6, 8 }, owned.host.?);
        }
    };
    try std.testing.checkAllAllocationFailures(a, CopyOom.run, .{ target, hidden, draft });

    // Failure while growing a populated cache must preserve every retained
    // tensor; ownership of the rejected new tensor stays with its caller.
    var fail_growth = std.testing.FailingAllocator.init(a, .{ .fail_index = 1 });
    var populated = State{ .allocator = fail_growth.allocator(), .draft_cb = draft };
    defer populated.deinit();
    const first = try draft.fromFloat32Shape(&.{ 1, 1, 1, 1 }, &.{ 1, 4 });
    _ = try populated.rememberDraftEmbedding(0, first, std.math.maxInt(usize));
    const capacity = populated.draft_embedding_cache.capacity;
    for (1..capacity) |id| {
        const tensor = try draft.fromFloat32Shape(&.{ 2, 2, 2, 2 }, &.{ 1, 4 });
        _ = try populated.rememberDraftEmbedding(@intCast(id), tensor, std.math.maxInt(usize));
    }
    const rejected = try draft.fromFloat32Shape(&.{ 3, 3, 3, 3 }, &.{ 1, 4 });
    defer draft.free(rejected);
    try std.testing.expectError(error.OutOfMemory, populated.rememberDraftEmbedding(@intCast(capacity), rejected, std.math.maxInt(usize)));
    try std.testing.expectEqual(capacity, populated.draft_embedding_cache.items.len);
    const first_values = try draft.toFloat32(populated.cachedDraftEmbedding(0).?, a);
    defer a.free(first_values);
    try std.testing.expectEqualSlices(f32, &.{ 1, 1, 1, 1 }, first_values);
    const rejected_values = try draft.toFloat32(rejected, a);
    defer a.free(rejected_values);
    try std.testing.expectEqualSlices(f32, &.{ 3, 3, 3, 3 }, rejected_values);

    const Oom = struct {
        fn run(allocator: std.mem.Allocator, cb: *const ComputeBackend) !void {
            var owned = State{ .allocator = allocator, .draft_cb = cb };
            defer owned.deinit();
            const tensor = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 1, 4 });
            errdefer cb.free(tensor);
            _ = try owned.rememberDraftEmbedding(1, tensor, 2);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Oom.run, .{draft});
}

test "speculative activation owner preserves actual host device provenance and bounded embedding tensors" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(populateActivationWeights, checkActualActivationOwner);
}

test "speculative activation owner retains actual Metal embeddings matching Native model values" {
    try @import("../ops/media_residency_test.zig").onMetal(populateActivationWeights, checkActualActivationOwner);
}

fn populateRoundWeights(store: *@import("../ops/native_compute.zig").WeightStore) !void {
    try populateActivationWeights(store);
    const fixture = @import("../ops/media_residency_test.zig");
    try fixture.putWeight(store, "pre_projection.weight", &.{ 4, 8 }, null);
    try fixture.putWeight(store, "post_projection.weight", &.{ 4, 4 }, null);
}

fn checkActualDraftRound(cb_value: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cb = &cb_value;
    const mtp = @import("../architectures/gemma4_mtp.zig");
    const target_config = gpt_mod.Config{ .family = .llama, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .vocab_size = 4, .weight_tying = true, .norm_type = .rms_norm, .position_encoding = .rope };
    var draft_config = target_config;
    draft_config.gemma4_mtp_assistant = true;
    draft_config.mtp_backbone_hidden_size = 4;
    const decode = gpt_arch.DecodeContext{ .total_sequence_len = 1, .query_sequence_len = 1, .kv_sequence_len = 1, .attention_mode = .full_recompute };
    const seed = [_]f32{ 1, -2, 3, -4 };
    const expected0 = try mtp.draftToken(.{ .allocator = a, .target_cb = cb, .draft_cb = cb, .target_config = target_config, .draft_config = draft_config, .token_id = 1, .activation = &seed, .decode_context = &decode, .debug_top_k = 1 });
    defer a.free(expected0.projected_activation);
    defer if (expected0.logits) |values| a.free(values);
    const expected1 = try mtp.draftToken(.{ .allocator = a, .target_cb = cb, .draft_cb = cb, .target_config = target_config, .draft_config = draft_config, .token_id = @intCast(expected0.token), .activation = expected0.projected_activation, .decode_context = &decode, .debug_top_k = 1 });
    defer a.free(expected1.projected_activation);
    defer if (expected1.logits) |values| a.free(values);
    var output_allocator = std.testing.FailingAllocator.init(a, .{});
    const target = decoder_forward.Runtime{ .context = .{ .allocator = a, .cb = cb, .gpt_config = &target_config, .weight_semantics = .traced_decoder } };
    const draft = decoder_forward.Runtime{ .context = .{ .allocator = a, .cb = cb, .gpt_config = &draft_config, .weight_semantics = .traced_decoder } };
    var state = State{ .allocator = output_allocator.allocator(), .draft_cb = cb };
    var state_live = true;
    defer if (state_live) state.deinit();
    state.replaceHost(try output_allocator.allocator().dupe(f32, &seed));
    var round = state.beginRound(target, draft, true);
    var round_live = true;
    defer if (round_live) round.deinit();
    const actual0 = try round.step(.{ .index = 0, .token_id = 1, .decode_context = &decode, .resident = false, .debug_top_k = 1, .profile_enabled = false, .profile_sync = false });
    try std.testing.expectEqual(expected0.token, actual0.token);
    try std.testing.expectEqualSlices(f32, expected0.projected_activation, round.host_steps[0].?);
    try std.testing.expectEqualSlices(f32, expected0.logits.?, round.logits[0].?);
    // Fail a real host allocation during the next original assistant call.
    // The earlier retained activation/logits remain owned and unmodified.
    output_allocator.fail_index = output_allocator.alloc_index;
    try std.testing.expectError(error.OutOfMemory, round.step(.{ .index = 1, .token_id = @intCast(actual0.token), .decode_context = &decode, .resident = false, .debug_top_k = 1, .profile_enabled = false, .profile_sync = false }));
    try std.testing.expect(round.host_steps[1] == null and round.logits[1] == null);
    try std.testing.expectEqualSlices(f32, expected0.projected_activation, round.host_steps[0].?);
    output_allocator.fail_index = std.math.maxInt(usize);
    const actual1 = try round.step(.{ .index = 1, .token_id = @intCast(actual0.token), .decode_context = &decode, .resident = false, .debug_top_k = 1, .profile_enabled = false, .profile_sync = false });
    try std.testing.expectEqual(expected1.token, actual1.token);
    try std.testing.expectEqualSlices(f32, expected1.projected_activation, round.host_steps[1].?);
    try std.testing.expectEqualSlices(f32, expected1.logits.?, round.logits[1].?);
    try std.testing.expectEqualSlices(f32, expected0.projected_activation, round.host_steps[0].?);
    try round.retainStep(0, false);
    try std.testing.expectError(error.MissingGemma4MtpActivation, round.retainStep(0, false));
    round.publishActivation();
    round.deinit();
    round_live = false;
    try std.testing.expectEqualSlices(f32, expected0.projected_activation, state.host.?);
    state.deinit();
    state_live = false;
    try std.testing.expect(output_allocator.allocated_bytes > 0);
    try std.testing.expectEqual(output_allocator.allocated_bytes, output_allocator.freed_bytes);
}

test "predictor round invokes original Native assistant and retains independent producer allocations through failure and handoff" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(populateRoundWeights, checkActualDraftRound);
}

test "predictor round invokes original Metal assistant with unchanged host chain and retained earlier rows" {
    try @import("../ops/media_residency_test.zig").onMetal(populateRoundWeights, checkActualDraftRound);
}

fn checkActualResidentDraftRound(cb_value: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cb = &cb_value;
    const mtp = @import("../architectures/gemma4_mtp.zig");
    if (!mtp.deviceResidentDraftAllowed(0)) return error.SkipZigTest;
    const config = gpt_mod.Config{ .family = .llama, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .vocab_size = 4, .weight_tying = true, .norm_type = .rms_norm, .position_encoding = .rope };
    var draft_config = config;
    draft_config.gemma4_mtp_assistant = true;
    draft_config.mtp_backbone_hidden_size = 4;
    const decode = gpt_arch.DecodeContext{ .total_sequence_len = 1, .query_sequence_len = 1, .kv_sequence_len = 1, .attention_mode = .full_recompute };
    const weight = try gpt_arch.getEmbeddingWeight(cb, config);
    defer cb.free(weight);
    const embedding0 = try cb.embeddingLookup(weight, &.{1}, 1, 4);
    defer cb.free(embedding0);
    var state = State{ .allocator = a, .draft_cb = cb };
    defer state.deinit();
    state.replaceDevice(try cb.fromFloat32Shape(&.{ 1, -2, 3, -4 }, &.{ 1, 4 }));
    const expected0 = try mtp.draftTokenDevice(.{ .allocator = a, .target_cb = cb, .draft_cb = cb, .target_config = config, .draft_config = draft_config, .token_id = 1, .activation = state.device.?, .target_embedding_on_draft = embedding0, .decode_context = &decode });
    defer cb.free(expected0.projected_activation);
    defer if (expected0.logits) |values| a.free(values);
    const embedding1 = try cb.embeddingLookup(weight, &.{@intCast(expected0.token)}, 1, 4);
    defer cb.free(embedding1);
    const expected1 = try mtp.draftTokenDevice(.{ .allocator = a, .target_cb = cb, .draft_cb = cb, .target_config = config, .draft_config = draft_config, .token_id = @intCast(expected0.token), .activation = expected0.projected_activation, .target_embedding_on_draft = embedding1, .decode_context = &decode });
    defer cb.free(expected1.projected_activation);
    defer if (expected1.logits) |values| a.free(values);
    const target = decoder_forward.Runtime{ .context = .{ .allocator = a, .cb = cb, .gpt_config = &config, .weight_semantics = .traced_decoder } };
    const draft = decoder_forward.Runtime{ .context = .{ .allocator = a, .cb = cb, .gpt_config = &draft_config, .weight_semantics = .traced_decoder } };
    var round = state.beginRound(target, draft, true);
    var round_live = true;
    defer if (round_live) round.deinit();
    const actual0 = try round.step(.{ .index = 0, .token_id = 1, .decode_context = &decode, .resident = true, .target_embedding_on_draft = embedding0, .debug_top_k = 0, .profile_enabled = false, .profile_sync = false });
    const actual1 = try round.step(.{ .index = 1, .token_id = @intCast(actual0.token), .decode_context = &decode, .resident = true, .target_embedding_on_draft = embedding1, .debug_top_k = 0, .profile_enabled = false, .profile_sync = false });
    try std.testing.expectEqual(expected0.token, actual0.token);
    try std.testing.expectEqual(expected1.token, actual1.token);
    const expected_first = try cb.toFloat32(expected0.projected_activation, a);
    defer a.free(expected_first);
    const expected_second = try cb.toFloat32(expected1.projected_activation, a);
    defer a.free(expected_second);
    const actual_first = try cb.toFloat32(round.device_steps[0].?, a);
    defer a.free(actual_first);
    const actual_second = try cb.toFloat32(round.device_steps[1].?, a);
    defer a.free(actual_second);
    try std.testing.expectEqualSlices(f32, expected_first, actual_first);
    try std.testing.expectEqualSlices(f32, expected_second, actual_second);
    try round.retainStep(0, true);
    try std.testing.expectError(error.MissingGemma4MtpActivation, round.retainStep(0, true));
    round.publishActivation();
    round.deinit();
    round_live = false;
    const held = try cb.toFloat32(state.device.?, a);
    defer a.free(held);
    try std.testing.expectEqualSlices(f32, expected_first, held);
}

test "predictor round preserves actual Metal resident assistant schedule and one-time deferred activation handoff" {
    try @import("../ops/media_residency_test.zig").onMetal(populateRoundWeights, checkActualResidentDraftRound);
}
