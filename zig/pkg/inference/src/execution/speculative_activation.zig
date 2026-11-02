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
