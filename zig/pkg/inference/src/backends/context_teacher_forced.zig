// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded, sampler-independent teacher-forced quality evidence.
//! The model pipeline owns context mutation; this module only validates and
//! scores one raw post-softcap vocabulary row at a time.
const std = @import("std");

pub const min_reference_tokens: usize = 2;
pub const max_reference_tokens: usize = 4096;

pub const Mode = enum { generation, teacher };
pub const Variant = enum { exact, attention_matching };
pub const ScoreSource = enum { prefill_final_row, exact_context_continuation, published_compacted_context };

/// Borrowed, parent-sealed request. The profile and implementation fields are
/// copied from the selected loaded-model authority before reference tokens are
/// exposed to the pipeline.
pub const ReferenceProvider = struct {
    ctx: *anyopaque,
    reveal_fn: *const fn (ctx: *anyopaque) anyerror![]const i64,

    pub fn reveal(self: ReferenceProvider) ![]const i64 {
        return self.reveal_fn(self.ctx);
    }
};

/// The generation sibling carries only the predeclared reference digest. The
/// teacher sibling receives the generation binding and an opaque provider;
/// the pipeline must validate the two frozen contexts before calling reveal.
/// This prevents reference tokens from entering selection, study, fitting, or
/// publication code paths.
pub const Request = struct {
    mode: Mode,
    variant: Variant,
    reference_token_count: u32,
    reference_sequence_sha256: [32]u8,
    rendered_prompt_sha256: [32]u8,
    loaded_generation: u64,
    profile_artifact_sha256: [32]u8,
    profile_semantic_sha256: [32]u8,
    implementation_sha256: [32]u8,
    generation_sibling: ?ContextBinding = null,
    reference_provider: ?ReferenceProvider = null,
};

pub const TokenScore = struct {
    sequence_position: u32,
    token_id: u32,
    /// Null only for the first token, whose conditioning state is the sealed
    /// rendered prompt. Later rows name the exact preceding teacher token.
    conditioned_on_token_id: ?u32,
    log_probability: f64,
    source: ScoreSource,
};

pub const QualityFingerprint = struct {
    context_instance_id: u64,
    publication_epoch: u64,
    logical_tokens: u32,
    payload_sha256: [32]u8,
    descriptor_sha256: [32]u8,
    diagnostic_kv_readback_bytes: u64,
};

pub const ContextBinding = struct {
    variant: Variant,
    context_instance_id: u64,
    publication_epoch: u64,
    context_logical_tokens: u32,
    prompt_tokens: u32,
    rendered_prompt_sha256: [32]u8,
    prompt_tokens_sha256: [32]u8,
    reference_token_count: u32,
    reference_sequence_sha256: [32]u8,
    loaded_generation: u64,
    profile_artifact_sha256: [32]u8,
    profile_semantic_sha256: [32]u8,
    implementation_sha256: [32]u8,
    published_payload_sha256: [32]u8,
    published_descriptor_sha256: [32]u8,
    diagnostic_kv_readback_bytes: u64,
};

pub fn validateSiblingContexts(generation: ContextBinding, teacher: ContextBinding) !void {
    if (generation.context_instance_id == 0 or teacher.context_instance_id == 0 or
        generation.context_instance_id == teacher.context_instance_id or
        generation.variant != teacher.variant or
        generation.publication_epoch == 0 or generation.publication_epoch != teacher.publication_epoch or
        generation.context_logical_tokens == 0 or generation.context_logical_tokens != teacher.context_logical_tokens or
        generation.prompt_tokens == 0 or generation.prompt_tokens != teacher.prompt_tokens or
        generation.context_logical_tokens != generation.prompt_tokens or
        @as(usize, generation.reference_token_count) < min_reference_tokens or
        generation.reference_token_count != teacher.reference_token_count or
        generation.loaded_generation == 0 or generation.loaded_generation != teacher.loaded_generation or
        !std.mem.eql(u8, &generation.rendered_prompt_sha256, &teacher.rendered_prompt_sha256) or
        !std.mem.eql(u8, &generation.prompt_tokens_sha256, &teacher.prompt_tokens_sha256) or
        !std.mem.eql(u8, &generation.reference_sequence_sha256, &teacher.reference_sequence_sha256) or
        !std.mem.eql(u8, &generation.profile_artifact_sha256, &teacher.profile_artifact_sha256) or
        !std.mem.eql(u8, &generation.profile_semantic_sha256, &teacher.profile_semantic_sha256) or
        !std.mem.eql(u8, &generation.implementation_sha256, &teacher.implementation_sha256) or
        !std.mem.eql(u8, &generation.published_payload_sha256, &teacher.published_payload_sha256) or
        !std.mem.eql(u8, &generation.published_descriptor_sha256, &teacher.published_descriptor_sha256) or
        std.mem.allEqual(u8, &generation.published_payload_sha256, 0) or
        std.mem.allEqual(u8, &generation.published_descriptor_sha256, 0) or
        generation.diagnostic_kv_readback_bytes == 0 or
        generation.diagnostic_kv_readback_bytes != teacher.diagnostic_kv_readback_bytes)
        return error.TeacherForcedSiblingMismatch;
}

pub fn revealVerifiedReference(request: Request, teacher: ContextBinding) ![]const i64 {
    if (request.mode != .teacher or request.variant != teacher.variant or
        @as(usize, request.reference_token_count) < min_reference_tokens or
        @as(usize, request.reference_token_count) > max_reference_tokens or
        !std.mem.eql(u8, &request.reference_sequence_sha256, &teacher.reference_sequence_sha256) or
        !std.mem.eql(u8, &request.rendered_prompt_sha256, &teacher.rendered_prompt_sha256) or
        request.loaded_generation != teacher.loaded_generation or
        !std.mem.eql(u8, &request.profile_artifact_sha256, &teacher.profile_artifact_sha256) or
        !std.mem.eql(u8, &request.profile_semantic_sha256, &teacher.profile_semantic_sha256) or
        !std.mem.eql(u8, &request.implementation_sha256, &teacher.implementation_sha256))
        return error.InvalidTeacherForcedRequest;
    const generation = request.generation_sibling orelse return error.InvalidTeacherForcedRequest;
    try validateSiblingContexts(generation, teacher);
    const provider = request.reference_provider orelse return error.InvalidTeacherForcedRequest;
    const reference = try provider.reveal();
    if (reference.len != @as(usize, request.reference_token_count) or
        !std.mem.eql(u8, &(try referenceSequenceDigest(reference)), &request.reference_sequence_sha256))
        return error.InvalidTeacherForcedReference;
    return reference;
}

pub const Evidence = union(Mode) {
    generation: ContextBinding,
    teacher: Receipt,

    pub fn deinit(self: *Evidence) void {
        switch (self.*) {
            .generation => {},
            .teacher => |*receipt| receipt.deinit(),
        }
        self.* = undefined;
    }
};

pub const Receipt = struct {
    allocator: std.mem.Allocator,
    binding: ContextBinding,
    rows: []TokenScore,
    elapsed_ns: u64,
    continuation_forward_steps: u32,
    published_compaction_forward_steps: u32,

    pub fn deinit(self: *Receipt) void {
        self.allocator.free(self.rows);
        self.* = undefined;
    }
};

pub fn referenceSequenceDigest(reference: []const i64) ![32]u8 {
    if (reference.len < min_reference_tokens or reference.len > max_reference_tokens)
        return error.InvalidTeacherForcedReference;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (reference) |token| {
        if (token < 0 or token > std.math.maxInt(u32)) return error.InvalidTeacherForcedToken;
        var encoded: [4]u8 = undefined;
        std.mem.writeInt(u32, &encoded, @intCast(token), .little);
        hash.update(&encoded);
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

/// Score an unmodified raw model row. Sampling temperature, top-k/top-p,
/// penalties, EOS handling, grammar masks, and suppress-token lists do not
/// enter perplexity evidence.
pub fn promptTokenDigest(tokens: []const i64) ![32]u8 {
    if (tokens.len == 0) return error.InvalidTeacherForcedRequest;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (tokens) |token| {
        if (token < 0) return error.InvalidTeacherForcedToken;
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(i64, &encoded, token, .little);
        hash.update(&encoded);
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub fn logProbability(logits: []const f32, target: u32) !f64 {
    if (logits.len == 0 or target >= logits.len) return error.InvalidTeacherForcedToken;
    var maximum = -std.math.inf(f64);
    for (logits) |raw| {
        if (!std.math.isFinite(raw)) return error.NonFiniteTeacherForcedLogits;
        maximum = @max(maximum, @as(f64, raw));
    }
    var total: f64 = 0;
    for (logits) |raw| total += @exp(@as(f64, raw) - maximum);
    if (!std.math.isFinite(total) or total <= 0) return error.NonFiniteTeacherForcedLogits;
    const result = @as(f64, logits[target]) - maximum - @log(total);
    if (!std.math.isFinite(result) or result > 0) return error.NonFiniteTeacherForcedLogits;
    return result;
}

pub fn validateRows(variant: Variant, reference: []const i64, rows: []const TokenScore) !void {
    if (reference.len < min_reference_tokens or reference.len > max_reference_tokens or rows.len != reference.len)
        return error.InvalidTeacherForcedReceipt;
    for (reference, rows, 0..) |token, row, index| {
        if (token < 0 or token > std.math.maxInt(u32) or
            row.sequence_position != index or row.token_id != @as(u32, @intCast(token)) or
            !std.math.isFinite(row.log_probability) or row.log_probability > 0)
            return error.InvalidTeacherForcedReceipt;
        const expected_previous: ?u32 = if (index == 0) null else @intCast(reference[index - 1]);
        const expected_source: ScoreSource = if (index == 0)
            .prefill_final_row
        else if (variant == .attention_matching)
            .published_compacted_context
        else
            .exact_context_continuation;
        if (row.conditioned_on_token_id != expected_previous or row.source != expected_source)
            return error.InvalidTeacherForcedReceipt;
    }
}

pub fn validateReceipt(receipt: Receipt, reference: []const i64) !void {
    try validateRows(receipt.binding.variant, reference, receipt.rows);
    const expected_steps = std.math.cast(u32, reference.len - 1) orelse
        return error.InvalidTeacherForcedReceipt;
    const expected_published_steps: u32 = if (receipt.binding.variant == .attention_matching) expected_steps else 0;
    if (@as(usize, receipt.binding.reference_token_count) != reference.len or
        !std.mem.eql(u8, &receipt.binding.reference_sequence_sha256, &(try referenceSequenceDigest(reference))) or
        receipt.elapsed_ns == 0 or receipt.continuation_forward_steps != expected_steps or
        receipt.published_compaction_forward_steps != expected_published_steps or
        (receipt.binding.variant == .attention_matching and receipt.published_compaction_forward_steps == 0))
        return error.InvalidTeacherForcedReceipt;
}

test "context teacher forced log probability is stable and sampler independent" {
    const logits = [_]f32{ 10000, 9999, 9998 };
    const got = try logProbability(&logits, 1);
    const expected = -1.4076059644443804;
    try std.testing.expectApproxEqAbs(expected, got, 1e-12);
    try std.testing.expectError(error.NonFiniteTeacherForcedLogits, logProbability(&.{ 0, std.math.nan(f32) }, 0));
    try std.testing.expectError(error.InvalidTeacherForcedToken, logProbability(&.{0}, 1));
}

test "context teacher forced receipt proves the exact conditional chain" {
    const reference = [_]i64{ 7, 9, 11 };
    const rows = [_]TokenScore{
        .{ .sequence_position = 0, .token_id = 7, .conditioned_on_token_id = null, .log_probability = -0.5, .source = .prefill_final_row },
        .{ .sequence_position = 1, .token_id = 9, .conditioned_on_token_id = 7, .log_probability = -1.5, .source = .published_compacted_context },
        .{ .sequence_position = 2, .token_id = 11, .conditioned_on_token_id = 9, .log_probability = -2.5, .source = .published_compacted_context },
    };
    try validateRows(.attention_matching, &reference, &rows);
    var forged = rows;
    forged[2].conditioned_on_token_id = 7;
    try std.testing.expectError(error.InvalidTeacherForcedReceipt, validateRows(.attention_matching, &reference, &forged));
    var receipt = Receipt{
        .allocator = std.testing.allocator,
        .binding = .{
            .variant = .attention_matching,
            .context_instance_id = 1,
            .publication_epoch = 2,
            .context_logical_tokens = 10,
            .prompt_tokens = 10,
            .rendered_prompt_sha256 = @splat(1),
            .prompt_tokens_sha256 = @splat(2),
            .reference_token_count = @intCast(reference.len),
            .reference_sequence_sha256 = try referenceSequenceDigest(&reference),
            .loaded_generation = 3,
            .profile_artifact_sha256 = @splat(4),
            .profile_semantic_sha256 = @splat(5),
            .implementation_sha256 = @splat(6),
            .published_payload_sha256 = @splat(7),
            .published_descriptor_sha256 = @splat(8),
            .diagnostic_kv_readback_bytes = 9,
        },
        .rows = @constCast(rows[0..]),
        .elapsed_ns = 1,
        .continuation_forward_steps = 2,
        .published_compaction_forward_steps = 2,
    };
    try validateReceipt(receipt, &reference);
    receipt.published_compaction_forward_steps = 1;
    try std.testing.expectError(error.InvalidTeacherForcedReceipt, validateReceipt(receipt, &reference));
}

test "context teacher forced reference digest binds order and length" {
    const a = try referenceSequenceDigest(&.{ 1, 2 });
    const b = try referenceSequenceDigest(&.{ 2, 1 });
    const c = try referenceSequenceDigest(&.{ 1, 2, 0 });
    try std.testing.expect(!std.mem.eql(u8, &a, &b));
    try std.testing.expect(!std.mem.eql(u8, &a, &c));
    try std.testing.expectError(error.InvalidTeacherForcedReference, referenceSequenceDigest(&.{}));
}

test "context teacher forced siblings require distinct contexts with identical published payloads" {
    const base = ContextBinding{
        .variant = .attention_matching,
        .context_instance_id = 1,
        .publication_epoch = 2,
        .context_logical_tokens = 32,
        .prompt_tokens = 32,
        .rendered_prompt_sha256 = @splat(13),
        .prompt_tokens_sha256 = @splat(1),
        .reference_token_count = 2,
        .reference_sequence_sha256 = @splat(2),
        .loaded_generation = 3,
        .profile_artifact_sha256 = @splat(4),
        .profile_semantic_sha256 = @splat(5),
        .implementation_sha256 = @splat(6),
        .published_payload_sha256 = @splat(7),
        .published_descriptor_sha256 = @splat(8),
        .diagnostic_kv_readback_bytes = 9,
    };
    var sibling = base;
    sibling.context_instance_id = 10;
    try validateSiblingContexts(base, sibling);
    sibling.published_payload_sha256 = @splat(11);
    try std.testing.expectError(error.TeacherForcedSiblingMismatch, validateSiblingContexts(base, sibling));
    sibling = base;
    sibling.context_instance_id = 10;
    sibling.loaded_generation = 12;
    try std.testing.expectError(error.TeacherForcedSiblingMismatch, validateSiblingContexts(base, sibling));
    sibling = base;
    sibling.context_instance_id = 10;
    sibling.diagnostic_kv_readback_bytes = 0;
    try std.testing.expectError(error.TeacherForcedSiblingMismatch, validateSiblingContexts(base, sibling));
}

test "context teacher forced reference remains hidden until sibling validation" {
    const Provider = struct {
        called: bool = false,
        reference: []const i64,

        fn reveal(raw: *anyopaque) ![]const i64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.called = true;
            return self.reference;
        }
    };
    const reference = [_]i64{ 3, 5 };
    const digest = try referenceSequenceDigest(&reference);
    const generation = ContextBinding{
        .variant = .attention_matching,
        .context_instance_id = 1,
        .publication_epoch = 2,
        .context_logical_tokens = 32,
        .prompt_tokens = 32,
        .rendered_prompt_sha256 = @splat(13),
        .prompt_tokens_sha256 = @splat(1),
        .reference_token_count = @intCast(reference.len),
        .reference_sequence_sha256 = digest,
        .loaded_generation = 3,
        .profile_artifact_sha256 = @splat(4),
        .profile_semantic_sha256 = @splat(5),
        .implementation_sha256 = @splat(6),
        .published_payload_sha256 = @splat(7),
        .published_descriptor_sha256 = @splat(8),
        .diagnostic_kv_readback_bytes = 9,
    };
    var teacher = generation;
    teacher.context_instance_id = 10;
    var provider = Provider{ .reference = &reference };
    const request = Request{
        .mode = .teacher,
        .variant = .attention_matching,
        .reference_token_count = @intCast(reference.len),
        .reference_sequence_sha256 = digest,
        .rendered_prompt_sha256 = generation.rendered_prompt_sha256,
        .loaded_generation = generation.loaded_generation,
        .profile_artifact_sha256 = generation.profile_artifact_sha256,
        .profile_semantic_sha256 = generation.profile_semantic_sha256,
        .implementation_sha256 = generation.implementation_sha256,
        .generation_sibling = generation,
        .reference_provider = .{ .ctx = &provider, .reveal_fn = Provider.reveal },
    };
    var mismatched = teacher;
    mismatched.published_payload_sha256 = @splat(99);
    try std.testing.expectError(error.TeacherForcedSiblingMismatch, revealVerifiedReference(request, mismatched));
    try std.testing.expect(!provider.called);
    const revealed = try revealVerifiedReference(request, teacher);
    try std.testing.expect(provider.called);
    try std.testing.expectEqualSlices(i64, &reference, revealed);
}
