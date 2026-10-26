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

//! Test-campaign coverage derived from the exact retained prompt owner. This
//! neither re-renders the template nor retokenizes the request. The caller
//! writes these bytes beside the actual model capture; Python reopens both.
const std = @import("std");
const generation = @import("pipelines/generation.zig");
const tokenizer_mod = @import("inference_tokenizer");
const payload = @import("runtime/kv/payload.zig");
const policy_mod = @import("runtime/context_policy.zig");
const profiles = @import("runtime/kv/compaction_profile.zig");
const studies = @import("backends/context_study_templates.zig");

pub const required_prefill_chunk_rows: u32 = 64;

pub const Bucket = enum { document_qa, exact_retrieval, code, structured_tool, multiturn };

pub const Document = struct {
    id: []const u8,
    sha256: [32]u8,
    bucket: Bucket,
};

pub const RequestBinding = struct {
    session_id: [32]u8,
    request_index: u32,
    question_sha256: [32]u8,
    request_content_sha256: [32]u8,
};

const Input = struct {
    request: RequestBinding,
    document: Document,
    policy: policy_mod.Policy,
    rendered_prompt: []const u8,
    ids: []const i32,
    attention_mask: []const i32,
    offsets: []const [2]u32,
    rendered_labels: []const u32,
    eligible: *const payload.Eligible,
    question: *const generation.AttributedCompactionQuestion,
    fit_study: *const generation.PreparedCompactionStudy,
    gate_study: *const generation.PreparedCompactionStudy,
    reserved_output_tokens: u32,
    admitted_prefill_chunk_rows: u32,
    load_generation: u64,
    identity: profiles.Identity,
    binary_sha256: [32]u8,
    source_build_sha256: [32]u8,
    elapsed_ns: u64,
    status_readback_bytes: u64,
    kv_readback_bytes: u64,
};

pub const Evidence = struct {
    pub const Study = struct {
        tokens_i64_le: []u8,
        token_count: u32,
        prompt_tokens_sha256: [32]u8,
        rendered_suffix_sha256: [32]u8,
        template_bytes: []const u8,
        template_sha256: [32]u8,
        generation_contract_bytes: []const u8,
        generation_contract_sha256: [32]u8,
        response_tokens: u32,
        generation_mode: profiles.QueryGenerationMode,

        fn deinit(self: *Study, allocator: std.mem.Allocator) void {
            allocator.free(self.tokens_i64_le);
            self.* = undefined;
        }
    };

    allocator: std.mem.Allocator,
    receipt_json: []u8,
    rendered_prompt: []u8,
    prompt_tokens_i64_le: []u8,
    /// Exact `[start, end)` byte offsets retained beside the IDs and mask that
    /// generation consumed. Each pair is two little-endian u32s.
    prompt_token_offsets_u32_le: []u8,
    question_tokens_i64_le: []u8,
    request: RequestBinding,
    question_source_start: u32,
    question_source_end: u32,
    load_generation: u64,
    identity: profiles.Identity,
    provenance: Provenance,
    fit_study: Study,
    gate_study: Study,

    pub fn deinit(self: *Evidence) void {
        self.allocator.free(self.receipt_json);
        self.allocator.free(self.rendered_prompt);
        self.allocator.free(self.prompt_tokens_i64_le);
        self.allocator.free(self.prompt_token_offsets_u32_le);
        self.allocator.free(self.question_tokens_i64_le);
        self.fit_study.deinit(self.allocator);
        self.gate_study.deinit(self.allocator);
        self.* = undefined;
    }
};

/// Test-campaign callback with explicit ownership transfer. A successful call
/// gives the complete `Evidence` allocation to the receiver, which must
/// eventually call `deinit`. This prevents a serving hook from discarding a
/// valid receipt after observing it.
pub const EvidenceSink = struct {
    ctx: *anyopaque,
    complete_fn: *const fn (*anyopaque, Evidence) void,

    pub fn complete(self: EvidenceSink, evidence: Evidence) void {
        self.complete_fn(self.ctx, evidence);
    }
};

pub const Provenance = struct {
    binary_sha256: [32]u8,
    source_build_sha256: [32]u8,
};

pub const Completion = struct {
    elapsed_ns: u64,
    status_readback_bytes: u64,
    kv_readback_bytes: u64,
};

const Part = struct {
    message_index: u32,
    part_index: u32,
    label: u32,
    token_start: u32,
    token_end: u32,
};
const Range = struct {
    message_index: u32,
    part_index: u32,
    label: u32,
    start: u32,
    end: u32,
};

fn nonzero(digest: [32]u8) bool {
    return !std.mem.allEqual(u8, &digest, 0);
}

fn captureStudy(
    allocator: std.mem.Allocator,
    kind: studies.Kind,
    study: *const generation.PreparedCompactionStudy,
    prompt_tokens: u32,
    prompt_tokens_sha256: [32]u8,
) !Evidence.Study {
    const authority = studies.authority(kind);
    try studies.validate(kind, authority);
    if (study.ids.len == 0 or study.ids.len > @import("backends/context_study_runner.zig").max_prefill_tokens or
        study.prompt_tokens != prompt_tokens or
        !std.mem.eql(u8, &study.prompt_tokens_sha256, &prompt_tokens_sha256) or
        !std.mem.eql(u8, study.template_bytes, studies.template(kind)) or
        !std.mem.eql(u8, study.generation_contract_bytes, studies.generation_contract))
        return error.InvalidCoverageStudy;
    var template_sha256: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(study.template_bytes, &template_sha256, .{});
    var contract_sha256: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(study.generation_contract_bytes, &contract_sha256, .{});
    if (!std.mem.eql(u8, &template_sha256, &authority.template_sha256) or
        !std.mem.eql(u8, &contract_sha256, &authority.generation_contract_sha256))
        return error.InvalidCoverageStudy;
    const bytes = try allocator.alloc(u8, try std.math.mul(usize, study.ids.len, 8));
    errdefer allocator.free(bytes);
    for (study.ids, 0..) |id, index| {
        if (id < 0) return error.InvalidCoverageStudy;
        std.mem.writeInt(i64, bytes[index * 8 ..][0..8], id, .little);
    }
    return .{
        .tokens_i64_le = bytes,
        .token_count = @intCast(study.ids.len),
        .prompt_tokens_sha256 = study.prompt_tokens_sha256,
        .rendered_suffix_sha256 = study.rendered_suffix_sha256,
        .template_bytes = studies.template(kind),
        .template_sha256 = template_sha256,
        .generation_contract_bytes = studies.generation_contract,
        .generation_contract_sha256 = contract_sha256,
        .response_tokens = authority.response_tokens,
        .generation_mode = authority.generation_mode,
    };
}

/// Re-derive ranges from the retained token labels and compare with the very
/// block list consumed by the runtime. A caller-supplied block subset cannot
/// produce a complete receipt. Parent source/binary bindings are checked again
/// by the external collector, and never authorize approximate execution.
fn capture(allocator: std.mem.Allocator, input: Input) !Evidence {
    try input.policy.validate();
    if (input.policy.mode != .attention_matching or input.policy.compaction_targets.count == 0 or
        input.document.id.len == 0 or input.document.id.len > 1024 or input.rendered_prompt.len == 0 or
        !nonzero(input.request.session_id) or
        !nonzero(input.document.sha256) or !nonzero(input.binary_sha256) or !nonzero(input.source_build_sha256) or
        !nonzero(input.request.question_sha256) or !nonzero(input.request.request_content_sha256) or
        input.load_generation == 0 or input.reserved_output_tokens == 0 or
        input.admitted_prefill_chunk_rows != required_prefill_chunk_rows or input.elapsed_ns == 0 or
        input.status_readback_bytes == 0 or input.kv_readback_bytes != 0)
        return error.InvalidCoverageObservation;
    inline for (.{ "artifact", "tokenizer", "template", "layout", "runtime" }) |field|
        if (!nonzero(@field(input.identity, field))) return error.InvalidCoverageObservation;
    if (input.ids.len != input.attention_mask.len or input.ids.len != input.offsets.len or
        input.rendered_prompt.len != input.rendered_labels.len)
        return error.InvalidPayloadProvenance;
    var count: usize = 0;
    while (count < input.attention_mask.len and input.attention_mask[count] == 1) : (count += 1) {}
    for (input.attention_mask[count..]) |mask| if (mask != 0) return error.InvalidPayloadProvenance;
    if (count == 0 or count > 131072 or count != input.eligible.labels.len)
        return error.InvalidPayloadProvenance;
    const qualified = std.math.add(u32, @intCast(count), input.reserved_output_tokens) catch
        return error.InvalidCoverageObservation;
    const public_limit = input.policy.max_context_tokens orelse return error.InvalidCoverageObservation;
    if (qualified > 131072 or qualified > public_limit)
        return error.InvalidCoverageObservation;

    const targets = input.policy.compaction_targets.slice();
    const parts = try allocator.alloc(Part, targets.len);
    defer allocator.free(parts);
    for (targets, parts, 0..) |target, *part, index| part.* = .{
        .message_index = target.message_index,
        .part_index = target.part_index,
        .label = @intCast(index + 1),
        .token_start = std.math.maxInt(u32),
        .token_end = 0,
    };
    for (input.rendered_labels) |label| if (label > targets.len) return error.InvalidPayloadProvenance;
    // Bounds describe tokens wholly owned by a selected rendered part,
    // including its protected prefix/tail. Boundary-spanning tokens cannot
    // create ownership or be made eligible by an offline manifest.
    for (input.offsets[0..count], input.ids[0..count], 0..) |span, id, index| {
        if (id < 0 or span[0] > span[1] or span[1] > input.rendered_labels.len)
            return error.InvalidPayloadProvenance;
        var label: u32 = 0;
        if (span[0] < span[1]) {
            const first = input.rendered_labels[span[0]];
            if (first != 0 and std.mem.allEqual(u32, input.rendered_labels[span[0]..span[1]], first)) label = first;
        }
        const eligible = input.eligible.labels[index];
        if (eligible != 0 and eligible != label) return error.InvalidPayloadProvenance;
        if ((index < input.policy.keep_prefix_tokens or index >= count -| input.policy.keep_recent_tokens) and eligible != 0)
            return error.InvalidPayloadProvenance;
        if (label != 0) {
            const part = &parts[label - 1];
            part.token_start = @min(part.token_start, @as(u32, @intCast(index)));
            part.token_end = @max(part.token_end, @as(u32, @intCast(index + 1)));
        }
    }
    std.mem.sort(Part, parts, {}, struct {
        fn less(_: void, a: Part, b: Part) bool {
            return a.token_start < b.token_start;
        }
    }.less);
    var previous_end: u32 = 0;
    for (parts) |part| {
        if (part.token_start >= part.token_end or part.token_start < previous_end)
            return error.UnsupportedCoveragePayloadLayout;
        previous_end = part.token_end;
    }

    var ranges: std.ArrayList(Range) = .empty;
    defer ranges.deinit(allocator);
    var blocks: std.ArrayList(Range) = .empty;
    defer blocks.deinit(allocator);
    var position: usize = 0;
    while (position < count) {
        const label = input.eligible.labels[position];
        if (label == 0) {
            position += 1;
            continue;
        }
        if (label > targets.len) return error.InvalidPayloadProvenance;
        const start = position;
        while (position < count and input.eligible.labels[position] == label) : (position += 1) {}
        const target = targets[label - 1];
        try ranges.append(allocator, .{ .message_index = target.message_index, .part_index = target.part_index, .label = label, .start = @intCast(start), .end = @intCast(position) });
        var block = start;
        while (position - block >= 512) : (block += 512) try blocks.append(allocator, .{
            .message_index = target.message_index,
            .part_index = target.part_index,
            .label = label,
            .start = @intCast(block),
            .end = @intCast(block + 512),
        });
    }
    if (ranges.items.len != input.eligible.ranges.len or blocks.items.len != input.eligible.blocks.len)
        return error.InvalidPayloadProvenance;
    for (ranges.items, input.eligible.ranges) |actual, retained|
        if (actual.start != retained.start or actual.end != retained.end) return error.InvalidPayloadProvenance;
    for (blocks.items, input.eligible.blocks) |actual, retained|
        if (actual.start != retained.start or actual.end != retained.end) return error.InvalidPayloadProvenance;

    const token_bytes = try allocator.alloc(u8, count * 8);
    errdefer allocator.free(token_bytes);
    for (input.ids[0..count], 0..) |id, index|
        std.mem.writeInt(i64, token_bytes[index * 8 ..][0..8], id, .little);
    var token_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(token_bytes, &token_digest, .{});
    const offset_bytes = try allocator.alloc(u8, count * 8);
    errdefer allocator.free(offset_bytes);
    var previous_mapped_start: u32 = 0;
    var previous_mapped_end: u32 = 0;
    var saw_mapped_span = false;
    var saw_nonempty_span = false;
    for (input.offsets[0..count], 0..) |span, index| {
        if (span[0] > span[1] or span[1] > input.rendered_prompt.len)
            return error.InvalidPayloadProvenance;
        // Added/special tokens deliberately use the protected zero span.
        // Text-bearing spans preserve source order. Exact duplicates and
        // overlaps are valid for normalizer and byte-fallback token mappings.
        // `{0, 0}` is the protected special-token sentinel and does not
        // participate in text order. Other zero-width mappings are valid.
        if (!std.meta.eql(span, .{ 0, 0 })) {
            if (saw_mapped_span and
                (span[0] < previous_mapped_start or span[1] < previous_mapped_end))
                return error.InvalidPayloadProvenance;
            saw_mapped_span = true;
            previous_mapped_start = span[0];
            previous_mapped_end = span[1];
        }
        if (span[0] != span[1]) saw_nonempty_span = true;
        std.mem.writeInt(u32, offset_bytes[index * 8 ..][0..4], span[0], .little);
        std.mem.writeInt(u32, offset_bytes[index * 8 + 4 ..][0..4], span[1], .little);
    }
    if (!saw_nonempty_span) return error.InvalidPayloadProvenance;
    var offset_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(offset_bytes, &offset_digest, .{});
    const question = input.question;
    if (question.attribution != .nonselected_user_part or question.message_index != 0 or
        question.part_index != 1 or question.prompt_tokens != count or
        !std.mem.eql(u8, &question.prompt_tokens_sha256, &token_digest) or
        question.source_start >= question.source_end or question.source_end > count or
        question.ids.len != question.source_end - question.source_start)
        return error.InvalidCoverageQuestion;
    if (!std.mem.allEqual(u32, input.eligible.labels[question.source_start..question.source_end], 0))
        return error.InvalidCoverageQuestion;
    for (input.offsets[question.source_start..question.source_end]) |span|
        if (span[0] == span[1] or !std.mem.allEqual(u32, input.rendered_labels[span[0]..span[1]], 0))
            return error.InvalidCoverageQuestion;
    for (question.ids, input.ids[question.source_start..question.source_end]) |question_id, prompt_id|
        if (prompt_id < 0 or question_id != prompt_id) return error.InvalidCoverageQuestion;
    const question_token_bytes = try allocator.alloc(u8, question.ids.len * 8);
    errdefer allocator.free(question_token_bytes);
    for (question.ids, 0..) |id, index|
        std.mem.writeInt(i64, question_token_bytes[index * 8 ..][0..8], id, .little);
    var question_token_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(question_token_bytes, &question_token_digest, .{});
    var fit_study = try captureStudy(allocator, .fit, input.fit_study, @intCast(count), token_digest);
    errdefer fit_study.deinit(allocator);
    var gate_study = try captureStudy(allocator, .gate, input.gate_study, @intCast(count), token_digest);
    errdefer gate_study.deinit(allocator);
    const document_hex = std.fmt.bytesToHex(input.document.sha256, .lower);
    const token_hex = std.fmt.bytesToHex(token_digest, .lower);
    const offset_hex = std.fmt.bytesToHex(offset_digest, .lower);
    const question_token_hex = std.fmt.bytesToHex(question_token_digest, .lower);
    const artifact_hex = std.fmt.bytesToHex(input.identity.artifact, .lower);
    const tokenizer_hex = std.fmt.bytesToHex(input.identity.tokenizer, .lower);
    const template_hex = std.fmt.bytesToHex(input.identity.template, .lower);
    const layout_hex = std.fmt.bytesToHex(input.identity.layout, .lower);
    const runtime_hex = std.fmt.bytesToHex(input.identity.runtime, .lower);
    const binary_hex = std.fmt.bytesToHex(input.binary_sha256, .lower);
    const source_hex = std.fmt.bytesToHex(input.source_build_sha256, .lower);
    const session_hex = std.fmt.bytesToHex(input.request.session_id, .lower);
    const question_hex = std.fmt.bytesToHex(input.request.question_sha256, .lower);
    const request_content_hex = std.fmt.bytesToHex(input.request.request_content_sha256, .lower);
    var rendered_prompt_sha256: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(input.rendered_prompt, &rendered_prompt_sha256, .{});
    const rendered_prompt_hex = std.fmt.bytesToHex(rendered_prompt_sha256, .lower);
    const rendered_prompt = try allocator.dupe(u8, input.rendered_prompt);
    errdefer allocator.free(rendered_prompt);
    const receipt = try std.json.Stringify.valueAlloc(allocator, .{
        .schema_version = @as(u32, 2),
        .stage = "context_payload_block_coverage",
        .request = .{
            .session_id = session_hex[0..],
            .request_index = input.request.request_index,
            .question_sha256 = question_hex[0..],
            .request_content_sha256 = request_content_hex[0..],
        },
        .document = .{ .id = input.document.id, .sha256 = document_hex[0..], .bucket = @tagName(input.document.bucket) },
        .qualified_context_tokens = qualified,
        .prompt_tokens = count,
        .reserved_output_tokens = input.reserved_output_tokens,
        .admitted_prefill_chunk_rows = input.admitted_prefill_chunk_rows,
        .prompt_tokens_sha256 = token_hex[0..],
        .prompt_token_offsets_sha256 = offset_hex[0..],
        .rendered_prompt_sha256 = rendered_prompt_hex[0..],
        .question = .{
            .message_index = question.message_index,
            .part_index = question.part_index.?,
            .source_start = question.source_start,
            .source_end = question.source_end,
            .token_count = question.ids.len,
            .tokens_sha256 = question_token_hex[0..],
        },
        .policy = .{ .keep_prefix_tokens = input.policy.keep_prefix_tokens, .keep_recent_tokens = input.policy.keep_recent_tokens, .block_tokens = @as(u32, 512), .compaction_targets = targets },
        .payload_parts = parts,
        .eligible_token_labels = input.eligible.labels,
        .eligible_ranges = ranges.items,
        .eligible_blocks = blocks.items,
        .loaded_identity = .{ .load_generation = input.load_generation, .artifact_sha256 = artifact_hex[0..], .tokenizer_sha256 = tokenizer_hex[0..], .template_sha256 = template_hex[0..], .layout_sha256 = layout_hex[0..], .implementation_sha256 = runtime_hex[0..] },
        .provenance = .{ .binary_sha256 = binary_hex[0..], .source_build_sha256 = source_hex[0..] },
        .completed = true,
        .cancelled = false,
        .elapsed_ns = input.elapsed_ns,
        .status_readback_bytes = input.status_readback_bytes,
        .kv_readback_bytes = input.kv_readback_bytes,
    }, .{});
    return .{
        .allocator = allocator,
        .receipt_json = receipt,
        .rendered_prompt = rendered_prompt,
        .prompt_tokens_i64_le = token_bytes,
        .prompt_token_offsets_u32_le = offset_bytes,
        .question_tokens_i64_le = question_token_bytes,
        .request = input.request,
        .question_source_start = question.source_start,
        .question_source_end = question.source_end,
        .load_generation = input.load_generation,
        .identity = input.identity,
        .provenance = .{ .binary_sha256 = input.binary_sha256, .source_build_sha256 = input.source_build_sha256 },
        .fit_study = fit_study,
        .gate_study = gate_study,
    };
}

/// Seal coverage only from the exact render/tokenization object produced by
/// `generation.prepareCompactionPayload`. The caller may attach request and
/// build identities, but cannot substitute separately reconstructed token,
/// offset, label, or eligibility arrays. Invoke this synchronously after the
/// same request completes, while `prepared` still owns all borrowed slices.
pub fn capturePrepared(
    allocator: std.mem.Allocator,
    request: RequestBinding,
    document: Document,
    policy: policy_mod.Policy,
    prepared: *const generation.PreparedCompactionPayload,
    question: *const generation.AttributedCompactionQuestion,
    fit_study: *const generation.PreparedCompactionStudy,
    gate_study: *const generation.PreparedCompactionStudy,
    reserved_output_tokens: u32,
    admitted_prefill_chunk_rows: u32,
    load_generation: u64,
    identity: profiles.Identity,
    provenance: Provenance,
    completion: Completion,
) !Evidence {
    return capturePreparedParts(
        allocator,
        request,
        document,
        policy,
        prepared.rendered.text,
        &prepared.encoded,
        prepared.rendered.labels,
        &prepared.eligible,
        question,
        fit_study,
        gate_study,
        reserved_output_tokens,
        admitted_prefill_chunk_rows,
        load_generation,
        identity,
        provenance,
        completion,
    );
}

/// Equivalent boundary for serving, where `PreparedCompactionPayload` has
/// transferred its exact encoded prompt, rendered labels, and eligibility into
/// the request-owned prompt estimate. Callers must pass those three retained
/// owners together; this function never re-renders or re-tokenizes them.
pub fn capturePreparedParts(
    allocator: std.mem.Allocator,
    request: RequestBinding,
    document: Document,
    policy: policy_mod.Policy,
    rendered_prompt: []const u8,
    encoded: *const tokenizer_mod.EncodeResult,
    rendered_labels: []const u32,
    eligible: *const payload.Eligible,
    question: *const generation.AttributedCompactionQuestion,
    fit_study: *const generation.PreparedCompactionStudy,
    gate_study: *const generation.PreparedCompactionStudy,
    reserved_output_tokens: u32,
    admitted_prefill_chunk_rows: u32,
    load_generation: u64,
    identity: profiles.Identity,
    provenance: Provenance,
    completion: Completion,
) !Evidence {
    const offsets = encoded.offsets orelse return error.InvalidPayloadProvenance;
    return capture(allocator, .{
        .request = request,
        .document = document,
        .policy = policy,
        .rendered_prompt = rendered_prompt,
        .ids = encoded.ids,
        .attention_mask = encoded.attention_mask,
        .offsets = offsets,
        .rendered_labels = rendered_labels,
        .eligible = eligible,
        .question = question,
        .fit_study = fit_study,
        .gate_study = gate_study,
        .reserved_output_tokens = reserved_output_tokens,
        .admitted_prefill_chunk_rows = admitted_prefill_chunk_rows,
        .load_generation = load_generation,
        .identity = identity,
        .binary_sha256 = provenance.binary_sha256,
        .source_build_sha256 = provenance.source_build_sha256,
        .elapsed_ns = completion.elapsed_ns,
        .status_readback_bytes = completion.status_readback_bytes,
        .kv_readback_bytes = completion.kv_readback_bytes,
    });
}

test "context runtime coverage binds whole prompt and rejects omitted blocks" {
    const allocator = std.testing.allocator;
    var ids: [1024]i32 = @splat(7);
    var mask: [1024]i32 = @splat(1);
    var labels: [1024]u32 = @splat(1);
    @memset(labels[1020..], 0);
    const rendered_prompt: [1024]u8 = @splat('x');
    var offsets: [1024][2]u32 = undefined;
    for (&offsets, 0..) |*span, index| span.* = .{ @intCast(index), @intCast(index + 1) };
    var eligible = try payload.tokenizeEligibility(allocator, &labels, &offsets, &ids, &.{}, 1, 0, 1);
    defer eligible.deinit();
    var full_prompt_hash = std.crypto.hash.sha2.Sha256.init(.{});
    var id_bytes: [8]u8 = undefined;
    for (ids) |id| {
        std.mem.writeInt(i64, &id_bytes, id, .little);
        full_prompt_hash.update(&id_bytes);
    }
    var full_prompt_sha256: [32]u8 = undefined;
    full_prompt_hash.final(&full_prompt_sha256);
    var question_ids: [4]i64 = @splat(7);
    const question = generation.AttributedCompactionQuestion{
        .allocator = allocator,
        .ids = &question_ids,
        .source_start = 1020,
        .source_end = 1024,
        .prompt_tokens = 1024,
        .prompt_tokens_sha256 = full_prompt_sha256,
        .attribution = .nonselected_user_part,
        .message_index = 0,
        .part_index = 1,
    };
    var fit_ids = [_]i64{ 10, 11 };
    var gate_ids = [_]i64{ 12, 13 };
    const fit_study = generation.PreparedCompactionStudy{
        .allocator = allocator,
        .ids = &fit_ids,
        .template_bytes = studies.template(.fit),
        .generation_contract_bytes = studies.generation_contract,
        .prompt_tokens = 1024,
        .prompt_tokens_sha256 = full_prompt_sha256,
        .rendered_suffix_sha256 = @splat(21),
    };
    const gate_study = generation.PreparedCompactionStudy{
        .allocator = allocator,
        .ids = &gate_ids,
        .template_bytes = studies.template(.gate),
        .generation_contract_bytes = studies.generation_contract,
        .prompt_tokens = 1024,
        .prompt_tokens_sha256 = full_prompt_sha256,
        .rendered_suffix_sha256 = @splat(22),
    };
    // Discovery may reserve a larger context ceiling before the exact rendered
    // prompt count is known. The receipt remains bound to the observed 1040.
    var policy = policy_mod.Policy{ .mode = .attention_matching, .max_context_tokens = 2048, .max_kv_tokens = 1024, .keep_prefix_tokens = 0, .keep_recent_tokens = 1 };
    try policy.compaction_targets.add(.{ .message_index = 0, .part_index = 0 });
    const input = Input{
        .request = .{
            .session_id = @splat(9),
            .request_index = 11,
            .question_sha256 = @splat(8),
            .request_content_sha256 = @splat(7),
        },
        .document = .{ .id = "coverage-fixture", .sha256 = @splat(1), .bucket = .document_qa },
        .policy = policy,
        .rendered_prompt = &rendered_prompt,
        .ids = &ids,
        .attention_mask = &mask,
        .offsets = &offsets,
        .rendered_labels = &labels,
        .eligible = &eligible,
        .question = &question,
        .fit_study = &fit_study,
        .gate_study = &gate_study,
        .reserved_output_tokens = 16,
        .admitted_prefill_chunk_rows = required_prefill_chunk_rows,
        .load_generation = 1,
        .identity = .{ .artifact = @splat(2), .tokenizer = @splat(3), .template = @splat(4), .layout = @splat(5), .runtime = @splat(6) },
        .binary_sha256 = @splat(7),
        .source_build_sha256 = @splat(8),
        .elapsed_ns = 1,
        .status_readback_bytes = 4,
        .kv_readback_bytes = 0,
    };
    const encoded = tokenizer_mod.EncodeResult{
        .ids = &ids,
        .attention_mask = &mask,
        .offsets = &offsets,
        .allocator = allocator,
    };
    var evidence = try capturePreparedParts(
        allocator,
        input.request,
        input.document,
        input.policy,
        input.rendered_prompt,
        &encoded,
        input.rendered_labels,
        input.eligible,
        input.question,
        input.fit_study,
        input.gate_study,
        input.reserved_output_tokens,
        required_prefill_chunk_rows,
        input.load_generation,
        input.identity,
        .{ .binary_sha256 = input.binary_sha256, .source_build_sha256 = input.source_build_sha256 },
        .{ .elapsed_ns = input.elapsed_ns, .status_readback_bytes = input.status_readback_bytes, .kv_readback_bytes = input.kv_readback_bytes },
    );
    defer evidence.deinit();
    try std.testing.expectEqual(@as(usize, 8192), evidence.prompt_tokens_i64_le.len);
    try std.testing.expectEqual(@as(usize, 8192), evidence.prompt_token_offsets_u32_le.len);
    try std.testing.expectEqual(@as(usize, 32), evidence.question_tokens_i64_le.len);
    try std.testing.expectEqual(@as(usize, 16), evidence.fit_study.tokens_i64_le.len);
    try std.testing.expectEqual(@as(usize, 16), evidence.gate_study.tokens_i64_le.len);
    try std.testing.expectEqual(@as(i64, 7), std.mem.readInt(i64, evidence.prompt_tokens_i64_le[8184..8192], .little));
    try std.testing.expectEqual(@as(u32, 1023), std.mem.readInt(u32, evidence.prompt_token_offsets_u32_le[8184..8188], .little));
    try std.testing.expectEqual(@as(u32, 1024), std.mem.readInt(u32, evidence.prompt_token_offsets_u32_le[8188..8192], .little));
    try std.testing.expectEqual(@as(i64, 7), std.mem.readInt(i64, evidence.question_tokens_i64_le[24..32], .little));
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, evidence.receipt_json, .{});
    defer parsed.deinit();
    const receipt = parsed.value.object;
    const request = receipt.get("request").?.object;
    try std.testing.expectEqual(@as(i64, 11), request.get("request_index").?.integer);
    try std.testing.expectEqualStrings("0808080808080808080808080808080808080808080808080808080808080808", request.get("question_sha256").?.string);
    try std.testing.expectEqualStrings("0707070707070707070707070707070707070707070707070707070707070707", request.get("request_content_sha256").?.string);
    try std.testing.expectEqual(@as(i64, 1040), receipt.get("qualified_context_tokens").?.integer);
    try std.testing.expectEqual(@as(i64, required_prefill_chunk_rows), receipt.get("admitted_prefill_chunk_rows").?.integer);
    try std.testing.expect(receipt.get("prompt_token_offsets_sha256") != null);
    const question_receipt = receipt.get("question").?.object;
    try std.testing.expectEqual(@as(i64, 1020), question_receipt.get("source_start").?.integer);
    try std.testing.expectEqual(@as(i64, 1024), question_receipt.get("source_end").?.integer);
    try std.testing.expectEqual(@as(usize, 1), receipt.get("eligible_blocks").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 1024), receipt.get("eligible_token_labels").?.array.items.len);
    try std.testing.expectEqual(@as(i64, 0), receipt.get("eligible_token_labels").?.array.items[1023].integer);
    var incomplete = eligible;
    incomplete.blocks = eligible.blocks[0..0];
    var changed = input;
    changed.rendered_prompt = rendered_prompt[0..1023];
    try std.testing.expectError(error.InvalidPayloadProvenance, capture(allocator, changed));
    changed = input;
    changed.eligible = &incomplete;
    try std.testing.expectError(error.InvalidPayloadProvenance, capture(allocator, changed));
    changed = input;
    changed.reserved_output_tokens = 1025;
    try std.testing.expectError(error.InvalidCoverageObservation, capture(allocator, changed));
    changed = input;
    changed.kv_readback_bytes = 1;
    try std.testing.expectError(error.InvalidCoverageObservation, capture(allocator, changed));
    const saved_span = offsets[9];
    offsets[9] = offsets[8];
    var overlapping = try capture(allocator, input);
    overlapping.deinit();
    offsets[9] = .{ 7, 10 };
    try std.testing.expectError(error.InvalidPayloadProvenance, capture(allocator, input));
    offsets[9] = saved_span;
    inline for (.{ 0, 63, 65 }) |invalid_rows| {
        changed = input;
        changed.admitted_prefill_chunk_rows = invalid_rows;
        try std.testing.expectError(error.InvalidCoverageObservation, capture(allocator, changed));
    }
    var invalid_study = fit_study;
    invalid_study.prompt_tokens_sha256[0] ^= 1;
    changed = input;
    changed.fit_study = &invalid_study;
    try std.testing.expectError(error.InvalidCoverageStudy, capture(allocator, changed));
    eligible.labels[1023] = 1;
    defer eligible.labels[1023] = 0;
    try std.testing.expectError(error.InvalidPayloadProvenance, capture(allocator, input));
}
