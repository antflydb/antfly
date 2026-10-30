// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Retained-input adapter for test-only native whole-head quality batches.
//! Preparation artifacts are re-tokenized using the pinned loaded model.
const std = @import("std");
const generation = @import("pipelines/generation.zig");
const quality = @import("runtime/kv/metal_checkpoint_quality.zig");
const ordered = @import("context_serving_messages.zig");
const serving = @import("context_serving_driver.zig");
const server = @import("server/server.zig");
const checkpoint = @import("runtime/kv/attention_match_checkpoint.zig");

pub const File = struct { path: []const u8, sha256: []const u8 };
pub const Case = struct {
    id: []const u8,
    bucket: []const u8,
    messages: File,
    rendered_prompt: File,
    prompt_tokens: File,
    prompt_count: usize,
    reference_utf8: File,
    reference_tokens: File,
    reference_count: usize,
    reference_sequence_sha256: []const u8,
    request: quality.Request,
};
pub const Input = struct {
    schema: []const u8,
    model: []const u8,
    main_gguf_sha256: []const u8,
    native_identity: checkpoint.Identity,
    cases: []const Case,
};

pub fn parseDigest(raw: []const u8) ![32]u8 {
    if (raw.len != 64) return error.InvalidNativeQualityDigest;
    var result: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&result, raw) catch return error.InvalidNativeQualityDigest;
    if (std.mem.allEqual(u8, &result, 0)) return error.InvalidNativeQualityDigest;
    return result;
}

fn digest(raw: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &result, .{});
    return result;
}

pub fn validPath(path: []const u8) bool {
    if (path.len == 0 or std.fs.path.isAbsolute(path)) return false;
    var parts = std.mem.splitScalar(u8, path, '/');
    while (parts.next()) |part|
        if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return false;
    return true;
}

pub const Session = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    inputs: std.Io.Dir,
    outputs: std.Io.Dir,
    model: *server.model_manager.LoadedModel,
    pipeline: *generation.NativeGenerationPipeline,
    manifest_sha256: [32]u8,

    fn read(self: *Session, file_ref: File) ![]u8 {
        if (!validPath(file_ref.path)) return error.InvalidNativeQualityPath;
        const expected = try parseDigest(file_ref.sha256);
        var dir = self.inputs;
        var owned = false;
        defer if (owned) dir.close(self.io);
        var parts = std.mem.splitScalar(u8, file_ref.path, '/');
        var part = parts.next() orelse return error.InvalidNativeQualityPath;
        while (parts.next()) |next| {
            const child = try dir.openDir(self.io, part, .{ .follow_symlinks = false });
            if (owned) dir.close(self.io);
            dir = child;
            owned = true;
            part = next;
        }
        const file = try dir.openFile(self.io, part, .{ .mode = .read_only, .allow_directory = false, .follow_symlinks = false, .resolve_beneath = true });
        defer file.close(self.io);
        const stat = try file.stat(self.io);
        if (stat.kind != .file or stat.size == 0 or stat.size > 16 * 1024 * 1024) return error.InvalidNativeQualityPath;
        const result = try self.allocator.alloc(u8, @intCast(stat.size));
        errdefer self.allocator.free(result);
        if (try file.readPositionalAll(self.io, result, 0) != result.len or
            !std.mem.eql(u8, &digest(result), &expected)) return error.NativeQualityInputChanged;
        return result;
    }

    fn readTokens(self: *Session, file_ref: File, count: usize) ![]i64 {
        const bytes = try self.read(file_ref);
        defer self.allocator.free(bytes);
        if (count == 0 or count > checkpoint.max_context_tokens or bytes.len != try std.math.mul(usize, count, 8))
            return error.InvalidNativeQualityTokens;
        const result = try self.allocator.alloc(i64, count);
        errdefer self.allocator.free(result);
        for (result, 0..) |*token, i| {
            token.* = std.mem.readInt(i64, bytes[i * 8 ..][0..8], .little);
            if (token.* < 0 or token.* >= self.pipeline.gpt_config.vocab_size) return error.InvalidNativeQualityTokens;
        }
        return result;
    }

    const Gold = struct {
        session: *Session,
        case: Case,
        prompt: []const u8,
        prompt_ids: []const i64,
        tokens: ?[]i64 = null,
        calls: usize = 0,

        fn reveal(raw: *anyopaque) ![]const i64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.calls != 0 or self.tokens != null) return error.NativeQualityReferenceAlreadyRevealed;
            self.calls += 1;
            const session = self.session;
            const model = session.model;
            const text = try session.read(self.case.reference_utf8);
            defer session.allocator.free(text);
            if (!std.unicode.utf8ValidateSlice(text)) return error.InvalidNativeQualityReference;
            const tokens = try session.readTokens(self.case.reference_tokens, self.case.reference_count);
            errdefer session.allocator.free(tokens);
            const combined = try std.mem.concat(session.allocator, u8, &.{ self.prompt, text });
            defer session.allocator.free(combined);
            var encoded = try generation.encodeNativeGenerationPrompt(model.getTokenizer(), session.allocator, combined, self.prompt_ids.len + self.case.request.output_tokens, model.manifest.add_bos_token, model.manifest.bos_token);
            defer encoded.deinit();
            var count: usize = 0;
            while (count < encoded.attention_mask.len and encoded.attention_mask[count] != 0) : (count += 1) {}
            if (count != self.prompt_ids.len + tokens.len) return error.UnstableQualityReferenceBoundary;
            for (self.prompt_ids, encoded.ids[0..self.prompt_ids.len]) |expected, actual|
                if (expected != actual) return error.UnstableQualityReferenceBoundary;
            for (tokens, encoded.ids[self.prompt_ids.len..count]) |expected, actual|
                if (expected != actual) return error.NativeQualityInputChanged;
            const signed = try session.allocator.alloc(i32, tokens.len);
            defer session.allocator.free(signed);
            for (tokens, signed) |token, *id| id.* = @intCast(token);
            const decoded = try model.getTokenizer().decode(session.allocator, signed);
            defer session.allocator.free(decoded);
            if (!std.mem.eql(u8, decoded, text)) return error.QualityReferenceTokenizerMismatch;
            self.tokens = tokens;
            return tokens;
        }
    };

    pub fn execute(self: *Session, case: Case, index: usize) !void {
        if (!@import("builtin").is_test or case.id.len == 0 or case.id.len > 256 or
            (case.prompt_count != 7936 and case.prompt_count != 32512) or
            case.request.output_tokens != quality.max_output_tokens or case.reference_count < 2 or case.reference_count > quality.max_output_tokens)
            return error.InvalidNativeQualityRequest;
        _ = try case.request.capacity(case.prompt_count, self.pipeline.gpt_config.max_position_embeddings);
        const message_bytes = try self.read(case.messages);
        defer self.allocator.free(message_bytes);
        var messages = try ordered.parseOwned(self.allocator, message_bytes);
        defer messages.deinit();
        const semantic = try ordered.semanticDigest(messages.value);
        var message_storage: [ordered.max_messages]generation.Message = undefined;
        var parts: [ordered.max_messages * ordered.max_parts_per_message]generation.Message.ContentPart = undefined;
        var calls: [ordered.max_total_tool_calls]generation.Message.ToolCall = undefined;
        const native = try serving.Driver.nativeMessages(.{
            .route = .native,
            .variant = .attention_matching,
            .model = "qualification",
            .prompt = "",
            .ordered = .{ .artifact = messages.value, .semantic_sha256 = semantic },
            .max_context_tokens = @intCast(case.prompt_count + quality.max_output_tokens),
            .max_kv_tokens = @intCast((case.prompt_count + quality.max_output_tokens) / 2),
            .keep_prefix_tokens = 512,
            .keep_recent_tokens = 512,
            .reserved_output_tokens = quality.max_output_tokens,
        }, &message_storage, &parts, &calls);
        const prompt = try (self.model.chat_tmpl orelse return error.MissingChatTemplate).applyWithOptions(self.allocator, native, .{ .enable_thinking = false });
        defer self.allocator.free(prompt);
        const expected_prompt = try self.read(case.rendered_prompt);
        defer self.allocator.free(expected_prompt);
        if (!std.mem.eql(u8, prompt, expected_prompt)) return error.NativeQualityInputChanged;
        const tokens = try self.readTokens(case.prompt_tokens, case.prompt_count);
        defer self.allocator.free(tokens);
        var encoded = try generation.encodeNativeGenerationPrompt(self.model.getTokenizer(), self.allocator, prompt, case.prompt_count + quality.max_output_tokens, self.model.manifest.add_bos_token, self.model.manifest.bos_token);
        defer encoded.deinit();
        var count: usize = 0;
        while (count < encoded.attention_mask.len and encoded.attention_mask[count] != 0) : (count += 1) {}
        if (count != case.prompt_count) return error.NativeQualityInputChanged;
        for (tokens, encoded.ids[0..count]) |expected, actual|
            if (expected != actual) return error.NativeQualityInputChanged;
        var gold: Gold = .{ .session = self, .case = case, .prompt = prompt, .prompt_ids = tokens };
        defer if (gold.tokens) |ids| self.allocator.free(ids);
        var report = try self.pipeline.qualifyMetalCheckpointQuality(tokens, prompt, case.request, .{
            .count = case.reference_count,
            .sha256 = try parseDigest(case.reference_sequence_sha256),
            .provider = .{ .ctx = &gold, .reveal_fn = Gold.reveal },
        });
        defer report.deinit();
        if (gold.calls != 1 or !report.collection_complete or !report.reference_revealed_after_both_greedy_branches)
            return error.IncompleteNativeQualityCollection;
        const body = try std.json.Stringify.valueAlloc(self.allocator, .{
            .schema = "antfly.native-checkpoint-quality.case.v2",
            .allocation_scope = "private_native_kv_owners",
            .full_serving_owner_qualified = false,
            .release_eligible = false,
            .collection_complete = report.collection_complete,
            .case_index = index,
            .case = case,
            .manifest_sha256 = self.manifest_sha256,
            .load_generation = self.model.load_generation,
            .frozen = report.frozen,
            .calibration_consumers = report.calibration_consumers[0..report.frozen.calibration.consumer_count],
            .phase_durations_ns = report.phase_durations_ns,
            .source_serving_prefill = report.source_serving_prefill,
            .collection_elapsed_ns = report.collection_elapsed_ns,
            .model_slot_runtime_kv_bytes_before = report.model_slot_runtime_kv_bytes_before,
            .model_slot_runtime_kv_bytes_after = report.model_slot_runtime_kv_bytes_after,
            .continuation_runtime_kv_bytes = report.continuation_runtime_kv_bytes,
            .compacted_greedy_runtime_kv_bytes_before = report.compacted_greedy_runtime_kv_bytes_before,
            .compacted_greedy_runtime_kv_bytes_after = report.compacted_greedy_runtime_kv_bytes_after,
            .compacted_teacher_runtime_kv_bytes_before = report.compacted_teacher_runtime_kv_bytes_before,
            .compacted_teacher_runtime_kv_bytes_after = report.compacted_teacher_runtime_kv_bytes_after,
            .model_slots_prepared_without_kv_allocation = report.model_slots_prepared_without_kv_allocation,
            .exact_greedy = report.exact_greedy,
            .compacted_greedy = report.compacted_greedy,
            .exact_teacher = report.exact_teacher,
            .compacted_teacher = report.compacted_teacher,
            .reference_sha256 = report.reference_sha256,
            .reference_revealed_after_both_greedy_branches = report.reference_revealed_after_both_greedy_branches,
            .reference_provider_calls = gold.calls,
            .source_unchanged = report.source_unchanged,
            .source_owner_id = report.source_owner_id,
            .compacted_owner_id = report.compacted_owner_id,
            .query_scope_released = report.query_scope_released,
            .query_scope_delta_bytes = report.query_scope_delta_bytes,
            .admitted_peak_bytes = report.admitted_peak_bytes,
            .source_global_bytes = report.source_global_bytes,
            .candidate_global_bytes = report.candidate_global_bytes,
            .max_head_mass_error = report.max_head_mass_error,
            .max_head_output_rmse = report.max_head_output_rmse,
            .fp64_accelerated_value_solves = report.fp64_accelerated_value_solves,
            .source_release = report.source_release,
            .compacted_release = report.compacted_release,
        }, .{ .whitespace = .indent_2 });
        defer self.allocator.free(body);
        var filename: [64]u8 = undefined;
        const name = try std.fmt.bufPrint(&filename, "case-{d:0>4}.json", .{index});
        var output = try self.outputs.createFile(self.io, name, .{ .exclusive = true, .truncate = false });
        defer output.close(self.io);
        try output.writeStreamingAll(self.io, body);
        try output.writeStreamingAll(self.io, "\n");
        try output.sync(self.io);
        std.debug.print("native_checkpoint_quality_completed index={d} case={s} encoding={s} exact_tokens={d} compacted_tokens={d} reference_tokens={d}\n", .{ index, case.id, @tagName(case.request.encoding), report.exact_greedy.count, report.compacted_greedy.count, report.exact_teacher.count });
    }
};

test "attention_match Metal native quality input paths and hashes fail closed" {
    for ([_][]const u8{ "", "/absolute", "a/../b", "./x", "a//b", "a/" }) |path|
        try std.testing.expect(!validPath(path));
    try std.testing.expect(validPath("prepared/case/messages.json"));
    try std.testing.expectError(error.InvalidNativeQualityDigest, parseDigest("abc"));
    try std.testing.expectError(error.InvalidNativeQualityDigest, parseDigest(&@as([64]u8, @splat('0'))));
}
