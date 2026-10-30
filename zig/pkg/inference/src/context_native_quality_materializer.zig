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

//! Test-only native whole-head quality preparation. This transport can only
//! render/tokenize sealed ordered messages or stop. It cannot issue model
//! forward work or select a release profile.
const std = @import("std");
const driver_mod = @import("context_serving_driver.zig");
const ordered_messages = @import("context_serving_messages.zig");
const context_policy = @import("runtime/context_policy.zig");
const quality_materializer = @import("context_quality_materializer.zig");
const generation = @import("pipelines/generation.zig");
const server = @import("server/server.zig");
const max_command_bytes = 64 * 1024;
const Command = @import("context_serving_protocol.zig").Command;

pub const Session = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    campaign_dir: std.Io.Dir,
    model: []const u8,
    loaded_model: *server.model_manager.LoadedModel,
    loaded_identity: server.ContextServingLoadedIdentity,
    expected_session_id: [64]u8,
    max_context_tokens: u32,
    max_kv_tokens: u32,
    keep_prefix_tokens: u32 = 512,
    keep_recent_tokens: u32 = 512,
    optimize_decode: bool = true,
    optimize_prefill: bool = true,
    fit_engine: @import("runtime/kv/compaction_profile.zig").FitEngine = .block_v1,
    implementation_sha256: [32]u8 = @import("compaction_identity").digest,
    profile_artifact_sha256: [32]u8 = @splat(0),
    profile_semantic_sha256: [32]u8 = @splat(0),
    next_request_index: u32 = 0,

    fn digest(bytes: []const u8) [32]u8 {
        var result: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
        return result;
    }

    fn expectHex(text: []const u8, actual: [32]u8) !void {
        if (text.len != 64) return error.ServingDriverDigestMismatch;
        var expected: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&expected, text) catch return error.ServingDriverDigestMismatch;
        if (!std.mem.eql(u8, &expected, &actual)) return error.ServingDriverDigestMismatch;
    }

    fn parseHex32(text: []const u8) ![32]u8 {
        if (text.len != 64) return error.ServingDriverDigestMismatch;
        var result: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&result, text) catch return error.ServingDriverDigestMismatch;
        return result;
    }

    fn validRelative(path: []const u8) bool {
        if (path.len == 0 or std.fs.path.isAbsolute(path)) return false;
        var it = std.mem.splitScalar(u8, path, '/');
        while (it.next()) |part| if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return false;
        return true;
    }

    fn readCampaignFile(self: *Session, path: []const u8) ![]u8 {
        if (!validRelative(path)) return error.InvalidServingDriverPath;
        var current = self.campaign_dir;
        var current_owned = false;
        defer if (current_owned) current.close(self.io);
        var parts = std.mem.splitScalar(u8, path, '/');
        var part = parts.next() orelse return error.InvalidServingDriverPath;
        while (parts.next()) |next| {
            const child = try current.openDir(self.io, part, .{ .follow_symlinks = false });
            if (current_owned) current.close(self.io);
            current = child;
            current_owned = true;
            part = next;
        }
        const file = try current.openFile(self.io, part, .{
            .mode = .read_only,
            .allow_directory = false,
            .follow_symlinks = false,
            .resolve_beneath = true,
        });
        defer file.close(self.io);
        const stat = try file.stat(self.io);
        if (stat.kind != .file or stat.size > 16 * 1024 * 1024) return error.InvalidServingDriverPath;
        const size = std.math.cast(usize, stat.size) orelse return error.InvalidServingDriverPath;
        const bytes = try self.allocator.alloc(u8, size);
        errdefer self.allocator.free(bytes);
        if (try file.readPositionalAll(self.io, bytes, 0) != size) return error.InvalidServingDriverPath;
        return bytes;
    }

    fn writeCampaignEvidenceFile(self: *Session, path: []const u8, bytes: []const u8) ![32]u8 {
        if (!validRelative(path) or bytes.len == 0) return error.InvalidServingDriverPath;
        var file = try self.campaign_dir.createFile(self.io, path, .{ .exclusive = true, .truncate = false });
        defer file.close(self.io);
        try file.writeStreamingAll(self.io, bytes);
        try file.sync(self.io);
        return digest(bytes);
    }

    fn generationSettings(reserved_output_tokens: u32) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.context-serving.generation.v1\x00");
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, reserved_output_tokens, .little);
        hash.update(&bytes);
        hash.update(&.{ 1, 0, 0, 0, 0, 0 });
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    fn variantSettings(self: *const Session, variant: driver_mod.Variant) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly.context-serving.variant.v2\x00");
        const name = @tagName(variant);
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, @intCast(name.len), .little);
        hash.update(&bytes);
        hash.update(name);
        inline for (.{
            self.max_context_tokens,
            self.max_kv_tokens,
            self.keep_prefix_tokens,
            self.keep_recent_tokens,
        }) |value| {
            std.mem.writeInt(u32, &bytes, value, .little);
            hash.update(&bytes);
        }
        hash.update(&.{ @intFromBool(self.optimize_decode), @intFromBool(self.optimize_prefill) });
        // The entrypoint derives implementation/profile digests from this
        // session's loaded profile engine. Preserve the v2 external digest
        // schema while binding the engine through those canonical identities.
        hash.update(&self.implementation_sha256);
        hash.update(&self.profile_artifact_sha256);
        hash.update(&self.profile_semantic_sha256);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    fn validate(self: *Session, command: Command) !void {
        if (!std.mem.eql(u8, command.session_id orelse return error.InvalidServingDriverCommand, &self.expected_session_id))
            return error.InvalidServingDriverCommand;
        if (command.command == .stop) return;
        if (command.command != .materialize_quality_v1 or self.next_request_index >= 2000 or
            command.request_index != self.next_request_index)
            return error.InvalidServingDriverCommand;
        const ordered = command.messages_file != null and command.messages_sha256 != null and
            command.source_sha256 != null and command.selected_part_sha256s != null and
            command.document_file == null and command.document_sha256 == null and
            command.question_file == null and command.question_sha256 == null;
        if (!std.mem.allEqual(u8, &self.profile_artifact_sha256, 0) or
            !std.mem.allEqual(u8, &self.profile_semantic_sha256, 0) or
            !ordered or command.route != null or command.variant != .attention_matching or
            command.case_id == null or command.request_content_sha256 == null or
            command.reference_file == null or command.reference_sha256 == null or
            command.reserved_output_tokens == null or command.reserved_output_tokens.? == 0 or
            command.settings_sha256 == null or command.variant_settings_sha256 == null or
            command.prompt_file != null or command.prompt_sha256 != null or command.prompt_tokens != null or
            command.reference_tokens_file != null or command.reference_tokens_sha256 != null or
            command.bucket != null or command.scorer != null or command.pair_id != null or
            command.warmup != null or command.fault != null or command.siblings != null or
            command.binary_receipt_id != null or command.source_receipt_id != null)
            return error.InvalidServingDriverCommand;
    }

    fn tokensI64Le(self: *Session, tokens: []const i64) ![]u8 {
        const size = std.math.mul(usize, tokens.len, 8) catch return error.InvalidQualityMaterialization;
        const result = try self.allocator.alloc(u8, size);
        errdefer self.allocator.free(result);
        for (tokens, 0..) |token, index|
            std.mem.writeInt(i64, result[index * 8 ..][0..8], token, .little);
        return result;
    }

    fn offsetsU32Le(self: *Session, offsets: []const [2]u32) ![]u8 {
        const size = std.math.mul(usize, offsets.len, 8) catch return error.InvalidQualityMaterialization;
        const result = try self.allocator.alloc(u8, size);
        errdefer self.allocator.free(result);
        for (offsets, 0..) |offset, index| {
            std.mem.writeInt(u32, result[index * 8 ..][0..4], offset[0], .little);
            std.mem.writeInt(u32, result[index * 8 + 4 ..][0..4], offset[1], .little);
        }
        return result;
    }

    fn labelsU32Le(self: *Session, labels: []const u32) ![]u8 {
        const size = std.math.mul(usize, labels.len, 4) catch return error.InvalidQualityMaterialization;
        const result = try self.allocator.alloc(u8, size);
        errdefer self.allocator.free(result);
        for (labels, 0..) |label, index|
            std.mem.writeInt(u32, result[index * 4 ..][0..4], label, .little);
        return result;
    }

    fn executeQualityMaterialization(
        self: *Session,
        command: Command,
        writer: *std.Io.Writer,
    ) !void {
        const messages_bytes = try self.readCampaignFile(command.messages_file.?);
        defer self.allocator.free(messages_bytes);
        try expectHex(command.messages_sha256.?, digest(messages_bytes));
        var parsed = try ordered_messages.parseOwned(self.allocator, messages_bytes);
        defer parsed.deinit();
        const request_digest = try ordered_messages.semanticDigest(parsed.value);
        const source_digest = try ordered_messages.selectedSourceDigest(parsed.value);
        var part_storage: [64][32]u8 = undefined;
        const parts = try ordered_messages.selectedPartDigests(parsed.value, &part_storage);
        try expectHex(command.request_content_sha256.?, request_digest);
        try expectHex(command.source_sha256.?, source_digest);
        if (parts.len != command.selected_part_sha256s.?.len) return error.ServingDriverDigestMismatch;
        for (command.selected_part_sha256s.?, parts) |expected, actual| try expectHex(expected, actual);
        const reference = try self.readCampaignFile(command.reference_file.?);
        defer self.allocator.free(reference);
        if (reference.len == 0 or !std.unicode.utf8ValidateSlice(reference)) return error.InvalidQualityInput;
        try expectHex(command.reference_sha256.?, digest(reference));
        try expectHex(command.settings_sha256.?, generationSettings(command.reserved_output_tokens.?));
        try expectHex(command.variant_settings_sha256.?, self.variantSettings(.attention_matching));

        var policy: context_policy.Policy = .{
            .mode = .attention_matching,
            .max_context_tokens = self.max_context_tokens,
            .max_kv_tokens = self.max_kv_tokens,
            .keep_prefix_tokens = self.keep_prefix_tokens,
            .keep_recent_tokens = self.keep_recent_tokens,
        };
        for (parsed.value.messages, 0..) |message, message_index| for (message.parts, 0..) |part, part_index|
            if (part.selected) try policy.compaction_targets.add(.{
                .message_index = @intCast(message_index),
                .part_index = @intCast(part_index),
            });
        try policy.validate();
        var evidence = try self.materializeQuality(.{
            .model = self.model,
            .ordered = .{ .artifact = parsed.value, .semantic_sha256 = request_digest },
            .policy = policy,
            .reserved_output_tokens = command.reserved_output_tokens.?,
            .optimize_decode = self.optimize_decode,
            .optimize_prefill = self.optimize_prefill,
            .fit_engine = self.fit_engine,
            .reference_utf8 = reference,
        });
        defer evidence.deinit();
        if (!std.mem.eql(u8, &evidence.loaded_identity.implementation_sha256, &self.implementation_sha256) or
            !std.mem.allEqual(u8, &evidence.loaded_identity.profile_sha256, 0))
            return error.ContextQualificationLoadedIdentityMismatch;

        const prompt_bytes = try self.tokensI64Le(evidence.materialized.prompt_tokens);
        defer self.allocator.free(prompt_bytes);
        const offset_bytes = try self.offsetsU32Le(evidence.materialized.prompt_token_offsets);
        defer self.allocator.free(offset_bytes);
        const rendered_label_bytes = try self.labelsU32Le(evidence.materialized.rendered_labels);
        defer self.allocator.free(rendered_label_bytes);
        const prompt_label_bytes = try self.labelsU32Le(evidence.materialized.prompt_token_labels);
        defer self.allocator.free(prompt_label_bytes);
        const combined_bytes = try self.tokensI64Le(evidence.materialized.combined_tokens);
        defer self.allocator.free(combined_bytes);
        const reference_json = try std.json.Stringify.valueAlloc(
            self.allocator,
            evidence.materialized.reference_tokens,
            .{},
        );
        defer self.allocator.free(reference_json);
        var name_buffer: [7][80]u8 = undefined;
        const rendered_name = try std.fmt.bufPrint(&name_buffer[0], "materialize-rendered-{d:0>6}.txt", .{command.request_index.?});
        const prompt_name = try std.fmt.bufPrint(&name_buffer[1], "materialize-prompt-{d:0>6}.i64le", .{command.request_index.?});
        const offsets_name = try std.fmt.bufPrint(&name_buffer[2], "materialize-offsets-{d:0>6}.u32le", .{command.request_index.?});
        const combined_name = try std.fmt.bufPrint(&name_buffer[3], "materialize-combined-{d:0>6}.i64le", .{command.request_index.?});
        const reference_name = try std.fmt.bufPrint(&name_buffer[4], "materialize-reference-{d:0>6}.json", .{command.request_index.?});
        const rendered_labels_name = try std.fmt.bufPrint(&name_buffer[5], "materialize-rendered-labels-{d:0>6}.u32le", .{command.request_index.?});
        const prompt_labels_name = try std.fmt.bufPrint(&name_buffer[6], "materialize-prompt-labels-{d:0>6}.u32le", .{command.request_index.?});
        const rendered_sha = try self.writeCampaignEvidenceFile(rendered_name, evidence.materialized.rendered_prompt);
        const prompt_sha = try self.writeCampaignEvidenceFile(prompt_name, prompt_bytes);
        const offsets_sha = try self.writeCampaignEvidenceFile(offsets_name, offset_bytes);
        const combined_sha = try self.writeCampaignEvidenceFile(combined_name, combined_bytes);
        const reference_sha = try self.writeCampaignEvidenceFile(reference_name, reference_json);
        const rendered_labels_sha = try self.writeCampaignEvidenceFile(rendered_labels_name, rendered_label_bytes);
        const prompt_labels_sha = try self.writeCampaignEvidenceFile(prompt_labels_name, prompt_label_bytes);
        const rendered_hex = std.fmt.bytesToHex(rendered_sha, .lower);
        const prompt_hex = std.fmt.bytesToHex(prompt_sha, .lower);
        const offsets_hex = std.fmt.bytesToHex(offsets_sha, .lower);
        const combined_hex = std.fmt.bytesToHex(combined_sha, .lower);
        const reference_hex = std.fmt.bytesToHex(reference_sha, .lower);
        const rendered_labels_hex = std.fmt.bytesToHex(rendered_labels_sha, .lower);
        const prompt_labels_hex = std.fmt.bytesToHex(prompt_labels_sha, .lower);
        const reference_sequence_hex = std.fmt.bytesToHex(evidence.materialized.reference_sequence_sha256, .lower);
        const artifact_hex = std.fmt.bytesToHex(evidence.loaded_identity.artifact_sha256, .lower);
        const tokenizer_hex = std.fmt.bytesToHex(evidence.loaded_identity.tokenizer_sha256, .lower);
        const template_hex = std.fmt.bytesToHex(evidence.loaded_identity.template_sha256, .lower);
        const layout_hex = std.fmt.bytesToHex(evidence.loaded_identity.layout_sha256, .lower);
        const implementation_hex = std.fmt.bytesToHex(evidence.loaded_identity.implementation_sha256, .lower);
        try std.json.Stringify.value(.{
            .schema_version = @as(u32, 2),
            .stage = "quality_tokenizer_materialization",
            .completed = true,
            .model_outputs_observed = false,
            .model_forward_steps = @as(u32, 0),
            .session_id = &self.expected_session_id,
            .request_index = command.request_index.?,
            .case_id = command.case_id.?,
            .messages_sha256 = command.messages_sha256.?,
            .request_content_sha256 = command.request_content_sha256.?,
            .source_sha256 = command.source_sha256.?,
            .selected_part_sha256s = command.selected_part_sha256s.?,
            .reference_sha256 = command.reference_sha256.?,
            .qualified_context_tokens = self.max_context_tokens,
            .reserved_output_tokens = command.reserved_output_tokens.?,
            .settings_sha256 = command.settings_sha256.?,
            .variant_settings_sha256 = command.variant_settings_sha256.?,
            .loaded_identity = .{
                .load_generation = evidence.loaded_identity.load_generation,
                .artifact_sha256 = artifact_hex[0..],
                .tokenizer_sha256 = tokenizer_hex[0..],
                .template_sha256 = template_hex[0..],
                .layout_sha256 = layout_hex[0..],
                .implementation_sha256 = implementation_hex[0..],
            },
            .rendered_prompt = .{ .path = rendered_name, .sha256 = rendered_hex[0..], .count = evidence.materialized.rendered_prompt.len },
            .prompt_tokens = .{ .path = prompt_name, .sha256 = prompt_hex[0..], .count = evidence.materialized.prompt_tokens.len },
            .prompt_token_offsets = .{ .path = offsets_name, .sha256 = offsets_hex[0..], .count = evidence.materialized.prompt_token_offsets.len },
            .rendered_labels = .{ .path = rendered_labels_name, .sha256 = rendered_labels_hex[0..], .count = evidence.materialized.rendered_labels.len },
            .prompt_token_labels = .{ .path = prompt_labels_name, .sha256 = prompt_labels_hex[0..], .count = evidence.materialized.prompt_token_labels.len },
            .combined_tokens = .{ .path = combined_name, .sha256 = combined_hex[0..], .count = evidence.materialized.combined_tokens.len },
            .reference_tokens = .{ .path = reference_name, .sha256 = reference_hex[0..], .count = evidence.materialized.reference_tokens.len },
            .reference_sequence_sha256 = reference_sequence_hex[0..],
            .stable_prefix_verified = true,
            .reference_roundtrip_verified = true,
        }, .{}, writer);
        try writer.writeByte('\n');
        try writer.flush();
    }

    fn writeError(self: *Session, writer: *std.Io.Writer, command: Command, err: anyerror) !void {
        try std.json.Stringify.value(.{
            .schema_version = 2,
            .completed = false,
            .request_index = command.request_index,
            .@"error" = @errorName(err),
        }, .{}, writer);
        try writer.writeByte('\n');
        try writer.flush();
        _ = self;
    }

    fn materializeQuality(self: *Session, request: driver_mod.MaterializationRequest) !driver_mod.MaterializationEvidence {
        if (!@import("builtin").is_test or self.loaded_model.load_generation != self.loaded_identity.load_generation or
            request.policy.mode != .attention_matching or request.reserved_output_tokens == 0 or
            request.reserved_output_tokens >= self.max_context_tokens)
            return error.UnsupportedQualityMaterialization;
        var message_storage: [ordered_messages.max_messages]generation.Message = undefined;
        var part_storage: [ordered_messages.max_messages * ordered_messages.max_parts_per_message]generation.Message.ContentPart = undefined;
        var tool_storage: [ordered_messages.max_total_tool_calls]generation.Message.ToolCall = undefined;
        const messages = try driver_mod.Driver.nativeMessages(.{
            .route = .native,
            .variant = .attention_matching,
            .model = self.model,
            .prompt = "",
            .ordered = request.ordered,
            .max_context_tokens = self.max_context_tokens,
            .max_kv_tokens = self.max_kv_tokens,
            .keep_prefix_tokens = self.keep_prefix_tokens,
            .keep_recent_tokens = self.keep_recent_tokens,
            .reserved_output_tokens = request.reserved_output_tokens,
            .optimize_decode = true,
            .optimize_prefill = true,
        }, &message_storage, &part_storage, &tool_storage);
        const model = self.loaded_model;
        const materialized = try quality_materializer.materialize(
            self.allocator,
            model.chat_tmpl orelse return error.MissingChatTemplate,
            model.getTokenizer(),
            messages,
            .{ .enable_thinking = false },
            request.policy,
            self.max_context_tokens - request.reserved_output_tokens,
            request.reserved_output_tokens,
            model.manifest.add_bos_token,
            model.manifest.bos_token,
            request.reference_utf8,
        );
        return .{ .materialized = materialized, .loaded_identity = self.loaded_identity };
    }

    pub fn run(self: *Session) !void {
        if (!@import("builtin").is_test or self.loaded_model.session.backend() != .metal or
            self.loaded_identity.load_generation == 0 or
            self.loaded_identity.load_generation != self.loaded_model.load_generation or
            !std.mem.eql(u8, &self.loaded_identity.implementation_sha256, &@import("compaction_identity").digest) or
            !std.mem.allEqual(u8, &self.loaded_identity.profile_sha256, 0) or
            self.max_context_tokens < 8192 or self.max_context_tokens > 65536 or
            self.max_kv_tokens != self.max_context_tokens / 2)
            return error.InvalidNativeQualityMaterializer;
        var stdin_buffer: [max_command_bytes]u8 = undefined;
        var stdin = std.Io.File.stdin().readerStreaming(self.io, &stdin_buffer);
        var stdout_buffer: [64 * 1024]u8 = undefined;
        var stdout = std.Io.File.stdout().writer(self.io, &stdout_buffer);
        while (try stdin.interface.takeDelimiter('\n')) |line| {
            if (line.len == 0) continue;
            var parsed = std.json.parseFromSlice(Command, self.allocator, line, .{ .ignore_unknown_fields = false }) catch |err| {
                try self.writeError(&stdout.interface, .{ .command = .stop }, err);
                return err;
            };
            defer parsed.deinit();
            self.validate(parsed.value) catch |err| {
                try self.writeError(&stdout.interface, parsed.value, err);
                return err;
            };
            if (parsed.value.command == .stop) return;
            self.executeQualityMaterialization(parsed.value, &stdout.interface) catch |err| {
                try self.writeError(&stdout.interface, parsed.value, err);
                return err;
            };
            self.next_request_index += 1;
        }
    }
};

test "context native quality materializer accepts only bound preparation commands" {
    var session: Session = .{
        .allocator = std.testing.allocator,
        .io = std.testing.io,
        .campaign_dir = undefined,
        .model = "fixture",
        .loaded_model = undefined,
        .loaded_identity = undefined,
        .expected_session_id = @splat('a'),
        .max_context_tokens = 16384,
        .max_kv_tokens = 8192,
    };
    const hash = "1111111111111111111111111111111111111111111111111111111111111111";
    const base: Command = .{
        .command = .materialize_quality_v1,
        .session_id = &session.expected_session_id,
        .request_index = 0,
        .variant = .attention_matching,
        .case_id = "case",
        .messages_file = "messages.json",
        .messages_sha256 = hash,
        .request_content_sha256 = hash,
        .source_sha256 = hash,
        .selected_part_sha256s = &.{hash},
        .reference_file = "reference.txt",
        .reference_sha256 = hash,
        .reserved_output_tokens = 256,
        .settings_sha256 = hash,
        .variant_settings_sha256 = hash,
    };
    try session.validate(base);
    var invalid = base;
    invalid.command = .quality;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(invalid));
    invalid = base;
    invalid.route = .native;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(invalid));
    invalid = base;
    invalid.session_id = hash;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(invalid));
    invalid = base;
    invalid.request_index = 1;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(invalid));
    invalid = base;
    invalid.reference_file = null;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(invalid));
    session.next_request_index = 2000;
    invalid = base;
    invalid.request_index = 2000;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(invalid));
    session.next_request_index = 0;
    session.profile_artifact_sha256[0] = 1;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(base));
    try session.validate(.{ .command = .stop, .session_id = &session.expected_session_id });
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(.{ .command = .stop }));
}
