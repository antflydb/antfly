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

//! Persistent JSONL command transport for the test-only serving driver.
const std = @import("std");
const driver_mod = @import("context_serving_driver.zig");
const coverage_mod = @import("context_capture_coverage.zig");
const coverage_trust = @import("context_serving_coverage_trust.zig");
const host_allocation_tracker = @import("runtime/kv/host_allocation_tracker.zig");
const context_execution_pool = @import("backends/context_execution_pool.zig");
const fault_injection = @import("context_fault_injection.zig");
const cli_process = @import("context_serving_cli_process.zig");
const ordered_messages = @import("context_serving_messages.zig");
const context_policy = @import("runtime/context_policy.zig");
const compaction_profile = @import("runtime/kv/compaction_profile.zig");

pub const max_command_bytes = 64 * 1024;
pub const Provenance = struct {
    binary_sha256: [32]u8,
    source_build_sha256: [32]u8,
    device_sha256: [32]u8,
    os_sha256: [32]u8,
};
pub const MixedSiblingCommand = struct {
    variant: driver_mod.Variant,
    document_file: []const u8,
    document_sha256: []const u8,
    question_file: []const u8,
    question_sha256: []const u8,
    request_content_sha256: []const u8,
    /// SHA-256 of the renderer-produced prompt, verified against the genuine
    /// server observation rather than the source document bytes.
    prompt_sha256: []const u8,
    prompt_tokens: u32,
    reserved_output_tokens: u32,
    settings_sha256: []const u8,
    variant_settings_sha256: []const u8,
};

pub const Command = struct {
    command: enum { next, measure, coverage, fault, quality, materialize_quality_v1, describe_provenance, stop },
    session_id: ?[]const u8 = null,
    request_index: ?u32 = null,
    route: ?driver_mod.Route = null,
    pair_id: ?[]const u8 = null,
    warmup: ?bool = null,
    variant: ?driver_mod.Variant = null,
    prompt_file: ?[]const u8 = null,
    prompt_sha256: ?[]const u8 = null,
    document_file: ?[]const u8 = null,
    question_file: ?[]const u8 = null,
    messages_file: ?[]const u8 = null,
    messages_sha256: ?[]const u8 = null,
    source_sha256: ?[]const u8 = null,
    selected_part_sha256s: ?[]const []const u8 = null,
    prompt_tokens: ?u32 = null,
    reserved_output_tokens: ?u32 = null,
    settings_sha256: ?[]const u8 = null,
    variant_settings_sha256: ?[]const u8 = null,
    fault: ?[]const u8 = null,
    case_id: ?[]const u8 = null,
    document_sha256: ?[]const u8 = null,
    question_sha256: ?[]const u8 = null,
    request_content_sha256: ?[]const u8 = null,
    bucket: ?coverage_mod.Bucket = null,
    binary_receipt_id: ?[]const u8 = null,
    source_receipt_id: ?[]const u8 = null,
    reference_file: ?[]const u8 = null,
    reference_sha256: ?[]const u8 = null,
    reference_tokens_file: ?[]const u8 = null,
    reference_tokens_sha256: ?[]const u8 = null,
    scorer: ?[]const u8 = null,
    siblings: ?[]const MixedSiblingCommand = null,
};

fn validateContextAllocationObservation(observation: @import("server/server.zig").ContextServingObservation) !void {
    if (!observation.full_owner_physical_available or
        observation.context_allocation_scope_version != host_allocation_tracker.context_allocation_scope_schema_version or
        !observation.context_allocation_scope_complete or
        observation.model_execution_pool_admission_version != context_execution_pool.admission_version or
        observation.model_execution_pool_admitted_backend_bytes != context_execution_pool.admitted_backend_bytes or
        !observation.owner_released or observation.owner_post_release_live_bytes != @as(?u64, 0) or
        !observation.owner_measurement.available or observation.owner_measurement.released_baseline_bytes != @as(?u64, 0))
        return error.IncompleteContextAllocationScope;
}

pub const Session = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    driver: *driver_mod.Driver,
    campaign_dir: std.Io.Dir,
    model: []const u8,
    expected_session_id: [64]u8,
    max_context_tokens: u32,
    max_kv_tokens: u32,
    keep_prefix_tokens: u32,
    keep_recent_tokens: u32,
    optimize_decode: bool,
    optimize_prefill: bool,
    /// Derived from the entrypoint's loaded profile. Commands deliberately
    /// have no corresponding field and therefore cannot switch fit engines.
    fit_engine: compaction_profile.FitEngine = .block_v1,
    admitted_prefill_chunk_rows: u32 = coverage_mod.required_prefill_chunk_rows,
    /// Derived by the test-binary entrypoint from the compiled implementation
    /// descriptor actually selected for this persistent session. Commands may
    /// repeat the resulting variant digest, but cannot supply this identity.
    implementation_sha256: [32]u8,
    /// Exact selected frozen artifact and semantic profile identities. The
    /// profile-free coverage bootstrap supplies explicit zero digests.
    profile_artifact_sha256: [32]u8,
    profile_semantic_sha256: [32]u8,
    /// The executable digest is self-measured by the entrypoint. Source,
    /// device, and OS bindings are inherited from the parent collector's
    /// separately audited campaign authority, never request JSON.
    provenance: Provenance = .{
        .binary_sha256 = @splat(0),
        .source_build_sha256 = @splat(0),
        .device_sha256 = @splat(0),
        .os_sha256 = @splat(0),
    },
    coverage: ?*coverage_trust.Trust = null,
    /// Separately measured production CLI executable. It remains a child
    /// client of this persistent server and has its own binary identity.
    cli_runner: ?*const cli_process.Runner = null,
    /// Profile-free bootstrap accepts only genuine exact-packed preparation
    /// coverage. It cannot run serving/performance/quality commands.
    coverage_only: bool = false,
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

    fn writeCampaignLogFile(self: *Session, path: []const u8, bytes: []const u8) ![32]u8 {
        if (!validRelative(path)) return error.InvalidServingDriverPath;
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

    fn validateFitEngineIdentity(self: *const Session) !void {
        const expected = compaction_profile.implementationDigest(
            compaction_profile.servingImplementation(
                self.fit_engine,
                self.optimize_decode,
                self.optimize_prefill,
            ),
        );
        if (!std.mem.eql(u8, &expected, &self.implementation_sha256))
            return error.ContextQualificationFitEngineMismatch;
    }

    fn validate(self: *Session, command: Command) !void {
        if (command.command == .stop) {
            if (command.session_id) |session_id|
                if (!std.mem.eql(u8, session_id, &self.expected_session_id)) return error.InvalidServingDriverCommand;
            return;
        }
        if (self.coverage_only and command.command != .coverage and command.command != .materialize_quality_v1)
            return error.CoverageBootstrapCommandRequired;
        const session_id = command.session_id orelse return error.InvalidServingDriverCommand;
        if (!std.mem.eql(u8, session_id, &self.expected_session_id) or
            command.request_index != self.next_request_index)
            return error.InvalidServingDriverCommand;
        if (command.command == .describe_provenance) {
            if (command.route != null or command.pair_id != null or command.warmup != null or
                command.variant != null or command.prompt_file != null or command.prompt_sha256 != null or
                command.document_file != null or command.question_file != null or command.messages_file != null or
                command.messages_sha256 != null or command.source_sha256 != null or
                command.selected_part_sha256s != null or command.prompt_tokens != null or
                command.reserved_output_tokens != null or command.settings_sha256 != null or
                command.variant_settings_sha256 != null or command.fault != null or command.case_id != null or
                command.document_sha256 != null or command.question_sha256 != null or
                command.request_content_sha256 != null or command.bucket != null or
                command.binary_receipt_id != null or command.source_receipt_id != null or
                command.reference_file != null or command.reference_sha256 != null or
                command.reference_tokens_file != null or command.reference_tokens_sha256 != null or
                command.scorer != null or command.siblings != null)
                return error.InvalidServingDriverCommand;
            return;
        }
        if (command.command == .materialize_quality_v1) {
            const ordered = command.messages_file != null and command.messages_sha256 != null and
                command.source_sha256 != null and command.selected_part_sha256s != null and
                command.document_file == null and command.document_sha256 == null and
                command.question_file == null and command.question_sha256 == null;
            if (!self.coverage_only or !std.mem.allEqual(u8, &self.profile_artifact_sha256, 0) or
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
            return;
        }
        if (command.route == null) return error.InvalidServingDriverCommand;
        if (command.command == .fault) {
            const fault = command.fault orelse return error.InvalidServingDriverCommand;
            if (command.route.? != .native or command.variant != .attention_matching or
                command.document_file == null or command.document_sha256 == null or
                command.question_file == null or command.question_sha256 == null or
                command.messages_file != null or command.messages_sha256 != null or command.source_sha256 != null or
                command.selected_part_sha256s != null or
                command.request_content_sha256 == null or command.prompt_file != null or
                command.prompt_sha256 == null or command.prompt_tokens == null or
                command.reserved_output_tokens == null or command.settings_sha256 == null or
                command.variant_settings_sha256 == null or command.pair_id != null or command.warmup != null)
                return error.InvalidServingDriverCommand;
            if (!std.mem.eql(u8, fault, "cancel_prefill") and
                !std.mem.eql(u8, fault, "cancel_replay") and
                !std.mem.eql(u8, fault, "cancel_fit") and
                !std.mem.eql(u8, fault, "cancel_publish") and
                !std.mem.eql(u8, fault, "oom") and
                !std.mem.eql(u8, fault, "nonfinite") and
                !std.mem.eql(u8, fault, "gpu_failure") and
                !std.mem.eql(u8, fault, "reload"))
                return error.UnsupportedFaultQualification;
        }
        if (command.route.? == .serial_mixed_batch) {
            if ((command.command != .measure and command.command != .next) or
                command.siblings == null or command.siblings.?.len < 2 or
                command.settings_sha256 == null or command.prompt_file != null or command.prompt_sha256 != null or
                command.document_file != null or command.question_file != null or
                command.messages_file != null or command.messages_sha256 != null or command.source_sha256 != null or
                command.selected_part_sha256s != null or
                command.prompt_tokens != null or command.variant != null or command.variant_settings_sha256 != null)
                return error.InvalidServingDriverCommand;
            var exact: usize = 0;
            var attention: usize = 0;
            for (command.siblings.?) |sibling| {
                if (sibling.document_file.len == 0 or sibling.document_sha256.len != 64 or
                    sibling.question_file.len == 0 or sibling.question_sha256.len != 64 or
                    sibling.request_content_sha256.len != 64 or sibling.prompt_sha256.len != 64 or
                    sibling.reserved_output_tokens == 0 or sibling.settings_sha256.len != 64 or
                    sibling.variant_settings_sha256.len != 64)
                    return error.InvalidServingDriverCommand;
                switch (sibling.variant) {
                    .exact => exact += 1,
                    .attention_matching => attention += 1,
                    .packed_exact => return error.UnsupportedMixedBatchVariant,
                }
            }
            if (exact == 0 or attention != 1) return error.InvalidServingDriverCommand;
            return;
        }
        if (command.variant == null or command.reserved_output_tokens == null)
            return error.InvalidServingDriverCommand;
        if ((command.command == .measure or command.command == .next or command.command == .fault) and
            (command.document_file == null or command.document_sha256 == null or
                command.question_file == null or command.question_sha256 == null or command.request_content_sha256 == null or
                command.messages_file != null or command.messages_sha256 != null or command.source_sha256 != null or
                command.selected_part_sha256s != null or
                command.prompt_file != null or command.prompt_sha256 == null or command.prompt_tokens == null or
                command.settings_sha256 == null or command.variant_settings_sha256 == null))
            return error.InvalidServingDriverCommand;
        if (command.command == .measure and (command.pair_id == null or command.warmup == null or command.route.? != .native))
            return error.UnsupportedPerformanceRoute;
        const legacy_quality_input = command.document_file != null and command.document_sha256 != null and
            command.question_file != null and command.question_sha256 != null and
            command.messages_file == null and command.messages_sha256 == null and command.source_sha256 == null and
            command.selected_part_sha256s == null;
        const ordered_quality_input = command.messages_file != null and command.messages_sha256 != null and
            command.source_sha256 != null and command.selected_part_sha256s != null and
            command.document_file == null and command.document_sha256 == null and
            command.question_file == null and command.question_sha256 == null;
        if (command.command == .quality and (command.case_id == null or command.bucket == null or
            (command.variant.? != .exact and command.variant.? != .attention_matching) or
            (!legacy_quality_input and !ordered_quality_input) or
            command.request_content_sha256 == null or command.prompt_file == null or
            command.prompt_sha256 == null or command.prompt_tokens == null or
            command.reference_file == null or command.reference_sha256 == null or
            command.reference_tokens_file == null or command.reference_tokens_sha256 == null or
            command.scorer == null or !std.mem.eql(u8, command.scorer.?, "utf8_nfc_trim_exact_v1") or
            command.settings_sha256 == null or command.variant_settings_sha256 == null or
            command.pair_id != null or command.warmup != null or command.fault != null or
            command.siblings != null))
            return error.InvalidServingDriverCommand;
        if (command.command == .next and (command.pair_id != null or command.warmup != null or
            command.variant.? != .attention_matching or command.route.? == .serial_mixed_batch))
            return error.UnsupportedSoakRoute;
        if (command.command == .coverage and (command.variant.? != .packed_exact or
            command.prompt_file != null or command.prompt_sha256 != null or command.document_file != null or
            command.question_file != null or
            command.messages_file != null or command.messages_sha256 != null or command.source_sha256 != null or
            command.selected_part_sha256s != null or
            command.case_id == null or command.document_sha256 == null or command.question_sha256 == null or
            command.request_content_sha256 == null or command.bucket == null or
            command.binary_receipt_id == null or command.source_receipt_id == null or
            command.settings_sha256 == null or command.variant_settings_sha256 == null))
            return error.InvalidServingDriverCommand;
        if (command.route.? == .cli and command.command != .next)
            return error.UnsupportedCliQualification;
        if (command.route.? == .cli and self.cli_runner == null)
            return error.MissingCliQualificationRunner;
    }

    fn parseReferenceTokens(self: *Session, bytes: []const u8) ![]i64 {
        var parsed = try std.json.parseFromSlice([]i64, self.allocator, bytes, .{});
        defer parsed.deinit();
        if (parsed.value.len < @import("backends/context_teacher_forced.zig").min_reference_tokens or
            parsed.value.len > @import("backends/context_teacher_forced.zig").max_reference_tokens)
            return error.InvalidQualityReferenceTokens;
        const result = try self.allocator.dupe(i64, parsed.value);
        errdefer self.allocator.free(result);
        for (result) |token|
            if (token < 0 or token > std.math.maxInt(i32)) return error.InvalidQualityReferenceTokens;
        return result;
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
        var evidence = try self.driver.materializeQuality(.{
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

    fn executeMixedBatch(self: *Session, command: Command, writer: *std.Io.Writer) !void {
        const commands = command.siblings.?;
        const documents = try self.allocator.alloc([]u8, commands.len);
        defer self.allocator.free(documents);
        const questions = try self.allocator.alloc([]u8, commands.len);
        defer self.allocator.free(questions);
        var loaded: usize = 0;
        defer for (0..loaded) |index| {
            self.allocator.free(documents[index]);
            self.allocator.free(questions[index]);
        };
        const siblings = try self.allocator.alloc(driver_mod.MixedSibling, commands.len);
        defer self.allocator.free(siblings);
        const common_reserved_output_tokens = commands[0].reserved_output_tokens;
        try expectHex(command.settings_sha256.?, generationSettings(common_reserved_output_tokens));
        for (commands, 0..) |item, index| {
            if (item.reserved_output_tokens != common_reserved_output_tokens)
                return error.MixedBatchGenerationSettingsMismatch;
            documents[index] = try self.readCampaignFile(item.document_file);
            questions[index] = self.readCampaignFile(item.question_file) catch |err| {
                self.allocator.free(documents[index]);
                return err;
            };
            loaded += 1;
            try expectHex(item.document_sha256, digest(documents[index]));
            try expectHex(item.question_sha256, digest(questions[index]));
            try expectHex(item.request_content_sha256, coverage_trust.structuredRequestDigest(documents[index], questions[index]));
            try expectHex(item.settings_sha256, generationSettings(item.reserved_output_tokens));
            try expectHex(item.variant_settings_sha256, self.variantSettings(item.variant));
            siblings[index] = .{
                .variant = item.variant,
                .structured = .{ .document = documents[index], .question = questions[index] },
                .reserved_output_tokens = item.reserved_output_tokens,
            };
        }
        var evidence = try self.driver.executeMixedBatch(.{
            .route = .serial_mixed_batch,
            .variant = .exact,
            .model = self.model,
            .prompt = "",
            .max_context_tokens = self.max_context_tokens,
            .max_kv_tokens = self.max_kv_tokens,
            .keep_prefix_tokens = self.keep_prefix_tokens,
            .keep_recent_tokens = self.keep_recent_tokens,
            .reserved_output_tokens = 1,
            .optimize_decode = self.optimize_decode,
            .optimize_prefill = self.optimize_prefill,
            .fit_engine = self.fit_engine,
            .admitted_prefill_chunk_rows = self.admitted_prefill_chunk_rows,
        }, siblings);
        defer evidence.deinit();

        var exact_groups: usize = 0;
        var attention_groups: usize = 0;
        for (evidence.allocations) |allocation| {
            const receipt = allocation.receipt;
            if (!receipt.released or
                receipt.context_allocation_scope_version != host_allocation_tracker.context_allocation_scope_schema_version or
                receipt.context_batch_scope_version != host_allocation_tracker.context_batch_allocation_scope_schema_version or
                !receipt.context_allocation_scope_complete or !receipt.full_owner_physical_available or
                !receipt.measurement.available or receipt.measurement.released_baseline_bytes != 0 or
                receipt.model_execution_pool_admission_version != context_execution_pool.admission_version or
                receipt.model_execution_pool_admitted_backend_bytes != context_execution_pool.admitted_backend_bytes or
                receipt.context_batch_expected_items != allocation.member_request_indices.len or
                receipt.context_batch_completed_items != allocation.member_request_indices.len)
                return error.IncompleteContextBatchAllocationScope;
            switch (receipt.context_batch_kind orelse return error.IncompleteContextBatchAllocationScope) {
                .exact_siblings => {
                    exact_groups += 1;
                    var expected_index: usize = 0;
                    for (commands, 0..) |item, index| {
                        if (item.variant != .exact) continue;
                        if (expected_index >= allocation.member_request_indices.len or
                            allocation.member_request_indices[expected_index] != index)
                            return error.ContextBatchMemberMismatch;
                        expected_index += 1;
                    }
                    if (expected_index != allocation.member_request_indices.len)
                        return error.ContextBatchMemberMismatch;
                },
                .attention_matching_singleton => {
                    attention_groups += 1;
                    if (allocation.member_request_indices.len != 1 or allocation.member_request_indices[0] >= commands.len or
                        commands[allocation.member_request_indices[0]].variant != .attention_matching)
                        return error.ContextBatchMemberMismatch;
                },
            }
        }
        if (exact_groups != 1 or attention_groups != 1) return error.ContextBatchGroupMismatch;
        if (evidence.items.len != commands.len) return error.IncompleteContextBatchServingObservations;

        const ValidatedItem = struct {
            allocation_group_index: usize,
            output_sha256: [32]u8,
        };
        const validated = try self.allocator.alloc(ValidatedItem, evidence.items.len);
        defer self.allocator.free(validated);
        var loaded_generation: ?u64 = null;
        for (evidence.items, validated, 0..) |item_evidence, *validated_item, item_position| {
            if (item_evidence.request_index != item_position) return error.ContextBatchMemberMismatch;
            const item = commands[item_position];
            const observation = item_evidence.observation;
            if (!observation.route_completed or observation.prompt_tokens != item.prompt_tokens or
                observation.reserved_output_tokens != item.reserved_output_tokens or
                observation.output_token_ids_len != item.reserved_output_tokens or
                item_evidence.elapsed_ns == 0 or item_evidence.ttft_ns == 0 or item_evidence.decode_ns == 0 or
                item_evidence.ttft_ns > item_evidence.elapsed_ns or item_evidence.decode_ns > item_evidence.elapsed_ns or
                observation.backend_kv_readback_after < observation.backend_kv_readback_before)
                return error.IncompleteContextBatchServingObservation;
            try expectHex(item.prompt_sha256, observation.rendered_prompt_sha256);
            try expectHex(item.variant_settings_sha256, self.variantSettings(item.variant));
            if (item.variant == .attention_matching) {
                if (!observation.published) return error.IncompleteContextBatchServingObservation;
            } else if (observation.published) return error.IncompleteContextBatchServingObservation;
            if (!std.mem.eql(u8, &observation.loaded_identity.implementation_sha256, &self.implementation_sha256) or
                !std.mem.eql(u8, &observation.loaded_identity.profile_sha256, &self.profile_artifact_sha256) or
                !std.mem.eql(u8, &observation.profile_semantic_digest, &self.profile_semantic_sha256))
                return error.ContextBatchLoadedIdentityMismatch;
            if (loaded_generation) |generation| {
                if (generation != observation.loaded_identity.load_generation)
                    return error.ContextBatchLoadedGenerationMismatch;
            } else loaded_generation = observation.loaded_identity.load_generation;

            var allocation_group_index: ?usize = null;
            for (evidence.allocations, 0..) |allocation, allocation_index| {
                if (std.mem.indexOfScalar(usize, allocation.member_request_indices, item_position) == null) continue;
                if (allocation_group_index != null) return error.ContextBatchMemberMismatch;
                const kind = allocation.receipt.context_batch_kind orelse return error.ContextBatchMemberMismatch;
                if ((item.variant == .exact and kind != .exact_siblings) or
                    (item.variant == .attention_matching and kind != .attention_matching_singleton))
                    return error.ContextBatchMemberMismatch;
                allocation_group_index = allocation_index;
            }
            validated_item.allocation_group_index = allocation_group_index orelse return error.ContextBatchMemberMismatch;
            var output_hash = std.crypto.hash.sha2.Sha256.init(.{});
            for (observation.output_token_ids[0..observation.output_token_ids_len]) |id| {
                var bytes: [4]u8 = undefined;
                std.mem.writeInt(u32, &bytes, id, .little);
                output_hash.update(&bytes);
            }
            output_hash.final(&validated_item.output_sha256);
        }

        // Semantic validation above is complete before any response byte is
        // emitted. Writer failures may truncate transport, but a bad later
        // item can never leave a syntactically plausible completed prefix.
        if (std.mem.allEqual(u8, &self.provenance.binary_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.source_build_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.device_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.os_sha256, 0))
            return error.MissingContextServingProvenance;
        const batch_identity = evidence.items[0].observation.loaded_identity;
        const batch_semantic = evidence.items[0].observation.profile_semantic_digest;
        for (evidence.items[1..]) |item|
            if (!std.mem.eql(u8, &item.observation.profile_semantic_digest, &batch_semantic))
                return error.MixedBatchProfileSemanticMismatch;
        try writer.writeAll("{\"schema_version\":2,\"completed\":true,\"operation\":\"mixed_batch\",\"route\":\"serial_mixed_batch\",\"scope_only\":false,\"serving_qualified\":true,\"semantic_identity\":");
        try std.json.Stringify.value(.{
            .artifact = std.fmt.bytesToHex(batch_identity.artifact_sha256, .lower),
            .tokenizer = std.fmt.bytesToHex(batch_identity.tokenizer_sha256, .lower),
            .template = std.fmt.bytesToHex(batch_identity.template_sha256, .lower),
            .layout = std.fmt.bytesToHex(batch_identity.layout_sha256, .lower),
            .profile = std.fmt.bytesToHex(batch_identity.profile_sha256, .lower),
            .implementation = std.fmt.bytesToHex(batch_identity.implementation_sha256, .lower),
            .dtype = "f16_kv_f32_bias",
            .kernel_route = "metal_context_optimized",
        }, .{}, writer);
        try writer.writeAll(",\"provenance\":");
        try std.json.Stringify.value(.{
            .binary_sha256 = std.fmt.bytesToHex(self.provenance.binary_sha256, .lower),
            .source_build_sha256 = std.fmt.bytesToHex(self.provenance.source_build_sha256, .lower),
            .device_sha256 = std.fmt.bytesToHex(self.provenance.device_sha256, .lower),
            .os_sha256 = std.fmt.bytesToHex(self.provenance.os_sha256, .lower),
        }, .{}, writer);
        try writer.writeAll(",\"profile_semantic_sha256\":");
        try std.json.Stringify.value(std.fmt.bytesToHex(batch_semantic, .lower), .{}, writer);
        try writer.writeAll(",\"session_id\":");
        try std.json.Stringify.value(&self.expected_session_id, .{}, writer);
        try writer.print(",\"request_index\":{d},\"items\":[", .{command.request_index.?});
        for (evidence.items, validated, 0..) |item_evidence, validated_item, item_position| {
            const item = commands[item_position];
            const observation = item_evidence.observation;
            const ids = observation.output_token_ids[0..observation.output_token_ids_len];
            const output_hex = std.fmt.bytesToHex(validated_item.output_sha256, .lower);
            const artifact_hex = std.fmt.bytesToHex(observation.loaded_identity.artifact_sha256, .lower);
            const tokenizer_hex = std.fmt.bytesToHex(observation.loaded_identity.tokenizer_sha256, .lower);
            const template_hex = std.fmt.bytesToHex(observation.loaded_identity.template_sha256, .lower);
            const layout_hex = std.fmt.bytesToHex(observation.loaded_identity.layout_sha256, .lower);
            const profile_hex = std.fmt.bytesToHex(observation.loaded_identity.profile_sha256, .lower);
            const implementation_hex = std.fmt.bytesToHex(observation.loaded_identity.implementation_sha256, .lower);
            const settings_hex = std.fmt.bytesToHex(generationSettings(item.reserved_output_tokens), .lower);
            const variant_hex = std.fmt.bytesToHex(self.variantSettings(item.variant), .lower);
            if (item_position != 0) try writer.writeByte(',');
            try std.json.Stringify.value(.{
                .request_index = item_position,
                .variant = @tagName(item.variant),
                .document_file = item.document_file,
                .document_sha256 = item.document_sha256,
                .question_file = item.question_file,
                .question_sha256 = item.question_sha256,
                .request_content_sha256 = item.request_content_sha256,
                .prompt_sha256 = item.prompt_sha256,
                .prompt_tokens = observation.prompt_tokens,
                .reserved_output_tokens = observation.reserved_output_tokens,
                .settings_sha256 = settings_hex[0..],
                .variant_settings_sha256 = variant_hex[0..],
                .loaded_identity = .{
                    .load_generation = observation.loaded_identity.load_generation,
                    .artifact_sha256 = artifact_hex[0..],
                    .tokenizer_sha256 = tokenizer_hex[0..],
                    .template_sha256 = template_hex[0..],
                    .layout_sha256 = layout_hex[0..],
                    .profile_sha256 = profile_hex[0..],
                    .implementation_sha256 = implementation_hex[0..],
                },
                .elapsed_ns = item_evidence.elapsed_ns,
                .ttft_ns = item_evidence.ttft_ns,
                .decode_ns = item_evidence.decode_ns,
                .backend_kv_readback_bytes = observation.backend_kv_readback_after - observation.backend_kv_readback_before,
                .published = observation.published,
                .exact_fallback = false,
                .output_token_ids = ids,
                .output_sha256 = output_hex[0..],
                .allocation_group_index = validated_item.allocation_group_index,
            }, .{}, writer);
        }
        try writer.writeAll("],\"model_execution_pool\":");
        try std.json.Stringify.value(.{
            .admission_version = context_execution_pool.admission_version,
            .admitted_backend_bytes = context_execution_pool.admitted_backend_bytes,
            .shared_once = true,
        }, .{}, writer);
        try writer.writeAll(",\"allocations\":");
        try std.json.Stringify.value(evidence.allocations, .{}, writer);
        try writer.writeByte('}');
        try writer.writeByte('\n');
        try writer.flush();
    }

    fn elapsedNs(start: std.Io.Timestamp, finish: std.Io.Timestamp) !u64 {
        const value = std.Io.Timestamp.durationTo(start, finish).nanoseconds;
        if (value < 0) return error.InvalidContextQualificationTiming;
        return std.math.cast(u64, value) orelse error.InvalidContextQualificationTiming;
    }

    fn executeReloadFault(
        self: *Session,
        command: Command,
        request: driver_mod.Request,
        exact: *const driver_mod.Evidence,
        started: std.Io.Timestamp,
        writer: *std.Io.Writer,
    ) !void {
        const transition = try self.driver.reloadModelForQualification(
            request.model,
            request.fit_engine,
            request.optimize_decode,
            request.optimize_prefill,
        );
        const reloaded_at = std.Io.Timestamp.now(self.io, .awake);
        if (!transition.stale_selection_rejected) return error.StaleContextSelectionAccepted;
        var attention_request = request;
        attention_request.variant = .attention_matching;
        var attention = try self.driver.execute(attention_request);
        defer attention.deinit();
        const published_at = std.Io.Timestamp.now(self.io, .awake);
        var observer = try self.driver.execute(request);
        defer observer.deinit();
        if (exact.observation.loaded_identity.load_generation != transition.loaded_generation_before or
            attention.observation.loaded_identity.load_generation != transition.loaded_generation_after or
            observer.observation.loaded_identity.load_generation != transition.loaded_generation_after or
            transition.loaded_generation_before == transition.loaded_generation_after)
            return error.ContextReloadGenerationMismatch;
        var attention_identity = attention.observation.loaded_identity;
        attention_identity.load_generation = exact.observation.loaded_identity.load_generation;
        var observer_identity = observer.observation.loaded_identity;
        observer_identity.load_generation = exact.observation.loaded_identity.load_generation;
        if (!std.meta.eql(exact.observation.loaded_identity, attention_identity) or
            !std.meta.eql(exact.observation.loaded_identity, observer_identity))
            return error.ContextReloadIdentityMismatch;
        if (attention.observation.prompt_tokens != command.prompt_tokens.? or
            observer.observation.prompt_tokens != command.prompt_tokens.?)
            return error.ContextQualificationPromptTokenMismatch;
        try expectHex(command.prompt_sha256.?, attention.observation.rendered_prompt_sha256);
        try expectHex(command.prompt_sha256.?, observer.observation.rendered_prompt_sha256);
        const before_ids = exact.observation.output_token_ids[0..exact.observation.output_token_ids_len];
        const attention_ids = attention.observation.output_token_ids[0..attention.observation.output_token_ids_len];
        const observer_ids = observer.observation.output_token_ids[0..observer.observation.output_token_ids_len];
        if (!std.mem.eql(u32, before_ids, attention_ids) or
            !std.mem.eql(u32, before_ids, observer_ids) or
            before_ids.len != command.reserved_output_tokens.? or
            !attention.observation.published or observer.observation.published)
            return error.ContextFaultRecoveryMismatch;
        const before_owner = exact.observation.owner_allocation_initial;
        const after_owner = exact.observation.owner_allocation_final;
        if (!exact.observation.owner_released or exact.observation.owner_post_release_live_bytes != @as(?u64, 0) or
            !attention.observation.owner_released or attention.observation.owner_post_release_live_bytes != @as(?u64, 0) or
            !observer.observation.owner_released or observer.observation.owner_post_release_live_bytes != @as(?u64, 0) or
            !before_owner.tracked_combined_physical_available or !after_owner.tracked_combined_physical_available or
            before_owner.tracked_combined_live_bytes != after_owner.tracked_combined_live_bytes or
            attention.observation.owner_allocation_initial.tracked_combined_live_bytes != before_owner.tracked_combined_live_bytes or
            attention.observation.owner_allocation_final.tracked_combined_live_bytes != before_owner.tracked_combined_live_bytes or
            observer.observation.owner_allocation_initial.tracked_combined_live_bytes != before_owner.tracked_combined_live_bytes or
            observer.observation.owner_allocation_final.tracked_combined_live_bytes != before_owner.tracked_combined_live_bytes)
            return error.ContextFaultOwnerLeak;
        var output_hash = std.crypto.hash.sha2.Sha256.init(.{});
        for (before_ids) |id| {
            var encoded: [4]u8 = undefined;
            std.mem.writeInt(u32, &encoded, id, .little);
            output_hash.update(&encoded);
        }
        var output_digest: [32]u8 = undefined;
        output_hash.final(&output_digest);
        const output_hex = std.fmt.bytesToHex(output_digest, .lower);
        const identity = attention.observation.loaded_identity;
        const artifact_hex = std.fmt.bytesToHex(identity.artifact_sha256, .lower);
        const tokenizer_hex = std.fmt.bytesToHex(identity.tokenizer_sha256, .lower);
        const template_hex = std.fmt.bytesToHex(identity.template_sha256, .lower);
        const layout_hex = std.fmt.bytesToHex(identity.layout_sha256, .lower);
        const profile_hex = std.fmt.bytesToHex(identity.profile_sha256, .lower);
        const implementation_hex = std.fmt.bytesToHex(identity.implementation_sha256, .lower);
        const binary_hex = std.fmt.bytesToHex(self.provenance.binary_sha256, .lower);
        const source_hex = std.fmt.bytesToHex(self.provenance.source_build_sha256, .lower);
        const device_hex = std.fmt.bytesToHex(self.provenance.device_sha256, .lower);
        const os_hex = std.fmt.bytesToHex(self.provenance.os_sha256, .lower);
        const reloaded_ns = try elapsedNs(started, reloaded_at);
        const published_ns = try elapsedNs(started, published_at);
        const observer_ns = try elapsedNs(started, std.Io.Timestamp.now(self.io, .awake));
        const live = before_owner.tracked_combined_live_bytes;
        try std.json.Stringify.value(.{
            .schema_version = @as(u32, 2),
            .semantic_identity = .{ .artifact = artifact_hex[0..], .tokenizer = tokenizer_hex[0..], .template = template_hex[0..], .layout = layout_hex[0..], .profile = profile_hex[0..], .implementation = implementation_hex[0..], .dtype = "f16_kv_f32_bias", .kernel_route = "metal_context_optimized" },
            .provenance = .{ .binary_sha256 = binary_hex[0..], .source_build_sha256 = source_hex[0..], .device_sha256 = device_hex[0..], .os_sha256 = os_hex[0..] },
            .loaded_identity = .{ .load_generation = identity.load_generation, .artifact_sha256 = artifact_hex[0..], .tokenizer_sha256 = tokenizer_hex[0..], .template_sha256 = template_hex[0..], .layout_sha256 = layout_hex[0..], .profile_sha256 = profile_hex[0..], .implementation_sha256 = implementation_hex[0..] },
            .session_id = &self.expected_session_id,
            .request_index = command.request_index.?,
            .route = "native",
            .fault = "reload",
            .variant = "attention_matching",
            .document_file = command.document_file.?,
            .document_sha256 = command.document_sha256.?,
            .question_file = command.question_file.?,
            .question_sha256 = command.question_sha256.?,
            .request_content_sha256 = command.request_content_sha256.?,
            .prompt_sha256 = command.prompt_sha256.?,
            .settings_sha256 = command.settings_sha256.?,
            .variant_settings_sha256 = command.variant_settings_sha256.?,
            .injection = .{ .kind = "reload", .stage = "reload", .mechanism = "model_manager_generation_transition", .completed_units = @as(u32, 1), .total_units = @as(u32, 1) },
            .loaded_generation_before = transition.loaded_generation_before,
            .loaded_generation_after = transition.loaded_generation_after,
            .request_generation = transition.loaded_generation_after,
            .stale_selection_rejected = transition.stale_selection_rejected,
            .pre_reload_exact_generation = transition.loaded_generation_before,
            .attention_matching_generation = transition.loaded_generation_after,
            .exact_after_generation = transition.loaded_generation_after,
            .prompt_tokens = command.prompt_tokens.?,
            .reserved_output_tokens = command.reserved_output_tokens.?,
            .live_bytes_before = live,
            .live_bytes_after_release = live,
            .context_disposition = "model_generation_reloaded",
            .original_context = null,
            .partial_context = null,
            .maintenance_kv_readback_bytes = @as(u64, 0),
            .pre_reload_exact_output_tokens = before_ids,
            .pre_reload_exact_output_sha256 = output_hex[0..],
            .attention_matching_output_tokens = attention_ids,
            .attention_matching_output_sha256 = output_hex[0..],
            .exact_after_output_tokens = observer_ids,
            .exact_after_output_sha256 = output_hex[0..],
            .exact_output_tokens = observer_ids,
            .observer_output_tokens = attention_ids,
            .exact_output_sha256 = output_hex[0..],
            .observer_output_sha256 = output_hex[0..],
            .events = &.{
                .{ .name = "request_started", .monotonic_ns = @as(u64, 0), .live_bytes = live },
                .{ .name = "fault_injected", .monotonic_ns = reloaded_ns, .live_bytes = live, .fault = "reload" },
                .{ .name = "model_generation_reloaded", .monotonic_ns = reloaded_ns, .live_bytes = live },
                .{ .name = "stale_selection_rejected", .monotonic_ns = reloaded_ns, .live_bytes = live },
                .{ .name = "fresh_attention_matching_published", .monotonic_ns = published_ns, .live_bytes = live },
                .{ .name = "observer_verified", .monotonic_ns = observer_ns, .live_bytes = live },
                .{ .name = "request_released", .monotonic_ns = observer_ns, .live_bytes = live },
            },
        }, .{}, writer);
        try writer.writeByte('\n');
        try writer.flush();
    }

    fn executeFault(
        self: *Session,
        command: Command,
        document: []const u8,
        question: []const u8,
        writer: *std.Io.Writer,
    ) !void {
        const kind: fault_injection.Kind = if (std.mem.eql(u8, command.fault.?, "cancel_prefill"))
            .cancel_prefill
        else if (std.mem.eql(u8, command.fault.?, "cancel_replay"))
            .cancel_replay
        else if (std.mem.eql(u8, command.fault.?, "cancel_fit"))
            .cancel_fit
        else if (std.mem.eql(u8, command.fault.?, "cancel_publish"))
            .cancel_publish
        else if (std.mem.eql(u8, command.fault.?, "oom"))
            .oom
        else if (std.mem.eql(u8, command.fault.?, "nonfinite"))
            .nonfinite
        else if (std.mem.eql(u8, command.fault.?, "gpu_failure"))
            .gpu_failure
        else if (std.mem.eql(u8, command.fault.?, "reload"))
            .cancel_publish // Reload branches before a controller is created.
        else
            return error.UnsupportedFaultQualification;
        const started = std.Io.Timestamp.now(self.io, .awake);
        const request = driver_mod.Request{
            .route = .native,
            .variant = .exact,
            .model = self.model,
            .prompt = "",
            .structured = .{ .document = document, .question = question },
            .max_context_tokens = self.max_context_tokens,
            .max_kv_tokens = self.max_kv_tokens,
            .keep_prefix_tokens = self.keep_prefix_tokens,
            .keep_recent_tokens = self.keep_recent_tokens,
            .reserved_output_tokens = command.reserved_output_tokens.?,
            .optimize_decode = self.optimize_decode,
            .optimize_prefill = self.optimize_prefill,
            .fit_engine = self.fit_engine,
            .admitted_prefill_chunk_rows = self.admitted_prefill_chunk_rows,
        };
        var exact = try self.driver.execute(request);
        defer exact.deinit();
        if (exact.observation.prompt_tokens != command.prompt_tokens.?)
            return error.ContextQualificationPromptTokenMismatch;
        try expectHex(command.prompt_sha256.?, exact.observation.rendered_prompt_sha256);
        if (std.mem.eql(u8, command.fault.?, "reload"))
            return self.executeReloadFault(command, request, &exact, started, writer);
        var controller = fault_injection.Controller{ .kind = kind };
        var fault_request = request;
        fault_request.variant = .attention_matching;
        const failed = try self.driver.executeNativeFault(fault_request, &controller);
        const injected_at = std.Io.Timestamp.now(self.io, .awake);
        var observer = try self.driver.execute(request);
        defer observer.deinit();
        if (observer.observation.prompt_tokens != command.prompt_tokens.?)
            return error.ContextQualificationPromptTokenMismatch;
        try expectHex(command.prompt_sha256.?, observer.observation.rendered_prompt_sha256);
        const observer_at = std.Io.Timestamp.now(self.io, .awake);

        const prefill_destroyed = kind == .cancel_prefill;
        const admission_rolled_back = kind == .oom;
        const empty_fingerprint = @import("backends/packed_context_session.zig").FailureFingerprint{ .epoch = 0, .logical_tokens = 0, .content_sha256 = @splat(0), .descriptors_sha256 = @splat(0), .kv_sha256 = @splat(0), .diagnostic_kv_readback_bytes = 0 };
        const before = if (prefill_destroyed)
            failed.partial_prefill orelse return error.MissingContextFaultFingerprint
        else if (admission_rolled_back)
            empty_fingerprint
        else
            failed.original_before orelse return error.MissingContextFaultFingerprint;
        const after = if (prefill_destroyed or admission_rolled_back)
            before
        else
            failed.original_after orelse return error.MissingContextFaultFingerprint;
        const expected_failure: anyerror = if (admission_rolled_back)
            error.OutOfMemory
        else if (kind == .nonfinite)
            error.NonFiniteCompactionInput
        else if (kind == .gpu_failure)
            error.ContextMetalDispatchFailed
        else
            error.Cancelled;
        if ((!prefill_destroyed and !admission_rolled_back and (!std.meta.eql(before, after) or failed.partial_prefill != null)) or
            (prefill_destroyed and (failed.original_before != null or failed.original_after != null or
                before.logical_tokens != failed.injection.completed_units)) or
            (admission_rolled_back and (failed.original_before != null or failed.original_after != null or failed.partial_prefill != null)) or
            failed.failure != expected_failure or
            failed.maintenance_kv_readback_after < failed.maintenance_kv_readback_before or
            failed.injection.kind != kind or !failed.injection.injected or
            failed.loaded_identity.load_generation != exact.observation.loaded_identity.load_generation or
            failed.loaded_identity.load_generation != observer.observation.loaded_identity.load_generation or
            !std.meta.eql(failed.loaded_identity, exact.observation.loaded_identity) or
            !std.meta.eql(failed.loaded_identity, observer.observation.loaded_identity))
            return error.ContextFaultIsolationFailed;
        const exact_ids = exact.observation.output_token_ids[0..exact.observation.output_token_ids_len];
        const observer_ids = observer.observation.output_token_ids[0..observer.observation.output_token_ids_len];
        if (!std.mem.eql(u32, exact_ids, observer_ids) or exact_ids.len != command.reserved_output_tokens.?)
            return error.ContextFaultRecoveryMismatch;
        const receipt = failed.release_receipt;
        if (!receipt.released or receipt.post_release_live_bytes != @as(?u64, 0) or
            !receipt.initial.tracked_combined_physical_available or !receipt.final.tracked_combined_physical_available or
            receipt.initial.tracked_combined_live_bytes != receipt.final.tracked_combined_live_bytes)
            return error.ContextFaultOwnerLeak;
        var output_hash = std.crypto.hash.sha2.Sha256.init(.{});
        for (exact_ids) |id| {
            var encoded: [4]u8 = undefined;
            std.mem.writeInt(u32, &encoded, id, .little);
            output_hash.update(&encoded);
        }
        var output_sha256: [32]u8 = undefined;
        output_hash.final(&output_sha256);
        const output_hex = std.fmt.bytesToHex(output_sha256, .lower);
        const artifact_hex = std.fmt.bytesToHex(failed.loaded_identity.artifact_sha256, .lower);
        const tokenizer_hex = std.fmt.bytesToHex(failed.loaded_identity.tokenizer_sha256, .lower);
        const template_hex = std.fmt.bytesToHex(failed.loaded_identity.template_sha256, .lower);
        const layout_hex = std.fmt.bytesToHex(failed.loaded_identity.layout_sha256, .lower);
        const profile_hex = std.fmt.bytesToHex(failed.loaded_identity.profile_sha256, .lower);
        const implementation_hex = std.fmt.bytesToHex(failed.loaded_identity.implementation_sha256, .lower);
        const binary_hex = std.fmt.bytesToHex(self.provenance.binary_sha256, .lower);
        const source_hex = std.fmt.bytesToHex(self.provenance.source_build_sha256, .lower);
        const device_hex = std.fmt.bytesToHex(self.provenance.device_sha256, .lower);
        const os_hex = std.fmt.bytesToHex(self.provenance.os_sha256, .lower);
        const before_content = std.fmt.bytesToHex(before.content_sha256, .lower);
        const before_descriptors = std.fmt.bytesToHex(before.descriptors_sha256, .lower);
        const before_kv = std.fmt.bytesToHex(before.kv_sha256, .lower);
        const after_content = std.fmt.bytesToHex(after.content_sha256, .lower);
        const after_descriptors = std.fmt.bytesToHex(after.descriptors_sha256, .lower);
        const after_kv = std.fmt.bytesToHex(after.kv_sha256, .lower);
        const SnapshotJson = struct {
            epoch: u64,
            logical_tokens: u32,
            content_sha256: []const u8,
            descriptors_sha256: []const u8,
            kv_sha256: []const u8,
            diagnostic_kv_readback_bytes: u64,
        };
        const OriginalJson = struct { before: SnapshotJson, after: SnapshotJson };
        const before_json = SnapshotJson{ .epoch = before.epoch, .logical_tokens = before.logical_tokens, .content_sha256 = before_content[0..], .descriptors_sha256 = before_descriptors[0..], .kv_sha256 = before_kv[0..], .diagnostic_kv_readback_bytes = before.diagnostic_kv_readback_bytes };
        const after_json = SnapshotJson{ .epoch = after.epoch, .logical_tokens = after.logical_tokens, .content_sha256 = after_content[0..], .descriptors_sha256 = after_descriptors[0..], .kv_sha256 = after_kv[0..], .diagnostic_kv_readback_bytes = after.diagnostic_kv_readback_bytes };
        const original_json: ?OriginalJson = if (prefill_destroyed or admission_rolled_back) null else .{ .before = before_json, .after = after_json };
        const partial_json: ?SnapshotJson = if (prefill_destroyed) before_json else null;
        const live = receipt.initial.tracked_combined_live_bytes;
        const injected_ns = try elapsedNs(started, injected_at);
        const observer_ns = try elapsedNs(started, observer_at);
        try std.json.Stringify.value(.{
            .schema_version = @as(u32, 2),
            .semantic_identity = .{
                .artifact = artifact_hex[0..],
                .tokenizer = tokenizer_hex[0..],
                .template = template_hex[0..],
                .layout = layout_hex[0..],
                .profile = profile_hex[0..],
                .implementation = implementation_hex[0..],
                .dtype = "f16_kv_f32_bias",
                .kernel_route = "metal_context_optimized",
            },
            .provenance = .{
                .binary_sha256 = binary_hex[0..],
                .source_build_sha256 = source_hex[0..],
                .device_sha256 = device_hex[0..],
                .os_sha256 = os_hex[0..],
            },
            .loaded_identity = .{
                .load_generation = failed.loaded_identity.load_generation,
                .artifact_sha256 = artifact_hex[0..],
                .tokenizer_sha256 = tokenizer_hex[0..],
                .template_sha256 = template_hex[0..],
                .layout_sha256 = layout_hex[0..],
                .profile_sha256 = profile_hex[0..],
                .implementation_sha256 = implementation_hex[0..],
            },
            .session_id = &self.expected_session_id,
            .request_index = command.request_index.?,
            .route = "native",
            .fault = command.fault.?,
            .variant = "attention_matching",
            .document_file = command.document_file.?,
            .document_sha256 = command.document_sha256.?,
            .question_file = command.question_file.?,
            .question_sha256 = command.question_sha256.?,
            .request_content_sha256 = command.request_content_sha256.?,
            .prompt_sha256 = command.prompt_sha256.?,
            .settings_sha256 = command.settings_sha256.?,
            .variant_settings_sha256 = command.variant_settings_sha256.?,
            .injection = .{
                .kind = @tagName(failed.injection.kind orelse return error.ContextFaultIsolationFailed),
                .stage = @tagName(failed.injection.stage orelse return error.ContextFaultIsolationFailed),
                .mechanism = switch (kind) {
                    .cancel_prefill, .cancel_replay, .cancel_fit => "cooperative_checkpoint_after_partial_work",
                    .cancel_publish => "transaction_boundary_cancellation",
                    .oom => "request_local_allocator_failure",
                    .nonfinite => "device_fitter_nonfinite_status",
                    .gpu_failure => "simulated_gpu_completion_error",
                },
                .completed_units = failed.injection.completed_units,
                .total_units = failed.injection.total_units,
            },
            .loaded_generation_before = failed.loaded_identity.load_generation,
            .loaded_generation_after = failed.loaded_identity.load_generation,
            .request_generation = failed.loaded_identity.load_generation,
            .prompt_tokens = command.prompt_tokens.?,
            .reserved_output_tokens = command.reserved_output_tokens.?,
            .live_bytes_before = live,
            .live_bytes_after_release = receipt.final.tracked_combined_live_bytes,
            .context_disposition = if (prefill_destroyed) "partial_prefill_destroyed" else if (admission_rolled_back) "preparation_allocation_rolled_back" else "original_context_unchanged",
            .original_context = original_json,
            .partial_context = partial_json,
            .maintenance_kv_readback_bytes = failed.maintenance_kv_readback_after - failed.maintenance_kv_readback_before,
            .exact_output_tokens = exact_ids,
            .observer_output_tokens = observer_ids,
            .exact_output_sha256 = output_hex[0..],
            .observer_output_sha256 = output_hex[0..],
            .events = &.{
                .{ .name = "request_started", .monotonic_ns = @as(u64, 0), .live_bytes = live },
                .{ .name = "fault_injected", .monotonic_ns = injected_ns, .live_bytes = live, .fault = command.fault.? },
                .{ .name = if (prefill_destroyed) "partial_context_destroyed" else if (admission_rolled_back) "allocation_rollback_verified" else "original_context_verified", .monotonic_ns = injected_ns, .live_bytes = live },
                .{ .name = "observer_verified", .monotonic_ns = observer_ns, .live_bytes = live },
                .{ .name = "request_released", .monotonic_ns = observer_ns, .live_bytes = live },
            },
        }, .{}, writer);
        try writer.writeByte('\n');
        try writer.flush();
    }

    fn executeProvenance(self: *Session, command: Command, writer: *std.Io.Writer) !void {
        if (self.coverage_only) return error.UnsupportedLoadedProvenance;
        var evidence = try self.driver.describeLoadedCompactionProvenance(
            self.model,
            self.fit_engine,
            self.optimize_decode,
            self.optimize_prefill,
        );
        defer evidence.deinit();
        const loaded = evidence.loaded_identity;
        if (!std.mem.eql(u8, &loaded.implementation_sha256, &self.implementation_sha256) or
            !std.mem.eql(u8, &loaded.profile_sha256, &self.profile_artifact_sha256) or
            !std.mem.eql(u8, &evidence.profile_semantic_sha256, &self.profile_semantic_sha256) or
            std.mem.allEqual(u8, &self.provenance.binary_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.source_build_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.device_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.os_sha256, 0))
            return error.ContextQualificationLoadedIdentityMismatch;

        var tokenizer_name_buffer: [128]u8 = undefined;
        const tokenizer_name = try std.fmt.bufPrint(
            &tokenizer_name_buffer,
            "loaded-tokenizer-{d:0>6}.json",
            .{command.request_index.?},
        );
        var template_name_buffer: [128]u8 = undefined;
        const template_name = try std.fmt.bufPrint(
            &template_name_buffer,
            "loaded-template-{d:0>6}.jinja",
            .{command.request_index.?},
        );
        var receipt_name_buffer: [128]u8 = undefined;
        const receipt_name = try std.fmt.bufPrint(
            &receipt_name_buffer,
            "loaded-provenance-{d:0>6}.json",
            .{command.request_index.?},
        );
        const tokenizer_digest = try self.writeCampaignEvidenceFile(
            tokenizer_name,
            evidence.exported.tokenizer_source,
        );
        const template_digest = try self.writeCampaignEvidenceFile(
            template_name,
            evidence.exported.template_source,
        );
        const tokenizer_hex = std.fmt.bytesToHex(tokenizer_digest, .lower);
        const template_file_hex = std.fmt.bytesToHex(template_digest, .lower);
        const artifact_hex = std.fmt.bytesToHex(loaded.artifact_sha256, .lower);
        const loaded_tokenizer_hex = std.fmt.bytesToHex(loaded.tokenizer_sha256, .lower);
        const loaded_template_hex = std.fmt.bytesToHex(loaded.template_sha256, .lower);
        const layout_hex = std.fmt.bytesToHex(loaded.layout_sha256, .lower);
        const profile_hex = std.fmt.bytesToHex(loaded.profile_sha256, .lower);
        const implementation_hex = std.fmt.bytesToHex(loaded.implementation_sha256, .lower);
        const parsed_tokenizer_hex = std.fmt.bytesToHex(evidence.exported.tokenizer_parsed_source_sha256, .lower);
        const profile_semantic_hex = std.fmt.bytesToHex(evidence.profile_semantic_sha256, .lower);
        const binary_hex = std.fmt.bytesToHex(self.provenance.binary_sha256, .lower);
        const source_build_hex = std.fmt.bytesToHex(self.provenance.source_build_sha256, .lower);
        const device_hex = std.fmt.bytesToHex(self.provenance.device_sha256, .lower);
        const os_hex = std.fmt.bytesToHex(self.provenance.os_sha256, .lower);
        const receipt_bytes = try std.json.Stringify.valueAlloc(self.allocator, .{
            .schema_version = @as(u32, 1),
            .stage = "loaded_compaction_provenance",
            .completed = true,
            .request = .{
                .session_id = &self.expected_session_id,
                .request_index = command.request_index.?,
            },
            .semantic_identity = .{
                .artifact = artifact_hex[0..],
                .tokenizer = loaded_tokenizer_hex[0..],
                .template = loaded_template_hex[0..],
                .layout = layout_hex[0..],
                .profile = profile_hex[0..],
                .implementation = implementation_hex[0..],
                .dtype = "f16_kv_f32_bias",
                .kernel_route = "metal_context_optimized",
            },
            .provenance = .{
                .binary_sha256 = binary_hex[0..],
                .source_build_sha256 = source_build_hex[0..],
                .device_sha256 = device_hex[0..],
                .os_sha256 = os_hex[0..],
            },
            .loaded_identity = .{
                .load_generation = loaded.load_generation,
                .artifact_sha256 = artifact_hex[0..],
                .tokenizer_sha256 = loaded_tokenizer_hex[0..],
                .template_sha256 = loaded_template_hex[0..],
                .layout_sha256 = layout_hex[0..],
                .profile_sha256 = profile_hex[0..],
                .implementation_sha256 = implementation_hex[0..],
            },
            .tokenizer = .{
                .kind = @tagName(evidence.exported.tokenizer_kind),
                .source = .{ .path = tokenizer_name, .sha256 = tokenizer_hex[0..] },
                .parsed_source_sha256 = parsed_tokenizer_hex[0..],
                .gguf_metadata = evidence.exported.tokenizer_gguf_metadata,
                .policy = .{
                    .add_bos_token = evidence.exported.tokenizer_add_bos_token,
                    .bos_token = evidence.exported.tokenizer_bos_token,
                },
                .loaded_sha256 = loaded_tokenizer_hex[0..],
            },
            .template = .{
                .source = .{ .path = template_name, .sha256 = template_file_hex[0..] },
                .bos_token = evidence.exported.template_bos_token,
                .eos_token = evidence.exported.template_eos_token,
                .unk_token = evidence.exported.template_unk_token,
                .pad_token = evidence.exported.template_pad_token,
                .loaded_sha256 = loaded_template_hex[0..],
            },
            .profile = .{
                .artifact_sha256 = profile_hex[0..],
                .semantic_sha256 = profile_semantic_hex[0..],
            },
        }, .{});
        defer self.allocator.free(receipt_bytes);
        const receipt_digest = try self.writeCampaignEvidenceFile(receipt_name, receipt_bytes);
        const receipt_hex = std.fmt.bytesToHex(receipt_digest, .lower);
        try std.json.Stringify.value(.{
            .schema_version = @as(u32, 2),
            .completed = true,
            .operation = "describe_provenance",
            .session_id = &self.expected_session_id,
            .request_index = command.request_index.?,
            .loaded_identity = .{
                .load_generation = loaded.load_generation,
                .artifact_sha256 = artifact_hex[0..],
                .tokenizer_sha256 = loaded_tokenizer_hex[0..],
                .template_sha256 = loaded_template_hex[0..],
                .layout_sha256 = layout_hex[0..],
                .profile_sha256 = profile_hex[0..],
                .implementation_sha256 = implementation_hex[0..],
            },
            .loaded_provenance = .{ .path = receipt_name, .sha256 = receipt_hex[0..] },
        }, .{}, writer);
        try writer.writeByte('\n');
        try writer.flush();
    }

    /// Emit the genuine route result together with a separately owned native
    /// teacher sibling. The report validates sibling payload identity and the
    /// complete per-reference-token conditional chain before serialization.
    fn writeQualityReport(
        self: *Session,
        command: Command,
        evidence: *const driver_mod.Evidence,
        reference_tokens: []const i64,
        ordered_source_sha256: ?[32]u8,
        ordered_part_sha256s: ?[]const [32]u8,
        writer: *std.Io.Writer,
    ) !void {
        const observation = evidence.observation;
        try expectHex(command.prompt_sha256.?, observation.rendered_prompt_sha256);
        try validateContextAllocationObservation(observation);
        const committed = observation.committed_context_bytes orelse return error.MissingTotalContextAccounting;
        const peak = observation.context_peak_bytes orelse return error.MissingTotalContextAccounting;
        if (!observation.route_completed or observation.reserved_output_tokens != command.reserved_output_tokens.? or
            observation.output_token_ids_len == 0 or
            observation.output_token_ids_len > command.reserved_output_tokens.?)
            return error.IncompleteContextQualificationTokens;
        const ids = observation.output_token_ids[0..observation.output_token_ids_len];
        var token_hash = std.crypto.hash.sha2.Sha256.init(.{});
        for (ids) |id| {
            if (id > std.math.maxInt(i32)) return error.InvalidQualityOutputToken;
            var bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &bytes, id, .little);
            token_hash.update(&bytes);
        }
        var output_digest: [32]u8 = undefined;
        token_hash.final(&output_digest);
        const teacher_forced = @import("backends/context_teacher_forced.zig");
        const generation_binding = observation.quality_generation_binding orelse
            return error.MissingTeacherForcedGenerationBinding;
        const receipt = evidence.teacher_forced_receipt orelse return error.MissingTeacherForcedReceipt;
        const teacher_observation = evidence.teacher_observation orelse return error.MissingTeacherForcedObservation;
        try teacher_forced.validateSiblingContexts(generation_binding, receipt.binding);
        try teacher_forced.validateReceipt(receipt, reference_tokens);
        if (!teacher_observation.route_completed or
            teacher_observation.published != (command.variant.? == .attention_matching) or
            teacher_observation.backend_kv_readback_before != teacher_observation.backend_kv_readback_after or
            teacher_observation.loaded_identity.load_generation != observation.loaded_identity.load_generation or
            teacher_observation.prompt_tokens != observation.prompt_tokens or
            !std.mem.eql(u8, &teacher_observation.rendered_prompt_sha256, &observation.rendered_prompt_sha256) or
            !std.mem.eql(u8, &teacher_observation.loaded_identity.artifact_sha256, &observation.loaded_identity.artifact_sha256) or
            !std.mem.eql(u8, &teacher_observation.loaded_identity.tokenizer_sha256, &observation.loaded_identity.tokenizer_sha256) or
            !std.mem.eql(u8, &teacher_observation.loaded_identity.template_sha256, &observation.loaded_identity.template_sha256) or
            !std.mem.eql(u8, &teacher_observation.loaded_identity.layout_sha256, &observation.loaded_identity.layout_sha256) or
            !std.mem.eql(u8, &teacher_observation.loaded_identity.profile_sha256, &observation.loaded_identity.profile_sha256) or
            !std.mem.eql(u8, &teacher_observation.loaded_identity.implementation_sha256, &observation.loaded_identity.implementation_sha256) or
            !teacher_observation.owner_released or teacher_observation.owner_post_release_live_bytes == null or
            teacher_observation.owner_measurement.released_baseline_bytes == null or
            teacher_observation.owner_post_release_live_bytes.? != teacher_observation.owner_measurement.released_baseline_bytes.?)
            return error.InvalidTeacherForcedObservation;
        const output_text = evidence.output_text orelse return error.MissingQualityOutputText;
        const reasoning_text = evidence.reasoning_text orelse return error.MissingQualityOutputText;
        if (output_text.len > 1024 * 1024 or reasoning_text.len > 1024 * 1024 or
            !std.unicode.utf8ValidateSlice(output_text) or !std.unicode.utf8ValidateSlice(reasoning_text))
            return error.InvalidQualityOutputText;
        const expected_published = command.variant.? == .attention_matching;
        if (observation.published != expected_published or
            observation.backend_kv_readback_after < observation.backend_kv_readback_before or
            observation.backend_kv_readback_after != observation.backend_kv_readback_before or
            evidence.elapsed_ns == 0)
            return error.IncompleteQualityExecutionEvidence;
        if (peak < @max(observation.committed_before_bytes, observation.committed_after_bytes) or
            (expected_published and observation.committed_after_bytes >= observation.committed_before_bytes) or
            (!expected_published and observation.committed_after_bytes != observation.committed_before_bytes))
            return error.IncompleteQualityExecutionEvidence;
        const baseline = observation.owner_measurement.released_baseline_bytes orelse
            return error.MissingPostReleaseLiveAccounting;
        if (observation.owner_post_release_live_bytes.? != baseline)
            return error.IncompleteQualityExecutionEvidence;
        if (std.mem.allEqual(u8, &self.provenance.binary_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.source_build_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.device_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.os_sha256, 0))
            return error.MissingContextServingProvenance;

        const output_hex = std.fmt.bytesToHex(output_digest, .lower);
        const reference_sequence_hex = std.fmt.bytesToHex(receipt.binding.reference_sequence_sha256, .lower);
        const output_text_hex = std.fmt.bytesToHex(digest(output_text), .lower);
        const reasoning_hex = std.fmt.bytesToHex(digest(reasoning_text), .lower);
        const artifact_hex = std.fmt.bytesToHex(observation.loaded_identity.artifact_sha256, .lower);
        const tokenizer_hex = std.fmt.bytesToHex(observation.loaded_identity.tokenizer_sha256, .lower);
        const template_hex = std.fmt.bytesToHex(observation.loaded_identity.template_sha256, .lower);
        const layout_hex = std.fmt.bytesToHex(observation.loaded_identity.layout_sha256, .lower);
        const profile_hex = std.fmt.bytesToHex(observation.loaded_identity.profile_sha256, .lower);
        const implementation_hex = std.fmt.bytesToHex(observation.loaded_identity.implementation_sha256, .lower);
        const binary_hex = std.fmt.bytesToHex(self.provenance.binary_sha256, .lower);
        const source_hex = std.fmt.bytesToHex(self.provenance.source_build_sha256, .lower);
        const device_hex = std.fmt.bytesToHex(self.provenance.device_sha256, .lower);
        const os_hex = std.fmt.bytesToHex(self.provenance.os_sha256, .lower);
        const settings_hex = std.fmt.bytesToHex(generationSettings(command.reserved_output_tokens.?), .lower);
        const variant_hex = std.fmt.bytesToHex(self.variantSettings(command.variant.?), .lower);
        var ordered_source_hex: ?[64]u8 = if (ordered_source_sha256) |value|
            std.fmt.bytesToHex(value, .lower)
        else
            null;
        var part_hex_storage: [64][64]u8 = undefined;
        var part_hex_slices: [64][]const u8 = undefined;
        const part_hex: ?[]const []const u8 = if (ordered_part_sha256s) |parts| blk: {
            for (parts, 0..) |part, index| {
                part_hex_storage[index] = std.fmt.bytesToHex(part, .lower);
                part_hex_slices[index] = &part_hex_storage[index];
            }
            break :blk part_hex_slices[0..parts.len];
        } else null;
        try std.json.Stringify.value(.{
            .schema_version = @as(u32, 2),
            .completed = true,
            .quality_generation_completed = true,
            .diagnostic_only = false,
            .semantic_identity = .{
                .artifact = artifact_hex[0..],
                .tokenizer = tokenizer_hex[0..],
                .template = template_hex[0..],
                .layout = layout_hex[0..],
                .profile = profile_hex[0..],
                .implementation = implementation_hex[0..],
                .dtype = "f16_kv_f32_bias",
                .kernel_route = "metal_context_optimized",
            },
            .provenance = .{
                .binary_sha256 = binary_hex[0..],
                .source_build_sha256 = source_hex[0..],
                .device_sha256 = device_hex[0..],
                .os_sha256 = os_hex[0..],
            },
            .loaded_identity = .{
                .load_generation = observation.loaded_identity.load_generation,
                .artifact_sha256 = artifact_hex[0..],
                .tokenizer_sha256 = tokenizer_hex[0..],
                .template_sha256 = template_hex[0..],
                .layout_sha256 = layout_hex[0..],
                .profile_sha256 = profile_hex[0..],
                .implementation_sha256 = implementation_hex[0..],
            },
            .session_id = &self.expected_session_id,
            .request_index = command.request_index.?,
            .case_id = command.case_id.?,
            .bucket = @tagName(command.bucket.?),
            .route = @tagName(command.route.?),
            .teacher_route = "native_internal",
            .variant = @tagName(command.variant.?),
            .document_sha256 = command.document_sha256,
            .question_sha256 = command.question_sha256,
            .messages_sha256 = command.messages_sha256,
            .source_sha256 = if (ordered_source_hex) |*value| value[0..] else command.document_sha256.?,
            .selected_part_sha256s = part_hex,
            .request_content_sha256 = command.request_content_sha256.?,
            .prompt_sha256 = command.prompt_sha256.?,
            .prompt_tokens = observation.prompt_tokens,
            .reserved_output_tokens = observation.reserved_output_tokens,
            .reference_sha256 = command.reference_sha256.?,
            .reference_tokens_sha256 = command.reference_tokens_sha256.?,
            .scorer = command.scorer.?,
            .settings_sha256 = settings_hex[0..],
            .variant_settings_sha256 = variant_hex[0..],
            .elapsed_ns = evidence.elapsed_ns,
            .committed_before_bytes = observation.committed_before_bytes,
            .committed_after_bytes = observation.committed_after_bytes,
            .committed_context_bytes = committed,
            .context_peak_bytes = peak,
            .backend_kv_readback_bytes = observation.backend_kv_readback_after - observation.backend_kv_readback_before,
            .live_bytes_before = baseline,
            .live_bytes_after_release = observation.owner_post_release_live_bytes.?,
            .context_allocation_scope = "request_exclusive_context",
            .context_allocation_scope_version = observation.context_allocation_scope_version,
            .context_allocation_scope_complete = observation.context_allocation_scope_complete,
            .full_owner_physical_available = observation.full_owner_physical_available,
            .model_execution_pool_admission_version = observation.model_execution_pool_admission_version,
            .model_execution_pool_admitted_backend_bytes = observation.model_execution_pool_admitted_backend_bytes,
            .model_execution_pool_shared_residency = true,
            .owner_released = observation.owner_released,
            .owner_post_release_live_bytes = observation.owner_post_release_live_bytes.?,
            .owner_measurement = observation.owner_measurement,
            .published = observation.published,
            .exact_fallback = false,
            .output_token_ids = ids,
            .output_sha256 = output_hex[0..],
            .output_utf8 = output_text,
            .output_utf8_sha256 = output_text_hex[0..],
            .reasoning_utf8 = reasoning_text,
            .reasoning_utf8_sha256 = reasoning_hex[0..],
            .reference_sequence_sha256 = reference_sequence_hex[0..],
            .continuation_forward_steps = receipt.continuation_forward_steps,
            .published_compaction_forward_steps = receipt.published_compaction_forward_steps,
            .teacher_forced_supported = true,
            .teacher_forced = receipt.rows,
            .teacher_context_instance_id = receipt.binding.context_instance_id,
            .generation_context_instance_id = generation_binding.context_instance_id,
            .teacher_diagnostic_kv_readback_bytes = receipt.binding.diagnostic_kv_readback_bytes,
        }, .{}, writer);
        try writer.writeByte('\n');
        try writer.flush();
    }

    // Insertion seam for the independently audited terminal loaded-provenance
    // command. It must not reuse quality evidence or caller-supplied digests.
    // describe_provenance: reserved for context_serving loaded authority.

    fn execute(self: *Session, command: Command, writer: *std.Io.Writer) !void {
        try self.validate(command);
        self.next_request_index = std.math.add(u32, self.next_request_index, 1) catch
            return error.ServingDriverRequestIndexOverflow;
        if (command.command == .describe_provenance)
            return self.executeProvenance(command, writer);
        if (command.command == .materialize_quality_v1)
            return self.executeQualityMaterialization(command, writer);
        if (command.route.? == .serial_mixed_batch)
            return self.executeMixedBatch(command, writer);
        const is_serving_request = command.command == .measure or command.command == .next or
            command.command == .fault or command.command == .quality;
        const is_ordered_quality = command.command == .quality and command.messages_file != null;
        const document = if (is_serving_request and !is_ordered_quality)
            try self.readCampaignFile(command.document_file.?)
        else
            try self.allocator.alloc(u8, 0);
        defer self.allocator.free(document);
        const question = if (is_serving_request and !is_ordered_quality)
            try self.readCampaignFile(command.question_file.?)
        else
            try self.allocator.alloc(u8, 0);
        defer self.allocator.free(question);
        const prompt = try self.allocator.alloc(u8, 0);
        defer self.allocator.free(prompt);
        var ordered_parsed: ?std.json.Parsed(ordered_messages.Artifact) = null;
        defer if (ordered_parsed) |*value| value.deinit();
        var ordered_digest: ?[32]u8 = null;
        var ordered_source_digest: ?[32]u8 = null;
        var ordered_part_storage: [64][32]u8 = undefined;
        var ordered_part_digests: ?[]const [32]u8 = null;
        if (is_ordered_quality) {
            const bytes = try self.readCampaignFile(command.messages_file.?);
            defer self.allocator.free(bytes);
            try expectHex(command.messages_sha256.?, digest(bytes));
            ordered_parsed = try ordered_messages.parseOwned(self.allocator, bytes);
            ordered_digest = try ordered_messages.semanticDigest(ordered_parsed.?.value);
            ordered_source_digest = try ordered_messages.selectedSourceDigest(ordered_parsed.?.value);
            ordered_part_digests = try ordered_messages.selectedPartDigests(
                ordered_parsed.?.value,
                &ordered_part_storage,
            );
            try expectHex(command.request_content_sha256.?, ordered_digest.?);
            try expectHex(command.source_sha256.?, ordered_source_digest.?);
            const expected_parts = command.selected_part_sha256s.?;
            if (expected_parts.len != ordered_part_digests.?.len) return error.ServingDriverDigestMismatch;
            for (expected_parts, ordered_part_digests.?) |expected, actual| try expectHex(expected, actual);
        } else if (is_serving_request) {
            try expectHex(command.document_sha256.?, digest(document));
            try expectHex(command.question_sha256.?, digest(question));
            try expectHex(command.request_content_sha256.?, coverage_trust.structuredRequestDigest(document, question));
        }
        var quality_reference_utf8: ?[]u8 = null;
        defer if (quality_reference_utf8) |value| self.allocator.free(value);
        var quality_reference_tokens: ?[]i64 = null;
        defer if (quality_reference_tokens) |tokens| self.allocator.free(tokens);
        if (command.command == .quality) {
            const sealed_prompt = try self.readCampaignFile(command.prompt_file.?);
            defer self.allocator.free(sealed_prompt);
            quality_reference_utf8 = try self.readCampaignFile(command.reference_file.?);
            const reference = quality_reference_utf8.?;
            const reference_tokens = try self.readCampaignFile(command.reference_tokens_file.?);
            defer self.allocator.free(reference_tokens);
            if (sealed_prompt.len == 0 or reference.len == 0 or !std.unicode.utf8ValidateSlice(sealed_prompt) or
                !std.unicode.utf8ValidateSlice(reference)) return error.InvalidQualityInput;
            try expectHex(command.prompt_sha256.?, digest(sealed_prompt));
            try expectHex(command.reference_sha256.?, digest(reference));
            try expectHex(command.reference_tokens_sha256.?, digest(reference_tokens));
            quality_reference_tokens = try self.parseReferenceTokens(reference_tokens);
            if (quality_reference_tokens.?.len > command.reserved_output_tokens.?)
                return error.InvalidQualityReferenceTokens;
        }
        if (command.settings_sha256) |expected|
            try expectHex(expected, generationSettings(command.reserved_output_tokens.?));
        if (command.variant_settings_sha256) |expected|
            try expectHex(expected, self.variantSettings(command.variant.?));
        if (command.command == .fault)
            return self.executeFault(command, document, question, writer);
        const selected_coverage: ?coverage_trust.Selected = if (command.command == .coverage)
            try (self.coverage orelse return error.UnsupportedCoverageQualification).select(.{
                .case_id = command.case_id.?,
                .document_sha256 = command.document_sha256.?,
                .question_sha256 = command.question_sha256.?,
                .request_content_sha256 = command.request_content_sha256.?,
                .bucket = command.bucket.?,
                .binary_receipt_id = command.binary_receipt_id.?,
                .source_receipt_id = command.source_receipt_id.?,
            })
        else
            null;
        defer if (selected_coverage) |selected| {
            self.allocator.free(selected.document_bytes);
            self.allocator.free(selected.question_bytes);
        };
        const session_id = try parseHex32(&self.expected_session_id);
        const driver_request = driver_mod.Request{
            .route = command.route.?,
            .variant = command.variant.?,
            .model = self.model,
            .prompt = prompt,
            .structured = if (is_serving_request)
                if (is_ordered_quality) null else .{ .document = document, .question = question }
            else
                null,
            .ordered = if (ordered_parsed) |parsed| .{
                .artifact = parsed.value,
                .semantic_sha256 = ordered_digest.?,
            } else null,
            .max_context_tokens = self.max_context_tokens,
            .max_kv_tokens = self.max_kv_tokens,
            .keep_prefix_tokens = self.keep_prefix_tokens,
            .keep_recent_tokens = self.keep_recent_tokens,
            .reserved_output_tokens = command.reserved_output_tokens.?,
            .optimize_decode = self.optimize_decode,
            .optimize_prefill = self.optimize_prefill,
            .fit_engine = self.fit_engine,
            .admitted_prefill_chunk_rows = self.admitted_prefill_chunk_rows,
            .capture_quality_output = command.command == .quality,
            .quality = if (quality_reference_tokens) |tokens| .{
                .reference_utf8 = quality_reference_utf8.?,
                .reference_tokens = tokens,
                .reference_sequence_sha256 = try @import("backends/context_teacher_forced.zig").referenceSequenceDigest(tokens),
                .rendered_prompt_sha256 = try parseHex32(command.prompt_sha256.?),
                .profile_artifact_sha256 = self.profile_artifact_sha256,
                .profile_semantic_sha256 = self.profile_semantic_sha256,
                .implementation_sha256 = self.implementation_sha256,
            } else null,
            .coverage = if (selected_coverage) |selected| .{
                .request = .{
                    .session_id = session_id,
                    .request_index = command.request_index.?,
                    .question_sha256 = selected.question_sha256,
                    .request_content_sha256 = selected.request_content_sha256,
                },
                .document = selected.document,
                .document_text = selected.document_bytes,
                .question_text = selected.question_bytes,
                .provenance = selected.provenance,
            } else null,
        };
        var evidence = if (command.route.? == .cli)
            try self.driver.executeCli(driver_request, self.cli_runner.?, .{
                .model = self.model,
                .document_file = command.document_file.?,
                .document_sha256 = command.document_sha256.?,
                .question_file = command.question_file.?,
                .question_sha256 = command.question_sha256.?,
                .max_context_tokens = self.max_context_tokens,
                .max_kv_tokens = self.max_kv_tokens,
                .keep_prefix_tokens = self.keep_prefix_tokens,
                .keep_recent_tokens = self.keep_recent_tokens,
                .reserved_output_tokens = command.reserved_output_tokens.?,
                // Streaming is mandatory because it exposes a genuine first
                // visible-token boundary. A non-streaming CLI cannot invent TTFT.
                .stream = true,
            })
        else
            try self.driver.execute(driver_request);
        defer evidence.deinit();
        const observation = evidence.observation;
        if (!std.mem.eql(u8, &observation.loaded_identity.implementation_sha256, &self.implementation_sha256) or
            !std.mem.eql(u8, &observation.loaded_identity.profile_sha256, &self.profile_artifact_sha256) or
            !std.mem.eql(u8, &observation.profile_semantic_digest, &self.profile_semantic_sha256))
            return error.ContextQualificationLoadedIdentityMismatch;
        if (command.prompt_tokens) |expected|
            if (observation.prompt_tokens != expected) return error.ContextQualificationPromptTokenMismatch;
        if (command.command == .coverage) {
            const captured = if (evidence.coverage) |*value| value else return error.MissingContextCoverageEvidence;
            if (captured.prompt_tokens_i64_le.len % 8 != 0 or
                captured.prompt_token_offsets_u32_le.len != captured.prompt_tokens_i64_le.len or
                captured.question_tokens_i64_le.len % 8 != 0 or
                observation.prompt_tokens != captured.prompt_tokens_i64_le.len / 8 or
                observation.loaded_identity.load_generation != captured.load_generation or
                !std.mem.eql(u8, &observation.loaded_identity.artifact_sha256, &captured.identity.artifact) or
                !std.mem.eql(u8, &observation.loaded_identity.tokenizer_sha256, &captured.identity.tokenizer) or
                !std.mem.eql(u8, &observation.loaded_identity.template_sha256, &captured.identity.template) or
                !std.mem.eql(u8, &observation.loaded_identity.layout_sha256, &captured.identity.layout) or
                !std.mem.eql(u8, &observation.loaded_identity.implementation_sha256, &captured.identity.runtime))
                return error.ContextQualificationLoadedIdentityMismatch;
            var receipt_name_buffer: [64]u8 = undefined;
            const receipt_name = try std.fmt.bufPrint(&receipt_name_buffer, "coverage-receipt-{d:0>6}.json", .{command.request_index.?});
            var prompt_name_buffer: [64]u8 = undefined;
            const prompt_name = try std.fmt.bufPrint(&prompt_name_buffer, "coverage-prompt-{d:0>6}.i64", .{command.request_index.?});
            var rendered_prompt_name_buffer: [64]u8 = undefined;
            const rendered_prompt_name = try std.fmt.bufPrint(&rendered_prompt_name_buffer, "coverage-rendered-prompt-{d:0>6}.txt", .{command.request_index.?});
            var offsets_name_buffer: [64]u8 = undefined;
            const offsets_name = try std.fmt.bufPrint(&offsets_name_buffer, "coverage-prompt-offsets-{d:0>6}.u32le", .{command.request_index.?});
            var question_name_buffer: [64]u8 = undefined;
            const question_name = try std.fmt.bufPrint(&question_name_buffer, "coverage-question-{d:0>6}.i64", .{command.request_index.?});
            var fit_name_buffer: [64]u8 = undefined;
            const fit_name = try std.fmt.bufPrint(&fit_name_buffer, "coverage-fit-{d:0>6}.i64", .{command.request_index.?});
            var gate_name_buffer: [64]u8 = undefined;
            const gate_name = try std.fmt.bufPrint(&gate_name_buffer, "coverage-gate-{d:0>6}.i64", .{command.request_index.?});
            var fit_template_name_buffer: [64]u8 = undefined;
            const fit_template_name = try std.fmt.bufPrint(&fit_template_name_buffer, "coverage-fit-template-{d:0>6}.txt", .{command.request_index.?});
            var gate_template_name_buffer: [64]u8 = undefined;
            const gate_template_name = try std.fmt.bufPrint(&gate_template_name_buffer, "coverage-gate-template-{d:0>6}.txt", .{command.request_index.?});
            var contract_name_buffer: [64]u8 = undefined;
            const contract_name = try std.fmt.bufPrint(&contract_name_buffer, "coverage-generation-contract-{d:0>6}.json", .{command.request_index.?});
            var inputs_name_buffer: [64]u8 = undefined;
            const inputs_name = try std.fmt.bufPrint(&inputs_name_buffer, "capture-inputs-{d:0>6}.json", .{command.request_index.?});

            const receipt_sha256 = try self.writeCampaignEvidenceFile(receipt_name, captured.receipt_json);
            const prompt_sha256 = try self.writeCampaignEvidenceFile(prompt_name, captured.prompt_tokens_i64_le);
            const rendered_prompt_sha256 = try self.writeCampaignEvidenceFile(rendered_prompt_name, captured.rendered_prompt);
            const offsets_sha256 = try self.writeCampaignEvidenceFile(offsets_name, captured.prompt_token_offsets_u32_le);
            const question_sha256 = try self.writeCampaignEvidenceFile(question_name, captured.question_tokens_i64_le);
            const fit_sha256 = try self.writeCampaignEvidenceFile(fit_name, captured.fit_study.tokens_i64_le);
            const gate_sha256 = try self.writeCampaignEvidenceFile(gate_name, captured.gate_study.tokens_i64_le);
            const fit_template_sha256 = try self.writeCampaignEvidenceFile(fit_template_name, captured.fit_study.template_bytes);
            const gate_template_sha256 = try self.writeCampaignEvidenceFile(gate_template_name, captured.gate_study.template_bytes);
            if (!std.mem.eql(u8, captured.fit_study.generation_contract_bytes, captured.gate_study.generation_contract_bytes))
                return error.InvalidCoverageStudy;
            const contract_sha256 = try self.writeCampaignEvidenceFile(contract_name, captured.fit_study.generation_contract_bytes);
            if (!std.mem.eql(u8, &fit_template_sha256, &captured.fit_study.template_sha256) or
                !std.mem.eql(u8, &gate_template_sha256, &captured.gate_study.template_sha256) or
                !std.mem.eql(u8, &contract_sha256, &captured.fit_study.generation_contract_sha256) or
                !std.mem.eql(u8, &contract_sha256, &captured.gate_study.generation_contract_sha256))
                return error.InvalidCoverageStudy;

            const receipt_hex = std.fmt.bytesToHex(receipt_sha256, .lower);
            const prompt_hex = std.fmt.bytesToHex(prompt_sha256, .lower);
            const rendered_prompt_hex = std.fmt.bytesToHex(rendered_prompt_sha256, .lower);
            const offsets_hex = std.fmt.bytesToHex(offsets_sha256, .lower);
            const question_hex = std.fmt.bytesToHex(question_sha256, .lower);
            const fit_hex = std.fmt.bytesToHex(fit_sha256, .lower);
            const gate_hex = std.fmt.bytesToHex(gate_sha256, .lower);
            const fit_template_hex = std.fmt.bytesToHex(fit_template_sha256, .lower);
            const gate_template_hex = std.fmt.bytesToHex(gate_template_sha256, .lower);
            const contract_hex = std.fmt.bytesToHex(contract_sha256, .lower);
            const prompt_binding_hex = std.fmt.bytesToHex(captured.fit_study.prompt_tokens_sha256, .lower);
            if (!std.mem.eql(u8, &captured.fit_study.prompt_tokens_sha256, &captured.gate_study.prompt_tokens_sha256))
                return error.InvalidCoverageStudy;
            const fit_suffix_hex = std.fmt.bytesToHex(captured.fit_study.rendered_suffix_sha256, .lower);
            const gate_suffix_hex = std.fmt.bytesToHex(captured.gate_study.rendered_suffix_sha256, .lower);
            const session_hex = std.fmt.bytesToHex(captured.request.session_id, .lower);
            const question_binding_hex = std.fmt.bytesToHex(captured.request.question_sha256, .lower);
            const request_content_hex = std.fmt.bytesToHex(captured.request.request_content_sha256, .lower);
            const artifact_hex = std.fmt.bytesToHex(captured.identity.artifact, .lower);
            const tokenizer_hex = std.fmt.bytesToHex(captured.identity.tokenizer, .lower);
            const template_hex = std.fmt.bytesToHex(captured.identity.template, .lower);
            const layout_hex = std.fmt.bytesToHex(captured.identity.layout, .lower);
            const implementation_hex = std.fmt.bytesToHex(captured.identity.runtime, .lower);
            const binary_hex = std.fmt.bytesToHex(captured.provenance.binary_sha256, .lower);
            const source_hex = std.fmt.bytesToHex(captured.provenance.source_build_sha256, .lower);
            const diagnostic_only = command.prompt_tokens == null;
            const capture_inputs = try std.json.Stringify.valueAlloc(self.allocator, .{
                .schema_version = @as(u32, 1),
                .stage = "context_numerical_capture_inputs",
                .request = .{
                    .session_id = session_hex[0..],
                    .request_index = captured.request.request_index,
                    .question_sha256 = question_binding_hex[0..],
                    .request_content_sha256 = request_content_hex[0..],
                },
                .coverage_receipt = .{ .path = receipt_name, .sha256 = receipt_hex[0..] },
                .prompt = .{ .path = prompt_name, .sha256 = prompt_hex[0..], .count = captured.prompt_tokens_i64_le.len / 8 },
                .prompt_token_offsets = .{ .path = offsets_name, .sha256 = offsets_hex[0..], .count = captured.prompt_token_offsets_u32_le.len / 8 },
                .rendered_prompt = .{ .path = rendered_prompt_name, .sha256 = rendered_prompt_hex[0..] },
                .question = .{
                    .tokens = .{ .path = question_name, .sha256 = question_hex[0..], .count = captured.question_tokens_i64_le.len / 8 },
                    .source_start = captured.question_source_start,
                    .source_end = captured.question_source_end,
                },
                .fit = .{
                    .tokens = .{ .path = fit_name, .sha256 = fit_hex[0..], .count = captured.fit_study.token_count },
                    .prompt_tokens_sha256 = prompt_binding_hex[0..],
                    .rendered_suffix_sha256 = fit_suffix_hex[0..],
                    .template = .{ .path = fit_template_name, .sha256 = fit_template_hex[0..] },
                    .generation_contract = .{ .path = contract_name, .sha256 = contract_hex[0..] },
                    .response_tokens = captured.fit_study.response_tokens,
                    .generation_mode = @tagName(captured.fit_study.generation_mode),
                },
                .gate = .{
                    .tokens = .{ .path = gate_name, .sha256 = gate_hex[0..], .count = captured.gate_study.token_count },
                    .prompt_tokens_sha256 = prompt_binding_hex[0..],
                    .rendered_suffix_sha256 = gate_suffix_hex[0..],
                    .template = .{ .path = gate_template_name, .sha256 = gate_template_hex[0..] },
                    .generation_contract = .{ .path = contract_name, .sha256 = contract_hex[0..] },
                    .response_tokens = captured.gate_study.response_tokens,
                    .generation_mode = @tagName(captured.gate_study.generation_mode),
                },
                .loaded_identity = .{
                    .load_generation = captured.load_generation,
                    .artifact_sha256 = artifact_hex[0..],
                    .tokenizer_sha256 = tokenizer_hex[0..],
                    .template_sha256 = template_hex[0..],
                    .layout_sha256 = layout_hex[0..],
                    .implementation_sha256 = implementation_hex[0..],
                },
                .provenance = .{ .binary_sha256 = binary_hex[0..], .source_build_sha256 = source_hex[0..] },
                .admitted_prefill_chunk_rows = self.admitted_prefill_chunk_rows,
                .completed = true,
                .diagnostic_only = diagnostic_only,
            }, .{});
            defer self.allocator.free(capture_inputs);
            const inputs_sha256 = try self.writeCampaignEvidenceFile(inputs_name, capture_inputs);
            const inputs_hex = std.fmt.bytesToHex(inputs_sha256, .lower);
            try std.json.Stringify.value(.{
                .schema_version = @as(u32, 2),
                .completed = true,
                .operation = "coverage",
                .session_id = &self.expected_session_id,
                .request_index = command.request_index.?,
                .diagnostic_only = command.prompt_tokens == null,
                .observed_prompt_tokens = observation.prompt_tokens,
                .admitted_prefill_chunk_rows = self.admitted_prefill_chunk_rows,
                .capture_inputs = .{ .path = inputs_name, .sha256 = inputs_hex[0..] },
                .receipt = .{ .path = receipt_name, .sha256 = receipt_hex[0..] },
                .prompt_tokens = .{ .path = prompt_name, .sha256 = prompt_hex[0..] },
                .prompt_token_offsets = .{ .path = offsets_name, .sha256 = offsets_hex[0..] },
                .rendered_prompt = .{ .path = rendered_prompt_name, .sha256 = rendered_prompt_hex[0..] },
                .question_tokens = .{ .path = question_name, .sha256 = question_hex[0..] },
            }, .{}, writer);
            try writer.writeByte('\n');
            try writer.flush();
            return;
        }
        if (command.command == .quality)
            return self.writeQualityReport(
                command,
                &evidence,
                quality_reference_tokens.?,
                ordered_source_digest,
                ordered_part_digests,
                writer,
            );
        try validateContextAllocationObservation(observation);
        const committed = observation.committed_context_bytes orelse return error.MissingTotalContextAccounting;
        const peak = observation.context_peak_bytes orelse return error.MissingTotalContextAccounting;
        if (observation.output_token_ids_len != command.reserved_output_tokens.?) return error.IncompleteContextQualificationTokens;
        const ids = observation.output_token_ids[0..observation.output_token_ids_len];
        var token_hash = std.crypto.hash.sha2.Sha256.init(.{});
        for (ids) |id| {
            var bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &bytes, id, .little);
            token_hash.update(&bytes);
        }
        var output_digest: [32]u8 = undefined;
        token_hash.final(&output_digest);
        const output_hex = std.fmt.bytesToHex(output_digest, .lower);
        try expectHex(command.prompt_sha256.?, observation.rendered_prompt_sha256);
        if (std.mem.allEqual(u8, &self.provenance.binary_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.source_build_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.device_sha256, 0) or
            std.mem.allEqual(u8, &self.provenance.os_sha256, 0))
            return error.MissingContextServingProvenance;
        if (evidence.elapsed_ns == 0 or evidence.ttft_ns == 0 or evidence.decode_ns == 0 or
            evidence.ttft_ns > evidence.elapsed_ns or evidence.decode_ns > evidence.elapsed_ns or
            observation.backend_kv_readback_after < observation.backend_kv_readback_before)
            return error.IncompleteContextServingTiming;
        const artifact_hex = std.fmt.bytesToHex(observation.loaded_identity.artifact_sha256, .lower);
        const tokenizer_hex = std.fmt.bytesToHex(observation.loaded_identity.tokenizer_sha256, .lower);
        const template_hex = std.fmt.bytesToHex(observation.loaded_identity.template_sha256, .lower);
        const layout_hex = std.fmt.bytesToHex(observation.loaded_identity.layout_sha256, .lower);
        const profile_hex = std.fmt.bytesToHex(observation.loaded_identity.profile_sha256, .lower);
        const implementation_hex = std.fmt.bytesToHex(observation.loaded_identity.implementation_sha256, .lower);
        const binary_hex = std.fmt.bytesToHex(self.provenance.binary_sha256, .lower);
        const source_hex = std.fmt.bytesToHex(self.provenance.source_build_sha256, .lower);
        const device_hex = std.fmt.bytesToHex(self.provenance.device_sha256, .lower);
        const os_hex = std.fmt.bytesToHex(self.provenance.os_sha256, .lower);
        const settings_hex = std.fmt.bytesToHex(generationSettings(command.reserved_output_tokens.?), .lower);
        const variant_hex = std.fmt.bytesToHex(self.variantSettings(command.variant.?), .lower);
        const baseline = observation.owner_measurement.released_baseline_bytes orelse
            return error.MissingPostReleaseLiveAccounting;
        var cli_argv_ref: ?struct { path: []const u8, sha256: [64]u8 } = null;
        var cli_stdout_ref: ?struct { path: []const u8, sha256: [64]u8 } = null;
        var cli_stderr_ref: ?struct { path: []const u8, sha256: [64]u8 } = null;
        var cli_argv_name_buffer: [64]u8 = undefined;
        var cli_stdout_name_buffer: [64]u8 = undefined;
        var cli_stderr_name_buffer: [64]u8 = undefined;
        if (evidence.cli_process) |receipt| {
            const argv_name = try std.fmt.bufPrint(&cli_argv_name_buffer, "cli-argv-{d:0>6}.bin", .{command.request_index.?});
            const stdout_name = try std.fmt.bufPrint(&cli_stdout_name_buffer, "cli-stdout-{d:0>6}.log", .{command.request_index.?});
            const stderr_name = try std.fmt.bufPrint(&cli_stderr_name_buffer, "cli-stderr-{d:0>6}.log", .{command.request_index.?});
            const argv_digest = try self.writeCampaignLogFile(argv_name, receipt.argv_preimage);
            const stdout_digest = try self.writeCampaignLogFile(stdout_name, receipt.stdout);
            const stderr_digest = try self.writeCampaignLogFile(stderr_name, receipt.stderr);
            if (!std.mem.eql(u8, &argv_digest, &receipt.argv_sha256) or
                !std.mem.eql(u8, &stdout_digest, &receipt.stdout_sha256) or
                !std.mem.eql(u8, &stderr_digest, &receipt.stderr_sha256))
                return error.CliQualificationLogDigestMismatch;
            cli_argv_ref = .{ .path = argv_name, .sha256 = std.fmt.bytesToHex(argv_digest, .lower) };
            cli_stdout_ref = .{ .path = stdout_name, .sha256 = std.fmt.bytesToHex(stdout_digest, .lower) };
            cli_stderr_ref = .{ .path = stderr_name, .sha256 = std.fmt.bytesToHex(stderr_digest, .lower) };
        }
        try std.json.Stringify.value(.{
            .schema_version = @as(u32, 2),
            .completed = true,
            .semantic_identity = .{
                .artifact = artifact_hex[0..],
                .tokenizer = tokenizer_hex[0..],
                .template = template_hex[0..],
                .layout = layout_hex[0..],
                .profile = profile_hex[0..],
                .implementation = implementation_hex[0..],
                .dtype = "f16_kv_f32_bias",
                .kernel_route = "metal_context_optimized",
            },
            .provenance = .{
                .binary_sha256 = binary_hex[0..],
                .source_build_sha256 = source_hex[0..],
                .device_sha256 = device_hex[0..],
                .os_sha256 = os_hex[0..],
            },
            .loaded_identity = .{
                .load_generation = observation.loaded_identity.load_generation,
                .artifact_sha256 = artifact_hex[0..],
                .tokenizer_sha256 = tokenizer_hex[0..],
                .template_sha256 = template_hex[0..],
                .layout_sha256 = layout_hex[0..],
                .profile_sha256 = profile_hex[0..],
                .implementation_sha256 = implementation_hex[0..],
            },
            .session_id = &self.expected_session_id,
            .request_index = command.request_index.?,
            .pair_id = command.pair_id,
            .warmup = command.warmup,
            .route = @tagName(command.route.?),
            .cli_process = if (evidence.cli_process) |receipt| .{
                .separate_process = true,
                .binary_sha256 = std.fmt.bytesToHex(receipt.binary_sha256, .lower),
                .argv_sha256 = std.fmt.bytesToHex(receipt.argv_sha256, .lower),
                .argv = cli_argv_ref.?,
                .request_sha256 = std.fmt.bytesToHex(receipt.request_sha256, .lower),
                .exit_code = receipt.exit_code,
                .elapsed_ns = receipt.elapsed_ns,
                .public_output_sha256 = std.fmt.bytesToHex(observation.public_output_sha256, .lower),
                .public_output_bytes = observation.public_output_bytes,
                .public_output_match = observation.public_output_bound,
                .stdout = cli_stdout_ref.?,
                .stderr = cli_stderr_ref.?,
            } else null,
            .variant = @tagName(command.variant.?),
            .document_file = command.document_file.?,
            .document_sha256 = command.document_sha256.?,
            .question_file = command.question_file.?,
            .question_sha256 = command.question_sha256.?,
            .request_content_sha256 = command.request_content_sha256.?,
            .prompt_sha256 = command.prompt_sha256.?,
            .prompt_tokens = observation.prompt_tokens,
            .reserved_output_tokens = observation.reserved_output_tokens,
            .settings_sha256 = settings_hex[0..],
            .variant_settings_sha256 = variant_hex[0..],
            .ttft_ns = evidence.ttft_ns,
            .decode_ns = evidence.decode_ns,
            .elapsed_ns = evidence.elapsed_ns,
            .committed_before_bytes = observation.committed_before_bytes,
            .committed_after_bytes = observation.committed_after_bytes,
            .committed_context_bytes = committed,
            .context_peak_bytes = peak,
            .backend_kv_readback_bytes = observation.backend_kv_readback_after - observation.backend_kv_readback_before,
            .live_bytes_before = baseline,
            .live_bytes_after_release = observation.owner_post_release_live_bytes.?,
            .context_allocation_scope = "request_exclusive_context",
            .context_allocation_scope_version = observation.context_allocation_scope_version,
            .context_allocation_scope_complete = observation.context_allocation_scope_complete,
            .full_owner_physical_available = observation.full_owner_physical_available,
            .model_execution_pool_admission_version = observation.model_execution_pool_admission_version,
            .model_execution_pool_admitted_backend_bytes = observation.model_execution_pool_admitted_backend_bytes,
            .model_execution_pool_shared_residency = true,
            .owner_released = observation.owner_released,
            .owner_post_release_live_bytes = observation.owner_post_release_live_bytes.?,
            .owner_measurement = observation.owner_measurement,
            .published = observation.published,
            .exact_fallback = false,
            .output_token_ids = ids,
            .output_sha256 = output_hex[0..],
        }, .{}, writer);
        try writer.writeByte('\n');
        try writer.flush();
    }

    pub fn run(self: *Session) !void {
        // Seal the internal engine to the canonical implementation identity
        // before accepting any command. No request can repair a mismatch.
        try self.validateFitEngineIdentity();
        var stdin_buffer: [max_command_bytes]u8 = undefined;
        var stdin = std.Io.File.stdin().readerStreaming(self.io, &stdin_buffer);
        var stdout_buffer: [64 * 1024]u8 = undefined;
        var stdout = std.Io.File.stdout().writer(self.io, &stdout_buffer);
        while (try stdin.interface.takeDelimiter('\n')) |line| {
            if (line.len == 0) continue;
            var parsed = std.json.parseFromSlice(Command, self.allocator, line, .{ .ignore_unknown_fields = false }) catch |err| {
                try self.writeError(&stdout.interface, .{ .command = .stop }, err);
                continue;
            };
            defer parsed.deinit();
            if (parsed.value.command == .stop) {
                self.validate(parsed.value) catch |err| {
                    try self.writeError(&stdout.interface, parsed.value, err);
                    continue;
                };
                return;
            }
            self.execute(parsed.value, &stdout.interface) catch |err| try self.writeError(&stdout.interface, parsed.value, err);
        }
    }
};

test "context soak command requires sealed structured attention matching input" {
    var session = Session{
        .allocator = std.testing.allocator,
        .io = std.testing.io,
        .driver = undefined,
        .campaign_dir = undefined,
        .model = "fixture",
        .expected_session_id = @splat('a'),
        .max_context_tokens = 1024,
        .max_kv_tokens = 512,
        .keep_prefix_tokens = 64,
        .keep_recent_tokens = 64,
        .optimize_decode = true,
        .optimize_prefill = true,
        .implementation_sha256 = @splat(1),
        .profile_artifact_sha256 = @splat(2),
        .profile_semantic_sha256 = @splat(3),
    };
    const complete = Command{
        .command = .next,
        .session_id = &session.expected_session_id,
        .request_index = 0,
        .route = .native,
        .variant = .attention_matching,
        .document_file = "document.txt",
        .document_sha256 = "1111111111111111111111111111111111111111111111111111111111111111",
        .question_file = "question.txt",
        .question_sha256 = "2222222222222222222222222222222222222222222222222222222222222222",
        .request_content_sha256 = "3333333333333333333333333333333333333333333333333333333333333333",
        .prompt_sha256 = "4444444444444444444444444444444444444444444444444444444444444444",
        .prompt_tokens = 960,
        .reserved_output_tokens = 32,
        .settings_sha256 = "5555555555555555555555555555555555555555555555555555555555555555",
        .variant_settings_sha256 = "6666666666666666666666666666666666666666666666666666666666666666",
    };
    try session.validate(complete);
    var exact = complete;
    exact.variant = .exact;
    try std.testing.expectError(error.UnsupportedSoakRoute, session.validate(exact));
    var missing = complete;
    missing.question_file = null;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(missing));
    const siblings = [_]MixedSiblingCommand{
        .{
            .variant = .exact,
            .document_file = "exact-document.txt",
            .document_sha256 = "1111111111111111111111111111111111111111111111111111111111111111",
            .question_file = "exact-question.txt",
            .question_sha256 = "2222222222222222222222222222222222222222222222222222222222222222",
            .request_content_sha256 = "3333333333333333333333333333333333333333333333333333333333333333",
            .prompt_sha256 = "4444444444444444444444444444444444444444444444444444444444444444",
            .prompt_tokens = 960,
            .reserved_output_tokens = 32,
            .settings_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            .variant_settings_sha256 = "5555555555555555555555555555555555555555555555555555555555555555",
        },
        .{
            .variant = .attention_matching,
            .document_file = "am-document.txt",
            .document_sha256 = "6666666666666666666666666666666666666666666666666666666666666666",
            .question_file = "am-question.txt",
            .question_sha256 = "7777777777777777777777777777777777777777777777777777777777777777",
            .request_content_sha256 = "8888888888888888888888888888888888888888888888888888888888888888",
            .prompt_sha256 = "9999999999999999999999999999999999999999999999999999999999999999",
            .prompt_tokens = 960,
            .reserved_output_tokens = 32,
            .settings_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            .variant_settings_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
    };
    var mixed = Command{
        .command = .next,
        .session_id = &session.expected_session_id,
        .request_index = 0,
        .route = .serial_mixed_batch,
        .settings_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        .siblings = &siblings,
    };
    try session.validate(mixed);
    mixed.siblings = siblings[0..1];
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(mixed));
}

test "context fault command exposes only implemented transactional checkpoints" {
    var session = Session{
        .allocator = std.testing.allocator,
        .io = std.testing.io,
        .driver = undefined,
        .campaign_dir = undefined,
        .model = "fixture",
        .expected_session_id = @splat('a'),
        .max_context_tokens = 1024,
        .max_kv_tokens = 512,
        .keep_prefix_tokens = 64,
        .keep_recent_tokens = 64,
        .optimize_decode = true,
        .optimize_prefill = true,
        .implementation_sha256 = @splat(1),
        .profile_artifact_sha256 = @splat(2),
        .profile_semantic_sha256 = @splat(3),
    };
    var command = Command{
        .command = .fault,
        .session_id = &session.expected_session_id,
        .request_index = 0,
        .route = .native,
        .variant = .attention_matching,
        .fault = "cancel_replay",
        .document_file = "document.txt",
        .document_sha256 = "1111111111111111111111111111111111111111111111111111111111111111",
        .question_file = "question.txt",
        .question_sha256 = "2222222222222222222222222222222222222222222222222222222222222222",
        .request_content_sha256 = "3333333333333333333333333333333333333333333333333333333333333333",
        .prompt_sha256 = "4444444444444444444444444444444444444444444444444444444444444444",
        .prompt_tokens = 960,
        .reserved_output_tokens = 32,
        .settings_sha256 = "5555555555555555555555555555555555555555555555555555555555555555",
        .variant_settings_sha256 = "6666666666666666666666666666666666666666666666666666666666666666",
    };
    inline for (.{ "cancel_prefill", "cancel_replay", "cancel_fit", "cancel_publish", "oom", "nonfinite", "gpu_failure", "reload" }) |fault| {
        command.fault = fault;
        try session.validate(command);
    }
    command.fault = "cancel_fit";
    command.route = .http;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(command));
}

test "context coverage bootstrap rejects serving commands without a candidate" {
    var session = Session{
        .allocator = std.testing.allocator,
        .io = std.testing.io,
        .driver = undefined,
        .campaign_dir = undefined,
        .model = "fixture",
        .expected_session_id = @splat('a'),
        .max_context_tokens = 1024,
        .max_kv_tokens = 512,
        .keep_prefix_tokens = 64,
        .keep_recent_tokens = 64,
        .optimize_decode = true,
        .optimize_prefill = true,
        .implementation_sha256 = @splat(1),
        .profile_artifact_sha256 = @splat(0),
        .profile_semantic_sha256 = @splat(0),
        .coverage_only = true,
    };
    try std.testing.expectError(
        error.CoverageBootstrapCommandRequired,
        session.validate(.{ .command = .measure }),
    );
    try session.validate(.{ .command = .stop });
}

test "context serving allocation claim requires versioned scope and pool disclosure" {
    const server = @import("server/server.zig");
    var observation = server.ContextServingObservation{};
    try std.testing.expectError(error.IncompleteContextAllocationScope, validateContextAllocationObservation(observation));
    observation.full_owner_physical_available = true;
    observation.context_allocation_scope_version = host_allocation_tracker.context_allocation_scope_schema_version;
    observation.context_allocation_scope_complete = true;
    observation.model_execution_pool_admission_version = context_execution_pool.admission_version;
    observation.model_execution_pool_admitted_backend_bytes = context_execution_pool.admitted_backend_bytes;
    observation.owner_released = true;
    observation.owner_post_release_live_bytes = 0;
    observation.owner_measurement = .{ .available = true, .released_baseline_bytes = 0 };
    try validateContextAllocationObservation(observation);

    inline for (.{ "scope", "pool_version", "pool_bytes", "release" }) |missing| {
        var invalid = observation;
        if (std.mem.eql(u8, missing, "scope")) invalid.context_allocation_scope_complete = false;
        if (std.mem.eql(u8, missing, "pool_version")) invalid.model_execution_pool_admission_version = 0;
        if (std.mem.eql(u8, missing, "pool_bytes")) invalid.model_execution_pool_admitted_backend_bytes = 0;
        if (std.mem.eql(u8, missing, "release")) invalid.owner_post_release_live_bytes = null;
        try std.testing.expectError(error.IncompleteContextAllocationScope, validateContextAllocationObservation(invalid));
    }
}

test "context variant settings bind every policy and runtime identity field" {
    var session = Session{
        .allocator = std.testing.allocator,
        .io = std.testing.io,
        .driver = undefined,
        .campaign_dir = undefined,
        .model = "fixture",
        .expected_session_id = @splat('a'),
        .max_context_tokens = 1040,
        .max_kv_tokens = 1024,
        .keep_prefix_tokens = 0,
        .keep_recent_tokens = 1,
        .optimize_decode = true,
        .optimize_prefill = true,
        .implementation_sha256 = @splat(0x11),
        .profile_artifact_sha256 = @splat(0),
        .profile_semantic_sha256 = @splat(0),
    };
    const expected = try Session.parseHex32("4ae5c9c7661822e0323f8800ee2758a66c27515dec29906b980f6d566150a27e");
    const actual = session.variantSettings(.packed_exact);
    try std.testing.expectEqualSlices(u8, &expected, &actual);

    const baseline = session.variantSettings(.packed_exact);
    inline for (.{
        "max_context_tokens",
        "max_kv_tokens",
        "keep_prefix_tokens",
        "keep_recent_tokens",
    }) |field| {
        var changed = session;
        @field(changed, field) +%= 1;
        try std.testing.expect(!std.meta.eql(baseline, changed.variantSettings(.packed_exact)));
    }
    session.optimize_decode = false;
    try std.testing.expect(!std.meta.eql(baseline, session.variantSettings(.packed_exact)));
    session.optimize_decode = true;
    session.optimize_prefill = false;
    try std.testing.expect(!std.meta.eql(baseline, session.variantSettings(.packed_exact)));
    session.optimize_prefill = true;
    session.implementation_sha256[0] ^= 1;
    try std.testing.expect(!std.meta.eql(baseline, session.variantSettings(.packed_exact)));
    session.implementation_sha256[0] ^= 1;
    session.profile_artifact_sha256[0] ^= 1;
    try std.testing.expect(!std.meta.eql(baseline, session.variantSettings(.packed_exact)));
    session.profile_artifact_sha256[0] ^= 1;
    session.profile_semantic_sha256[0] ^= 1;
    try std.testing.expect(!std.meta.eql(baseline, session.variantSettings(.packed_exact)));
}

test "context command cannot select a fit engine" {
    const encoded =
        \\{"command":"stop","fit_engine":"native_whole_head_v2"}
    ;
    if (std.json.parseFromSlice(Command, std.testing.allocator, encoded, .{
        .ignore_unknown_fields = false,
    })) |forged| {
        defer forged.deinit();
        return error.ExpectedUnknownFitEngineField;
    } else |_| {}
}

test "context session rejects an engine not bound by implementation identity" {
    var session = Session{
        .allocator = std.testing.allocator,
        .io = std.testing.io,
        .driver = undefined,
        .campaign_dir = undefined,
        .model = "fixture",
        .expected_session_id = @splat('a'),
        .max_context_tokens = 1024,
        .max_kv_tokens = 512,
        .keep_prefix_tokens = 64,
        .keep_recent_tokens = 64,
        .optimize_decode = true,
        .optimize_prefill = true,
        .fit_engine = .native_whole_head_v2,
        .implementation_sha256 = compaction_profile.implementationDigest(
            compaction_profile.servingImplementation(.native_whole_head_v2, true, true),
        ),
        .profile_artifact_sha256 = @splat(2),
        .profile_semantic_sha256 = @splat(3),
    };
    try session.validateFitEngineIdentity();
    session.fit_engine = .block_v1;
    try std.testing.expectError(
        error.ContextQualificationFitEngineMismatch,
        session.validateFitEngineIdentity(),
    );
}

test "context quality command requires sealed structured and reference inputs" {
    var session = Session{
        .allocator = std.testing.allocator,
        .io = std.testing.io,
        .driver = undefined,
        .campaign_dir = undefined,
        .model = "fixture",
        .expected_session_id = @splat('a'),
        .max_context_tokens = 8192,
        .max_kv_tokens = 4096,
        .keep_prefix_tokens = 64,
        .keep_recent_tokens = 64,
        .optimize_decode = true,
        .optimize_prefill = true,
        .implementation_sha256 = @splat(1),
        .profile_artifact_sha256 = @splat(2),
        .profile_semantic_sha256 = @splat(3),
    };
    const hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const valid = Command{
        .command = .quality,
        .session_id = &session.expected_session_id,
        .request_index = 0,
        .route = .http,
        .variant = .attention_matching,
        .case_id = "case",
        .bucket = .document_qa,
        .document_file = "document.txt",
        .document_sha256 = hash,
        .question_file = "question.txt",
        .question_sha256 = hash,
        .request_content_sha256 = hash,
        .prompt_file = "prompt.txt",
        .prompt_sha256 = hash,
        .prompt_tokens = 8000,
        .reserved_output_tokens = 192,
        .reference_file = "reference.txt",
        .reference_sha256 = hash,
        .reference_tokens_file = "reference-tokens.json",
        .reference_tokens_sha256 = hash,
        .scorer = "utf8_nfc_trim_exact_v1",
        .settings_sha256 = hash,
        .variant_settings_sha256 = hash,
    };
    try session.validate(valid);
    var missing_reference = valid;
    missing_reference.reference_file = null;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(missing_reference));
    var packed_command = valid;
    packed_command.variant = .packed_exact;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(packed_command));

    var ordered = valid;
    ordered.document_file = null;
    ordered.document_sha256 = null;
    ordered.question_file = null;
    ordered.question_sha256 = null;
    ordered.messages_file = "messages.json";
    ordered.messages_sha256 = hash;
    ordered.source_sha256 = hash;
    ordered.selected_part_sha256s = &.{hash};
    try session.validate(ordered);
    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, ordered, .{});
    defer std.testing.allocator.free(encoded);
    var parsed = try std.json.parseFromSlice(Command, std.testing.allocator, encoded, .{
        .ignore_unknown_fields = false,
    });
    defer parsed.deinit();
    try session.validate(parsed.value);
    try std.testing.expectEqualStrings("prompt.txt", parsed.value.prompt_file.?);
    ordered.document_file = "ambiguous-document.txt";
    ordered.document_sha256 = hash;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(ordered));
    ordered.document_file = null;
    ordered.document_sha256 = null;
    ordered.messages_sha256 = null;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(ordered));
}

test "context quality reference token parser rejects vacuous and noninteger chains" {
    var session = Session{
        .allocator = std.testing.allocator,
        .io = std.testing.io,
        .driver = undefined,
        .campaign_dir = undefined,
        .model = "fixture",
        .expected_session_id = @splat('a'),
        .max_context_tokens = 8192,
        .max_kv_tokens = 4096,
        .keep_prefix_tokens = 64,
        .keep_recent_tokens = 64,
        .optimize_decode = true,
        .optimize_prefill = true,
        .implementation_sha256 = @splat(1),
        .profile_artifact_sha256 = @splat(2),
        .profile_semantic_sha256 = @splat(3),
    };
    const tokens = try session.parseReferenceTokens("[7,9]");
    defer std.testing.allocator.free(tokens);
    try std.testing.expectEqualSlices(i64, &.{ 7, 9 }, tokens);
    try std.testing.expectError(error.InvalidQualityReferenceTokens, session.parseReferenceTokens("[7]"));
    if (session.parseReferenceTokens("[true,9]")) |forged| {
        std.testing.allocator.free(forged);
        return error.ExpectedInvalidQualityReferenceTokens;
    } else |_| {}
}

test "context tokenizer materialization command is ordered profile-free and persistent" {
    var session = Session{
        .allocator = std.testing.allocator,
        .io = std.testing.io,
        .driver = undefined,
        .campaign_dir = undefined,
        .model = "fixture",
        .expected_session_id = @splat('a'),
        .max_context_tokens = 8192,
        .max_kv_tokens = 4096,
        .keep_prefix_tokens = 64,
        .keep_recent_tokens = 64,
        .optimize_decode = true,
        .optimize_prefill = true,
        .implementation_sha256 = @splat(1),
        .profile_artifact_sha256 = @splat(0),
        .profile_semantic_sha256 = @splat(0),
        .coverage_only = true,
    };
    const hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const valid = Command{
        .command = .materialize_quality_v1,
        .session_id = &session.expected_session_id,
        .request_index = 0,
        .variant = .attention_matching,
        .case_id = "case",
        .messages_file = "messages.json",
        .messages_sha256 = hash,
        .source_sha256 = hash,
        .selected_part_sha256s = &.{hash},
        .request_content_sha256 = hash,
        .reserved_output_tokens = 32,
        .reference_file = "reference.txt",
        .reference_sha256 = hash,
        .settings_sha256 = hash,
        .variant_settings_sha256 = hash,
    };
    try session.validate(valid);
    var routed = valid;
    routed.route = .native;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(routed));
    var pretokenized = valid;
    pretokenized.reference_tokens_file = "caller-tokens.json";
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(pretokenized));
    var legacy = valid;
    legacy.messages_file = null;
    legacy.messages_sha256 = null;
    legacy.source_sha256 = null;
    legacy.selected_part_sha256s = null;
    legacy.document_file = "document.txt";
    legacy.document_sha256 = hash;
    try std.testing.expectError(error.InvalidServingDriverCommand, session.validate(legacy));

    const tokens = try session.tokensI64Le(&.{ 7, 9 });
    defer std.testing.allocator.free(tokens);
    try std.testing.expectEqualSlices(u8, &.{ 7, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0 }, tokens);
    const offsets = try session.offsetsU32Le(&.{.{ 1, 3 }});
    defer std.testing.allocator.free(offsets);
    try std.testing.expectEqualSlices(u8, &.{ 1, 0, 0, 0, 3, 0, 0, 0 }, offsets);
    const labels = try session.labelsU32Le(&.{ 0, 1, 65537 });
    defer std.testing.allocator.free(labels);
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0 }, labels);
}
