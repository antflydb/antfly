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

//! Qualification-only exporter for a sealed production query campaign.
const std = @import("std");
const builtin = @import("builtin");
const coordinator = @import("backends/compaction_coordinator.zig");
const campaign_mod = @import("backends/context_query_campaign_pool.zig");
const packed_session = @import("backends/packed_context_session.zig");
const policy = @import("runtime/context_policy.zig");
const profiles = @import("runtime/kv/compaction_profile.zig");

pub const Provenance = struct {
    binary_sha256: [32]u8,
    source_build_sha256: [32]u8,
};

pub const Capture = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    directory: std.Io.Dir,
    stem: []const u8,
    layout: policy.Layout,
    identity: profiles.Identity,
    provenance: Provenance,
    capture_inputs_manifest_sha256: [32]u8,
    coverage_receipt_sha256: [32]u8,
    request_content_sha256: [32]u8,
    prompt_tokens: u32,
    prompt_tokens_sha256: [32]u8,
    question_source_start: u32,
    question_source_end: u32,
    question_tokens_sha256: [32]u8,
    admitted_prefill_chunk_rows: u32,
    prefix_flash_prefill_dispatches: u64,
    implementation_sha256: [32]u8,
    fit_template_sha256: [32]u8,
    gate_template_sha256: [32]u8,
    generation_contract_sha256: [32]u8,
    native_repeat_training_plan_sha256: ?[32]u8 = null,
    live: *const packed_session.Session,
    eligible_block_starts: []const u32,
    document_id: []const u8,
    document_sha256: [32]u8,
    document_bucket: []const u8,

    pub fn observer(self: *Capture) coordinator.CampaignObserver {
        return .{ .ctx = self, .observe_fn = observe };
    }

    /// Separate, opt-in diagnostic source for whole-head qualification. It
    /// includes the exact prefix and recent tail that block captures omit.
    /// Names and the index are separate from the immutable block campaign.
    pub fn writeNativeFullHeadsDiagnostic(self: *Capture, load_generation: u64) !void {
        if (comptime !@import("build_options").enable_metal) return error.MetalNotEnabled;
        if (!builtin.is_test) return error.UnqualifiedCompactionArtifact;
        if (load_generation == 0 or self.live.logical_tokens != self.prompt_tokens) return error.InvalidCampaignCapture;
        try self.live.drainCampaignCaptureReadback();
        var full = self.*;
        full.stem = try std.fmt.allocPrint(self.allocator, "{s}-native-head", .{self.stem});
        defer self.allocator.free(full.stem);
        var entries: std.ArrayList(BaseEntry) = .empty;
        defer {
            for (entries.items) |entry| self.allocator.free(entry.path);
            entries.deinit(self.allocator);
        }
        var readback: usize = 0;
        for (self.layout.layers[0..self.layout.len], 0..) |layer, owner| {
            if (layer.owner != owner or layer.sliding_window != 0) continue;
            const block = try self.live.captureExactBlock(@intCast(owner), 0, self.prompt_tokens);
            const result = try full.writePackedBaseBlock(block, load_generation);
            errdefer self.allocator.free(result.descriptor_path);
            try entries.append(self.allocator, .{ .owner_layer = @intCast(owner), .block_start = 0, .path = result.descriptor_path, .sha256 = result.descriptor_sha256 });
            readback = try std.math.add(usize, readback, result.readback_bytes);
        }
        if (entries.items.len == 0) return error.InvalidCampaignCapture;
        var index_entries: std.ArrayList(BaseIndexEntry) = .empty;
        defer index_entries.deinit(self.allocator);
        var hexes: [256][64]u8 = undefined;
        if (entries.items.len > hexes.len) return error.InvalidCampaignCapture;
        for (entries.items, 0..) |entry, i| {
            hexes[i] = std.fmt.bytesToHex(entry.sha256, .lower);
            try index_entries.append(self.allocator, .{ .owner_layer = entry.owner_layer, .block_start = entry.block_start, .path = entry.path, .sha256 = &hexes[i] });
        }
        const prompt_hex = std.fmt.bytesToHex(self.prompt_tokens_sha256, .lower);
        const artifact_hex = std.fmt.bytesToHex(self.identity.artifact, .lower);
        const binary_hex = std.fmt.bytesToHex(self.provenance.binary_sha256, .lower);
        const source_hex = std.fmt.bytesToHex(self.provenance.source_build_sha256, .lower);
        const json = try std.json.Stringify.valueAlloc(self.allocator, .{
            .schema = "antfly.context.native.full-head-source.v2",
            .diagnostic_only = true,
            .serving_qualified = false,
            .prompt_tokens = self.prompt_tokens,
            .prompt_tokens_sha256 = prompt_hex[0..],
            .load_generation = load_generation,
            .artifact_sha256 = artifact_hex[0..],
            .binary_sha256 = binary_hex[0..],
            .source_build_sha256 = source_hex[0..],
            .eligible_block_starts = self.eligible_block_starts,
            .kv_export_readback_bytes = readback,
            .descriptors = index_entries.items,
        }, .{});
        defer self.allocator.free(json);
        try full.writeFile("index.json", json);
    }

    /// A separate development protocol: history-only representative replay,
    /// with disjoint positions from the same private branch in each partition.
    /// This never relabels the five-source campaign's held-out queries.
    pub fn writeNativeRepeatDiagnostic(self: *Capture, branch: *const @import("backends/model_replay_context.zig").Session, tokens: []const i64, load_generation: u64) ![32]u8 {
        if (comptime !@import("build_options").enable_metal) return error.MetalNotEnabled;
        if (!builtin.is_test or !branch.sealed or branch.failed or load_generation == 0 or
            tokens.len < 2 or tokens.len > 512 or branch.config.query_sources != null or
            branch.config.plan.prompt_tokens != self.prompt_tokens or branch.logical_tokens != self.prompt_tokens + tokens.len)
            return error.InvalidCampaignCapture;
        var repeat = self.*;
        repeat.stem = try std.fmt.allocPrint(self.allocator, "{s}-native-repeat", .{self.stem});
        defer self.allocator.free(repeat.stem);
        const token_ref = try repeat.writeNativeTensor("tokens.i64", std.mem.sliceAsBytes(tokens));
        defer token_ref.deinit(self.allocator);
        var owners: std.ArrayList(NativeRepeatOwner) = .empty;
        defer {
            for (owners.items) |owner| owner.deinit(self.allocator);
            owners.deinit(self.allocator);
        }
        var readback: usize = 0;
        for (self.layout.layers[0..self.layout.len], 0..) |layer, owner| {
            if (layer.owner != owner or layer.sliding_window != 0) continue;
            const pool = try branch.sealedQueries(@intCast(owner));
            if (pool.shape.protocol != null or pool.shape.training_rows == 0 or pool.shape.validation_rows == 0)
                return error.InvalidCampaignCapture;
            const train = try branch.sealedQueryPartition(@intCast(owner), .training);
            const held = try branch.sealedQueryPartition(@intCast(owner), .validation);
            const stride_rows = try std.math.add(u32, pool.shape.training_rows, pool.shape.validation_rows);
            if (train.heads != layer.kv_heads or held.heads != layer.kv_heads or
                train.dim != layer.head_dim or held.dim != layer.head_dim or
                train.rows_per_head != pool.shape.training_rows or held.rows_per_head != pool.shape.validation_rows or
                train.stride_rows != stride_rows or held.stride_rows != stride_rows or
                train.row_offset != 0 or held.row_offset != pool.shape.training_rows)
                return error.InvalidCampaignCapture;
            for (0..layer.kv_heads) |kv_head| {
                var strata: std.ArrayList(Stratum) = .empty;
                defer strata.deinit(self.allocator);
                try appendNativeRepeatHeadStrata(self.allocator, self.layout, owner, @intCast(kv_head), &strata);
                if (strata.items.len == 0 or train.rows_per_head != strata.items.len * pool.shape.train_per_stratum or
                    held.rows_per_head != strata.items.len * pool.shape.held_per_stratum)
                    return error.InvalidCampaignCapture;
                const names = [_][]const u8{ "training.f32", "validation.f32", "training-positions.u32", "validation-positions.u32" };
                const data = [_][]const u8{
                    std.mem.sliceAsBytes(try train.queryHead(kv_head)),
                    std.mem.sliceAsBytes(try held.queryHead(kv_head)),
                    std.mem.sliceAsBytes(try train.positionHead(kv_head)),
                    std.mem.sliceAsBytes(try held.positionHead(kv_head)),
                };
                var refs: [4]NativeTensorRef = undefined;
                var initialized: usize = 0;
                errdefer for (refs[0..initialized]) |ref| ref.deinit(self.allocator);
                for (names, data, 0..) |name, bytes, i| {
                    const suffix = try std.fmt.allocPrint(self.allocator, "layer{d}-head{d}-{s}", .{ owner, kv_head, name });
                    defer self.allocator.free(suffix);
                    refs[i] = try repeat.writeNativeTensor(suffix, bytes);
                    initialized += 1;
                }
                readback = try std.math.add(usize, readback, data[0].len + data[1].len);
                var owned_strata: ?[]Stratum = try self.allocator.dupe(Stratum, strata.items);
                errdefer if (owned_strata) |value| self.allocator.free(value);
                try owners.append(self.allocator, .{
                    .owner_layer = @intCast(owner),
                    .source_heads = layer.kv_heads,
                    .kv_head = @intCast(kv_head),
                    .head_dim = layer.head_dim,
                    .strata = owned_strata.?,
                    .training_rows = train.rows_per_head,
                    .validation_rows = held.rows_per_head,
                    .training_group_rows = pool.shape.train_per_stratum,
                    .validation_group_rows = pool.shape.held_per_stratum,
                    .training = refs[0],
                    .validation = refs[1],
                    .training_positions = refs[2],
                    .validation_positions = refs[3],
                });
                // Ownership moved into `owners`; suppress the local error cleanup.
                initialized = 0;
                owned_strata = null;
            }
        }
        if (owners.items.len == 0 or branch.kv_readback_bytes != 0) return error.InvalidCampaignCapture;
        const source_name = try self.artifactName("native-head-index.json");
        defer self.allocator.free(source_name);
        const source_bytes = try self.directory.readFileAlloc(self.io, source_name, self.allocator, .limited(2 * 1024 * 1024));
        defer self.allocator.free(source_bytes);
        const source_hex = std.fmt.bytesToHex(digest(source_bytes), .lower);
        const input_hex = std.fmt.bytesToHex(self.capture_inputs_manifest_sha256, .lower);
        const binary_hex = std.fmt.bytesToHex(self.provenance.binary_sha256, .lower);
        const build_hex = std.fmt.bytesToHex(self.provenance.source_build_sha256, .lower);
        var training_plan_hex: [64]u8 = undefined;
        const training_plan_sha256: ?[]const u8 = if (self.native_repeat_training_plan_sha256) |value| blk: {
            training_plan_hex = std.fmt.bytesToHex(value, .lower);
            break :blk training_plan_hex[0..];
        } else null;
        const json = try std.json.Stringify.valueAlloc(self.allocator, .{
            .schema = "antfly.context.native.repeat-source.v2",
            .diagnostic_only = true,
            .evaluation_role = "internal_development",
            .serving_qualified = false,
            .fresh_unseen_qualified = false,
            .query_sampling = "representative_repeat_partition_v1",
            .selection = "history_only_cuda_study_v1",
            .position_partition = "hashed_quartet_v1",
            .repeat_start = self.prompt_tokens,
            .repeat_rows = tokens.len,
            .repeat_tokens = token_ref,
            .load_generation = load_generation,
            .binary_sha256 = binary_hex[0..],
            .source_build_sha256 = build_hex[0..],
            .training_plan_sha256 = training_plan_sha256,
            .capture_inputs_manifest_sha256 = input_hex[0..],
            .full_head_source_index = .{ .path = source_name, .sha256 = source_hex[0..] },
            .branch_physical_bytes = branch.sizes.physical_bytes,
            .source_kv_readback_bytes = branch.kv_readback_bytes,
            .diagnostic_query_readback_bytes = readback,
            .owners = owners.items,
        }, .{});
        defer self.allocator.free(json);
        try repeat.writeFile("index.json", json);
        return digest(json);
    }

    const NativeTensorRef = struct {
        path: []const u8,
        sha256: []const u8,
        bytes: usize,
        fn deinit(self: NativeTensorRef, a: std.mem.Allocator) void {
            a.free(self.path);
            a.free(self.sha256);
        }
    };
    const NativeRepeatOwner = struct {
        owner_layer: u32,
        source_heads: u32,
        kv_head: u32,
        head_dim: u32,
        strata: []Stratum,
        training_rows: u32,
        validation_rows: u32,
        training_group_rows: u32,
        validation_group_rows: u32,
        training: NativeTensorRef,
        validation: NativeTensorRef,
        training_positions: NativeTensorRef,
        validation_positions: NativeTensorRef,
        fn deinit(self: NativeRepeatOwner, a: std.mem.Allocator) void {
            a.free(self.strata);
            for ([_]NativeTensorRef{ self.training, self.validation, self.training_positions, self.validation_positions }) |ref| ref.deinit(a);
        }
    };

    fn appendNativeRepeatHeadStrata(
        allocator: std.mem.Allocator,
        layout: policy.Layout,
        owner: usize,
        kv_head: u32,
        strata: *std.ArrayList(Stratum),
    ) !void {
        if (owner >= layout.len) return error.InvalidCampaignCapture;
        const source = layout.layers[owner];
        if (source.owner != owner or source.kv_heads == 0 or kv_head >= source.kv_heads)
            return error.InvalidCampaignCapture;
        for (layout.layers[0..layout.len], 0..) |consumer, consumer_layer| {
            if (consumer.owner != owner) continue;
            if (consumer.kv_heads != source.kv_heads or consumer.head_dim != source.head_dim or
                consumer.query_heads == 0 or consumer.query_heads % consumer.kv_heads != 0)
                return error.InvalidCampaignCapture;
            const groups = consumer.query_heads / consumer.kv_heads;
            const first = try std.math.mul(u32, kv_head, groups);
            const end = try std.math.add(u32, first, groups);
            for (first..end) |query_head| try strata.append(allocator, .{
                .consumer_layer = @intCast(consumer_layer),
                .query_head = @intCast(query_head),
                .kv_head = kv_head,
            });
        }
    }

    fn writeNativeTensor(self: *Capture, suffix: []const u8, bytes: []const u8) !NativeTensorRef {
        const path = try self.artifactName(suffix);
        errdefer self.allocator.free(path);
        const hex = try self.allocator.dupe(u8, &std.fmt.bytesToHex(digest(bytes), .lower));
        errdefer self.allocator.free(hex);
        try self.writeFile(suffix, bytes);
        return .{ .path = path, .sha256 = hex, .bytes = bytes.len };
    }

    fn observe(ctx: *anyopaque, observation: coordinator.CampaignObservation) !void {
        if (comptime !@import("build_options").enable_metal) return error.MetalNotEnabled;
        const self: *Capture = @ptrCast(@alignCast(ctx));
        try self.live.drainCampaignCaptureReadback();
        const observed_question_sha256 = tokenDigest(observation.question.prefix_ids);
        if (comptime builtin.target.cpu.arch.endian() != .little) return error.UnsupportedCampaignCaptureEndian;
        if (observation.prompt_tokens != self.prompt_tokens or
            !std.mem.eql(u8, &observation.prompt_tokens_sha256, &self.prompt_tokens_sha256))
            return error.CampaignCapturePromptMismatch;
        if (observation.question_source_start != self.question_source_start or
            observation.question_source_end != self.question_source_end or
            observation.question_source_start >= observation.question_source_end or
            observation.question_source_end > self.prompt_tokens or
            observation.question_source_end - observation.question_source_start != observation.question.prefix_ids.len or
            !std.mem.eql(u8, &observed_question_sha256, &self.question_tokens_sha256))
            return error.CampaignCaptureQuestionMismatch;
        if (self.admitted_prefill_chunk_rows == 0 or self.prefix_flash_prefill_dispatches == 0)
            return error.CampaignCapturePrefillRouteMismatch;
        if (observation.publication_requested or observation.live_epoch_before != observation.live_epoch_after or
            observation.live_session_identity_before == 0 or
            observation.live_session_identity_before != observation.live_session_identity_after)
            return error.CampaignCapturePublicationChanged;
        const before_fingerprint = observation.live_fingerprint_before orelse return error.CampaignCaptureMissingLiveFingerprint;
        const after_fingerprint = observation.live_fingerprint_after orelse return error.CampaignCaptureMissingLiveFingerprint;
        if (before_fingerprint.context_instance_id != observation.live_session_identity_before or
            after_fingerprint.context_instance_id != observation.live_session_identity_after or
            before_fingerprint.publication_epoch != observation.live_epoch_before or
            after_fingerprint.publication_epoch != observation.live_epoch_after or
            before_fingerprint.logical_tokens != observation.prompt_tokens or
            after_fingerprint.logical_tokens != observation.prompt_tokens or
            !std.mem.eql(u8, &before_fingerprint.payload_sha256, &after_fingerprint.payload_sha256) or
            !std.mem.eql(u8, &before_fingerprint.descriptor_sha256, &after_fingerprint.descriptor_sha256) or
            before_fingerprint.diagnostic_kv_readback_bytes == 0 or
            before_fingerprint.diagnostic_kv_readback_bytes != after_fingerprint.diagnostic_kv_readback_bytes)
            return error.CampaignCaptureLivePayloadChanged;
        if (observation.live_bytes_before != observation.live_bytes_after) {
            std.debug.print("context_campaign_capture_rejected: live_bytes_before={d} live_bytes_after={d}\n", .{ observation.live_bytes_before, observation.live_bytes_after });
            return error.CampaignCaptureLiveAllocationChanged;
        }
        if (observation.load_generation == 0 or observation.campaign.load_generation != observation.load_generation)
            return error.CampaignCaptureLoadGenerationMismatch;
        if (observation.campaign_backend_bytes == 0 or observation.campaign_host_bytes == 0 or
            observation.campaign_physical_bytes != try std.math.add(usize, observation.campaign_backend_bytes, observation.campaign_host_bytes) or
            observation.campaign_query_bytes != observation.query_export_bytes)
            return error.CampaignCaptureAllocationMismatch;
        if (!std.mem.eql(u8, &self.identity.runtime, &self.implementation_sha256))
            return error.CampaignCaptureImplementationMismatch;
        if (observation.query_export_bytes != try observation.campaign.captureQueryExportBytes())
            return error.CampaignCaptureQueryExportMismatch;
        const receipts = [_]coordinator.CampaignBranchReceipt{ observation.question, observation.fit, observation.gate };
        const question_provider_delta = try requireQuestionProviderEvidence(
            observation.question.elapsed_ns,
            observation.question.provider_delta,
        );
        if (receipts[0].branch != .question or receipts[1].branch != .fit or receipts[2].branch != .gate or
            receipts[0].physical_branch_id != 0 or receipts[1].physical_branch_id != 1 or receipts[2].physical_branch_id != 2 or
            receipts[0].route != .question_hidden_replay or receipts[1].route != .fixed_serial_private_study or
            receipts[2].route != .fixed_serial_private_study or receipts[0].session_identity == 0 or
            receipts[1].session_identity == 0 or receipts[2].session_identity == 0 or
            receipts[0].session_identity == receipts[1].session_identity or receipts[0].session_identity == receipts[2].session_identity or
            receipts[1].session_identity == receipts[2].session_identity)
            return error.CampaignCaptureBranchIdentityMismatch;
        var serial_attention_dispatches: u64 = 0;
        var split_attention_dispatches: u64 = 0;
        var replay_split_scratch_bytes: usize = 0;
        for (receipts) |receipt| {
            serial_attention_dispatches = try std.math.add(u64, serial_attention_dispatches, receipt.serial_attention_dispatches);
            split_attention_dispatches = try std.math.add(u64, split_attention_dispatches, receipt.split_attention_dispatches);
            replay_split_scratch_bytes = @max(replay_split_scratch_bytes, receipt.replay_split_scratch_bytes);
        }
        if (observation.replay_options.global_attention_split) |split| {
            if (split.max_query_rows == 0 or split.split_count < 2 or replay_split_scratch_bytes == 0 or split_attention_dispatches == 0)
                return error.CampaignCaptureAllocationMismatch;
            for (receipts) |receipt| if (receipt.replay_split_scratch_bytes != replay_split_scratch_bytes)
                return error.CampaignCaptureAllocationMismatch;
        } else if (replay_split_scratch_bytes != 0 or split_attention_dispatches != 0) {
            return error.CampaignCaptureAllocationMismatch;
        }

        const prompt = self.prompt_tokens;
        const fit_response_start = try std.math.add(u32, prompt, @intCast(observation.fit.prefix_ids.len));
        const gate_response_start = try std.math.add(u32, prompt, @intCast(observation.gate.prefix_ids.len));
        const sources = [_]Source{
            .{ .id = "request-question-replay", .kind = "request_question_replay", .partition = "training", .conditioning = "completed_prompt_private_replay", .tokens = observation.question.prefix_ids, .logical_start = prompt, .branch_start = prompt, .branch_count = @intCast(observation.question.prefix_ids.len), .receipt = observation.question, .source_index = 0 },
            .{ .id = "fit-study-prefill", .kind = "fixed_study_prefill", .partition = "training", .conditioning = "completed_prompt_private_replay", .tokens = observation.fit.prefix_ids, .logical_start = prompt, .branch_start = prompt, .branch_count = @intCast(observation.fit.prefix_ids.len + observation.fit.generated_ids.len), .receipt = observation.fit, .source_index = 1 },
            .{ .id = "fit-study-response", .kind = "fixed_study_generated_response", .partition = "training", .conditioning = "completed_prompt_study_generated_response", .tokens = observation.fit.generated_ids, .logical_start = fit_response_start, .branch_start = prompt, .branch_count = @intCast(observation.fit.prefix_ids.len + observation.fit.generated_ids.len), .receipt = observation.fit, .source_index = 2 },
            .{ .id = "gate-study-prefill", .kind = "fixed_study_prefill", .partition = "validation", .conditioning = "completed_prompt_private_replay", .tokens = observation.gate.prefix_ids, .logical_start = prompt, .branch_start = prompt, .branch_count = @intCast(observation.gate.prefix_ids.len + observation.gate.generated_ids.len), .receipt = observation.gate, .source_index = 3 },
            .{ .id = "gate-study-response", .kind = "fixed_study_generated_response", .partition = "validation", .conditioning = "completed_prompt_study_generated_response", .tokens = observation.gate.generated_ids, .logical_start = gate_response_start, .branch_start = prompt, .branch_count = @intCast(observation.gate.prefix_ids.len + observation.gate.generated_ids.len), .receipt = observation.gate, .source_index = 4 },
        };
        var emitted_query_bytes: usize = 0;
        var artifacts: std.ArrayList(ArtifactEntry) = .empty;
        defer {
            for (artifacts.items) |artifact| self.allocator.free(artifact.path);
            artifacts.deinit(self.allocator);
        }
        for (sources) |source| {
            if (source.tokens.len == 0) return error.CampaignCaptureEmptySource;
            if (source.receipt.elapsed_ns == 0) return error.CampaignCaptureMissingTiming;
            if (source.receipt.status_readback_bytes == 0) return error.CampaignCaptureMissingStatusReadback;
            if (source.receipt.physical_bytes < source.receipt.query_bytes or source.receipt.peak_bytes < source.receipt.physical_bytes)
                return error.CampaignCaptureBranchAllocationMismatch;
            try artifacts.ensureUnusedCapacity(self.allocator, 1);
            artifacts.appendAssumeCapacity(try self.writeTokens(source));
        }
        for (self.layout.layers[0..self.layout.len], 0..) |layer, layer_index| {
            if (layer.owner != layer_index or layer.sliding_window != 0) continue;
            for (sources) |source| {
                try artifacts.ensureUnusedCapacity(self.allocator, 3);
                const exported = try self.writeSource(observation.campaign, @intCast(layer_index), source);
                emitted_query_bytes = try std.math.add(usize, emitted_query_bytes, exported.query_bytes);
                for (exported.artifacts) |artifact| artifacts.appendAssumeCapacity(artifact);
            }
        }
        if (emitted_query_bytes != observation.query_export_bytes) return error.CampaignCaptureQueryExportMismatch;
        const base_export = try self.writePackedBaseBlocks(observation.load_generation);
        try artifacts.ensureUnusedCapacity(self.allocator, 1);
        artifacts.appendAssumeCapacity(.{ .kind = "packed_base_index", .source_id = null, .owner_layer = null, .path = try self.artifactName("base-index.json"), .sha256 = base_export.index_sha256 });
        const artifact_index_sha256 = try self.writeArtifactIndex(artifacts.items);
        const artifact_hex = std.fmt.bytesToHex(self.identity.artifact, .lower);
        const tokenizer_hex = std.fmt.bytesToHex(self.identity.tokenizer, .lower);
        const template_hex = std.fmt.bytesToHex(self.identity.template, .lower);
        const layout_hex = std.fmt.bytesToHex(self.identity.layout, .lower);
        const implementation_hex = std.fmt.bytesToHex(self.identity.runtime, .lower);
        const binary_hex = std.fmt.bytesToHex(self.provenance.binary_sha256, .lower);
        const source_hex = std.fmt.bytesToHex(self.provenance.source_build_sha256, .lower);
        const capture_inputs_hex = std.fmt.bytesToHex(self.capture_inputs_manifest_sha256, .lower);
        const coverage_receipt_hex = std.fmt.bytesToHex(self.coverage_receipt_sha256, .lower);
        const request_content_hex = std.fmt.bytesToHex(self.request_content_sha256, .lower);
        const fit_template_hex = std.fmt.bytesToHex(self.fit_template_sha256, .lower);
        const gate_template_hex = std.fmt.bytesToHex(self.gate_template_sha256, .lower);
        const contract_hex = std.fmt.bytesToHex(self.generation_contract_sha256, .lower);
        const prompt_hex = std.fmt.bytesToHex(observation.prompt_tokens_sha256, .lower);
        const question_token_digest = tokenDigest(observation.question.prefix_ids);
        const question_token_hex = std.fmt.bytesToHex(question_token_digest, .lower);
        const live_payload_before_hex = std.fmt.bytesToHex(before_fingerprint.payload_sha256, .lower);
        const live_payload_after_hex = std.fmt.bytesToHex(after_fingerprint.payload_sha256, .lower);
        const live_descriptor_before_hex = std.fmt.bytesToHex(before_fingerprint.descriptor_sha256, .lower);
        const live_descriptor_after_hex = std.fmt.bytesToHex(after_fingerprint.descriptor_sha256, .lower);
        const document_hex = std.fmt.bytesToHex(self.document_sha256, .lower);
        const base_index_hex = std.fmt.bytesToHex(base_export.index_sha256, .lower);
        const base_index_path = try self.artifactName("base-index.json");
        defer self.allocator.free(base_index_path);
        const artifact_index_hex = std.fmt.bytesToHex(artifact_index_sha256, .lower);
        const artifact_index_path = try self.artifactName("artifact-index.json");
        defer self.allocator.free(artifact_index_path);
        const summary = try std.json.Stringify.valueAlloc(self.allocator, .{
            .schema_version = @as(u32, 7),
            .stage = "context_query_campaign_capture",
            .completed = true,
            .publication_requested = false,
            .branch_order = [_][]const u8{ "question", "fit", "gate" },
            .prefix_storage = "packed_exact",
            .prefix_kernel_route = "metal_context_flash_prefill_abi2",
            .admitted_prefill_chunk_rows = self.admitted_prefill_chunk_rows,
            .prefix_flash_prefill_dispatches = self.prefix_flash_prefill_dispatches,
            .private_kernel_route = if (observation.replay_options.global_attention_split != null) "bounded_split_global_replay_v1" else "fixed_serial_private_replay",
            .private_replay_attention = .{
                .serial_dispatches = serial_attention_dispatches,
                .split_dispatches = split_attention_dispatches,
                .split_scratch_bytes = replay_split_scratch_bytes,
                .split_max_query_rows = if (observation.replay_options.global_attention_split) |split| split.max_query_rows else null,
                .split_count = if (observation.replay_options.global_attention_split) |split| split.split_count else null,
            },
            // Schema 6 requires the question timing/provider observation and
            // validates it before writing artifacts. Study timing/provider
            // observations remain passive and nullable; null means unavailable.
            .private_question_observation = .{ .physical_branch_id = "question", .elapsed_ns = observation.question.elapsed_ns, .provider_delta = question_provider_delta },
            .private_study_observation = .{
                .fit = .{ .physical_branch_id = "fit-study", .timing = observation.fit.study_timing, .provider_delta = observation.fit.provider_delta, .singleton_paged_decode_forwards = observation.fit.singleton_paged_decode_forwards, .singleton_paged_decode_expected_layer_dispatches = observation.fit.singleton_paged_decode_expected_layer_dispatches },
                .gate = .{ .physical_branch_id = "gate-study", .timing = observation.gate.study_timing, .provider_delta = observation.gate.provider_delta, .singleton_paged_decode_forwards = observation.gate.singleton_paged_decode_forwards, .singleton_paged_decode_expected_layer_dispatches = observation.gate.singleton_paged_decode_expected_layer_dispatches },
            },
            .study_singleton_paged_decode_requested = observation.replay_options.study_singleton_paged_decode,
            .fit_template_sha256 = fit_template_hex[0..],
            .gate_template_sha256 = gate_template_hex[0..],
            .generation_contract_sha256 = contract_hex[0..],
            .capture_inputs_manifest_sha256 = capture_inputs_hex[0..],
            .coverage_receipt_sha256 = coverage_receipt_hex[0..],
            .request_content_sha256 = request_content_hex[0..],
            .prompt_tokens = observation.prompt_tokens,
            .prompt_tokens_sha256 = prompt_hex[0..],
            .question = .{ .source_start = observation.question_source_start, .source_end = observation.question_source_end, .token_count = observation.question.prefix_ids.len, .tokens_sha256 = question_token_hex[0..] },
            .document = .{ .id = self.document_id, .sha256 = document_hex[0..], .bucket = self.document_bucket },
            .eligible_block_starts = self.eligible_block_starts,
            .load_generation = observation.load_generation,
            .live_epoch_before = observation.live_epoch_before,
            .live_epoch_after = observation.live_epoch_after,
            .live_bytes_before = observation.live_bytes_before,
            .live_bytes_after = observation.live_bytes_after,
            .live_session_identity_before = observation.live_session_identity_before,
            .live_session_identity_after = observation.live_session_identity_after,
            .live_fingerprint_publication_epoch_before = before_fingerprint.publication_epoch,
            .live_fingerprint_publication_epoch_after = after_fingerprint.publication_epoch,
            .live_fingerprint_logical_tokens_before = before_fingerprint.logical_tokens,
            .live_fingerprint_logical_tokens_after = after_fingerprint.logical_tokens,
            .live_payload_sha256_before = live_payload_before_hex[0..],
            .live_payload_sha256_after = live_payload_after_hex[0..],
            .live_descriptor_sha256_before = live_descriptor_before_hex[0..],
            .live_descriptor_sha256_after = live_descriptor_after_hex[0..],
            .live_fingerprint_diagnostic_readback_bytes_before = before_fingerprint.diagnostic_kv_readback_bytes,
            .live_fingerprint_diagnostic_readback_bytes_after = after_fingerprint.diagnostic_kv_readback_bytes,
            .query_export_bytes = observation.query_export_bytes,
            .campaign_query_bytes = observation.campaign_query_bytes,
            .campaign_backend_bytes = observation.campaign_backend_bytes,
            .campaign_host_bytes = observation.campaign_host_bytes,
            .campaign_physical_bytes = observation.campaign_physical_bytes,
            .observer_output_admitted_bytes = observation.observer_output_admitted_bytes,
            .base_index = .{ .path = base_index_path, .sha256 = base_index_hex[0..] },
            .artifact_index = .{ .path = artifact_index_path, .sha256 = artifact_index_hex[0..] },
            .kv_export_readback_bytes = base_export.readback_bytes,
            .loaded_identity = .{ .load_generation = observation.load_generation, .artifact_sha256 = artifact_hex[0..], .tokenizer_sha256 = tokenizer_hex[0..], .template_sha256 = template_hex[0..], .layout_sha256 = layout_hex[0..], .implementation_sha256 = implementation_hex[0..] },
            .provenance = .{ .binary_sha256 = binary_hex[0..], .source_build_sha256 = source_hex[0..] },
        }, .{});
        defer self.allocator.free(summary);
        try self.writeFile("campaign.json", summary);
    }

    const BaseExport = struct { readback_bytes: usize, index_sha256: [32]u8 };
    const BaseEntry = struct { owner_layer: u32, block_start: u32, path: []u8, sha256: [32]u8 };
    const BaseIndexEntry = struct { owner_layer: u32, block_start: u32, path: []const u8, sha256: []const u8 };

    fn writePackedBaseBlocks(self: *Capture, load_generation: u64) !BaseExport {
        if (self.eligible_block_starts.len == 0 or self.document_id.len == 0 or self.document_bucket.len == 0)
            return error.InvalidCampaignCapture;
        var readback_bytes: usize = 0;
        var entries: std.ArrayList(BaseEntry) = .empty;
        defer {
            for (entries.items) |entry| self.allocator.free(entry.path);
            entries.deinit(self.allocator);
        }
        for (self.layout.layers[0..self.layout.len], 0..) |layer, layer_index| {
            if (layer.owner != layer_index or layer.sliding_window != 0) continue;
            for (self.eligible_block_starts) |block_start| {
                const block = try self.live.captureExactBlock(@intCast(layer_index), block_start, 512);
                if (block.heads != layer.kv_heads or block.dim != layer.head_dim or block.rows != 512)
                    return error.InvalidCampaignCapture;
                const result = try self.writePackedBaseBlock(block, load_generation);
                readback_bytes = try std.math.add(usize, readback_bytes, result.readback_bytes);
                entries.append(self.allocator, .{ .owner_layer = block.layer, .block_start = block.logical_start, .path = result.descriptor_path, .sha256 = result.descriptor_sha256 }) catch |err| {
                    self.allocator.free(result.descriptor_path);
                    return err;
                };
            }
        }
        if (entries.items.len == 0) return error.InvalidCampaignCapture;
        const hexes = try self.allocator.alloc([64]u8, entries.items.len);
        defer self.allocator.free(hexes);
        const index_entries = try self.allocator.alloc(BaseIndexEntry, entries.items.len);
        defer self.allocator.free(index_entries);
        for (entries.items, index_entries, hexes) |entry, *json_entry, *hex| {
            hex.* = std.fmt.bytesToHex(entry.sha256, .lower);
            json_entry.* = .{ .owner_layer = entry.owner_layer, .block_start = entry.block_start, .path = entry.path, .sha256 = hex[0..] };
        }
        const prompt_hex = std.fmt.bytesToHex(self.prompt_tokens_sha256, .lower);
        const document_hex = std.fmt.bytesToHex(self.document_sha256, .lower);
        const index = try std.json.Stringify.valueAlloc(self.allocator, .{
            .schema_version = @as(u32, 1),
            .stage = "context_packed_exact_block_index",
            .completed = true,
            .prompt = .{ .tokens = self.prompt_tokens, .sha256 = prompt_hex[0..] },
            .document = .{ .id = self.document_id, .sha256 = document_hex[0..], .bucket = self.document_bucket },
            .eligible_block_starts = self.eligible_block_starts,
            .descriptors = index_entries,
        }, .{});
        defer self.allocator.free(index);
        try self.writeFile("base-index.json", index);
        return .{ .readback_bytes = readback_bytes, .index_sha256 = digest(index) };
    }

    const BaseBlockResult = struct { readback_bytes: usize, descriptor_path: []u8, descriptor_sha256: [32]u8 };
    const Consumer = struct { layer: u32, query_heads: u32 };
    const Stratum = struct { consumer_layer: u32, query_head: u32, kv_head: u32 };

    fn writePackedBaseBlock(self: *Capture, block: packed_session.ExactBlockCapture, load_generation: u64) !BaseBlockResult {
        const keys_name = try std.fmt.allocPrint(self.allocator, "base-layer{d}-block{d}-keys.f16", .{ block.layer, block.logical_start });
        defer self.allocator.free(keys_name);
        const values_name = try std.fmt.allocPrint(self.allocator, "base-layer{d}-block{d}-values.f16", .{ block.layer, block.logical_start });
        defer self.allocator.free(values_name);
        const keys_path = try self.artifactName(keys_name);
        defer self.allocator.free(keys_path);
        const values_path = try self.artifactName(values_name);
        defer self.allocator.free(values_path);
        var keys_file = try self.create(keys_name);
        defer keys_file.close(self.io);
        var values_file = try self.create(values_name);
        defer values_file.close(self.io);
        var keys_hash = std.crypto.hash.sha2.Sha256.init(.{});
        var values_hash = std.crypto.hash.sha2.Sha256.init(.{});
        var bytes: usize = 0;
        // Canonical base capture order is [logical row][KV head][head dim].
        for (0..block.rows) |row| for (0..block.heads) |head| {
            const keys = try block.row(.keys, @intCast(row), @intCast(head));
            const values = try block.row(.values, @intCast(row), @intCast(head));
            try keys_file.writeStreamingAll(self.io, keys);
            try values_file.writeStreamingAll(self.io, values);
            keys_hash.update(keys);
            values_hash.update(values);
            bytes = try std.math.add(usize, bytes, try std.math.add(usize, keys.len, values.len));
        };
        try keys_file.sync(self.io);
        try values_file.sync(self.io);
        var keys_digest: [32]u8 = undefined;
        keys_hash.final(&keys_digest);
        var values_digest: [32]u8 = undefined;
        values_hash.final(&values_digest);
        const keys_hex = std.fmt.bytesToHex(keys_digest, .lower);
        const values_hex = std.fmt.bytesToHex(values_digest, .lower);
        const artifact_hex = std.fmt.bytesToHex(self.identity.artifact, .lower);
        const tokenizer_hex = std.fmt.bytesToHex(self.identity.tokenizer, .lower);
        const template_hex = std.fmt.bytesToHex(self.identity.template, .lower);
        const layout_hex = std.fmt.bytesToHex(self.identity.layout, .lower);
        const implementation_hex = std.fmt.bytesToHex(self.identity.runtime, .lower);
        const prompt_hex = std.fmt.bytesToHex(self.prompt_tokens_sha256, .lower);
        const document_hex = std.fmt.bytesToHex(self.document_sha256, .lower);
        const binary_hex = std.fmt.bytesToHex(self.provenance.binary_sha256, .lower);
        const source_hex = std.fmt.bytesToHex(self.provenance.source_build_sha256, .lower);
        var consumers: [256]Consumer = undefined;
        var consumer_count: usize = 0;
        var strata: [128]Stratum = undefined;
        var strata_count: usize = 0;
        for (self.layout.layers[0..self.layout.len], 0..) |consumer, consumer_layer| {
            if (consumer.owner != block.layer) continue;
            if (consumer.kv_heads != block.heads or consumer.head_dim != block.dim or
                consumer.query_heads == 0 or consumer.query_heads % consumer.kv_heads != 0 or
                consumer_count >= consumers.len)
                return error.InvalidCampaignCapture;
            consumers[consumer_count] = .{ .layer = @intCast(consumer_layer), .query_heads = consumer.query_heads };
            consumer_count += 1;
            const groups = consumer.query_heads / consumer.kv_heads;
            for (0..consumer.query_heads) |query_head| {
                if (strata_count >= strata.len) return error.InvalidCampaignCapture;
                strata[strata_count] = .{ .consumer_layer = @intCast(consumer_layer), .query_head = @intCast(query_head), .kv_head = @intCast(query_head / groups) };
                strata_count += 1;
            }
        }
        if (consumer_count == 0 or strata_count == 0) return error.InvalidCampaignCapture;
        const descriptor = try std.json.Stringify.valueAlloc(self.allocator, .{
            .schema_version = @as(u32, 1),
            .stage = "context_packed_exact_block_capture",
            .completed = true,
            .prefix_storage = "packed_exact",
            .prefix_kernel_route = "metal_context_flash_prefill_abi2",
            .owner_layer = block.layer,
            .block_start = block.logical_start,
            .rows = block.rows,
            .heads = block.heads,
            .dim = block.dim,
            .scale = 1.0 / @sqrt(@as(f32, @floatFromInt(block.dim))),
            .consumers = consumers[0..consumer_count],
            .strata = strata[0..strata_count],
            .tensor_order = "logical_row_kv_head_head_dim",
            .tensor_dtype = "f16_le",
            .keys = .{ .path = keys_path, .sha256 = keys_hex[0..] },
            .values = .{ .path = values_path, .sha256 = values_hex[0..] },
            .prompt = .{ .tokens = self.prompt_tokens, .sha256 = prompt_hex[0..] },
            .document = .{ .id = self.document_id, .sha256 = document_hex[0..], .bucket = self.document_bucket },
            .loaded_identity = .{ .load_generation = load_generation, .artifact_sha256 = artifact_hex[0..], .tokenizer_sha256 = tokenizer_hex[0..], .template_sha256 = template_hex[0..], .layout_sha256 = layout_hex[0..], .implementation_sha256 = implementation_hex[0..] },
            .provenance = .{ .binary_sha256 = binary_hex[0..], .source_build_sha256 = source_hex[0..] },
            .kv_export_readback_bytes = bytes,
        }, .{});
        defer self.allocator.free(descriptor);
        const descriptor_name = try std.fmt.allocPrint(self.allocator, "base-layer{d}-block{d}.json", .{ block.layer, block.logical_start });
        defer self.allocator.free(descriptor_name);
        try self.writeFile(descriptor_name, descriptor);
        return .{ .readback_bytes = bytes, .descriptor_path = try self.artifactName(descriptor_name), .descriptor_sha256 = digest(descriptor) };
    }

    const ArtifactEntry = struct {
        kind: []const u8,
        source_id: ?[]const u8,
        owner_layer: ?u32,
        path: []u8,
        sha256: [32]u8,
    };
    const ArtifactJson = struct {
        kind: []const u8,
        source_id: ?[]const u8,
        owner_layer: ?u32,
        path: []const u8,
        sha256: []const u8,
    };

    fn writeArtifactIndex(self: *Capture, artifacts: []const ArtifactEntry) ![32]u8 {
        if (artifacts.len == 0) return error.InvalidCampaignCapture;
        const hexes = try self.allocator.alloc([64]u8, artifacts.len);
        defer self.allocator.free(hexes);
        const rows = try self.allocator.alloc(ArtifactJson, artifacts.len);
        defer self.allocator.free(rows);
        for (artifacts, rows, hexes) |artifact, *row, *hex| {
            hex.* = std.fmt.bytesToHex(artifact.sha256, .lower);
            row.* = .{ .kind = artifact.kind, .source_id = artifact.source_id, .owner_layer = artifact.owner_layer, .path = artifact.path, .sha256 = hex[0..] };
        }
        const index = try std.json.Stringify.valueAlloc(self.allocator, .{
            .schema_version = @as(u32, 1),
            .stage = "context_query_campaign_artifact_index",
            .completed = true,
            .artifacts = rows,
        }, .{});
        defer self.allocator.free(index);
        try self.writeFile("artifact-index.json", index);
        return digest(index);
    }

    fn writeTokens(self: *Capture, source: Source) !ArtifactEntry {
        const bytes = std.mem.sliceAsBytes(source.tokens);
        const suffix = try std.fmt.allocPrint(self.allocator, "{s}-tokens.i64", .{source.id});
        defer self.allocator.free(suffix);
        try self.writeFile(suffix, bytes);
        return .{ .kind = "source_tokens", .source_id = source.id, .owner_layer = null, .path = try self.artifactName(suffix), .sha256 = digest(bytes) };
    }

    const SourceExport = struct { query_bytes: usize, artifacts: [3]ArtifactEntry };

    fn writeSource(self: *Capture, campaign: *const campaign_mod.Campaign, layer: u32, source: Source) !SourceExport {
        const captured = try campaign.captureSourceObservation(layer, source.source_index);
        const geometry = captured.geometry;
        const count = geometry.rows_per_source_stratum;
        if (geometry.heads == 0 or geometry.dim == 0 or geometry.strata == 0 or count == 0 or
            captured.queries.len != @as(usize, geometry.heads) * geometry.stride_rows * geometry.dim or
            geometry.positions.len != @as(usize, geometry.heads) * geometry.stride_rows)
            return error.InvalidCampaignCapture;
        const query_name = try std.fmt.allocPrint(self.allocator, "{s}-layer{d}-queries.f32", .{ source.id, layer });
        defer self.allocator.free(query_name);
        const position_name = try std.fmt.allocPrint(self.allocator, "{s}-layer{d}-positions.u32", .{ source.id, layer });
        defer self.allocator.free(position_name);
        const query_path = try self.artifactName(query_name);
        errdefer self.allocator.free(query_path);
        const position_path = try self.artifactName(position_name);
        errdefer self.allocator.free(position_path);
        var query_file = try self.create(query_name);
        defer query_file.close(self.io);
        var position_file = try self.create(position_name);
        defer position_file.close(self.io);
        var query_hash = std.crypto.hash.sha2.Sha256.init(.{});
        var position_hash = std.crypto.hash.sha2.Sha256.init(.{});
        var rows: usize = 0;
        for (0..geometry.heads) |head| {
            for (0..geometry.strata) |stratum| {
                const row = @as(usize, @intCast(head)) * geometry.stride_rows + geometry.partition_offset +
                    @as(usize, @intCast(stratum)) * geometry.partition_source_count * count + geometry.source_local_index * count;
                const positions = geometry.positions[row..][0..count];
                const logical_end = try std.math.add(u32, source.logical_start, @intCast(source.tokens.len));
                for (positions) |position| if (position < source.logical_start or position >= logical_end)
                    return error.InvalidCampaignCapture;
                const queries = captured.queries[row * geometry.dim ..][0 .. @as(usize, count) * geometry.dim];
                for (queries) |value| if (!std.math.isFinite(value)) return error.InvalidCampaignCapture;
                try query_file.writeStreamingAll(self.io, std.mem.sliceAsBytes(queries));
                try position_file.writeStreamingAll(self.io, std.mem.sliceAsBytes(positions));
                query_hash.update(std.mem.sliceAsBytes(queries));
                position_hash.update(std.mem.sliceAsBytes(positions));
                rows = try std.math.add(usize, rows, count);
            }
        }
        try query_file.sync(self.io);
        try position_file.sync(self.io);
        var query_digest: [32]u8 = undefined;
        query_hash.final(&query_digest);
        var position_digest: [32]u8 = undefined;
        position_hash.final(&position_digest);
        const artifact_hex = std.fmt.bytesToHex(self.identity.artifact, .lower);
        const tokenizer_hex = std.fmt.bytesToHex(self.identity.tokenizer, .lower);
        const template_hex = std.fmt.bytesToHex(self.identity.template, .lower);
        const layout_hex = std.fmt.bytesToHex(self.identity.layout, .lower);
        const implementation_hex = std.fmt.bytesToHex(self.identity.runtime, .lower);
        const contract_hex = std.fmt.bytesToHex(self.generation_contract_sha256, .lower);
        const token_hex = std.fmt.bytesToHex(tokenDigest(source.tokens), .lower);
        const query_hex = std.fmt.bytesToHex(query_digest, .lower);
        const position_hex = std.fmt.bytesToHex(position_digest, .lower);
        const physical_branch_id: []const u8 = switch (source.receipt.physical_branch_id) {
            0 => "question",
            1 => "fit-study",
            2 => "gate-study",
            else => return error.InvalidCampaignCapture,
        };
        const active_sources: u32 = if (source.receipt.physical_branch_id == 0) 1 else 2;
        const study_pair_id: ?[]const u8 = if (source.source_index == 1 or source.source_index == 2)
            "fit-study"
        else if (source.source_index == 3 or source.source_index == 4)
            "gate-study"
        else
            null;
        const metadata = try std.json.Stringify.valueAlloc(self.allocator, .{
            .source_id = source.id,
            .kind = source.kind,
            .partition = source.partition,
            .capture_route = "private_model_replay",
            .campaign_route = "production_query_campaign",
            .conditioning = source.conditioning,
            .physical_branch_id = physical_branch_id,
            .study_pair_id = study_pair_id,
            .generation_contract_sha256 = if (source.source_index == 2 or source.source_index == 4) contract_hex[0..] else null,
            .generation_mode = if (source.source_index == 2 or source.source_index == 4) "greedy_fixed_length_ignore_eos" else null,
            .prefix_storage = "packed_exact",
            .private_kernel_route = @tagName(source.receipt.route),
            .loaded_identity = .{ .load_generation = campaign.load_generation, .artifact_sha256 = artifact_hex[0..], .tokenizer_sha256 = tokenizer_hex[0..], .template_sha256 = template_hex[0..], .layout_sha256 = layout_hex[0..], .implementation_sha256 = implementation_hex[0..] },
            .tokens_sha256 = token_hex[0..],
            .queries_sha256 = query_hex[0..],
            .positions_sha256 = position_hex[0..],
            .logical_start = source.logical_start,
            .logical_count = source.tokens.len,
            .branch_logical_start = source.branch_start,
            .branch_logical_count = source.branch_count,
            .branch_rows_per_stratum = count * active_sources,
            .owner_layer = layer,
            .heads = geometry.heads,
            .dim = geometry.dim,
            .rows_per_head = count * geometry.strata,
            .rows_per_stratum = count,
            .physical_bytes = source.receipt.physical_bytes,
            .peak_bytes = source.receipt.peak_bytes,
            .query_bytes = source.receipt.query_bytes,
            .replay_split_scratch_bytes = source.receipt.replay_split_scratch_bytes,
            .serial_attention_dispatches = source.receipt.serial_attention_dispatches,
            .split_attention_dispatches = source.receipt.split_attention_dispatches,
            .elapsed_ns = source.receipt.elapsed_ns,
            .status_readback_bytes = source.receipt.status_readback_bytes,
            .kv_readback_bytes = @as(u64, 0),
            .completed = true,
            .cancelled = false,
        }, .{});
        defer self.allocator.free(metadata);
        const metadata_name = try std.fmt.allocPrint(self.allocator, "{s}-layer{d}-source.json", .{ source.id, layer });
        defer self.allocator.free(metadata_name);
        try self.writeFile(metadata_name, metadata);
        const metadata_path = try self.artifactName(metadata_name);
        return .{ .query_bytes = rows * geometry.dim * @sizeOf(f32), .artifacts = .{
            .{ .kind = "source_descriptor", .source_id = source.id, .owner_layer = layer, .path = metadata_path, .sha256 = digest(metadata) },
            .{ .kind = "source_queries", .source_id = source.id, .owner_layer = layer, .path = query_path, .sha256 = query_digest },
            .{ .kind = "source_positions", .source_id = source.id, .owner_layer = layer, .path = position_path, .sha256 = position_digest },
        } };
    }

    fn create(self: *Capture, suffix: []const u8) !std.Io.File {
        if (!validStem(self.stem) or !validStem(suffix)) return error.InvalidCampaignCapturePath;
        const name = try std.fmt.allocPrint(self.allocator, "{s}-{s}", .{ self.stem, suffix });
        defer self.allocator.free(name);
        return self.directory.createFile(self.io, name, .{ .exclusive = true });
    }

    fn artifactName(self: *Capture, suffix: []const u8) ![]u8 {
        if (!validStem(self.stem) or !validStem(suffix)) return error.InvalidCampaignCapturePath;
        return std.fmt.allocPrint(self.allocator, "{s}-{s}", .{ self.stem, suffix });
    }

    fn writeFile(self: *Capture, suffix: []const u8, bytes: []const u8) !void {
        var file = try self.create(suffix);
        defer file.close(self.io);
        try file.writeStreamingAll(self.io, bytes);
        try file.sync(self.io);
    }
};

fn requireQuestionProviderEvidence(
    elapsed_ns: u64,
    provider_delta: ?coordinator.StudyProviderDelta,
) !coordinator.StudyProviderDelta {
    if (elapsed_ns == 0) return error.CampaignCaptureMissingQuestionTiming;
    const provider = provider_delta orelse return error.CampaignCaptureMissingQuestionProviderDelta;
    if (provider.frame_submits > provider.frame_begins or
        provider.to_host_device_calls > provider.to_host_calls)
        return error.CampaignCaptureInvalidQuestionProviderDelta;
    return provider;
}

fn tokenDigest(tokens: []const i64) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(std.mem.sliceAsBytes(tokens));
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}

const Source = struct {
    id: []const u8,
    kind: []const u8,
    partition: []const u8,
    conditioning: []const u8,
    tokens: []const i64,
    logical_start: u32,
    branch_start: u32,
    branch_count: u32,
    receipt: coordinator.CampaignBranchReceipt,
    source_index: u32,
};

fn validStem(value: []const u8) bool {
    if (value.len == 0 or value.len > 128) return false;
    for (value) |c| if (!(std.ascii.isAlphanumeric(c) or c == '-' or c == '_' or c == '.')) return false;
    return true;
}

fn nativeRepeatTestLayout() policy.Layout {
    var layout = policy.Layout{ .len = 3 };
    layout.layers[0] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 8, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10_000 };
    layout.layers[1] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 4, .head_dim = 64, .rope_active_dim = 64, .rope_frequency_dim = 64, .rope_theta = 10_000 };
    layout.layers[2] = .{ .owner = 2, .sliding_window = 0, .kv_heads = 1, .query_heads = 1, .head_dim = 32, .rope_active_dim = 32, .rope_frequency_dim = 32, .rope_theta = 10_000 };
    return layout;
}

fn nativeRepeatHeadStrataAllocationCase(allocator: std.mem.Allocator) !void {
    var strata: std.ArrayList(Capture.Stratum) = .empty;
    defer strata.deinit(allocator);
    try Capture.appendNativeRepeatHeadStrata(allocator, nativeRepeatTestLayout(), 0, 1, &strata);
    try std.testing.expectEqual(@as(usize, 6), strata.items.len);
}

test "context native repeat export derives distinct head-local shared-consumer GQA strata" {
    const layout = nativeRepeatTestLayout();

    var head0: std.ArrayList(Capture.Stratum) = .empty;
    defer head0.deinit(std.testing.allocator);
    var head1: std.ArrayList(Capture.Stratum) = .empty;
    defer head1.deinit(std.testing.allocator);
    try Capture.appendNativeRepeatHeadStrata(std.testing.allocator, layout, 0, 0, &head0);
    try Capture.appendNativeRepeatHeadStrata(std.testing.allocator, layout, 0, 1, &head1);

    const expected0 = [_]Capture.Stratum{
        .{ .consumer_layer = 0, .query_head = 0, .kv_head = 0 },
        .{ .consumer_layer = 0, .query_head = 1, .kv_head = 0 },
        .{ .consumer_layer = 0, .query_head = 2, .kv_head = 0 },
        .{ .consumer_layer = 0, .query_head = 3, .kv_head = 0 },
        .{ .consumer_layer = 1, .query_head = 0, .kv_head = 0 },
        .{ .consumer_layer = 1, .query_head = 1, .kv_head = 0 },
    };
    const expected1 = [_]Capture.Stratum{
        .{ .consumer_layer = 0, .query_head = 4, .kv_head = 1 },
        .{ .consumer_layer = 0, .query_head = 5, .kv_head = 1 },
        .{ .consumer_layer = 0, .query_head = 6, .kv_head = 1 },
        .{ .consumer_layer = 0, .query_head = 7, .kv_head = 1 },
        .{ .consumer_layer = 1, .query_head = 2, .kv_head = 1 },
        .{ .consumer_layer = 1, .query_head = 3, .kv_head = 1 },
    };
    try std.testing.expectEqualSlices(Capture.Stratum, &expected0, head0.items);
    try std.testing.expectEqualSlices(Capture.Stratum, &expected1, head1.items);
    try std.testing.expectError(error.InvalidCampaignCapture, Capture.appendNativeRepeatHeadStrata(std.testing.allocator, layout, 0, 2, &head1));
}

test "context native repeat export frees partial head-local strata on allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, nativeRepeatHeadStrataAllocationCase, .{});
}

test "context campaign capture requires valid question provider evidence" {
    const valid = coordinator.StudyProviderDelta{
        .frame_begins = 2,
        .frame_submits = 1,
        .frame_wait_ns = 0,
        .frame_gpu_ns = 0,
        .compute_encoders = 0,
        .blit_encoders = 0,
        .precomputed_tail_blocks = 0,
        .precomputed_tail_failures = 0,
        .paged_attention_1x_calls = 0,
        .context_session_paged_decode_framed_dispatches = 0,
        .decode_gqa_split_calls = 0,
        .generated_attention_decode_1x_calls = 0,
        .generated_attention_flash_prefill_calls = 0,
        .attention_prefill_direct_kv_calls = 0,
        .attention_prefill_paged_kv_calls = 0,
        .to_host_calls = 1,
        .to_host_device_calls = 1,
        .host_download_bytes = 0,
    };
    try std.testing.expectEqual(valid, try requireQuestionProviderEvidence(1, valid));
    try std.testing.expectError(error.CampaignCaptureMissingQuestionTiming, requireQuestionProviderEvidence(0, valid));
    try std.testing.expectError(error.CampaignCaptureMissingQuestionProviderDelta, requireQuestionProviderEvidence(1, null));
    var invalid_frames = valid;
    invalid_frames.frame_submits = invalid_frames.frame_begins + 1;
    try std.testing.expectError(error.CampaignCaptureInvalidQuestionProviderDelta, requireQuestionProviderEvidence(1, invalid_frames));
    var invalid_downloads = valid;
    invalid_downloads.to_host_device_calls = invalid_downloads.to_host_calls + 1;
    try std.testing.expectError(error.CampaignCaptureInvalidQuestionProviderDelta, requireQuestionProviderEvidence(1, invalid_downloads));
}
