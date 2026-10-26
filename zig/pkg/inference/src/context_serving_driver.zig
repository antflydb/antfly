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

//! Test-binary-only persistent context qualification route owner. This module
//! executes genuine generated HTTP routes and returns the request-local server
//! observation only after the handler has released every request owner.
const std = @import("std");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const server = @import("server/server.zig");
const Loopback = @import("context_serving_loopback.zig").Loopback;
const coverage_mod = @import("context_capture_coverage.zig");
const host_allocation_tracker = @import("runtime/kv/host_allocation_tracker.zig");
const teacher_forced = @import("backends/context_teacher_forced.zig");
const cli_process = @import("context_serving_cli_process.zig");
const context_policy = @import("runtime/context_policy.zig");
const session_factory = @import("architectures/session_factory.zig");
const model_manager = @import("server/model_manager.zig");
const ordered_messages = @import("context_serving_messages.zig");
const quality_materializer = @import("context_quality_materializer.zig");
const compaction_profile = @import("runtime/kv/compaction_profile.zig");

fn resolveGeneratorModelDir(node: *server.Node, io: std.Io, model_name: []const u8) ![]const u8 {
    return node.resolveModelPath(io, model_name, "generators");
}

pub const Variant = enum { exact, packed_exact, attention_matching };
pub const Route = enum { native, cli, http, stream, serial_mixed_batch };
/// Qualification must render with the same explicit template setting used by
/// every serving request body below. A default option is not an identity.
pub const qualification_enable_thinking = false;

pub const LoadedProvenanceEvidence = struct {
    exported: model_manager.LoadedModel.CompactionProvenanceExport,
    loaded_identity: server.ContextServingLoadedIdentity,
    profile_semantic_sha256: [32]u8,

    pub fn deinit(self: *LoadedProvenanceEvidence) void {
        self.exported.deinit();
        self.* = undefined;
    }
};

pub const MaterializationRequest = struct {
    model: []const u8,
    ordered: OrderedInput,
    policy: context_policy.Policy,
    reserved_output_tokens: u32,
    optimize_decode: bool,
    optimize_prefill: bool,
    fit_engine: compaction_profile.FitEngine = .block_v1,
    reference_utf8: []const u8,
};

pub const MaterializationEvidence = struct {
    materialized: quality_materializer.Result,
    loaded_identity: server.ContextServingLoadedIdentity,

    pub fn deinit(self: *MaterializationEvidence) void {
        self.materialized.deinit();
        self.* = undefined;
    }
};

pub const Request = struct {
    route: Route,
    variant: Variant,
    model: []const u8,
    prompt: []const u8,
    /// Renderer-owned structured input used by every qualification command.
    /// The document is the only attributed compaction target; the question is
    /// a distinct, non-selected part. Plain `prompt` remains only for legacy
    /// exact diagnostics and can never authorize attention matching.
    structured: ?StructuredInput = null,
    /// Versioned ordered messages for genuine multi-turn qualification. The
    /// selected bits are converted to context-policy targets and never sent as
    /// caller-controlled implementation flags.
    ordered: ?OrderedInput = null,
    max_context_tokens: u32,
    max_kv_tokens: u32,
    keep_prefix_tokens: u32,
    keep_recent_tokens: u32,
    reserved_output_tokens: u32,
    optimize_decode: bool = false,
    optimize_prefill: bool = false,
    /// Frozen by the loaded qualification profile. This is deliberately not
    /// represented in the public request body or JSON command protocol.
    fit_engine: compaction_profile.FitEngine = .block_v1,
    admitted_prefill_chunk_rows: u32 = 0,
    coverage: ?CoverageRequest = null,
    /// Preserve the public and private text carried by the actual selected
    /// serving route. The sealed reference is held by the driver only for the
    /// post-publication tokenizer round-trip and private teacher sibling.
    capture_quality_output: bool = false,
    quality: ?QualityRequest = null,
};

pub const QualityRequest = struct {
    reference_utf8: []const u8,
    reference_tokens: []const i64,
    reference_sequence_sha256: [32]u8,
    rendered_prompt_sha256: [32]u8,
    profile_artifact_sha256: [32]u8,
    profile_semantic_sha256: [32]u8,
    implementation_sha256: [32]u8,
    loaded_generation: u64 = 0,

    fn reveal(raw: *anyopaque) anyerror![]const i64 {
        const self: *const QualityRequest = @ptrCast(@alignCast(raw));
        return self.reference_tokens;
    }

    fn request(
        self: *const QualityRequest,
        variant: Variant,
        load_generation: u64,
        mode: teacher_forced.Mode,
        sibling: ?teacher_forced.ContextBinding,
    ) !teacher_forced.Request {
        const reference_digest = try teacher_forced.referenceSequenceDigest(self.reference_tokens);
        if (variant == .packed_exact or self.reference_tokens.len < teacher_forced.min_reference_tokens or
            self.reference_tokens.len > teacher_forced.max_reference_tokens or load_generation == 0 or
            !std.mem.eql(u8, &reference_digest, &self.reference_sequence_sha256))
            return error.InvalidQualityReferenceTokens;
        return .{
            .mode = mode,
            .variant = if (variant == .exact) .exact else .attention_matching,
            .reference_token_count = @intCast(self.reference_tokens.len),
            .reference_sequence_sha256 = self.reference_sequence_sha256,
            .rendered_prompt_sha256 = self.rendered_prompt_sha256,
            .loaded_generation = load_generation,
            .profile_artifact_sha256 = self.profile_artifact_sha256,
            .profile_semantic_sha256 = self.profile_semantic_sha256,
            .implementation_sha256 = self.implementation_sha256,
            .generation_sibling = sibling,
            .reference_provider = if (mode == .teacher) .{ .ctx = @ptrCast(@constCast(self)), .reveal_fn = reveal } else null,
        };
    }
};

fn requireExactQualityReferenceBytes(sealed: []const u8, decoded: []const u8) !void {
    if (sealed.len == 0 or !std.mem.eql(u8, sealed, decoded))
        return error.QualityReferenceTokenizerMismatch;
}

pub const StructuredInput = struct {
    document: []const u8,
    question: []const u8,
};

pub const OrderedInput = struct {
    artifact: ordered_messages.Artifact,
    semantic_sha256: [32]u8,
};

pub const CoverageRequest = struct {
    request: coverage_mod.RequestBinding,
    document: coverage_mod.Document,
    document_text: []const u8,
    question_text: []const u8,
    provenance: coverage_mod.Provenance,
};

pub const MixedSibling = struct {
    variant: Variant,
    structured: StructuredInput,
    reserved_output_tokens: u32,
};

pub const BatchAllocationEvidence = struct {
    member_request_indices: []usize,
    receipt: host_allocation_tracker.RequestAllocationOwner.ReleaseReceipt,
};

pub const BatchItemEvidence = struct {
    request_index: usize,
    observation: server.ContextServingObservation,
    elapsed_ns: u64,
    ttft_ns: u64,
    decode_ns: u64,
};

pub const BatchEvidence = struct {
    allocator: std.mem.Allocator,
    status: u16,
    content_type: []u8,
    body: []u8,
    allocations: []BatchAllocationEvidence,
    items: []BatchItemEvidence,

    pub fn deinit(self: *BatchEvidence) void {
        for (self.allocations) |value| self.allocator.free(value.member_request_indices);
        self.allocator.free(self.allocations);
        self.allocator.free(self.items);
        self.allocator.free(self.content_type);
        self.allocator.free(self.body);
        self.* = undefined;
    }
};

pub const ReloadEvidence = struct {
    loaded_generation_before: u64,
    loaded_generation_after: u64,
    stale_selection_rejected: bool,
};

pub const Evidence = struct {
    allocator: std.mem.Allocator,
    status: u16,
    content_type: []u8,
    body: []u8,
    observation: server.ContextServingObservation,
    elapsed_ns: u64 = 0,
    ttft_ns: u64 = 0,
    decode_ns: u64 = 0,
    coverage: ?coverage_mod.Evidence = null,
    output_text: ?[]u8 = null,
    reasoning_text: ?[]u8 = null,
    teacher_forced_receipt: ?teacher_forced.Receipt = null,
    teacher_observation: ?server.ContextServingObservation = null,
    cli_process: ?cli_process.Receipt = null,

    pub fn deinit(self: *Evidence) void {
        if (self.coverage) |*value| value.deinit();
        if (self.output_text) |value| self.allocator.free(value);
        if (self.reasoning_text) |value| self.allocator.free(value);
        if (self.teacher_forced_receipt) |*receipt| receipt.deinit();
        if (self.cli_process) |*receipt| receipt.deinit();
        self.allocator.free(self.content_type);
        self.allocator.free(self.body);
        self.* = undefined;
    }
};

const QualityText = struct {
    output: []u8,
    reasoning: []u8,

    fn deinit(self: *QualityText, allocator: std.mem.Allocator) void {
        allocator.free(self.output);
        allocator.free(self.reasoning);
        self.* = undefined;
    }
};

fn objectField(value: std.json.Value, name: []const u8) !std.json.Value {
    if (value != .object) return error.InvalidServingDriverQualityResponse;
    return value.object.get(name) orelse error.InvalidServingDriverQualityResponse;
}

fn optionalTextField(value: std.json.Value, name: []const u8) ![]const u8 {
    if (value != .object) return error.InvalidServingDriverQualityResponse;
    const field = value.object.get(name) orelse return "";
    return switch (field) {
        .null => "",
        .string => |text| text,
        else => error.InvalidServingDriverQualityResponse,
    };
}

fn requiredTextField(value: std.json.Value, name: []const u8) ![]const u8 {
    if (value != .object) return error.InvalidServingDriverQualityResponse;
    const field = value.object.get(name) orelse return error.InvalidServingDriverQualityResponse;
    return switch (field) {
        .string => |text| text,
        else => error.InvalidServingDriverQualityResponse,
    };
}

fn parseQualityHttpResponse(allocator: std.mem.Allocator, bytes: []const u8) !QualityText {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    const choices = try objectField(parsed.value, "choices");
    if (choices != .array or choices.array.items.len != 1) return error.InvalidServingDriverQualityResponse;
    const message = try objectField(choices.array.items[0], "message");
    const output = try requiredTextField(message, "content");
    const reasoning = try optionalTextField(message, "reasoning_content");
    const owned_output = try allocator.dupe(u8, output);
    errdefer allocator.free(owned_output);
    return .{ .output = owned_output, .reasoning = try allocator.dupe(u8, reasoning) };
}

fn parseQualityStreamResponse(allocator: std.mem.Allocator, bytes: []const u8) !QualityText {
    var output: std.Io.Writer.Allocating = .init(allocator);
    defer output.deinit();
    var reasoning: std.Io.Writer.Allocating = .init(allocator);
    defer reasoning.deinit();
    var saw_done = false;
    var saw_finish = false;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trimEnd(u8, raw_line, "\r");
        if (line.len == 0) continue;
        if (!std.mem.startsWith(u8, line, "data: ")) return error.InvalidServingDriverQualityResponse;
        const payload = line[6..];
        if (std.mem.eql(u8, payload, "[DONE]")) {
            if (saw_done) return error.InvalidServingDriverQualityResponse;
            saw_done = true;
            continue;
        }
        if (saw_done) return error.InvalidServingDriverQualityResponse;
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, payload, .{});
        defer parsed.deinit();
        const choices = try objectField(parsed.value, "choices");
        if (choices != .array or choices.array.items.len > 1) return error.InvalidServingDriverQualityResponse;
        if (choices.array.items.len == 0) continue;
        const choice = choices.array.items[0];
        const delta = try objectField(choice, "delta");
        if (delta != .object) return error.InvalidServingDriverQualityResponse;
        if (delta.object.get("tool_calls")) |tool_calls|
            if (tool_calls != .null) return error.InvalidServingDriverQualityResponse;
        try output.writer.writeAll(try optionalTextField(delta, "content"));
        try reasoning.writer.writeAll(try optionalTextField(delta, "reasoning_content"));
        if (choice != .object) return error.InvalidServingDriverQualityResponse;
        if (choice.object.get("finish_reason")) |finish| switch (finish) {
            .null => {},
            .string => {
                if (saw_finish) return error.InvalidServingDriverQualityResponse;
                saw_finish = true;
            },
            else => return error.InvalidServingDriverQualityResponse,
        };
    }
    if (!saw_done or !saw_finish) return error.IncompleteServingDriverStream;
    const owned_output = try output.toOwnedSlice();
    errdefer allocator.free(owned_output);
    return .{ .output = owned_output, .reasoning = try reasoning.toOwnedSlice() };
}

const CoverageMailbox = struct {
    mutex: std.atomic.Mutex = .unlocked,
    value: ?coverage_mod.Evidence = null,

    fn lock(self: *CoverageMailbox) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn reset(self: *CoverageMailbox) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.value != null) @panic("unclaimed context coverage evidence");
    }

    fn complete(raw: *anyopaque, evidence: coverage_mod.Evidence) void {
        const self: *CoverageMailbox = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (self.value != null) @panic("duplicate context coverage evidence");
        self.value = evidence;
    }

    fn take(self: *CoverageMailbox) !coverage_mod.Evidence {
        self.lock();
        defer self.mutex.unlock();
        const value = self.value orelse return error.MissingContextCoverageEvidence;
        self.value = null;
        return value;
    }

    fn discard(self: *CoverageMailbox) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.value) |*value| value.deinit();
        self.value = null;
    }

    fn deinit(self: *CoverageMailbox) void {
        self.discard();
    }
};

const Mailbox = struct {
    mutex: std.atomic.Mutex = .unlocked,
    value: ?server.ContextServingObservation = null,

    fn lock(self: *Mailbox) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn reset(self: *Mailbox) void {
        self.lock();
        defer self.mutex.unlock();
        self.value = null;
    }

    fn complete(raw: *anyopaque, observation: server.ContextServingObservation) void {
        const self: *Mailbox = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        // A serial command owns exactly one serving observation. Duplicate
        // completion is a route-integrity failure, never last-writer-wins.
        if (self.value != null) @panic("duplicate context serving observation");
        self.value = observation;
    }

    fn take(self: *Mailbox) !server.ContextServingObservation {
        self.lock();
        defer self.mutex.unlock();
        const value = self.value orelse return error.MissingContextServingObservation;
        self.value = null;
        return value;
    }
};

const max_batch_allocation_groups = 256;
const max_batch_allocation_members = 256;

const StoredBatchAllocation = struct {
    member_request_indices: [max_batch_allocation_members]usize = @splat(0),
    member_count: usize = 0,
    receipt: host_allocation_tracker.RequestAllocationOwner.ReleaseReceipt = .{},
};

const BatchMailbox = struct {
    mutex: std.atomic.Mutex = .unlocked,
    values: [max_batch_allocation_groups]StoredBatchAllocation = @splat(.{}),
    count: usize = 0,
    invalid: bool = false,

    fn lock(self: *BatchMailbox) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn reset(self: *BatchMailbox) void {
        self.lock();
        defer self.mutex.unlock();
        self.count = 0;
        self.invalid = false;
    }

    fn complete(raw: *anyopaque, observation: server.ContextBatchAllocationObservation) void {
        const self: *BatchMailbox = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (self.count >= self.values.len or observation.member_request_indices.len > max_batch_allocation_members) {
            self.invalid = true;
            return;
        }
        const target = &self.values[self.count];
        target.* = .{ .member_count = observation.member_request_indices.len, .receipt = observation.receipt };
        @memcpy(target.member_request_indices[0..target.member_count], observation.member_request_indices);
        self.count += 1;
    }

    fn take(self: *BatchMailbox, allocator: std.mem.Allocator) ![]BatchAllocationEvidence {
        self.lock();
        defer self.mutex.unlock();
        if (self.invalid or self.count == 0) return error.MissingContextBatchAllocationObservation;
        const result = try allocator.alloc(BatchAllocationEvidence, self.count);
        errdefer allocator.free(result);
        var initialized: usize = 0;
        errdefer for (result[0..initialized]) |value| allocator.free(value.member_request_indices);
        for (self.values[0..self.count], result) |value, *target| {
            target.* = .{
                .member_request_indices = try allocator.dupe(usize, value.member_request_indices[0..value.member_count]),
                .receipt = value.receipt,
            };
            initialized += 1;
        }
        self.count = 0;
        return result;
    }
};

const BatchItemMailbox = struct {
    mutex: std.atomic.Mutex = .unlocked,
    values: [max_batch_allocation_members]BatchItemEvidence = undefined,
    count: usize = 0,
    invalid: bool = false,

    fn lock(self: *BatchItemMailbox) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn reset(self: *BatchItemMailbox) void {
        self.lock();
        defer self.mutex.unlock();
        self.count = 0;
        self.invalid = false;
    }

    fn complete(raw: *anyopaque, value: server.ContextBatchServingObservation) void {
        const self: *BatchItemMailbox = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (self.count >= self.values.len) {
            self.invalid = true;
            return;
        }
        for (self.values[0..self.count]) |existing| {
            if (existing.request_index != value.request_index) continue;
            self.invalid = true;
            return;
        }
        self.values[self.count] = .{
            .request_index = value.request_index,
            .observation = value.observation,
            .elapsed_ns = value.elapsed_ns,
            .ttft_ns = value.ttft_ns,
            .decode_ns = value.decode_ns,
        };
        self.count += 1;
    }

    fn take(self: *BatchItemMailbox, allocator: std.mem.Allocator, expected: usize) ![]BatchItemEvidence {
        self.lock();
        defer self.mutex.unlock();
        if (self.invalid or self.count != expected) return error.IncompleteContextBatchServingObservations;
        const result = try allocator.dupe(BatchItemEvidence, self.values[0..self.count]);
        self.count = 0;
        std.mem.sort(BatchItemEvidence, result, {}, struct {
            fn lessThan(_: void, lhs: BatchItemEvidence, rhs: BatchItemEvidence) bool {
                return lhs.request_index < rhs.request_index;
            }
        }.lessThan);
        for (result, 0..) |value, index|
            if (value.request_index != index) {
                allocator.free(result);
                return error.ContextBatchMemberMismatch;
            };
        return result;
    }
};

const FailureMailbox = struct {
    mutex: std.atomic.Mutex = .unlocked,
    value: ?server.ContextServingFailureObservation = null,
    invalid: bool = false,

    fn lock(self: *FailureMailbox) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn reset(self: *FailureMailbox) void {
        self.lock();
        defer self.mutex.unlock();
        self.value = null;
        self.invalid = false;
    }
    fn complete(raw: *anyopaque, observation: server.ContextServingFailureObservation) void {
        const self: *FailureMailbox = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (self.value != null) {
            self.invalid = true;
            return;
        }
        self.value = observation;
    }
    fn take(self: *FailureMailbox) !server.ContextServingFailureObservation {
        self.lock();
        defer self.mutex.unlock();
        if (self.invalid) return error.DuplicateContextFailureObservation;
        const result = self.value orelse return error.MissingContextFailureObservation;
        self.value = null;
        return result;
    }
};

pub const Driver = struct {
    allocator: std.mem.Allocator,
    node: *server.Node,
    loopback: *Loopback,
    mailbox: Mailbox = .{},
    batch_mailbox: BatchMailbox = .{},
    batch_item_mailbox: BatchItemMailbox = .{},
    coverage_mailbox: CoverageMailbox = .{},
    failure_mailbox: FailureMailbox = .{},

    pub fn init(allocator: std.mem.Allocator, node: *server.Node) !*Driver {
        const result = try allocator.create(Driver);
        errdefer allocator.destroy(result);
        result.* = .{
            .allocator = allocator,
            .node = node,
            .loopback = try Loopback.init(allocator, node),
        };
        errdefer result.loopback.deinit();
        result.node.context_serving_observation_sink = .{
            .ctx = @ptrCast(&result.mailbox),
            .complete_fn = Mailbox.complete,
        };
        result.node.context_batch_allocation_sink = .{
            .ctx = @ptrCast(&result.batch_mailbox),
            .complete_fn = BatchMailbox.complete,
        };
        result.node.context_batch_serving_sink = .{
            .ctx = @ptrCast(&result.batch_item_mailbox),
            .complete_fn = BatchItemMailbox.complete,
        };
        result.node.context_serving_failure_sink = .{
            .ctx = @ptrCast(&result.failure_mailbox),
            .complete_fn = FailureMailbox.complete,
        };
        errdefer {
            result.node.context_serving_failure_sink = null;
            result.node.context_batch_serving_sink = null;
            result.node.context_batch_allocation_sink = null;
            result.node.context_serving_observation_sink = null;
        }
        try result.loopback.start();
        return result;
    }

    /// Qualification protocol commands carry the same installed-model
    /// identifier accepted by serving (for example `owner/model--antfly-*`).
    /// ModelManager's acquireFromDir API is deliberately lower level and only
    /// accepts a resolved artifact directory, so keep resolution immediately
    /// adjacent to the acquisition boundary.
    fn acquireGeneratorModel(self: *Driver, model_name: []const u8) !model_manager.ModelHandle {
        const io = self.node.session_manager.io orelse return error.MissingIoRuntime;
        const model_dir = try resolveGeneratorModelDir(self.node, io, model_name);
        defer self.node.allocator.free(model_dir);
        return self.node.model_manager.acquireFromDir(model_dir);
    }

    pub fn deinit(self: *Driver) void {
        // Callback and qualification storage are borrowed by handler tasks.
        // Stop and join them before clearing either borrowed address.
        self.loopback.finish() catch {};
        self.node.context_serving_observation_sink = null;
        self.node.context_serving_failure_sink = null;
        self.node.context_batch_allocation_sink = null;
        self.node.context_batch_serving_sink = null;
        self.node.context_serving_qualification = null;
        self.coverage_mailbox.deinit();
        self.loopback.deinit();
        const allocator = self.allocator;
        self.* = undefined;
        allocator.destroy(self);
    }

    fn policyMode(request: Request) []const u8 {
        return if (request.variant == .attention_matching or request.coverage != null) "attention_matching" else "exact";
    }

    const HttpTarget = struct { message_index: u32, part_index: u32 };

    fn requestBodyWithMessages(self: *Driver, request: Request, messages: anytype) ![]u8 {
        var target_storage: [ordered_messages.max_messages * ordered_messages.max_parts_per_message]HttpTarget = undefined;
        var target_count: usize = 0;
        if (request.variant == .attention_matching or request.coverage != null) {
            if (request.ordered) |ordered| {
                try ordered_messages.validate(ordered.artifact);
                for (ordered.artifact.messages, 0..) |message, message_index| {
                    for (message.parts, 0..) |part, part_index| {
                        if (!part.selected) continue;
                        target_storage[target_count] = .{
                            .message_index = @intCast(message_index),
                            .part_index = @intCast(part_index),
                        };
                        target_count += 1;
                    }
                }
            } else {
                target_storage[0] = .{ .message_index = 0, .part_index = 0 };
                target_count = 1;
            }
        }
        const targets: ?[]const HttpTarget = if (target_count == 0) null else target_storage[0..target_count];
        return std.json.Stringify.valueAlloc(self.allocator, .{
            .model = request.model,
            .backend = "metal",
            .messages = messages,
            .context_policy = .{
                .compaction_targets = targets,
                .mode = policyMode(request),
                .max_context_tokens = request.max_context_tokens,
                .max_kv_tokens = if (request.max_kv_tokens == 0) null else request.max_kv_tokens,
                .keep_prefix_tokens = request.keep_prefix_tokens,
                .keep_recent_tokens = request.keep_recent_tokens,
            },
            .max_tokens = request.reserved_output_tokens,
            .temperature = 0,
            .top_p = 0,
            .top_k = 1,
            .ignore_eos = true,
            .stream = request.route == .stream,
            .chat_template_kwargs = .{ .enable_thinking = qualification_enable_thinking },
            .prompt_cache = false,
        }, .{});
    }

    const HttpPart = struct { type: []const u8 = "text", text: []const u8 };
    const HttpFunction = struct { name: []const u8, arguments: []const u8 };
    const HttpToolCall = struct { id: []const u8, type: []const u8, function: HttpFunction };
    const HttpMessage = struct { role: []const u8, content: []const HttpPart };
    const HttpMessageV2 = struct {
        role: []const u8,
        content: ?[]const HttpPart,
        tool_calls: ?[]const HttpToolCall = null,
        tool_call_id: ?[]const u8 = null,
    };

    fn requestBodyWithOrdered(self: *Driver, request: Request, ordered: OrderedInput) ![]u8 {
        try ordered_messages.validate(ordered.artifact);
        const actual = try ordered_messages.semanticDigest(ordered.artifact);
        if (!std.mem.eql(u8, &actual, &ordered.semantic_sha256)) return error.OrderedMessageIdentityMismatch;
        var parts: [ordered_messages.max_messages * ordered_messages.max_parts_per_message]HttpPart = undefined;
        if (ordered.artifact.schema_version == ordered_messages.schema_version) {
            var messages: [ordered_messages.max_messages]HttpMessage = undefined;
            var cursor: usize = 0;
            for (ordered.artifact.messages, 0..) |message, index| {
                const start = cursor;
                for (message.parts) |part| {
                    parts[cursor] = .{ .text = part.text };
                    cursor += 1;
                }
                messages[index] = .{ .role = @tagName(message.role), .content = parts[start..cursor] };
            }
            return self.requestBodyWithMessages(request, messages[0..ordered.artifact.messages.len]);
        }
        var messages: [ordered_messages.max_messages]HttpMessageV2 = undefined;
        var tool_calls: [ordered_messages.max_total_tool_calls]HttpToolCall = undefined;
        var cursor: usize = 0;
        var tool_cursor: usize = 0;
        for (ordered.artifact.messages, 0..) |message, index| {
            const start = cursor;
            for (message.parts) |part| {
                parts[cursor] = .{ .text = part.text };
                cursor += 1;
            }
            const tool_start = tool_cursor;
            if (message.tool_calls) |calls| for (calls) |call| {
                tool_calls[tool_cursor] = .{
                    .id = call.id,
                    .type = call.type,
                    .function = .{ .name = call.function.name, .arguments = call.function.arguments_json },
                };
                tool_cursor += 1;
            };
            messages[index] = .{
                .role = @tagName(message.role),
                .content = if (message.content_is_null) null else parts[start..cursor],
                .tool_calls = if (message.tool_calls != null) tool_calls[tool_start..tool_cursor] else null,
                .tool_call_id = message.tool_call_id,
            };
        }
        return self.requestBodyWithMessages(request, messages[0..ordered.artifact.messages.len]);
    }

    fn requestBody(self: *Driver, request: Request) ![]u8 {
        if ((request.coverage == null and request.structured == null and request.ordered == null and request.prompt.len == 0) or
            (request.coverage != null and (request.structured != null or request.ordered != null)) or
            (request.structured != null and request.ordered != null) or request.reserved_output_tokens < 1 or
            request.max_context_tokens <= request.reserved_output_tokens or
            ((request.variant == .attention_matching or request.coverage != null) and request.max_kv_tokens == 0))
            return error.InvalidServingDriverCommand;
        if (request.coverage) |coverage| {
            if (coverage.document_text.len == 0 or coverage.question_text.len == 0)
                return error.InvalidServingDriverCommand;
            return self.requestBodyWithMessages(request, &.{.{
                .role = "user",
                .content = &.{
                    .{ .type = "text", .text = coverage.document_text },
                    .{ .type = "text", .text = coverage.question_text },
                },
            }});
        }
        if (request.structured) |structured| {
            if (structured.document.len == 0 or structured.question.len == 0)
                return error.InvalidServingDriverCommand;
            return self.requestBodyWithMessages(request, &.{.{
                .role = "user",
                .content = &.{
                    .{ .type = "text", .text = structured.document },
                    .{ .type = "text", .text = structured.question },
                },
            }});
        }
        if (request.ordered) |ordered| return self.requestBodyWithOrdered(request, ordered);
        if (request.variant == .attention_matching) return error.StructuredCompactionInputRequired;
        return self.requestBodyWithMessages(request, &.{.{ .role = "user", .content = request.prompt }});
    }

    fn policy(request: Request) !@import("runtime/context_policy.zig").Policy {
        var result = @import("runtime/context_policy.zig").Policy{
            .mode = if (request.variant == .attention_matching or request.coverage != null) .attention_matching else .exact,
            .max_context_tokens = request.max_context_tokens,
            .max_kv_tokens = if (request.max_kv_tokens == 0) null else request.max_kv_tokens,
            .keep_prefix_tokens = request.keep_prefix_tokens,
            .keep_recent_tokens = request.keep_recent_tokens,
        };
        if ((request.variant == .attention_matching or request.coverage != null) and request.ordered == null)
            try result.compaction_targets.add(.{ .message_index = 0, .part_index = 0 });
        if (request.ordered) |ordered| {
            if (result.mode == .attention_matching) {
                try ordered_messages.validate(ordered.artifact);
                for (ordered.artifact.messages, 0..) |message, message_index| for (message.parts, 0..) |part, part_index|
                    if (part.selected) try result.compaction_targets.add(.{
                        .message_index = @intCast(message_index),
                        .part_index = @intCast(part_index),
                    });
            }
        }
        try result.validate();
        return result;
    }

    fn nativeMessages(
        request: Request,
        messages: *[ordered_messages.max_messages]@import("pipelines/generation.zig").Message,
        parts: *[ordered_messages.max_messages * ordered_messages.max_parts_per_message]@import("pipelines/generation.zig").Message.ContentPart,
        tool_calls: *[ordered_messages.max_total_tool_calls]@import("pipelines/generation.zig").Message.ToolCall,
    ) ![]const @import("pipelines/generation.zig").Message {
        if (request.ordered) |ordered| {
            try ordered_messages.validate(ordered.artifact);
            const actual = try ordered_messages.semanticDigest(ordered.artifact);
            if (!std.mem.eql(u8, &actual, &ordered.semantic_sha256)) return error.OrderedMessageIdentityMismatch;
            var cursor: usize = 0;
            var tool_cursor: usize = 0;
            for (ordered.artifact.messages, 0..) |message, index| {
                const start = cursor;
                for (message.parts) |part| {
                    parts[cursor] = .{ .text = part.text };
                    cursor += 1;
                }
                const tool_start = tool_cursor;
                if (message.tool_calls) |calls| for (calls) |call| {
                    tool_calls[tool_cursor] = .{
                        .id = call.id,
                        .type = call.type,
                        .name = call.function.name,
                        .arguments = call.function.arguments_json,
                    };
                    tool_cursor += 1;
                };
                messages[index] = .{
                    .role = @tagName(message.role),
                    .content = "",
                    .content_is_null = message.content_is_null,
                    .tool_calls = if (message.tool_calls != null) tool_calls[tool_start..tool_cursor] else null,
                    .tool_call_id = message.tool_call_id,
                    .content_parts = if (message.content_is_null) null else parts[start..cursor],
                };
            }
            return messages[0..ordered.artifact.messages.len];
        }
        var part_count: usize = 0;
        if (request.coverage) |coverage| {
            parts[0] = .{ .text = coverage.document_text };
            parts[1] = .{ .text = coverage.question_text };
            part_count = 2;
        } else if (request.structured) |structured| {
            parts[0] = .{ .text = structured.document };
            parts[1] = .{ .text = structured.question };
            part_count = 2;
        }
        messages[0] = .{
            .role = "user",
            .content = if (request.coverage == null and request.structured == null) request.prompt else "",
            .content_parts = if (part_count == 0) null else parts[0..part_count],
        };
        return messages[0..1];
    }

    fn executeNative(self: *Driver, request: Request) !Evidence {
        self.coverage_mailbox.reset();
        errdefer self.coverage_mailbox.discard();
        const generation = @import("pipelines/generation.zig");
        var message_storage: [ordered_messages.max_messages]generation.Message = undefined;
        var part_storage: [ordered_messages.max_messages * ordered_messages.max_parts_per_message]generation.Message.ContentPart = undefined;
        var tool_call_storage: [ordered_messages.max_total_tool_calls]generation.Message.ToolCall = undefined;
        const messages = try nativeMessages(request, &message_storage, &part_storage, &tool_call_storage);
        const output = try self.node.generateMessagesDirectQualification(
            self.allocator,
            request.model,
            messages,
            @intCast(request.reserved_output_tokens),
            .{
                .variant = switch (request.variant) {
                    .exact => .exact,
                    .packed_exact => .packed_exact,
                    .attention_matching => .attention_matching,
                },
                .policy = try policy(request),
                .optimize_decode = request.optimize_decode,
                .optimize_prefill = request.optimize_prefill,
                .fit_engine = request.fit_engine,
                .admitted_prefill_chunk_rows = request.admitted_prefill_chunk_rows,
                .quality = if (request.quality) |*quality| try quality.request(
                    request.variant,
                    quality.loaded_generation,
                    .generation,
                    null,
                ) else null,
                .coverage = if (request.coverage) |value| .{
                    .allocator = self.allocator,
                    .request = value.request,
                    .document = value.document,
                    .provenance = value.provenance,
                    .sink = .{ .ctx = @ptrCast(&self.coverage_mailbox), .complete_fn = CoverageMailbox.complete },
                } else null,
            },
        );
        defer self.allocator.free(output.text);
        defer if (output.reasoning_text) |reasoning| self.allocator.free(reasoning);
        const observation = output.context_observation orelse return error.MissingContextServingObservation;
        if (!observation.route_completed) return error.IncompleteContextQualification;
        var owned_content_type: ?[]u8 = try self.allocator.dupe(u8, "text/plain; charset=utf-8");
        errdefer if (owned_content_type) |value| self.allocator.free(value);
        var owned_body: ?[]u8 = try self.allocator.dupe(u8, output.text);
        errdefer if (owned_body) |value| self.allocator.free(value);
        var result = Evidence{
            .allocator = self.allocator,
            .status = 200,
            .content_type = owned_content_type.?,
            .body = owned_body.?,
            .observation = observation,
            .elapsed_ns = output.elapsed_ns,
            .ttft_ns = output.ttft_ns,
            .decode_ns = output.decode_ns,
        };
        owned_content_type = null;
        owned_body = null;
        errdefer result.deinit();
        if (request.capture_quality_output) {
            result.output_text = try self.allocator.dupe(u8, output.text);
            result.reasoning_text = if (output.reasoning_text) |reasoning|
                try self.allocator.dupe(u8, reasoning)
            else
                try self.allocator.alloc(u8, 0);
        }
        if (request.coverage != null) result.coverage = try self.coverage_mailbox.take();
        return result;
    }

    /// Render and tokenize one sealed ordered quality case with the exact
    /// tokenizer/template generation held by this persistent driver. A fresh
    /// driver loads the model on demand and pins that generation throughout
    /// identity capture and rendering. This path cannot execute model forward
    /// work.
    pub fn materializeQuality(
        self: *Driver,
        request: MaterializationRequest,
    ) !MaterializationEvidence {
        if (!@import("builtin").is_test or request.policy.mode != .attention_matching or
            request.reserved_output_tokens == 0)
            return error.UnsupportedQualityMaterialization;
        try ordered_messages.validate(request.ordered.artifact);
        const request_identity = try ordered_messages.semanticDigest(request.ordered.artifact);
        if (!std.mem.eql(u8, &request_identity, &request.ordered.semantic_sha256))
            return error.OrderedMessageIdentityMismatch;
        var handle = try self.acquireGeneratorModel(request.model);
        defer handle.release();
        const model = handle.get();
        const gpt_config = session_factory.getGptConfig(model.session) orelse
            return error.UnsupportedContextCombination;
        const layout = try context_policy.Layout.init(gpt_config);
        const implementation = server.contextServingCompactionImplementationForEngine(
            request.fit_engine,
            request.optimize_decode,
            request.optimize_prefill,
        );
        const identity = try model.compactionIdentity(layout, implementation);
        const prompt_limit = try @import("pipelines/generation.zig").nativeGenerationPromptTokenLimitWithPolicy(
            gpt_config,
            null,
            request.reserved_output_tokens,
            0,
            0,
            request.policy,
        );
        var message_storage: [ordered_messages.max_messages]@import("pipelines/generation.zig").Message = undefined;
        var part_storage: [ordered_messages.max_messages * ordered_messages.max_parts_per_message]@import("pipelines/generation.zig").Message.ContentPart = undefined;
        var tool_call_storage: [ordered_messages.max_total_tool_calls]@import("pipelines/generation.zig").Message.ToolCall = undefined;
        const messages = try nativeMessages(.{
            .route = .native,
            .variant = .attention_matching,
            .model = request.model,
            .prompt = "",
            .ordered = request.ordered,
            .max_context_tokens = request.policy.max_context_tokens orelse return error.InvalidContextPolicy,
            .max_kv_tokens = request.policy.max_kv_tokens orelse return error.InvalidContextPolicy,
            .keep_prefix_tokens = request.policy.keep_prefix_tokens,
            .keep_recent_tokens = request.policy.keep_recent_tokens,
            .reserved_output_tokens = request.reserved_output_tokens,
            .optimize_decode = request.optimize_decode,
            .optimize_prefill = request.optimize_prefill,
        }, &message_storage, &part_storage, &tool_call_storage);
        var materialized = try quality_materializer.materialize(
            self.allocator,
            model.chat_tmpl orelse return error.UnsupportedCompactionTemplate,
            model.getTokenizer(),
            messages,
            .{ .enable_thinking = qualification_enable_thinking },
            request.policy,
            prompt_limit,
            request.reserved_output_tokens,
            model.manifest.add_bos_token,
            model.manifest.bos_token,
            request.reference_utf8,
        );
        errdefer materialized.deinit();
        return .{
            .materialized = materialized,
            .loaded_identity = .{
                .load_generation = identity.load_generation,
                .artifact_sha256 = identity.identity.artifact,
                .tokenizer_sha256 = identity.identity.tokenizer,
                .template_sha256 = identity.identity.template,
                .layout_sha256 = identity.identity.layout,
                .profile_sha256 = @splat(0),
                .implementation_sha256 = identity.identity.runtime,
            },
        };
    }

    pub fn executeNativeFault(
        self: *Driver,
        request: Request,
        controller: *@import("context_fault_injection.zig").Controller,
    ) !server.ContextServingFailureObservation {
        if (request.route != .native or request.variant != .attention_matching or request.structured == null or
            request.coverage != null)
            return error.InvalidServingDriverCommand;
        self.failure_mailbox.reset();
        const generation = @import("pipelines/generation.zig");
        const structured = request.structured.?;
        const parts = [_]generation.Message.ContentPart{
            .{ .text = structured.document },
            .{ .text = structured.question },
        };
        const messages = [_]generation.Message{.{ .role = "user", .content = "", .content_parts = &parts }};
        _ = self.node.generateMessagesDirectQualification(
            self.allocator,
            request.model,
            &messages,
            @intCast(request.reserved_output_tokens),
            .{
                .variant = .attention_matching,
                .policy = try policy(request),
                .optimize_decode = request.optimize_decode,
                .optimize_prefill = request.optimize_prefill,
                .fit_engine = request.fit_engine,
                .admitted_prefill_chunk_rows = request.admitted_prefill_chunk_rows,
                .fault_controller = controller,
            },
        ) catch |err| {
            const expected: anyerror = switch (controller.kind) {
                .oom => error.OutOfMemory,
                .nonfinite => error.NonFiniteCompactionInput,
                .gpu_failure => error.ContextMetalDispatchFailed,
                else => error.Cancelled,
            };
            if (err != expected) return err;
            return self.failure_mailbox.take();
        };
        return error.MissingContextFaultInjection;
    }

    /// Export the exact tokenizer and template hash preimages from the
    /// generation pinned by a real ModelHandle. Profile identity is selected
    /// again from that same generation and the compiled implementation; no
    /// command field or environment digest is trusted as loaded provenance.
    pub fn describeLoadedCompactionProvenance(
        self: *Driver,
        model_name: []const u8,
        fit_engine: compaction_profile.FitEngine,
        optimize_decode: bool,
        optimize_prefill: bool,
    ) !LoadedProvenanceEvidence {
        if (!@import("builtin").is_test) return error.UnsupportedContextQualification;
        var handle = try self.acquireGeneratorModel(model_name);
        defer handle.release();
        const model = handle.get();
        const gpt_config = session_factory.getGptConfig(model.session) orelse
            return error.UnsupportedContextCombination;
        const layout = try context_policy.Layout.init(gpt_config);
        const implementation = server.contextServingCompactionImplementationForEngine(
            fit_engine,
            optimize_decode,
            optimize_prefill,
        );
        const selection = try model.selectCompactionProfile(layout, implementation);
        var exported = try model.exportCompactionProvenance(self.allocator);
        errdefer exported.deinit();
        if (exported.load_generation != selection.load_generation or
            !std.mem.eql(u8, &exported.artifact_sha256, &selection.identity.artifact) or
            !std.mem.eql(u8, &exported.tokenizer_loaded_sha256, &selection.identity.tokenizer) or
            !std.mem.eql(u8, &exported.template_loaded_sha256, &selection.identity.template))
            return error.ContextQualificationLoadedIdentityMismatch;
        return .{
            .exported = exported,
            .loaded_identity = .{
                .load_generation = selection.load_generation,
                .artifact_sha256 = selection.identity.artifact,
                .tokenizer_sha256 = selection.identity.tokenizer,
                .template_sha256 = selection.identity.template,
                .layout_sha256 = selection.identity.layout,
                .profile_sha256 = selection.profile_sha256,
                .implementation_sha256 = selection.identity.runtime,
            },
            .profile_semantic_sha256 = selection.profile_semantic_digest,
        };
    }

    /// Retire the actually published model object and load the same immutable
    /// artifact through ModelManager. Generation tokens come only from model
    /// publication; this helper never edits them. Existing request handles
    /// must already be released before entry.
    pub fn reloadModelForQualification(
        self: *Driver,
        model_name: []const u8,
        fit_engine: compaction_profile.FitEngine,
        optimize_decode: bool,
        optimize_prefill: bool,
    ) !ReloadEvidence {
        if (!@import("builtin").is_test) return error.UnqualifiedFaultInjection;
        var old = self.node.model_manager.acquireLoadedModel(model_name) orelse
            return error.ModelNotLoaded;
        const old_model = old.get();
        const before = old_model.load_generation;
        const gpt_config = session_factory.getGptConfig(old_model.session) orelse
            return error.UnsupportedContextCombination;
        const layout = try context_policy.Layout.init(gpt_config);
        const implementation = server.contextServingCompactionImplementationForEngine(
            fit_engine,
            optimize_decode,
            optimize_prefill,
        );
        const old_selection = try old_model.selectCompactionProfile(layout, implementation);
        var retired_pin = self.node.model_manager.acquireLoadedModel(model_name) orelse
            return error.ModelNotLoaded;
        defer retired_pin.release();
        const model_dir = try self.allocator.dupe(u8, old_model.model_dir);
        defer self.allocator.free(model_dir);
        old.retire();
        var fresh = try self.node.model_manager.acquireFromDir(model_dir);
        defer fresh.release();
        const fresh_model = fresh.get();
        const after = fresh_model.load_generation;
        if (before == 0 or after == 0 or after == before) return error.ModelReloadGenerationUnchanged;
        if (fresh_model.validatePinnedCompactionSelection(layout, implementation, old_selection)) |_| {
            return error.StaleContextSelectionAccepted;
        } else |err| {
            if (err != error.StaleContextView) return err;
        }
        const fresh_selection = try fresh_model.selectCompactionProfile(layout, implementation);
        if (fresh_selection.load_generation != after or
            !std.meta.eql(fresh_selection.identity, old_selection.identity) or
            !std.mem.eql(u8, &fresh_selection.profile_sha256, &old_selection.profile_sha256) or
            !std.mem.eql(u8, &fresh_selection.profile_semantic_digest, &old_selection.profile_semantic_digest))
            return error.ModelReloadIdentityMismatch;
        return .{
            .loaded_generation_before = before,
            .loaded_generation_after = after,
            .stale_selection_rejected = true,
        };
    }

    /// Execute the genuine HTTP batch handler. The batch allocation sink is
    /// multi-event because exact siblings and the AM singleton are distinct
    /// execution groups with distinct owner receipts.
    pub fn executeMixedBatch(
        self: *Driver,
        base: Request,
        siblings: []const MixedSibling,
    ) !BatchEvidence {
        if (base.route != .serial_mixed_batch or siblings.len < 2 or siblings.len > max_batch_allocation_members)
            return error.InvalidServingDriverCommand;
        var exact_count: usize = 0;
        var attention_count: usize = 0;
        self.batch_mailbox.reset();
        self.batch_item_mailbox.reset();
        errdefer self.batch_mailbox.reset();
        errdefer self.batch_item_mailbox.reset();
        self.node.context_serving_qualification = .{
            .variant = .attention_matching,
            .optimize_decode = base.optimize_decode,
            .optimize_prefill = base.optimize_prefill,
            .fit_engine = base.fit_engine,
            .admitted_prefill_chunk_rows = base.admitted_prefill_chunk_rows,
        };
        defer {
            self.loopback.idle() catch {
                self.loopback.listener.requestStop();
                self.loopback.listener.join() catch {};
            };
            self.node.context_serving_qualification = null;
        }

        var payload: std.Io.Writer.Allocating = .init(self.allocator);
        defer payload.deinit();
        try payload.writer.writeAll("{\"requests\":[");
        for (siblings, 0..) |sibling, index| {
            if (sibling.structured.document.len == 0 or sibling.structured.question.len == 0 or
                sibling.reserved_output_tokens == 0)
                return error.InvalidServingDriverCommand;
            switch (sibling.variant) {
                .exact => exact_count += 1,
                .attention_matching => attention_count += 1,
                .packed_exact => return error.UnsupportedMixedBatchVariant,
            }
            var item = base;
            item.route = .http;
            item.variant = sibling.variant;
            item.prompt = "";
            item.structured = sibling.structured;
            item.reserved_output_tokens = sibling.reserved_output_tokens;
            item.coverage = null;
            const body = try self.requestBody(item);
            defer self.allocator.free(body);
            if (index != 0) try payload.writer.writeByte(',');
            try payload.writer.print("{{\"custom_id\":\"item-{d}\",\"body\":", .{index});
            try payload.writer.writeAll(body);
            try payload.writer.writeByte('}');
        }
        if (exact_count == 0 or attention_count != 1) return error.InvalidServingDriverMixedBatch;
        try payload.writer.writeAll("]}");

        var response = try self.loopback.request(.POST, "/ai/v1/generate/batch", payload.written());
        defer response.deinit();
        if (response.status.code != 200) return error.ServingDriverHttpFailure;
        const response_body = response.body orelse return error.MissingServingDriverBody;
        try self.loopback.idle();
        const content_type = response.header("Content-Type") orelse return error.MissingServingDriverContentType;
        const owned_content_type = try self.allocator.dupe(u8, content_type);
        errdefer self.allocator.free(owned_content_type);
        const owned_body = try self.allocator.dupe(u8, response_body);
        errdefer self.allocator.free(owned_body);
        const allocations = try self.batch_mailbox.take(self.allocator);
        errdefer {
            for (allocations) |value| self.allocator.free(value.member_request_indices);
            self.allocator.free(allocations);
        }
        const items = try self.batch_item_mailbox.take(self.allocator, siblings.len);
        errdefer self.allocator.free(items);
        return .{
            .allocator = self.allocator,
            .status = response.status.code,
            .content_type = owned_content_type,
            .body = owned_body,
            .allocations = allocations,
            .items = items,
        };
    }

    /// Execute a distinct production CLI child against this Driver's warm,
    /// already-loaded loopback server. The server mailbox supplies tokens,
    /// identity, allocation, and release evidence; child stdout is never
    /// parsed into those facts.
    pub fn executeCli(
        self: *Driver,
        request: Request,
        runner: *const cli_process.Runner,
        invocation: cli_process.Request,
    ) !Evidence {
        if (request.route != .cli or request.variant != .attention_matching or
            request.structured == null or request.quality != null or request.coverage != null or
            !invocation.stream)
            return error.InvalidCliQualificationRequest;
        self.mailbox.reset();
        self.node.context_serving_qualification = .{
            .variant = .attention_matching,
            .optimize_decode = request.optimize_decode,
            .optimize_prefill = request.optimize_prefill,
            .fit_engine = request.fit_engine,
            .admitted_prefill_chunk_rows = request.admitted_prefill_chunk_rows,
        };
        defer {
            self.loopback.idle() catch {
                self.loopback.listener.requestStop();
                self.loopback.listener.join() catch {};
            };
            self.node.context_serving_qualification = null;
        }
        const server_url = try self.loopback.baseUrlAlloc(self.allocator);
        defer self.allocator.free(server_url);
        var child = try runner.run(server_url, invocation);
        var child_owned = true;
        errdefer if (child_owned) child.deinit();
        try self.loopback.idle();
        const observation = try self.mailbox.take();
        if (!observation.route_completed or observation.route_elapsed_ns == 0 or
            observation.route_ttft_ns == 0 or observation.route_decode_ns == 0 or
            observation.route_ttft_ns > observation.route_elapsed_ns or
            observation.route_decode_ns > observation.route_elapsed_ns)
            return error.IncompleteCliQualificationTiming;
        if (!observation.public_output_bound)
            return error.MissingCliPublicOutputBinding;
        try cli_process.validatePublicStdout(
            child.stdout,
            observation.public_output_bytes,
            observation.public_output_sha256,
        );
        var content_type: ?[]u8 = try self.allocator.dupe(u8, "text/plain; charset=utf-8");
        errdefer if (content_type) |value| self.allocator.free(value);
        const body = try self.allocator.alloc(u8, 0);
        errdefer self.allocator.free(body);
        const result = Evidence{
            .allocator = self.allocator,
            .status = 200,
            .content_type = content_type.?,
            .body = body,
            .observation = observation,
            .elapsed_ns = observation.route_elapsed_ns,
            .ttft_ns = observation.route_ttft_ns,
            .decode_ns = observation.route_decode_ns,
            .cli_process = child,
        };
        content_type = null;
        child_owned = false;
        return result;
    }

    fn executeOnce(self: *Driver, request: Request) !Evidence {
        switch (request.route) {
            .native => return self.executeNative(request),
            .cli => return error.UnsupportedCliQualification,
            .serial_mixed_batch => return error.MixedBatchRequiresExplicitSiblings,
            .http, .stream => {},
        }
        self.mailbox.reset();
        self.coverage_mailbox.reset();
        // Registered before the handler drain below so error unwinding joins
        // every possible producer before reclaiming transferred Evidence.
        errdefer self.coverage_mailbox.discard();
        self.node.context_serving_qualification = .{
            .variant = switch (request.variant) {
                .exact => .exact,
                .packed_exact => .packed_exact,
                .attention_matching => .attention_matching,
            },
            .optimize_decode = request.optimize_decode,
            .optimize_prefill = request.optimize_prefill,
            .fit_engine = request.fit_engine,
            .admitted_prefill_chunk_rows = request.admitted_prefill_chunk_rows,
            .quality = if (request.quality) |*quality| try quality.request(
                request.variant,
                quality.loaded_generation,
                .generation,
                null,
            ) else null,
            .coverage = if (request.coverage) |value| .{
                .allocator = self.allocator,
                .request = value.request,
                .document = value.document,
                .provenance = value.provenance,
                .sink = .{ .ctx = @ptrCast(&self.coverage_mailbox), .complete_fn = CoverageMailbox.complete },
            } else null,
        };
        // A timeout can return to this thread while a server task is still
        // unwinding. Preserve borrowed qualification and sink state until a
        // successful drain, or stop/join the listener before clearing it.
        defer {
            self.loopback.idle() catch {
                self.loopback.listener.requestStop();
                self.loopback.listener.join() catch {};
            };
            self.node.context_serving_qualification = null;
        }
        const body = try self.requestBody(request);
        defer self.allocator.free(body);
        const started_ns = platform.time.monotonicNs();
        var response = try self.loopback.request(.POST, "/ai/v1/generate", body);
        defer response.deinit();
        if (response.status.code != 200) return error.ServingDriverHttpFailure;
        const response_body = response.body orelse return error.MissingServingDriverBody;
        if (request.route == .stream and std.mem.indexOf(u8, response_body, "data: [DONE]") == null)
            return error.IncompleteServingDriverStream;
        // The client can finish reading the body before the handler's outer
        // post-release observation defer runs. Drain before touching mailbox.
        try self.loopback.idle();
        const content_type = response.header("Content-Type") orelse return error.MissingServingDriverContentType;
        var owned_content_type: ?[]u8 = try self.allocator.dupe(u8, content_type);
        errdefer if (owned_content_type) |value| self.allocator.free(value);
        var owned_body: ?[]u8 = try self.allocator.dupe(u8, response_body);
        errdefer if (owned_body) |value| self.allocator.free(value);
        const observation = try self.mailbox.take();
        var quality_text: ?QualityText = if (request.capture_quality_output)
            if (request.route == .stream)
                try parseQualityStreamResponse(self.allocator, response_body)
            else
                try parseQualityHttpResponse(self.allocator, response_body)
        else
            null;
        errdefer if (quality_text) |*value| value.deinit(self.allocator);
        const finished_ns = platform.time.monotonicNs();
        var result = Evidence{
            .allocator = self.allocator,
            .status = response.status.code,
            .content_type = owned_content_type.?,
            .body = owned_body.?,
            .observation = observation,
            .elapsed_ns = @max(1, finished_ns -| started_ns),
            .output_text = if (quality_text) |value| value.output else null,
            .reasoning_text = if (quality_text) |value| value.reasoning else null,
        };
        owned_content_type = null;
        owned_body = null;
        quality_text = null;
        errdefer result.deinit();
        if (request.coverage != null) result.coverage = try self.coverage_mailbox.take();
        return result;
    }

    fn executeTeacherSibling(
        self: *Driver,
        request: Request,
        generation_binding: teacher_forced.ContextBinding,
    ) !struct { receipt: teacher_forced.Receipt, observation: server.ContextServingObservation, elapsed_ns: u64 } {
        const quality = request.quality orelse return error.MissingQualityReferenceTokens;
        const generation = @import("pipelines/generation.zig");
        var message_storage: [ordered_messages.max_messages]generation.Message = undefined;
        var part_storage: [ordered_messages.max_messages * ordered_messages.max_parts_per_message]generation.Message.ContentPart = undefined;
        var tool_call_storage: [ordered_messages.max_total_tool_calls]generation.Message.ToolCall = undefined;
        const messages = try nativeMessages(request, &message_storage, &part_storage, &tool_call_storage);
        var output = try self.node.generateMessagesDirectQualification(
            self.allocator,
            request.model,
            messages,
            @intCast(request.reserved_output_tokens),
            .{
                .variant = if (request.variant == .exact) .exact else .attention_matching,
                .policy = try policy(request),
                .optimize_decode = request.optimize_decode,
                .optimize_prefill = request.optimize_prefill,
                .fit_engine = request.fit_engine,
                .admitted_prefill_chunk_rows = request.admitted_prefill_chunk_rows,
                .quality = try quality.request(request.variant, quality.loaded_generation, .teacher, generation_binding),
            },
        );
        defer self.allocator.free(output.text);
        defer if (output.reasoning_text) |reasoning| self.allocator.free(reasoning);
        defer if (output.teacher_forced_receipt) |*receipt| receipt.deinit();
        const observation = output.context_observation orelse return error.MissingContextServingObservation;
        if (!observation.route_completed or observation.loaded_identity.load_generation != quality.loaded_generation)
            return error.IncompleteContextQualification;
        const receipt = output.teacher_forced_receipt orelse return error.MissingTeacherForcedReceipt;
        output.teacher_forced_receipt = null;
        return .{ .receipt = receipt, .observation = observation, .elapsed_ns = output.elapsed_ns };
    }

    pub fn execute(self: *Driver, original: Request) !Evidence {
        if (original.quality == null) return self.executeOnce(original);
        if (original.coverage != null or (original.structured == null and original.ordered == null) or
            (original.variant != .exact and original.variant != .attention_matching))
            return error.InvalidServingDriverCommand;
        var model_handle = self.node.model_manager.acquireLoadedModel(original.model) orelse return error.ModelNotLoaded;
        defer model_handle.release();
        var request = original;
        request.capture_quality_output = true;
        request.quality.?.loaded_generation = model_handle.get().load_generation;
        var evidence = try self.executeOnce(request);
        errdefer evidence.deinit();
        if (evidence.observation.loaded_identity.load_generation != request.quality.?.loaded_generation)
            return error.QualityModelGenerationChanged;
        const output_ids = evidence.observation.output_token_ids[0..evidence.observation.output_token_ids_len];
        const signed_ids = try self.allocator.alloc(i32, output_ids.len);
        defer self.allocator.free(signed_ids);
        for (output_ids, signed_ids) |token, *out| {
            if (token > std.math.maxInt(i32)) return error.InvalidQualityOutputToken;
            out.* = @intCast(token);
        }
        const decoded_output = try model_handle.get().getTokenizer().decode(self.allocator, signed_ids);
        defer self.allocator.free(decoded_output);
        if (evidence.output_text == null or !std.mem.eql(u8, evidence.output_text.?, decoded_output))
            return error.QualityOutputTokenizerMismatch;
        const reference_tokens = request.quality.?.reference_tokens;
        const signed_reference = try self.allocator.alloc(i32, reference_tokens.len);
        defer self.allocator.free(signed_reference);
        for (reference_tokens, signed_reference) |token, *out| {
            if (token < 0 or token > std.math.maxInt(i32)) return error.InvalidQualityReferenceTokens;
            out.* = @intCast(token);
        }
        // This runs only after the generation sibling has frozen and published
        // its selected context. Neither the source bytes nor token provider are
        // passed to selection, study, fitting, or publication.
        const decoded_reference = try model_handle.get().getTokenizer().decode(self.allocator, signed_reference);
        defer self.allocator.free(decoded_reference);
        try requireExactQualityReferenceBytes(request.quality.?.reference_utf8, decoded_reference);
        const generation_binding = evidence.observation.quality_generation_binding orelse
            return error.MissingTeacherForcedGenerationBinding;
        var teacher = try self.executeTeacherSibling(request, generation_binding);
        var teacher_receipt_owned = true;
        errdefer if (teacher_receipt_owned) teacher.receipt.deinit();
        const total_elapsed_ns = std.math.add(u64, evidence.elapsed_ns, teacher.elapsed_ns) catch
            return error.QualityTimingOverflow;
        evidence.teacher_forced_receipt = teacher.receipt;
        teacher_receipt_owned = false;
        evidence.teacher_observation = teacher.observation;
        evidence.elapsed_ns = total_elapsed_ns;
        return evidence;
    }
};

fn coverageFixture(allocator: std.mem.Allocator) !coverage_mod.Evidence {
    const receipt = try allocator.dupe(u8, "{\"schema_version\":2}");
    errdefer allocator.free(receipt);
    const prompt = try allocator.dupe(u8, &.{ 1, 0, 0, 0, 0, 0, 0, 0 });
    errdefer allocator.free(prompt);
    const rendered_prompt = try allocator.dupe(u8, "x");
    errdefer allocator.free(rendered_prompt);
    const prompt_offsets = try allocator.dupe(u8, &.{ 0, 0, 0, 0, 1, 0, 0, 0 });
    errdefer allocator.free(prompt_offsets);
    const question = try allocator.dupe(u8, prompt);
    errdefer allocator.free(question);
    const fit = try allocator.dupe(u8, prompt);
    errdefer allocator.free(fit);
    const gate = try allocator.dupe(u8, prompt);
    errdefer allocator.free(gate);
    return .{
        .allocator = allocator,
        .receipt_json = receipt,
        .rendered_prompt = rendered_prompt,
        .prompt_tokens_i64_le = prompt,
        .prompt_token_offsets_u32_le = prompt_offsets,
        .question_tokens_i64_le = question,
        .request = .{ .session_id = @splat(1), .request_index = 1, .question_sha256 = @splat(2), .request_content_sha256 = @splat(3) },
        .question_source_start = 0,
        .question_source_end = 1,
        .load_generation = 1,
        .identity = .{ .artifact = @splat(4), .tokenizer = @splat(5), .template = @splat(6), .layout = @splat(7), .runtime = @splat(8) },
        .provenance = .{ .binary_sha256 = @splat(9), .source_build_sha256 = @splat(10) },
        .fit_study = .{
            .tokens_i64_le = fit,
            .token_count = 1,
            .prompt_tokens_sha256 = @splat(11),
            .rendered_suffix_sha256 = @splat(12),
            .template_bytes = "fit",
            .template_sha256 = @splat(13),
            .generation_contract_bytes = "contract",
            .generation_contract_sha256 = @splat(14),
            .response_tokens = 1,
            .generation_mode = .greedy_fixed_length_ignore_eos,
        },
        .gate_study = .{
            .tokens_i64_le = gate,
            .token_count = 1,
            .prompt_tokens_sha256 = @splat(11),
            .rendered_suffix_sha256 = @splat(15),
            .template_bytes = "gate",
            .template_sha256 = @splat(16),
            .generation_contract_bytes = "contract",
            .generation_contract_sha256 = @splat(14),
            .response_tokens = 1,
            .generation_mode = .greedy_fixed_length_ignore_eos,
        },
    };
}

fn failAfterCoverageTransfer(mailbox: *CoverageMailbox, evidence: coverage_mod.Evidence) !void {
    errdefer mailbox.discard();
    CoverageMailbox.complete(@ptrCast(mailbox), evidence);
    return error.InjectedPostCaptureFailure;
}

test "context direct coverage error reclaims transferred evidence" {
    var mailbox = CoverageMailbox{};
    try std.testing.expectError(
        error.InjectedPostCaptureFailure,
        failAfterCoverageTransfer(&mailbox, try coverageFixture(std.testing.allocator)),
    );
    try std.testing.expect(mailbox.value == null);
}

test "context HTTP coverage drain transfers evidence exactly once" {
    var mailbox = CoverageMailbox{};
    CoverageMailbox.complete(@ptrCast(&mailbox), try coverageFixture(std.testing.allocator));
    var evidence = try mailbox.take();
    defer evidence.deinit();
    mailbox.discard();
    try std.testing.expect(mailbox.value == null);
}

test "context mixed batch item mailbox requires every unique observed index" {
    var mailbox = BatchItemMailbox{};
    BatchItemMailbox.complete(@ptrCast(&mailbox), .{
        .request_index = 1,
        .observation = .{ .route_completed = true },
        .elapsed_ns = 3,
        .ttft_ns = 1,
        .decode_ns = 2,
    });
    try std.testing.expectError(
        error.IncompleteContextBatchServingObservations,
        mailbox.take(std.testing.allocator, 2),
    );
    BatchItemMailbox.complete(@ptrCast(&mailbox), .{
        .request_index = 0,
        .observation = .{ .route_completed = true },
        .elapsed_ns = 4,
        .ttft_ns = 2,
        .decode_ns = 2,
    });
    const items = try mailbox.take(std.testing.allocator, 2);
    defer std.testing.allocator.free(items);
    try std.testing.expectEqual(@as(usize, 0), items[0].request_index);
    try std.testing.expectEqual(@as(usize, 1), items[1].request_index);
    try std.testing.expect(items[0].observation.route_completed);
}

test "context mixed batch item mailbox rejects duplicate completion without panic" {
    var mailbox = BatchItemMailbox{};
    const value = server.ContextBatchServingObservation{
        .request_index = 0,
        .observation = .{ .route_completed = true },
        .elapsed_ns = 3,
        .ttft_ns = 1,
        .decode_ns = 2,
    };
    BatchItemMailbox.complete(@ptrCast(&mailbox), value);
    BatchItemMailbox.complete(@ptrCast(&mailbox), value);
    try std.testing.expectError(
        error.IncompleteContextBatchServingObservations,
        mailbox.take(std.testing.allocator, 1),
    );
    mailbox.reset();
    try std.testing.expect(!mailbox.invalid and mailbox.count == 0);
}

test "context failure mailbox rejects duplicate callback evidence" {
    var mailbox = FailureMailbox{};
    const fingerprint = @import("backends/packed_context_session.zig").FailureFingerprint{
        .epoch = 1,
        .logical_tokens = 8,
        .content_sha256 = @splat(1),
        .descriptors_sha256 = @splat(2),
        .kv_sha256 = @splat(3),
        .diagnostic_kv_readback_bytes = 16,
    };
    const observation = server.ContextServingFailureObservation{
        .failure = error.Cancelled,
        .loaded_identity = .{
            .load_generation = 1,
            .artifact_sha256 = @splat(1),
            .tokenizer_sha256 = @splat(2),
            .template_sha256 = @splat(3),
            .layout_sha256 = @splat(4),
            .profile_sha256 = @splat(5),
            .implementation_sha256 = @splat(6),
        },
        .original_before = fingerprint,
        .original_after = fingerprint,
        .partial_prefill = null,
        .injection = .{
            .injected = true,
            .kind = .cancel_fit,
            .stage = .fit,
            .completed_units = 1,
            .total_units = 2,
        },
        .maintenance_kv_readback_before = 0,
        .maintenance_kv_readback_after = 0,
    };
    FailureMailbox.complete(&mailbox, observation);
    FailureMailbox.complete(&mailbox, observation);
    try std.testing.expectError(error.DuplicateContextFailureObservation, mailbox.take());
}

test "context quality HTTP parser preserves public and reasoning channels" {
    var value = try parseQualityHttpResponse(
        std.testing.allocator,
        "{\"choices\":[{\"message\":{\"content\":\"answer\",\"reasoning_content\":\"private\"}}]}",
    );
    defer value.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("answer", value.output);
    try std.testing.expectEqualStrings("private", value.reasoning);
}

test "context quality stream parser concatenates actual ordered deltas" {
    const body =
        "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n" ++
        "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"pri\"},\"finish_reason\":null}]}\n\n" ++
        "data: {\"choices\":[{\"delta\":{\"content\":\"ans\"},\"finish_reason\":null}]}\n\n" ++
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\n" ++
        "data: {\"choices\":[]}\n\n" ++
        "data: [DONE]\n\n";
    var value = try parseQualityStreamResponse(std.testing.allocator, body);
    defer value.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("ans", value.output);
    try std.testing.expectEqualStrings("pri", value.reasoning);
    try std.testing.expectError(
        error.IncompleteServingDriverStream,
        parseQualityStreamResponse(std.testing.allocator, "data: {\"choices\":[]}\n\n"),
    );
    try std.testing.expectError(
        error.InvalidServingDriverQualityResponse,
        parseQualityStreamResponse(
            std.testing.allocator,
            "data: {\"choices\":[{\"delta\":7,\"finish_reason\":null}]}\n\n" ++
                "data: [DONE]\n\n",
        ),
    );
}

test "context quality reference remains opaque until teacher sibling validation" {
    const reference = [_]i64{ 7, 9 };
    var quality = QualityRequest{
        .reference_utf8 = "reference",
        .reference_tokens = &reference,
        .reference_sequence_sha256 = try teacher_forced.referenceSequenceDigest(&reference),
        .rendered_prompt_sha256 = @splat(1),
        .profile_artifact_sha256 = @splat(2),
        .profile_semantic_sha256 = @splat(3),
        .implementation_sha256 = @splat(4),
        .loaded_generation = 5,
    };
    const generation_request = try quality.request(.attention_matching, 5, .generation, null);
    try std.testing.expect(generation_request.reference_provider == null);
    try std.testing.expect(generation_request.generation_sibling == null);
    var binding = teacher_forced.ContextBinding{
        .variant = .attention_matching,
        .context_instance_id = 11,
        .publication_epoch = 2,
        .context_logical_tokens = 16,
        .prompt_tokens = 16,
        .rendered_prompt_sha256 = @splat(1),
        .prompt_tokens_sha256 = @splat(6),
        .reference_token_count = 2,
        .reference_sequence_sha256 = quality.reference_sequence_sha256,
        .loaded_generation = 5,
        .profile_artifact_sha256 = @splat(2),
        .profile_semantic_sha256 = @splat(3),
        .implementation_sha256 = @splat(4),
        .published_payload_sha256 = @splat(7),
        .published_descriptor_sha256 = @splat(8),
        .diagnostic_kv_readback_bytes = 32,
    };
    const teacher_request = try quality.request(.attention_matching, 5, .teacher, binding);
    try std.testing.expect(teacher_request.reference_provider != null);
    binding.context_instance_id = 12;
    try std.testing.expectEqualSlices(i64, &reference, try teacher_forced.revealVerifiedReference(teacher_request, binding));
}

test "context quality reference text must exactly match loaded-tokenizer decode" {
    try requireExactQualityReferenceBytes("answer", "answer");
    try std.testing.expectError(
        error.QualityReferenceTokenizerMismatch,
        requireExactQualityReferenceBytes("answer", "Answer"),
    );
    try std.testing.expectError(
        error.QualityReferenceTokenizerMismatch,
        requireExactQualityReferenceBytes("", ""),
    );
}

test "context ordered messages preserve roles parts and exact selected targets" {
    const artifact = ordered_messages.Artifact{ .schema_version = 1, .messages = &.{
        .{ .role = .user, .parts = &.{.{ .text = "document", .selected = true }} },
        .{ .role = .assistant, .parts = &.{.{ .text = "acknowledged" }} },
        .{ .role = .user, .parts = &.{
            .{ .text = "second source", .selected = true },
            .{ .text = "question" },
        } },
    } };
    const digest = try ordered_messages.semanticDigest(artifact);
    const ordered = OrderedInput{ .artifact = artifact, .semantic_sha256 = digest };
    const request = Request{
        .route = .native,
        .variant = .attention_matching,
        .model = "model",
        .prompt = "",
        .ordered = ordered,
        .max_context_tokens = 4096,
        .max_kv_tokens = 2048,
        .keep_prefix_tokens = 256,
        .keep_recent_tokens = 512,
        .reserved_output_tokens = 32,
    };
    const selected = try Driver.policy(request);
    try std.testing.expectEqual(@as(u8, 2), selected.compaction_targets.count);
    try std.testing.expectEqual(@as(u32, 0), selected.compaction_targets.items[0].message_index);
    try std.testing.expectEqual(@as(u32, 0), selected.compaction_targets.items[0].part_index);
    try std.testing.expectEqual(@as(u32, 2), selected.compaction_targets.items[1].message_index);
    try std.testing.expectEqual(@as(u32, 0), selected.compaction_targets.items[1].part_index);

    const generation = @import("pipelines/generation.zig");
    var messages: [ordered_messages.max_messages]generation.Message = undefined;
    var parts: [ordered_messages.max_messages * ordered_messages.max_parts_per_message]generation.Message.ContentPart = undefined;
    var tool_calls: [ordered_messages.max_total_tool_calls]generation.Message.ToolCall = undefined;
    const native = try Driver.nativeMessages(request, &messages, &parts, &tool_calls);
    try std.testing.expectEqual(@as(usize, 3), native.len);
    try std.testing.expectEqualStrings("assistant", native[1].role);
    try std.testing.expectEqualStrings("second source", native[2].content_parts.?[0].text);
    try std.testing.expectEqualStrings("question", native[2].content_parts.?[1].text);

    var driver: Driver = undefined;
    driver.allocator = std.testing.allocator;
    const body = try driver.requestBody(request);
    defer std.testing.allocator.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"role\":\"assistant\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"message_index\":2,\"part_index\":0") != null);
    try std.testing.expect(!qualification_enable_thinking);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"enable_thinking\":false") != null);
}

test "context ordered exact route carries messages without compaction targets" {
    const artifact = ordered_messages.Artifact{ .schema_version = 1, .messages = &.{
        .{ .role = .user, .parts = &.{.{ .text = "document", .selected = true }} },
        .{ .role = .user, .parts = &.{.{ .text = "question" }} },
    } };
    const request = Request{
        .route = .native,
        .variant = .exact,
        .model = "model",
        .prompt = "",
        .ordered = .{ .artifact = artifact, .semantic_sha256 = try ordered_messages.semanticDigest(artifact) },
        .max_context_tokens = 4096,
        .max_kv_tokens = 0,
        .keep_prefix_tokens = 256,
        .keep_recent_tokens = 512,
        .reserved_output_tokens = 32,
    };
    const exact = try Driver.policy(request);
    try std.testing.expectEqual(@as(u8, 0), exact.compaction_targets.count);
    var invalid = request;
    invalid.ordered.?.semantic_sha256 = @splat(0);
    const generation = @import("pipelines/generation.zig");
    var messages: [ordered_messages.max_messages]generation.Message = undefined;
    var parts: [ordered_messages.max_messages * ordered_messages.max_parts_per_message]generation.Message.ContentPart = undefined;
    var tool_calls: [ordered_messages.max_total_tool_calls]generation.Message.ToolCall = undefined;
    try std.testing.expectError(
        error.OrderedMessageIdentityMismatch,
        Driver.nativeMessages(invalid, &messages, &parts, &tool_calls),
    );
}

test "context ordered tool history round trips native and HTTP metadata without selecting it" {
    const artifact = ordered_messages.Artifact{ .schema_version = 2, .messages = &.{
        .{ .role = .user, .parts = &.{.{ .text = "retrieve the source" }} },
        .{
            .role = .assistant,
            .parts = &.{},
            .content_is_null = true,
            .tool_calls = &.{.{
                .id = "call-7",
                .function = .{ .name = "read_source_ranges", .arguments_json = "{\"end\":9,\"start\":1}" },
            }},
        },
        .{ .role = .tool, .parts = &.{.{ .text = "trusted source bytes", .selected = true }}, .tool_call_id = "call-7" },
        .{ .role = .user, .parts = &.{.{ .text = "return canonical JSON" }} },
    } };
    const request = Request{
        .route = .native,
        .variant = .attention_matching,
        .model = "model",
        .prompt = "",
        .ordered = .{ .artifact = artifact, .semantic_sha256 = try ordered_messages.semanticDigest(artifact) },
        .max_context_tokens = 4096,
        .max_kv_tokens = 2048,
        .keep_prefix_tokens = 256,
        .keep_recent_tokens = 512,
        .reserved_output_tokens = 32,
    };
    const policy_value = try Driver.policy(request);
    try std.testing.expectEqual(@as(u8, 1), policy_value.compaction_targets.count);
    try std.testing.expectEqual(@as(u32, 2), policy_value.compaction_targets.items[0].message_index);
    try std.testing.expectEqual(@as(u32, 0), policy_value.compaction_targets.items[0].part_index);

    const generation = @import("pipelines/generation.zig");
    var messages: [ordered_messages.max_messages]generation.Message = undefined;
    var parts: [ordered_messages.max_messages * ordered_messages.max_parts_per_message]generation.Message.ContentPart = undefined;
    var tool_calls: [ordered_messages.max_total_tool_calls]generation.Message.ToolCall = undefined;
    const native = try Driver.nativeMessages(request, &messages, &parts, &tool_calls);
    try std.testing.expect(native[1].content_is_null);
    try std.testing.expect(native[1].content_parts == null);
    try std.testing.expectEqualStrings("call-7", native[1].tool_calls.?[0].id);
    try std.testing.expectEqualStrings("read_source_ranges", native[1].tool_calls.?[0].name);
    try std.testing.expectEqualStrings("{\"end\":9,\"start\":1}", native[1].tool_calls.?[0].arguments);
    try std.testing.expectEqualStrings("call-7", native[2].tool_call_id.?);
    try std.testing.expectEqualStrings("trusted source bytes", native[2].content_parts.?[0].text);

    var driver: Driver = undefined;
    driver.allocator = std.testing.allocator;
    const body = try driver.requestBody(request);
    defer std.testing.allocator.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"content\":null") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"tool_calls\":[{\"id\":\"call-7\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"name\":\"read_source_ranges\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"arguments\":\"{\\\"end\\\":9,\\\"start\\\":1}\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"tool_call_id\":\"call-7\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"message_index\":2,\"part_index\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"message_index\":1") == null);
}

test "context Gemma style tool scan preserves selected response provenance only" {
    const artifact = ordered_messages.Artifact{ .schema_version = 2, .messages = &.{
        .{ .role = .user, .parts = &.{.{ .text = "retrieve the source" }} },
        .{
            .role = .assistant,
            .parts = &.{},
            .content_is_null = true,
            .tool_calls = &.{.{
                .id = "call-7",
                .function = .{ .name = "read_source_ranges", .arguments_json = "{\"ranges\":[]}" },
            }},
        },
        .{ .role = .tool, .parts = &.{.{ .text = "trusted source bytes", .selected = true }}, .tool_call_id = "call-7" },
        .{ .role = .user, .parts = &.{.{ .text = "return canonical JSON" }} },
    } };
    const request = Request{
        .route = .native,
        .variant = .attention_matching,
        .model = "model",
        .prompt = "",
        .ordered = .{ .artifact = artifact, .semantic_sha256 = try ordered_messages.semanticDigest(artifact) },
        .max_context_tokens = 4096,
        .max_kv_tokens = 2048,
        .keep_prefix_tokens = 256,
        .keep_recent_tokens = 512,
        .reserved_output_tokens = 32,
    };
    const generation = @import("pipelines/generation.zig");
    var messages: [ordered_messages.max_messages]generation.Message = undefined;
    var parts: [ordered_messages.max_messages * ordered_messages.max_parts_per_message]generation.Message.ContentPart = undefined;
    var tool_calls: [ordered_messages.max_total_tool_calls]generation.Message.ToolCall = undefined;
    const native = try Driver.nativeMessages(request, &messages, &parts, &tool_calls);
    const selected = try Driver.policy(request);
    const source =
        "{% for message in messages %}" ++
        "{% if message.tool_calls %}" ++
        "{% for k in range(loop.index0 + 1, messages | length) %}" ++
        "{% set follow = messages[k] %}" ++
        "{% if follow.role == 'tool' %}" ++
        "{% for call in message.tool_calls %}" ++
        "{% if call.id == follow.tool_call_id %}" ++
        "CALL={{ call.id }}:{{ call.function.name }}|" ++
        "{% for part in follow.content %}{% if part.type == 'text' %}{{ part.text }}{% endif %}{% endfor %}" ++
        "{% endif %}{% endfor %}{% endif %}{% endfor %}{% endif %}{% endfor %}";
    var template = try generation.ChatTemplate.init(std.testing.allocator, source, "", "", "", "");
    defer template.deinit();
    const rendered = try template.applyWithProvenance(
        std.testing.allocator,
        native,
        .{ .enable_thinking = qualification_enable_thinking },
        &selected.compaction_targets,
    );
    defer std.testing.allocator.free(rendered.text);
    defer std.testing.allocator.free(rendered.labels);
    const metadata = "CALL=call-7:read_source_ranges";
    const metadata_start = std.mem.indexOf(u8, rendered.text, metadata) orelse return error.MissingToolCallMetadata;
    try std.testing.expect(std.mem.allEqual(u32, rendered.labels[metadata_start..][0..metadata.len], 0));
    const payload = "trusted source bytes";
    const payload_start = std.mem.indexOf(u8, rendered.text, payload) orelse return error.MissingToolResponsePayload;
    try std.testing.expect(std.mem.allEqual(u32, rendered.labels[payload_start..][0..payload.len], 1));
}

test "context qualification resolves installed generator identifiers before directory acquisition" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/google/gemma-installed");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/google/gemma-installed/config.json",
        .data = "{}",
    });
    const models_root = try tmp.dir.realPathFileAlloc(io, "models", allocator);
    defer allocator.free(models_root);
    const installed_root = try tmp.dir.realPathFileAlloc(
        io,
        "models/google/gemma-installed",
        allocator,
    );
    defer allocator.free(installed_root);

    var node: server.Node = undefined;
    node.config = .{ .models_dir = models_root };
    node.allocator = allocator;

    const named = try resolveGeneratorModelDir(&node, io, "google/gemma-installed");
    defer allocator.free(named);
    try std.testing.expectEqualStrings(installed_root, named);
    const absolute = try resolveGeneratorModelDir(&node, io, installed_root);
    defer allocator.free(absolute);
    try std.testing.expectEqualStrings(installed_root, absolute);
    try std.testing.expectError(
        error.ModelNotFound,
        resolveGeneratorModelDir(&node, io, "google/missing-generator"),
    );

    inline for (.{
        .{ "q8", "q8-0-bundle-v1" },
        .{ "f16", "f16-bundle-v1" },
    }) |fixture| {
        try tmp.dir.createDirPath(io, "models/provisioned/" ++ fixture[0]);
        try tmp.dir.writeFile(io, .{
            .sub_path = "models/provisioned/" ++ fixture[0] ++ "/model.gguf",
            .data = "decoder",
        });
        const receipt = try std.fmt.allocPrint(
            allocator,
            "{{\"version\":2,\"source\":{{\"owner\":\"owner\",\"name\":\"ambiguous\",\"variant\":\"{s}\"}},\"artifacts\":[{{\"path\":\"model.gguf\",\"size\":7}}]}}",
            .{fixture[1]},
        );
        defer allocator.free(receipt);
        try tmp.dir.writeFile(io, .{
            .sub_path = "models/provisioned/" ++ fixture[0] ++ "/.antfly-download-complete.json",
            .data = receipt,
        });
    }
    try std.testing.expectError(
        error.AmbiguousModelIdentifier,
        resolveGeneratorModelDir(&node, io, "owner/ambiguous"),
    );
}
