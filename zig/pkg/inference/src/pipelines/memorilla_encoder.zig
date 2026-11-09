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

//! Managed encoder-to-memory bridge. Authentication/source closure belongs to
//! its caller; producer metadata is an identity check, never an access grant.
const std = @import("std");
const manager = @import("../server/model_manager.zig");
const factory = @import("../architectures/session_factory.zig");
const qualified = @import("../models/memorilla_qualification.zig");
const catalog = @import("../registry/memorilla_catalog.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const memory = @import("../runtime/root.zig").tier.memory;
const A = std.mem.Allocator;
pub const width = qualified.width;

pub const Producer = struct {
    profile: qualified.Profile = .qwen3_encoder_f32_v1,
    publication: [32]u8,
    identity: [64]u8,
    source_lock_sha256: [64]u8,
    recipe_sha256: [64]u8,
};

/// Stored-vector identity checks do not replace caller authorization or source
/// version closure. The caller supplies the exact expected publication pin.
pub fn requireProducer(producer: Producer, expected_publication: [32]u8) !void {
    var source: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(qualified.lock_bytes, &source, .{});
    if (producer.profile != .qwen3_encoder_f32_v1 or
        !std.mem.eql(u8, &producer.publication, &expected_publication) or
        !std.mem.eql(u8, &producer.identity, &qualified.executionIdentity(.qwen3_encoder_f32_v1)) or
        !std.mem.eql(u8, &producer.source_lock_sha256, &std.fmt.bytesToHex(source, .lower)) or
        !std.mem.eql(u8, &producer.recipe_sha256, catalog.encoder_manifest_sha256))
        return error.IncompatibleMemorillaProducer;
}

pub const Features = struct {
    allocator: A,
    document_count: usize,
    question_tokens: usize,
    documents_fp16: []f16,
    question_fp16: []f16,
    /// Exact BF16-valued features widened to F32 for the memory compute API.
    documents: []f32,
    question: []f32,
    question_text: []u8,
    document_sha256: [][32]u8,
    producer: Producer,
    lease: ?memory.AdmissionLease,

    pub fn deinit(self: *Features) void {
        self.allocator.free(self.documents_fp16);
        self.allocator.free(self.question_fp16);
        self.allocator.free(self.documents);
        self.allocator.free(self.question);
        self.allocator.free(self.question_text);
        self.allocator.free(self.document_sha256);
        if (self.lease) |*lease| lease.release();
        self.* = undefined;
    }
};

pub const canonicalQuestion = qualified.canonicalQuestion;

pub const StoredValue = struct { fp16: f16, memory: f32 };
pub fn storedValue(value: f32) !StoredValue {
    if (!std.math.isFinite(value)) return error.NonFiniteMemorillaFeature;
    const half: f16 = @floatCast(value);
    if (!std.math.isFinite(half)) return error.NonFiniteMemorillaFeature;
    const widened: f32 = @floatCast(half);
    const bits: u32 = @bitCast(widened);
    const rounded = bits +% (0x7fff + ((bits >> 16) & 1));
    const bf16: u32 = (rounded >> 16) << 16;
    return .{ .fp16 = half, .memory = @bitCast(bf16) };
}

fn copyFeature(source: []const f32, stored: []f16, destination: []f32) !void {
    if (source.len != width or stored.len != width or destination.len != width) return error.InvalidMemorillaFeatureWidth;
    var norm: f64 = 0;
    for (source) |value| {
        if (!std.math.isFinite(value)) return error.NonFiniteMemorillaFeature;
        norm += @as(f64, value) * value;
    }
    if (@abs(@sqrt(norm) - 1) > 1e-4) return error.UnnormalizedMemorillaFeature;
    for (source, stored, destination) |value, *half, *memory_value| {
        const converted = try storedValue(value);
        half.* = converted.fp16;
        memory_value.* = converted.memory;
    }
}
pub const convertFeature = copyFeature;

const TokenBudget = struct {
    total: usize = 0,
    question_text: []const u8,
    question_tokens: usize = 0,
    fn observe(raw: *anyopaque, text: []const u8, ids: []const i32, mask: []const i32) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (ids.len != mask.len) return error.InvalidMemorillaTokens;
        var active: usize = 0;
        var last: i32 = -1;
        for (ids, mask) |id, used| {
            if (used != 0 and used != 1) return error.InvalidMemorillaTokens;
            if (used == 1) {
                active += 1;
                last = id;
            }
        }
        if (active == 0 or active > qualified.max_text_tokens or last != 151643) return error.MemorillaTextLimitExceeded;
        self.total = try std.math.add(usize, self.total, active);
        if (self.total > 16384) return error.MemorillaTextLimitExceeded;
        if (std.mem.eql(u8, text, self.question_text)) {
            if (self.question_tokens != 0 and self.question_tokens != active) return error.InvalidMemorillaTokens;
            self.question_tokens = active;
        }
    }
};

/// The caller retains its handle and authorization closure. This call adds an
/// independent lifetime pin and keeps the complete output admitted until deinit.
pub fn encode(a: A, handle: *const manager.ModelHandle, documents: []const []const u8, question: []const u8, limits: memory.Limits, control: Control) !Features {
    return encodeImpl(a, handle, documents, question, limits, control, false);
}

/// Query execution over supplied stored features encodes only the question.
/// Its empty documents arrays are intentional and never encode an empty corpus.
pub fn encodeQuestion(a: A, handle: *const manager.ModelHandle, question: []const u8, limits: memory.Limits, control: Control) !Features {
    return encodeImpl(a, handle, &.{}, question, limits, control, true);
}

fn encodeImpl(a: A, handle: *const manager.ModelHandle, documents: []const []const u8, question: []const u8, limits: memory.Limits, control: Control, question_only: bool) !Features {
    try control.check();
    if (documents.len == 0 and !question_only) return error.EmptyMemorillaCollection;
    if (documents.len > qualified.max_documents or question.len > 8192) return error.MemorillaTextLimitExceeded;
    const canonical = try canonicalQuestion(question);
    if (canonical.len == 0) return error.EmptyMemorillaQuestion;
    for (documents) |text| {
        if (text.len > 8192) return error.MemorillaTextLimitExceeded;
        _ = try std.unicode.Utf8View.init(text);
    }
    const backing = @import("antfly_platform").allocator.reclaimingScratch(a);
    var pin = handle.duplicate();
    defer pin.release();
    const model = pin.get();
    try qualified.requireRecipe(.qwen3_encoder_f32_v1, model.manifest);
    try factory.requireMemorillaProfile(model.session, .qwen3_encoder_f32_v1);
    const publication = model.manifest.managed_publication orelse return error.MemorillaManagedProducerRequired;
    const elements = try std.math.mul(usize, documents.len + 1, width);
    const owned_bytes = try std.math.add(usize, try std.math.mul(usize, elements, 10), canonical.len + documents.len * 32 + (documents.len + 1) * @sizeOf([]const u8));
    var lease = try pin.manager.acquireRunResourceAmounts(.cpu, limits, .{ .host_scratch_bytes = owned_bytes });
    var owns_lease = true;
    defer if (owns_lease) lease.release();
    try control.check();
    var assets = try model.acquireEmbeddingAssetLeaseWithControl(false, control);
    defer assets.release();
    var pipeline = blk: {
        try model.lockEmbeddingAssetsWithControl(control);
        defer model.unlockEmbeddingAssets();
        try model.ensurePrimaryEmbeddingAssetsLockedWithControl(true, false, control);
        break :blk model.embeddingPipelineLocked(backing);
    };
    try model.verifyEmbeddingIdentityWithControl(control);
    const identity = model.embedding_identity orelse return error.InvalidEmbeddingIdentity;
    pipeline.execution_control = control;
    pipeline.execution_lane = &model.embedding_execution_lane;
    const lane = try model.embedding_execution_lane.acquire(control);
    defer lane.release();
    // This owner explicitly admits all retained full-width F32 output rows.
    // Keep bounded physical batches without the Gemma-only retained-row floor.
    pipeline.execution_lane_held = true;
    pipeline.config.max_length = qualified.max_text_tokens;
    pipeline.config.batch_size = 4;
    pipeline.config.strict_max_length = true;
    pipeline.config.reject_truncation = true;
    var budget = TokenBudget{ .question_text = canonical };
    pipeline.tokenization_observer = .{ .ptr = &budget, .observe = TokenBudget.observe };
    const texts = try backing.alloc([]const u8, documents.len + 1);
    defer backing.free(texts);
    @memcpy(texts[0..documents.len], documents);
    texts[documents.len] = canonical;
    const vectors = try pipeline.embed(texts);
    defer {
        for (vectors) |vector| backing.free(vector);
        backing.free(vectors);
    }
    try control.check();
    try model.verifyEmbeddingIdentityWithControl(control);
    try factory.requireMemorillaProfile(model.session, .qwen3_encoder_f32_v1);
    if (vectors.len != texts.len) return error.InvalidMemorillaFeatureWidth;
    if (budget.question_tokens == 0) return error.InvalidMemorillaTokens;
    const documents_fp16 = try backing.alloc(f16, documents.len * width);
    errdefer backing.free(documents_fp16);
    const question_fp16 = try backing.alloc(f16, width);
    errdefer backing.free(question_fp16);
    const document_features = try backing.alloc(f32, documents.len * width);
    errdefer backing.free(document_features);
    const question_features = try backing.alloc(f32, width);
    errdefer backing.free(question_features);
    const question_text = try backing.dupe(u8, canonical);
    errdefer backing.free(question_text);
    const document_sha256 = try backing.alloc([32]u8, documents.len);
    errdefer backing.free(document_sha256);
    for (documents, vectors[0..documents.len], 0..) |text, vector, i| {
        try control.check();
        try copyFeature(vector, documents_fp16[i * width ..][0..width], document_features[i * width ..][0..width]);
        std.crypto.hash.sha2.Sha256.hash(text, &document_sha256[i], .{});
    }
    try copyFeature(vectors[documents.len], question_fp16, question_features);
    var lock_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(qualified.lock_bytes, &lock_hash, .{});
    var recipe_hash: [64]u8 = undefined;
    @memcpy(&recipe_hash, catalog.encoder_manifest_sha256);
    try control.check();
    owns_lease = false;
    return .{ .allocator = backing, .document_count = documents.len, .question_tokens = budget.question_tokens, .documents_fp16 = documents_fp16, .question_fp16 = question_fp16, .documents = document_features, .question = question_features, .question_text = question_text, .document_sha256 = document_sha256, .producer = .{ .publication = publication, .identity = identity, .source_lock_sha256 = std.fmt.bytesToHex(lock_hash, .lower), .recipe_sha256 = recipe_hash }, .lease = lease };
}

test "Memorilla question rendering matches Python strip including Unicode boundaries" {
    try std.testing.expectEqualStrings("María", try canonicalQuestion("\x1c\t\u{3000}María\u{a0}\u{202f}"));
    try std.testing.expectError(error.EmptyMemorillaQuestion, canonicalQuestion("\u{2007}\r\n"));
    try std.testing.expectEqualStrings("x\u{200b}", try canonicalQuestion(" x\u{200b} "));
    try std.testing.expectError(error.InvalidUtf8, canonicalQuestion("\xff"));
    try std.testing.expectError(error.InvalidMemorillaQuestion, canonicalQuestion("Use <|memory|> directly"));
}

test "Memorilla storage casts FP16 before BF16 with ties to even" {
    const result = try storedValue(1.00390625);
    try std.testing.expectEqual(@as(f32, 1), result.memory);
    try std.testing.expectEqual(@as(f32, 1.00390625), @as(f32, @floatCast(result.fp16)));
    try std.testing.expectEqual(@as(f32, 1.015625), (try storedValue(1.01171875)).memory);
    try std.testing.expectError(error.NonFiniteMemorillaFeature, storedValue(std.math.nan(f32)));
}

test "Memorilla encoder rejects cancellation empty collection and injected question before touching a handle" {
    var untouched: manager.ModelHandle = undefined;
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, encode(std.testing.allocator, &untouched, &.{"doc"}, "question", .{}, .{ .check_fn = Cancel.check }));
    try std.testing.expectError(error.EmptyMemorillaCollection, encode(std.testing.allocator, &untouched, &.{}, "question", .{}, .{}));
    try std.testing.expectError(error.InvalidMemorillaQuestion, encodeQuestion(std.testing.allocator, &untouched, "<|memory|>", .{}, .{}));
}
