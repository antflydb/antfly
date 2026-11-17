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

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const Allocator = std.mem.Allocator;
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const inference_request_context = @import("antfly_inference_execution_context");
const RequestContext = inference_request_context.RequestContext;
const utf8_text = @import("utf8_text.zig");
const template_mod = if (builtin.os.tag == .freestanding or builtin.os.tag == .wasi or builtin.is_test or build_options.bench_minimal_deps)
    @import("../template_stub.zig")
else
    @import("../../../template.zig");
const inference_work = @import("antfly_inference_work");
const antfly_image = @import("antfly_image");

pub const DenseEmbedFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8, dims: u32) anyerror![]f32;
pub const DenseEmbedBatchFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, texts: []const []const u8, dims: u32) anyerror![]const []const f32;
pub const DenseEmbedPartsFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, parts: []const template_mod.ContentPart, dims: u32) anyerror![]f32;
/// Embeds each content part as an independently addressable work item. This is
/// the document-page path: a window of page images produces one vector per
/// page rather than an implicit document-level pool.
pub const DenseEmbedPartItemsFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, items: []const template_mod.ContentPart, dims: u32) anyerror![]const []const f32;
pub const DenseEmbedRasterItemsFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, items: []const antfly_image.BorrowedRasterAttachment, dims: u32) anyerror![]const []const f32;
/// These callbacks are invocation-local and safe for concurrent calls. They
/// must not mutate shared cancellation/progress fields on the provider.
pub const DenseEmbedPartItemsWithContextFn = *const fn (*anyopaque, Allocator, []const u8, []const template_mod.ContentPart, u32, RequestContext) anyerror![]const []const f32;
pub const DenseEmbedRasterItemsWithContextFn = *const fn (*anyopaque, Allocator, []const u8, []const antfly_image.BorrowedRasterAttachment, u32, RequestContext) anyerror![]const []const f32;
pub const DenseEmbedWithContextFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8, dims: u32, context: RequestContext) anyerror![]f32;
pub const DenseEmbedBatchWithContextFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, texts: []const []const u8, dims: u32, context: RequestContext) anyerror![]const []const f32;
pub const DenseEmbedPartsWithContextFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, parts: []const template_mod.ContentPart, dims: u32, context: RequestContext) anyerror![]f32;
pub const DenseMediaPartLimitFn = *const fn (ptr: *anyopaque, embedding_name: []const u8) ?usize;
pub const DenseCapabilitiesFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8) anyerror!inference_work.InferenceCapabilities;
pub const DensePartInvocationMemory = inference_work.InvocationMemoryPlan;
pub const DensePartInvocationShape = struct {
    item_count: usize,
    /// Exact fixed JSON envelopes for the concrete part variants, excluding
    /// binary payload data whose representation is charged by the attachment
    /// transport.
    item_envelope_json_bytes: usize = 0,
    /// Sum of conservative JSON-string upper bounds for text, URLs, and MIME
    /// values. Keeping this aggregate preserves heterogeneous accounting
    /// without retaining or allocating a synthetic common MIME type.
    string_json_bytes: usize = 0,
    /// Host-side normalization structures and replacement buffers that can be
    /// live alongside the concrete invocation. This belongs in the route plan,
    /// not in a private execution-only allowance, so scheduler admission and
    /// the bounded executor observe the same peak.
    preparation_bytes: usize = 0,
};
pub const DensePartInvocationMemoryFn = *const fn (
    ptr: *anyopaque,
    embedding_name: []const u8,
    shape: DensePartInvocationShape,
    dims: u32,
    capabilities: ?inference_work.InferenceCapabilities,
) anyerror!DensePartInvocationMemory;
pub const DenseEmbedDeinitFn = *const fn (ptr: *anyopaque, alloc: Allocator) void;
pub const EmbedSetCancellationFn = *const fn (ptr: *anyopaque, cancellation: CancellationToken) void;
pub const EmbedSetProgressFn = *const fn (ptr: *anyopaque, progress: inference_request_context.ProgressSink) void;
pub const RecoveryIdentity = struct {
    model: []const u8,
    backend: []const u8 = "",
};
pub const EmbedRecoveryIdentityFn = *const fn (ptr: *anyopaque, embedding_name: []const u8) ?RecoveryIdentity;
pub const SparseEmbedFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8) anyerror!SparseEmbedding;
pub const SparseEmbedBatchFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, texts: []const []const u8) anyerror![]SparseEmbedding;
pub const SparseEmbedWithContextFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8, context: RequestContext) anyerror!SparseEmbedding;
pub const SparseEmbedBatchWithContextFn = *const fn (ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, texts: []const []const u8, context: RequestContext) anyerror![]SparseEmbedding;
pub const SparseEmbedDeinitFn = *const fn (ptr: *anyopaque, alloc: Allocator) void;

pub const SparseEmbedding = @import("antfly_sparse_embedding").SparseEmbedding;

/// Optional pure-computation memo. Every artifact publication still validates
/// its source revision and generation; cached vectors carry no write authority.
pub const DenseResultCache = struct {
    /// Opt in only when lookup/remember may overlap on this cache owner.
    concurrent: bool = false,
    ptr: *anyopaque,
    lookup: *const fn (*anyopaque, Allocator, [32]u8) anyerror!?[]f32,
    remember: *const fn (*anyopaque, [32]u8, []const f32) void,
};

/// Invocation-local accounting at a text provider callback boundary.
pub const TextDispatchAccounting = struct {
    ptr: *anyopaque,
    begin: *const fn (*anyopaque) anyerror!u64,
    finish: *const fn (*anyopaque, u64) void,
};

pub const DenseEmbedder = struct {
    pub fn withRequestContext(self: DenseEmbedder, context: ?RequestContext) DenseEmbedder {
        var copy = self;
        if (context) |value| copy.part_request_context = value;
        return copy;
    }

    /// Reconstruct from a retained catalog while sharing external services.
    clone_for_indexes_fn: ?*const fn (*anyopaque, Allocator, []const u8, RequestContext) anyerror!DenseEmbedder = null,
    ptr: *anyopaque,
    dense_embed_fn: DenseEmbedFn,
    /// Stable effective provider/model/credential identity plus exact text and
    /// dimensions. Opaque custom providers opt out by leaving this null.
    computation_key_fn: ?*const fn (*anyopaque, []const u8, []const u8, u32) anyerror!?[32]u8 = null,
    dense_embed_batch_fn: ?DenseEmbedBatchFn = null,
    /// Execute against the supplied immutable model generation/route, or reject
    /// it as stale. Revision-bearing generations reject legacy execution;
    /// before/after alias checks cannot detect an ABA during inference.
    dense_embed_parts_planned_fn: ?*const fn (*anyopaque, Allocator, []const u8, []const template_mod.ContentPart, u32, ?RequestContext, inference_work.CapabilityLease) anyerror![]f32 = null,
    dense_embed_batch_planned_fn: ?*const fn (*anyopaque, Allocator, []const u8, []const []const u8, u32, ?RequestContext, inference_work.CapabilityLease) anyerror![]const []const f32 = null,
    dense_embed_parts_fn: ?DenseEmbedPartsFn = null,
    /// Opt-in semantic guarantee: one rendered text part has exactly the same
    /// embedding as the text API, including task/instruction handling. Opaque
    /// providers retain their parts callback unless they explicitly opt in.
    text_parts_equivalent_fn: ?*const fn (*anyopaque, []const u8) bool = null,
    dense_embed_part_items_fn: ?DenseEmbedPartItemsFn = null,
    dense_embed_raster_items_fn: ?DenseEmbedRasterItemsFn = null,
    dense_embed_part_items_with_context_fn: ?DenseEmbedPartItemsWithContextFn = null,
    dense_embed_raster_items_with_context_fn: ?DenseEmbedRasterItemsWithContextFn = null,
    part_request_context: ?RequestContext = null,
    text_dispatch_accounting: ?TextDispatchAccounting = null,
    /// One immutable coordinator-resolved value for admission and execution.
    /// Descriptor and route authority cannot be replaced independently.
    part_lease: ?inference_work.CapabilityLease = null,
    resolve_part_lease_fn: ?*const fn (*anyopaque, Allocator, []const u8, ?RequestContext) anyerror!inference_work.CapabilityLease = null,
    part_batch_limit_fn: ?*const fn (*anyopaque, []const u8, u32, inference_work.CapabilityLease, usize) anyerror!usize = null,
    capabilities_with_context_fn: ?*const fn (*anyopaque, Allocator, []const u8, RequestContext) anyerror!inference_work.InferenceCapabilities = null,
    dense_embed_part_items_planned_fn: ?*const fn (*anyopaque, Allocator, []const u8, []const template_mod.ContentPart, u32, ?RequestContext, inference_work.CapabilityLease) anyerror![]const []const f32 = null,
    dense_embed_with_context_fn: ?DenseEmbedWithContextFn = null,
    dense_embed_batch_with_context_fn: ?DenseEmbedBatchWithContextFn = null,
    /// Maximum simultaneous batch calls supported by this provider instance.
    /// Invocation-local context alone does not guarantee provider reentrancy.
    dense_batch_concurrency: u8 = 1,
    dense_embed_parts_with_context_fn: ?DenseEmbedPartsWithContextFn = null,
    media_part_limit_fn: ?DenseMediaPartLimitFn = null,
    /// Legacy synchronous capabilities. Under request control this is a checked,
    /// non-blocking metadata fallback when no controlled callback is supplied.
    /// I/O-backed providers must supply controlled capabilities or a lease
    /// resolver so discovery and activation honor the request lifetime.
    capabilities_fn: ?DenseCapabilitiesFn = null,
    part_invocation_memory_fn: ?DensePartInvocationMemoryFn = null,
    deinit_fn: ?DenseEmbedDeinitFn = null,
    set_cancellation_fn: ?EmbedSetCancellationFn = null,
    set_progress_fn: ?EmbedSetProgressFn = null,
    cache_revision_fn: ?*const fn (*anyopaque, Allocator, []const u8) anyerror!?[32]u8 = null,
    recovery_identity_fn: ?EmbedRecoveryIdentityFn = null,
    /// The implementation guarantees that each provider invocation has its
    /// own finite deadline. Foreground post-commit replay rejects legacy
    /// implementations that cannot make this guarantee; background replay
    /// remains source-compatible.
    foreground_bounded: bool = false,

    pub const TextGeneration = struct { revision: ?[32]u8 = null, lease: ?inference_work.CapabilityLease = null };

    /// Resolve before execution. Managed Antfly providers carry this immutable
    /// lease through native/HTTP execution, rather than observing a later alias.
    pub fn captureTextGeneration(self: DenseEmbedder, a: Allocator, name: []const u8) !TextGeneration {
        if (self.part_request_context) |context| try context.check();
        if (self.part_lease) |lease| return .{ .revision = if (lease.capabilities) |caps| caps.model_revision else null, .lease = lease };
        if (self.resolve_part_lease_fn != null) {
            const lease = self.part_lease orelse try self.resolvePartLease(a, name);
            return .{ .revision = if (lease.capabilities) |caps| caps.model_revision else null, .lease = lease };
        }
        const caps = self.capabilities(a, name) catch |err| switch (err) {
            error.EmbeddingCapabilitiesUnavailable => return .{},
            else => return err,
        };
        return .{ .revision = caps.model_revision, .lease = .{ .capabilities = caps } };
    }
    /// Planning control is independent of execution support: legacy synchronous
    /// executors remain usable in background lanes, while discovery still sees
    /// the coordinator's original cancellation token and deadline.
    pub fn captureTextGenerationWithContext(self: DenseEmbedder, a: Allocator, name: []const u8, context: RequestContext) !TextGeneration {
        var controlled = self;
        controlled.part_request_context = context;
        return controlled.captureTextGeneration(a, name);
    }

    pub fn withTextGeneration(self: DenseEmbedder, generation: TextGeneration) DenseEmbedder {
        return if (generation.lease) |lease| self.withPartLease(lease) else self;
    }
    pub fn requireTextGeneration(self: DenseEmbedder, a: Allocator, name: []const u8, generation: TextGeneration) !void {
        // Revalidate through the same controlled resolver used for planning,
        // never by consulting the captured lease or an uncontrolled I/O path.
        var current = self;
        current.part_lease = null;
        const observed = try current.captureTextGeneration(a, name);
        if (!std.meta.eql(generation.revision, observed.revision)) return error.EnrichmentSourceChanged;
    }
    pub fn requireTextGenerationWithContext(self: DenseEmbedder, a: Allocator, name: []const u8, generation: TextGeneration, context: RequestContext) !void {
        var controlled = self;
        controlled.part_request_context = context;
        return controlled.requireTextGeneration(a, name, generation);
    }

    fn embedLeasedTexts(self: DenseEmbedder, a: Allocator, name: []const u8, texts: []const []const u8, dims: u32) ![]const []const f32 {
        return self.dense_embed_batch_planned_fn.?(self.ptr, a, name, texts, dims, self.part_request_context, self.part_lease.?);
    }

    fn usesPinnedTextExecution(self: DenseEmbedder) bool {
        const lease = self.part_lease orelse return false;
        return self.dense_embed_batch_planned_fn != null or
            (if (lease.capabilities) |caps| caps.model_revision != null else false);
    }

    pub fn embedDense(self: DenseEmbedder, alloc: Allocator, embedding_name: []const u8, text: []const u8, dims: u32) ![]f32 {
        if (self.usesPinnedTextExecution()) return takeSingleDenseVector(alloc, try self.embedDenseBatch(alloc, embedding_name, &.{text}, dims));
        if (self.part_request_context) |context| return self.embedDenseWithContext(alloc, embedding_name, text, dims, context);
        var sanitized = try utf8_text.sanitizeWithoutSourceMapAlloc(alloc, text, "dense embedder");
        defer sanitized.deinit(alloc);
        return try self.dense_embed_fn(self.ptr, alloc, embedding_name, sanitized.text, dims);
    }

    pub fn embedDenseBatch(
        self: DenseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
        dims: u32,
    ) ![]const []const f32 {
        var sanitized = try sanitizeUtf8BatchForEmbeddingAlloc(alloc, texts);
        defer sanitized.deinit(alloc);
        return self.embedBoundedTexts(alloc, embedding_name, sanitized.texts(), dims, false);
    }

    pub fn embedDenseWithContext(
        self: DenseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        text: []const u8,
        dims: u32,
        context: RequestContext,
    ) ![]f32 {
        try context.check();
        if (self.usesPinnedTextExecution()) return takeSingleDenseVector(alloc, try self.embedDenseBatchWithContext(alloc, embedding_name, &.{text}, dims, context));
        const embed = self.dense_embed_with_context_fn orelse return error.UncancellableInferenceProvider;
        var sanitized = try utf8_text.sanitizeWithoutSourceMapAlloc(alloc, text, "dense embedder");
        defer sanitized.deinit(alloc);
        const vector = try embed(self.ptr, alloc, embedding_name, sanitized.text, dims, context);
        context.check() catch |err| {
            alloc.free(vector);
            return err;
        };
        return vector;
    }

    pub fn embedDenseBatchWithContext(
        self: DenseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
        dims: u32,
        context: RequestContext,
    ) ![]const []const f32 {
        try context.check();
        var sanitized = try sanitizeUtf8BatchForEmbeddingAlloc(alloc, texts);
        defer sanitized.deinit(alloc);
        var planned = self;
        planned.part_request_context = context;
        return planned.embedBoundedTexts(alloc, embedding_name, sanitized.texts(), dims, false);
    }

    /// Background replay retains request control for discovery and generation
    /// fences, while permitting synchronous execution with boundary checks.
    /// Foreground callers must use embedDenseBatchWithContext instead.
    pub fn embedDenseBatchBackgroundWithContext(self: DenseEmbedder, a: Allocator, name: []const u8, texts: []const []const u8, dims: u32, context: RequestContext) ![]const []const f32 {
        try context.check();
        var sanitized = try sanitizeUtf8BatchForEmbeddingAlloc(a, texts);
        defer sanitized.deinit(a);
        var controlled = self;
        controlled.part_request_context = context;
        return controlled.embedBoundedTexts(a, name, sanitized.texts(), dims, true);
    }

    // One executor boundary for text calls, including foreground re-embedding.
    // Resolve once, retain the lease across partitions and clean partial results
    // if a later invocation fails or is canceled.
    fn embedBoundedTexts(self: DenseEmbedder, a: Allocator, name: []const u8, texts: []const []const u8, dims: u32, allow_synchronous: bool) ![]const []const f32 {
        if (texts.len == 0) return a.alloc([]const f32, 0);
        const generation = try self.captureTextGeneration(a, name);
        const planned = self.withTextGeneration(generation);
        const limit = if (generation.lease != null) try planned.partBatchLimit(name, dims, texts.len) else texts.len;
        if (texts.len <= limit) return planned.dispatchGenerationTexts(a, name, texts, dims, generation, allow_synchronous);
        const vectors = try a.alloc([]const f32, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (vectors[0..initialized]) |v| a.free(v);
            a.free(vectors);
        }
        while (initialized < texts.len) {
            const end = initialized + @min(limit, texts.len - initialized);
            const batch = try planned.dispatchGenerationTexts(a, name, texts[initialized..end], dims, generation, allow_synchronous);
            defer a.free(batch);
            if (batch.len != end - initialized) {
                for (batch) |v| a.free(v);
                return error.InvalidEmbeddingResponse;
            }
            @memcpy(vectors[initialized..end], batch);
            initialized = end;
        }
        return vectors;
    }

    fn dispatchGenerationTexts(self: DenseEmbedder, a: Allocator, name: []const u8, texts: []const []const u8, dims: u32, generation: TextGeneration, allow_synchronous: bool) anyerror![]const []const f32 {
        // Alias observations before/after execution cannot detect A -> B -> A.
        // A revision-bearing result requires the executor to consume its lease.
        if (generation.revision != null and self.dense_embed_batch_planned_fn == null)
            return error.CommitRequirementUnavailable;
        return self.dispatchTexts(a, name, texts, dims, allow_synchronous);
    }

    fn accountedTextDispatch(self: DenseEmbedder, callback: anytype, args: anytype) @TypeOf(@call(.auto, callback, args)) {
        const accounting = self.text_dispatch_accounting orelse return @call(.auto, callback, args);
        const started = try accounting.begin(accounting.ptr);
        defer accounting.finish(accounting.ptr, started);
        return @call(.auto, callback, args);
    }

    fn dispatchTexts(self: DenseEmbedder, a: Allocator, name: []const u8, texts: []const []const u8, dims: u32, allow_synchronous: bool) ![]const []const f32 {
        if (self.part_request_context) |context| try context.check();
        const vectors = if (self.part_lease != null and self.dense_embed_batch_planned_fn != null)
            try self.accountedTextDispatch(embedLeasedTexts, .{ self, a, name, texts, dims })
        else if (self.part_request_context) |context| blk: {
            if (self.dense_embed_batch_with_context_fn) |embed_batch|
                break :blk try self.accountedTextDispatch(embed_batch, .{ self.ptr, a, name, texts, dims, context });
            if (self.dense_embed_with_context_fn != null)
                break :blk try fallbackDenseBatchWithContext(self, a, name, texts, dims, context);
            if (allow_synchronous) {
                if (self.dense_embed_batch_fn) |embed_batch|
                    break :blk try self.accountedTextDispatch(embed_batch, .{ self.ptr, a, name, texts, dims });
                break :blk try fallbackDenseBatch(self, a, name, texts, dims);
            }
            return error.UncancellableInferenceProvider;
        } else if (self.dense_embed_batch_fn) |embed_batch|
            try self.accountedTextDispatch(embed_batch, .{ self.ptr, a, name, texts, dims })
        else
            try fallbackDenseBatch(self, a, name, texts, dims);
        if (self.part_request_context) |context| context.check() catch |err| {
            freeDenseEmbeddingBatch(a, vectors);
            return err;
        };
        return vectors;
    }

    pub fn textPartsEquivalent(self: DenseEmbedder, name: []const u8) bool {
        const equivalent = self.text_parts_equivalent_fn orelse return false;
        return equivalent(self.ptr, name);
    }

    pub fn supportsParts(self: DenseEmbedder) bool {
        return self.dense_embed_parts_fn != null;
    }

    pub fn supportsPartItems(self: DenseEmbedder) bool {
        return self.dense_embed_part_items_fn != null;
    }

    pub fn supportsRasterItems(self: DenseEmbedder) bool {
        return self.dense_embed_raster_items_fn != null;
    }

    pub fn mediaPartLimit(self: DenseEmbedder, embedding_name: []const u8) ?usize {
        const media_part_limit_fn = self.media_part_limit_fn orelse return null;
        return media_part_limit_fn(self.ptr, embedding_name);
    }

    pub fn capabilities(self: DenseEmbedder, alloc: Allocator, embedding_name: []const u8) !inference_work.InferenceCapabilities {
        if (self.part_request_context) |context| try context.check();
        const result = if (self.part_request_context != null and self.capabilities_with_context_fn != null)
            try self.capabilities_with_context_fn.?(self.ptr, alloc, embedding_name, self.part_request_context.?)
        else if (self.part_request_context != null and self.resolve_part_lease_fn != null)
            (try self.resolveProviderLease(alloc, embedding_name)).capabilities.?
        else if (self.capabilities_fn) |metadata|
            try metadata(self.ptr, alloc, embedding_name)
        else
            return error.EmbeddingCapabilitiesUnavailable;
        if (self.part_request_context) |context| try context.check();
        try result.validate();
        if (result.task != .embed) return error.InvalidInferenceCapabilities;
        return result;
    }

    /// Resolve once on the coordinator, before materialization/admission. The
    /// value owns its token and revision; cache eviction cannot revoke a plan.
    pub fn resolvePartLease(self: DenseEmbedder, alloc: Allocator, name: []const u8) !inference_work.CapabilityLease {
        if (self.part_request_context) |context| try context.check();
        if (self.resolve_part_lease_fn != null) return self.resolveProviderLease(alloc, name);
        return .{ .capabilities = try self.capabilities(alloc, name) };
    }

    // Shared callback boundary for discovery and metadata-only capability
    // callers. Keep fallback selection outside this function: resolution must
    // not recurse back through capabilities when a resolver is available.
    fn resolveProviderLease(self: DenseEmbedder, alloc: Allocator, name: []const u8) !inference_work.CapabilityLease {
        if (self.part_request_context) |context| try context.check();
        const resolve = self.resolve_part_lease_fn orelse return error.EmbeddingCapabilitiesUnavailable;
        const lease = try resolve(self.ptr, alloc, name, self.part_request_context);
        if (self.part_request_context) |context| try context.check();
        const caps = lease.capabilities orelse return error.EmbeddingCapabilitiesUnavailable;
        try caps.validate();
        if (caps.task != .embed) return error.InvalidInferenceCapabilities;
        return lease;
    }

    pub fn withPartLease(self: DenseEmbedder, lease: inference_work.CapabilityLease) DenseEmbedder {
        var planned = self;
        planned.part_lease = lease;
        return planned;
    }

    pub fn partBatchLimit(self: DenseEmbedder, name: []const u8, dims: u32, requested: usize) !usize {
        if (requested == 0) return error.InvalidInferenceCapabilities;
        const lease = self.part_lease orelse return error.EmbeddingCapabilitiesUnavailable;
        const caps = lease.capabilities orelse return error.EmbeddingCapabilitiesUnavailable;
        const limit = @min(requested, caps.batch.max_items);
        const actual = if (self.part_batch_limit_fn) |resolve| try resolve(self.ptr, name, dims, lease, limit) else limit;
        if (actual == 0 or actual > limit) return error.InvalidInferenceCapabilities;
        return actual;
    }

    /// Return the concrete route's complete non-media peak and attachment
    /// representation. A media-capable implementation must publish this
    /// contract: silently assuming borrowed input would make admission depend
    /// on an implementation detail that can change at runtime.
    pub fn partInvocationMemory(
        self: DenseEmbedder,
        embedding_name: []const u8,
        shape: DensePartInvocationShape,
        dims: u32,
    ) !DensePartInvocationMemory {
        const memory_fn = self.part_invocation_memory_fn orelse
            return error.InferenceInvocationMemoryUnavailable;
        const plan = try memory_fn(self.ptr, embedding_name, shape, dims, if (self.part_lease) |lease| lease.capabilities else null);
        try plan.validate();
        return plan;
    }

    /// Planning helper for document renderers that know the output MIME before
    /// page bytes exist. Runtime execution derives the same shape from the
    /// concrete heterogeneous item list.
    pub fn partInvocationMemoryForMime(
        self: DenseEmbedder,
        embedding_name: []const u8,
        item_count: usize,
        mime_type: []const u8,
        dims: u32,
    ) !DensePartInvocationMemory {
        _ = inference_work.mimeTypeEssence(mime_type) catch
            return error.UnsupportedInferenceMimeType;
        const one_mime = try jsonStringUpperBound(mime_type);
        return try self.partInvocationMemory(embedding_name, .{
            .item_count = item_count,
            .item_envelope_json_bytes = std.math.mul(
                usize,
                item_count,
                "{\"type\":\"media\",\"data\":\"\",\"mime_type\":}".len,
            ) catch return error.InferenceEncodedBytesExceeded,
            .string_json_bytes = std.math.mul(usize, item_count, one_mime) catch
                return error.InferenceEncodedBytesExceeded,
        }, dims);
    }

    pub fn embedDenseParts(
        self: DenseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        parts: []const template_mod.ContentPart,
        dims: u32,
    ) ![]f32 {
        if (self.part_request_context) |context| return self.embedDensePartsWithContext(alloc, embedding_name, parts, dims, context);
        if (parts.len == 1 and parts[0] == .text and self.textPartsEquivalent(embedding_name)) {
            const vectors = try self.embedDenseBatch(alloc, embedding_name, &.{parts[0].text}, dims);
            return takeSingleDenseVector(alloc, vectors);
        }
        return self.dispatchGroupedParts(alloc, embedding_name, parts, dims, self.part_request_context, true);
    }

    fn dispatchGroupedParts(self: DenseEmbedder, alloc: Allocator, name: []const u8, parts: []const template_mod.ContentPart, dims: u32, context: ?RequestContext, allow_synchronous: bool) ![]f32 {
        var controlled = self;
        controlled.part_request_context = context;
        const generation = try controlled.captureTextGeneration(alloc, name);
        var sanitized = try sanitizeContentPartsForEmbeddingAlloc(alloc, parts);
        defer sanitized.deinit(alloc);
        if (context) |value| try value.check();
        if (self.dense_embed_parts_planned_fn == null and generation.revision != null)
            return error.CommitRequirementUnavailable;
        const vector = if (generation.lease != null and self.dense_embed_parts_planned_fn != null)
            try self.dense_embed_parts_planned_fn.?(self.ptr, alloc, name, sanitized.partsSlice(), dims, context, generation.lease.?)
        else if (context != null and self.dense_embed_parts_with_context_fn != null)
            try self.dense_embed_parts_with_context_fn.?(self.ptr, alloc, name, sanitized.partsSlice(), dims, context.?)
        else if (allow_synchronous)
            try (self.dense_embed_parts_fn orelse return error.UnsupportedEmbeddingProvider)(self.ptr, alloc, name, sanitized.partsSlice(), dims)
        else
            return error.UncancellableInferenceProvider;
        errdefer alloc.free(vector);
        if (context) |value| try value.check();
        return vector;
    }

    pub fn embedDensePartItems(
        self: DenseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        items: []const template_mod.ContentPart,
        dims: u32,
    ) ![]const []const f32 {
        const embed_items = self.dense_embed_part_items_fn orelse return error.UnsupportedEmbeddingProvider;
        if (self.part_request_context) |context| try context.check();
        var planned = self;
        if (planned.part_lease == null and (self.resolve_part_lease_fn != null or self.capabilities_fn != null))
            planned = self.withPartLease(try self.resolvePartLease(alloc, embedding_name));
        if (items.len > 0 and planned.part_lease != null) {
            const limit = try planned.partBatchLimit(embedding_name, dims, items.len);
            if (items.len > limit) {
                const vectors = try alloc.alloc([]const f32, items.len);
                var initialized: usize = 0;
                errdefer {
                    for (vectors[0..initialized]) |vector| alloc.free(vector);
                    alloc.free(vectors);
                }
                while (initialized < items.len) {
                    const end = initialized + @min(limit, items.len - initialized);
                    const chunk = try planned.embedDensePartItems(alloc, embedding_name, items[initialized..end], dims);
                    defer alloc.free(chunk);
                    @memcpy(vectors[initialized..end], chunk);
                    initialized = end;
                }
                return vectors;
            }
        }
        const invocation_plan = try planned.partInvocationMemory(
            embedding_name,
            try densePartInvocationShape(items),
            dims,
        );
        var transport_copy_bytes: usize = 0;
        for (items) |item| switch (item) {
            .binary => |binary| {
                const resident = try invocation_plan.attachment_transport.peakResidentSize(binary.data.len, binary.mime_type.len);
                transport_copy_bytes = std.math.add(
                    usize,
                    transport_copy_bytes,
                    resident - binary.data.len,
                ) catch return error.InferenceEncodedBytesExceeded;
            },
            else => {},
        };
        const invocation_limit = std.math.add(
            usize,
            invocation_plan.allocator_limit_bytes,
            transport_copy_bytes,
        ) catch return error.InferenceEncodedBytesExceeded;
        var bounded = inference_work.BoundedInvocationAllocator.init(alloc, invocation_limit);
        const invocation_alloc = bounded.allocator();
        var sanitized = sanitizeContentPartsForEmbeddingAlloc(invocation_alloc, items) catch |err| {
            if (bounded.limit_exceeded) return error.InferenceInvocationMemoryExceeded;
            return err;
        };
        defer sanitized.deinit(invocation_alloc);
        const safe_items = sanitized.partsSlice();
        const vectors = (if (self.dense_embed_part_items_planned_fn) |call|
            call(self.ptr, invocation_alloc, embedding_name, safe_items, dims, self.part_request_context, planned.part_lease orelse return error.EmbeddingCapabilitiesUnavailable)
        else if (self.part_request_context) |context| blk: {
            try context.check();
            const call = self.dense_embed_part_items_with_context_fn orelse return error.UncancellableInferenceProvider;
            break :blk call(self.ptr, invocation_alloc, embedding_name, safe_items, dims, context);
        } else embed_items(self.ptr, invocation_alloc, embedding_name, safe_items, dims)) catch |err| {
            if (bounded.limit_exceeded) return error.InferenceInvocationMemoryExceeded;
            return err;
        };
        if (vectors.len != items.len) {
            freeDenseEmbeddingBatch(alloc, vectors);
            return error.InvalidEmbeddingResponse;
        }
        if (self.part_request_context) |context| context.check() catch |err| {
            freeDenseEmbeddingBatch(alloc, vectors);
            return err;
        };
        var actual_values: usize = 0;
        for (vectors) |vector| {
            if (vector.len != dims) {
                freeDenseEmbeddingBatch(alloc, vectors);
                return error.InvalidEmbeddingDimensions;
            }
            for (vector) |value| if (!std.math.isFinite(value)) {
                freeDenseEmbeddingBatch(alloc, vectors);
                return error.InvalidEmbeddingResponse;
            };
            const item_bytes = std.math.mul(usize, vector.len, @sizeOf(f32)) catch {
                freeDenseEmbeddingBatch(alloc, vectors);
                return error.InvalidEmbeddingResponse;
            };
            if (item_bytes > invocation_plan.max_result_bytes_per_item) {
                freeDenseEmbeddingBatch(alloc, vectors);
                return error.InferenceResultTooLarge;
            }
            actual_values = std.math.add(usize, actual_values, vector.len) catch {
                freeDenseEmbeddingBatch(alloc, vectors);
                return error.InvalidEmbeddingResponse;
            };
        }
        const result_bytes = std.math.mul(usize, actual_values, @sizeOf(f32)) catch {
            freeDenseEmbeddingBatch(alloc, vectors);
            return error.InvalidEmbeddingResponse;
        };
        if (result_bytes > invocation_plan.max_result_bytes) {
            freeDenseEmbeddingBatch(alloc, vectors);
            return error.InferenceResultTooLarge;
        }
        return vectors;
    }

    pub fn embedDenseRasterItems(
        self: DenseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        items: []const antfly_image.BorrowedRasterAttachment,
        dims: u32,
    ) ![]const []const f32 {
        const embed_items = self.dense_embed_raster_items_fn orelse
            return error.UnsupportedEmbeddingProvider;
        for (items) |item| try item.validate();
        const vectors = if (self.part_request_context) |context| blk: {
            try context.check();
            const call = self.dense_embed_raster_items_with_context_fn orelse return error.UncancellableInferenceProvider;
            break :blk try call(self.ptr, alloc, embedding_name, items, dims, context);
        } else try embed_items(self.ptr, alloc, embedding_name, items, dims);
        errdefer freeDenseEmbeddingBatch(alloc, vectors);
        if (self.part_request_context) |context| try context.check();
        if (vectors.len != items.len) return error.InvalidEmbeddingResponse;
        for (vectors) |vector| {
            if (vector.len != dims) return error.InvalidEmbeddingDimensions;
            for (vector) |value| if (!std.math.isFinite(value))
                return error.InvalidEmbeddingResponse;
        }
        return vectors;
    }

    /// Background discovery and fences retain cancellation even for legacy
    /// synchronous executors. Foreground execution still requires control.
    pub fn embedDensePartsBackgroundWithContext(self: DenseEmbedder, a: Allocator, name: []const u8, parts: []const template_mod.ContentPart, dims: u32, context: RequestContext) ![]f32 {
        try context.check();
        if (parts.len == 1 and parts[0] == .text and self.textPartsEquivalent(name)) {
            const vectors = try self.embedDenseBatchBackgroundWithContext(a, name, &.{parts[0].text}, dims, context);
            return takeSingleDenseVector(a, vectors);
        }
        return self.dispatchGroupedParts(a, name, parts, dims, context, true);
    }

    pub fn embedDensePartsWithContext(
        self: DenseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        parts: []const template_mod.ContentPart,
        dims: u32,
        context: RequestContext,
    ) ![]f32 {
        try context.check();
        if (parts.len == 1 and parts[0] == .text and self.textPartsEquivalent(embedding_name)) {
            const vectors = try self.embedDenseBatchWithContext(alloc, embedding_name, &.{parts[0].text}, dims, context);
            return takeSingleDenseVector(alloc, vectors);
        }
        return self.dispatchGroupedParts(alloc, embedding_name, parts, dims, context, false);
    }

    pub fn deinit(self: DenseEmbedder, alloc: Allocator) void {
        const deinit_fn = self.deinit_fn orelse return;
        deinit_fn(self.ptr, alloc);
    }

    pub fn setCancellation(self: DenseEmbedder, cancellation: CancellationToken) void {
        const set_cancellation_fn = self.set_cancellation_fn orelse return;
        set_cancellation_fn(self.ptr, cancellation);
    }

    pub fn setProgress(self: DenseEmbedder, progress: inference_request_context.ProgressSink) void {
        const set_progress_fn = self.set_progress_fn orelse return;
        set_progress_fn(self.ptr, progress);
    }

    pub fn recoveryIdentity(self: DenseEmbedder, embedding_name: []const u8) RecoveryIdentity {
        const get_identity = self.recovery_identity_fn orelse return .{ .model = embedding_name };
        return get_identity(self.ptr, embedding_name) orelse .{ .model = embedding_name };
    }
};

fn jsonStringUpperBound(value: []const u8) !usize {
    const escaped = std.math.mul(usize, value.len, 6) catch
        return error.InferenceEncodedBytesExceeded;
    return std.math.add(usize, escaped, 2) catch
        error.InferenceEncodedBytesExceeded;
}

fn densePartInvocationShape(parts: []const template_mod.ContentPart) !DensePartInvocationShape {
    var item_envelope_json_bytes: usize = 0;
    var string_json_bytes: usize = 0;
    for (parts) |part| switch (part) {
        .binary => |binary| {
            _ = inference_work.mimeTypeEssence(binary.mime_type) catch
                return error.UnsupportedInferenceMimeType;
            item_envelope_json_bytes = std.math.add(
                usize,
                item_envelope_json_bytes,
                "{\"type\":\"media\",\"data\":\"\",\"mime_type\":}".len,
            ) catch return error.InferenceEncodedBytesExceeded;
            string_json_bytes = std.math.add(
                usize,
                string_json_bytes,
                try jsonStringUpperBound(binary.mime_type),
            ) catch return error.InferenceEncodedBytesExceeded;
        },
        .media_url => |url| {
            item_envelope_json_bytes = std.math.add(
                usize,
                item_envelope_json_bytes,
                "{\"type\":\"image_url\",\"image_url\":{\"url\":}}".len,
            ) catch return error.InferenceEncodedBytesExceeded;
            string_json_bytes = std.math.add(
                usize,
                string_json_bytes,
                try jsonStringUpperBound(url),
            ) catch return error.InferenceEncodedBytesExceeded;
        },
        .text => |text| {
            item_envelope_json_bytes = std.math.add(
                usize,
                item_envelope_json_bytes,
                "{\"type\":\"text\",\"text\":}".len,
            ) catch return error.InferenceEncodedBytesExceeded;
            string_json_bytes = std.math.add(
                usize,
                string_json_bytes,
                try jsonStringUpperBound(text),
            ) catch return error.InferenceEncodedBytesExceeded;
        },
    };
    return .{
        .item_count = parts.len,
        .item_envelope_json_bytes = item_envelope_json_bytes,
        .string_json_bytes = string_json_bytes,
        .preparation_bytes = try contentPartPreparationBytes(parts),
    };
}

fn contentPartPreparationBytes(parts: []const template_mod.ContentPart) !usize {
    var total: usize = 0;
    var has_invalid_text = false;
    for (parts) |part| switch (part) {
        .text => |value| {
            if (std.unicode.utf8ValidateSlice(value)) continue;
            has_invalid_text = true;
            // The repair helper sizes exactly before allocating. Invalid UTF-8
            // can still expand to one three-byte replacement scalar per source
            // byte, so reserve that complete final buffer.
            const sanitized = std.math.mul(usize, value.len, 3) catch
                return error.InferenceEncodedBytesExceeded;
            total = std.math.add(usize, total, sanitized) catch
                return error.InferenceEncodedBytesExceeded;
        },
        .media_url => {},
        .binary => {},
    };
    if (has_invalid_text) {
        total = std.math.add(
            usize,
            total,
            std.math.mul(usize, parts.len, @sizeOf(template_mod.ContentPart)) catch
                return error.InferenceEncodedBytesExceeded,
        ) catch return error.InferenceEncodedBytesExceeded;
        total = std.math.add(
            usize,
            total,
            std.math.mul(usize, parts.len, @sizeOf(?[]u8)) catch
                return error.InferenceEncodedBytesExceeded,
        ) catch return error.InferenceEncodedBytesExceeded;
    }
    return total;
}

pub const SparseEmbedder = struct {
    pub fn withRequestContext(self: SparseEmbedder, context: ?RequestContext) SparseEmbedder {
        var copy = self;
        if (context) |value| copy.request_context = value;
        return copy;
    }

    pub const Generation = DenseEmbedder.TextGeneration;
    pub fn captureGeneration(self: SparseEmbedder, a: Allocator, name: []const u8) !Generation {
        if (self.request_context) |context| try context.check();
        if (self.captured_generation) |generation| return generation;
        if (self.resolve_generation_fn) |resolve| {
            const generation = try resolve(self.ptr, a, name, self.request_context);
            if (self.request_context) |context| try context.check();
            return generation;
        }
        return .{ .revision = try self.captureUnpinnedRevision(a, name) };
    }

    pub fn withGeneration(self: SparseEmbedder, generation: Generation) SparseEmbedder {
        var copy = self;
        // A lease is what makes execution "alias-bound" (the planned-batch
        // call below consumes it to stamp one resolved generation across the
        // whole batch); a bare revision is not. `captureGeneration`'s own
        // legacy fallback (`captureUnpinnedRevision`, for a provider with
        // only `cache_revision_fn`/`cache_revision_with_context_fn` and no
        // `resolve_generation_fn`) only ever returns a revision, never a
        // lease -- that generation is "unpinned" by name, and pinning it
        // here anyway forced every such legacy provider through
        // `embedPlannedBatch`, which then correctly, but wrongly for this
        // case, rejected it with `error.CommitRequirementUnavailable` (no
        // `sparse_embed_batch_planned_fn` to consume the lease that was
        // never there). Mirrors `DenseEmbedder.withTextGeneration`, which
        // pins on `generation.lease` alone for the same reason.
        copy.captured_generation = if (generation.lease == null) null else generation;
        return copy;
    }

    fn embedPlannedBatch(self: SparseEmbedder, a: Allocator, name: []const u8, texts: []const []const u8) anyerror![]SparseEmbedding {
        const generation = self.captured_generation.?;
        const execute = self.sparse_embed_batch_planned_fn orelse {
            // Observing a revision is insufficient to stamp an alias-bound
            // execution. Legacy providers remain usable without a revision.
            if (generation.revision != null) return error.CommitRequirementUnavailable;
            var unpinned = self;
            unpinned.captured_generation = null;
            return unpinned.embedSparseBatch(a, name, texts);
        };
        var sanitized = try sanitizeUtf8BatchForEmbeddingAlloc(a, texts);
        defer sanitized.deinit(a);
        const safe = sanitized.texts();
        const limit = if (generation.lease) |lease| if (lease.capabilities) |caps| @min(@max(safe.len, 1), caps.batch.max_items) else @max(safe.len, 1) else @max(safe.len, 1);
        if (limit == 0) return error.InvalidInferenceCapabilities;
        const output = try a.alloc(SparseEmbedding, safe.len);
        var initialized: usize = 0;
        errdefer {
            for (output[0..initialized]) |*vector| vector.deinit(a);
            a.free(output);
        }
        while (initialized < safe.len) {
            if (self.request_context) |context| try context.check();
            const end = initialized + @min(limit, safe.len - initialized);
            const batch = try execute(self.ptr, a, name, safe[initialized..end], self.request_context, generation);
            if (batch.len != end - initialized) {
                freeSparseEmbeddingBatch(a, batch);
                return error.InvalidEmbeddingResponse;
            }
            @memcpy(output[initialized..end], batch);
            a.free(batch);
            initialized = end;
            if (self.request_context) |context| try context.check();
        }
        return output;
    }

    captured_generation: ?Generation = null,
    resolve_generation_fn: ?*const fn (*anyopaque, Allocator, []const u8, ?RequestContext) anyerror!Generation = null,
    sparse_embed_batch_planned_fn: ?*const fn (*anyopaque, Allocator, []const u8, []const []const u8, ?RequestContext, Generation) anyerror![]SparseEmbedding = null,

    request_context: ?RequestContext = null,
    ptr: *anyopaque,
    sparse_embed_fn: SparseEmbedFn,
    sparse_embed_batch_fn: ?SparseEmbedBatchFn = null,
    sparse_embed_with_context_fn: ?SparseEmbedWithContextFn = null,
    sparse_embed_batch_with_context_fn: ?SparseEmbedBatchWithContextFn = null,
    deinit_fn: ?SparseEmbedDeinitFn = null,
    set_cancellation_fn: ?EmbedSetCancellationFn = null,
    set_progress_fn: ?EmbedSetProgressFn = null,
    cache_revision_with_context_fn: ?*const fn (*anyopaque, Allocator, []const u8, RequestContext) anyerror!?[32]u8 = null,
    cache_revision_fn: ?*const fn (*anyopaque, Allocator, []const u8) anyerror!?[32]u8 = null,
    recovery_identity_fn: ?EmbedRecoveryIdentityFn = null,
    foreground_bounded: bool = false,

    /// The provider generation behind this name, when it can report one.
    pub fn captureRevision(self: SparseEmbedder, a: Allocator, name: []const u8) !?[32]u8 {
        if (self.captured_generation) |generation| return generation.revision;
        if (self.resolve_generation_fn != null) return (try self.captureGeneration(a, name)).revision;
        return self.captureUnpinnedRevision(a, name);
    }

    fn captureUnpinnedRevision(self: SparseEmbedder, a: Allocator, name: []const u8) !?[32]u8 {
        if (self.request_context) |context| {
            try context.check();
            if (self.cache_revision_with_context_fn) |resolve| {
                const revision = try resolve(self.ptr, a, name, context);
                try context.check();
                return revision;
            }
            // A provider with no revision contract has nothing to discover.
            if (self.cache_revision_fn != null) return error.UncancellableInferenceProvider;
            return null;
        }
        const resolve = self.cache_revision_fn orelse return null;
        const revision = try resolve(self.ptr, a, name);
        if (self.request_context) |context| try context.check();
        return revision;
    }

    pub fn requireRevision(self: SparseEmbedder, a: Allocator, name: []const u8, revision: ?[32]u8) !void {
        var current = self;
        current.captured_generation = null;
        if (!std.meta.eql(revision, try current.captureRevision(a, name))) return error.EnrichmentSourceChanged;
    }

    pub fn embedSparse(self: SparseEmbedder, alloc: Allocator, embedding_name: []const u8, text: []const u8) !SparseEmbedding {
        if (self.captured_generation != null) {
            const batch = try self.embedPlannedBatch(alloc, embedding_name, &.{text});
            const result = batch[0];
            alloc.free(batch);
            return result;
        }
        if (self.request_context) |context| return self.embedSparseWithContext(alloc, embedding_name, text, context);
        var sanitized = try utf8_text.sanitizeWithoutSourceMapAlloc(alloc, text, "sparse embedder");
        defer sanitized.deinit(alloc);
        return try self.sparse_embed_fn(self.ptr, alloc, embedding_name, sanitized.text);
    }

    pub fn embedSparseBatch(
        self: SparseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
    ) ![]SparseEmbedding {
        if (self.captured_generation != null) return self.embedPlannedBatch(alloc, embedding_name, texts);
        if (self.request_context) |context| return self.embedSparseBatchWithContext(alloc, embedding_name, texts, context);
        var sanitized = try sanitizeUtf8BatchForEmbeddingAlloc(alloc, texts);
        defer sanitized.deinit(alloc);
        const safe_texts = sanitized.texts();
        const sparse_embed_batch_fn = self.sparse_embed_batch_fn orelse return try fallbackSparseBatch(self, alloc, embedding_name, safe_texts);
        return try sparse_embed_batch_fn(self.ptr, alloc, embedding_name, safe_texts);
    }

    pub fn embedSparseWithContext(
        self: SparseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        text: []const u8,
        context: RequestContext,
    ) !SparseEmbedding {
        if (self.captured_generation != null) return self.withRequestContext(context).embedSparse(alloc, embedding_name, text);
        try context.check();
        const embed = self.sparse_embed_with_context_fn orelse return error.UncancellableInferenceProvider;
        var sanitized = try utf8_text.sanitizeWithoutSourceMapAlloc(alloc, text, "sparse embedder");
        defer sanitized.deinit(alloc);
        var result = try embed(self.ptr, alloc, embedding_name, sanitized.text, context);
        context.check() catch |err| {
            result.deinit(alloc);
            return err;
        };
        return result;
    }

    pub fn embedSparseBatchWithContext(
        self: SparseEmbedder,
        alloc: Allocator,
        embedding_name: []const u8,
        texts: []const []const u8,
        context: RequestContext,
    ) ![]SparseEmbedding {
        if (self.captured_generation != null) return self.withRequestContext(context).embedPlannedBatch(alloc, embedding_name, texts);
        try context.check();
        var sanitized = try sanitizeUtf8BatchForEmbeddingAlloc(alloc, texts);
        defer sanitized.deinit(alloc);
        const safe_texts = sanitized.texts();
        const embeddings = if (self.sparse_embed_batch_with_context_fn) |embed_batch|
            try embed_batch(self.ptr, alloc, embedding_name, safe_texts, context)
        else if (self.sparse_embed_with_context_fn != null)
            try fallbackSparseBatchWithContext(self, alloc, embedding_name, safe_texts, context)
        else
            return error.UncancellableInferenceProvider;
        context.check() catch |err| {
            freeSparseEmbeddingBatch(alloc, embeddings);
            return err;
        };
        return embeddings;
    }

    pub fn deinit(self: SparseEmbedder, alloc: Allocator) void {
        const deinit_fn = self.deinit_fn orelse return;
        deinit_fn(self.ptr, alloc);
    }

    pub fn setCancellation(self: SparseEmbedder, cancellation: CancellationToken) void {
        const set_cancellation_fn = self.set_cancellation_fn orelse return;
        set_cancellation_fn(self.ptr, cancellation);
    }

    pub fn setProgress(self: SparseEmbedder, progress: inference_request_context.ProgressSink) void {
        const set_progress_fn = self.set_progress_fn orelse return;
        set_progress_fn(self.ptr, progress);
    }

    pub fn recoveryIdentity(self: SparseEmbedder, embedding_name: []const u8) RecoveryIdentity {
        const get_identity = self.recovery_identity_fn orelse return .{ .model = embedding_name };
        return get_identity(self.ptr, embedding_name) orelse .{ .model = embedding_name };
    }
};

pub const SanitizedTextBatch = struct {
    original: []const []const u8,
    sanitized: ?[][]const u8 = null,
    owned: ?[]?[]u8 = null,

    pub fn texts(self: @This()) []const []const u8 {
        return self.sanitized orelse self.original;
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.owned) |owned| {
            for (owned) |maybe_text| {
                if (maybe_text) |text| alloc.free(text);
            }
            alloc.free(owned);
        }
        if (self.sanitized) |sanitized| alloc.free(sanitized);
        self.* = undefined;
    }
};

const SanitizedContentParts = struct {
    original: []const template_mod.ContentPart,
    parts: ?[]template_mod.ContentPart = null,
    owned_texts: ?[]?[]u8 = null,

    fn partsSlice(self: @This()) []const template_mod.ContentPart {
        return self.parts orelse self.original;
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.owned_texts) |owned_texts| {
            for (owned_texts) |maybe_text| {
                if (maybe_text) |text| alloc.free(text);
            }
            alloc.free(owned_texts);
        }
        if (self.parts) |parts| alloc.free(parts);
        self.* = undefined;
    }
};

pub fn sanitizeUtf8BatchForEmbeddingAlloc(alloc: Allocator, texts: []const []const u8) !SanitizedTextBatch {
    var has_invalid = false;
    for (texts) |text| {
        if (!std.unicode.utf8ValidateSlice(text)) {
            has_invalid = true;
            break;
        }
    }
    if (!has_invalid) return .{ .original = texts };

    const sanitized = try alloc.alloc([]const u8, texts.len);
    errdefer alloc.free(sanitized);
    const owned = try alloc.alloc(?[]u8, texts.len);
    @memset(owned, null);
    errdefer {
        for (owned) |maybe_text| {
            if (maybe_text) |text| alloc.free(text);
        }
        alloc.free(owned);
    }

    for (texts, 0..) |text, i| {
        if (std.unicode.utf8ValidateSlice(text)) {
            sanitized[i] = text;
            continue;
        }
        const safe_text = try utf8_text.replacementAlloc(alloc, text, "embedding batch");
        owned[i] = safe_text;
        sanitized[i] = safe_text;
    }

    return .{
        .original = texts,
        .sanitized = sanitized,
        .owned = owned,
    };
}

fn sanitizeContentPartsForEmbeddingAlloc(alloc: Allocator, parts: []const template_mod.ContentPart) !SanitizedContentParts {
    var has_invalid = false;
    for (parts) |part| {
        switch (part) {
            .text => |text| {
                if (!std.unicode.utf8ValidateSlice(text)) {
                    has_invalid = true;
                    break;
                }
            },
            .media_url, .binary => {},
        }
    }
    if (!has_invalid) return .{ .original = parts };

    const sanitized_parts = try alloc.alloc(template_mod.ContentPart, parts.len);
    errdefer alloc.free(sanitized_parts);
    const owned_texts = try alloc.alloc(?[]u8, parts.len);
    @memset(owned_texts, null);
    errdefer {
        for (owned_texts) |maybe_text| {
            if (maybe_text) |text| alloc.free(text);
        }
        alloc.free(owned_texts);
    }

    for (parts, 0..) |part, i| {
        switch (part) {
            .text => |text| {
                if (std.unicode.utf8ValidateSlice(text)) {
                    sanitized_parts[i] = part;
                    continue;
                }
                const safe_text = try utf8_text.replacementAlloc(alloc, text, "embedding content parts");
                owned_texts[i] = safe_text;
                sanitized_parts[i] = .{ .text = safe_text };
            },
            .media_url, .binary => sanitized_parts[i] = part,
        }
    }

    return .{
        .original = parts,
        .parts = sanitized_parts,
        .owned_texts = owned_texts,
    };
}

fn takeSingleDenseVector(alloc: Allocator, vectors: []const []const f32) ![]f32 {
    if (vectors.len != 1) {
        freeDenseEmbeddingBatch(alloc, vectors);
        return error.InvalidEmbeddingResponse;
    }
    const vector = vectors[0];
    alloc.free(vectors);
    return @constCast(vector);
}

pub fn freeDenseEmbeddingBatch(alloc: Allocator, batch: []const []const f32) void {
    for (batch) |vector| alloc.free(@constCast(vector));
    alloc.free(@constCast(batch));
}

pub fn freeSparseEmbeddingBatch(alloc: Allocator, batch: []SparseEmbedding) void {
    for (batch) |*embedding| embedding.deinit(alloc);
    alloc.free(batch);
}

fn fallbackDenseBatch(
    self: DenseEmbedder,
    alloc: Allocator,
    embedding_name: []const u8,
    texts: []const []const u8,
    dims: u32,
) ![]const []const f32 {
    const batch = try alloc.alloc([]const f32, texts.len);
    var initialized: usize = 0;
    errdefer {
        for (batch[0..initialized]) |vector| alloc.free(@constCast(vector));
        alloc.free(batch);
    }
    for (texts, 0..) |text, i| {
        // Texts were sanitized by the batch boundary. This path explicitly
        // permits synchronous background execution; retain request checks
        // without routing back into the foreground-only scalar adapter.
        if (self.part_request_context) |context| try context.check();
        batch[i] = try self.accountedTextDispatch(self.dense_embed_fn, .{ self.ptr, alloc, embedding_name, text, dims });
        initialized += 1;
    }
    return batch;
}

fn fallbackDenseBatchWithContext(
    self: DenseEmbedder,
    alloc: Allocator,
    embedding_name: []const u8,
    texts: []const []const u8,
    dims: u32,
    context: RequestContext,
) ![]const []const f32 {
    const batch = try alloc.alloc([]const f32, texts.len);
    var initialized: usize = 0;
    errdefer {
        for (batch[0..initialized]) |vector| alloc.free(@constCast(vector));
        alloc.free(batch);
    }
    for (texts, 0..) |text, i| {
        try context.update(.executing, i, texts.len);
        batch[i] = try self.accountedTextDispatch(DenseEmbedder.embedDenseWithContext, .{ self, alloc, embedding_name, text, dims, context });
        initialized += 1;
    }
    return batch;
}

fn fallbackSparseBatch(
    self: SparseEmbedder,
    alloc: Allocator,
    embedding_name: []const u8,
    texts: []const []const u8,
) ![]SparseEmbedding {
    const batch = try alloc.alloc(SparseEmbedding, texts.len);
    var initialized: usize = 0;
    errdefer {
        for (batch[0..initialized]) |*embedding| embedding.deinit(alloc);
        alloc.free(batch);
    }
    for (texts, 0..) |text, i| {
        batch[i] = try self.embedSparse(alloc, embedding_name, text);
        initialized += 1;
    }
    return batch;
}

fn fallbackSparseBatchWithContext(
    self: SparseEmbedder,
    alloc: Allocator,
    embedding_name: []const u8,
    texts: []const []const u8,
    context: RequestContext,
) ![]SparseEmbedding {
    const batch = try alloc.alloc(SparseEmbedding, texts.len);
    var initialized: usize = 0;
    errdefer {
        for (batch[0..initialized]) |*embedding| embedding.deinit(alloc);
        alloc.free(batch);
    }
    for (texts, 0..) |text, i| {
        try context.update(.executing, i, texts.len);
        batch[i] = try self.embedSparseWithContext(alloc, embedding_name, text, context);
        initialized += 1;
    }
    return batch;
}

pub const DeterministicDenseEmbedder = struct {
    seed: u64 = 0xcbf29ce484222325,

    pub fn embedDense(ptr: *anyopaque, alloc: Allocator, _: []const u8, text: []const u8, dims: u32) ![]f32 {
        const self: *DeterministicDenseEmbedder = @ptrCast(@alignCast(ptr));
        const values = try alloc.alloc(f32, dims);
        errdefer alloc.free(values);

        var hash = self.seed;
        for (text) |byte| {
            hash = (hash ^ byte) *% 0x100000001b3;
        }

        for (values, 0..) |*value, i| {
            hash = (hash ^ @as(u64, @intCast(i + 1))) *% 0x9e3779b185ebca87;
            const lane: u32 = @intCast((hash >> 16) & 0xffff);
            value.* = @as(f32, @floatFromInt(lane)) / 65535.0;
        }
        return values;
    }

    pub fn interface(self: *DeterministicDenseEmbedder) DenseEmbedder {
        return .{
            .ptr = self,
            .dense_embed_fn = embedDense,
            .deinit_fn = null,
        };
    }
};

pub const DeterministicSparseEmbedder = struct {
    seed: u64 = 0xcbf29ce484222325,

    pub fn embedSparse(ptr: *anyopaque, alloc: Allocator, _: []const u8, text: []const u8) !SparseEmbedding {
        const self: *DeterministicSparseEmbedder = @ptrCast(@alignCast(ptr));

        var hash = self.seed;
        for (text) |byte| {
            hash = (hash ^ byte) *% 0x100000001b3;
        }

        const indices = try alloc.alloc(u32, 2);
        errdefer alloc.free(indices);
        const values = try alloc.alloc(f32, 2);
        errdefer alloc.free(values);

        indices[0] = @intCast((hash >> 16) % 1024);
        hash = (hash ^ 0x9e3779b185ebca87) *% 0x100000001b3;
        indices[1] = @intCast((hash >> 24) % 1024);
        if (indices[1] == indices[0]) indices[1] = (indices[1] + 1) % 1024;
        if (indices[1] < indices[0]) std.mem.swap(u32, &indices[0], &indices[1]);

        values[0] = @as(f32, @floatFromInt((hash >> 8) & 0xffff)) / 65535.0;
        hash = (hash ^ 0x517cc1b727220a95) *% 0x9e3779b185ebca87;
        values[1] = @as(f32, @floatFromInt((hash >> 12) & 0xffff)) / 65535.0;
        return .{
            .indices = indices,
            .values = values,
        };
    }

    pub fn interface(self: *DeterministicSparseEmbedder) SparseEmbedder {
        return .{
            .ptr = self,
            .sparse_embed_fn = embedSparse,
            .deinit_fn = null,
        };
    }
};

test "deterministic dense embedder is stable for same input" {
    const alloc = std.testing.allocator;
    var embedder = DeterministicDenseEmbedder{};
    const iface = embedder.interface();

    const a = try iface.embedDense(alloc, "", "hello world", 4);
    defer alloc.free(a);
    const b = try iface.embedDense(alloc, "", "hello world", 4);
    defer alloc.free(b);

    try std.testing.expectEqual(@as(usize, 4), a.len);
    try std.testing.expectEqualSlices(f32, a, b);
}

test "deterministic dense embedder batch fallback is stable" {
    const alloc = std.testing.allocator;
    var embedder = DeterministicDenseEmbedder{};
    const iface = embedder.interface();

    const batch = try iface.embedDenseBatch(alloc, "", &.{ "hello world", "zig batch" }, 4);
    defer freeDenseEmbeddingBatch(alloc, batch);

    try std.testing.expectEqual(@as(usize, 2), batch.len);
    try std.testing.expectEqual(@as(usize, 4), batch[0].len);
    const single = try iface.embedDense(alloc, "", "hello world", 4);
    defer alloc.free(single);
    try std.testing.expectEqualSlices(f32, single, batch[0]);
}

test "deterministic sparse embedder is stable for same input" {
    const alloc = std.testing.allocator;
    var embedder = DeterministicSparseEmbedder{};
    const iface = embedder.interface();

    var a = try iface.embedSparse(alloc, "", "hello world");
    defer a.deinit(alloc);
    var b = try iface.embedSparse(alloc, "", "hello world");
    defer b.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), a.indices.len);
    try std.testing.expectEqualSlices(u32, a.indices, b.indices);
    try std.testing.expectEqualSlices(f32, a.values, b.values);
}

test "deterministic sparse embedder batch fallback is stable" {
    const alloc = std.testing.allocator;
    var embedder = DeterministicSparseEmbedder{};
    const iface = embedder.interface();

    const batch = try iface.embedSparseBatch(alloc, "", &.{ "hello world", "zig batch" });
    defer freeSparseEmbeddingBatch(alloc, batch);

    try std.testing.expectEqual(@as(usize, 2), batch.len);
    var single = try iface.embedSparse(alloc, "", "hello world");
    defer single.deinit(alloc);
    try std.testing.expectEqualSlices(u32, single.indices, batch[0].indices);
    try std.testing.expectEqualSlices(f32, single.values, batch[0].values);
}

const Utf8AssertingEmbedder = struct {
    saw_replacement: bool = false,

    fn dense(ptr: *anyopaque, alloc: Allocator, _: []const u8, text: []const u8, dims: u32) ![]f32 {
        const self: *Utf8AssertingEmbedder = @ptrCast(@alignCast(ptr));
        try std.testing.expect(std.unicode.utf8ValidateSlice(text));
        if (std.mem.indexOf(u8, text, &std.unicode.replacement_character_utf8) != null) self.saw_replacement = true;
        const vector = try alloc.alloc(f32, dims);
        @memset(vector, 1.0);
        return vector;
    }

    fn sparseBatch(ptr: *anyopaque, alloc: Allocator, _: []const u8, texts: []const []const u8) ![]SparseEmbedding {
        const self: *Utf8AssertingEmbedder = @ptrCast(@alignCast(ptr));
        const out = try alloc.alloc(SparseEmbedding, texts.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*embedding| embedding.deinit(alloc);
            alloc.free(out);
        }
        for (texts, 0..) |text, i| {
            try std.testing.expect(std.unicode.utf8ValidateSlice(text));
            if (std.mem.indexOf(u8, text, &std.unicode.replacement_character_utf8) != null) self.saw_replacement = true;
            const indices = try alloc.dupe(u32, &.{1});
            const values = alloc.dupe(f32, &.{1.0}) catch |err| {
                alloc.free(indices);
                return err;
            };
            out[i] = .{
                .indices = indices,
                .values = values,
            };
            initialized += 1;
        }
        return out;
    }

    fn denseParts(ptr: *anyopaque, alloc: Allocator, _: []const u8, parts: []const template_mod.ContentPart, dims: u32) ![]f32 {
        const self: *Utf8AssertingEmbedder = @ptrCast(@alignCast(ptr));
        for (parts) |part| {
            switch (part) {
                .text => |text| {
                    try std.testing.expect(std.unicode.utf8ValidateSlice(text));
                    if (std.mem.indexOf(u8, text, &std.unicode.replacement_character_utf8) != null) self.saw_replacement = true;
                },
                .media_url, .binary => {},
            }
        }
        const vector = try alloc.alloc(f32, dims);
        @memset(vector, 1.0);
        return vector;
    }

    fn denseInterface(self: *Utf8AssertingEmbedder) DenseEmbedder {
        return .{
            .ptr = self,
            .dense_embed_fn = dense,
            .dense_embed_parts_fn = denseParts,
        };
    }

    fn sparseInterface(self: *Utf8AssertingEmbedder) SparseEmbedder {
        return .{
            .ptr = self,
            .sparse_embed_fn = sparse,
            .sparse_embed_batch_fn = sparseBatch,
        };
    }

    fn sparse(ptr: *anyopaque, alloc: Allocator, embedding_name: []const u8, text: []const u8) !SparseEmbedding {
        const batch = try sparseBatch(ptr, alloc, embedding_name, &.{text});
        defer alloc.free(batch);
        return batch[0];
    }
};

test "enrichment dense embedder replaces invalid utf8 before provider call" {
    const alloc = std.testing.allocator;
    var embedder = Utf8AssertingEmbedder{};
    const vector = try embedder.denseInterface().embedDense(alloc, "", "alpha\xc2 beta", 1);
    defer alloc.free(vector);

    try std.testing.expect(embedder.saw_replacement);
}

test "enrichment sparse batch embedder replaces invalid utf8 before provider call" {
    const alloc = std.testing.allocator;
    var embedder = Utf8AssertingEmbedder{};
    const batch = try embedder.sparseInterface().embedSparseBatch(alloc, "", &.{ "valid", "alpha\xc2 beta" });
    defer freeSparseEmbeddingBatch(alloc, batch);

    try std.testing.expect(embedder.saw_replacement);
}

test "enrichment dense parts embedder replaces invalid text part utf8 before provider call" {
    const alloc = std.testing.allocator;
    var embedder = Utf8AssertingEmbedder{};
    const parts = [_]template_mod.ContentPart{.{ .text = "alpha\xc2 beta" }};
    const vector = try embedder.denseInterface().embedDenseParts(alloc, "", &parts, 1);
    defer alloc.free(vector);

    try std.testing.expect(embedder.saw_replacement);
}

test "media part item embedding partitions response limited batches with ordered ownership" {
    const Harness = struct {
        calls: usize = 0,
        fail_second: bool = false,
        fn dense(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
            return error.UnexpectedEmbeddingRoute;
        }
        fn limit(_: *anyopaque, _: []const u8, _: u32, _: inference_work.CapabilityLease, requested: usize) !usize {
            return @min(requested, 2);
        }
        fn memory(_: *anyopaque, _: []const u8, shape: DensePartInvocationShape, _: u32, _: ?inference_work.InferenceCapabilities) !DensePartInvocationMemory {
            try std.testing.expect(shape.item_count <= 2);
            return .{ .attachment_transport = .borrowed_binary, .fixed_bytes = 4096, .allocator_limit_bytes = 4096, .max_result_bytes_per_item = 4, .max_result_bytes = shape.item_count * 4 };
        }
        fn items(ptr: *anyopaque, alloc: Allocator, _: []const u8, parts: []const template_mod.ContentPart, _: u32) ![]const []const f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.fail_second and self.calls == 2) return error.QueueFull;
            const vectors = try alloc.alloc([]const f32, parts.len);
            var initialized: usize = 0;
            errdefer {
                for (vectors[0..initialized]) |vector| alloc.free(vector);
                alloc.free(vectors);
            }
            for (parts, vectors) |part, *vector| {
                vector.* = try alloc.dupe(f32, &.{@floatFromInt(part.binary.data[0])});
                initialized += 1;
            }
            return vectors;
        }
        fn verify(alloc: Allocator, fail_second: bool) !void {
            var harness = @This(){ .fail_second = fail_second };
            const embedder = DenseEmbedder{ .ptr = &harness, .dense_embed_fn = dense, .dense_embed_part_items_fn = items, .part_invocation_memory_fn = memory, .part_batch_limit_fn = limit, .part_lease = .{ .capabilities = .{ .task = .embed, .input_modalities = .{ .image = true }, .input_granularity = .page, .output = .embedding, .batch = .{ .max_items = 4 } } } };
            const parts = [_]template_mod.ContentPart{
                .{ .binary = .{ .data = &.{1}, .mime_type = "image/png" } },
                .{ .binary = .{ .data = &.{2}, .mime_type = "image/png" } },
                .{ .binary = .{ .data = &.{3}, .mime_type = "image/png" } },
                .{ .binary = .{ .data = &.{4}, .mime_type = "image/png" } },
                .{ .binary = .{ .data = &.{5}, .mime_type = "image/png" } },
            };
            const vectors = embedder.embedDensePartItems(alloc, "visual", &parts, 1) catch |err| {
                if (fail_second and err == error.QueueFull) return;
                return err;
            };
            defer freeDenseEmbeddingBatch(alloc, vectors);
            try std.testing.expect(!fail_second);
            try std.testing.expectEqual(@as(usize, 3), harness.calls);
            try std.testing.expectEqual(parts.len, vectors.len);
            for (vectors, 1..) |vector, i| try std.testing.expectEqual(@as(f32, @floatFromInt(i)), vector[0]);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.verify, .{false});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.verify, .{true});
}

test "media part item embedding fails closed without an invocation contract" {
    const Stub = struct {
        fn dense(_: *anyopaque, alloc: Allocator, _: []const u8, _: []const u8, dims: u32) ![]f32 {
            return try alloc.alloc(f32, dims);
        }

        fn items(_: *anyopaque, alloc: Allocator, _: []const u8, parts: []const template_mod.ContentPart, dims: u32) ![]const []const f32 {
            const out = try alloc.alloc([]const f32, parts.len);
            errdefer alloc.free(out);
            for (out, 0..) |*vector, i| {
                vector.* = alloc.alloc(f32, dims) catch |err| {
                    for (out[0..i]) |initialized| alloc.free(initialized);
                    return err;
                };
            }
            return out;
        }
    };
    var context: u8 = 0;
    const embedder: DenseEmbedder = .{
        .ptr = &context,
        .dense_embed_fn = Stub.dense,
        .dense_embed_part_items_fn = Stub.items,
    };
    const parts = [_]template_mod.ContentPart{.{
        .binary = .{ .mime_type = "image/png", .data = &.{1} },
    }};
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        embedder.embedDensePartItems(std.testing.allocator, "images", &parts, 1),
    );
    const url_parts = [_]template_mod.ContentPart{.{
        .media_url = "DATA:image/png;BASE64,AQ==",
    }};
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        embedder.embedDensePartItems(std.testing.allocator, "images", &url_parts, 1),
    );
}

test "media part item embedding enforces its result contract" {
    const Stub = struct {
        fn dense(_: *anyopaque, alloc: Allocator, _: []const u8, _: []const u8, dims: u32) ![]f32 {
            return try alloc.alloc(f32, dims);
        }

        fn items(_: *anyopaque, alloc: Allocator, _: []const u8, parts: []const template_mod.ContentPart, dims: u32) ![]const []const f32 {
            const out = try alloc.alloc([]const f32, parts.len);
            errdefer alloc.free(out);
            for (out, 0..) |*vector, i| {
                vector.* = alloc.alloc(f32, dims) catch |err| {
                    for (out[0..i]) |initialized| alloc.free(initialized);
                    return err;
                };
            }
            return out;
        }

        fn memory(_: *anyopaque, _: []const u8, _: DensePartInvocationShape, _: u32, _: ?inference_work.InferenceCapabilities) !DensePartInvocationMemory {
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 64,
                .allocator_limit_bytes = 64,
                .max_result_bytes_per_item = 3,
                .max_result_bytes = 3,
            };
        }
    };
    var context: u8 = 0;
    const embedder: DenseEmbedder = .{
        .ptr = &context,
        .dense_embed_fn = Stub.dense,
        .dense_embed_part_items_fn = Stub.items,
        .part_invocation_memory_fn = Stub.memory,
    };
    const parts = [_]template_mod.ContentPart{.{
        .binary = .{ .mime_type = "image/png", .data = &.{1} },
    }};
    try std.testing.expectError(
        error.InferenceResultTooLarge,
        embedder.embedDensePartItems(std.testing.allocator, "images", &parts, 1),
    );
    const url_parts = [_]template_mod.ContentPart{.{
        .media_url = "DATA:image/png;BASE64,AQ==",
    }};
    try std.testing.expectError(
        error.InferenceResultTooLarge,
        embedder.embedDensePartItems(std.testing.allocator, "images", &url_parts, 1),
    );
}

test "media part item embedding validates each vector shape and values" {
    const Context = struct { non_finite: bool = false };
    const Stub = struct {
        fn dense(_: *anyopaque, alloc: Allocator, _: []const u8, _: []const u8, dims: u32) ![]f32 {
            return try alloc.alloc(f32, dims);
        }

        fn items(ptr: *anyopaque, alloc: Allocator, _: []const u8, _: []const template_mod.ContentPart, _: u32) ![]const []const f32 {
            const context: *Context = @ptrCast(@alignCast(ptr));
            const out = try alloc.alloc([]const f32, 2);
            errdefer alloc.free(out);
            out[0] = try alloc.alloc(f32, if (context.non_finite) 2 else 1);
            errdefer alloc.free(@constCast(out[0]));
            out[1] = try alloc.alloc(f32, if (context.non_finite) 2 else 3);
            @memset(@constCast(out[0]), 0);
            @memset(@constCast(out[1]), 0);
            if (context.non_finite) @constCast(out[1])[0] = std.math.nan(f32);
            return out;
        }

        fn memory(_: *anyopaque, _: []const u8, _: DensePartInvocationShape, _: u32, _: ?inference_work.InferenceCapabilities) !DensePartInvocationMemory {
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 256,
                .allocator_limit_bytes = 256,
                .max_result_bytes_per_item = 16,
                .max_result_bytes = 32,
            };
        }
    };
    var context = Context{};
    const embedder: DenseEmbedder = .{
        .ptr = &context,
        .dense_embed_fn = Stub.dense,
        .dense_embed_part_items_fn = Stub.items,
        .part_invocation_memory_fn = Stub.memory,
    };
    const parts = [_]template_mod.ContentPart{
        .{ .binary = .{ .mime_type = "image/png", .data = &.{1} } },
        .{ .binary = .{ .mime_type = "image/jpeg", .data = &.{2} } },
    };
    try std.testing.expectError(
        error.InvalidEmbeddingDimensions,
        embedder.embedDensePartItems(std.testing.allocator, "images", &parts, 2),
    );
    context.non_finite = true;
    try std.testing.expectError(
        error.InvalidEmbeddingResponse,
        embedder.embedDensePartItems(std.testing.allocator, "images", &parts, 2),
    );
}

test "media part item embedding bounds boundary allocations under executor admission" {
    const Stub = struct {
        fn dense(_: *anyopaque, alloc: Allocator, _: []const u8, _: []const u8, dims: u32) ![]f32 {
            return try alloc.alloc(f32, dims);
        }

        fn items(_: *anyopaque, alloc: Allocator, _: []const u8, parts: []const template_mod.ContentPart, dims: u32) ![]const []const f32 {
            const scratch = try alloc.alloc(u8, 4096);
            defer alloc.free(scratch);
            const out = try alloc.alloc([]const f32, parts.len);
            errdefer alloc.free(out);
            for (out, 0..) |*vector, i| {
                vector.* = alloc.alloc(f32, dims) catch |err| {
                    for (out[0..i]) |initialized| alloc.free(initialized);
                    return err;
                };
                @memset(@constCast(vector.*), 0);
            }
            return out;
        }

        fn memory(_: *anyopaque, _: []const u8, shape: DensePartInvocationShape, dims: u32, _: ?inference_work.InferenceCapabilities) !DensePartInvocationMemory {
            const per_item = @as(usize, dims) * @sizeOf(f32);
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 64,
                .allocator_limit_bytes = 64,
                .allocator_owner = .executor,
                .max_result_bytes_per_item = per_item,
                .max_result_bytes = per_item * shape.item_count,
            };
        }
    };
    var context: u8 = 0;
    const embedder: DenseEmbedder = .{
        .ptr = &context,
        .dense_embed_fn = Stub.dense,
        .dense_embed_part_items_fn = Stub.items,
        .part_invocation_memory_fn = Stub.memory,
    };
    const parts = [_]template_mod.ContentPart{.{
        .binary = .{ .mime_type = "image/png; charset=binary", .data = &.{1} },
    }};
    try std.testing.expectError(
        error.InferenceInvocationMemoryExceeded,
        embedder.embedDensePartItems(std.testing.allocator, "images", &parts, 1),
    );
}

test "media part item embedding planning preserves heterogeneous MIME accounting" {
    const parts = [_]template_mod.ContentPart{
        .{ .binary = .{ .mime_type = "image/png", .data = &.{1} } },
        .{ .binary = .{ .mime_type = "audio/wav; codecs=pcm", .data = &.{2} } },
    };
    const shape = try densePartInvocationShape(&parts);
    try std.testing.expectEqual(@as(usize, 2), shape.item_count);
    try std.testing.expect(shape.string_json_bytes >= "image/png".len + "audio/wav; codecs=pcm".len);
    try std.testing.expectEqual(@as(usize, 0), shape.preparation_bytes);

    const invalid_text = [_]template_mod.ContentPart{.{ .text = &.{0xff} }};
    const repaired_shape = try densePartInvocationShape(&invalid_text);
    try std.testing.expect(repaired_shape.preparation_bytes >=
        @sizeOf(template_mod.ContentPart) + @sizeOf(?[]u8) + 3);
}

test "context-aware embedder receives the request lifetime and fails closed when absent" {
    const Probe = struct {
        calls: usize = 0,

        fn legacy(_: *anyopaque, alloc: Allocator, _: []const u8, _: []const u8, dims: u32) ![]f32 {
            return try alloc.alloc(f32, dims);
        }

        fn controlled(
            ptr: *anyopaque,
            alloc: Allocator,
            _: []const u8,
            _: []const u8,
            dims: u32,
            context: RequestContext,
        ) ![]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.check();
            try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), context.deadline_ns);
            self.calls += 1;
            return try alloc.alloc(f32, dims);
        }
    };

    var probe = Probe{};
    const controlled = DenseEmbedder{
        .ptr = &probe,
        .dense_embed_fn = Probe.legacy,
        .dense_embed_with_context_fn = Probe.controlled,
    };
    const context = RequestContext{
        .io = std.testing.io,
        .deadline_ns = std.math.maxInt(u64),
    };
    const vector = try controlled.embedDenseWithContext(std.testing.allocator, "model", "text", 3, context);
    defer std.testing.allocator.free(vector);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);

    const legacy = DenseEmbedder{ .ptr = &probe, .dense_embed_fn = Probe.legacy };
    try std.testing.expectError(
        error.UncancellableInferenceProvider,
        legacy.embedDenseWithContext(std.testing.allocator, "model", "text", 3, context),
    );
}

test "dense text batch boundary preserves limits order cancellation and allocation cleanup" {
    const Probe = struct {
        fn single(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
            return error.UnexpectedSingleCall;
        }
        fn capabilities(_: *anyopaque, _: Allocator, _: []const u8) !inference_work.InferenceCapabilities {
            return .{ .task = .embed, .input_modalities = .{ .text = true }, .accepted_mime_types = .{ .text_plain = true }, .input_granularity = .chunk, .output = .embedding, .batch = .{ .mode = .serial_compatibility, .max_items = 1 } };
        }
        fn controlledCapabilities(ptr: *anyopaque, a: Allocator, name: []const u8, context: RequestContext) !inference_work.InferenceCapabilities {
            try context.check();
            return capabilities(ptr, a, name);
        }
        fn batch(_: *anyopaque, a: Allocator, _: []const u8, texts: []const []const u8, _: u32) ![]const []const f32 {
            try std.testing.expectEqual(@as(usize, 1), texts.len);
            if (std.mem.eql(u8, texts[0], "fail")) return error.EmbedRequestFailed;
            const rows = try a.alloc([]const f32, 1);
            errdefer a.free(rows);
            rows[0] = try a.dupe(f32, &.{try std.fmt.parseFloat(f32, texts[0])});
            return rows;
        }
        fn controlled(ptr: *anyopaque, a: Allocator, name: []const u8, texts: []const []const u8, dims: u32, context: RequestContext) ![]const []const f32 {
            try context.check();
            const rows = try batch(ptr, a, name, texts, dims);
            if (context.cancellation) |token| {
                const signal: *const std.atomic.Value(bool) = @ptrCast(@alignCast(token.ptr.?));
                @constCast(signal).store(true, .release);
            }
            return rows;
        }
        fn provider() DenseEmbedder {
            return .{ .ptr = undefined, .dense_embed_fn = single, .dense_embed_batch_fn = batch, .dense_embed_batch_with_context_fn = controlled, .capabilities_fn = capabilities, .capabilities_with_context_fn = controlledCapabilities };
        }
        fn sweep(a: Allocator) !void {
            const rows = try provider().embedDenseBatch(a, "model", &.{ "1", "2", "3" }, 1);
            defer freeDenseEmbeddingBatch(a, rows);
            for (rows, 1..) |row, i| try std.testing.expectEqual(@as(f32, @floatFromInt(i)), row[0]);
        }
    };
    const a = std.testing.allocator;
    try std.testing.checkAllAllocationFailures(a, Probe.sweep, .{});
    const provider = Probe.provider();
    var canceled = std.atomic.Value(bool).init(false);
    try std.testing.expectError(error.Cancelled, provider.embedDenseBatchWithContext(a, "model", &.{ "1", "2", "3" }, 1, .{
        .io = std.testing.io,
        .deadline_ns = null,
        .cancellation = CancellationToken.fromAtomic(&canceled),
    }));
    try std.testing.expect(canceled.load(.acquire));
    try std.testing.expectError(error.EmbedRequestFailed, provider.embedDenseBatch(a, "model", &.{ "1", "fail", "3" }, 1));
    try std.testing.expectError(error.EmbedRequestFailed, provider.embedDenseBatchWithContext(a, "model", &.{ "1", "fail", "3" }, 1, .{ .io = std.testing.io, .deadline_ns = null }));
    const rows = try provider.embedDenseBatchWithContext(a, "model", &.{ "1", "2", "3" }, 1, .{ .io = std.testing.io, .deadline_ns = null });
    defer freeDenseEmbeddingBatch(a, rows);
    for (rows, 1..) |row, i| try std.testing.expectEqual(@as(f32, @floatFromInt(i)), row[0]);
}

test "dense text planning retains metadata-only custom provider compatibility" {
    const P = struct {
        fn single(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
            return error.UnexpectedSingle;
        }
        fn caps(_: *anyopaque, _: Allocator, _: []const u8) !inference_work.InferenceCapabilities {
            return .{ .task = .embed, .input_modalities = .{ .text = true }, .accepted_mime_types = .{ .text_plain = true }, .input_granularity = .chunk, .output = .embedding, .batch = .{ .mode = .native, .preferred_items = 2, .max_items = 2 } };
        }
        fn batch(_: *anyopaque, a: Allocator, _: []const u8, texts: []const []const u8, _: u32, context: RequestContext) ![]const []const f32 {
            try context.check();
            const rows = try a.alloc([]const f32, texts.len);
            var initialized: usize = 0;
            errdefer {
                for (rows[0..initialized]) |row| a.free(row);
                a.free(rows);
            }
            for (rows) |*row| {
                row.* = try a.dupe(f32, &.{1});
                initialized += 1;
            }
            return rows;
        }
    };
    const provider = DenseEmbedder{ .ptr = undefined, .dense_embed_fn = P.single, .dense_embed_batch_with_context_fn = P.batch, .capabilities_fn = P.caps, .foreground_bounded = true };
    const rows = try provider.embedDenseBatchWithContext(std.testing.allocator, "model", &.{ "a", "b" }, 1, .{ .io = std.testing.io, .deadline_ns = null });
    defer freeDenseEmbeddingBatch(std.testing.allocator, rows);
    try std.testing.expectEqual(@as(usize, 2), rows.len);
}

test "commit requirements dense text rejects revision-bearing legacy execution before dispatch" {
    const P = struct {
        revision: u8 = 1,
        calls: usize = 0,
        resolutions: usize = 0,
        fn single(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
            return error.UnexpectedSingle;
        }
        fn caps(ptr: *anyopaque, _: Allocator, _: []const u8) !inference_work.InferenceCapabilities {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .task = .embed, .input_modalities = .{ .text = true }, .accepted_mime_types = .{ .text_plain = true }, .input_granularity = .chunk, .output = .embedding, .batch = .{ .mode = .serial_compatibility, .max_items = 1 }, .model_revision = @as([32]u8, @splat(self.revision)) };
        }
        fn resolve(ptr: *anyopaque, a: Allocator, name: []const u8, context: ?RequestContext) !inference_work.CapabilityLease {
            if (context) |request| try request.check();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.resolutions += 1;
            return .{ .capabilities = try caps(ptr, a, name) };
        }
        fn controlled(ptr: *anyopaque, a: Allocator, name: []const u8, texts: []const []const u8, dims: u32, context: RequestContext) ![]const []const f32 {
            try context.check();
            return batch(ptr, a, name, texts, dims);
        }
        fn batch(ptr: *anyopaque, a: Allocator, _: []const u8, texts: []const []const u8, _: u32) ![]const []const f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), texts.len);
            const rows = try a.alloc([]const f32, 1);
            errdefer a.free(rows);
            rows[0] = try a.dupe(f32, &.{@floatFromInt(self.revision)});
            self.revision += 1;
            self.calls += 1;
            return rows;
        }
    };
    const a = std.testing.allocator;
    // Legacy execution cannot consume a pinned lease. Reject both simple
    // metadata snapshots and controlled resolvers, for split and unsplit calls.
    for ([_]bool{ false, true }) |resolve_lease| {
        for (0..3) |mode| {
            for ([_]usize{ 1, 2 }) |count| {
                var p = P{};
                const provider = DenseEmbedder{ .ptr = &p, .dense_embed_fn = P.single, .dense_embed_batch_fn = P.batch, .dense_embed_batch_with_context_fn = P.controlled, .capabilities_fn = if (resolve_lease) null else P.caps, .resolve_part_lease_fn = if (resolve_lease) P.resolve else null };
                const texts = [_][]const u8{ "a", "b" };
                const result = switch (mode) {
                    0 => provider.embedDenseBatch(a, "model", texts[0..count], 1),
                    1 => provider.embedDenseBatchWithContext(a, "model", texts[0..count], 1, .{ .io = std.testing.io, .deadline_ns = null }),
                    else => provider.embedDenseBatchBackgroundWithContext(a, "model", texts[0..count], 1, .{ .io = std.testing.io, .deadline_ns = null }),
                };
                if (result) |rows| {
                    freeDenseEmbeddingBatch(a, rows);
                    return error.ExpectedGenerationFence;
                } else |err| try std.testing.expectEqual(error.CommitRequirementUnavailable, err);
                try std.testing.expectEqual(@as(usize, 0), p.calls);
                try std.testing.expectEqual(@as(usize, if (resolve_lease) 1 else 0), p.resolutions);
            }
        }
    }
}

test "dense text metadata checks cancellation before and after its snapshot" {
    const P = struct {
        canceled: std.atomic.Value(bool) = .init(false),
        calls: usize = 0,
        fn single(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
            return error.UnexpectedExecution;
        }
        fn metadata(ptr: *anyopaque, _: Allocator, _: []const u8) !inference_work.InferenceCapabilities {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.canceled.store(true, .release);
            return .{ .task = .embed, .input_modalities = .{ .text = true }, .input_granularity = .chunk, .output = .embedding };
        }
    };
    var p = P{};
    const provider = DenseEmbedder{ .ptr = &p, .dense_embed_fn = P.single, .capabilities_fn = P.metadata };
    const context = RequestContext{ .io = std.testing.io, .deadline_ns = null, .cancellation = CancellationToken.fromAtomic(&p.canceled) };
    try std.testing.expectError(error.Cancelled, provider.captureTextGenerationWithContext(std.testing.allocator, "model", context));
    try std.testing.expectEqual(@as(usize, 1), p.calls);
    try std.testing.expectError(error.Cancelled, provider.captureTextGenerationWithContext(std.testing.allocator, "model", context));
    try std.testing.expectEqual(@as(usize, 1), p.calls);
}

test "dense capability snapshots prefer controlled leases over legacy discovery" {
    const P = struct {
        calls: usize = 0,
        fn single(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
            return error.UnexpectedExecution;
        }
        fn legacy(_: *anyopaque, _: Allocator, _: []const u8) !inference_work.InferenceCapabilities {
            return error.UncontrolledDiscovery;
        }
        fn resolve(ptr: *anyopaque, _: Allocator, _: []const u8, context: ?RequestContext) !inference_work.CapabilityLease {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try (context orelse return error.MissingDiscoveryContext).check();
            self.calls += 1;
            return .{ .capabilities = .{ .task = .embed, .input_modalities = .{ .text = true }, .input_granularity = .chunk, .output = .embedding } };
        }
    };
    var p = P{};
    const provider = DenseEmbedder{ .ptr = &p, .dense_embed_fn = P.single, .capabilities_fn = P.legacy, .resolve_part_lease_fn = P.resolve, .part_request_context = .{ .io = std.testing.io, .deadline_ns = null } };
    _ = try provider.capabilities(std.testing.allocator, "model");
    try std.testing.expectEqual(@as(usize, 1), p.calls);
}

test "commit requirements dense grouped parts require and consume the captured lease" {
    const P = struct {
        revision: u8 = 1,
        planned: bool = false,
        fn single(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
            return error.UnexpectedSingle;
        }
        fn caps(ptr: *anyopaque, _: Allocator, _: []const u8) !inference_work.InferenceCapabilities {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .task = .embed, .input_modalities = .{ .text = true }, .accepted_mime_types = .{ .text_plain = true }, .input_granularity = .chunk, .output = .embedding, .batch = .{ .mode = .serial_compatibility, .max_items = 1 }, .model_revision = @as([32]u8, @splat(self.revision)) };
        }
        fn parts(ptr: *anyopaque, a: Allocator, _: []const u8, items: []const template_mod.ContentPart, _: u32) ![]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 2), items.len);
            try std.testing.expectEqualStrings("first", items[0].text);
            try std.testing.expectEqualStrings("second", items[1].text);
            self.revision += 1;
            return a.dupe(f32, &.{1});
        }
        fn controlledParts(ptr: *anyopaque, a: Allocator, name: []const u8, items: []const template_mod.ContentPart, dims: u32, context: RequestContext) ![]f32 {
            try context.check();
            return parts(ptr, a, name, items, dims);
        }
        fn plannedParts(ptr: *anyopaque, a: Allocator, name: []const u8, items: []const template_mod.ContentPart, dims: u32, context: ?RequestContext, lease: inference_work.CapabilityLease) ![]f32 {
            if (context) |value| try value.check();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.planned = true;
            try std.testing.expectEqual(@as([32]u8, @splat(1)), lease.capabilities.?.model_revision.?);
            return parts(ptr, a, name, items, dims);
        }
    };
    for ([_]bool{ false, true }) |planned| {
        for (0..3) |mode| {
            var state = P{};
            const provider = DenseEmbedder{ .ptr = &state, .dense_embed_fn = P.single, .dense_embed_parts_fn = P.parts, .dense_embed_parts_with_context_fn = if (mode == 2) null else P.controlledParts, .dense_embed_parts_planned_fn = if (planned) P.plannedParts else null, .capabilities_fn = P.caps };
            const items = [_]template_mod.ContentPart{ .{ .text = "first" }, .{ .text = "second" } };
            const result = switch (mode) {
                0 => provider.embedDenseParts(std.testing.allocator, "model", &items, 1),
                1 => provider.embedDensePartsWithContext(std.testing.allocator, "model", &items, 1, .{ .io = std.testing.io, .deadline_ns = null }),
                else => provider.embedDensePartsBackgroundWithContext(std.testing.allocator, "model", &items, 1, .{ .io = std.testing.io, .deadline_ns = null }),
            };
            if (planned) {
                const vector = try result;
                std.testing.allocator.free(vector);
                try std.testing.expectError(error.EnrichmentSourceChanged, provider.requireTextGeneration(std.testing.allocator, "model", .{ .revision = @splat(1) }));
            } else try std.testing.expectError(error.CommitRequirementUnavailable, result);
            try std.testing.expectEqual(planned, state.planned);
        }
    }
}

test "commit requirements sparse revision discovery carries control and rejects legacy resolvers" {
    const Probe = struct {
        seen: bool = false,
        fn embed(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !SparseEmbedding {
            return error.UnexpectedInference;
        }
        fn legacy(_: *anyopaque, _: Allocator, _: []const u8) !?[32]u8 {
            return error.UnexpectedLegacyDiscovery;
        }
        fn controlled(ptr: *anyopaque, _: Allocator, _: []const u8, context: RequestContext) !?[32]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.check();
            try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), context.deadline_ns);
            self.seen = true;
            return @as([32]u8, @splat(7));
        }
    };
    var probe = Probe{};
    const context: RequestContext = .{ .io = std.testing.io, .deadline_ns = std.math.maxInt(u64) };
    var provider = SparseEmbedder{ .ptr = &probe, .sparse_embed_fn = Probe.embed, .cache_revision_fn = Probe.legacy, .cache_revision_with_context_fn = Probe.controlled };
    try std.testing.expectEqual(@as(?[32]u8, @as([32]u8, @splat(7))), try provider.withRequestContext(context).captureRevision(std.testing.allocator, "model"));
    try std.testing.expect(probe.seen);
    provider.cache_revision_with_context_fn = null;
    try std.testing.expectError(error.UncancellableInferenceProvider, provider.withRequestContext(context).captureRevision(std.testing.allocator, "model"));
}

test "commit requirements sparse execution pins one generation across partitions and ABA" {
    const Probe = struct {
        revision: u8 = 1,
        calls: usize = 0,
        fn alias(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) !SparseEmbedding {
            return error.UnexpectedScalarInference;
        }
        fn resolve(ptr: *anyopaque, _: Allocator, _: []const u8, _: ?RequestContext) !SparseEmbedder.Generation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .revision = @splat(self.revision), .lease = .{ .capabilities = .{ .task = .embed, .input_modalities = .{ .text = true }, .accepted_mime_types = .{ .text_plain = true }, .input_granularity = .item, .output = .embedding, .batch = .{ .mode = .native, .preferred_items = 2, .max_items = 2 }, .model_revision = @splat(self.revision) } } };
        }
        fn execute(ptr: *anyopaque, a: Allocator, _: []const u8, texts: []const []const u8, context: ?RequestContext, generation: SparseEmbedder.Generation) ![]SparseEmbedding {
            if (context) |control| try control.check();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(texts.len <= 2);
            try std.testing.expectEqual(@as(u8, 1), generation.revision.?[0]);
            self.calls += 1;
            self.revision = 2;
            const out = try a.alloc(SparseEmbedding, texts.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |*v| v.deinit(a);
                a.free(out);
            }
            for (out) |*v| {
                const indices = try a.dupe(u32, &.{7});
                errdefer a.free(indices);
                v.* = .{ .indices = indices, .values = try a.dupe(f32, &.{@floatFromInt(generation.revision.?[0])}) };
                initialized += 1;
            }
            return out;
        }
    };
    const a = std.testing.allocator;
    var probe = Probe{};
    const provider = (SparseEmbedder{ .ptr = &probe, .sparse_embed_fn = Probe.alias, .resolve_generation_fn = Probe.resolve, .sparse_embed_batch_planned_fn = Probe.execute }).withRequestContext(.{ .io = std.testing.io, .deadline_ns = std.math.maxInt(u64) });
    const generation = try provider.captureGeneration(a, "sparse");
    const pinned = provider.withGeneration(generation);
    const vectors = try pinned.embedSparseBatch(a, "sparse", &.{ "a", "b", "c", "d", "e" });
    defer freeSparseEmbeddingBatch(a, vectors);
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    for (vectors) |v| try std.testing.expectEqual(@as(f32, 1), v.values[0]);
    try std.testing.expectError(error.EnrichmentSourceChanged, pinned.requireRevision(a, "sparse", generation.revision));
    probe.revision = 1;
    try pinned.requireRevision(a, "sparse", generation.revision);
    var unqualified = pinned;
    unqualified.sparse_embed_batch_planned_fn = null;
    try std.testing.expectError(error.CommitRequirementUnavailable, unqualified.embedSparse(a, "sparse", "reject"));
}

test "commit requirements dense execution consumes one revision across scalar batch and grouped ABA" {
    const Probe = struct {
        revision: u8 = 1,
        calls: usize = 0,
        fn legacy(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
            return error.UnexpectedAliasExecution;
        }
        fn resolve(ptr: *anyopaque, _: Allocator, _: []const u8, context: ?RequestContext) !inference_work.CapabilityLease {
            if (context) |control| try control.check();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .capabilities = .{ .task = .embed, .input_modalities = .{ .text = true }, .accepted_mime_types = .{ .text_plain = true }, .input_granularity = .chunk, .output = .embedding, .batch = .{ .mode = .native, .preferred_items = 2, .max_items = 2 }, .model_revision = @splat(self.revision) } };
        }
        fn batch(ptr: *anyopaque, a: Allocator, _: []const u8, texts: []const []const u8, _: u32, context: ?RequestContext, lease: inference_work.CapabilityLease) ![]const []const f32 {
            if (context) |control| try control.check();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(texts.len <= 2);
            try std.testing.expectEqual(@as(u8, 1), lease.capabilities.?.model_revision.?[0]);
            self.calls += 1;
            self.revision = 2;
            const out = try a.alloc([]const f32, texts.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |vector| a.free(vector);
                a.free(out);
            }
            for (out) |*vector| {
                vector.* = try a.dupe(f32, &.{@floatFromInt(lease.capabilities.?.model_revision.?[0])});
                initialized += 1;
            }
            // Returning the alias to A cannot relabel a B result: execution
            // above used A's captured lease rather than the live alias.
            self.revision = 1;
            return out;
        }
        fn parts(ptr: *anyopaque, a: Allocator, name: []const u8, _: []const template_mod.ContentPart, dims: u32, context: ?RequestContext, lease: inference_work.CapabilityLease) ![]f32 {
            return takeSingleDenseVector(a, try batch(ptr, a, name, &.{"group"}, dims, context, lease));
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |controlled| {
        var probe = Probe{};
        const provider = (DenseEmbedder{ .ptr = &probe, .dense_embed_fn = Probe.legacy, .resolve_part_lease_fn = Probe.resolve, .dense_embed_batch_planned_fn = Probe.batch, .dense_embed_parts_planned_fn = Probe.parts }).withRequestContext(if (controlled) .{ .io = std.testing.io, .deadline_ns = null } else null);
        const generation = try provider.captureTextGeneration(a, "dense");
        const pinned = provider.withTextGeneration(generation);
        const vectors = try pinned.embedDenseBatch(a, "dense", &.{ "a", "b", "c", "d", "e" }, 1);
        defer freeDenseEmbeddingBatch(a, vectors);
        for (vectors) |vector| try std.testing.expectEqual(@as(f32, 1), vector[0]);
        const scalar = try pinned.embedDense(a, "dense", "scalar", 1);
        defer a.free(scalar);
        try std.testing.expectEqual(@as(f32, 1), scalar[0]);
        const grouped = try pinned.embedDenseParts(a, "dense", &.{ .{ .text = "first" }, .{ .text = "second" } }, 1);
        defer a.free(grouped);
        try std.testing.expectEqual(@as(f32, 1), grouped[0]);
        try std.testing.expectEqual(@as(usize, 5), probe.calls);
        var unqualified = pinned;
        unqualified.dense_embed_batch_planned_fn = null;
        try std.testing.expectError(error.CommitRequirementUnavailable, unqualified.embedDense(a, "dense", "reject", 1));
        unqualified.dense_embed_parts_planned_fn = null;
        try std.testing.expectError(error.CommitRequirementUnavailable, unqualified.embedDenseParts(a, "dense", &.{ .{ .text = "first" }, .{ .text = "second" } }, 1));
        try std.testing.expectEqual(@as(usize, 5), probe.calls);
    }
}

test "commit requirements background legacy dense fallback retains cancellation boundaries" {
    const Probe = struct {
        calls: usize = 0,
        cancel: bool = false,
        canceled: std.atomic.Value(bool) = .init(false),
        fn embed(ptr: *anyopaque, a: Allocator, _: []const u8, _: []const u8, _: u32) ![]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.cancel) self.canceled.store(true, .release);
            return a.dupe(f32, &.{1});
        }
    };
    const a = std.testing.allocator;
    var probe = Probe{};
    const provider = DenseEmbedder{ .ptr = &probe, .dense_embed_fn = Probe.embed };
    const context: RequestContext = .{ .io = std.testing.io, .deadline_ns = null, .cancellation = CancellationToken.fromAtomic(&probe.canceled) };
    const rows = try provider.embedDenseBatchBackgroundWithContext(a, "legacy", &.{ "one", "two" }, 1, context);
    defer freeDenseEmbeddingBatch(a, rows);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    probe.calls = 0;
    probe.cancel = true;
    try std.testing.expectError(error.Cancelled, provider.embedDenseBatchBackgroundWithContext(a, "legacy", &.{ "one", "two" }, 1, context));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    probe.canceled.store(false, .release);
    try std.testing.expectError(error.UncancellableInferenceProvider, provider.embedDenseBatchWithContext(a, "legacy", &.{"one"}, 1, context));
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
}
