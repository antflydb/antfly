// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const antfly_image = @import("antfly_image");
const inference_work = @import("../../../inference/work.zig");
const CancellationToken = @import("../../../common/cancellation.zig").CancellationToken;
const request_context = @import("../../../inference/execution_context.zig");
const RequestContext = request_context.RequestContext;

const Allocator = std.mem.Allocator;

/// Controls owned by the logical invocation rather than by a shared producer.
/// A producer handle is copied before these controls are installed, so a
/// foreground deadline can safely overlap the background worker's lifecycle
/// cancellation without mutating provider-global state.
pub const InvocationContext = struct {
    io: ?std.Io = null,
    deadline_ns: ?u64 = null,
    cancellation: CancellationToken = .none,
    max_response_bytes: ?usize = null,
    progress: ?request_context.ProgressSink = null,

    pub fn check(self: InvocationContext) !void {
        try self.cancellation.check();
        if (self.deadline_ns) |deadline| {
            if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.Timeout;
        }
    }

    pub fn fromRequestContext(context: RequestContext) InvocationContext {
        return .{
            .io = context.io,
            .deadline_ns = context.deadline_ns,
            .cancellation = context.cancellation orelse .none,
            .progress = context.progress,
        };
    }
};

pub const ProducerType = enum {
    copy,
    document_extraction,
    generator,
    reader,
    transcriber,
    extractor,

    pub fn parse(text: []const u8) ?ProducerType {
        if (std.mem.eql(u8, text, "copy")) return .copy;
        if (std.mem.eql(u8, text, "document_extraction")) return .document_extraction;
        if (std.mem.eql(u8, text, "generator")) return .generator;
        if (std.mem.eql(u8, text, "reader")) return .reader;
        if (std.mem.eql(u8, text, "transcriber")) return .transcriber;
        if (std.mem.eql(u8, text, "extractor")) return .extractor;
        return null;
    }
};

pub const ProducerConfig = struct {
    type: ProducerType = .copy,
    config_json: []const u8 = "",

    pub fn deinit(self: *ProducerConfig, alloc: Allocator) void {
        if (self.config_json.len > 0) alloc.free(@constCast(self.config_json));
        self.* = undefined;
    }
};

pub fn parseProducerConfig(alloc: Allocator, raw: []const u8) !ProducerConfig {
    if (raw.len == 0) return .{};

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidAssetProducerConfig;

    const type_value = parsed.value.object.get("type") orelse return error.InvalidAssetProducerConfig;
    if (type_value != .string) return error.InvalidAssetProducerConfig;
    const producer_type = ProducerType.parse(type_value.string) orelse return error.InvalidAssetProducerConfig;

    const config_value = parsed.value.object.get("config") orelse .null;
    const config_json = if (config_value == .null)
        ""
    else
        try std.json.Stringify.valueAlloc(alloc, config_value, .{});

    return .{
        .type = producer_type,
        .config_json = config_json,
    };
}

pub const Request = struct {
    producer_type: ProducerType,
    config_json: []const u8,
    source_text: []const u8,
    source_parts_json: ?[]const u8 = null,
    content_type: []const u8 = "",
    /// Only producers that generated the inline media themselves may set this
    /// value. User-authored fields and URLs remain untrusted by default.
    inline_media_trusted: bool = false,
    /// Stable source fingerprint for opt-in producer/inference profiling.
    source_fingerprint: ?[]const u8 = null,
    /// Stable per-item identity used when compatible requests from different
    /// documents share one provider batch.
    item_id: []const u8 = "",
    page_number: ?u32 = null,
    /// Trusted, borrowed encoded media generated inside Antfly. Providers that
    /// cannot consume binary media directly adapt this to their wire format at
    /// the final transport boundary.
    media: []const EncodedMedia = &.{},
};

pub const EncodedMedia = struct {
    bytes: []const u8,
    mime_type: []const u8,
};

pub const ProducedItem = inference_work.WorkItemResult([]u8);

pub const ProducedBatch = struct {
    items: []ProducedItem,
    execution: inference_work.ExecutionReport,

    /// Validate the complete producer envelope against the logical requests
    /// that own its results. Execution telemetry is request-scoped at this
    /// boundary; task executors may keep finer-grained model telemetry inside
    /// their typed response, but it must not be confused with these items.
    pub fn validateForRequests(self: @This(), requests: []const Request) !void {
        try self.execution.validate();
        if (self.items.len != requests.len)
            return error.InvalidProducedBatchCardinality;
        if (self.execution.requested_items != requests.len)
            return error.InvalidProducedBatchExecutionCardinality;
        for (self.items, requests) |item, request| {
            const expected = inference_work.WorkIdentity{
                .item_id = request.item_id,
                .source_fingerprint = request.source_fingerprint,
                .page_number = request.page_number,
            };
            if (!item.identity.eql(expected))
                return error.InvalidAssetProducerResponseIdentity;
        }
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.items) |item| switch (item.result) {
            .value => |value| if (value.len > 0) alloc.free(value),
            .item_error => {},
        };
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }

    /// Compatibility transfer for callers that cannot represent per-item
    /// failures. No successful sibling is leaked when one item failed.
    pub fn intoOutputs(self: *@This(), alloc: Allocator) ![][]u8 {
        for (self.items) |item| switch (item.result) {
            .value => {},
            .item_error => |failure| return failure.cause,
        };
        const out = try alloc.alloc([]u8, self.items.len);
        for (self.items, 0..) |*item, i| switch (item.result) {
            .value => |value| {
                out[i] = value;
                item.result = .{ .value = &.{} };
            },
            .item_error => unreachable,
        };
        return out;
    }
};

pub fn producedBatchFromOutputs(
    alloc: Allocator,
    requests: []const Request,
    outputs: [][]u8,
    execution: inference_work.ExecutionReport,
) !ProducedBatch {
    var outputs_owned = true;
    defer if (outputs_owned) {
        for (outputs) |output| alloc.free(output);
        alloc.free(outputs);
    };
    if (outputs.len != requests.len) return error.InvalidProducedBatchCardinality;
    try execution.validate();
    if (execution.requested_items != requests.len)
        return error.InvalidProducedBatchExecutionCardinality;
    const items = try alloc.alloc(ProducedItem, outputs.len);
    errdefer alloc.free(items);
    for (outputs, requests, 0..) |output, request, i| items[i] = .{
        .identity = .{
            .item_id = request.item_id,
            .source_fingerprint = request.source_fingerprint,
            .page_number = request.page_number,
        },
        .result = .{ .value = output },
    };
    alloc.free(outputs);
    outputs_owned = false;
    return .{ .items = items, .execution = execution };
}

pub const Producer = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    invocation_context: InvocationContext = .{},

    pub const VTable = struct {
        produce: *const fn (ptr: *anyopaque, alloc: Allocator, request: Request) anyerror![]u8,
        produce_with_context: ?*const fn (ptr: *anyopaque, alloc: Allocator, request: Request, context: InvocationContext) anyerror![]u8 = null,
        produce_batch: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror![][]u8 = null,
        produce_batch_with_context: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request, context: InvocationContext) anyerror![][]u8 = null,
        produce_batch_reported: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!ProducedBatch = null,
        produce_batch_reported_with_context: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request, context: InvocationContext) anyerror!ProducedBatch = null,
        batch_mode: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!inference_work.BatchMode = null,
        batch_mode_with_context: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request, context: InvocationContext) anyerror!inference_work.BatchMode = null,
        can_produce_batch: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!bool = null,
        capabilities_for_requests: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!?inference_work.InferenceCapabilities = null,
        capabilities_for_requests_with_context: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request, context: InvocationContext) anyerror!?inference_work.InferenceCapabilities = null,
        /// Complete peak-memory and result contract for the concrete route
        /// selected by these requests. Implementations that publish this hook
        /// apply it to every invocation, independent of how media is encoded.
        invocation_memory_for_requests: ?*const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            requests: []const Request,
        ) anyerror!inference_work.InvocationMemoryPlan = null,
        invocation_memory_for_requests_with_context: ?*const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            requests: []const Request,
            context: InvocationContext,
        ) anyerror!inference_work.InvocationMemoryPlan = null,
        deinit: ?*const fn (ptr: *anyopaque, alloc: Allocator) void = null,
        /// See embedder.DenseEmbedder.foreground_bounded. This is deliberately
        /// opt-in so a custom callback cannot silently defeat the write
        /// visibility deadline.
        foreground_bounded: bool = false,
        /// Optional request-aware refinement of foreground_bounded. Producers
        /// that can dispatch to both bounded transports and unbounded local
        /// callbacks use this hook to preserve the bounded fast path without
        /// overstating the contract for a particular request.
        foreground_bounded_for_requests: ?*const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            requests: []const Request,
        ) anyerror!bool = null,
        /// Task-neutral physical raster transport. The logical task remains
        /// encoded by each Request and only resolved executors which advertise
        /// borrowed-raster support may install this callback. Every raster and
        /// identity slice is borrowed strictly for the synchronous call.
        produce_borrowed_raster_batch_reported: ?*const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            requests: []const Request,
            rasters: []const antfly_image.BorrowedRasterAttachment,
        ) anyerror!ProducedBatch = null,
        produce_borrowed_raster_batch_reported_with_context: ?*const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            requests: []const Request,
            rasters: []const antfly_image.BorrowedRasterAttachment,
            context: InvocationContext,
        ) anyerror!ProducedBatch = null,
        borrowed_raster_batch_available: ?*const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            requests: []const Request,
        ) anyerror!bool = null,
        borrowed_raster_batch_available_with_context: ?*const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            requests: []const Request,
            context: InvocationContext,
        ) anyerror!bool = null,
    };

    pub fn withInvocationContext(self: Producer, context: InvocationContext) Producer {
        var scoped = self;
        scoped.invocation_context = context;
        return scoped;
    }

    pub fn foregroundBoundedForRequests(self: Producer, alloc: Allocator, requests: []const Request) !bool {
        // A finite implementation-specific timeout is not sufficient for a
        // foreground request: the provider must accept the caller's actual
        // deadline and cancellation token. Legacy callbacks remain available
        // to durable background replay, but can never advertise foreground
        // cancellability.
        if (self.vtable.produce_with_context == null) return false;
        if (self.vtable.foreground_bounded_for_requests) |foreground_bounded|
            return try foreground_bounded(self.ptr, alloc, requests);
        return self.vtable.foreground_bounded;
    }

    pub fn produce(self: Producer, alloc: Allocator, request: Request) ![]u8 {
        const requests = [_]Request{request};
        const plan = try self.resolvedInvocationPlan(alloc, &requests);
        if (plan) |resolved| {
            var bounded = inference_work.BoundedInvocationAllocator.init(
                alloc,
                try invocationAllocatorLimit(resolved, &requests),
            );
            const bounded_alloc = bounded.allocator();
            const output = self.invokeProduce(bounded_alloc, request) catch |err| {
                if (bounded.limit_exceeded) return error.InferenceInvocationMemoryExceeded;
                return err;
            };
            if (output.len > resolved.max_result_bytes_per_item or output.len > resolved.max_result_bytes) {
                alloc.free(output);
                return error.InferenceResultTooLarge;
            }
            return output;
        }
        return try self.invokeProduce(alloc, request);
    }

    fn invokeProduce(self: Producer, alloc: Allocator, request: Request) ![]u8 {
        if (self.vtable.produce_with_context) |produce_fn|
            return try produce_fn(self.ptr, alloc, request, self.invocation_context);
        return try self.vtable.produce(self.ptr, alloc, request);
    }

    pub fn produceWithContext(self: Producer, alloc: Allocator, request: Request, context: RequestContext) ![]u8 {
        try context.check();
        const output = try self.withInvocationContext(.fromRequestContext(context)).produce(alloc, request);
        context.check() catch |err| {
            alloc.free(output);
            return err;
        };
        return output;
    }

    pub fn produceBatch(self: Producer, alloc: Allocator, requests: []const Request) ![][]u8 {
        const plan = try self.resolvedInvocationPlan(alloc, requests);
        if (plan) |resolved| {
            var bounded = inference_work.BoundedInvocationAllocator.init(
                alloc,
                try invocationAllocatorLimit(resolved, requests),
            );
            const outputs = self.produceBatchUnchecked(bounded.allocator(), requests) catch |err| {
                if (bounded.limit_exceeded) return error.InferenceInvocationMemoryExceeded;
                return err;
            };
            validateOutputBytes(outputs, resolved.max_result_bytes_per_item, resolved.max_result_bytes) catch |err| {
                for (outputs) |output| if (output.len > 0) alloc.free(output);
                alloc.free(outputs);
                return err;
            };
            return outputs;
        }
        return try self.produceBatchUnchecked(alloc, requests);
    }

    fn produceBatchUnchecked(self: Producer, alloc: Allocator, requests: []const Request) ![][]u8 {
        if (self.vtable.produce_batch_with_context) |produce_batch|
            return try produce_batch(self.ptr, alloc, requests, self.invocation_context);
        if (self.vtable.produce_batch_reported_with_context != null or self.vtable.produce_batch_reported != null) {
            var batch = if (self.vtable.produce_batch_reported_with_context) |reported|
                try reported(self.ptr, alloc, requests, self.invocation_context)
            else
                try self.vtable.produce_batch_reported.?(self.ptr, alloc, requests);
            defer batch.deinit(alloc);
            return try batch.intoOutputs(alloc);
        }
        if (self.vtable.produce_batch) |produce_batch| return try produce_batch(self.ptr, alloc, requests);

        const out = try alloc.alloc([]u8, requests.len);
        errdefer {
            for (out) |item| {
                if (item.len > 0) alloc.free(item);
            }
            alloc.free(out);
        }
        for (out) |*item| item.* = "";
        for (requests, 0..) |request, i| {
            out[i] = try self.invokeProduce(alloc, request);
        }
        return out;
    }

    /// Returns the execution path that actually completed. Legacy callbacks
    /// are conservatively classified as compatibility execution; capability
    /// prediction is never presented as observed telemetry.
    pub fn produceBatchReported(self: Producer, alloc: Allocator, requests: []const Request) !ProducedBatch {
        const plan = try self.resolvedInvocationPlan(alloc, requests);
        if (plan) |resolved| {
            var bounded = inference_work.BoundedInvocationAllocator.init(
                alloc,
                try invocationAllocatorLimit(resolved, requests),
            );
            var batch = self.produceBatchReportedUnchecked(bounded.allocator(), requests) catch |err| {
                if (bounded.limit_exceeded) return error.InferenceInvocationMemoryExceeded;
                return err;
            };
            validateProducedResultBytes(
                batch.items,
                resolved.max_result_bytes_per_item,
                resolved.max_result_bytes,
            ) catch |err| {
                batch.deinit(alloc);
                return err;
            };
            return batch;
        }
        return try self.produceBatchReportedUnchecked(alloc, requests);
    }

    /// Execute a batch directly from caller-owned decoded rasters. This is a
    /// distinct transport entry point rather than a raw pseudo-MIME in
    /// Request.media: encoded and decoded byte limits have different meanings,
    /// and remote executors must continue through the encoded compatibility
    /// path. The callback must complete before this function returns.
    pub fn produceBorrowedRasterBatchReported(
        self: Producer,
        alloc: Allocator,
        requests: []const Request,
        rasters: []const antfly_image.BorrowedRasterAttachment,
    ) !ProducedBatch {
        if (requests.len == 0 or requests.len != rasters.len)
            return error.InvalidBorrowedRasterCardinality;
        for (rasters, requests) |raster, request| {
            try raster.validate();
            const expected = inference_work.WorkIdentity{
                .item_id = request.item_id,
                .source_fingerprint = request.source_fingerprint,
                .page_number = request.page_number,
            };
            const actual = inference_work.WorkIdentity{
                .item_id = raster.item_id,
                .source_fingerprint = raster.source_fingerprint,
                .page_number = raster.page_number,
            };
            if (!actual.eql(expected)) return error.InvalidBorrowedRasterIdentity;
        }
        const capabilities = (try self.capabilitiesForRequests(alloc, requests)) orelse
            return error.BorrowedRasterUnsupported;
        if (!capabilities.borrowed_rasters) return error.BorrowedRasterUnsupported;

        const plan = try self.resolvedInvocationPlan(alloc, requests);
        if (plan) |resolved| {
            var bounded = inference_work.BoundedInvocationAllocator.init(
                alloc,
                try invocationAllocatorLimit(resolved, requests),
            );
            var batch = self.produceBorrowedRasterBatchReportedUnchecked(
                bounded.allocator(),
                requests,
                rasters,
            ) catch |err| {
                if (bounded.limit_exceeded) return error.InferenceInvocationMemoryExceeded;
                return err;
            };
            validateProducedResultBytes(
                batch.items,
                resolved.max_result_bytes_per_item,
                resolved.max_result_bytes,
            ) catch |err| {
                batch.deinit(alloc);
                return err;
            };
            return batch;
        }
        return try self.produceBorrowedRasterBatchReportedUnchecked(alloc, requests, rasters);
    }

    /// Resolve both the logical model capability and the physical transport.
    /// Callers use this before choosing a render representation so a remote or
    /// older executor is rendered directly as encoded media, never rendered
    /// raw and then rerendered after a transport failure.
    pub fn borrowedRasterBatchAvailable(
        self: Producer,
        alloc: Allocator,
        requests: []const Request,
    ) !bool {
        if (requests.len == 0) return false;
        if (self.vtable.borrowed_raster_batch_available_with_context) |available|
            return try available(self.ptr, alloc, requests, self.invocation_context);
        if (self.vtable.borrowed_raster_batch_available) |available|
            return try available(self.ptr, alloc, requests);
        return false;
    }

    fn produceBorrowedRasterBatchReportedUnchecked(
        self: Producer,
        alloc: Allocator,
        requests: []const Request,
        rasters: []const antfly_image.BorrowedRasterAttachment,
    ) !ProducedBatch {
        var batch = if (self.vtable.produce_borrowed_raster_batch_reported_with_context) |reported|
            try reported(self.ptr, alloc, requests, rasters, self.invocation_context)
        else if (self.vtable.produce_borrowed_raster_batch_reported) |reported|
            try reported(self.ptr, alloc, requests, rasters)
        else
            return error.BorrowedRasterUnsupported;
        errdefer batch.deinit(alloc);
        try batch.validateForRequests(requests);
        return batch;
    }

    fn produceBatchReportedUnchecked(self: Producer, alloc: Allocator, requests: []const Request) !ProducedBatch {
        if (self.vtable.produce_batch_reported_with_context) |reported| {
            var batch = try reported(self.ptr, alloc, requests, self.invocation_context);
            errdefer batch.deinit(alloc);
            try batch.validateForRequests(requests);
            return batch;
        }
        if (self.vtable.produce_batch_reported) |reported| {
            var batch = try reported(self.ptr, alloc, requests);
            errdefer batch.deinit(alloc);
            try batch.validateForRequests(requests);
            return batch;
        }
        const items = try self.produceBatchUnchecked(alloc, requests);
        return try producedBatchFromOutputs(alloc, requests, items, inference_work.ExecutionReport.compatibility(requests.len));
    }

    fn resolvedInvocationPlan(
        self: Producer,
        alloc: Allocator,
        requests: []const Request,
    ) !?inference_work.InvocationMemoryPlan {
        if (requests.len == 0) return null;
        if (self.vtable.invocation_memory_for_requests == null and
            self.vtable.invocation_memory_for_requests_with_context == null)
        {
            if (requestsRequireInvocationContract(requests))
                return error.InferenceInvocationMemoryUnavailable;
            return null;
        }
        return try self.resolveInvocationMemoryForRequests(alloc, requests);
    }

    /// Describes how the request set will execute. This preserves the important
    /// distinction between a fused/native model batch and an API-compatible
    /// serial loop.
    pub fn batchMode(self: Producer, alloc: Allocator, requests: []const Request) !inference_work.BatchMode {
        if (self.vtable.produce_batch == null and self.vtable.produce_batch_with_context == null and
            self.vtable.produce_batch_reported == null and self.vtable.produce_batch_reported_with_context == null) return .none;
        if (self.vtable.batch_mode_with_context) |batch_mode|
            return try batch_mode(self.ptr, alloc, requests, self.invocation_context);
        if (self.vtable.batch_mode) |batch_mode|
            return try batch_mode(self.ptr, alloc, requests);
        if (self.vtable.can_produce_batch) |can_produce_batch|
            return if (try can_produce_batch(self.ptr, alloc, requests)) .native else .none;
        return .serial_compatibility;
    }

    pub fn produceBatchWithContext(self: Producer, alloc: Allocator, requests: []const Request, context: RequestContext) ![][]u8 {
        try context.check();
        const out = try self.withInvocationContext(.fromRequestContext(context)).produceBatch(alloc, requests);
        context.check() catch |err| {
            for (out) |item| if (item.len > 0) alloc.free(item);
            alloc.free(out);
            return err;
        };
        return out;
    }

    /// Compatibility wrapper for callers that only need to choose between the
    /// batch entry point and singleton execution.
    pub fn canProduceBatch(self: Producer, alloc: Allocator, requests: []const Request) !bool {
        return try self.batchMode(alloc, requests) != .none;
    }

    pub fn capabilitiesForRequests(self: Producer, alloc: Allocator, requests: []const Request) !?inference_work.InferenceCapabilities {
        if (self.vtable.capabilities_for_requests_with_context) |resolve| {
            const result = try resolve(self.ptr, alloc, requests, self.invocation_context);
            if (result) |capabilities| try capabilities.validate();
            return result;
        }
        const resolve = self.vtable.capabilities_for_requests orelse return null;
        const result = try resolve(self.ptr, alloc, requests);
        if (result) |capabilities| try capabilities.validate();
        return result;
    }

    pub fn invocationMemoryForRequests(
        self: Producer,
        alloc: Allocator,
        requests: []const Request,
    ) !inference_work.InvocationMemoryPlan {
        if (self.vtable.invocation_memory_for_requests == null and
            self.vtable.invocation_memory_for_requests_with_context == null)
        {
            if (requestsRequireInvocationContract(requests))
                return error.InferenceInvocationMemoryUnavailable;
            return .{ .attachment_transport = .borrowed_binary, .fixed_bytes = 0 };
        }
        return try self.resolveInvocationMemoryForRequests(alloc, requests);
    }

    fn resolveInvocationMemoryForRequests(
        self: Producer,
        alloc: Allocator,
        requests: []const Request,
    ) !inference_work.InvocationMemoryPlan {
        const resolve = self.vtable.invocation_memory_for_requests;
        const resolve_with_context = self.vtable.invocation_memory_for_requests_with_context;
        if (resolve == null and resolve_with_context == null)
            return error.InferenceInvocationMemoryUnavailable;
        var bounded = inference_work.BoundedInvocationAllocator.init(
            alloc,
            try invocationResolutionLimit(requests),
        );
        const plan = (if (resolve_with_context) |contextual|
            contextual(self.ptr, bounded.allocator(), requests, self.invocation_context)
        else
            resolve.?(self.ptr, bounded.allocator(), requests)) catch |err| {
            if (bounded.limit_exceeded) return error.InferenceInvocationMemoryExceeded;
            return err;
        };
        if (requests.len > 0) try plan.validate();
        return plan;
    }

    pub fn deinit(self: Producer, alloc: Allocator) void {
        if (self.vtable.deinit) |deinit_fn| deinit_fn(self.ptr, alloc);
    }
};

fn requestsRequireInvocationContract(requests: []const Request) bool {
    for (requests) |request| {
        if (request.media.len > 0 or request.source_parts_json != null or
            switch (request.producer_type) {
                .reader, .generator, .extractor, .transcriber => true,
                .copy, .document_extraction => false,
            }) return true;
    }
    return false;
}

fn invocationResolutionLimit(requests: []const Request) !usize {
    var source_bytes: usize = 0;
    for (requests) |request| {
        source_bytes = std.math.add(usize, source_bytes, request.config_json.len) catch
            return error.InferenceEncodedBytesExceeded;
        source_bytes = std.math.add(usize, source_bytes, request.source_text.len) catch
            return error.InferenceEncodedBytesExceeded;
        if (request.source_parts_json) |parts| source_bytes = std.math.add(
            usize,
            source_bytes,
            parts.len,
        ) catch return error.InferenceEncodedBytesExceeded;
    }
    const parsed = std.math.mul(usize, source_bytes, 8) catch
        return error.InferenceEncodedBytesExceeded;
    const control = std.math.mul(usize, @max(requests.len, 1), 4096) catch
        return error.InferenceEncodedBytesExceeded;
    return std.math.add(usize, parsed, control) catch
        error.InferenceEncodedBytesExceeded;
}

fn invocationAllocatorLimit(
    plan: inference_work.InvocationMemoryPlan,
    requests: []const Request,
) !usize {
    var transport_copy_bytes: usize = 0;
    for (requests) |request| for (request.media) |media| {
        const resident = try plan.attachment_transport.peakResidentSize(media.bytes.len, media.mime_type.len);
        transport_copy_bytes = std.math.add(
            usize,
            transport_copy_bytes,
            resident - media.bytes.len,
        ) catch return error.InferenceEncodedBytesExceeded;
    };
    return std.math.add(usize, plan.allocator_limit_bytes, transport_copy_bytes) catch
        error.InferenceEncodedBytesExceeded;
}

fn validateOutputBytes(outputs: []const []u8, per_item_limit: usize, aggregate_limit: usize) !void {
    var total: usize = 0;
    for (outputs) |output| {
        if (output.len > per_item_limit) return error.InferenceResultTooLarge;
        total = std.math.add(usize, total, output.len) catch return error.InferenceResultTooLarge;
        if (total > aggregate_limit) return error.InferenceResultTooLarge;
    }
}

fn validateProducedResultBytes(items: []const ProducedItem, per_item_limit: usize, aggregate_limit: usize) !void {
    var total: usize = 0;
    for (items) |item| switch (item.result) {
        .value => |output| {
            if (output.len > per_item_limit) return error.InferenceResultTooLarge;
            total = std.math.add(usize, total, output.len) catch return error.InferenceResultTooLarge;
            if (total > aggregate_limit) return error.InferenceResultTooLarge;
        },
        .item_error => {},
    };
}

test "asset producer parses default copy" {
    var cfg = try parseProducerConfig(std.testing.allocator, "");
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqual(ProducerType.copy, cfg.type);
    try std.testing.expectEqual(@as(usize, 0), cfg.config_json.len);
}

test "asset producer parses typed config" {
    const alloc = std.testing.allocator;
    var cfg = try parseProducerConfig(alloc,
        \\{"type":"reader","config":{"provider":"vertex","model":"gemini-2.5-flash"}}
    );
    defer cfg.deinit(alloc);
    try std.testing.expectEqual(ProducerType.reader, cfg.type);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"provider\":\"vertex\"") != null);
}

test "asset producer parses document extraction config" {
    const alloc = std.testing.allocator;
    var cfg = try parseProducerConfig(alloc,
        \\{"type":"document_extraction","config":{"routes":[{"match":{"content_type":"text/plain"},"extractor":{"type":"text","unit":"document"}}]}}
    );
    defer cfg.deinit(alloc);
    try std.testing.expectEqual(ProducerType.document_extraction, cfg.type);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"routes\"") != null);
}

test "asset producer media invocation memory fails closed without route contract" {
    const Stub = struct {
        fn produce(_: *anyopaque, alloc: Allocator, _: Request) ![]u8 {
            return try alloc.alloc(u8, 0);
        }
    };
    var context: u8 = 0;
    const producer = Producer{
        .ptr = &context,
        .vtable = &.{ .produce = Stub.produce },
    };
    const media = [_]EncodedMedia{.{ .bytes = &.{1}, .mime_type = "image/png" }};
    const request = Request{
        .producer_type = .reader,
        .config_json = "{}",
        .source_text = "",
        .inline_media_trusted = true,
        .media = &media,
    };
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        producer.invocationMemoryForRequests(std.testing.allocator, &.{request}),
    );
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        producer.produce(std.testing.allocator, request),
    );
}

test "asset producer enforces media allocator and result contracts at execution" {
    const Stub = struct {
        fn produce(_: *anyopaque, alloc: Allocator, request: Request) ![]u8 {
            return try alloc.alloc(u8, request.source_text.len);
        }

        fn memory(
            _: *anyopaque,
            _: Allocator,
            _: []const Request,
        ) !inference_work.InvocationMemoryPlan {
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 16,
                .allocator_limit_bytes = 16,
                .max_result_bytes_per_item = 4,
                .max_result_bytes = 4,
            };
        }
    };
    var context: u8 = 0;
    const producer = Producer{
        .ptr = &context,
        .vtable = &.{
            .produce = Stub.produce,
            .invocation_memory_for_requests = Stub.memory,
        },
    };
    const media = [_]EncodedMedia{.{ .bytes = &.{1}, .mime_type = "image/png" }};
    var request = Request{
        .producer_type = .reader,
        .config_json = "{}",
        .source_text = "12345",
        .inline_media_trusted = true,
        .media = &media,
    };
    try std.testing.expectError(
        error.InferenceResultTooLarge,
        producer.produce(std.testing.allocator, request),
    );
    request.source_text = "12345678901234567";
    try std.testing.expectError(
        error.InferenceInvocationMemoryExceeded,
        producer.produce(std.testing.allocator, request),
    );
}

test "asset producer enforces invocation contracts for non-media and source parts" {
    const Stub = struct {
        fn produce(_: *anyopaque, alloc: Allocator, request: Request) ![]u8 {
            return try alloc.dupe(u8, request.source_text);
        }

        fn memory(_: *anyopaque, _: Allocator, _: []const Request) !inference_work.InvocationMemoryPlan {
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 32,
                .allocator_limit_bytes = 32,
                .max_result_bytes_per_item = 4,
                .max_result_bytes = 4,
            };
        }
    };
    var context: u8 = 0;
    const bounded = Producer{
        .ptr = &context,
        .vtable = &.{
            .produce = Stub.produce,
            .invocation_memory_for_requests = Stub.memory,
        },
    };
    try std.testing.expectError(
        error.InferenceResultTooLarge,
        bounded.produce(std.testing.allocator, .{
            .producer_type = .copy,
            .config_json = "{}",
            .source_text = "12345",
        }),
    );

    const legacy = Producer{
        .ptr = &context,
        .vtable = &.{ .produce = Stub.produce },
    };
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        legacy.produce(std.testing.allocator, .{
            .producer_type = .generator,
            .config_json = "{}",
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"hello\"}]",
        }),
    );
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        legacy.produce(std.testing.allocator, .{
            .producer_type = .transcriber,
            .config_json = "{}",
            .source_text = "file:///tmp/audio.wav",
        }),
    );
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        legacy.produce(std.testing.allocator, .{
            .producer_type = .reader,
            .config_json = "{}",
            .source_text = "https://example.invalid/page.png",
        }),
    );
}

test "asset producer enforces invocation contracts with per-item and aggregate result ceilings" {
    const Stub = struct {
        fn produce(_: *anyopaque, alloc: Allocator, _: Request) ![]u8 {
            return try alloc.alloc(u8, 5);
        }

        fn memory(_: *anyopaque, _: Allocator, requests: []const Request) !inference_work.InvocationMemoryPlan {
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 128,
                .allocator_limit_bytes = 128,
                .max_result_bytes_per_item = 4,
                .max_result_bytes = 4 * requests.len,
            };
        }
    };
    var context: u8 = 0;
    const producer = Producer{
        .ptr = &context,
        .vtable = &.{
            .produce = Stub.produce,
            .invocation_memory_for_requests = Stub.memory,
        },
    };
    const requests = [_]Request{
        .{ .producer_type = .reader, .config_json = "{}", .source_text = "a" },
        .{ .producer_type = .reader, .config_json = "{}", .source_text = "b" },
    };
    try std.testing.expectError(
        error.InferenceResultTooLarge,
        producer.produceBatch(std.testing.allocator, &requests),
    );
}

test "asset producer still bounds boundary allocations with executor-owned model admission" {
    const Stub = struct {
        fn produce(_: *anyopaque, alloc: Allocator, _: Request) ![]u8 {
            const scratch = try alloc.alloc(u8, 4096);
            defer alloc.free(scratch);
            return try alloc.alloc(u8, 1);
        }

        fn memory(_: *anyopaque, _: Allocator, _: []const Request) !inference_work.InvocationMemoryPlan {
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 64,
                .allocator_limit_bytes = 64,
                .allocator_owner = .executor,
                .max_result_bytes_per_item = 1,
                .max_result_bytes = 1,
            };
        }
    };
    var context: u8 = 0;
    const producer = Producer{
        .ptr = &context,
        .vtable = &.{
            .produce = Stub.produce,
            .invocation_memory_for_requests = Stub.memory,
        },
    };
    try std.testing.expectError(
        error.InferenceInvocationMemoryExceeded,
        producer.produce(std.testing.allocator, .{
            .producer_type = .reader,
            .config_json = "{}",
            .source_text = "https://example.invalid/page.png",
        }),
    );
}

test "asset producer bounds invocation contract resolution allocations" {
    const Stub = struct {
        fn produce(_: *anyopaque, alloc: Allocator, _: Request) ![]u8 {
            return try alloc.alloc(u8, 0);
        }

        fn memory(_: *anyopaque, alloc: Allocator, _: []const Request) !inference_work.InvocationMemoryPlan {
            _ = try alloc.alloc(u8, 8192);
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 1,
                .allocator_limit_bytes = 1,
                .max_result_bytes_per_item = 1,
                .max_result_bytes = 1,
            };
        }
    };
    var context: u8 = 0;
    const producer = Producer{
        .ptr = &context,
        .vtable = &.{
            .produce = Stub.produce,
            .invocation_memory_for_requests = Stub.memory,
        },
    };
    try std.testing.expectError(
        error.InferenceInvocationMemoryExceeded,
        producer.produce(std.testing.allocator, .{
            .producer_type = .copy,
            .config_json = "{}",
            .source_text = "small",
        }),
    );
}

test "asset producer parses extractor config" {
    const alloc = std.testing.allocator;
    var cfg = try parseProducerConfig(alloc,
        \\{"type":"extractor","config":{"provider":"antfly","model":"gliner","schema":{"entities":["person"]}}}
    );
    defer cfg.deinit(alloc);
    try std.testing.expectEqual(ProducerType.extractor, cfg.type);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"entities\"") != null);
}

test "asset producer destroys returned values when reported metadata is invalid" {
    const Fake = struct {
        fn produce(_: *anyopaque, alloc: Allocator, _: Request) anyerror![]u8 {
            return try alloc.dupe(u8, "unused");
        }

        fn produceBatchReported(_: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!ProducedBatch {
            const items = try alloc.alloc(ProducedItem, requests.len);
            errdefer alloc.free(items);
            for (items, requests) |*item, request| item.* = .{
                .identity = .{
                    .item_id = request.item_id,
                    .source_fingerprint = request.source_fingerprint,
                    .page_number = request.page_number,
                },
                .result = .{ .value = try alloc.dupe(u8, "owned-result") },
            };
            return .{
                .items = items,
                // Deliberately omits the completed serial items.
                .execution = .{ .requested_items = requests.len },
            };
        }

        fn memory(_: *anyopaque, _: Allocator, requests: []const Request) !inference_work.InvocationMemoryPlan {
            const limit = 1024 * requests.len;
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = limit,
                .allocator_limit_bytes = limit,
                .max_result_bytes_per_item = 256,
                .max_result_bytes = limit,
            };
        }
    };

    var context: u8 = 0;
    const producer: Producer = .{
        .ptr = &context,
        .vtable = &.{
            .produce = Fake.produce,
            .produce_batch_reported = Fake.produceBatchReported,
            .invocation_memory_for_requests = Fake.memory,
        },
    };
    const requests = [_]Request{.{
        .producer_type = .reader,
        .config_json = "{}",
        .source_text = "source",
        .item_id = "item-0",
    }};
    try std.testing.expectError(
        error.InvalidExecutionReport,
        producer.produceBatchReported(std.testing.allocator, &requests),
    );
}

test "asset producer destroys returned values when reported cardinality is invalid" {
    const Fake = struct {
        fn produce(_: *anyopaque, alloc: Allocator, _: Request) anyerror![]u8 {
            return try alloc.dupe(u8, "unused");
        }

        fn produceBatchReported(_: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!ProducedBatch {
            const items = try alloc.alloc(ProducedItem, 1);
            errdefer alloc.free(items);
            items[0] = .{
                .identity = .{ .item_id = requests[0].item_id },
                .result = .{ .value = try alloc.dupe(u8, "owned-result") },
            };
            return .{
                .items = items,
                .execution = inference_work.ExecutionReport.serial(requests.len),
            };
        }

        fn memory(_: *anyopaque, _: Allocator, requests: []const Request) !inference_work.InvocationMemoryPlan {
            const limit = 1024 * requests.len;
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = limit,
                .allocator_limit_bytes = limit,
                .max_result_bytes_per_item = 256,
                .max_result_bytes = limit,
            };
        }
    };

    var context: u8 = 0;
    const producer: Producer = .{
        .ptr = &context,
        .vtable = &.{
            .produce = Fake.produce,
            .produce_batch_reported = Fake.produceBatchReported,
            .invocation_memory_for_requests = Fake.memory,
        },
    };
    const requests = [_]Request{
        .{ .producer_type = .reader, .config_json = "{}", .source_text = "source-0", .item_id = "item-0" },
        .{ .producer_type = .reader, .config_json = "{}", .source_text = "source-1", .item_id = "item-1" },
    };
    try std.testing.expectError(
        error.InvalidProducedBatchCardinality,
        producer.produceBatchReported(std.testing.allocator, &requests),
    );
}

test "asset producer destroys returned values when execution telemetry describes different logical items" {
    const Fake = struct {
        fn produce(_: *anyopaque, alloc: Allocator, _: Request) anyerror![]u8 {
            return try alloc.dupe(u8, "unused");
        }

        fn produceBatchReported(_: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!ProducedBatch {
            const items = try alloc.alloc(ProducedItem, requests.len);
            errdefer alloc.free(items);
            items[0] = .{
                .identity = .{ .item_id = requests[0].item_id },
                .result = .{ .value = try alloc.dupe(u8, "owned-result") },
            };
            // This is internally coherent executor telemetry, but it describes
            // two nested inputs rather than the one ProducedItem/request.
            return .{ .items = items, .execution = inference_work.ExecutionReport.serial(2) };
        }

        fn memory(_: *anyopaque, _: Allocator, requests: []const Request) !inference_work.InvocationMemoryPlan {
            const limit = 1024 * requests.len;
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = limit,
                .allocator_limit_bytes = limit,
                .max_result_bytes_per_item = 256,
                .max_result_bytes = limit,
            };
        }
    };

    var context: u8 = 0;
    const producer: Producer = .{
        .ptr = &context,
        .vtable = &.{
            .produce = Fake.produce,
            .produce_batch_reported = Fake.produceBatchReported,
            .invocation_memory_for_requests = Fake.memory,
        },
    };
    const requests = [_]Request{.{
        .producer_type = .reader,
        .config_json = "{}",
        .source_text = "source",
        .item_id = "item-0",
    }};
    try std.testing.expectError(
        error.InvalidProducedBatchExecutionCardinality,
        producer.produceBatchReported(std.testing.allocator, &requests),
    );
}

test "asset producer destroys returned values when result identity mismatches its request" {
    const Fake = struct {
        fn produce(_: *anyopaque, alloc: Allocator, _: Request) anyerror![]u8 {
            return try alloc.dupe(u8, "unused");
        }

        fn produceBatchReported(_: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!ProducedBatch {
            const items = try alloc.alloc(ProducedItem, requests.len);
            errdefer alloc.free(items);
            items[0] = .{
                .identity = .{ .item_id = "different-item" },
                .result = .{ .value = try alloc.dupe(u8, "owned-result") },
            };
            return .{ .items = items, .execution = inference_work.ExecutionReport.serial(requests.len) };
        }

        fn memory(_: *anyopaque, _: Allocator, requests: []const Request) !inference_work.InvocationMemoryPlan {
            const limit = 1024 * requests.len;
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = limit,
                .allocator_limit_bytes = limit,
                .max_result_bytes_per_item = 256,
                .max_result_bytes = limit,
            };
        }
    };

    var context: u8 = 0;
    const producer: Producer = .{
        .ptr = &context,
        .vtable = &.{
            .produce = Fake.produce,
            .produce_batch_reported = Fake.produceBatchReported,
            .invocation_memory_for_requests = Fake.memory,
        },
    };
    const requests = [_]Request{.{
        .producer_type = .generator,
        .config_json = "{}",
        .source_text = "source",
        .source_fingerprint = "document",
        .item_id = "item-0",
        .page_number = 1,
    }};
    try std.testing.expectError(
        error.InvalidAssetProducerResponseIdentity,
        producer.produceBatchReported(std.testing.allocator, &requests),
    );
}

test "asset producer enforces invocation contracts with immutable planning and execution context" {
    const State = struct {
        planning_calls: usize = 0,
        execution_calls: usize = 0,

        fn legacyProduce(_: *anyopaque, _: Allocator, _: Request) anyerror![]u8 {
            return error.LegacyCallbackInvoked;
        }

        fn produceWithContext(
            ptr: *anyopaque,
            alloc: Allocator,
            _: Request,
            context: InvocationContext,
        ) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.execution_calls += 1;
            try std.testing.expectEqual(@as(?u64, 1234), context.deadline_ns);
            try std.testing.expectEqual(@as(?usize, 2048), context.max_response_bytes);
            try context.cancellation.check();
            try std.testing.expect(context.io != null);
            return try alloc.dupe(u8, "contextual");
        }

        fn memoryWithContext(
            ptr: *anyopaque,
            _: Allocator,
            _: []const Request,
            context: InvocationContext,
        ) anyerror!inference_work.InvocationMemoryPlan {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.planning_calls += 1;
            try std.testing.expectEqual(@as(?u64, 1234), context.deadline_ns);
            try std.testing.expectEqual(@as(?usize, 2048), context.max_response_bytes);
            return .{
                .attachment_transport = .borrowed_binary,
                .fixed_bytes = 4096,
                .allocator_limit_bytes = 4096,
                .max_result_bytes_per_item = 1024,
                .max_result_bytes = 1024,
            };
        }
    };

    var state = State{};
    const producer = Producer{
        .ptr = &state,
        .vtable = &.{
            .produce = State.legacyProduce,
            .produce_with_context = State.produceWithContext,
            .invocation_memory_for_requests_with_context = State.memoryWithContext,
        },
    };
    var canceled = std.atomic.Value(bool).init(false);
    const scoped = producer.withInvocationContext(.{
        .io = std.Io.Threaded.global_single_threaded.io(),
        .deadline_ns = 1234,
        .cancellation = CancellationToken.fromAtomic(&canceled),
        .max_response_bytes = 2048,
    });
    const result = try scoped.produce(std.testing.allocator, .{
        .producer_type = .generator,
        .config_json = "{}",
        .source_text = "source",
    });
    defer std.testing.allocator.free(result);

    try std.testing.expectEqualStrings("contextual", result);
    try std.testing.expectEqual(@as(usize, 1), state.planning_calls);
    try std.testing.expectEqual(@as(usize, 1), state.execution_calls);
    try std.testing.expectEqual(@as(?u64, null), producer.invocation_context.deadline_ns);
    try std.testing.expectEqual(@as(?usize, null), producer.invocation_context.max_response_bytes);
}

test "asset producer forwards request context to cancellable implementations" {
    const Probe = struct {
        context_calls: usize = 0,

        fn legacy(_: *anyopaque, _: Allocator, _: Request) anyerror![]u8 {
            return error.LegacyCallbackInvoked;
        }

        fn controlled(raw: *anyopaque, alloc: Allocator, _: Request, context: InvocationContext) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try context.check();
            self.context_calls += 1;
            return try alloc.dupe(u8, "controlled");
        }
    };

    var probe = Probe{};
    const producer = Producer{
        .ptr = &probe,
        .vtable = &.{ .produce = Probe.legacy, .produce_with_context = Probe.controlled },
    };
    const output = try producer.produceWithContext(std.testing.allocator, .{
        .producer_type = .copy,
        .config_json = "",
        .source_text = "input",
    }, .{ .io = std.testing.io, .deadline_ns = null });
    defer std.testing.allocator.free(output);
    try std.testing.expectEqualStrings("controlled", output);
    try std.testing.expectEqual(@as(usize, 1), probe.context_calls);
}
