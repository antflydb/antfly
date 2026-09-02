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
const inference_work = @import("../../../inference/work.zig");

const Allocator = std.mem.Allocator;

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

    pub const VTable = struct {
        produce: *const fn (ptr: *anyopaque, alloc: Allocator, request: Request) anyerror![]u8,
        produce_batch: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror![][]u8 = null,
        produce_batch_reported: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!ProducedBatch = null,
        batch_mode: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!inference_work.BatchMode = null,
        can_produce_batch: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!bool = null,
        capabilities_for_requests: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!?inference_work.InferenceCapabilities = null,
        /// Complete peak-memory and result contract for the concrete route
        /// selected by these requests. Implementations that publish this hook
        /// apply it to every invocation, independent of how media is encoded.
        invocation_memory_for_requests: ?*const fn (
            ptr: *anyopaque,
            alloc: Allocator,
            requests: []const Request,
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
    };

    pub fn foregroundBoundedForRequests(self: Producer, alloc: Allocator, requests: []const Request) !bool {
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
            const output = self.vtable.produce(self.ptr, bounded_alloc, request) catch |err| {
                if (bounded.limit_exceeded) return error.InferenceInvocationMemoryExceeded;
                return err;
            };
            if (output.len > resolved.max_result_bytes) {
                alloc.free(output);
                return error.InferenceResultTooLarge;
            }
            return output;
        }
        return try self.vtable.produce(self.ptr, alloc, request);
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
            validateOutputBytes(outputs, resolved.max_result_bytes) catch |err| {
                for (outputs) |output| if (output.len > 0) alloc.free(output);
                alloc.free(outputs);
                return err;
            };
            return outputs;
        }
        return try self.produceBatchUnchecked(alloc, requests);
    }

    fn produceBatchUnchecked(self: Producer, alloc: Allocator, requests: []const Request) ![][]u8 {
        if (self.vtable.produce_batch_reported) |reported| {
            var batch = try reported(self.ptr, alloc, requests);
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
            out[i] = try self.vtable.produce(self.ptr, alloc, request);
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
            validateProducedResultBytes(batch.items, resolved.max_result_bytes) catch |err| {
                batch.deinit(alloc);
                return err;
            };
            return batch;
        }
        return try self.produceBatchReportedUnchecked(alloc, requests);
    }

    fn produceBatchReportedUnchecked(self: Producer, alloc: Allocator, requests: []const Request) !ProducedBatch {
        if (self.vtable.produce_batch_reported) |reported| {
            var batch = try reported(self.ptr, alloc, requests);
            errdefer batch.deinit(alloc);
            try batch.execution.validate();
            if (batch.items.len != requests.len) {
                return error.InvalidProducedBatchCardinality;
            }
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
        if (self.vtable.invocation_memory_for_requests == null) {
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
        if (self.vtable.produce_batch == null and self.vtable.produce_batch_reported == null) return .none;
        if (self.vtable.batch_mode) |batch_mode|
            return try batch_mode(self.ptr, alloc, requests);
        if (self.vtable.can_produce_batch) |can_produce_batch|
            return if (try can_produce_batch(self.ptr, alloc, requests)) .native else .none;
        return .serial_compatibility;
    }

    /// Compatibility wrapper for callers that only need to choose between the
    /// batch entry point and singleton execution.
    pub fn canProduceBatch(self: Producer, alloc: Allocator, requests: []const Request) !bool {
        return try self.batchMode(alloc, requests) != .none;
    }

    pub fn capabilitiesForRequests(self: Producer, alloc: Allocator, requests: []const Request) !?inference_work.InferenceCapabilities {
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
        if (self.vtable.invocation_memory_for_requests == null) {
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
        const resolve = self.vtable.invocation_memory_for_requests.?;
        var bounded = inference_work.BoundedInvocationAllocator.init(
            alloc,
            try invocationResolutionLimit(requests),
        );
        const plan = resolve(self.ptr, bounded.allocator(), requests) catch |err| {
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
            request.producer_type == .transcriber) return true;
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

fn validateOutputBytes(outputs: []const []u8, limit: usize) !void {
    var total: usize = 0;
    for (outputs) |output| {
        total = std.math.add(usize, total, output.len) catch return error.InferenceResultTooLarge;
        if (total > limit) return error.InferenceResultTooLarge;
    }
}

fn validateProducedResultBytes(items: []const ProducedItem, limit: usize) !void {
    var total: usize = 0;
    for (items) |item| switch (item.result) {
        .value => |output| {
            total = std.math.add(usize, total, output.len) catch return error.InferenceResultTooLarge;
            if (total > limit) return error.InferenceResultTooLarge;
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
    };

    var context: u8 = 0;
    const producer: Producer = .{
        .ptr = &context,
        .vtable = &.{
            .produce = Fake.produce,
            .produce_batch_reported = Fake.produceBatchReported,
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
    };

    var context: u8 = 0;
    const producer: Producer = .{
        .ptr = &context,
        .vtable = &.{
            .produce = Fake.produce,
            .produce_batch_reported = Fake.produceBatchReported,
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
