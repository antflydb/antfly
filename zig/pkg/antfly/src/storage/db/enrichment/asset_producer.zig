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
            .item_error => |err| return err,
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
        return try self.vtable.produce(self.ptr, alloc, request);
    }

    pub fn produceBatch(self: Producer, alloc: Allocator, requests: []const Request) ![][]u8 {
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
            out[i] = try self.produce(alloc, request);
        }
        return out;
    }

    /// Returns the execution path that actually completed. Legacy callbacks
    /// are conservatively classified as compatibility execution; capability
    /// prediction is never presented as observed telemetry.
    pub fn produceBatchReported(self: Producer, alloc: Allocator, requests: []const Request) !ProducedBatch {
        if (self.vtable.produce_batch_reported) |reported| {
            const batch = try reported(self.ptr, alloc, requests);
            try batch.execution.validate();
            if (batch.items.len != requests.len) {
                var owned = batch;
                owned.deinit(alloc);
                return error.InvalidProducedBatchCardinality;
            }
            return batch;
        }
        const items = try self.produceBatch(alloc, requests);
        return try producedBatchFromOutputs(alloc, requests, items, inference_work.ExecutionReport.compatibility(requests.len));
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

    pub fn deinit(self: Producer, alloc: Allocator) void {
        if (self.vtable.deinit) |deinit_fn| deinit_fn(self.ptr, alloc);
    }
};

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

test "asset producer parses extractor config" {
    const alloc = std.testing.allocator;
    var cfg = try parseProducerConfig(alloc,
        \\{"type":"extractor","config":{"provider":"antfly","model":"gliner","schema":{"entities":["person"]}}}
    );
    defer cfg.deinit(alloc);
    try std.testing.expectEqual(ProducerType.extractor, cfg.type);
    try std.testing.expect(std.mem.indexOf(u8, cfg.config_json, "\"entities\"") != null);
}
