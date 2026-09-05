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
const RequestContext = @import("../../../inference/request_context.zig").RequestContext;

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
};

pub const Producer = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        produce: *const fn (ptr: *anyopaque, alloc: Allocator, request: Request) anyerror![]u8,
        produce_with_context: ?*const fn (ptr: *anyopaque, alloc: Allocator, request: Request, context: RequestContext) anyerror![]u8 = null,
        produce_batch: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror![][]u8 = null,
        produce_batch_with_context: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request, context: RequestContext) anyerror![][]u8 = null,
        can_produce_batch: ?*const fn (ptr: *anyopaque, alloc: Allocator, requests: []const Request) anyerror!bool = null,
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

    pub fn produceWithContext(self: Producer, alloc: Allocator, request: Request, context: RequestContext) ![]u8 {
        try context.check();
        const output = if (self.vtable.produce_with_context) |produce_with_context|
            try produce_with_context(self.ptr, alloc, request, context)
        else
            try self.produce(alloc, request);
        context.check() catch |err| {
            alloc.free(output);
            return err;
        };
        return output;
    }

    pub fn produceBatch(self: Producer, alloc: Allocator, requests: []const Request) ![][]u8 {
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

    pub fn produceBatchWithContext(self: Producer, alloc: Allocator, requests: []const Request, context: RequestContext) ![][]u8 {
        try context.check();
        const out = if (self.vtable.produce_batch_with_context) |produce_batch|
            try produce_batch(self.ptr, alloc, requests, context)
        else blk: {
            const items = try alloc.alloc([]u8, requests.len);
            errdefer {
                for (items) |item| if (item.len > 0) alloc.free(item);
                alloc.free(items);
            }
            for (items) |*item| item.* = "";
            for (requests, 0..) |request, i| items[i] = try self.produceWithContext(alloc, request, context);
            break :blk items;
        };
        context.check() catch |err| {
            for (out) |item| if (item.len > 0) alloc.free(item);
            alloc.free(out);
            return err;
        };
        return out;
    }

    /// Reports whether produceBatch can execute the request set as one native
    /// operation. A missing hook preserves the existing producer contract: a
    /// provided batch function is assumed native, while the generic fallback is
    /// explicitly sequential.
    pub fn canProduceBatch(self: Producer, alloc: Allocator, requests: []const Request) !bool {
        if (self.vtable.produce_batch == null) return false;
        if (self.vtable.can_produce_batch) |can_produce_batch|
            return try can_produce_batch(self.ptr, alloc, requests);
        return true;
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

test "asset producer forwards request context to cancellable implementations" {
    const Probe = struct {
        context_calls: usize = 0,

        fn legacy(_: *anyopaque, _: Allocator, _: Request) anyerror![]u8 {
            return error.LegacyCallbackInvoked;
        }

        fn controlled(raw: *anyopaque, alloc: Allocator, _: Request, context: RequestContext) anyerror![]u8 {
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
