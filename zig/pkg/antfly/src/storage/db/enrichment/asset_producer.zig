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

const Allocator = std.mem.Allocator;

pub const ProducerType = enum {
    copy,
    generator,
    reader,
    transcriber,

    pub fn parse(text: []const u8) ?ProducerType {
        if (std.mem.eql(u8, text, "copy")) return .copy;
        if (std.mem.eql(u8, text, "generator")) return .generator;
        if (std.mem.eql(u8, text, "reader")) return .reader;
        if (std.mem.eql(u8, text, "transcriber")) return .transcriber;
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
};

pub const Producer = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        produce: *const fn (ptr: *anyopaque, alloc: Allocator, request: Request) anyerror![]u8,
        deinit: ?*const fn (ptr: *anyopaque, alloc: Allocator) void = null,
    };

    pub fn produce(self: Producer, alloc: Allocator, request: Request) ![]u8 {
        return try self.vtable.produce(self.ptr, alloc, request);
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
