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

//! Pure producer envelope parsing shared by admission and execution.
const std = @import("std");
const generating_runtime = @import("antfly_generating");
const constrained = @import("antfly_structured");
const Allocator = std.mem.Allocator;

pub const ToolOutput = enum {
    arguments,
    content,
};

pub const Config = struct {
    generator: generating_runtime.GeneratorConfig,
    parsed: std.json.Parsed(std.json.Value),
    tool_choice: ?std.json.Value = null,
    tool_name: ?[]const u8 = null,
    tool_output: ToolOutput = .content,
    output_schema: ?std.json.Value = null,
    instructions: []const u8 = "",
    output: constrained.Output = .auto,
    batch: constrained.Batch = .{},

    pub fn deinit(self: *Config, alloc: Allocator) void {
        self.generator.deinit(alloc);
        self.parsed.deinit();
        self.* = undefined;
    }
};

pub fn parse(alloc: Allocator, raw: []const u8) !Config {
    var parsed = try @import("antfly-json").parseValueLossless(alloc, raw);
    errdefer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGeneratorConfig;

    var cfg = try generatorConfigFromValue(alloc, parsed.value);
    errdefer cfg.deinit(alloc);

    const schema = parsed.value.object.get("output_schema");
    if (schema) |value| {
        if (value != .object or parsed.value.object.contains("tools") or parsed.value.object.contains("tool_choice") or parsed.value.object.contains("tool_output")) return error.InvalidConstrainedGenerationConfig;
        try constrained.validateSchema(value);
        if (cfg.provider != .antfly) try constrained.validateProviderSchema(cfg, try constrained.hostedCapabilities(cfg).select(.auto, null), value);
    }
    const instructions = if (parsed.value.object.get("instructions")) |value| blk: {
        if (value != .string or value.string.len > 65536) return error.InvalidConstrainedGenerationConfig;
        break :blk value.string;
    } else "";
    const output = if (parsed.value.object.get("output")) |value| blk: {
        if (value != .string) return error.InvalidConstrainedGenerationConfig;
        break :blk std.meta.stringToEnum(constrained.Output, value.string) orelse return error.InvalidConstrainedGenerationConfig;
    } else .auto;
    const batch = if (parsed.value.object.get("batch")) |value| (try std.json.parseFromValue(constrained.Batch, parsed.arena.allocator(), value, .{})).value else constrained.Batch{};
    try batch.validate();
    if (schema == null and (output != .auto or parsed.value.object.contains("batch") or parsed.value.object.contains("instructions"))) return error.InvalidConstrainedGenerationConfig;
    const tools = parsed.value.object.get("tools");
    const tool_choice = parsed.value.object.get("tool_choice");
    const tool_name = if (parsed.value.object.get("tool_name")) |value|
        if (value == .string) value.string else null
    else
        forcedToolName(tool_choice);
    const tool_output = if (parsed.value.object.get("tool_output")) |value| blk: {
        if (value != .string) return error.InvalidGeneratorToolConfig;
        if (std.mem.eql(u8, value.string, "arguments")) break :blk ToolOutput.arguments;
        if (std.mem.eql(u8, value.string, "content")) break :blk ToolOutput.content;
        return error.InvalidGeneratorToolConfig;
    } else if (tools != null) ToolOutput.arguments else ToolOutput.content;

    return .{
        .generator = cfg,
        .parsed = parsed,
        .tool_choice = tool_choice,
        .tool_name = tool_name,
        .tool_output = tool_output,
        .output_schema = schema,
        .instructions = instructions,
        .output = output,
        .batch = batch,
    };
}

pub fn generatorConfigFromValue(alloc: Allocator, value: std.json.Value) !generating_runtime.GeneratorConfig {
    if (value != .object) return error.InvalidGeneratorConfig;
    const nested = value.object.get("generator");
    // Asset-only envelope fields are handled here; all provider configuration
    // is parsed by the canonical generator contract (including future fields).
    var object: std.json.ObjectMap = .empty;
    defer object.deinit(alloc);
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (std.mem.eql(u8, key, "generator") or std.mem.eql(u8, key, "tools") or std.mem.eql(u8, key, "tool_choice") or
            std.mem.eql(u8, key, "tool_name") or std.mem.eql(u8, key, "tool_output") or
            std.mem.eql(u8, key, "output_schema") or std.mem.eql(u8, key, "instructions") or
            std.mem.eql(u8, key, "output") or std.mem.eql(u8, key, "batch")) continue;
        try object.put(alloc, key, entry.value_ptr.*);
    }
    var cfg = if (nested) |generator| try generating_runtime.parseConfigFromValue(alloc, generator) else try generating_runtime.parseConfigFromValue(alloc, .{ .object = object });
    errdefer cfg.deinit(alloc);
    if (value.object.get("tools")) |tools| cfg.tools_json = try std.json.Stringify.valueAlloc(alloc, tools, .{});
    if (value.object.get("tool_choice")) |choice| cfg.tool_choice_json = try std.json.Stringify.valueAlloc(alloc, choice, .{});
    return cfg;
}

pub fn forcedToolName(tool_choice: ?std.json.Value) ?[]const u8 {
    const choice = tool_choice orelse return null;
    switch (choice) {
        .object => |obj| {
            const function = obj.get("function") orelse return null;
            if (function != .object) return null;
            const name = function.object.get("name") orelse return null;
            return if (name == .string) name.string else null;
        },
        else => return null,
    }
}

test "generating backend producer configuration retains exact output schema literals" {
    const a = std.testing.allocator;
    var cfg = try parse(a,
        \\{"generator":{"provider":"antfly","model":"fixture","temperature":0.5},"output_schema":{"type":"number","minimum":1e-400,"maximum":1.0000000000000001}}
    );
    defer cfg.deinit(a);
    try std.testing.expectEqualStrings("1e-400", cfg.output_schema.?.object.get("minimum").?.number_string);
    try std.testing.expectEqualStrings("1.0000000000000001", cfg.output_schema.?.object.get("maximum").?.number_string);
}
