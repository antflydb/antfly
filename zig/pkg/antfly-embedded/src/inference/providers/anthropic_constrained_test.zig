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
const httpx = @import("httpx");
const constrained = @import("antfly_structured");
const runtime = @import("../../generating/constrained_runtime.zig");
const remote = @import("antfly_inference_remote_capabilities");
const A = std.mem.Allocator;
const V = std.json.Value;

fn checkSchema(schema: V) anyerror!void {
    for ([_][]const u8{ "minimum", "maximum", "minLength", "maxLength", "maxItems", "uniqueItems" }) |key| try std.testing.expect(!schema.object.contains(key));
    if (schema.object.get("properties")) |properties| for (properties.object.values()) |child| try checkSchema(child);
    if (schema.object.get("items")) |child| try checkSchema(child);
    if (schema.object.get("anyOf")) |children| for (children.array.items) |child| try checkSchema(child);
}

const Fixture = struct {
    fn check(request: httpx.testing_mod.RequestInfo) !void {
        try std.testing.expectEqualStrings("fixture-key", request.header("x-api-key").?);
        try std.testing.expectEqualStrings("2026-10-21", request.header("anthropic-version").?);
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const body = try std.json.parseFromSliceLeaky(V, arena.allocator(), request.body, .{});
        const model = body.object.get("model").?.string;
        if (std.mem.eql(u8, model, "claude-sonnet-5-5")) {
            try std.testing.expect(!body.object.contains("tool_choice"));
            try std.testing.expect(!body.object.contains("tools"));
            const format = body.object.get("output_config").?.object.get("format").?;
            try std.testing.expectEqualStrings("json_schema", format.object.get("type").?.string);
            try checkSchema(format.object.get("schema").?);
        } else {
            try std.testing.expectEqualStrings("claude-sonnet-4-5", model);
            try std.testing.expect(!body.object.contains("output_config"));
            try std.testing.expectEqualStrings("tool", body.object.get("tool_choice").?.object.get("type").?.string);
            const tool = body.object.get("tools").?.array.items[0];
            try std.testing.expect(tool.object.get("strict").?.bool);
            try checkSchema(tool.object.get("input_schema").?);
        }
    }

    fn run(a: A, client: *httpx.Client, url: []const u8, model: []const u8, failure: *?anyerror) std.Io.Cancelable!void {
        execute(a, client, url, model) catch |err| {
            failure.* = err;
        };
    }

    fn execute(backing: A, client: *httpx.Client, url: []const u8, model: []const u8) !void {
        var arena = std.heap.ArenaAllocator.init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        const config_json = try std.json.Stringify.valueAlloc(a, .{
            .provider = "anthropic",
            .model = model,
            .api_key = "fixture-key",
            .url = url,
            .schema = .{ .entities = .{"person"} },
        }, .{});
        var config = try constrained.extraction.parse(a, config_json);
        defer config.deinit();
        var cache = remote.Cache.init(backing, client.io);
        defer cache.deinit();
        var engine = runtime.Runtime{ .http = client, .cache = &cache };
        const caps = try engine.capabilities(a, &config.value.generator);
        const mode = try caps.select(config.value.output, config.value.probabilities);
        const result = try engine.execute(a, config.value.generator, mode, try constrained.extraction.outputSchema(a, config.value), "extract people", config.value.batch, &.{"Ada"});
        try std.testing.expectEqual(@as(usize, 1), result.calls);
        try std.testing.expect(result.failures[0] == null);
        try std.testing.expectEqualStrings("Ada", result.values[0].?.object.get("entities").?.array.items[0].object.get("text").?.string);
    }
};

pub fn run() !void {
    const a = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const answer = "{\"items\":[{\"item\":0,\"value\":{\"entities\":[{\"label\":\"person\",\"text\":\"Ada\",\"present\":true}],\"relations\":[]}}]}";
    for ([_][]const u8{ "claude-sonnet-5-5", "claude-sonnet-4-5" }) |model| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const content = if (std.mem.eql(u8, model, "claude-sonnet-5-5"))
            try std.json.Stringify.valueAlloc(scratch, .{.{ .type = "text", .text = answer }}, .{})
        else
            try std.fmt.allocPrint(scratch, "[{{\"type\":\"tool_use\",\"id\":\"call-1\",\"name\":\"antfly_output\",\"input\":{s}}}]", .{answer});
        const response = try std.fmt.allocPrint(scratch, "{{\"id\":\"msg-1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"{s}\",\"content\":{s},\"stop_reason\":\"{s}\",\"usage\":{{\"input_tokens\":3,\"output_tokens\":4}}}}", .{ model, content, if (std.mem.eql(u8, model, "claude-sonnet-5-5")) @as([]const u8, "end_turn") else "tool_use" });
        var server = try httpx.TestServer.start(a, io, &.{.{ .method = .POST, .path = "/v1/messages", .assert_request = Fixture.check, .respond = .{ .body = response } }});
        defer server.deinit();
        var client = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
        defer client.deinit();
        const url = try std.fmt.allocPrint(scratch, "{s}/v1", .{server.baseUrl()});
        var failure: ?anyerror = null;
        var group = std.Io.Group.init;
        try group.concurrent(io, Fixture.run, .{ a, &client, url, model, &failure });
        try server.handleOne();
        try group.await(io);
        if (failure) |err| return err;
    }
}
