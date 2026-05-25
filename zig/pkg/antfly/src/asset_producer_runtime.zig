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
const httpx = @import("httpx");
const generating_runtime = @import("generating/mod.zig");
const readers = @import("antfly_readers");
const transcribing = @import("antfly_transcribing");
const asset_producer = @import("storage/db/enrichment/asset_producer.zig");

const Allocator = std.mem.Allocator;

pub const Runtime = struct {
    alloc: Allocator,
    http: *httpx.Client,

    pub fn init(alloc: Allocator, http: *httpx.Client) Runtime {
        return .{
            .alloc = alloc,
            .http = http,
        };
    }

    pub fn producer(self: *Runtime) asset_producer.Producer {
        return .{
            .ptr = self,
            .vtable = &.{ .produce = produce },
        };
    }

    fn produce(ptr: *anyopaque, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        return switch (request.producer_type) {
            .copy => try alloc.dupe(u8, request.source_text),
            .generator => try self.generate(alloc, request),
            .reader => try self.read(alloc, request),
            .transcriber => try self.transcribe(alloc, request),
        };
    }

    fn generate(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        const cfg = try generating_runtime.parseConfigFromSlice(alloc, request.config_json);
        defer cfg.deinit(alloc);
        const link = generating_runtime.ChainLink{ .generator = cfg };
        var result = try generating_runtime.executeChain(alloc, self.http, &.{link}, &.{
            .{ .role = .user, .content = request.source_text },
        });
        defer result.deinit();
        return try alloc.dupe(u8, result.content);
    }

    fn read(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var cfg_parsed = try std.json.parseFromSlice(readers.Config, alloc, request.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();

        var registry = readers.Registry.init(alloc);
        defer registry.deinit();
        try registry.registerConfig("asset", cfg_parsed.value);

        var runtime = readers.Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.loadFromRegistry(self.http, &registry);

        var source = try parseReaderSource(alloc, request.source_text, request.source_parts_json);
        defer source.deinit(alloc);

        const provider = try runtime.get("asset");
        const results = try provider.read(alloc, .{
            .images = source.images,
            .prompt = source.prompt,
        });
        defer {
            for (results) |*result| readers.deinitResult(alloc, result);
            alloc.free(results);
        }

        return try encodeReaderResults(alloc, request.content_type, results);
    }

    fn transcribe(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var cfg_parsed = try std.json.parseFromSlice(transcribing.Config, alloc, request.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();

        var registry = transcribing.Registry.init(alloc);
        defer registry.deinit();
        try registry.registerConfig("asset", cfg_parsed.value);

        var runtime = transcribing.Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.loadFromRegistry(self.http, &registry);

        const provider = try runtime.get("asset");
        var result = try provider.transcribe(alloc, .{ .url = request.source_text });
        defer transcribing.deinitResponse(alloc, &result);

        if (isJsonContentType(request.content_type)) {
            return try std.json.Stringify.valueAlloc(alloc, result, .{});
        }
        return try alloc.dupe(u8, result.text orelse "");
    }
};

const ReaderSource = struct {
    images: []const []const u8,
    prompt: ?[]const u8 = null,

    fn deinit(self: *ReaderSource, alloc: Allocator) void {
        for (self.images) |image| alloc.free(@constCast(image));
        alloc.free(self.images);
        if (self.prompt) |prompt| alloc.free(@constCast(prompt));
        self.* = undefined;
    }
};

fn parseReaderSource(alloc: Allocator, source_text: []const u8, source_parts_json: ?[]const u8) !ReaderSource {
    if (source_parts_json) |raw_parts| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_parts, .{});
        defer parsed.deinit();
        if (parsed.value == .array) {
            var images = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (images.items) |image| alloc.free(@constCast(image));
                images.deinit(alloc);
            }
            var prompt = std.ArrayListUnmanaged(u8).empty;
            errdefer prompt.deinit(alloc);

            for (parsed.value.array.items) |part| {
                if (part != .object) continue;
                const type_value = part.object.get("type") orelse continue;
                if (type_value != .string) continue;
                if (std.mem.eql(u8, type_value.string, "text")) {
                    const text = part.object.get("text") orelse continue;
                    if (text != .string) continue;
                    if (prompt.items.len > 0) try prompt.append(alloc, '\n');
                    try prompt.appendSlice(alloc, text.string);
                } else if (std.mem.eql(u8, type_value.string, "media")) {
                    if (part.object.get("url")) |url| {
                        if (url == .string) try images.append(alloc, try alloc.dupe(u8, url.string));
                    } else if (part.object.get("mime_type")) |mime| {
                        const data = part.object.get("data") orelse continue;
                        if (mime == .string and data == .string) {
                            try images.append(alloc, try std.fmt.allocPrint(alloc, "data:{s};base64,{s}", .{ mime.string, data.string }));
                        }
                    }
                }
            }

            if (images.items.len > 0) {
                return .{
                    .images = try images.toOwnedSlice(alloc),
                    .prompt = if (prompt.items.len > 0) try prompt.toOwnedSlice(alloc) else null,
                };
            }
            prompt.deinit(alloc);
            images.deinit(alloc);
        }
    }

    const images = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(images);
    images[0] = try alloc.dupe(u8, source_text);
    return .{ .images = images };
}

fn encodeReaderResults(alloc: Allocator, content_type: []const u8, results: []const readers.Result) ![]u8 {
    if (isJsonContentType(content_type)) {
        return try std.json.Stringify.valueAlloc(alloc, results, .{});
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (results, 0..) |result, i| {
        if (i > 0) try out.append(alloc, '\n');
        try out.appendSlice(alloc, result.text);
    }
    return try out.toOwnedSlice(alloc);
}

fn isJsonContentType(content_type: []const u8) bool {
    return std.mem.eql(u8, content_type, "application/json") or
        std.mem.endsWith(u8, content_type, "+json");
}

test "asset producer runtime parses reader multimodal parts" {
    const alloc = std.testing.allocator;
    var source = try parseReaderSource(alloc, "", "[{\"type\":\"text\",\"text\":\"read\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,aaa\"}]");
    defer source.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), source.images.len);
    try std.testing.expectEqualStrings("read", source.prompt.?);
}
