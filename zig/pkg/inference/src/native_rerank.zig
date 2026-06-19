// Copyright 2026 Antfly, Inc.
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
const backends = @import("backends/backends.zig");
const model_manager_mod = @import("server/model_manager.zig");
const native_backend_choice = @import("native_backend_choice.zig");

const print = std.debug.print;

const Options = struct {
    model_dir: []const u8,
    query: []const u8 = "",
    documents: std.ArrayListUnmanaged([]const u8) = .empty,
    backend: native_backend_choice.Choice = .auto,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        self.documents.deinit(allocator);
    }
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var opts = try parseArgs(allocator, args);
    defer opts.deinit(allocator);

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    try native_backend_choice.validate(opts.backend);
    native_backend_choice.configureSessionPreference(&session_manager, opts.backend);

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(opts.model_dir);
    var pipeline = model.rerankingPipeline(allocator);
    const scores = try pipeline.rerank(opts.query, opts.documents.items);
    defer allocator.free(scores);

    try writeRerankJson(allocator, opts, scores);
}

fn parseArgs(allocator: std.mem.Allocator, args: []const []const u8) !Options {
    if (args.len < 1) {
        printUsage();
        return error.InvalidArguments;
    }

    var opts = Options{ .model_dir = args[0] };
    errdefer opts.deinit(allocator);

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--query")) {
            i += 1;
            if (i >= args.len) return error.MissingQueryValue;
            opts.query = args[i];
        } else if (std.mem.eql(u8, arg, "--doc")) {
            i += 1;
            if (i >= args.len) return error.MissingDocumentValue;
            try opts.documents.append(allocator, args[i]);
        } else if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = native_backend_choice.parse(args[i]) orelse return error.InvalidBackend;
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }

    if (opts.query.len == 0 or opts.documents.items.len == 0) {
        printUsage();
        return error.InvalidArguments;
    }

    return opts;
}

fn writeRerankJson(allocator: std.mem.Allocator, opts: Options, scores: []const f32) !void {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, opts.model_dir);
    try buf.appendSlice(allocator, ",\"query\":");
    try jsonEncodeString(&buf, allocator, opts.query);
    try buf.appendSlice(allocator, ",\"scores\":[");
    for (scores, 0..) |score, idx| {
        if (idx > 0) try buf.append(allocator, ',');
        const fragment = try std.fmt.allocPrint(allocator, "{{\"index\":{d},\"score\":{d}}}", .{ idx, score });
        defer allocator.free(fragment);
        try buf.appendSlice(allocator, fragment);
    }
    try buf.appendSlice(allocator, "]}\n");

    print("{s}", .{buf.items});
}

fn jsonEncodeString(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    try buf.append(allocator, '"');
    for (s) |ch| {
        switch (ch) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => {
                if (ch < 0x20) {
                    const hex = try std.fmt.allocPrint(allocator, "\\u{x:0>4}", .{ch});
                    defer allocator.free(hex);
                    try buf.appendSlice(allocator, hex);
                } else {
                    try buf.append(allocator, ch);
                }
            },
        }
    }
    try buf.append(allocator, '"');
}

fn printUsage() void {
    print(
        \\usage: antfly inference rerank <model-dir> --query <query> --doc <document>... [--backend auto|onnx|native|metal|mlx|cuda]
        \\  Runs local reranking and prints JSON scores in document order.
        \\
    , .{});
}

test "parseArgs collects query documents and backend" {
    const allocator = std.testing.allocator;
    var opts = try parseArgs(allocator, &.{
        "/tmp/model",
        "--query",
        "what is cuda",
        "--doc",
        "cuda is a gpu platform",
        "--doc",
        "unrelated",
        "--backend",
        "native",
    });
    defer opts.deinit(allocator);

    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqualStrings("what is cuda", opts.query);
    try std.testing.expectEqual(@as(usize, 2), opts.documents.items.len);
    try std.testing.expectEqualStrings("cuda is a gpu platform", opts.documents.items[0]);
    try std.testing.expectEqual(native_backend_choice.Choice.native, opts.backend);
}
