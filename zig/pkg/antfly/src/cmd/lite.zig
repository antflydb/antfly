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
const local = @import("antfly_local_sources").cmd_lite;
const antfly = @import("../cli_root.zig");
const lite_serve = if (@import("builtin").is_test) @import("lite_serve.zig") else if (antfly.build_options.linked_storage) struct {} else @import("antfly_source_root").antfly_sources.lite_serve;

pub fn runFromIterator(init: std.process.Init, argv0: []const u8, args: *std.process.Args.Iterator) !void {
    const label = try std.fmt.allocPrint(init.gpa, "{s} lite", .{argv0});
    defer init.gpa.free(label);
    return local.runWithServe(init, label, args, if (antfly.build_options.linked_storage) null else lite_serve.run);
}
const ServeOptions = lite_serve.ServeOptions;
const LiteListenAddress = lite_serve.LiteListenAddress;
const parseServeOptions = lite_serve.parseServeOptions;
const isReservedLiteServeFlag = lite_serve.isReservedLiteServeFlag;
const parseLiteBool = lite_serve.parseLiteBool;
const parseLiteListenAddress = lite_serve.parseLiteListenAddress;
const isLiteLocalListenHost = lite_serve.isLiteLocalListenHost;

test "lite serve parser preserves convenience flags and forwards standalone options" {
    {
        const argv = [_][*:0]const u8{"app.aflite"};
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        var opts = try parseServeOptions(std.testing.allocator, &args);
        defer opts.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings("app.aflite", opts.path);
        try std.testing.expectEqualStrings("127.0.0.1:8080", opts.addr);
        try std.testing.expect(opts.fsync);
        const listen = try parseLiteListenAddress(opts.addr);
        try std.testing.expectEqualStrings("127.0.0.1", listen.host);
        try std.testing.expectEqual(@as(u16, 8080), listen.port);
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--fsync=false" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        var opts = try parseServeOptions(std.testing.allocator, &args);
        defer opts.deinit(std.testing.allocator);
        try std.testing.expect(!opts.fsync);
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--addr", "127.0.0.1:9090" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        var opts = try parseServeOptions(std.testing.allocator, &args);
        defer opts.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings("app.aflite", opts.path);
        try std.testing.expectEqualStrings("127.0.0.1:9090", opts.addr);
        const listen = try parseLiteListenAddress(opts.addr);
        try std.testing.expectEqualStrings("127.0.0.1", listen.host);
        try std.testing.expectEqual(@as(u16, 9090), listen.port);
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--port", "9090" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        try std.testing.expectError(error.InvalidArguments, parseServeOptions(std.testing.allocator, &args));
    }
    {
        const argv = [_][*:0]const u8{ "app.aflite", "--config", "production.json", "--admin-api-token", "secret" };
        var args = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
        var opts = try parseServeOptions(std.testing.allocator, &args);
        defer opts.deinit(std.testing.allocator);
        try std.testing.expectEqualSlices([]const u8, &.{ "--config", "production.json", "--admin-api-token", "secret" }, opts.standalone_args.items);
    }
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress("127.0.0.1"));
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress(":8080"));
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress("0.0.0.0:8080"));
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress("192.168.1.10:8080"));
    try std.testing.expectError(error.InvalidArguments, parseLiteListenAddress("[::]:8080"));
    {
        const listen = try parseLiteListenAddress("localhost:8080");
        try std.testing.expectEqualStrings("localhost", listen.host);
        try std.testing.expectEqual(@as(u16, 8080), listen.port);
    }
}
