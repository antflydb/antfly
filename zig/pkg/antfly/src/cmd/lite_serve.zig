// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const standalone_runtime = @import("../standalone/runtime.zig");

pub const ServeOptions = struct {
    path: []const u8,
    addr: []const u8 = "127.0.0.1:8080",
    fsync: bool = true,
    standalone_args: std.ArrayListUnmanaged([]const u8) = .empty,

    pub fn deinit(self: *ServeOptions, alloc: std.mem.Allocator) void {
        self.standalone_args.deinit(alloc);
    }
};

pub const LiteListenAddress = struct {
    host: []const u8,
    port: u16,
};

pub fn run(init: std.process.Init, args: *std.process.Args.Iterator) !void {
    var opts = try parseServeOptions(init.gpa, args);
    defer opts.deinit(init.gpa);
    return try serveWithOptions(init, opts);
}

fn serveWithOptions(init: std.process.Init, opts: ServeOptions) !void {
    try requireAflitePath(opts.path);
    const listen = try parseLiteListenAddress(opts.addr);
    return try standalone_runtime.runLite(init, opts.path, listen.host, listen.port, opts.fsync, opts.standalone_args.items);
}

pub fn parseServeOptions(alloc: std.mem.Allocator, args: *std.process.Args.Iterator) !ServeOptions {
    const path = args.next() orelse {
        std.debug.print("error: database path is required\n", .{});
        return error.InvalidArguments;
    };
    var opts: ServeOptions = .{ .path = path };
    errdefer opts.deinit(alloc);
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--addr")) {
            opts.addr = args.next() orelse {
                std.debug.print("error: --addr value is required\n", .{});
                return error.InvalidArguments;
            };
        } else if (std.mem.eql(u8, arg, "--fsync")) {
            opts.fsync = parseLiteBool(args.next() orelse return error.InvalidArguments) orelse return error.InvalidArguments;
        } else if (std.mem.startsWith(u8, arg, "--fsync=")) {
            opts.fsync = parseLiteBool(arg["--fsync=".len..]) orelse return error.InvalidArguments;
        } else if (isReservedLiteServeFlag(arg)) {
            std.debug.print("error: {s} is controlled by antfly lite serve\n", .{arg});
            return error.InvalidArguments;
        } else {
            try opts.standalone_args.append(alloc, arg);
        }
    }
    return opts;
}

pub fn isReservedLiteServeFlag(arg: []const u8) bool {
    for ([_][]const u8{ "--storage-engine", "--storage-path", "--host", "--port" }) |flag| {
        if (std.mem.eql(u8, arg, flag) or (arg.len > flag.len and std.mem.startsWith(u8, arg, flag) and arg[flag.len] == '=')) return true;
    }
    return false;
}

pub fn parseLiteBool(value: []const u8) ?bool {
    if (std.mem.eql(u8, value, "true")) return true;
    if (std.mem.eql(u8, value, "false")) return false;
    return null;
}

pub fn parseLiteListenAddress(addr: []const u8) !LiteListenAddress {
    const sep = std.mem.lastIndexOfScalar(u8, addr, ':') orelse return error.InvalidArguments;
    if (sep == 0 or sep + 1 >= addr.len) return error.InvalidArguments;
    const port = try std.fmt.parseInt(u16, addr[sep + 1 ..], 10);
    const host = addr[0..sep];
    if (!isLiteLocalListenHost(host)) return error.InvalidArguments;
    return .{ .host = host, .port = port };
}

pub fn isLiteLocalListenHost(host: []const u8) bool {
    return std.mem.eql(u8, host, "localhost") or
        std.mem.eql(u8, host, "127.0.0.1") or
        std.mem.eql(u8, host, "::1") or
        std.mem.eql(u8, host, "[::1]");
}

fn requireAflitePath(path: []const u8) !void {
    if (!std.mem.endsWith(u8, path, ".aflite")) {
        std.debug.print("error: Antfly Lite database paths must end in .aflite: {s}\n", .{path});
        return error.InvalidArguments;
    }
}
