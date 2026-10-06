// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Process context and symbol visibility shared by the independent runtime roots.

const builtin = @import("builtin");

const std = @import("std");

const platform = @import("antfly_platform");

const bridge = @import("runtime_bridge.zig");

pub fn runtimeEntry(
    context: *const bridge.Context,
    comptime role_name: []const u8,
    comptime run: fn (std.process.Init, []const u8, *std.process.Args.Iterator) anyerror!void,
) c_int {
    if (!context.valid()) {
        std.debug.print("antfly {s}: invalid runtime process ABI context\n", .{role_name});
        return 1;
    }
    var process = RuntimeProcess.init(context) catch |err| {
        std.debug.print("antfly {s}: failed to initialize runtime process context (error.{s})\n", .{ role_name, @errorName(err) });
        return 1;
    };
    defer process.deinit();
    var args = std.process.Args.Iterator.initAllocator(process.processArgs(), process.alloc) catch |err| {
        std.debug.print("antfly {s}: failed to initialize runtime arguments (error.{s})\n", .{ role_name, @errorName(err) });
        return 1;
    };
    defer args.deinit();
    _ = args.next(); // synthetic argv[0], owned by this runtime unit
    const command = context.command.slice();

    run(process.processInit(), command, &args) catch |err| {
        if (@errorReturnTrace()) |trace| std.debug.dumpErrorReturnTrace(trace);
        const message = switch (err) {
            error.FileNotFound => "required file was not found; check the configured path",
            error.AddressInUse => "listen address is already in use",
            error.InvalidCharacter, error.InvalidArguments => "invalid command-line value; run with --help",
            else => "startup failed; see the preceding diagnostic for details",
        };
        std.debug.print("antfly {s}: {s} (error.{s})\n", .{ role_name, message, @errorName(err) });
        return 1;
    };
    return 0;
}

const RuntimeProcess = struct {
    alloc: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    io_impl: std.Io.Threaded,
    process_environ: std.process.Environ,
    environ_map: std.process.Environ.Map,
    argument_storage: [][:0]u8,
    argument_ptrs: [][*:0]const u8,
    /// Windows `std.process.Args` is a WTF-16 command line rather than argv.
    windows_command_line: if (builtin.os.tag == .windows) [:0]u16 else void,
    preopens: std.process.Preopens,

    fn init(context: *const bridge.Context) !RuntimeProcess {
        const alloc = runtimeAllocator();
        const input_arguments = context.arguments() orelse return error.InvalidArgument;
        const argument_storage = try alloc.alloc([:0]u8, input_arguments.len + 1);
        errdefer alloc.free(argument_storage);
        var initialized_arguments: usize = 0;
        errdefer for (argument_storage[0..initialized_arguments]) |argument| alloc.free(argument);
        argument_storage[0] = try alloc.dupeSentinel(u8, "antfly-runtime", 0);
        initialized_arguments = 1;
        for (input_arguments, 1..) |argument, index| {
            argument_storage[index] = try alloc.dupeSentinel(u8, argument.slice(), 0);
            initialized_arguments += 1;
        }
        const argument_ptrs = try alloc.alloc([*:0]const u8, argument_storage.len);
        errdefer alloc.free(argument_ptrs);
        for (argument_storage, argument_ptrs) |argument, *pointer| pointer.* = argument.ptr;

        var environ_map = std.process.Environ.Map.init(alloc);
        errdefer environ_map.deinit();
        for (context.environment() orelse return error.InvalidArgument) |entry| {
            if (!std.process.Environ.Map.validateKeyForPut(entry.name.slice()) or
                std.mem.indexOfScalar(u8, entry.value.slice(), 0) != null)
                return error.InvalidArgument;
            try environ_map.put(entry.name.slice(), entry.value.slice());
        }

        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const preopens = try std.process.Preopens.init(arena.allocator());
        const process_environ: std.process.Environ = switch (builtin.os.tag) {
            .wasi => @compileError("partitioned Antfly runtime process ABI does not support WASI"),
            // Runtime units share the host process, so the PEB environment is
            // the same one the bridge context was built from.
            .windows => .{ .block = .global },
            else => .{ .block = try environ_map.createPosixBlock(alloc, .{}) },
        };
        errdefer process_environ.block.deinit(alloc);
        const windows_command_line = if (builtin.os.tag == .windows)
            try platform.process.windowsCommandLine(alloc, argument_storage[0], argument_ptrs[1..])
        else {};
        errdefer if (builtin.os.tag == .windows) alloc.free(windows_command_line);
        const io_impl = std.Io.Threaded.init(alloc, .{ .environ = process_environ });

        return .{
            .alloc = alloc,
            .arena = arena,
            .io_impl = io_impl,
            .process_environ = process_environ,
            .environ_map = environ_map,
            .argument_storage = argument_storage,
            .argument_ptrs = argument_ptrs,
            .windows_command_line = windows_command_line,
            .preopens = preopens,
        };
    }

    pub fn deinit(self: *RuntimeProcess) void {
        self.io_impl.deinit();
        self.process_environ.block.deinit(self.alloc);
        self.environ_map.deinit();
        self.arena.deinit();
        for (self.argument_storage) |argument| self.alloc.free(argument);
        self.alloc.free(self.argument_storage);
        self.alloc.free(self.argument_ptrs);
        if (builtin.os.tag == .windows) self.alloc.free(self.windows_command_line);
        self.* = undefined;
    }

    fn processArgs(self: *const RuntimeProcess) std.process.Args {
        switch (builtin.os.tag) {
            .windows => return .{ .vector = self.windows_command_line },
            .wasi => @compileError("partitioned Antfly runtime process ABI does not support WASI"),
            else => return .{ .vector = self.argument_ptrs },
        }
    }

    fn processInit(self: *RuntimeProcess) std.process.Init {
        return .{
            .minimal = .{ .args = self.processArgs(), .environ = self.process_environ },
            .arena = &self.arena,
            .gpa = self.alloc,
            .io = self.io_impl.io(),
            .environ_map = &self.environ_map,
            .preopens = self.preopens,
        };
    }
};

pub fn exportInternal(comptime function: anytype, comptime name: []const u8) void {
    @export(function, .{ .name = name, .visibility = .hidden });
}

pub fn runtimeAllocator() std.mem.Allocator {
    const fallback = if (!builtin.single_threaded) std.heap.smp_allocator else std.heap.page_allocator;
    return platform.allocator.processAllocator(fallback);
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
