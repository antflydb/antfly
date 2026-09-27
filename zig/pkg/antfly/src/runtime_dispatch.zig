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
const runtime_bridge = @import("runtime_bridge.zig");
pub const Context = runtime_bridge.Context;

pub fn run(runtime_entry: *const fn (*const Context) callconv(.c) c_int, command: []const u8, init: std.process.Init, args: *std.process.Args.Iterator) !void {
    var argument_views: std.ArrayListUnmanaged(runtime_bridge.Bytes) = .empty;
    defer argument_views.deinit(init.gpa);
    while (args.next()) |arg| try argument_views.append(init.gpa, .init(arg));

    const environment_names = init.environ_map.keys();
    const environment_values = init.environ_map.values();
    std.debug.assert(environment_names.len == environment_values.len);
    const environment = try init.gpa.alloc(runtime_bridge.EnvironmentEntry, environment_names.len);
    defer init.gpa.free(environment);
    for (environment, environment_names, environment_values) |*entry, name, value| {
        entry.* = .{ .name = .init(name), .value = .init(value) };
    }

    const context = runtime_bridge.Context{
        .command = .init(command),
        .arguments_ptr = if (argument_views.items.len == 0) null else argument_views.items.ptr,
        .arguments_len = argument_views.items.len,
        .environment_ptr = if (environment.len == 0) null else environment.ptr,
        .environment_len = environment.len,
    };
    const code = runtime_entry(&context);
    if (code != 0) std.process.exit(@intCast(code));
}
