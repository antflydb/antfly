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
const antfly_client = @import("antfly-client");
const cli = @import("mod.zig");

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse return listTablespaces(allocator, io, client);

    if (std.mem.eql(u8, subcommand, "list")) return listTablespaces(allocator, io, client);
    if (std.mem.eql(u8, subcommand, "get")) return getTablespace(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "create")) return createTablespace(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "drop")) return dropTablespace(allocator, io, client, args);

    if (std.mem.eql(u8, subcommand, "rename")) {
        const name = args.next() orelse cli.argumentError("resource name is required", .{});
        const new_name = args.next() orelse cli.argumentError("new name is required", .{});
        if (args.next() != null) cli.argumentError("unexpected rename argument", .{});
        var response = try client.inner.renameTablespace(name, .{ .name = new_name });
        defer response.deinit();
        cli.expectHttpSuccess(response);
        if (response.status_code != 204) return error.InvalidApiResponse;
        return;
    }

    cli.argumentError("unknown tablespace subcommand: {s}", .{subcommand});
}

pub fn listTablespaces(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    cli.output.context = .{ .scope = .discovery, .execution = .complete };
    var resp = try client.inner.listTablespaces();
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |parsed| try cli.writeJson(allocator, io, parsed.value);
}

pub fn getTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    cli.output.context = .{ .scope = .discovery, .execution = .complete };
    const tablespace_name = nextNameArg(args, "tablespace name is required");
    var resp = try client.inner.getTablespace(tablespace_name);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |parsed| try cli.writeJson(allocator, io, parsed.value);
}

pub fn createTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var parsed = parseCreateArgs(allocator, args);
    defer parsed.deinit(allocator);
    const tablespace_name = parsed.tablespace_name orelse cli.argumentError("tablespace name is required", .{});
    const body = antfly_client.types.CreateTablespaceRequest{
        .location_json = parsed.location_json,
        .placement_policy_json = parsed.placement_policy_json,
    };
    var resp = try client.inner.createTablespace(tablespace_name, body);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |data| try cli.writeJson(allocator, io, data.value);
}

pub fn dropTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const tablespace_name = nextNameArg(args, "tablespace name is required");
    var resp = try client.inner.dropTablespace(tablespace_name);
    defer resp.deinit();
    cli.expectHttpSuccess(resp);
    if (resp.data) |parsed| try cli.writeJson(allocator, io, parsed.value);
}

const CreateArgs = struct {
    tablespace_name: ?[]const u8 = null,
    location_json: ?[]const u8 = null,
    placement_policy_json: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.location_json) |value| alloc.free(value);
        if (self.placement_policy_json) |value| alloc.free(value);
    }
};

fn parseCreateArgs(allocator: std.mem.Allocator, args: *std.process.Args.Iterator) CreateArgs {
    var out = CreateArgs{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--location-json")) {
            out.location_json = allocator.dupe(u8, args.next() orelse cli.argumentError("--location-json requires a value", .{})) catch cli.fatal("out of memory", .{});
        } else if (std.mem.eql(u8, arg, "--placement-policy-json")) {
            out.placement_policy_json = allocator.dupe(u8, args.next() orelse cli.argumentError("--placement-policy-json requires a value", .{})) catch cli.fatal("out of memory", .{});
        } else if (!std.mem.startsWith(u8, arg, "--") and out.tablespace_name == null) {
            out.tablespace_name = arg;
        }
    }
    return out;
}

fn nextNameArg(args: *std.process.Args.Iterator, comptime missing_message: []const u8) []const u8 {
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--tablespace")) {
            return args.next() orelse cli.argumentError("--tablespace requires a value", .{});
        }
        if (!std.mem.startsWith(u8, arg, "--")) return arg;
    }
    cli.argumentError(missing_message, .{});
}

test "tablespace cli module compiles" {
    _ = run;
}
