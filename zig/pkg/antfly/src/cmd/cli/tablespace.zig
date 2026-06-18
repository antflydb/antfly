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
const antfly_client = @import("antfly-client");
const cli = @import("mod.zig");

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse return listTablespaces(allocator, io, client);

    if (std.mem.eql(u8, subcommand, "list")) return listTablespaces(allocator, io, client);
    if (std.mem.eql(u8, subcommand, "get")) return getTablespace(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "create")) return createTablespace(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "drop")) return dropTablespace(allocator, io, client, args);

    cli.fatal("unknown tablespace subcommand: {s}", .{subcommand});
}

fn listTablespaces(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    var resp = try client.inner.listTablespaces();
    defer resp.deinit();
    if (resp.data) |parsed| try cli.writeJson(allocator, io, parsed.value);
}

fn getTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const tablespace_name = nextNameArg(args, "tablespace name is required");
    var resp = try client.inner.getTablespace(tablespace_name);
    defer resp.deinit();
    if (resp.data) |parsed| try cli.writeJson(allocator, io, parsed.value);
}

fn createTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var parsed = parseCreateArgs(allocator, args);
    defer parsed.deinit(allocator);
    const tablespace_name = parsed.tablespace_name orelse cli.fatal("tablespace name is required", .{});
    const body = antfly_client.types.CreateTablespaceRequest{
        .location_json = parsed.location_json,
        .placement_policy_json = parsed.placement_policy_json,
    };
    var resp = try client.inner.createTablespace(tablespace_name, body);
    defer resp.deinit();
    if (resp.data) |data| try cli.writeJson(allocator, io, data.value);
}

fn dropTablespace(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const tablespace_name = nextNameArg(args, "tablespace name is required");
    var resp = try client.inner.dropTablespace(tablespace_name);
    defer resp.deinit();
    if (resp.data) |parsed| try cli.writeJson(allocator, io, parsed.value);
}

const CreateArgs = struct {
    tablespace_name: ?[]const u8 = null,
    location_json: ?[]const u8 = null,
    placement_policy_json: ?[]const u8 = null,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.location_json) |value| alloc.free(value);
        if (self.placement_policy_json) |value| alloc.free(value);
    }
};

fn parseCreateArgs(allocator: std.mem.Allocator, args: *std.process.Args.Iterator) CreateArgs {
    var out = CreateArgs{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--location-json")) {
            out.location_json = allocator.dupe(u8, args.next() orelse cli.fatal("--location-json requires a value", .{})) catch cli.fatal("out of memory", .{});
        } else if (std.mem.eql(u8, arg, "--placement-policy-json")) {
            out.placement_policy_json = allocator.dupe(u8, args.next() orelse cli.fatal("--placement-policy-json requires a value", .{})) catch cli.fatal("out of memory", .{});
        } else if (!std.mem.startsWith(u8, arg, "--") and out.tablespace_name == null) {
            out.tablespace_name = arg;
        }
    }
    return out;
}

fn nextNameArg(args: *std.process.Args.Iterator, comptime missing_message: []const u8) []const u8 {
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--tablespace")) {
            return args.next() orelse cli.fatal("--tablespace requires a value", .{});
        }
        if (!std.mem.startsWith(u8, arg, "--")) return arg;
    }
    cli.fatal(missing_message, .{});
}

test "tablespace cli module compiles" {
    _ = run;
}
