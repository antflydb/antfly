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
const printResponse = cli.printResponse;

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse return me(allocator, io, client);

    if (std.mem.eql(u8, subcommand, "me")) return me(allocator, io, client);
    if (std.mem.eql(u8, subcommand, "users")) return users(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "permissions")) return permissions(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "roles")) return roles(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "row-filters")) return rowFilters(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "subjects")) return subjects(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "api-keys")) return apiKeys(allocator, io, client, args);

    cli.fatal("unknown auth subcommand: {s}", .{subcommand});
}

fn me(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    var resp = try client.inner.getCurrentUser();
    defer resp.deinit();
    try printResponse(allocator, io, &resp);
}

fn users(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse return listUsers(allocator, io, client);
    if (std.mem.eql(u8, sub, "list")) return listUsers(allocator, io, client);
    if (std.mem.eql(u8, sub, "get")) {
        const username = args.next() orelse cli.fatal("username is required", .{});
        var resp = try client.inner.getUserByName(username);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "create")) return createUser(allocator, io, client, args);
    if (std.mem.eql(u8, sub, "delete")) {
        const username = args.next() orelse cli.fatal("username is required", .{});
        var resp = try client.inner.deleteUser(username);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "password")) {
        const username = args.next() orelse cli.fatal("username is required", .{});
        const password = nextFlagValue(args, "--password") orelse cli.fatal("--password is required", .{});
        var resp = try client.inner.updateUserPassword(username, .{ .new_password = password });
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    cli.fatal("unknown auth users subcommand: {s}", .{sub});
}

fn listUsers(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    var resp = try client.inner.listUsers();
    defer resp.deinit();
    try printResponse(allocator, io, &resp);
}

fn createUser(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const username = args.next() orelse cli.fatal("username is required", .{});
    var password: ?[]const u8 = null;
    var file: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--password")) {
            password = args.next();
        } else if (std.mem.eql(u8, arg, "--file")) {
            file = args.next();
        }
    }
    if (file) |path| {
        var parsed = try parseFile(antfly_client.types.CreateUserRequest, allocator, io, path);
        defer parsed.deinit();
        var resp = try client.inner.createUser(username, parsed.value);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    const pass = password orelse cli.fatal("--password is required unless --file is provided", .{});
    var resp = try client.inner.createUser(username, .{ .username = username, .password = pass });
    defer resp.deinit();
    try printResponse(allocator, io, &resp);
}

fn permissions(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse cli.fatal("permissions subcommand is required", .{});
    const username = args.next() orelse cli.fatal("username is required", .{});
    if (std.mem.eql(u8, sub, "list")) {
        var resp = try client.inner.getUserPermissions(username);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "add")) {
        const perm = parsePermission(args);
        var resp = try client.inner.addPermissionToUser(username, perm);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "remove")) {
        var resource: ?[]const u8 = null;
        var resource_type: ?[]const u8 = null;
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--resource")) resource = args.next() else if (std.mem.eql(u8, arg, "--resource-type")) resource_type = args.next();
        }
        var resp = try client.inner.removePermissionFromUser(username, .{
            .resource = resource orelse cli.fatal("--resource is required", .{}),
            .resource_type = resource_type orelse cli.fatal("--resource-type is required", .{}),
        });
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    cli.fatal("unknown auth permissions subcommand: {s}", .{sub});
}

fn roles(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse cli.fatal("roles subcommand is required", .{});
    const username = args.next() orelse cli.fatal("username is required", .{});
    if (std.mem.eql(u8, sub, "list")) {
        var resp = try client.inner.listUserRoles(username);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    const role = args.next() orelse cli.fatal("role is required", .{});
    if (std.mem.eql(u8, sub, "add")) {
        var resp = try client.inner.addRoleToUser(username, .{ .role = role });
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "remove")) {
        var resp = try client.inner.removeRoleFromUser(username, .{ .role = role });
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    cli.fatal("unknown auth roles subcommand: {s}", .{sub});
}

fn rowFilters(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse cli.fatal("row-filters subcommand is required", .{});
    const username = args.next() orelse cli.fatal("username is required", .{});
    if (std.mem.eql(u8, sub, "list")) {
        var resp = try client.inner.listRowFilters(username);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    const table = args.next() orelse cli.fatal("table is required", .{});
    if (std.mem.eql(u8, sub, "get")) {
        var resp = try client.inner.getRowFilter(username, table);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "set")) {
        const file = nextFlagValue(args, "--file") orelse cli.fatal("--file is required", .{});
        var parsed = try parseFile(std.json.ArrayHashMap(std.json.Value), allocator, io, file);
        defer parsed.deinit();
        var resp = try client.inner.setRowFilter(username, table, parsed.value);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "remove")) {
        var resp = try client.inner.removeRowFilter(username, table);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    cli.fatal("unknown auth row-filters subcommand: {s}", .{sub});
}

fn subjects(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse return listSubjects(allocator, io, client);
    if (std.mem.eql(u8, sub, "list")) return listSubjects(allocator, io, client);
    if (std.mem.eql(u8, sub, "row-filters")) {
        const subject = args.next() orelse cli.fatal("subject is required", .{});
        const action = args.next() orelse "list";
        if (std.mem.eql(u8, action, "list")) {
            var resp = try client.inner.listSubjectRowFilters(subject);
            defer resp.deinit();
            return printResponse(allocator, io, &resp);
        }
        const table = args.next() orelse cli.fatal("table is required", .{});
        if (std.mem.eql(u8, action, "get")) {
            var resp = try client.inner.getSubjectRowFilter(subject, table);
            defer resp.deinit();
            return printResponse(allocator, io, &resp);
        }
        if (std.mem.eql(u8, action, "set")) {
            const file = nextFlagValue(args, "--file") orelse cli.fatal("--file is required", .{});
            var parsed = try parseFile(std.json.ArrayHashMap(std.json.Value), allocator, io, file);
            defer parsed.deinit();
            var resp = try client.inner.setSubjectRowFilter(subject, table, parsed.value);
            defer resp.deinit();
            return printResponse(allocator, io, &resp);
        }
        if (std.mem.eql(u8, action, "remove")) {
            var resp = try client.inner.removeSubjectRowFilter(subject, table);
            defer resp.deinit();
            return printResponse(allocator, io, &resp);
        }
        cli.fatal("unknown subject row-filters action: {s}", .{action});
    }
    cli.fatal("unknown auth subjects subcommand: {s}", .{sub});
}

fn listSubjects(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    var resp = try client.inner.listAuthSubjects();
    defer resp.deinit();
    try printResponse(allocator, io, &resp);
}

fn apiKeys(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse cli.fatal("api-keys subcommand is required", .{});
    const username = args.next() orelse cli.fatal("username is required", .{});
    if (std.mem.eql(u8, sub, "list")) {
        var resp = try client.inner.listApiKeys(username);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "create")) {
        var name: ?[]const u8 = null;
        var expires_in: ?[]const u8 = null;
        var file: ?[]const u8 = null;
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--name")) name = args.next() else if (std.mem.eql(u8, arg, "--expires-in")) expires_in = args.next() else if (std.mem.eql(u8, arg, "--file")) file = args.next();
        }
        if (file) |path| {
            var parsed = try parseFile(antfly_client.types.CreateApiKeyRequest, allocator, io, path);
            defer parsed.deinit();
            var resp = try client.inner.createApiKey(username, parsed.value);
            defer resp.deinit();
            return printResponse(allocator, io, &resp);
        }
        var resp = try client.inner.createApiKey(username, .{ .name = name orelse cli.fatal("--name is required unless --file is provided", .{}), .expires_in = expires_in });
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "delete")) {
        const key_id = args.next() orelse cli.fatal("key id is required", .{});
        var resp = try client.inner.deleteApiKey(username, key_id);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    cli.fatal("unknown auth api-keys subcommand: {s}", .{sub});
}

fn parsePermission(args: *std.process.Args.Iterator) antfly_client.types.Permission {
    var resource: ?[]const u8 = null;
    var resource_type: ?[]const u8 = null;
    var permission_type: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--resource")) resource = args.next() else if (std.mem.eql(u8, arg, "--resource-type")) resource_type = args.next() else if (std.mem.eql(u8, arg, "--type")) permission_type = args.next();
    }
    return .{
        .resource = resource orelse cli.fatal("--resource is required", .{}),
        .resource_type = parseResourceType(resource_type orelse cli.fatal("--resource-type is required", .{})),
        .type = parsePermissionType(permission_type orelse cli.fatal("--type is required", .{})),
    };
}

fn parseResourceType(raw: []const u8) antfly_client.types.ResourceType {
    if (std.mem.eql(u8, raw, "table")) return .table;
    if (std.mem.eql(u8, raw, "user")) return .user;
    if (std.mem.eql(u8, raw, "*")) return .@"*";
    cli.fatal("invalid resource type: {s}", .{raw});
}

fn parsePermissionType(raw: []const u8) antfly_client.types.PermissionType {
    if (std.mem.eql(u8, raw, "read")) return .read;
    if (std.mem.eql(u8, raw, "write")) return .write;
    if (std.mem.eql(u8, raw, "admin")) return .admin;
    cli.fatal("invalid permission type: {s}", .{raw});
}

fn nextFlagValue(args: *std.process.Args.Iterator, flag: []const u8) ?[]const u8 {
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, flag)) return args.next();
    }
    return null;
}

fn parseFile(comptime T: type, allocator: std.mem.Allocator, io: std.Io, path: []const u8) !std.json.Parsed(T) {
    const data = try cli.readFileAlloc(io, allocator, path, 16 * 1024 * 1024);
    defer allocator.free(data);
    return std.json.parseFromSlice(T, allocator, data, .{ .ignore_unknown_fields = true });
}
