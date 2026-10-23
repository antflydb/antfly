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
const printResponse = cli.printResponse;

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse return me(allocator, io, client);

    if (std.mem.eql(u8, subcommand, "me")) {
        cli.rejectRemainingArgs(args, "auth me");
        return me(allocator, io, client);
    }
    if (std.mem.eql(u8, subcommand, "users")) return users(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "permissions")) return permissions(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "roles")) return roles(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "row-filters")) return rowFilters(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "subjects")) return subjects(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "api-keys")) return apiKeys(allocator, io, client, args);

    cli.argumentError("unknown auth subcommand: {s}", .{subcommand});
}

fn me(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    var resp = try client.inner.getCurrentUser();
    defer resp.deinit();
    try printResponse(allocator, io, &resp);
}

fn users(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse return listUsers(allocator, io, client);
    if (std.mem.eql(u8, sub, "list")) {
        cli.rejectRemainingArgs(args, "auth users list");
        return listUsers(allocator, io, client);
    }
    if (std.mem.eql(u8, sub, "get")) {
        const username = args.next() orelse cli.argumentError("username is required", .{});
        cli.rejectRemainingArgs(args, "auth users get");
        var resp = try client.inner.getUserByName(username);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "create")) return createUser(allocator, io, client, args);
    if (std.mem.eql(u8, sub, "delete")) {
        const username = args.next() orelse cli.argumentError("username is required", .{});
        cli.rejectRemainingArgs(args, "auth users delete");
        var resp = try client.inner.deleteUser(username);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "password")) {
        const username = args.next() orelse cli.argumentError("username is required", .{});
        var password_value: ?[]const u8 = null;
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--password")) {
                cli.takeUniqueValue(args, &password_value, arg);
            } else {
                cli.argumentError("unknown auth users password option: {s}", .{arg});
            }
        }
        const password = password_value orelse cli.argumentError("--password is required", .{});
        var resp = try client.inner.updateUserPassword(username, .{ .new_password = password });
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    cli.argumentError("unknown auth users subcommand: {s}", .{sub});
}

pub fn listUsers(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    var resp = try client.inner.listUsers();
    defer resp.deinit();
    try printResponse(allocator, io, &resp);
}

pub fn createUser(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const username = args.next() orelse cli.argumentError("username is required", .{});
    var password: ?[]const u8 = null;
    var file: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--password")) {
            cli.takeUniqueValue(args, &password, arg);
        } else if (std.mem.eql(u8, arg, "--file")) {
            cli.takeUniqueValue(args, &file, arg);
        } else {
            cli.argumentError("unknown auth users create option: {s}", .{arg});
        }
    }
    if (file) |path| {
        if (password != null) cli.argumentError("use only one of --password or --file", .{});
        var parsed = try parseFile(antfly_client.types.CreateUserRequest, allocator, io, path);
        defer parsed.deinit();
        var resp = try client.inner.createUser(username, parsed.value);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    const pass = password orelse cli.argumentError("--password is required unless --file is provided", .{});
    var resp = try client.inner.createUser(username, .{ .username = username, .password = pass });
    defer resp.deinit();
    try printResponse(allocator, io, &resp);
}

fn permissions(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse cli.argumentError("permissions subcommand is required", .{});
    const username = args.next() orelse cli.argumentError("username is required", .{});
    if (std.mem.eql(u8, sub, "list")) {
        cli.rejectRemainingArgs(args, "auth permissions list");
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
            if (std.mem.eql(u8, arg, "--resource")) {
                cli.takeUniqueValue(args, &resource, arg);
            } else if (std.mem.eql(u8, arg, "--resource-type")) {
                cli.takeUniqueValue(args, &resource_type, arg);
            } else {
                cli.argumentError("unknown auth permissions remove option: {s}", .{arg});
            }
        }
        var resp = try client.inner.removePermissionFromUser(username, .{
            .resource = resource orelse cli.argumentError("--resource is required", .{}),
            .resource_type = resource_type orelse cli.argumentError("--resource-type is required", .{}),
        });
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    cli.argumentError("unknown auth permissions subcommand: {s}", .{sub});
}

fn roles(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse cli.argumentError("roles subcommand is required", .{});
    const username = args.next() orelse cli.argumentError("username is required", .{});
    if (std.mem.eql(u8, sub, "list")) {
        cli.rejectRemainingArgs(args, "auth roles list");
        var resp = try client.inner.listUserRoles(username);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    const role = args.next() orelse cli.argumentError("role is required", .{});
    if (std.mem.eql(u8, sub, "add")) {
        cli.rejectRemainingArgs(args, "auth roles add");
        var resp = try client.inner.addRoleToUser(username, .{ .role = role });
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "remove")) {
        cli.rejectRemainingArgs(args, "auth roles remove");
        var resp = try client.inner.removeRoleFromUser(username, .{ .role = role });
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    cli.argumentError("unknown auth roles subcommand: {s}", .{sub});
}

fn rowFilters(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse cli.argumentError("row-filters subcommand is required", .{});
    const username = args.next() orelse cli.argumentError("username is required", .{});
    if (std.mem.eql(u8, sub, "list")) {
        cli.rejectRemainingArgs(args, "auth row-filters list");
        var resp = try client.inner.listRowFilters(username);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    const table = args.next() orelse cli.argumentError("table is required", .{});
    if (std.mem.eql(u8, sub, "get")) {
        cli.rejectRemainingArgs(args, "auth row-filters get");
        var resp = try client.inner.getRowFilter(username, table, .{});
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "set")) {
        var file_value: ?[]const u8 = null;
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--file")) {
                cli.takeUniqueValue(args, &file_value, arg);
            } else {
                cli.argumentError("unknown auth row-filters set option: {s}", .{arg});
            }
        }
        const file = file_value orelse cli.argumentError("--file is required", .{});
        var parsed = try parseFile(std.json.ArrayHashMap(std.json.Value), allocator, io, file);
        defer parsed.deinit();
        var resp = try client.inner.setRowFilter(username, table, parsed.value, .{});
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "remove")) {
        cli.rejectRemainingArgs(args, "auth row-filters remove");
        var resp = try client.inner.removeRowFilter(username, table, .{});
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    cli.argumentError("unknown auth row-filters subcommand: {s}", .{sub});
}

fn subjects(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse return listSubjects(allocator, io, client);
    if (std.mem.eql(u8, sub, "list")) {
        cli.rejectRemainingArgs(args, "auth subjects list");
        return listSubjects(allocator, io, client);
    }
    if (std.mem.eql(u8, sub, "row-filters")) {
        const subject = args.next() orelse cli.argumentError("subject is required", .{});
        const action = args.next() orelse "list";
        if (std.mem.eql(u8, action, "list")) {
            cli.rejectRemainingArgs(args, "auth subjects row-filters list");
            var resp = try client.inner.listSubjectRowFilters(subject);
            defer resp.deinit();
            return printResponse(allocator, io, &resp);
        }
        const table = args.next() orelse cli.argumentError("table is required", .{});
        if (std.mem.eql(u8, action, "get")) {
            cli.rejectRemainingArgs(args, "auth subjects row-filters get");
            var resp = try client.inner.getSubjectRowFilter(subject, table, .{});
            defer resp.deinit();
            return printResponse(allocator, io, &resp);
        }
        if (std.mem.eql(u8, action, "set")) {
            var file_value: ?[]const u8 = null;
            while (args.next()) |arg| {
                if (std.mem.eql(u8, arg, "--file")) {
                    cli.takeUniqueValue(args, &file_value, arg);
                } else {
                    cli.argumentError("unknown auth subjects row-filters set option: {s}", .{arg});
                }
            }
            const file = file_value orelse cli.argumentError("--file is required", .{});
            var parsed = try parseFile(std.json.ArrayHashMap(std.json.Value), allocator, io, file);
            defer parsed.deinit();
            var resp = try client.inner.setSubjectRowFilter(subject, table, parsed.value, .{});
            defer resp.deinit();
            return printResponse(allocator, io, &resp);
        }
        if (std.mem.eql(u8, action, "remove")) {
            cli.rejectRemainingArgs(args, "auth subjects row-filters remove");
            var resp = try client.inner.removeSubjectRowFilter(subject, table, .{});
            defer resp.deinit();
            return printResponse(allocator, io, &resp);
        }
        cli.argumentError("unknown subject row-filters action: {s}", .{action});
    }
    cli.argumentError("unknown auth subjects subcommand: {s}", .{sub});
}

fn listSubjects(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    var resp = try client.inner.listAuthSubjects();
    defer resp.deinit();
    try printResponse(allocator, io, &resp);
}

fn apiKeys(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const sub = args.next() orelse cli.argumentError("api-keys subcommand is required", .{});
    const username = args.next() orelse cli.argumentError("username is required", .{});
    if (std.mem.eql(u8, sub, "list")) {
        cli.rejectRemainingArgs(args, "auth api-keys list");
        var resp = try client.inner.listApiKeys(username);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "create")) {
        var name: ?[]const u8 = null;
        var expires_in: ?[]const u8 = null;
        var file: ?[]const u8 = null;
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--name")) {
                cli.takeUniqueValue(args, &name, arg);
            } else if (std.mem.eql(u8, arg, "--expires-in")) {
                cli.takeUniqueValue(args, &expires_in, arg);
            } else if (std.mem.eql(u8, arg, "--file")) {
                cli.takeUniqueValue(args, &file, arg);
            } else {
                cli.argumentError("unknown auth api-keys create option: {s}", .{arg});
            }
        }
        if (file) |path| {
            if (name != null or expires_in != null) cli.argumentError("--file cannot be combined with --name or --expires-in", .{});
            var parsed = try parseFile(antfly_client.types.CreateApiKeyRequest, allocator, io, path);
            defer parsed.deinit();
            var resp = try client.inner.createApiKey(username, parsed.value);
            defer resp.deinit();
            return printResponse(allocator, io, &resp);
        }
        var resp = try client.inner.createApiKey(username, .{ .name = name orelse cli.argumentError("--name is required unless --file is provided", .{}), .expires_in = expires_in });
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    if (std.mem.eql(u8, sub, "delete")) {
        const key_id = args.next() orelse cli.argumentError("key id is required", .{});
        cli.rejectRemainingArgs(args, "auth api-keys delete");
        var resp = try client.inner.deleteApiKey(username, key_id);
        defer resp.deinit();
        return printResponse(allocator, io, &resp);
    }
    cli.argumentError("unknown auth api-keys subcommand: {s}", .{sub});
}

fn parsePermission(args: *std.process.Args.Iterator) antfly_client.types.Permission {
    var resource: ?[]const u8 = null;
    var resource_type: ?[]const u8 = null;
    var permission_type: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--resource")) {
            cli.takeUniqueValue(args, &resource, arg);
        } else if (std.mem.eql(u8, arg, "--resource-type")) {
            cli.takeUniqueValue(args, &resource_type, arg);
        } else if (std.mem.eql(u8, arg, "--type")) {
            cli.takeUniqueValue(args, &permission_type, arg);
        } else {
            cli.argumentError("unknown auth permission option: {s}", .{arg});
        }
    }
    return .{
        .resource = resource orelse cli.argumentError("--resource is required", .{}),
        .resource_type = parseResourceType(resource_type orelse cli.argumentError("--resource-type is required", .{})),
        .type = parsePermissionType(permission_type orelse cli.argumentError("--type is required", .{})),
    };
}

fn parseResourceType(raw: []const u8) antfly_client.types.ResourceType {
    if (std.mem.eql(u8, raw, "table")) return .table;
    if (std.mem.eql(u8, raw, "database")) return .database;
    if (std.mem.eql(u8, raw, "namespace")) return .namespace;
    if (std.mem.eql(u8, raw, "tablespace")) return .tablespace;
    if (std.mem.eql(u8, raw, "user")) return .user;
    if (std.mem.eql(u8, raw, "inference")) return .inference;
    if (std.mem.eql(u8, raw, "*")) return .@"*";
    cli.argumentError("invalid resource type: {s}", .{raw});
}

fn parsePermissionType(raw: []const u8) antfly_client.types.PermissionType {
    if (std.mem.eql(u8, raw, "read")) return .read;
    if (std.mem.eql(u8, raw, "write")) return .write;
    if (std.mem.eql(u8, raw, "admin")) return .admin;
    cli.argumentError("invalid permission type: {s}", .{raw});
}

fn parseFile(comptime T: type, allocator: std.mem.Allocator, io: std.Io, path: []const u8) !std.json.Parsed(T) {
    const data = try cli.readFileAlloc(io, allocator, path, 16 * 1024 * 1024);
    defer allocator.free(data);
    return std.json.parseFromSlice(T, allocator, data, .{ .ignore_unknown_fields = false });
}
