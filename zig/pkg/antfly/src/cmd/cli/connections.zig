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
const builtin = @import("builtin");
const cli = @import("mod.zig");
const AntflyClient = @import("antfly-client").AntflyClient;

pub const usage =
    \\usage: antfly connections login chatgpt [--connection-id <id>] [--no-browser] [--timeout <seconds>]
    \\       antfly connections list chatgpt
    \\       antfly connections models chatgpt <connection-id>
    \\       antfly connections logout chatgpt <connection-id>
    \\  Start a local standalone server first. ANTFLY_URL defaults to http://127.0.0.1:8080.
    \\  Authenticated servers: set ANTFLY_USERNAME and ANTFLY_PASSWORD (unset ANTFLY_TOKEN).
    \\  Login opens a local browser; --no-browser prints the URL for manual opening.
    \\  Credentials stay in the server's private store. JSON results go to stdout.
    \\  Logout clears the local grant and attempts upstream revocation.
    \\  Remote credential transfer and additional providers are not yet implemented.
    \\
;

const Operation = enum { login, list, models, logout };
const Options = struct {
    operation: Operation,
    connection_id: ?[]const u8 = null,
    no_browser: bool = false,
    timeout_seconds: u32 = 600,
};

fn parse(args: *std.process.Args.Iterator) !Options {
    const operation = std.meta.stringToEnum(Operation, args.next() orelse return error.MissingConnectionCommand) orelse return error.UnknownConnectionCommand;
    // Providers own their authentication method and scopes; never infer a provider from an ID.
    const provider = args.next() orelse return error.MissingConnectionProvider;
    if (!std.mem.eql(u8, provider, "chatgpt")) return error.UnsupportedConnectionProvider;
    var options: Options = .{ .operation = operation };
    if (operation == .models or operation == .logout) options.connection_id = args.next() orelse return error.MissingConnectionId;
    var timeout_seen = false;
    while (args.next()) |arg| {
        if (operation != .login) return error.UnexpectedConnectionArgument;
        if (std.mem.eql(u8, arg, "--no-browser")) {
            if (options.no_browser) return error.DuplicateConnectionArgument;
            options.no_browser = true;
        } else if (std.mem.eql(u8, arg, "--connection-id")) {
            if (options.connection_id != null) return error.DuplicateConnectionArgument;
            options.connection_id = args.next() orelse return error.MissingConnectionId;
        } else if (std.mem.eql(u8, arg, "--timeout")) {
            if (timeout_seen) return error.DuplicateConnectionArgument;
            timeout_seen = true;
            options.timeout_seconds = std.fmt.parseInt(u32, args.next() orelse return error.InvalidLoginTimeout, 10) catch return error.InvalidLoginTimeout;
            if (options.timeout_seconds == 0 or options.timeout_seconds > 600) return error.InvalidLoginTimeout;
        } else return error.UnexpectedConnectionArgument;
    }
    return options;
}

fn validateLocalServer(url: []const u8) !void {
    const uri = std.Uri.parse(url) catch return error.LocalConnectionServerRequired;
    if (!std.mem.eql(u8, uri.scheme, "http") or uri.user != null or uri.password != null or uri.query != null or uri.fragment != null) return error.LocalConnectionServerRequired;
    var buffer: [std.Io.net.HostName.max_len]u8 = undefined;
    const host = (uri.getHost(&buffer) catch return error.LocalConnectionServerRequired).bytes;
    if (!std.mem.eql(u8, host, "127.0.0.1") and !std.mem.eql(u8, host, "localhost") and !std.mem.eql(u8, host, "[::1]")) return error.LocalConnectionServerRequired;
}

fn validateAuthorizationUrl(url: []const u8) !void {
    // Exact authority and path; reject controls before passing URL to an OS opener.
    if (url.len > 16 << 10 or !std.mem.startsWith(u8, url, "https://auth.openai.com/api/accounts/authorize?")) return error.InvalidAuthorizationUrl;
    for (url) |c| if (c <= 32 or c == 127) return error.InvalidAuthorizationUrl;
    const uri = std.Uri.parse(url) catch return error.InvalidAuthorizationUrl;
    if (uri.fragment != null) return error.InvalidAuthorizationUrl;
}

fn openBrowser(io: std.Io, url: []const u8) !void {
    const argv: []const []const u8 = switch (builtin.os.tag) {
        .macos => &.{ "/usr/bin/open", url },
        .linux => &.{ "xdg-open", url },
        .windows => &.{ "rundll32.exe", "url.dll,FileProtocolHandler", url },
        else => return error.BrowserOpenerUnavailable,
    };
    var child = try std.process.spawn(io, .{ .argv = argv, .stdin = .ignore, .stdout = .ignore, .stderr = .ignore });
    const term = try child.wait(io);
    switch (term) {
        .exited => |code| if (code != 0) return error.BrowserOpenerFailed,
        else => return error.BrowserOpenerFailed,
    }
}

fn requireData(response: anytype) !void {
    if (response.status_code == 401) return error.ConnectionAuthenticationRequired;
    if (response.status_code == 403) return error.ConnectionOwnerOrLocalServerRequired;
    if (response.status_code == 404) return error.ConnectionNotFound;
    if (response.status_code < 200 or response.status_code >= 300) return error.ConnectionRequestFailed;
    if (response.data == null) return error.InvalidConnectionResponse;
}

fn completed(status: []const u8) !bool {
    if (std.mem.eql(u8, status, "connected")) return true;
    if (std.mem.eql(u8, status, "pending") or std.mem.eql(u8, status, "exchanging")) return false;
    if (std.mem.eql(u8, status, "declined")) return error.ConnectionLoginDeclined;
    if (std.mem.eql(u8, status, "expired")) return error.ConnectionLoginExpired;
    if (std.mem.eql(u8, status, "error")) return error.ConnectionLoginFailed;
    return error.InvalidConnectionResponse;
}

fn login(allocator: std.mem.Allocator, io: std.Io, client: *AntflyClient, options: Options) !void {
    var begin = try client.authorizeChatGPT(.{ .connection_id = options.connection_id });
    defer begin.deinit();
    try requireData(begin);
    const attempt = begin.data.?.value;
    try validateAuthorizationUrl(attempt.authorization_url);
    std.debug.print("Continue with ChatGPT in your local browser:\n{s}\n", .{attempt.authorization_url});
    if (!options.no_browser) openBrowser(io, attempt.authorization_url) catch {
        std.debug.print("Open the URL above manually; waiting for authorization.\n", .{});
    };
    const deadline = std.Io.Clock.awake.now(io).nanoseconds + @as(i128, options.timeout_seconds) * std.time.ns_per_s;
    while (std.Io.Clock.awake.now(io).nanoseconds < deadline) {
        var outcome = try client.getChatGPTAttempt(attempt.attempt_id);
        defer outcome.deinit();
        try requireData(outcome);
        const value = outcome.data.?.value;
        if (try completed(value.status)) {
            const id = value.connection_id orelse return error.InvalidConnectionResponse;
            // Only the safe result is emitted, never the authorization URL or upstream error text.
            return cli.writeJson(allocator, io, .{ .provider = "chatgpt", .status = "connected", .connection_id = id });
        }
        try io.sleep(.fromSeconds(1), .awake);
    }
    return error.ConnectionLoginTimedOut;
}

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *AntflyClient, args: *std.process.Args.Iterator) !void {
    const options = try parse(args);
    try validateLocalServer(client.inner.base_url);
    switch (options.operation) {
        .login => try login(allocator, io, client, options),
        .list => {
            var response = try client.listChatGPTAccounts();
            defer response.deinit();
            try requireData(response);
            try cli.writeJson(allocator, io, response.data.?.value);
        },
        .models => {
            var response = try client.listChatGPTModels(options.connection_id.?);
            defer response.deinit();
            try requireData(response);
            try cli.writeJson(allocator, io, response.data.?.value);
        },
        .logout => {
            var response = try client.disconnectChatGPT(options.connection_id.?);
            defer response.deinit();
            try requireData(response);
            try cli.writeJson(allocator, io, response.data.?.value);
        },
    }
}

test "connections reject remote servers and ambiguous authorization URLs" {
    try validateLocalServer("http://127.0.0.1:8080");
    try validateLocalServer("http://localhost:8080");
    try validateLocalServer("http://[::1]:8080");
    for ([_][]const u8{ "https://example.com", "http://127.0.0.1.evil.test", "http://user@localhost", "http://0.0.0.0:8080", "http://localhost?redirect=evil" }) |url| try std.testing.expectError(error.LocalConnectionServerRequired, validateLocalServer(url));
    try validateAuthorizationUrl("https://auth.openai.com/api/accounts/authorize?state=test");
    for ([_][]const u8{ "https://auth.openai.com.evil.test/api/accounts/authorize?x=y", "https://auth.openai.com/api/accounts/authorize?x=y#fragment", "https://auth.openai.com/api/accounts/authorize?x=\n" }) |url| try std.testing.expectError(error.InvalidAuthorizationUrl, validateAuthorizationUrl(url));
}

test "connections parsing keeps provider and operation explicit" {
    var argv = [_][*:0]const u8{ "login", "chatgpt", "--no-browser", "--connection-id", "account-1", "--timeout", "12" };
    var args = std.process.Args.Iterator.init(.{ .vector = &argv });
    const options = try parse(&args);
    try std.testing.expect(options.no_browser);
    try std.testing.expectEqual(@as(u32, 12), options.timeout_seconds);
    try std.testing.expectEqualStrings("account-1", options.connection_id.?);
    var unsupported = [_][*:0]const u8{ "login", "google" };
    args = std.process.Args.Iterator.init(.{ .vector = &unsupported });
    try std.testing.expectError(error.UnsupportedConnectionProvider, parse(&args));
    var duplicate = [_][*:0]const u8{ "login", "chatgpt", "--timeout", "1", "--timeout", "2" };
    args = std.process.Args.Iterator.init(.{ .vector = &duplicate });
    try std.testing.expectError(error.DuplicateConnectionArgument, parse(&args));
}

test "connections require a terminal successful authorization" {
    try std.testing.expect(!try completed("pending"));
    try std.testing.expect(!try completed("exchanging"));
    try std.testing.expect(try completed("connected"));
    try std.testing.expectError(error.ConnectionLoginDeclined, completed("declined"));
    try std.testing.expectError(error.ConnectionLoginExpired, completed("expired"));
    try std.testing.expectError(error.ConnectionLoginFailed, completed("error"));
    try std.testing.expectError(error.InvalidConnectionResponse, completed("unknown"));
}
