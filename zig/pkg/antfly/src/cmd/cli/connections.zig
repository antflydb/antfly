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
    \\       antfly connections login google [--project <project>] [--no-browser]
    \\       antfly connections list google
    \\       antfly connections logout google
    \\       antfly connections login aws --profile <name> [--sso] [--no-browser]
    \\       antfly connections list aws --profile <name>
    \\       antfly connections logout aws --profile <name>
    \\  Google/AWS commands manage this machine's shared credentials without a server.
    \\  Install gcloud for Google ADC, or AWS CLI v2 (>=2.32 for console login).
    \\  AWS uses configured SSO automatically; --no-browser requires SSO.
    \\  Restart Antfly after login/logout. Remote credential transfer is not implemented.
    \\
;

/// Dispatch cloud credentials before constructing an Antfly HTTP client.
pub fn runLocalProviderIfRequested(alloc: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !bool {
    var probe = args.*;
    _ = probe.next() orelse return false;
    const provider = probe.next() orelse return false;
    if (!std.mem.eql(u8, provider, "google") and !std.mem.eql(u8, provider, "aws")) return false;
    try cli.cloud_connections.run(alloc, io, args);
    return true;
}

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
    const host = (uri.host orelse return error.LocalConnectionServerRequired).toRaw(&buffer) catch return error.LocalConnectionServerRequired;
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
    if (response.status_code == 403) {
        if (response.err_body) |body| {
            var detail = std.json.parseFromSlice(struct { error_code: []const u8 = "" }, std.heap.page_allocator, body, .{ .ignore_unknown_fields = true }) catch return error.ConnectionOwnerOrLocalServerRequired;
            defer detail.deinit();
            if (std.mem.eql(u8, detail.value.error_code, "ChatGPTDisabled")) return error.ChatGPTDisabled;
        }
        return error.ConnectionOwnerOrLocalServerRequired;
    }
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
    const id = try waitForConnection(allocator, io, client, attempt.attempt_id, deadline, 60_000);
    defer allocator.free(id);
    // Only the safe result is emitted, never the authorization URL or upstream error text.
    try cli.writeJson(allocator, io, .{ .provider = "chatgpt", .status = "connected", .connection_id = id });
}

fn remainingLoginMs(io: std.Io, deadline: i128) !u64 {
    const remaining = deadline - std.Io.Clock.awake.now(io).nanoseconds;
    if (remaining <= 0) return error.ConnectionLoginTimedOut;
    return @intCast(try std.math.divCeil(i128, remaining, std.time.ns_per_ms));
}

fn waitForConnection(allocator: std.mem.Allocator, io: std.Io, client: *AntflyClient, attempt_id: []const u8, deadline: i128, poll_timeout_ms: u64) ![]u8 {
    while (true) {
        const remaining = try remainingLoginMs(io, deadline);
        var outcome = client.getChatGPTAttemptWithTimeout(attempt_id, @min(remaining, poll_timeout_ms)) catch |err| {
            // This GET only observes an existing attempt. A timeout must not
            // discard a valid OAuth exchange or start another authorization.
            if (err == error.Timeout) continue;
            return err;
        };
        defer outcome.deinit();
        _ = try remainingLoginMs(io, deadline);
        try requireData(outcome);
        const value = outcome.data.?.value;
        if (try completed(value.status)) {
            const id = value.connection_id orelse return error.InvalidConnectionResponse;
            return allocator.dupe(u8, id);
        }
        try io.sleep(.fromMilliseconds(@intCast(@min(1000, try remainingLoginMs(io, deadline)))), .awake);
    }
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

test "connections polling retries only status reads within the overall login budget" {
    const httpx = @import("httpx");
    const Mock = struct {
        const Mode = enum(u8) { slow_first, delayed, declined };
        mode: std.atomic.Value(Mode) = .init(.slow_first),
        polls: std.atomic.Value(u32) = .init(0),
        authorizations: std.atomic.Value(u32) = .init(0),
        fn status(self: *@This(), ctx: *httpx.Context) !httpx.Response {
            try std.testing.expectEqualStrings("Basic dXNlcjpwYXNz", ctx.header("Authorization").?);
            const count = self.polls.fetchAdd(1, .acq_rel);
            const mode = self.mode.load(.acquire);
            if (mode == .delayed or (mode == .slow_first and count == 0)) try ctx.io.sleep(.fromMilliseconds(200), .awake);
            return ctx.json(.{ .status = if (mode == .declined) "declined" else "connected", .connection_id = "one" });
        }
        fn authorize(self: *@This(), ctx: *httpx.Context) !httpx.Response {
            _ = self.authorizations.fetchAdd(1, .acq_rel);
            return ctx.status(400).text("unexpected authorization replay");
        }
        fn serve(server: *httpx.Server) std.Io.Cancelable!void {
            server.listen() catch {};
        }
    };
    const a = std.testing.allocator;
    const io = std.testing.io;
    var mock: Mock = .{};
    var server = httpx.Server.initWithConfig(a, io, .{ .host = "127.0.0.1", .port = 0 });
    defer server.deinit();
    try server.get("/db/v1/connections/chatgpt/attempts/attempt-1", httpx.Handler.bind(&mock, Mock.status));
    try server.post("/db/v1/connections/chatgpt/authorize", httpx.Handler.bind(&mock, Mock.authorize));
    try server.bind();
    const origin = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}", .{server.boundAddress().?.getPort()});
    defer a.free(origin);
    var group: std.Io.Group = .init;
    try group.concurrent(io, Mock.serve, .{&server});
    defer {
        server.stop();
        group.cancel(io);
    }
    var http = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
    defer http.deinit();
    var client = try AntflyClient.init(a, &http, origin);
    defer client.deinit();
    try client.setBasicAuth("user", "pass");
    const deadline = std.Io.Clock.awake.now(io).nanoseconds + 2 * std.time.ns_per_s;
    const id = try waitForConnection(a, io, &client, "attempt-1", deadline, 50);
    defer a.free(id);
    try std.testing.expectEqualStrings("one", id);
    try std.testing.expect(mock.polls.load(.acquire) >= 2);
    mock.mode.store(.delayed, .release);
    const short_deadline = std.Io.Clock.awake.now(io).nanoseconds + 50 * std.time.ns_per_ms;
    try std.testing.expectError(error.ConnectionLoginTimedOut, waitForConnection(a, io, &client, "attempt-1", short_deadline, 1_000));
    mock.mode.store(.declined, .release);
    const declined_deadline = std.Io.Clock.awake.now(io).nanoseconds + std.time.ns_per_s;
    const before = mock.polls.load(.acquire);
    try std.testing.expectError(error.ConnectionLoginDeclined, waitForConnection(a, io, &client, "attempt-1", declined_deadline, 500));
    try std.testing.expectEqual(before + 1, mock.polls.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), mock.authorizations.load(.acquire));
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

test "connections recognize disabled connector errors before browser login" {
    const Response = struct { status_code: u16, err_body: ?[]const u8, data: ?u8 = null };
    try std.testing.expectError(error.ChatGPTDisabled, requireData(Response{ .status_code = 403, .err_body = "{\"error_code\":\"ChatGPTDisabled\"}" }));
    try std.testing.expectError(error.ConnectionOwnerOrLocalServerRequired, requireData(Response{ .status_code = 403, .err_body = "forbidden" }));
}
