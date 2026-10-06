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

//! Credentials for the machine running this CLI, independent of Antfly identity.
const std = @import("std");
const platform = @import("antfly_platform");
const credentials = @import("../../common/cloud_credentials.zig");
const cli = @import("mod.zig");

pub const Provider = enum { google, aws };
const Operation = enum { login, list, logout };
const Options = struct {
    provider: Provider,
    operation: Operation,
    profile: ?[]const u8 = null,
    project: ?[]const u8 = null,
    sso: bool = false,
    no_browser: bool = false,
};

fn parse(args: *std.process.Args.Iterator) !Options {
    const operation = std.meta.stringToEnum(Operation, args.next() orelse return error.MissingConnectionCommand) orelse return error.UnsupportedCloudConnectionCommand;
    const provider = std.meta.stringToEnum(Provider, args.next() orelse return error.MissingConnectionProvider) orelse return error.UnsupportedConnectionProvider;
    var options = Options{ .provider = provider, .operation = operation };
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--profile") and provider == .aws) {
            if (options.profile != null) return error.DuplicateConnectionArgument;
            options.profile = args.next() orelse return error.MissingAwsProfile;
            try credentials.validateProfile(options.profile.?);
        } else if (std.mem.eql(u8, arg, "--project") and provider == .google and operation == .login) {
            if (options.project != null) return error.DuplicateConnectionArgument;
            options.project = args.next() orelse return error.MissingGoogleProject;
            if (options.project.?.len == 0 or options.project.?[0] == '-') return error.InvalidGoogleProject;
            for (options.project.?) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '-' and byte != ':' and byte != '.') return error.InvalidGoogleProject;
        } else if (std.mem.eql(u8, arg, "--sso") and provider == .aws and operation == .login) {
            if (options.sso) return error.DuplicateConnectionArgument;
            options.sso = true;
        } else if (std.mem.eql(u8, arg, "--no-browser") and operation == .login) {
            if (options.no_browser) return error.DuplicateConnectionArgument;
            options.no_browser = true;
        } else return error.UnexpectedConnectionArgument;
    }
    if (provider == .aws and options.profile == null) return error.MissingAwsProfile;
    // Console aws login has no no-browser flag; don't implicitly enable its
    // separate remote credential-transfer flow.
    return options;
}

fn interactive(alloc: std.mem.Allocator, io: std.Io, argv: []const []const u8, provider: Provider) !void {
    var command = credentials.VendorCommand.init(alloc, io, argv) catch |err| switch (err) {
        error.FileNotFound => return if (provider == .aws) error.AwsCliRequired else error.GcloudCliRequired,
        else => return err,
    };
    defer command.deinit();
    // Vendor prompts and success banners remain interactive on stderr while
    // stdout contains only Antfly's JSON result. Keep stdin inherited for codes.
    var child = std.process.spawn(io, .{ .argv = command.argv, .environ_map = &command.environment, .stdout = .{ .file = std.Io.File.stderr() } }) catch |err| switch (err) {
        error.FileNotFound => return if (provider == .aws) error.AwsCliRequired else error.GcloudCliRequired,
        else => return err,
    };
    defer child.kill(io);
    switch (try child.wait(io)) {
        .exited => |code| if (code != 0) return error.CloudConnectionCommandFailed,
        else => return error.CloudConnectionCommandFailed,
    }
}

fn googleList(alloc: std.mem.Allocator, io: std.Io) !void {
    const result = credentials.runVendorCaptured(alloc, io, &.{ "gcloud", "auth", "application-default", "print-access-token", "--quiet" }, 30_000, null) catch |err| switch (err) {
        error.FileNotFound => return error.GcloudCliRequired,
        else => return err,
    };
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);
    switch (result.term) {
        .exited => |code| if (code != 0) return error.GoogleLoginRequired,
        else => return error.GoogleLoginRequired,
    }
    if (std.mem.trim(u8, result.stdout, &std.ascii.whitespace).len == 0) return error.GoogleLoginRequired;
    // The access token is only used to check refresh; never emitted to users.
    try cli.writeJson(alloc, io, .{ .provider = "google", .credential_source = "adc", .status = "available" });
}

pub fn run(alloc: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    const options = try parse(args);
    if (options.provider == .google) {
        if (platform.env.getenv("GOOGLE_APPLICATION_CREDENTIALS") != null) {
            std.debug.print("GOOGLE_APPLICATION_CREDENTIALS overrides local ADC. Unset it to manage gcloud user login; explicit credential files remain supported by provider configuration.\n", .{});
            return error.GoogleAdcOverrideActive;
        }
        switch (options.operation) {
            .login => {
                var argv: std.ArrayList([]const u8) = .empty;
                defer argv.deinit(alloc);
                try argv.appendSlice(alloc, &.{ "gcloud", "auth", "application-default", "login", "--scopes=openid,https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/cloud-platform" });
                if (options.project) |project| try argv.appendSlice(alloc, &.{ "--project", project });
                if (options.no_browser) try argv.append(alloc, "--no-launch-browser");
                std.debug.print("Connecting local Google Application Default Credentials using gcloud.\n", .{});
                try interactive(alloc, io, argv.items, .google);
                try googleList(alloc, io);
            },
            .list => return googleList(alloc, io),
            .logout => {
                std.debug.print("Revoking local Google Application Default Credentials shared with other local applications.\n", .{});
                try interactive(alloc, io, &.{ "gcloud", "auth", "application-default", "revoke", "--quiet" }, .google);
                try cli.writeJson(alloc, io, .{ .provider = "google", .credential_source = "adc", .status = "disconnected" });
            },
        }
    } else {
        const profile = options.profile.?;
        const resolved = try credentials.resolveAwsProfile(alloc, io, profile);
        defer alloc.free(resolved.login_profile);
        const kind = resolved.kind;
        const sso = options.sso or kind == .sso;
        switch (options.operation) {
            .login => {
                if (resolved.requires_export and kind == .none and !options.sso) return error.AwsBrowserLoginNotConfigured;
                if (options.no_browser and !sso) return error.AwsConsoleLoginRequiresBrowser;
                var argv: std.ArrayList([]const u8) = .empty;
                defer argv.deinit(alloc);
                try argv.append(alloc, "aws");
                if (sso) try argv.append(alloc, "sso");
                try argv.appendSlice(alloc, &.{ "login", "--profile", resolved.login_profile, "--no-cli-pager" });
                if (options.no_browser) try argv.append(alloc, "--no-browser");
                std.debug.print("Connecting local AWS profile {s} using {s}.\n", .{ profile, if (sso) "IAM Identity Center" else "console sign-in" });
                try interactive(alloc, io, argv.items, .aws);
                var exported = try credentials.exportAwsCredentials(alloc, io, profile, 30_000, null);
                defer exported.deinit();
                // Login always promises a browser grant, even if the vendor did
                // not persist a config marker (or static keys shadow the grant).
                var readiness = resolved;
                readiness.requires_export = true;
                try credentials.validateAwsProfileExport(exported.value, readiness, io);
                try cli.writeJson(alloc, io, .{ .provider = "aws", .credential_source = "profile", .profile = profile, .status = "available" });
            },
            .list => {
                var exported = try credentials.exportAwsCredentials(alloc, io, profile, 30_000, null);
                defer exported.deinit();
                try credentials.validateAwsProfileExport(exported.value, resolved, io);
                try cli.writeJson(alloc, io, .{ .provider = "aws", .credential_source = "profile", .profile = profile, .status = "available" });
                return;
            },
            .logout => {
                if (sso) {
                    std.debug.print("AWS SSO logout clears every cached SSO session. Use aws sso logout explicitly to sign out of all SSO profiles.\n", .{});
                    return error.AwsSsoLogoutIsGlobal;
                }
                if (kind != .console) {
                    std.debug.print("This AWS profile has no console login session. Manage static credentials and workload identities through their existing credential source.\n", .{});
                    return error.AwsBrowserLoginNotConfigured;
                }
                try interactive(alloc, io, &.{ "aws", "logout", "--profile", resolved.login_profile, "--no-cli-pager" }, .aws);
                try cli.writeJson(alloc, io, .{ .provider = "aws", .credential_source = "profile", .profile = profile, .status = "disconnected" });
            },
        }
    }
    std.debug.print("Restart an already-running Antfly server to use the updated local credentials. Cloud credentials stay on this machine; remote servers are not updated.\n", .{});
}

test "connections cloud login validates provider-specific options before execution" {
    const cases = [_][]const [*:0]const u8{
        &.{ "login", "google", "--project", "my-project", "--no-browser" },
        &.{ "login", "aws", "--profile", "work", "--sso", "--no-browser" },
        &.{ "list", "aws", "--profile", "work" },
        &.{ "logout", "google" },
    };
    for (cases) |argv| {
        var args = std.process.Args.Iterator.init(.{ .vector = argv });
        _ = try parse(&args);
    }
    var missing = std.process.Args.Iterator.init(.{ .vector = &.{ "login", "aws" } });
    try std.testing.expectError(error.MissingAwsProfile, parse(&missing));
    var unsupported = std.process.Args.Iterator.init(.{ .vector = &.{ "login", "google", "--profile", "work" } });
    try std.testing.expectError(error.UnexpectedConnectionArgument, parse(&unsupported));
    var duplicated = std.process.Args.Iterator.init(.{ .vector = &.{ "login", "aws", "--profile", "work", "--profile", "other" } });
    try std.testing.expectError(error.DuplicateConnectionArgument, parse(&duplicated));
    var chatgpt = std.process.Args.Iterator.init(.{ .vector = &.{ "login", "google", "--connection-id", "one" } });
    try std.testing.expectError(error.UnexpectedConnectionArgument, parse(&chatgpt));
}

test {
    _ = credentials;
}
