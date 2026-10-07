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

//! Local vendor credential sources. Never run shell commands or expose exports.
const std = @import("std");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const platform = @import("antfly_platform");

pub const AwsLoginKind = enum { none, console, sso };

pub fn validateProfile(profile: []const u8) !void {
    if (profile.len == 0 or profile.len > 256 or profile[0] == '-') return error.InvalidAwsProfile;
    for (profile) |byte| if (byte < 32 or byte == 127) return error.InvalidAwsProfile;
}

/// login_profile names the browser session to authenticate; exports always use
/// the originally selected profile so AWS performs every role assumption.
pub const AwsProfile = struct {
    kind: AwsLoginKind,
    login_profile: []const u8,
    requires_export: bool,
};

pub fn awsProfileFromConfig(data: []const u8, profile: []const u8) !AwsProfile {
    try validateProfile(profile);
    var current = profile;
    var visited: [32][]const u8 = undefined;
    var count: usize = 0;
    var requires_export = false;
    while (true) {
        for (visited[0..count]) |previous| if (std.mem.eql(u8, previous, current)) return error.InvalidAwsProfileChain;
        if (count == visited.len) return error.InvalidAwsProfileChain;
        visited[count] = current;
        count += 1;
        var selected = false;
        var found = false;
        var kind: AwsLoginKind = .none;
        var source: ?[]const u8 = null;
        var role = false;
        var lines = std.mem.splitScalar(u8, data, '\n');
        while (lines.next()) |raw| {
            const line = std.mem.trim(u8, raw, " \t\r");
            if (line.len == 0 or line[0] == '#' or line[0] == ';') continue;
            if (line[0] == '[' and line[line.len - 1] == ']') {
                const section = std.mem.trim(u8, line[1 .. line.len - 1], " \t");
                selected = if (std.mem.startsWith(u8, section, "profile "))
                    std.mem.eql(u8, std.mem.trim(u8, section[8..], " \t"), current)
                else
                    std.mem.eql(u8, section, "default") and std.mem.eql(u8, current, "default");
                if (selected) {
                    if (found) return error.DuplicateAwsProfile;
                    found = true;
                }
                continue;
            }
            if (!selected) continue;
            const eq = std.mem.indexOfScalar(u8, line, '=') orelse continue;
            const key = std.mem.trim(u8, line[0..eq], " \t");
            const value = std.mem.trim(u8, line[eq + 1 ..], " \t");
            if (std.mem.eql(u8, key, "role_arn")) {
                if (role or value.len == 0) return error.InvalidAwsProfile;
                role = true;
            } else if (std.mem.eql(u8, key, "source_profile")) {
                if (source != null) return error.InvalidAwsProfile;
                try validateProfile(value);
                source = value;
            } else {
                const next: AwsLoginKind = if (std.mem.eql(u8, key, "login_session")) .console else if (std.mem.eql(u8, key, "sso_session") or std.mem.eql(u8, key, "sso_start_url")) .sso else continue;
                if (value.len == 0 or (kind != .none and kind != next)) return error.InvalidAwsProfile;
                kind = next;
            }
        }
        requires_export = requires_export or role or kind != .none;
        if (source) |parent| {
            if (!role or kind != .none) return error.InvalidAwsProfile;
            // AWS allows a role to source static keys from its own profile.
            // Delegate that case to AWS, which validates the source credentials.
            if (std.mem.eql(u8, parent, current))
                return .{ .kind = .none, .login_profile = current, .requires_export = true };
            current = parent;
            continue;
        }
        return .{ .kind = kind, .login_profile = current, .requires_export = requires_export };
    }
}

pub fn awsLoginKindFromConfig(data: []const u8, profile: []const u8) !AwsLoginKind {
    return (try awsProfileFromConfig(data, profile)).kind;
}

/// The returned login_profile is owned by the caller.
pub fn resolveAwsProfile(alloc: std.mem.Allocator, io: std.Io, profile: []const u8) !AwsProfile {
    try validateProfile(profile);
    const path = if (platform.env.getenv("AWS_CONFIG_FILE")) |value| try alloc.dupe(u8, value) else blk: {
        const home = platform.env.getenv("HOME") orelse return .{ .kind = .none, .login_profile = try alloc.dupe(u8, profile), .requires_export = false };
        break :blk try std.fs.path.join(alloc, &.{ home, ".aws", "config" });
    };
    defer alloc.free(path);
    const data = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(1 << 20)) catch |err| switch (err) {
        error.FileNotFound => return .{ .kind = .none, .login_profile = try alloc.dupe(u8, profile), .requires_export = false },
        else => return err,
    };
    defer alloc.free(data);
    var resolved = try awsProfileFromConfig(data, profile);
    resolved.login_profile = try alloc.dupe(u8, resolved.login_profile);
    return resolved;
}

pub const AwsExport = struct {
    Version: u32,
    AccessKeyId: []const u8,
    SecretAccessKey: []const u8,
    SessionToken: ?[]const u8 = null,
    Expiration: ?[]const u8 = null,
};

/// AWS CLI uses both Z and +00:00 UTC timestamps. Reject other offsets,
/// invalid calendar dates, trailing data and nonnumeric fractions.
pub fn parseExpiration(value: []const u8) !u64 {
    if (value.len < 20 or value[4] != '-' or value[7] != '-' or value[10] != 'T' or value[13] != ':' or value[16] != ':') return error.InvalidAwsCredentialExpiration;
    const year = std.fmt.parseUnsigned(u16, value[0..4], 10) catch return error.InvalidAwsCredentialExpiration;
    const month = std.fmt.parseUnsigned(u8, value[5..7], 10) catch return error.InvalidAwsCredentialExpiration;
    const day = std.fmt.parseUnsigned(u8, value[8..10], 10) catch return error.InvalidAwsCredentialExpiration;
    const hour = std.fmt.parseUnsigned(u8, value[11..13], 10) catch return error.InvalidAwsCredentialExpiration;
    const minute = std.fmt.parseUnsigned(u8, value[14..16], 10) catch return error.InvalidAwsCredentialExpiration;
    const second = std.fmt.parseUnsigned(u8, value[17..19], 10) catch return error.InvalidAwsCredentialExpiration;
    const suffix = if (std.mem.endsWith(u8, value, "Z")) value.len - 1 else if (std.mem.endsWith(u8, value, "+00:00")) value.len - 6 else return error.InvalidAwsCredentialExpiration;
    if (suffix != 19) {
        if (suffix < 21 or value[19] != '.') return error.InvalidAwsCredentialExpiration;
        for (value[20..suffix]) |byte| if (!std.ascii.isDigit(byte)) return error.InvalidAwsCredentialExpiration;
    }
    if (year < 1970 or month < 1 or month > 12 or hour > 23 or minute > 59 or second > 59) return error.InvalidAwsCredentialExpiration;
    const leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0);
    const days_in_month = [_]u8{ 31, if (leap) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (day < 1 or day > days_in_month[month - 1]) return error.InvalidAwsCredentialExpiration;
    var y: i64 = year;
    if (month <= 2) y -= 1;
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const shifted_month: i64 = if (month > 2) month - 3 else @as(i64, month) + 9;
    const doy = @divFloor(153 * shifted_month + 2, 5) + day - 1;
    const days = era * 146097 + yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy - 719468;
    return @as(u64, @intCast(days)) * std.time.s_per_day + @as(u64, hour) * std.time.s_per_hour + @as(u64, minute) * std.time.s_per_min + second;
}

pub fn parseAwsExport(alloc: std.mem.Allocator, raw: []const u8) !std.json.Parsed(AwsExport) {
    var parsed = std.json.parseFromSlice(AwsExport, alloc, raw, .{ .ignore_unknown_fields = true, .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidAwsCredentialExport,
    };
    errdefer parsed.deinit();
    if (parsed.value.Version != 1 or parsed.value.AccessKeyId.len == 0 or parsed.value.SecretAccessKey.len == 0) return error.InvalidAwsCredentialExport;
    // Shared-file session tokens have no expiration metadata. Browser/role
    // readiness applies the stricter temporary-grant contract separately.
    if (parsed.value.SessionToken) |token| if (token.len == 0) return error.InvalidAwsCredentialExport;
    if (parsed.value.Expiration) |expiration| _ = try parseExpiration(expiration);
    return parsed;
}

/// Browser and assumed-role profiles must resolve to temporary credentials.
/// Shared by CLI readiness checks and the runtime to prevent false success.
pub fn validateTemporaryAwsExport(value: AwsExport, now: u64) !u64 {
    const expiration = try parseExpiration(value.Expiration orelse return error.InvalidAwsCredentialExport);
    if (expiration <= now) return error.AwsLoginRequired;
    const token = value.SessionToken orelse return error.InvalidAwsCredentialExport;
    if (token.len == 0) return error.InvalidAwsCredentialExport;
    return expiration;
}

pub fn validateAwsProfileExport(value: AwsExport, profile: AwsProfile, io: std.Io) !void {
    if (profile.requires_export) {
        const now = std.Io.Timestamp.now(io, .real).toSeconds();
        if (now < 0) return error.AwsLoginRequired;
        _ = try validateTemporaryAwsExport(value, @intCast(now));
    }
}

/// Some borrowed Io runtimes deliberately have an empty startup environment.
/// Vendor tools must receive the process credential environment explicitly,
/// while all filesystem/process operations still use the supplied authority.
pub const VendorCommand = struct {
    alloc: std.mem.Allocator,
    environment: std.process.Environ.Map,
    argv: [][]const u8,
    executable: []u8,

    pub fn init(alloc: std.mem.Allocator, io: std.Io, argv: []const []const u8) !VendorCommand {
        const builtin = @import("builtin");
        const environ: std.process.Environ = switch (builtin.os.tag) {
            .windows => .{ .block = .global },
            .freestanding, .wasi => return error.CloudLoginUnsupported,
            else => if (builtin.link_libc)
                .{ .block = .{ .slice = @ptrCast(std.mem.span(std.c.environ)) } }
            else
                return error.CloudLoginUnsupported,
        };
        var environment = try environ.createMap(alloc);
        errdefer environment.deinit();
        const executable = try findExecutable(alloc, io, environment.get("PATH") orelse return error.FileNotFound, argv[0]);
        errdefer alloc.free(executable);
        const owned_argv = try alloc.dupe([]const u8, argv);
        owned_argv[0] = executable;
        return .{ .alloc = alloc, .environment = environment, .argv = owned_argv, .executable = executable };
    }

    pub fn deinit(self: *VendorCommand) void {
        self.environment.deinit();
        self.alloc.free(self.argv);
        self.alloc.free(self.executable);
    }
};

fn findExecutable(alloc: std.mem.Allocator, io: std.Io, path: []const u8, name: []const u8) ![]u8 {
    const separator: u8 = if (@import("builtin").os.tag == .windows) ';' else ':';
    var directories = std.mem.splitScalar(u8, path, separator);
    while (directories.next()) |directory| {
        // Do not search implicit current-directory entries for a credential tool.
        if (directory.len == 0) continue;
        const candidate = try std.fs.path.join(alloc, &.{ directory, name });
        std.Io.Dir.cwd().access(io, candidate, .{ .execute = true }) catch |err| {
            alloc.free(candidate);
            switch (err) {
                error.FileNotFound, error.AccessDenied, error.PermissionDenied => continue,
                else => return err,
            }
        };
        return candidate;
    }
    return error.FileNotFound;
}

pub fn runVendorCaptured(alloc: std.mem.Allocator, io: std.Io, argv: []const []const u8, timeout_ms: u64, cancellation: ?CancellationToken) !std.process.RunResult {
    var command = try VendorCommand.init(alloc, io, argv);
    defer command.deinit();
    return runCaptured(alloc, io, .{
        .argv = command.argv,
        .environ_map = &command.environment,
        .stdout_limit = .limited(64 << 10),
        .stderr_limit = .limited(64 << 10),
    }, timeout_ms, cancellation);
}

/// Bound the entire subprocess, including waiting after its output pipes close.
/// Cancellation kills and reaps the child before freeing its captured output.
pub fn runCaptured(alloc: std.mem.Allocator, io: std.Io, options: std.process.RunOptions, timeout_ms: u64, cancellation: ?CancellationToken) !std.process.RunResult {
    const Task = struct {
        done: std.Io.Event = .unset,
        result: ?std.process.RunResult = null,
        failure: ?anyerror = null,

        fn execute(self: *@This(), a: std.mem.Allocator, task_io: std.Io, opts: std.process.RunOptions) void {
            defer self.done.set(task_io);
            self.result = std.process.run(a, task_io, opts) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
    if (timeout_ms == 0) return error.Timeout;
    const deadline = std.Io.Timestamp.now(io, .awake).toNanoseconds() + @as(i96, timeout_ms) * std.time.ns_per_ms;
    var task: Task = .{};
    var future = try io.concurrent(Task.execute, .{ &task, alloc, io, options });
    var transferred = false;
    defer {
        future.cancel(io);
        if (!transferred) if (task.result) |result| {
            alloc.free(result.stdout);
            alloc.free(result.stderr);
        };
    }
    while (true) {
        if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        const remaining = deadline - std.Io.Timestamp.now(io, .awake).toNanoseconds();
        if (remaining <= 0) return error.Timeout;
        task.done.waitTimeout(io, .{ .duration = .{ .raw = .fromNanoseconds(@min(remaining, 50 * std.time.ns_per_ms)), .clock = .awake } }) catch |err| switch (err) {
            error.Timeout => continue,
            else => return err,
        };
        future.await(io);
        if (task.failure) |failure| return failure;
        transferred = true;
        return task.result.?;
    }
}

pub fn exportAwsCredentials(alloc: std.mem.Allocator, io: std.Io, profile: []const u8, timeout_ms: u64, cancellation: ?CancellationToken) !std.json.Parsed(AwsExport) {
    try validateProfile(profile);
    const result = runVendorCaptured(alloc, io, &.{ "aws", "configure", "export-credentials", "--profile", profile, "--format", "process", "--no-cli-pager", "--no-cli-auto-prompt" }, timeout_ms, cancellation) catch |err| switch (err) {
        error.FileNotFound => return error.AwsCliRequired,
        else => return err,
    };
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);
    switch (result.term) {
        .exited => |code| if (code != 0) return error.AwsLoginRequired,
        else => return error.AwsLoginRequired,
    }
    // In particular, never print stderr: vendor diagnostics may contain tokens.
    var parsed = try parseAwsExport(alloc, result.stdout);
    errdefer parsed.deinit();
    if (parsed.value.Expiration) |expiration| {
        const now = std.Io.Timestamp.now(io, .real).toSeconds();
        if (now < 0 or try parseExpiration(expiration) <= @as(u64, @intCast(now))) return error.AwsLoginRequired;
    }
    return parsed;
}

test "cloud credentials select only the requested AWS login profile" {
    const raw = "[profile work]\nsso_session = company\n[profile personal]\nlogin_session = arn:aws:signin:session\n[sso-session company]\nsso_start_url = https://example.awsapps.com\n[default]\nregion = us-east-1\n";
    try std.testing.expectEqual(AwsLoginKind.sso, try awsLoginKindFromConfig(raw, "work"));
    try std.testing.expectEqual(AwsLoginKind.console, try awsLoginKindFromConfig(raw, "personal"));
    try std.testing.expectEqual(AwsLoginKind.none, try awsLoginKindFromConfig(raw, "default"));
    try std.testing.expectEqual(AwsLoginKind.none, try awsLoginKindFromConfig(raw, "company"));
    try std.testing.expectError(error.InvalidAwsProfile, awsLoginKindFromConfig("[profile work]\nsso_session=x\nlogin_session=y", "work"));
    try std.testing.expectError(error.DuplicateAwsProfile, awsLoginKindFromConfig("[profile work]\n[profile work]", "work"));
    try std.testing.expectError(error.InvalidAwsProfile, validateProfile("--debug"));
}

test "cloud credentials resolve role chains and reject cycles" {
    const config = "[profile work]\nrole_arn=arn:role/work\nsource_profile=middle\n[profile middle]\nrole_arn=arn:role/middle\nsource_profile=company\n[profile company]\nsso_session=company\n";
    const resolved = try awsProfileFromConfig(config, "work");
    try std.testing.expectEqual(AwsLoginKind.sso, resolved.kind);
    try std.testing.expectEqualStrings("company", resolved.login_profile);
    try std.testing.expect(resolved.requires_export);
    const static_role = try awsProfileFromConfig("[profile work]\nrole_arn=arn:role/work\nsource_profile=static\n", "work");
    try std.testing.expectEqual(AwsLoginKind.none, static_role.kind);
    try std.testing.expect(static_role.requires_export);
    const self_role = try awsProfileFromConfig("[profile work]\nrole_arn=arn:role/work\nsource_profile=work\n", "work");
    try std.testing.expect(self_role.requires_export);
    try std.testing.expectEqual(AwsLoginKind.none, self_role.kind);
    try std.testing.expectError(error.InvalidAwsProfileChain, awsProfileFromConfig("[profile work]\nrole_arn=arn:role/work\nsource_profile=other\n[profile other]\nrole_arn=arn:role/other\nsource_profile=work\n", "work"));
    try std.testing.expectError(error.InvalidAwsProfile, awsProfileFromConfig("[profile work]\nrole_arn=arn:role/work\nsource_profile=other\nsource_profile=third\n", "work"));
}

test "cloud credentials require temporary grants for browser and role readiness" {
    var value: AwsExport = .{ .Version = 1, .AccessKeyId = "access", .SecretAccessKey = "secret" };
    try std.testing.expectError(error.InvalidAwsCredentialExport, validateTemporaryAwsExport(value, 0));
    value.Expiration = "2030-01-01T00:00:00Z";
    try std.testing.expectError(error.InvalidAwsCredentialExport, validateTemporaryAwsExport(value, 0));
    value.SessionToken = "";
    try std.testing.expectError(error.InvalidAwsCredentialExport, validateTemporaryAwsExport(value, 0));
    value.SessionToken = "session";
    const expiration = try validateTemporaryAwsExport(value, 0);
    try std.testing.expectError(error.AwsLoginRequired, validateTemporaryAwsExport(value, expiration));
}

test "cloud credentials reject malformed AWS exports without leaking secrets" {
    const a = std.testing.allocator;
    var valid = try parseAwsExport(a, "{\"Version\":1,\"AccessKeyId\":\"access\",\"SecretAccessKey\":\"secret\",\"SessionToken\":\"session\",\"Expiration\":\"2030-01-01T00:00:00Z\"}");
    defer valid.deinit();
    try std.testing.expectEqualStrings("session", valid.value.SessionToken.?);
    for ([_][]const u8{ "{}", "{\"Version\":2,\"AccessKeyId\":\"a\",\"SecretAccessKey\":\"s\"}", "{\"Version\":1,\"AccessKeyId\":\"\",\"SecretAccessKey\":\"s\"}", "{\"Version\":1,\"AccessKeyId\":\"a\",\"SecretAccessKey\":\"s\",\"SessionToken\":\"\"}" }) |raw| try std.testing.expectError(error.InvalidAwsCredentialExport, parseAwsExport(a, raw));
}

test "cloud credentials accept shared-file session tokens without expiration" {
    const a = std.testing.allocator;
    var parsed = try parseAwsExport(a, "{\"Version\":1,\"AccessKeyId\":\"access\",\"SecretAccessKey\":\"secret\",\"SessionToken\":\"session\"}");
    defer parsed.deinit();
    try std.testing.expectEqualStrings("session", parsed.value.SessionToken.?);
    try std.testing.expect(parsed.value.Expiration == null);
    try std.testing.expectError(error.InvalidAwsCredentialExport, validateTemporaryAwsExport(parsed.value, 0));
}

test "cloud credentials validate AWS UTC expiration" {
    const z = try parseExpiration("2030-01-02T03:04:05Z");
    try std.testing.expectEqual(z, try parseExpiration("2030-01-02T03:04:05+00:00"));
    try std.testing.expectEqual(z, try parseExpiration("2030-01-02T03:04:05.123+00:00"));
    for ([_][]const u8{ "2030-02-29T00:00:00Z", "2030-01-02T03:04:05Zbad", "2030-01-02T03:04:05.badZ", "2030-01-02T03:04:05+02:00" }) |bad| try std.testing.expectError(error.InvalidAwsCredentialExpiration, parseExpiration(bad));
}

test "cloud credentials bound subprocess exit and honor cancellation" {
    if (@import("builtin").os.tag == .windows) return error.SkipZigTest;
    var threaded = platform.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const options: std.process.RunOptions = .{ .argv = &.{ "/bin/sh", "-c", "exec /bin/sleep 3 >&- 2>&-" }, .stdout_limit = .limited(1024), .stderr_limit = .limited(1024) };
    const started = std.Io.Timestamp.now(io, .awake).toNanoseconds();
    try std.testing.expectError(error.Timeout, runCaptured(std.testing.allocator, io, options, 100, null));
    try std.testing.expect(std.Io.Timestamp.now(io, .awake).toNanoseconds() - started < std.time.ns_per_s);
    var signal = std.atomic.Value(bool).init(false);
    const Cancel = struct {
        fn execute(task_io: std.Io, flag: *std.atomic.Value(bool)) void {
            task_io.sleep(.fromMilliseconds(50), .awake) catch return;
            flag.store(true, .release);
        }
    };
    var canceller = try io.concurrent(Cancel.execute, .{ io, &signal });
    defer canceller.cancel(io);
    try std.testing.expectError(error.Cancelled, runCaptured(std.testing.allocator, io, options, 3_000, CancellationToken.fromAtomic(&signal)));
}
