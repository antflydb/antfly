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

//! Node-local personal OAuth registrations, intentionally outside database metadata.
const std = @import("std");
const httpx = @import("httpx");
const paths = @import("antfly_runtime_fs").fs_paths;
const protocol = @import("protocol.zig");
const RequestContext = @import("antfly_inference_execution_context").RequestContext;

pub const Summary = struct { connection_id: []const u8, email: []const u8, label: []const u8, connected: bool, plan_enabled: bool };
pub const Begin = struct { attempt_id: []const u8, authorization_url: []const u8, expires_at: i64 };
pub const Outcome = struct { status: Status, connection_id: ?[]const u8 = null, @"error": ?[]const u8 = null };
pub const Status = enum { pending, exchanging, connected, declined, expired, @"error" };
const Record = struct {
    connection_id: []const u8,
    owner: []const u8,
    subject: []const u8,
    email: []const u8,
    client_id: []const u8,
    access_token: []const u8,
    refresh_token: []const u8,
    id_token: []const u8,
    scope: []const u8,
    expires_at: i64,
};
const State = struct { version: u32 = 2, host_id: []const u8, accounts: []Record = &.{} };
// Every successful save must remain readable, including after a restart.
const max_store_bytes = 1024 * 1024;
const Token = struct { access_token: []const u8, refresh_token: ?[]const u8 = null, id_token: ?[]const u8 = null, scope: ?[]const u8 = null, expires_in: i64, token_type: []const u8 };
const Session = struct { id: []const u8, epoch: std.atomic.Value(u64) = .init(0) };
pub const Lease = struct {
    alloc: std.mem.Allocator,
    access_token: []u8,
    session: *Session,
    epoch: u64,
    pub fn deinit(self: *Lease) void {
        std.crypto.secureZero(u8, self.access_token);
        self.alloc.free(self.access_token);
    }
    pub fn cancelled(self: *const Lease) bool {
        return self.session.epoch.load(.acquire) != self.epoch;
    }
};
const Attempt = struct {
    manager: *Manager,
    arena: std.heap.ArenaAllocator,
    server: httpx.Server,
    group: std.Io.Group = .init,
    owner: []const u8,
    id: []const u8,
    nonce: []const u8,
    verifier: []const u8,
    callback_uri: []const u8,
    selected_id: ?[]const u8,
    selected_pin: ?Pin = null,
    client_id: []const u8,
    expires_at: i64,
    outcome: Outcome = .{ .status = .pending },
    fn serve(self: *Attempt) std.Io.Cancelable!void {
        self.server.listen() catch {};
    }
    fn expire(self: *Attempt) std.Io.Cancelable!void {
        try self.manager.io.sleep(.fromSeconds(600), .awake);
        self.manager.mutex.lockUncancelable(self.manager.io);
        if (self.outcome.status == .pending) self.outcome.status = .expired;
        self.manager.mutex.unlock(self.manager.io);
        self.server.stop();
    }
    fn callback(self: *Attempt, ctx: *httpx.Context) !httpx.Response {
        // Code/state remain local to this one-time loopback listener. No tokens
        // are returned to the browser, nor are callback query strings logged.
        const state = ctx.query("state") orelse return ctx.status(400).text("Missing sign-in state.");
        self.manager.mutex.lockUncancelable(self.manager.io);
        defer self.manager.mutex.unlock(self.manager.io);
        if (!std.mem.eql(u8, state, self.id) or self.outcome.status != .pending or now(self.manager.io) >= self.expires_at)
            return ctx.status(400).text("Invalid or expired sign-in attempt.");
        self.outcome.status = .exchanging;
        self.complete(ctx) catch {
            self.outcome = .{ .status = .@"error", .@"error" = "ChatGPT authorization could not be verified. Start a new sign-in." };
        };
        std.crypto.secureZero(u8, @constCast(self.verifier));
        self.server.shutdown(1000);
        return ctx.text(if (self.outcome.status == .connected) "ChatGPT connection saved. Return to Antfly." else "ChatGPT connection was not enabled. Return to Antfly.");
    }
    fn complete(self: *Attempt, ctx: *httpx.Context) !void {
        try self.manager.checkOwner(self.owner);
        if (self.selected_pin) |pin| try pin.check(self.selected_id.?);
        if (ctx.query("error")) |err| {
            self.outcome = .{ .status = if (std.mem.eql(u8, err, "access_denied")) .declined else .@"error" };
            return;
        }
        const code = ctx.query("code") orelse return error.InvalidCallback;
        const client_id = ctx.query("client_id") orelse if (self.selected_id != null) self.client_id else return error.InvalidCallback;
        if (std.mem.eql(u8, client_id, "dynamic_agent_client") or client_id.len == 0) return error.InvalidCallback;
        if (self.selected_id != null and !std.mem.eql(u8, client_id, self.client_id)) return error.InvalidCallback;
        const a = self.arena.allocator();
        const body = try protocol.form(a, &.{ .{ "grant_type", "authorization_code" }, .{ "client_id", client_id }, .{ "code", code }, .{ "code_verifier", self.verifier }, .{ "redirect_uri", self.callback_uri }, .{ "resource", protocol.resource } });
        var http = self.manager.client(a);
        defer http.deinit();
        defer std.crypto.secureZero(u8, body);
        var token = try tokenRequest(a, &http, try self.manager.authUrl(a, "/api/accounts/oauth/token"), body);
        defer {
            scrubToken(token.value);
            token.deinit();
        }
        const discovery = try getJson(a, &http, try self.manager.authUrl(a, "/.well-known/openid-configuration"));
        const Discovery = struct { issuer: []const u8, jwks_uri: []const u8 };
        const endpoints = try std.json.parseFromSliceLeaky(Discovery, a, discovery, .{ .ignore_unknown_fields = true });
        if (!std.mem.eql(u8, endpoints.issuer, protocol.issuer) or !self.manager.trustedAuth(endpoints.jwks_uri)) return error.InvalidIdentity;
        const jwks = try getJson(a, &http, endpoints.jwks_uri);
        var identity = try protocol.verify(a, token.value.id_token orelse return error.InvalidIdentity, jwks, client_id, self.nonce, now(self.manager.io));
        defer identity.deinit();
        var saved = try self.manager.load(a);
        defer {
            scrub(saved.value);
            saved.deinit();
        }
        var existing: ?*Record = null;
        if (self.selected_id) |id| {
            existing = try find(&saved.value, self.owner, id);
            if (!std.mem.eql(u8, existing.?.subject, identity.value.sub) or !std.mem.eql(u8, existing.?.client_id, client_id)) return error.InvalidIdentity;
        } else {
            for (saved.value.accounts) |*account| if (std.mem.eql(u8, account.owner, self.owner) and std.mem.eql(u8, account.client_id, client_id)) {
                existing = account;
                break;
            };
        }
        if (existing) |record| if (!std.mem.eql(u8, record.subject, identity.value.sub)) return error.InvalidIdentity;
        if (self.selected_pin) |pin| try pin.check(self.selected_id.?);
        const id = if (existing) |record| record.connection_id else try self.manager.random(a);
        const record: Record = .{ .connection_id = id, .owner = self.owner, .subject = identity.value.sub, .email = identity.value.email, .client_id = client_id, .access_token = token.value.access_token, .refresh_token = token.value.refresh_token orelse "", .id_token = token.value.id_token.?, .scope = token.value.scope orelse "", .expires_at = now(self.manager.io) + token.value.expires_in };
        if (existing) |old| {
            scrubRecord(old.*);
            old.* = record;
        } else {
            if (saved.value.accounts.len >= 128) return error.CapacityExhausted;
            const accounts = try a.alloc(Record, saved.value.accounts.len + 1);
            @memcpy(accounts[0..saved.value.accounts.len], saved.value.accounts);
            accounts[accounts.len - 1] = record;
            saved.value.accounts = accounts;
        }
        try self.manager.save(saved.value);
        const session = try self.manager.session(id);
        _ = session.epoch.fetchAdd(1, .acq_rel);
        self.outcome = .{ .status = .connected, .connection_id = try a.dupe(u8, id) };
    }
};

pub const Manager = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    root: []const u8,
    path: []const u8,
    mutex: std.Io.Mutex = .init,
    process_lock: std.Io.File,
    attempts: std.ArrayList(*Attempt) = .empty,
    sessions: std.ArrayList(*Session) = .empty,
    revoked_owners: std.StringHashMapUnmanaged(void) = .{},
    // Transport override exists only in test builds. Production origins are pinned
    // and cannot be supplied through configuration or a management request.
    test_auth_origin: if (@import("builtin").is_test) ?[]const u8 else void = if (@import("builtin").is_test) null else {},
    fn authUrl(self: *Manager, a: std.mem.Allocator, suffix: []const u8) ![]u8 {
        const origin = if (@import("builtin").is_test) self.test_auth_origin orelse protocol.issuer else protocol.issuer;
        return std.fmt.allocPrint(a, "{s}{s}", .{ origin, suffix });
    }
    fn trustedAuth(self: *Manager, url: []const u8) bool {
        if (trustedAuthUrl(url)) return true;
        if (@import("builtin").is_test) {
            if (self.test_auth_origin) |origin| return std.mem.startsWith(u8, url, origin) and url.len > origin.len and url[origin.len] == '/' and std.mem.indexOfAny(u8, url, "?#") == null;
        }
        return false;
    }
    pub fn init(alloc: std.mem.Allocator, io: std.Io, root: []const u8) !Manager {
        const owned = try alloc.dupe(u8, root);
        errdefer alloc.free(owned);
        const path = try std.fs.path.join(alloc, &.{ root, "accounts.json" });
        errdefer alloc.free(path);
        try paths.createDirPathPortable(io, root);
        // Permissions require a normal descriptor: Linux O_PATH handles cannot
        // be passed to fchmod. Zig uses O_PATH when iteration is disabled.
        var dir = try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true });
        defer dir.close(io);
        if (@import("builtin").os.tag != .windows) try dir.setPermissions(io, @fromBackingInt(0o700));
        const lock_path = try std.fs.path.join(alloc, &.{ root, "session.lock" });
        defer alloc.free(lock_path);
        var lock_file = try paths.createFilePortable(io, lock_path, .{ .truncate = false, .permissions = @fromBackingInt(0o600) });
        errdefer lock_file.close(io);
        if (!try lock_file.tryLock(io, .exclusive)) return error.ChatGPTStoreInUse;
        var manager: Manager = .{ .alloc = alloc, .io = io, .root = owned, .path = path, .process_lock = lock_file };
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var saved = try manager.load(arena.allocator());
        defer {
            scrub(saved.value);
            saved.deinit();
        }
        // Username-based grants cannot be safely attributed to a user creation.
        // Retain the host identity, but require explicit consent under v2 owners.
        if (saved.value.version == 1) {
            scrub(saved.value);
            saved.value.accounts = &.{};
            saved.value.version = 2;
            try manager.save(saved.value);
        }
        return manager;
    }
    pub fn deinit(self: *Manager) void {
        for (self.sessions.items) |session_ptr| _ = session_ptr.epoch.fetchAdd(1, .acq_rel);
        for (self.attempts.items) |attempt| {
            attempt.server.stop();
            attempt.group.cancel(self.io);
            attempt.server.deinit();
            attempt.arena.deinit();
            self.alloc.destroy(attempt);
        }
        for (self.sessions.items) |s| {
            self.alloc.free(s.id);
            self.alloc.destroy(s);
        }
        self.sessions.deinit(self.alloc);
        var revoked = self.revoked_owners.keyIterator();
        while (revoked.next()) |owner| self.alloc.free(owner.*);
        self.revoked_owners.deinit(self.alloc);
        self.attempts.deinit(self.alloc);
        self.process_lock.unlock(self.io);
        self.process_lock.close(self.io);
        self.alloc.free(self.path);
        self.alloc.free(self.root);
    }
    fn random(self: *Manager, a: std.mem.Allocator) ![]u8 {
        var bytes: [32]u8 = undefined;
        try self.io.randomSecure(&bytes);
        return protocol.base64(a, &bytes);
    }
    fn client(self: *Manager, a: std.mem.Allocator) httpx.Client {
        return httpx.Client.initWithConfig(a, self.io, .{ .keep_alive = false, .max_response_size = 1024 * 1024, .retry_policy = .{ .max_retries = 0 } });
    }
    fn load(self: *Manager, a: std.mem.Allocator) !std.json.Parsed(State) {
        if (@import("builtin").os.tag != .windows) {
            const stat = std.Io.Dir.cwd().statFile(self.io, self.path, .{}) catch |err| switch (err) {
                error.FileNotFound => null,
                else => return err,
            };
            if (stat) |info| if (info.kind != .file or info.permissions.toMode() & 0o077 != 0) return error.InsecureCredentialStore;
        }
        const bytes = std.Io.Dir.cwd().readFileAlloc(self.io, self.path, a, .limited(max_store_bytes)) catch |err| switch (err) {
            error.FileNotFound => {
                var uuid: [16]u8 = undefined;
                try self.io.randomSecure(&uuid);
                uuid[6] = (uuid[6] & 15) | 64;
                uuid[8] = (uuid[8] & 63) | 128;
                const hex = std.fmt.bytesToHex(uuid, .lower);
                const host_id = try std.fmt.allocPrint(a, "urn:uuid:{s}-{s}-{s}-{s}-{s}", .{ hex[0..8], hex[8..12], hex[12..16], hex[16..20], hex[20..32] });
                try self.save(.{ .host_id = host_id });
                return self.load(a);
            },
            else => return err,
        };
        defer {
            std.crypto.secureZero(u8, bytes);
            a.free(bytes);
        }
        var result = try std.json.parseFromSlice(State, a, bytes, .{ .allocate = .alloc_always });
        errdefer {
            scrub(result.value);
            result.deinit();
        }
        if ((result.value.version != 1 and result.value.version != 2) or !std.mem.startsWith(u8, result.value.host_id, "urn:uuid:") or result.value.accounts.len > 128) return error.InvalidCredentialStore;
        for (result.value.accounts, 0..) |account, i| {
            if (account.connection_id.len == 0 or account.owner.len == 0 or account.subject.len == 0 or account.client_id.len == 0 or std.mem.eql(u8, account.client_id, "dynamic_agent_client")) return error.InvalidCredentialStore;
            for (result.value.accounts[0..i]) |previous| if (std.mem.eql(u8, previous.connection_id, account.connection_id)) return error.InvalidCredentialStore;
        }
        return result;
    }
    fn save(self: *Manager, state: State) !void {
        const bytes = try std.json.Stringify.valueAlloc(self.alloc, state, .{});
        defer {
            std.crypto.secureZero(u8, bytes);
            self.alloc.free(bytes);
        }
        // Reject before creating a temporary file or replacing durable grants.
        if (bytes.len > max_store_bytes) return error.CapacityExhausted;
        const suffix = try self.random(self.alloc);
        defer self.alloc.free(suffix);
        const temp = try std.fmt.allocPrint(self.alloc, "{s}.{s}.tmp", .{ self.path, suffix });
        defer self.alloc.free(temp);
        var file = try paths.createFilePortable(self.io, temp, .{ .exclusive = true, .permissions = @fromBackingInt(0o600) });
        defer file.close(self.io);
        defer std.Io.Dir.cwd().deleteFile(self.io, temp) catch {};
        var buffer: [4096]u8 = undefined;
        var writer = file.writer(self.io, &buffer);
        try writer.interface.writeAll(bytes);
        try writer.end();
        try file.sync(self.io);
        try std.Io.Dir.rename(std.Io.Dir.cwd(), temp, std.Io.Dir.cwd(), self.path, self.io);
        try paths.syncDirPortable(self.io, self.root);
    }
    fn session(self: *Manager, id: []const u8) !*Session {
        for (self.sessions.items) |s| if (std.mem.eql(u8, s.id, id)) return s;
        const s = try self.alloc.create(Session);
        errdefer self.alloc.destroy(s);
        s.* = .{ .id = try self.alloc.dupe(u8, id) };
        errdefer self.alloc.free(s.id);
        try self.sessions.append(self.alloc, s);
        return s;
    }
    fn reap(self: *Manager) void {
        while (true) {
            self.mutex.lockUncancelable(self.io);
            var expired: ?*Attempt = null;
            for (self.attempts.items, 0..) |attempt, i| {
                if (attempt.expires_at <= now(self.io) and attempt.outcome.status != .exchanging) {
                    expired = self.attempts.swapRemove(i);
                    break;
                }
            }
            self.mutex.unlock(self.io);
            const attempt = expired orelse break;
            attempt.server.stop();
            attempt.group.cancel(self.io);
            attempt.server.deinit();
            std.crypto.secureZero(u8, @constCast(attempt.verifier));
            attempt.arena.deinit();
            self.alloc.destroy(attempt);
        }
    }
    pub fn begin(self: *Manager, a: std.mem.Allocator, owner: []const u8, selected: ?[]const u8) !Begin {
        self.reap();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        try self.checkOwner(owner);
        if (self.attempts.items.len >= 64) return error.CapacityExhausted;
        const attempt = try self.alloc.create(Attempt);
        errdefer self.alloc.destroy(attempt);
        attempt.* = .{ .manager = self, .arena = std.heap.ArenaAllocator.init(self.alloc), .server = httpx.Server.initWithConfig(self.alloc, self.io, .{ .host = "127.0.0.1", .port = 0, .max_connections = 2, .max_request_tasks = 2 }), .owner = "", .id = "", .nonce = "", .verifier = "", .callback_uri = "", .selected_id = null, .client_id = "dynamic_agent_client", .expires_at = now(self.io) + 600 };
        errdefer {
            attempt.server.deinit();
            attempt.arena.deinit();
        }
        const local = attempt.arena.allocator();
        attempt.owner = try local.dupe(u8, owner);
        attempt.id = try self.random(local);
        attempt.nonce = try self.random(local);
        attempt.verifier = try self.random(local);
        var saved = try self.load(local);
        defer {
            scrub(saved.value);
            saved.deinit();
        }
        if (selected) |id| {
            const rec = try find(&saved.value, owner, id);
            attempt.selected_id = try local.dupe(u8, id);
            attempt.client_id = try local.dupe(u8, rec.client_id);
            const current = try self.session(id);
            attempt.selected_pin = .{ .session = current, .epoch = current.epoch.load(.acquire) };
        }
        try attempt.server.get("/auth/callback", httpx.Handler.bind(attempt, Attempt.callback));
        try attempt.server.bind();
        attempt.callback_uri = try std.fmt.allocPrint(local, "http://127.0.0.1:{d}/auth/callback", .{attempt.server.boundAddress().?.getPort()});
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(attempt.verifier, &digest, .{});
        const challenge = try protocol.base64(local, &digest);
        var pairs: std.ArrayList([2][]const u8) = .empty;
        try pairs.appendSlice(local, &.{ .{ "client_id", attempt.client_id }, .{ "ext_agent_host_id", saved.value.host_id }, .{ "response_type", "code" }, .{ "redirect_uri", attempt.callback_uri }, .{ "scope", protocol.scopes }, .{ "resource", protocol.resource }, .{ "state", attempt.id }, .{ "nonce", attempt.nonce }, .{ "code_challenge_method", "S256" }, .{ "code_challenge", challenge } });
        if (selected == null) try pairs.append(local, .{ "agent_name_hint", "Antfly" });
        if (selected) |id| {
            const rec = try find(&saved.value, owner, id);
            if (rec.email.len > 0) try pairs.append(local, .{ "login_hint", rec.email });
            if (!protocol.hasScope(rec.scope, "chatgpt.tokens.use.direct")) try pairs.append(local, .{ "prompt", "consent" });
        }
        const query = try protocol.form(local, pairs.items);
        const url = try std.fmt.allocPrint(a, protocol.issuer ++ "/api/accounts/authorize?{s}", .{query});
        errdefer a.free(url);
        // Finish all fallible return-value allocation before tasks retain the
        // attempt. Error cleanup below may destroy it only before they start.
        const attempt_id = try a.dupe(u8, attempt.id);
        errdefer a.free(attempt_id);
        try self.attempts.append(self.alloc, attempt);
        errdefer _ = self.attempts.pop();
        try attempt.group.concurrent(self.io, Attempt.serve, .{attempt});
        attempt.group.concurrent(self.io, Attempt.expire, .{attempt}) catch |err| {
            attempt.server.stop();
            attempt.group.cancel(self.io);
            return err;
        };
        return .{ .attempt_id = attempt_id, .authorization_url = url, .expires_at = attempt.expires_at };
    }
    pub fn outcome(self: *Manager, a: std.mem.Allocator, owner: []const u8, id: []const u8) !Outcome {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (self.attempts.items) |attempt| if (std.mem.eql(u8, attempt.owner, owner) and std.mem.eql(u8, attempt.id, id)) {
            var result = attempt.outcome;
            if (result.connection_id) |value| result.connection_id = try a.dupe(u8, value);
            return result;
        };
        return error.NotFound;
    }
    pub fn summaries(self: *Manager, a: std.mem.Allocator, owner: []const u8) ![]Summary {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var saved = try self.load(a);
        defer {
            scrub(saved.value);
            saved.deinit();
        }
        var result: std.ArrayList(Summary) = .empty;
        for (saved.value.accounts) |rec| if (std.mem.eql(u8, rec.owner, owner)) {
            try result.append(a, .{ .connection_id = try a.dupe(u8, rec.connection_id), .email = try a.dupe(u8, rec.email), .label = try a.dupe(u8, rec.client_id), .connected = rec.access_token.len != 0, .plan_enabled = rec.access_token.len != 0 and protocol.hasScope(rec.scope, "chatgpt.tokens.use.direct") });
        };
        return result.toOwnedSlice(a);
    }
    /// Request-long pin prevents disconnect/reconnect from reviving a tool loop.
    pub fn pin(self: *Manager, owner: []const u8, id: []const u8) !Pin {
        return self.pinWithContext(owner, id, .{ .io = self.io, .deadline_ns = null });
    }
    fn lockWithContext(self: *Manager, context: RequestContext) !void {
        while (true) {
            try context.check();
            if (self.mutex.tryLock()) return;
            try self.io.sleep(.fromMilliseconds(1), .awake);
        }
    }
    pub fn pinWithContext(self: *Manager, owner: []const u8, id: []const u8, context: RequestContext) !Pin {
        try self.lockWithContext(context);
        defer self.mutex.unlock(self.io);
        try self.checkOwner(owner);
        var state = try self.load(self.alloc);
        defer {
            scrub(state.value);
            state.deinit();
        }
        _ = try find(&state.value, owner, id);
        try context.check();
        const current = try self.session(id);
        return .{ .session = current, .epoch = current.epoch.load(.acquire) };
    }
    pub fn lease(self: *Manager, a: std.mem.Allocator, owner: []const u8, id: []const u8) !Lease {
        return self.leaseBound(a, owner, id, null);
    }
    pub fn leaseBound(self: *Manager, a: std.mem.Allocator, owner: []const u8, id: []const u8, expected: ?Pin) !Lease {
        return self.leaseBoundWithContext(a, owner, id, expected, .{ .io = self.io, .deadline_ns = null });
    }
    pub fn leaseBoundWithContext(self: *Manager, a: std.mem.Allocator, owner: []const u8, id: []const u8, expected: ?Pin, context: RequestContext) !Lease {
        try self.lockWithContext(context);
        defer self.mutex.unlock(self.io);
        try self.checkOwner(owner);
        if (expected) |binding| try binding.check(id);
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const local = arena.allocator();
        var saved = try self.load(local);
        defer {
            scrub(saved.value);
            saved.deinit();
        }
        const rec = try find(&saved.value, owner, id);
        if (rec.access_token.len == 0) return error.ChatGPTReconnectRequired;
        if (!protocol.hasScope(rec.scope, "chatgpt.tokens.use.direct")) return error.ChatGPTPlanDisabled;
        if (rec.expires_at <= now(self.io) + 60) {
            if (rec.refresh_token.len == 0) return error.ChatGPTReconnectRequired;
            var http = self.client(local);
            defer http.deinit();
            const body = try protocol.form(local, &.{ .{ "grant_type", "refresh_token" }, .{ "client_id", rec.client_id }, .{ "refresh_token", rec.refresh_token }, .{ "resource", protocol.resource } });
            defer std.crypto.secureZero(u8, body);
            var token = tokenRequestWithContext(local, &http, try self.authUrl(local, "/api/accounts/oauth/token"), body, context) catch |err| {
                if (err == error.ChatGPTReconnectRequired) {
                    scrubRecord(rec.*);
                    rec.access_token = "";
                    rec.refresh_token = "";
                    rec.id_token = "";
                    try self.save(saved.value);
                    _ = (try self.session(id)).epoch.fetchAdd(1, .acq_rel);
                }
                return err;
            };
            defer {
                scrubToken(token.value);
                token.deinit();
            }
            std.crypto.secureZero(u8, @constCast(rec.access_token));
            rec.access_token = try local.dupe(u8, token.value.access_token);
            if (token.value.refresh_token) |rotated| {
                std.crypto.secureZero(u8, @constCast(rec.refresh_token));
                rec.refresh_token = try local.dupe(u8, rotated);
            }
            if (token.value.scope) |scope| rec.scope = try local.dupe(u8, scope);
            rec.expires_at = now(self.io) + token.value.expires_in;
            try self.save(saved.value);
            if (!protocol.hasScope(rec.scope, "chatgpt.tokens.use.direct")) return error.ChatGPTPlanDisabled;
        }
        try context.check();
        const s = try self.session(id);
        return .{ .alloc = a, .access_token = try a.dupe(u8, rec.access_token), .session = s, .epoch = s.epoch.load(.acquire) };
    }
    fn checkOwner(self: *Manager, owner: []const u8) !void {
        if (self.revoked_owners.contains(owner)) return error.ChatGPTReconnectRequired;
    }
    /// User deletion must complete local revocation before username reuse.
    /// Fence request-start identities too: an already authenticated request
    /// cannot begin another sign-in after the deletion guard has run.
    pub fn removeOwner(self: *Manager, owner: []const u8) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (!self.revoked_owners.contains(owner)) {
            const owned = try self.alloc.dupe(u8, owner);
            errdefer self.alloc.free(owned);
            try self.revoked_owners.put(self.alloc, owned, {});
        }
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var saved = try self.load(a);
        defer {
            scrub(saved.value);
            saved.deinit();
        }
        var retained: std.ArrayList(Record) = .empty;
        for (saved.value.accounts) |record| {
            if (std.mem.eql(u8, record.owner, owner)) {
                for (self.sessions.items) |current| if (std.mem.eql(u8, current.id, record.connection_id)) {
                    _ = current.epoch.fetchAdd(1, .acq_rel);
                };
                scrubRecord(record);
            } else try retained.append(a, record);
        }
        for (self.attempts.items) |attempt| {
            if (attempt.outcome.status != .pending or !std.mem.eql(u8, attempt.owner, owner)) continue;
            attempt.outcome = .{ .status = .declined };
            std.crypto.secureZero(u8, @constCast(attempt.verifier));
        }
        saved.value.accounts = retained.items;
        try self.save(saved.value);
    }
    pub fn revokeDeletedUser(raw: *anyopaque, instance_id: [16]u8) bool {
        const self: *Manager = @ptrCast(@alignCast(raw));
        const owner = userOwner(self.alloc, instance_id) catch return false;
        defer self.alloc.free(owner);
        self.removeOwner(owner) catch return false;
        return true;
    }
    pub fn disconnect(self: *Manager, owner: []const u8, id: []const u8) !bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var saved = try self.load(a);
        defer {
            scrub(saved.value);
            saved.deinit();
        }
        const rec = try find(&saved.value, owner, id);
        const refresh = try a.dupe(u8, rec.refresh_token);
        defer std.crypto.secureZero(u8, refresh);
        const client_id = rec.client_id;
        scrubRecord(rec.*);
        rec.access_token = "";
        rec.refresh_token = "";
        rec.id_token = "";
        rec.scope = "";
        const current = try self.session(id);
        try self.save(saved.value);
        _ = current.epoch.fetchAdd(1, .acq_rel);
        // An unselected registration has no account binding yet. Cancel it for
        // this owner as well so it cannot revive a disconnected registration.
        // Keep the bounded callback listener until expiry to reject late redirects.
        for (self.attempts.items) |attempt| {
            if (attempt.outcome.status != .pending or !std.mem.eql(u8, attempt.owner, owner)) continue;
            if (attempt.selected_id) |selected| if (!std.mem.eql(u8, selected, id)) continue;
            attempt.outcome = .{ .status = .declined };
            std.crypto.secureZero(u8, @constCast(attempt.verifier));
        }
        if (refresh.len == 0) return true;
        var http = self.client(a);
        defer http.deinit();
        const discovery = getJson(a, &http, try self.authUrl(a, "/.well-known/openid-configuration")) catch return false;
        const endpoint = std.json.parseFromSliceLeaky(struct { revocation_endpoint: []const u8 }, a, discovery, .{ .ignore_unknown_fields = true }) catch return false;
        if (!self.trustedAuth(endpoint.revocation_endpoint)) return false;
        const body = try protocol.form(a, &.{ .{ "token", refresh }, .{ "token_type_hint", "refresh_token" }, .{ "client_id", client_id } });
        defer std.crypto.secureZero(u8, body);
        var response = http.post(endpoint.revocation_endpoint, .{ .body = body, .headers = &.{.{ "Content-Type", "application/x-www-form-urlencoded" }}, .timeout_ms = 15_000, .follow_redirects = false, .max_retries = 0, .cookies_enabled = false }) catch return false;
        defer response.deinit();
        return response.status.code == 200;
    }
    pub fn models(self: *Manager, a: std.mem.Allocator, owner: []const u8, id: []const u8) ![]u8 {
        var credential = try self.lease(a, owner, id);
        defer credential.deinit();
        const bearer = try std.fmt.allocPrint(a, "Bearer {s}", .{credential.access_token});
        defer {
            std.crypto.secureZero(u8, bearer);
            a.free(bearer);
        }
        var http = self.client(a);
        defer http.deinit();
        var response = try http.get(protocol.resource ++ "/models", .{ .headers = &.{.{ "Authorization", bearer }}, .timeout_ms = 30_000, .follow_redirects = false, .max_retries = 0, .cookies_enabled = false });
        defer response.deinit();
        if (credential.cancelled()) return error.ChatGPTReconnectRequired;
        if (response.status.code == 401) return error.ChatGPTReconnectRequired;
        if (!response.ok()) return error.ChatGPTModelsUnavailable;
        return a.dupe(u8, response.body orelse return error.InvalidResponse);
    }
};
fn now(io: std.Io) i64 {
    return @intCast(@divFloor(std.Io.Clock.now(.real, io).nanoseconds, std.time.ns_per_s));
}
fn find(state: *State, owner: []const u8, id: []const u8) !*Record {
    for (state.accounts) |*rec| if (std.mem.eql(u8, rec.connection_id, id) and std.mem.eql(u8, rec.owner, owner)) return rec;
    return error.NotFound;
}
fn trustedAuthUrl(url: []const u8) bool {
    return std.mem.startsWith(u8, url, protocol.issuer ++ "/") and std.mem.indexOfAny(u8, url, "?#") == null;
}
fn getJson(a: std.mem.Allocator, http: *httpx.Client, url: []const u8) ![]u8 {
    var response = try http.get(url, .{ .timeout_ms = 15_000, .follow_redirects = false, .max_retries = 0, .cookies_enabled = false });
    defer response.deinit();
    if (!response.ok()) return error.ChatGPTAuthorizationUnavailable;
    return a.dupe(u8, response.body orelse return error.InvalidResponse);
}
fn tokenRequest(a: std.mem.Allocator, http: *httpx.Client, url: []const u8, body: []const u8) !std.json.Parsed(Token) {
    return tokenRequestWithContext(a, http, url, body, .{ .io = http.io, .deadline_ns = null });
}
fn tokenRequestWithContext(a: std.mem.Allocator, http: *httpx.Client, url: []const u8, body: []const u8, context: RequestContext) !std.json.Parsed(Token) {
    const Cancel = struct {
        fn cancelled(raw: *const anyopaque) bool {
            const request: *const RequestContext = @ptrCast(@alignCast(raw));
            if (request.cancellation) |token| return token.isCancelled();
            return false;
        }
    };
    var response = try http.post(url, .{ .body = body, .headers = &.{.{ "Content-Type", "application/x-www-form-urlencoded" }}, .timeout_ms = @min(15_000, try context.remainingTimeoutMs() orelse 15_000), .cancellation = httpx.CancellationToken.fromCallback(&context, Cancel.cancelled), .follow_redirects = false, .max_retries = 0, .cookies_enabled = false });
    defer response.deinit();
    defer if (response.body) |bytes| std.crypto.secureZero(u8, @constCast(bytes));
    if (!response.ok()) {
        var err = std.json.parseFromSlice(struct { @"error": []const u8 = "" }, a, response.body orelse "{}", .{ .ignore_unknown_fields = true }) catch return error.ChatGPTAuthorizationUnavailable;
        defer err.deinit();
        for ([_][]const u8{ "invalid_grant", "invalid_refresh_token", "token_expired", "refresh_token_expired", "refresh_token_invalidated", "refresh_token_reused" }) |code| if (std.mem.eql(u8, err.value.@"error", code)) return error.ChatGPTReconnectRequired;
        return error.ChatGPTAuthorizationUnavailable;
    }
    var result = try std.json.parseFromSlice(Token, a, response.body orelse return error.InvalidResponse, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
    errdefer result.deinit();
    if (result.value.expires_in <= 0 or result.value.expires_in > 86400 or result.value.access_token.len == 0 or !std.ascii.eqlIgnoreCase(result.value.token_type, "Bearer")) return error.InvalidToken;
    return result;
}

pub const Pin = struct {
    session: *Session,
    epoch: u64,
    pub fn check(self: Pin, id: []const u8) !void {
        if (!std.mem.eql(u8, self.session.id, id) or self.session.epoch.load(.acquire) != self.epoch) return error.ChatGPTReconnectRequired;
    }
};
fn scrubToken(token: Token) void {
    std.crypto.secureZero(u8, @constCast(token.access_token));
    if (token.refresh_token) |value| std.crypto.secureZero(u8, @constCast(value));
    if (token.id_token) |value| std.crypto.secureZero(u8, @constCast(value));
}
fn scrubRecord(rec: Record) void {
    std.crypto.secureZero(u8, @constCast(rec.access_token));
    std.crypto.secureZero(u8, @constCast(rec.refresh_token));
    std.crypto.secureZero(u8, @constCast(rec.id_token));
}
fn scrub(state: State) void {
    for (state.accounts) |rec| scrubRecord(rec);
}

test "chatgpt generation bounds refresh and credential queueing by deadline and cancellation" {
    const Mock = struct {
        cancel: ?*std.atomic.Value(bool) = null,
        requests: std.atomic.Value(u32) = .init(0),
        fn token(self: *@This(), ctx: *httpx.Context) !httpx.Response {
            _ = self.requests.fetchAdd(1, .acq_rel);
            if (self.cancel) |signal| signal.store(true, .release);
            try ctx.io.sleep(.fromSeconds(1), .awake);
            return ctx.status(400).json(.{ .@"error" = "invalid_grant" });
        }
        fn serve(server: *httpx.Server) std.Io.Cancelable!void {
            server.listen() catch {};
        }
    };
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var instance = try Manager.init(a, io, root);
    defer instance.deinit();
    var saved = try instance.load(a);
    defer saved.deinit();
    try instance.save(.{ .host_id = saved.value.host_id, .accounts = @constCast(&[_]Record{
        .{ .connection_id = "one", .owner = "alice", .subject = "user-a", .email = "a@example.com", .client_id = "client-a", .access_token = "expired", .refresh_token = "refresh", .id_token = "", .scope = protocol.scopes, .expires_at = 1 },
    }) });
    var cancelled: std.atomic.Value(bool) = .init(false);
    var mock: Mock = .{};
    var server = httpx.Server.initWithConfig(a, io, .{ .host = "127.0.0.1", .port = 0 });
    defer server.deinit();
    try server.post("/api/accounts/oauth/token", httpx.Handler.bind(&mock, Mock.token));
    try server.bind();
    const origin = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}", .{server.boundAddress().?.getPort()});
    defer a.free(origin);
    instance.test_auth_origin = origin;
    var group: std.Io.Group = .init;
    try group.concurrent(io, Mock.serve, .{&server});
    defer {
        server.stop();
        group.cancel(io);
    }
    var http = instance.client(a);
    defer http.deinit();
    var provider: @import("responses.zig").Provider = .{ .http = &http, .registrations = &instance, .owner = "alice", .connection_id = "one", .timeout_ms = 50 };
    // A short provider timeout includes refresh, not only the Responses POST.
    var started = @import("antfly_platform").time.monotonicNs();
    try std.testing.expectError(error.Timeout, provider.generate(a, "model", &.{}));
    try std.testing.expect(@import("antfly_platform").time.monotonicNs() - started < 750 * std.time.ns_per_ms);
    // Cancellation arriving during refresh interrupts it and preserves the grant.
    mock.cancel = &cancelled;
    provider.timeout_ms = 5_000;
    provider.request_context = .{ .io = io, .deadline_ns = null, .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled) };
    started = @import("antfly_platform").time.monotonicNs();
    try std.testing.expectError(error.Cancelled, provider.generate(a, "model", &.{}));
    try std.testing.expect(@import("antfly_platform").time.monotonicNs() - started < 750 * std.time.ns_per_ms);
    try std.testing.expectEqual(@as(u32, 2), mock.requests.load(.acquire));
    var retained = try instance.load(a);
    defer {
        scrub(retained.value);
        retained.deinit();
    }
    try std.testing.expectEqualStrings("expired", retained.value.accounts[0].access_token);
    try std.testing.expectEqualStrings("refresh", retained.value.accounts[0].refresh_token);
    // Waiting behind another refresh must obey the same controls for both pins
    // and leases, including cancellation before any network request.
    instance.mutex.lockUncancelable(io);
    defer instance.mutex.unlock(io);
    const cancel_context = provider.request_context.?;
    try std.testing.expectError(error.Cancelled, instance.pinWithContext("alice", "one", cancel_context));
    try std.testing.expectError(error.Cancelled, instance.leaseBoundWithContext(a, "alice", "one", null, cancel_context));
    const deadline_context: RequestContext = .{ .io = io, .deadline_ns = @import("antfly_platform").time.monotonicNs() + 10 * std.time.ns_per_ms };
    try std.testing.expectError(error.Timeout, instance.pinWithContext("alice", "one", deadline_context));
    try std.testing.expectError(error.Timeout, instance.leaseBoundWithContext(a, "alice", "one", null, deadline_context));
    try std.testing.expectEqual(@as(u32, 2), mock.requests.load(.acquire));
}

test "chatgpt authorization return allocation failure leaves no running attempt" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var instance = try Manager.init(a, io, root);
    defer instance.deinit();
    // The URL allocation succeeds; copying the attempt ID fails.
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 1 });
    try std.testing.expectError(error.OutOfMemory, instance.begin(failing.allocator(), "alice", null));
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), instance.attempts.items.len);
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
}

test "chatgpt mock OAuth rotates refresh and rejects changed subject" {
    const Mock = struct {
        origin: []const u8 = "",
        identity: []const u8 = @embedFile("fixtures/identity.jwt"),
        refreshes: std.atomic.Value(u32) = .init(0),
        fn discovery(self: *@This(), ctx: *httpx.Context) !httpx.Response {
            const keys_url = try std.fmt.allocPrint(ctx.allocator, "{s}/keys", .{self.origin});
            defer ctx.allocator.free(keys_url);
            const revoke_url = try std.fmt.allocPrint(ctx.allocator, "{s}/revoke", .{self.origin});
            defer ctx.allocator.free(revoke_url);
            return ctx.json(.{ .issuer = protocol.issuer, .jwks_uri = keys_url, .revocation_endpoint = revoke_url });
        }
        fn keys(_: *@This(), ctx: *httpx.Context) !httpx.Response {
            return ctx.text(@embedFile("fixtures/jwks.json"));
        }
        fn token(self: *@This(), ctx: *httpx.Context) !httpx.Response {
            const body = (try ctx.body()) orelse return error.InvalidRequest;
            if (std.mem.indexOf(u8, body, "grant_type=refresh_token") != null) {
                _ = self.refreshes.fetchAdd(1, .monotonic);
                if (std.mem.indexOf(u8, body, "refresh_token=refresh-one") == null) return ctx.status(400).json(.{ .@"error" = "invalid_grant" });
                return ctx.json(.{ .access_token = "access-two", .refresh_token = "refresh-two", .expires_in = 3600, .token_type = "Bearer", .scope = protocol.scopes });
            }
            if (std.mem.indexOf(u8, body, "client_id=issued-client") == null or std.mem.indexOf(u8, body, "code_verifier=") == null) return error.InvalidRequest;
            return ctx.json(.{ .access_token = "access-one", .refresh_token = "refresh-one", .id_token = self.identity, .expires_in = 3600, .token_type = "Bearer", .scope = protocol.scopes });
        }
        fn revoke(_: *@This(), ctx: *httpx.Context) !httpx.Response {
            if (std.mem.indexOf(u8, (try ctx.body()).?, "token=refresh-two") == null) return error.InvalidRequest;
            return ctx.text("");
        }
        fn serve(server: *httpx.Server) std.Io.Cancelable!void {
            server.listen() catch {};
        }
    };
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var mock: Mock = .{};
    var server = httpx.Server.initWithConfig(a, io, .{ .host = "127.0.0.1", .port = 0 });
    defer server.deinit();
    try server.get("/.well-known/openid-configuration", httpx.Handler.bind(&mock, Mock.discovery));
    try server.get("/keys", httpx.Handler.bind(&mock, Mock.keys));
    try server.post("/api/accounts/oauth/token", httpx.Handler.bind(&mock, Mock.token));
    try server.post("/revoke", httpx.Handler.bind(&mock, Mock.revoke));
    try server.bind();
    mock.origin = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}", .{server.boundAddress().?.getPort()});
    defer a.free(mock.origin);
    var group: std.Io.Group = .init;
    try group.concurrent(io, Mock.serve, .{&server});
    defer {
        server.stop();
        group.cancel(io);
    }
    var instance = try Manager.init(a, io, root);
    defer instance.deinit();
    instance.test_auth_origin = mock.origin;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const local = arena.allocator();
    var http = instance.client(local);
    defer http.deinit();
    const started = try instance.begin(local, "alice", null);
    try std.testing.expect(std.mem.indexOf(u8, started.authorization_url, "client_id=dynamic_agent_client") != null);
    try std.testing.expect(std.mem.indexOf(u8, started.authorization_url, "code_challenge_method=S256") != null);
    instance.attempts.items[0].nonce = "test-nonce";
    const callback = try std.fmt.allocPrint(local, "{s}?state={s}&code=test&client_id=issued-client", .{ instance.attempts.items[0].callback_uri, started.attempt_id });
    var response = try http.get(callback, .{ .timeout_ms = 5000 });
    response.deinit();
    const outcome = try instance.outcome(local, "alice", started.attempt_id);
    try std.testing.expectEqual(Status.connected, outcome.status);
    const id = outcome.connection_id.?;
    try std.testing.expectError(error.NotFound, instance.outcome(local, "bob", started.attempt_id));
    var saved = try instance.load(local);
    saved.value.accounts[0].expires_at = 1;
    try instance.save(saved.value);
    scrub(saved.value);
    saved.deinit();
    const Refresh = struct {
        fn run(m: *Manager, alloc: std.mem.Allocator, registration: []const u8) !void {
            var lease = try m.lease(alloc, "alice", registration);
            defer lease.deinit();
            try std.testing.expectEqualStrings("access-two", lease.access_token);
        }
    };
    var first = try io.concurrent(Refresh.run, .{ &instance, a, id });
    var second = try io.concurrent(Refresh.run, .{ &instance, a, id });
    try first.await(io);
    try second.await(io);
    try std.testing.expectEqual(@as(u32, 1), mock.refreshes.load(.acquire));
    saved = try instance.load(local);
    try std.testing.expectEqualStrings("refresh-two", saved.value.accounts[0].refresh_token);
    scrub(saved.value);
    saved.deinit();
    mock.identity = @embedFile("fixtures/other-identity.jwt");
    const returning = try instance.begin(local, "alice", id);
    try std.testing.expect(std.mem.indexOf(u8, returning.authorization_url, "client_id=issued-client") != null);
    const attempt = instance.attempts.items[1];
    attempt.nonce = "test-nonce";
    const wrong_callback = try std.fmt.allocPrint(local, "{s}?state={s}&code=test&client_id=issued-client", .{ attempt.callback_uri, returning.attempt_id });
    response = try http.get(wrong_callback, .{ .timeout_ms = 5000 });
    response.deinit();
    try std.testing.expectEqual(Status.@"error", (try instance.outcome(local, "alice", returning.attempt_id)).status);
    var unchanged = try instance.lease(a, "alice", id);
    try std.testing.expectEqualStrings("access-two", unchanged.access_token);
    unchanged.deinit();
    mock.identity = @embedFile("fixtures/identity.jwt");
    const pending_signin = try instance.begin(local, "alice", id);
    const pending_attempt = instance.attempts.items[2];
    pending_attempt.nonce = "test-nonce";
    const unselected = try instance.begin(local, "alice", null);
    const other_owner = try instance.begin(local, "bob", null);
    const selected_pin = pending_attempt.selected_pin.?;
    try std.testing.expect(try instance.disconnect("alice", id));
    try std.testing.expectError(error.ChatGPTReconnectRequired, selected_pin.check(id));
    try std.testing.expectEqual(Status.declined, (try instance.outcome(local, "alice", pending_signin.attempt_id)).status);
    try std.testing.expectEqual(Status.declined, (try instance.outcome(local, "alice", unselected.attempt_id)).status);
    try std.testing.expectEqual(Status.pending, (try instance.outcome(local, "bob", other_owner.attempt_id)).status);
    const late_callback = try std.fmt.allocPrint(local, "{s}?state={s}&code=test&client_id=issued-client", .{ pending_attempt.callback_uri, pending_signin.attempt_id });
    response = try http.get(late_callback, .{ .timeout_ms = 5000 });
    try std.testing.expectEqual(@as(u16, 400), response.status.code);
    response.deinit();
    try std.testing.expect(!(try instance.summaries(local, "alice"))[0].connected);
    try std.testing.expectError(error.ChatGPTReconnectRequired, instance.lease(a, "alice", id));
    // A deliberate sign-in started after logout can reconnect the same account.
    const fresh = try instance.begin(local, "alice", id);
    const fresh_attempt = instance.attempts.items[5];
    fresh_attempt.nonce = "test-nonce";
    const fresh_callback = try std.fmt.allocPrint(local, "{s}?state={s}&code=test&client_id=issued-client", .{ fresh_attempt.callback_uri, fresh.attempt_id });
    response = try http.get(fresh_callback, .{ .timeout_ms = 5000 });
    response.deinit();
    try std.testing.expectEqual(Status.connected, (try instance.outcome(local, "alice", fresh.attempt_id)).status);
}

test "chatgpt manager persists host identity and partitions personal registrations" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var instance = try Manager.init(a, io, root);
    var initial = try instance.load(a);
    const host = try a.dupe(u8, initial.value.host_id);
    defer a.free(host);
    try instance.save(.{ .host_id = initial.value.host_id, .accounts = @constCast(&[_]Record{
        .{ .connection_id = "one", .owner = "alice", .subject = "user-a", .email = "same@example.com", .client_id = "client-a", .access_token = "test-access", .refresh_token = "", .id_token = "", .scope = protocol.scopes, .expires_at = now(io) + 3600 },
        .{ .connection_id = "two", .owner = "bob", .subject = "user-b", .email = "same@example.com", .client_id = "client-b", .access_token = "test-access-b", .refresh_token = "", .id_token = "", .scope = "openid profile email", .expires_at = now(io) + 3600 },
    }) });
    initial.deinit();
    {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const summaries = try instance.summaries(arena.allocator(), "alice");
        try std.testing.expectEqual(@as(usize, 1), summaries.len);
        try std.testing.expectEqualStrings("one", summaries[0].connection_id);
        const json = try std.json.Stringify.valueAlloc(arena.allocator(), summaries, .{});
        try std.testing.expect(std.mem.indexOf(u8, json, "access_token") == null);
    }
    try std.testing.expectError(error.NotFound, instance.lease(a, "bob", "one"));
    try std.testing.expectError(error.ChatGPTPlanDisabled, instance.lease(a, "bob", "two"));
    var lease = try instance.lease(a, "alice", "one");
    const pin = try instance.pin("alice", "one");
    try std.testing.expectEqualStrings("test-access", lease.access_token);
    try std.testing.expect(try instance.disconnect("alice", "one"));
    try std.testing.expect(lease.cancelled());
    try std.testing.expectError(error.ChatGPTReconnectRequired, pin.check("one"));
    try std.testing.expectError(error.ChatGPTReconnectRequired, instance.lease(a, "alice", "one"));
    // No request may outlive its runtime; release the lease before reopening.
    lease.deinit();
    instance.deinit();
    var reopened = try Manager.init(a, io, root);
    defer reopened.deinit();
    var saved = try reopened.load(a);
    defer {
        scrub(saved.value);
        saved.deinit();
    }
    try std.testing.expectEqualStrings(host, saved.value.host_id);
    try std.testing.expectEqualStrings("client-a", saved.value.accounts[0].client_id);
    try std.testing.expectEqualStrings("", saved.value.accounts[0].access_token);
    if (@import("builtin").os.tag != .windows) {
        const directory = try std.Io.Dir.cwd().statFile(io, root, .{});
        try std.testing.expectEqual(@as(u32, 0o700), @as(u32, @intCast(directory.permissions.toMode())) & 0o777);
        const stat = try std.Io.Dir.cwd().statFile(io, reopened.path, .{});
        try std.testing.expectEqual(@as(u32, 0), @as(u32, @intCast(stat.permissions.toMode())) & 0o077);
    }
}

test "chatgpt manager callback is one time owner bound and state checked" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var instance = try Manager.init(a, io, root);
    defer instance.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const local = arena.allocator();
    const begin = try instance.begin(local, "alice", null);
    try std.testing.expect(std.mem.startsWith(u8, begin.authorization_url, protocol.issuer ++ "/api/accounts/authorize?"));
    try std.testing.expectError(error.NotFound, instance.outcome(local, "bob", begin.attempt_id));
    const attempt = instance.attempts.items[0];
    var http = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
    defer http.deinit();
    const bad_url = try std.fmt.allocPrint(local, "{s}?error=access_denied&state=wrong", .{attempt.callback_uri});
    var bad = try http.get(bad_url, .{});
    defer bad.deinit();
    try std.testing.expectEqual(@as(u16, 400), bad.status.code);
    try std.testing.expectEqual(Status.pending, (try instance.outcome(local, "alice", begin.attempt_id)).status);
    const declined_url = try std.fmt.allocPrint(local, "{s}?error=access_denied&state={s}", .{ attempt.callback_uri, begin.attempt_id });
    var declined = try http.get(declined_url, .{});
    defer declined.deinit();
    try std.testing.expectEqual(@as(u16, 200), declined.status.code);
    try std.testing.expectEqual(Status.declined, (try instance.outcome(local, "alice", begin.attempt_id)).status);
    attempt.expires_at = now(io) - 1;
    instance.reap();
    try std.testing.expectEqual(@as(usize, 0), instance.attempts.items.len);
    try std.testing.expectError(error.NotFound, instance.outcome(local, "alice", begin.attempt_id));
}

test "chatgpt manager rejects oversized updates without replacing durable credentials" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var instance = try Manager.init(a, io, root);
    var closed = false;
    defer if (!closed) instance.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const local = arena.allocator();
    var original = try instance.load(local);
    defer original.deinit();
    const host = original.value.host_id;
    const accounts = try local.alloc(Record, 128);
    for (accounts, 0..) |*account, i| account.* = .{
        .connection_id = try std.fmt.allocPrint(local, "connection-{d}", .{i}),
        .owner = "alice",
        .subject = "user-a",
        .email = "a@example.com",
        .client_id = "client-a",
        .access_token = "access",
        .refresh_token = "refresh",
        .id_token = "identity",
        .scope = protocol.scopes,
        .expires_at = 2_000_000_000,
    };
    try instance.save(.{ .host_id = host, .accounts = accounts });
    const before = try tmp.dir.readFileAlloc(io, "accounts.json", local, .limited(max_store_bytes));
    const large_token = try local.alloc(u8, 4096);
    @memset(large_token, 'a');
    for (accounts) |*account| {
        account.access_token = large_token;
        account.id_token = large_token;
    }
    // This update previously succeeded then made every subsequent load fail.
    try std.testing.expectError(error.CapacityExhausted, instance.save(.{ .host_id = host, .accounts = accounts }));
    const after = try tmp.dir.readFileAlloc(io, "accounts.json", local, .limited(max_store_bytes));
    try std.testing.expectEqualStrings(before, after);
    instance.deinit();
    closed = true;
    var reopened = try Manager.init(a, io, root);
    defer reopened.deinit();
    var retained = try reopened.load(local);
    defer {
        scrub(retained.value);
        retained.deinit();
    }
    try std.testing.expectEqualStrings(host, retained.value.host_id);
    try std.testing.expectEqual(@as(usize, 128), retained.value.accounts.len);
    for (retained.value.accounts) |account| {
        try std.testing.expectEqualStrings("access", account.access_token);
        try std.testing.expectEqualStrings("refresh", account.refresh_token);
        try std.testing.expectEqualStrings("identity", account.id_token);
    }
}

/// Namespaces distinguish authenticated users from the auth-disabled local owner.
pub fn userOwner(a: std.mem.Allocator, instance_id: [16]u8) ![]u8 {
    if (std.mem.allEqual(u8, &instance_id, 0)) return error.Forbidden;
    const hex = std.fmt.bytesToHex(instance_id, .lower);
    return std.fmt.allocPrint(a, "user:{s}", .{hex});
}

test "chatgpt user deletion cancels grants and signins before username recreation" {
    const usermgr = @import("../usermgr/user_manager.zig");
    const casbin = @import("antfly_casbin");
    const a = std.testing.allocator;
    const io = std.testing.io;
    var store = usermgr.MemoryStore.init(a);
    defer store.deinit();
    var policies = casbin.MemoryAdapter.init(a);
    defer policies.deinit();
    var users = try usermgr.UserManager.init(a, store.iface(), try usermgr.initDefaultEnforcer(a, policies.iface()));
    defer users.deinit();
    var first = try users.createUser("alice", "first-password", &.{});
    defer first.deinit(a);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var instance = try Manager.init(a, io, root);
    var closed = false;
    defer if (!closed) instance.deinit();
    users.personal_grant_revoker = .{ .ptr = &instance, .revoke_fn = Manager.revokeDeletedUser };
    defer users.personal_grant_revoker = null;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const local = arena.allocator();
    const owner = try userOwner(local, first.instance_id);
    var saved = try instance.load(local);
    defer saved.deinit();
    try instance.save(.{ .host_id = saved.value.host_id, .accounts = @constCast(&[_]Record{
        .{ .connection_id = "one", .owner = owner, .subject = "old-chatgpt-user", .email = "old@example.com", .client_id = "issued-client", .access_token = "old-access", .refresh_token = "refresh", .id_token = "", .scope = protocol.scopes, .expires_at = now(io) + 3600 },
        .{ .connection_id = "other", .owner = "other-owner", .subject = "other", .email = "other@example.com", .client_id = "other-client", .access_token = "other-access", .refresh_token = "", .id_token = "", .scope = protocol.scopes, .expires_at = now(io) + 3600 },
    }) });
    var lease = try instance.lease(a, owner, "one");
    defer lease.deinit();
    const pin = try instance.pin(owner, "one");
    const selected = try instance.begin(local, owner, "one");
    const pending = try instance.begin(local, owner, null);
    const unrelated = try instance.begin(local, "other-owner", null);
    const callback_uri = instance.attempts.items[0].callback_uri;
    try users.deleteUser("alice");
    try std.testing.expect(lease.cancelled());
    try std.testing.expectError(error.ChatGPTReconnectRequired, pin.check("one"));
    try std.testing.expectEqual(Status.declined, (try instance.outcome(local, owner, selected.attempt_id)).status);
    try std.testing.expectEqual(Status.declined, (try instance.outcome(local, owner, pending.attempt_id)).status);
    try std.testing.expectEqual(Status.pending, (try instance.outcome(local, "other-owner", unrelated.attempt_id)).status);
    try std.testing.expectError(error.ChatGPTReconnectRequired, instance.begin(local, owner, null));
    try std.testing.expectError(error.ChatGPTReconnectRequired, instance.lease(a, owner, "one"));
    var http = instance.client(local);
    defer http.deinit();
    const late_url = try std.fmt.allocPrint(local, "{s}?state={s}&code=late&client_id=issued-client", .{ callback_uri, selected.attempt_id });
    var response = try http.get(late_url, .{ .timeout_ms = 5000 });
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 400), response.status.code);
    var replacement = try users.createUser("alice", "replacement-password", &.{});
    defer replacement.deinit(a);
    var authenticated = try users.authenticateUser("alice", "replacement-password");
    defer authenticated.deinit(a);
    const new_owner = try userOwner(local, authenticated.instance_id);
    try std.testing.expect(!std.mem.eql(u8, owner, new_owner));
    try std.testing.expectEqual(@as(usize, 0), (try instance.summaries(local, new_owner)).len);
    try std.testing.expectError(error.NotFound, instance.lease(a, new_owner, "one"));
    _ = try instance.begin(local, new_owner, null);
    var retained = try instance.load(local);
    defer {
        scrub(retained.value);
        retained.deinit();
    }
    try std.testing.expectEqual(@as(usize, 1), retained.value.accounts.len);
    try std.testing.expectEqualStrings("other-access", retained.value.accounts[0].access_token);
    // The anonymous owner namespace cannot collide with a database username.
    var named_local = try users.createUser("local-owner", "password", &.{});
    defer named_local.deinit(a);
    try std.testing.expect(!std.mem.eql(u8, "local:", try userOwner(local, named_local.instance_id)));
    users.personal_grant_revoker = null;
    instance.deinit();
    closed = true;
    var reopened = try Manager.init(a, io, root);
    defer reopened.deinit();
    try std.testing.expectEqual(@as(usize, 0), (try reopened.summaries(local, owner)).len);
    try std.testing.expectEqual(@as(usize, 0), (try reopened.summaries(local, new_owner)).len);
    try std.testing.expectError(error.NotFound, reopened.lease(a, new_owner, "one"));
    var other = try reopened.lease(a, "other-owner", "other");
    defer other.deinit();
    try std.testing.expectEqualStrings("other-access", other.access_token);
}

test "chatgpt legacy username grants require consent after ownership migration" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var instance = try Manager.init(a, io, root);
    var closed = false;
    defer if (!closed) instance.deinit();
    var saved = try instance.load(a);
    defer saved.deinit();
    const host = try a.dupe(u8, saved.value.host_id);
    defer a.free(host);
    try instance.save(.{ .version = 1, .host_id = host, .accounts = @constCast(&[_]Record{
        .{ .connection_id = "one", .owner = "alice", .subject = "old", .email = "old@example.com", .client_id = "issued-client", .access_token = "old-access", .refresh_token = "refresh", .id_token = "", .scope = protocol.scopes, .expires_at = now(io) + 3600 },
    }) });
    instance.deinit();
    closed = true;
    var reopened = try Manager.init(a, io, root);
    defer reopened.deinit();
    var migrated = try reopened.load(a);
    defer migrated.deinit();
    try std.testing.expectEqual(@as(u32, 2), migrated.value.version);
    try std.testing.expectEqual(@as(usize, 0), migrated.value.accounts.len);
    try std.testing.expectEqualStrings(host, migrated.value.host_id);
    try std.testing.expectError(error.NotFound, reopened.lease(a, "alice", "one"));
}
