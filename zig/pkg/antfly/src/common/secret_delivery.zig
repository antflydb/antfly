// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Read-only delivery, with explicit per-consumer key grants. Dedicated mounted
//! 256-bit credentials authenticate and encrypt both directions even across a
//! TLS-terminating proxy. Replies authenticate the complete randomized request,
//! preventing replay of an old value/absence against a fresh lookup. No cache.
const std = @import("std");
const contract = @import("secret_contract.zig");
const secrets = @import("secrets.zig");
const http = @import("http/http_common.zig");
const auth = @import("../api/internal_service_auth.zig");
const Aead = std.crypto.aead.chacha_poly.XChaCha20Poly1305;
pub const path = "/internal/v1/secrets:read";
pub const max_request_bytes = 16 * 1024;
pub const max_response_bytes = 4 * 1024 * 1024;
const request_domain = "antfly-secret-delivery-request-v1";
const Request = struct { operation: enum { resolve, list, refresh }, scope: []const u8, key: ?[]const u8 = null, min_revision: u64 = 0 };
const Response = struct { revision: u64, entry_revision: ?u64 = null, value_hex: ?[]const u8 = null, entries: []const contract.Metadata = &.{} };

fn credential(alloc: std.mem.Allocator, io: std.Io, file: []const u8) ![32]u8 {
    const raw = std.Io.Dir.cwd().readFileAlloc(io, file, alloc, .limited(128)) catch return error.Unavailable;
    defer {
        std.crypto.secureZero(u8, raw);
        alloc.free(raw);
    }
    const trimmed = std.mem.trim(u8, raw, " \r\n\t");
    if (trimmed.len != 64) return error.InvalidConfig;
    var key: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&key, trimmed) catch return error.InvalidConfig;
    return key;
}

fn seal(alloc: std.mem.Allocator, io: std.Io, key: [32]u8, aad: []const u8, plaintext: []const u8) ![]u8 {
    const bytes = try alloc.alloc(u8, 24 + plaintext.len + 16);
    errdefer alloc.free(bytes);
    try io.randomSecure(bytes[0..24]);
    var tag: [16]u8 = undefined;
    Aead.encrypt(bytes[24..][0..plaintext.len], &tag, plaintext, aad, bytes[0..24].*, key);
    @memcpy(bytes[bytes.len - 16 ..], &tag);
    return bytes;
}
fn open(alloc: std.mem.Allocator, key: [32]u8, aad: []const u8, bytes: []const u8, limit: usize) ![]u8 {
    if (bytes.len < 40 or bytes.len > limit) return error.Unauthorized;
    const plain = try alloc.alloc(u8, bytes.len - 40);
    errdefer {
        std.crypto.secureZero(u8, plain);
        alloc.free(plain);
    }
    Aead.decrypt(plain, bytes[24 .. bytes.len - 16], bytes[bytes.len - 16 ..][0..16].*, aad, bytes[0..24].*, key) catch return error.Unauthorized;
    return plain;
}
fn responseAad(request: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-secret-delivery-response-v1");
    hash.update(request);
    var out: [32]u8 = undefined;
    hash.final(&out);
    return out;
}
fn allowed(grant: secrets.Config.Native.Grant, key: []const u8) bool {
    for (grant.keys) |item| if (std.mem.eql(u8, item, key)) return true;
    return false;
}

/// Returns ciphertext only. Callers must not expose an unauthenticated plaintext
/// variant of this endpoint. Grant IDs are selectors, never authorization.
pub fn serve(alloc: std.mem.Allocator, store: *secrets.FileStore, grant_name: []const u8, body: []const u8) ![]u8 {
    const cfg = (store.native_config orelse return error.Unavailable).value;
    const source = store.native_source orelse return error.Unavailable;
    const grant = for (cfg.grants) |item| {
        if (std.mem.eql(u8, grant_name, item.name)) break item;
    } else return error.Unauthorized;
    var key = try credential(alloc, store.io, grant.credential_path);
    defer std.crypto.secureZero(u8, &key);
    const plain = try open(alloc, key, request_domain, body, max_request_bytes);
    defer {
        std.crypto.secureZero(u8, plain);
        alloc.free(plain);
    }
    var parsed = std.json.parseFromSlice(Request, alloc, plain, .{}) catch return error.InvalidArgument;
    defer parsed.deinit();
    const request = parsed.value;
    if (!std.mem.eql(u8, request.scope, cfg.scope)) return error.Unauthorized;
    var lookup: ?contract.Lookup = null;
    defer if (lookup) |*value| value.deinit(alloc);
    var listing: ?contract.Listing = null;
    defer if (listing) |*value| value.deinit(alloc);
    var entries = std.ArrayList(contract.Metadata).empty;
    defer entries.deinit(alloc);
    var hex: ?[]u8 = null;
    defer if (hex) |bytes| {
        std.crypto.secureZero(u8, bytes);
        alloc.free(bytes);
    };
    var response: Response = undefined;
    switch (request.operation) {
        .resolve => {
            const name = request.key orelse return error.InvalidArgument;
            if (!allowed(grant, name)) return error.Unauthorized;
            lookup = try source.resolve(alloc, cfg.scope, name, .{ .min_revision = request.min_revision });
            response = .{ .revision = lookup.?.revision };
            if (lookup.?.value) |value| {
                hex = try alloc.alloc(u8, value.secret.bytes.len * 2);
                for (value.secret.bytes, 0..) |byte, i| {
                    hex.?[i * 2] = "0123456789abcdef"[byte >> 4];
                    hex.?[i * 2 + 1] = "0123456789abcdef"[byte & 15];
                }
                response.value_hex = hex;
                response.entry_revision = value.revision;
            }
        },
        .list => {
            listing = try source.listMetadata(alloc, cfg.scope, .{ .min_revision = request.min_revision });
            for (listing.?.entries) |entry| if (allowed(grant, entry.key)) {
                try entries.append(alloc, entry);
            };
            response = .{ .revision = listing.?.revision, .entries = entries.items };
        },
        .refresh => {
            const refreshed = try source.refresh(cfg.scope);
            if (refreshed.revision < request.min_revision) return error.Unavailable;
            response = .{ .revision = refreshed.revision };
        },
    }
    const encoded = try std.json.Stringify.valueAlloc(alloc, response, .{});
    defer {
        std.crypto.secureZero(u8, encoded);
        alloc.free(encoded);
    }
    if (encoded.len + 40 > max_response_bytes) return error.ResourceRequestTooLarge;
    return seal(alloc, store.io, key, &responseAad(body), encoded);
}

pub const Remote = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    scope: []const u8,
    config: secrets.Config.Native.Reader,
    executor: http.RequestExecutor,
    internal_service: ?auth.Config = null,
    pub fn source(self: *Remote) contract.Source {
        return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .list_metadata = list, .refresh = refresh } };
    }
    fn request(self: *Remote, alloc: std.mem.Allocator, req: Request) !std.json.Parsed(Response) {
        if (!std.mem.eql(u8, req.scope, self.scope)) return error.Unauthorized;
        var key = try credential(alloc, self.io, self.config.credential_path);
        defer std.crypto.secureZero(u8, &key);
        const json = try std.json.Stringify.valueAlloc(alloc, req, .{});
        defer alloc.free(json);
        const body = try seal(alloc, self.io, key, request_domain, json);
        defer alloc.free(body);
        // Every attempt is a fresh authoritative read; never reuse cached absence
        // or forward a credential to a redirect chosen by an untrusted peer.
        for (self.config.urls) |base| {
            const uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ std.mem.trimEnd(u8, base, "/"), path });
            defer alloc.free(uri);
            var response = auth.executeRequest(alloc, self.executor, .{ .method = .POST, .uri = uri, .body = body, .content_type = "application/octet-stream", .headers = &.{.{ .name = "X-Antfly-Secret-Grant", .value = self.config.name }}, .timeout_ms = 2000 }, self.internal_service) catch continue;
            defer response.deinit(alloc);
            if (response.status == 401 or response.status == 403) return error.Unauthorized;
            if (response.status != 200) continue;
            const plain = try open(alloc, key, &responseAad(body), response.body, max_response_bytes);
            defer {
                std.crypto.secureZero(u8, plain);
                alloc.free(plain);
            }
            var parsed = try std.json.parseFromSlice(Response, alloc, plain, .{ .allocate = .alloc_always });
            errdefer destroyResponse(&parsed);
            if (parsed.value.revision < req.min_revision) return error.Unavailable;
            return parsed;
        }
        return error.Unavailable;
    }
    fn destroyResponse(parsed: *std.json.Parsed(Response)) void {
        if (parsed.value.value_hex) |hex| std.crypto.secureZero(u8, @constCast(hex));
        parsed.deinit();
    }
    fn resolve(ptr: *anyopaque, alloc: std.mem.Allocator, scope: []const u8, name: []const u8, opts: contract.ReadOptions) !contract.Lookup {
        const self: *Remote = @ptrCast(@alignCast(ptr));
        var parsed = try self.request(alloc, .{ .operation = .resolve, .scope = scope, .key = name, .min_revision = opts.min_revision });
        defer destroyResponse(&parsed);
        const response = parsed.value;
        var result = contract.Lookup{ .revision = response.revision };
        if (response.value_hex) |hex| {
            const revision = response.entry_revision orelse return error.CorruptInput;
            if (revision == 0 or revision > response.revision or hex.len % 2 != 0 or hex.len / 2 > contract.max_value_bytes) return error.CorruptInput;
            const value = try alloc.alloc(u8, hex.len / 2);
            errdefer {
                std.crypto.secureZero(u8, value);
                alloc.free(value);
            }
            _ = std.fmt.hexToBytes(value, hex) catch return error.CorruptInput;
            result.value = .{ .revision = revision, .secret = .{ .bytes = value } };
        } else if (response.entry_revision != null) return error.CorruptInput;
        return result;
    }
    fn list(ptr: *anyopaque, alloc: std.mem.Allocator, scope: []const u8, opts: contract.ReadOptions) !contract.Listing {
        const self: *Remote = @ptrCast(@alignCast(ptr));
        var parsed = try self.request(alloc, .{ .operation = .list, .scope = scope, .min_revision = opts.min_revision });
        defer destroyResponse(&parsed);
        const entries = try alloc.alloc(contract.Metadata, parsed.value.entries.len);
        var initialized: usize = 0;
        errdefer {
            for (entries[0..initialized]) |entry| alloc.free(entry.key);
            alloc.free(entries);
        }
        for (parsed.value.entries, 0..) |entry, i| {
            entries[i] = .{ .key = try alloc.dupe(u8, entry.key), .revision = entry.revision };
            initialized += 1;
        }
        return .{ .revision = parsed.value.revision, .entries = entries };
    }
    fn refresh(ptr: *anyopaque, scope: []const u8) !contract.Health {
        const self: *Remote = @ptrCast(@alignCast(ptr));
        var parsed = try self.request(self.alloc, .{ .operation = .refresh, .scope = scope });
        defer destroyResponse(&parsed);
        return .{ .revision = parsed.value.revision };
    }
};
