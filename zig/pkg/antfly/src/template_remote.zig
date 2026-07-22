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
const httpx = @import("httpx");
const hbs = @import("handlebars");
const template_mod = @import("template.zig");
const pdf_mod = @import("antfly_pdf");
const scraping = @import("antfly_scraping");
const common_secrets = @import("common/secrets.zig");

const Allocator = std.mem.Allocator;

pub const RenderError = error{
    PermanentPromptFailure,
    TransientPromptFailure,
};

const RenderContext = struct {
    alloc: Allocator,
    pdf_backend: pdf_mod.Backend,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    secret_store: ?*common_secrets.FileStore = null,
};

pub const RenderConfig = struct {
    pdf_backend: pdf_mod.Backend = pdf_mod.Backend.system(),
    remote_content: ?*const scraping.RemoteContentConfig = null,
    secret_store: ?*common_secrets.FileStore = null,
};

pub const default_remote_fetch_max_download_size_bytes: u64 = 100 * 1024 * 1024;

const remote_fetch_security = scraping.ContentSecurityConfig{
    .block_private_ips = true,
    .max_download_size_bytes = default_remote_fetch_max_download_size_bytes,
};

threadlocal var active_render_context: ?*RenderContext = null;

pub fn renderJsonToText(
    alloc: Allocator,
    template_source: []const u8,
    json_doc: []const u8,
) ![]const u8 {
    return try renderJsonToTextWithConfig(alloc, template_source, json_doc, .{});
}

pub fn renderJsonToTextWithConfig(
    alloc: Allocator,
    template_source: []const u8,
    json_doc: []const u8,
    config: RenderConfig,
) ![]const u8 {
    var helper_arena_state = std.heap.ArenaAllocator.init(alloc);
    defer helper_arena_state.deinit();
    const helper_arena = helper_arena_state.allocator();

    var extra_helpers: hbs.HelperMap = .{};
    try extra_helpers.put(helper_arena, "remoteMedia", hbs.Helper.from(&remoteMediaHelper));
    try extra_helpers.put(helper_arena, "remotePDF", hbs.Helper.from(&remotePdfHelper));
    try extra_helpers.put(helper_arena, "remoteText", hbs.Helper.from(&remoteTextHelper));

    var render_ctx = RenderContext{
        .alloc = alloc,
        .pdf_backend = config.pdf_backend,
        .remote_content = config.remote_content,
        .secret_store = config.secret_store,
    };
    const prev_ctx = active_render_context;
    active_render_context = &render_ctx;
    defer active_render_context = prev_ctx;

    return try template_mod.renderDocumentWithHelpers(alloc, template_source, json_doc, &extra_helpers);
}

pub fn renderJsonToValidatedTextWithConfig(
    alloc: Allocator,
    template_source: []const u8,
    json_doc: []const u8,
    config: RenderConfig,
) ![]const u8 {
    const rendered = try renderJsonToTextWithConfig(alloc, template_source, json_doc, config);
    errdefer alloc.free(rendered);
    try validateRenderedTemplate(alloc, rendered);
    return rendered;
}

pub fn renderJsonToParts(
    alloc: Allocator,
    template_source: []const u8,
    json_doc: []const u8,
) ![]template_mod.ContentPart {
    return try renderJsonToPartsWithConfig(alloc, template_source, json_doc, .{});
}

pub fn renderJsonToPartsWithConfig(
    alloc: Allocator,
    template_source: []const u8,
    json_doc: []const u8,
    config: RenderConfig,
) ![]template_mod.ContentPart {
    const rendered = try renderJsonToTextWithConfig(alloc, template_source, json_doc, config);
    defer alloc.free(rendered);
    try validateRenderedTemplate(alloc, rendered);
    return try template_mod.textToParts(alloc, rendered);
}

fn validateRenderedTemplate(alloc: Allocator, rendered: []const u8) !void {
    const directives = try template_mod.parseErrorDirectives(alloc, rendered);
    defer template_mod.freeErrorDirectives(alloc, directives);
    if (directives.len == 0) return;
    if (directives[0].isPermanent()) return RenderError.PermanentPromptFailure;
    return RenderError.TransientPromptFailure;
}

fn remoteMediaHelper(ctx: hbs.HelperContext) anyerror!hbs.Value {
    const url = ctx.hash.get("url") orelse return .{ .safe_string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .safe_string = "" },
    };
    if (url_str.len == 0) return .{ .safe_string = "" };

    const mode = if (ctx.hash.get("mode")) |value| switch (value) {
        .string => |s| s,
        else => "raw",
    } else "raw";
    if (std.mem.startsWith(u8, url_str, "data:")) {
        const result = try std.fmt.allocPrint(ctx.arena, "<<<dotprompt:media:url {s}>>>", .{url_str});
        return .{ .safe_string = result };
    }

    const render_ctx = active_render_context orelse {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remoteMedia missing HTTP context");
        return .{ .safe_string = result };
    };

    const fetched = downloadRemoteContentOutcomeAlloc(render_ctx, url_str, credentialName(ctx)) catch |err| {
        const result = try formatRemoteFetchErrorDirective(ctx.arena, err);
        return .{ .safe_string = result };
    };
    if (fetched == .http_error) {
        const result = try template_mod.formatErrorDirective(ctx.arena, fetched.http_error.status, fetched.http_error.message);
        return .{ .safe_string = result };
    }
    defer if (fetched == .ok) {
        var response = fetched.ok;
        response.deinit(render_ctx.alloc);
    };

    const response = fetched.ok;
    const is_pdf = std.mem.eql(u8, response.content_type, "application/pdf");
    if (is_pdf and std.mem.eql(u8, mode, "extract")) {
        const extracted = render_ctx.pdf_backend.extractText(render_ctx.alloc, response.data) catch |err| {
            const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
            return .{ .safe_string = result };
        };
        defer render_ctx.alloc.free(extracted);
        return .{ .string = try ctx.arena.dupe(u8, extracted) };
    }
    if (is_pdf and std.mem.eql(u8, mode, "render")) {
        const png_bytes = render_ctx.pdf_backend.renderFirstPagePng(render_ctx.alloc, response.data) catch |err| {
            const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
            return .{ .safe_string = result };
        };
        defer render_ctx.alloc.free(png_bytes);

        const encoded_len = std.base64.standard.Encoder.calcSize(png_bytes.len);
        const encoded = try ctx.arena.alloc(u8, encoded_len);
        _ = std.base64.standard.Encoder.encode(encoded, png_bytes);
        const result = try std.fmt.allocPrint(ctx.arena, "<<<dotprompt:media:url data:image/png;base64,{s}>>>", .{encoded});
        return .{ .safe_string = result };
    }

    const encoded_len = std.base64.standard.Encoder.calcSize(response.data.len);
    const encoded = try ctx.arena.alloc(u8, encoded_len);
    _ = std.base64.standard.Encoder.encode(encoded, response.data);

    const result = try std.fmt.allocPrint(ctx.arena, "<<<dotprompt:media:url data:{s};base64,{s}>>>", .{
        response.content_type,
        encoded,
    });
    return .{ .safe_string = result };
}

fn remoteTextHelper(ctx: hbs.HelperContext) anyerror!hbs.Value {
    const url = ctx.hash.get("url") orelse return .{ .string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .string = "" },
    };
    if (url_str.len == 0) return .{ .string = "" };

    const render_ctx = active_render_context orelse {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remoteText missing HTTP context");
        return .{ .safe_string = result };
    };

    const fetched = downloadRemoteContentOutcomeAlloc(render_ctx, url_str, credentialName(ctx)) catch |err| {
        const result = try formatRemoteFetchErrorDirective(ctx.arena, err);
        return .{ .safe_string = result };
    };
    if (fetched == .http_error) {
        const result = try template_mod.formatErrorDirective(ctx.arena, fetched.http_error.status, fetched.http_error.message);
        return .{ .safe_string = result };
    }
    defer if (fetched == .ok) {
        var response = fetched.ok;
        response.deinit(render_ctx.alloc);
    };

    const response = fetched.ok;
    if (!std.mem.startsWith(u8, response.content_type, "text/")) {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remoteText requires a text/* response");
        return .{ .safe_string = result };
    }

    const text_copy = try ctx.arena.dupe(u8, response.data);
    return .{ .string = text_copy };
}

/// Deprecated compatibility helper. Prefer document_extraction for durable PDF
/// ingestion or remoteMedia for template-time multimodal inference input.
fn remotePdfHelper(ctx: hbs.HelperContext) anyerror!hbs.Value {
    const url = ctx.hash.get("url") orelse return .{ .safe_string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .safe_string = "" },
    };
    if (url_str.len == 0) return .{ .safe_string = "" };

    const render_ctx = active_render_context orelse {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remotePDF missing HTTP context");
        return .{ .safe_string = result };
    };

    const fetched = downloadRemoteContentOutcomeAlloc(render_ctx, url_str, credentialName(ctx)) catch |err| {
        const result = try formatRemoteFetchErrorDirective(ctx.arena, err);
        return .{ .safe_string = result };
    };
    if (fetched == .http_error) {
        const result = try template_mod.formatErrorDirective(ctx.arena, fetched.http_error.status, fetched.http_error.message);
        return .{ .safe_string = result };
    }
    defer if (fetched == .ok) {
        var response = fetched.ok;
        response.deinit(render_ctx.alloc);
    };

    const response = fetched.ok;
    if (std.mem.startsWith(u8, response.content_type, "text/")) {
        const text_copy = try ctx.arena.dupe(u8, response.data);
        return .{ .string = text_copy };
    }

    if (std.mem.eql(u8, response.content_type, "application/pdf")) {
        const extracted = render_ctx.pdf_backend.extractText(render_ctx.alloc, response.data) catch |err| {
            const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
            return .{ .safe_string = result };
        };
        defer render_ctx.alloc.free(extracted);
        return .{ .string = try ctx.arena.dupe(u8, extracted) };
    }

    const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remotePDF requires an application/pdf response");
    return .{ .safe_string = result };
}

fn credentialName(ctx: hbs.HelperContext) ?[]const u8 {
    const value = ctx.hash.get("credentials") orelse return null;
    return switch (value) {
        .string => |text| if (text.len > 0) text else null,
        else => null,
    };
}

fn formatRemoteFetchErrorDirective(alloc: Allocator, err: anyerror) ![]const u8 {
    return switch (err) {
        error.StreamTooLong => try template_mod.formatErrorDirective(alloc, 413, @errorName(err)),
        else => try template_mod.formatErrorDirective(alloc, 0, @errorName(err)),
    };
}

fn downloadRemoteContentOutcomeAlloc(
    render_ctx: *RenderContext,
    url: []const u8,
    credential_name: ?[]const u8,
) !scraping.DownloadOutcome {
    var snapshot = if (render_ctx.remote_content) |remote_content| remote_content.acquire() else null;
    defer if (snapshot) |*held| held.deinit();
    const remote_content = if (snapshot) |*held| held.config else null;
    var resolved = try resolveRemoteContentFetchOptions(render_ctx.alloc, remote_content, render_ctx.secret_store, url, credential_name);
    defer resolved.deinit(render_ctx.alloc);
    return try scraping.downloadContentOutcomeAllocWithHeaders(
        render_ctx.alloc,
        url,
        &resolved.security,
        if (resolved.s3_credentials) |*creds| creds else null,
        resolved.http_headers,
    );
}

pub fn downloadRemoteContentOutcomeAllocWithConfig(
    alloc: Allocator,
    remote_content: ?*const scraping.RemoteContentConfig,
    secret_store: ?*common_secrets.FileStore,
    url: []const u8,
    credential_name: ?[]const u8,
) !scraping.DownloadOutcome {
    return try downloadRemoteContentOutcomeAllocWithExecutor(
        alloc,
        remote_content,
        secret_store,
        url,
        credential_name,
        scraping.downloadContentOutcomeAllocWithHeaders,
    );
}

const RemoteContentDownloadExecutor = *const fn (
    Allocator,
    []const u8,
    ?*const scraping.ContentSecurityConfig,
    ?*const scraping.S3CredentialsConfig,
    ?[]const scraping.HTTPHeader,
) anyerror!scraping.DownloadOutcome;

fn downloadRemoteContentOutcomeAllocWithExecutor(
    alloc: Allocator,
    remote_content: ?*const scraping.RemoteContentConfig,
    secret_store: ?*common_secrets.FileStore,
    url: []const u8,
    credential_name: ?[]const u8,
    executor: RemoteContentDownloadExecutor,
) !scraping.DownloadOutcome {
    var snapshot = if (remote_content) |configured| configured.acquire() else null;
    defer if (snapshot) |*held| held.deinit();
    const effective_remote_content = if (snapshot) |*held| held.config else null;
    var resolved = try resolveRemoteContentFetchOptions(alloc, effective_remote_content, secret_store, url, credential_name);
    defer resolved.deinit(alloc);
    return try executor(
        alloc,
        url,
        &resolved.security,
        if (resolved.s3_credentials) |*creds| creds else null,
        resolved.http_headers,
    );
}

const ResolvedRemoteContentFetchOptions = struct {
    security: scraping.ContentSecurityConfig = remote_fetch_security,
    s3_credentials: ?scraping.S3CredentialsConfig = null,
    http_headers: ?[]scraping.HTTPHeader = null,

    fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.s3_credentials) |*creds| creds.deinit(alloc);
        if (self.http_headers) |headers| {
            for (headers) |header| {
                alloc.free(@constCast(header.name));
                alloc.free(@constCast(header.value));
            }
            alloc.free(headers);
        }
        self.* = undefined;
    }
};

fn resolveRemoteContentFetchOptions(
    alloc: Allocator,
    remote_content: ?*const scraping.RemoteContentConfig,
    secret_store: ?*common_secrets.FileStore,
    url: []const u8,
    credential_name: ?[]const u8,
) !ResolvedRemoteContentFetchOptions {
    const cfg = remote_content orelse return .{};
    const parsed = std.Uri.parse(url) catch return .{ .security = effectiveRemoteContentSecurity(cfg, null) };
    if (isUriScheme(parsed, "s3")) {
        const credential = selectS3Credential(cfg, parsed, credential_name);
        return .{
            .security = effectiveRemoteContentSecurity(cfg, if (credential) |creds| creds.security else null),
            .s3_credentials = if (credential) |creds| try resolveS3Credential(alloc, secret_store, creds) else null,
        };
    }
    if (isUriScheme(parsed, "http") or isUriScheme(parsed, "https")) {
        const credential = selectHttpCredential(cfg, url, credential_name);
        return .{
            .security = effectiveRemoteContentSecurity(cfg, if (credential) |creds| creds.security else null),
            .http_headers = if (credential) |creds| try resolveHttpHeaders(alloc, secret_store, creds) else null,
        };
    }
    return .{ .security = effectiveRemoteContentSecurity(cfg, null) };
}

fn effectiveRemoteContentSecurity(
    cfg: *const scraping.RemoteContentConfig,
    credential_security: ?scraping.ContentSecurityConfig,
) scraping.ContentSecurityConfig {
    var effective = remote_fetch_security;
    if (cfg.security) |security| applyContentSecurityOverride(&effective, security);
    if (credential_security) |security| applyContentSecurityOverride(&effective, security);
    return effective;
}

fn applyContentSecurityOverride(
    effective: *scraping.ContentSecurityConfig,
    override: scraping.ContentSecurityConfig,
) void {
    if (override.allowed_hosts) |value| effective.allowed_hosts = value;
    if (override.block_private_ips) |value| effective.block_private_ips = value;
    if (override.max_download_size_bytes) |value| effective.max_download_size_bytes = value;
    if (override.download_timeout_seconds) |value| effective.download_timeout_seconds = value;
    if (override.max_image_dimension) |value| effective.max_image_dimension = value;
    if (override.allowed_paths) |value| effective.allowed_paths = value;
    if (override.user_agent) |value| effective.user_agent = value;
}

fn isUriScheme(parsed: std.Uri, scheme: []const u8) bool {
    return std.ascii.eqlIgnoreCase(parsed.scheme, scheme);
}

fn selectS3Credential(
    cfg: *const scraping.RemoteContentConfig,
    parsed: std.Uri,
    credential_name: ?[]const u8,
) ?*const scraping.S3CredentialConfig {
    if (credential_name) |name| return cfg.getS3(name);
    const bucket = (parsed.host orelse return null).percent_encoded;
    var it = cfg.s3.iterator();
    while (it.next()) |entry| {
        const credential = entry.value_ptr;
        const patterns = credential.buckets orelse continue;
        for (patterns) |pattern| {
            if (bucketPatternMatches(pattern, bucket)) return credential;
        }
    }
    if (cfg.default_s3) |name| return cfg.getS3(name);
    return null;
}

fn selectHttpCredential(
    cfg: *const scraping.RemoteContentConfig,
    url: []const u8,
    credential_name: ?[]const u8,
) ?*const scraping.HTTPCredentialConfig {
    if (credential_name) |name| return cfg.getHttp(name);
    var it = cfg.http.iterator();
    while (it.next()) |entry| {
        const credential = entry.value_ptr;
        const base_url = credential.base_url orelse continue;
        if (std.mem.startsWith(u8, url, base_url)) return credential;
    }
    return null;
}

fn resolveS3Credential(
    alloc: Allocator,
    secret_store: ?*common_secrets.FileStore,
    credential: *const scraping.S3CredentialConfig,
) !scraping.S3CredentialsConfig {
    return .{
        .endpoint = if (credential.endpoint) |value| try common_secrets.resolveReferenceOwned(alloc, secret_store, value) else null,
        .use_ssl = credential.use_ssl,
        .access_key_id = if (credential.access_key_id) |value|
            try common_secrets.resolveReferenceOwned(alloc, secret_store, value)
        else
            common_secrets.envValueOwned(alloc, "AWS_ACCESS_KEY_ID"),
        .secret_access_key = if (credential.secret_access_key) |value|
            try common_secrets.resolveReferenceOwned(alloc, secret_store, value)
        else
            common_secrets.envValueOwned(alloc, "AWS_SECRET_ACCESS_KEY"),
        .session_token = if (credential.session_token) |value|
            try common_secrets.resolveReferenceOwned(alloc, secret_store, value)
        else
            common_secrets.envValueOwned(alloc, "AWS_SESSION_TOKEN"),
    };
}

fn resolveHttpHeaders(
    alloc: Allocator,
    secret_store: ?*common_secrets.FileStore,
    credential: *const scraping.HTTPCredentialConfig,
) ![]scraping.HTTPHeader {
    var headers = try alloc.alloc(scraping.HTTPHeader, credential.headers.count());
    errdefer alloc.free(headers);
    var written: usize = 0;
    errdefer {
        for (headers[0..written]) |header| {
            alloc.free(@constCast(header.name));
            alloc.free(@constCast(header.value));
        }
    }
    var it = credential.headers.iterator();
    while (it.next()) |entry| {
        const name = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer alloc.free(name);
        const value = try common_secrets.resolveReferenceOwned(alloc, secret_store, entry.value_ptr.*);
        errdefer alloc.free(value);
        headers[written] = .{
            .name = name,
            .value = value,
        };
        written += 1;
    }
    return headers;
}

fn bucketPatternMatches(pattern: []const u8, bucket: []const u8) bool {
    if (std.mem.eql(u8, pattern, "*")) return true;
    const star = std.mem.indexOfScalar(u8, pattern, '*') orelse return std.mem.eql(u8, pattern, bucket);
    const prefix = pattern[0..star];
    const suffix = pattern[star + 1 ..];
    return std.mem.startsWith(u8, bucket, prefix) and std.mem.endsWith(u8, bucket, suffix);
}

test "template remote does not apply s3 credentials to http urls" {
    const alloc = std.testing.allocator;

    var cfg = scraping.RemoteContentConfig{};
    defer cfg.deinit(alloc);
    cfg.default_s3 = try alloc.dupe(u8, "primary");

    {
        const key = try alloc.dupe(u8, "primary");
        errdefer alloc.free(key);
        const endpoint = try alloc.dupe(u8, "s3.amazonaws.com");
        errdefer alloc.free(endpoint);
        const access_key_id = try alloc.dupe(u8, "primary-key");
        errdefer alloc.free(access_key_id);
        const secret_access_key = try alloc.dupe(u8, "primary-secret");
        errdefer alloc.free(secret_access_key);

        try cfg.s3.put(alloc, key, .{
            .endpoint = endpoint,
            .access_key_id = access_key_id,
            .secret_access_key = secret_access_key,
        });
    }

    var resolved = try resolveRemoteContentFetchOptions(
        alloc,
        &cfg,
        null,
        "https://raw.githubusercontent.com/antflydb/antfly/HEAD/README.md",
        null,
    );
    defer resolved.deinit(alloc);

    try std.testing.expect(resolved.s3_credentials == null);
    try std.testing.expectEqual(@as(?bool, true), resolved.security.block_private_ips);
}

test "template remote S3 credentials observe same-length secret rotation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/secrets.json", .{tmp.sub_path});
    defer alloc.free(store_path);

    var store = try common_secrets.FileStore.init(alloc, store_path);
    defer store.deinit();
    var first_access = try store.put(alloc, "s3.access", "ACCESS-ONE");
    defer first_access.deinit(alloc);
    var first_secret = try store.put(alloc, "s3.secret", "SECRET-ONE");
    defer first_secret.deinit(alloc);

    var cfg = scraping.RemoteContentConfig{};
    defer cfg.deinit(alloc);
    cfg.default_s3 = try alloc.dupe(u8, "primary");
    try cfg.s3.put(alloc, try alloc.dupe(u8, "primary"), .{
        .access_key_id = try alloc.dupe(u8, "${secret:s3.access}"),
        .secret_access_key = try alloc.dupe(u8, "${secret:s3.secret}"),
    });

    var first = try resolveRemoteContentFetchOptions(alloc, &cfg, &store, "s3://bucket/document.pdf", null);
    defer first.deinit(alloc);
    try std.testing.expectEqualStrings("ACCESS-ONE", first.s3_credentials.?.access_key_id.?);
    try std.testing.expectEqualStrings("SECRET-ONE", first.s3_credentials.?.secret_access_key.?);

    var rotated_access = try store.put(alloc, "s3.access", "ACCESS-TWO");
    defer rotated_access.deinit(alloc);
    var rotated_secret = try store.put(alloc, "s3.secret", "SECRET-TWO");
    defer rotated_secret.deinit(alloc);

    var rotated = try resolveRemoteContentFetchOptions(alloc, &cfg, &store, "s3://bucket/document.pdf", null);
    defer rotated.deinit(alloc);
    try std.testing.expectEqualStrings("ACCESS-TWO", rotated.s3_credentials.?.access_key_id.?);
    try std.testing.expectEqualStrings("SECRET-TWO", rotated.s3_credentials.?.secret_access_key.?);
}

test "template remote S3 credentials fall back to standard AWS environment" {
    if (!@import("builtin").link_libc) return error.SkipZigTest;
    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };
    const alloc = std.testing.allocator;
    const old_access = common_secrets.envValueOwned(alloc, "AWS_ACCESS_KEY_ID");
    defer if (old_access) |value| alloc.free(value);
    const old_secret = common_secrets.envValueOwned(alloc, "AWS_SECRET_ACCESS_KEY");
    defer if (old_secret) |value| alloc.free(value);
    const old_access_z = if (old_access) |value| try alloc.dupeZ(u8, value) else null;
    defer if (old_access_z) |value| alloc.free(value);
    const old_secret_z = if (old_secret) |value| try alloc.dupeZ(u8, value) else null;
    defer if (old_secret_z) |value| alloc.free(value);
    defer {
        if (old_access_z) |value| {
            _ = c.setenv("AWS_ACCESS_KEY_ID", value.ptr, 1);
        } else {
            _ = c.unsetenv("AWS_ACCESS_KEY_ID");
        }
        if (old_secret_z) |value| {
            _ = c.setenv("AWS_SECRET_ACCESS_KEY", value.ptr, 1);
        } else {
            _ = c.unsetenv("AWS_SECRET_ACCESS_KEY");
        }
    }
    try std.testing.expectEqual(@as(c_int, 0), c.setenv("AWS_ACCESS_KEY_ID", "ENV-ACCESS", 1));
    try std.testing.expectEqual(@as(c_int, 0), c.setenv("AWS_SECRET_ACCESS_KEY", "ENV-SECRET", 1));

    var cfg = scraping.RemoteContentConfig{};
    defer cfg.deinit(alloc);
    cfg.default_s3 = try alloc.dupe(u8, "primary");
    try cfg.s3.put(alloc, try alloc.dupe(u8, "primary"), .{});

    var resolved = try resolveRemoteContentFetchOptions(alloc, &cfg, null, "s3://bucket/document.pdf", null);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("ENV-ACCESS", resolved.s3_credentials.?.access_key_id.?);
    try std.testing.expectEqualStrings("ENV-SECRET", resolved.s3_credentials.?.secret_access_key.?);
}

test "template remote S3 fetch passes rotated credentials to downloader" {
    const alloc = std.testing.allocator;
    const FakeDownloader = struct {
        var request_count: usize = 0;

        fn execute(
            a: Allocator,
            _: []const u8,
            _: ?*const scraping.ContentSecurityConfig,
            credentials: ?*const scraping.S3CredentialsConfig,
            _: ?[]const scraping.HTTPHeader,
        ) !scraping.DownloadOutcome {
            const expected_access, const expected_secret = switch (request_count) {
                0 => .{ "ACCESS-ONE", "SECRET-ONE" },
                1 => .{ "ACCESS-TWO", "SECRET-TWO" },
                else => return error.TestUnexpectedResult,
            };
            request_count += 1;
            const resolved = credentials orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualStrings(expected_access, resolved.access_key_id.?);
            try std.testing.expectEqualStrings(expected_secret, resolved.secret_access_key.?);
            return .{ .ok = .{
                .content_type = try a.dupe(u8, "text/plain"),
                .data = try a.dupe(u8, "rotated-object"),
            } };
        }
    };
    FakeDownloader.request_count = 0;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/secrets.json", .{tmp.sub_path});
    defer alloc.free(store_path);
    var store = try common_secrets.FileStore.init(alloc, store_path);
    defer store.deinit();
    var first_access = try store.put(alloc, "s3.access", "ACCESS-ONE");
    defer first_access.deinit(alloc);
    var first_secret = try store.put(alloc, "s3.secret", "SECRET-ONE");
    defer first_secret.deinit(alloc);

    var cfg = scraping.RemoteContentConfig{};
    defer cfg.deinit(alloc);
    cfg.default_s3 = try alloc.dupe(u8, "primary");
    try cfg.s3.put(alloc, try alloc.dupe(u8, "primary"), .{
        .endpoint = try alloc.dupe(u8, "s3.example.invalid"),
        .use_ssl = false,
        .access_key_id = try alloc.dupe(u8, "${secret:s3.access}"),
        .secret_access_key = try alloc.dupe(u8, "${secret:s3.secret}"),
    });

    var first = try downloadRemoteContentOutcomeAllocWithExecutor(alloc, &cfg, &store, "s3://bucket/document.txt", null, FakeDownloader.execute);
    defer if (first == .ok) first.ok.deinit(alloc);
    try std.testing.expect(first == .ok);
    try std.testing.expectEqualStrings("rotated-object", first.ok.data);

    var rotated_access = try store.put(alloc, "s3.access", "ACCESS-TWO");
    defer rotated_access.deinit(alloc);
    var rotated_secret = try store.put(alloc, "s3.secret", "SECRET-TWO");
    defer rotated_secret.deinit(alloc);

    var second = try downloadRemoteContentOutcomeAllocWithExecutor(alloc, &cfg, &store, "s3://bucket/document.txt", null, FakeDownloader.execute);
    defer if (second == .ok) second.ok.deinit(alloc);
    try std.testing.expect(second == .ok);
    try std.testing.expectEqualStrings("rotated-object", second.ok.data);
    try std.testing.expectEqual(@as(usize, 2), FakeDownloader.request_count);
}

test "template remote applies remote content security to http urls" {
    const alloc = std.testing.allocator;

    var cfg = scraping.RemoteContentConfig{
        .security = .{ .block_private_ips = false },
    };
    defer cfg.deinit(alloc);

    var resolved = try resolveRemoteContentFetchOptions(
        alloc,
        &cfg,
        null,
        "http://127.0.0.1:8080/doc.txt",
        null,
    );
    defer resolved.deinit(alloc);

    try std.testing.expectEqual(@as(?bool, false), resolved.security.block_private_ips);
    try std.testing.expectEqual(@as(?u64, default_remote_fetch_max_download_size_bytes), resolved.security.max_download_size_bytes);
}

test "template remote preserves PDF content type across a multi-megabyte download" {
    const alloc = std.testing.allocator;

    const FakePdfBackend = struct {
        fn extract(_: *const anyopaque, a: Allocator, _: []const u8) ![]u8 {
            return try a.dupe(u8, "pdf extracted text");
        }

        fn render(_: *const anyopaque, a: Allocator, _: []const u8) ![]u8 {
            return try a.dupe(u8, "png-bytes");
        }
    };

    const FakeApp = struct {
        fn executor() httpx.RequestExecutor {
            unreachable;
        }
    };
    _ = FakeApp;

    const ListenerApp = struct {
        fn executor() @import("raft/transport/http_common.zig").RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, req_alloc: Allocator, req: @import("raft/transport/http_common.zig").HttpRequest) !@import("raft/transport/http_common.zig").HttpResponse {
            try std.testing.expectEqual(@import("raft/transport/http_common.zig").Method.GET, req.method);
            if (std.mem.endsWith(u8, req.uri, "/doc.pdf")) {
                const body = try req_alloc.alloc(u8, 3_062_180);
                @memset(body, 'x');
                return .{
                    .status = 200,
                    .content_type = try req_alloc.dupe(u8, "application/pdf"),
                    .body = body,
                };
            }
            return .{
                .status = 404,
                .content_type = try req_alloc.dupe(u8, "application/pdf"),
                .body = try req_alloc.dupe(u8, "missing"),
            };
        }
    };

    var listener = @import("raft/transport/std_http_listener.zig").StdHttpListener.init(alloc, .{}, ListenerApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);
    const pdf_url = try std.fmt.allocPrint(alloc, "{s}/doc.pdf", .{base_uri});
    defer alloc.free(pdf_url);

    const json_doc = try std.fmt.allocPrint(alloc, "{{\"pdf_url\":{f}}}", .{std.json.fmt(pdf_url, .{})});
    defer alloc.free(json_doc);

    const backend = pdf_mod.Backend{
        .ptr = undefined,
        .extract_text_fn = FakePdfBackend.extract,
        .render_first_page_png_fn = FakePdfBackend.render,
    };
    var remote_content = scraping.RemoteContentConfig{
        .security = .{ .block_private_ips = false },
    };
    defer remote_content.deinit(alloc);

    const rendered = try renderJsonToTextWithConfig(alloc, "{{remotePDF url=pdf_url}}", json_doc, .{
        .pdf_backend = backend,
        .remote_content = &remote_content,
    });
    defer alloc.free(rendered);
    try std.testing.expectEqualStrings("pdf extracted text", rendered);
}

test "template remote renders remoteMedia pdf mode=render with injected pdf backend" {
    const alloc = std.testing.allocator;

    const FakePdfBackend = struct {
        fn extract(_: *const anyopaque, a: Allocator, _: []const u8) ![]u8 {
            return try a.dupe(u8, "pdf extracted text");
        }

        fn render(_: *const anyopaque, a: Allocator, _: []const u8) ![]u8 {
            return try a.dupe(u8, &.{ 1, 2, 3, 4 });
        }
    };

    const ListenerApp = struct {
        fn executor() @import("raft/transport/http_common.zig").RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, req_alloc: Allocator, req: @import("raft/transport/http_common.zig").HttpRequest) !@import("raft/transport/http_common.zig").HttpResponse {
            try std.testing.expectEqual(@import("raft/transport/http_common.zig").Method.GET, req.method);
            return .{
                .status = 200,
                .content_type = try req_alloc.dupe(u8, "application/pdf"),
                .body = try req_alloc.dupe(u8, "%PDF-fake"),
            };
        }
    };

    var listener = @import("raft/transport/std_http_listener.zig").StdHttpListener.init(alloc, .{}, ListenerApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);
    const pdf_url = try std.fmt.allocPrint(alloc, "{s}/doc.pdf", .{base_uri});
    defer alloc.free(pdf_url);

    const json_doc = try std.fmt.allocPrint(alloc, "{{\"pdf_url\":{f}}}", .{std.json.fmt(pdf_url, .{})});
    defer alloc.free(json_doc);

    const backend = pdf_mod.Backend{
        .ptr = undefined,
        .extract_text_fn = FakePdfBackend.extract,
        .render_first_page_png_fn = FakePdfBackend.render,
    };
    var remote_content = scraping.RemoteContentConfig{
        .security = .{ .block_private_ips = false },
    };
    defer remote_content.deinit(alloc);

    const parts = try renderJsonToPartsWithConfig(alloc, "{{remoteMedia url=pdf_url mode=\"render\"}}", json_doc, .{
        .pdf_backend = backend,
        .remote_content = &remote_content,
    });
    defer template_mod.freeContentParts(alloc, parts);

    try std.testing.expectEqual(@as(usize, 1), parts.len);
    switch (parts[0]) {
        .binary => |binary| {
            try std.testing.expectEqualStrings("image/png", binary.mime_type);
            try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3, 4 }, binary.data);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "template remote preserves http status from shared scraping fetches" {
    const alloc = std.testing.allocator;

    const ListenerApp = struct {
        fn executor() @import("raft/transport/http_common.zig").RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, req_alloc: Allocator, req: @import("raft/transport/http_common.zig").HttpRequest) !@import("raft/transport/http_common.zig").HttpResponse {
            try std.testing.expectEqual(@import("raft/transport/http_common.zig").Method.GET, req.method);
            return .{
                .status = 404,
                .content_type = try req_alloc.dupe(u8, "text/plain"),
                .body = try req_alloc.dupe(u8, "missing"),
            };
        }
    };

    var listener = @import("raft/transport/std_http_listener.zig").StdHttpListener.init(alloc, .{}, ListenerApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);
    const missing_url = try std.fmt.allocPrint(alloc, "{s}/missing.txt", .{base_uri});
    defer alloc.free(missing_url);

    const json_doc = try std.fmt.allocPrint(alloc, "{{\"missing_url\":{f}}}", .{std.json.fmt(missing_url, .{})});
    defer alloc.free(json_doc);
    var remote_content = scraping.RemoteContentConfig{
        .security = .{ .block_private_ips = false },
    };
    defer remote_content.deinit(alloc);

    const rendered = try renderJsonToTextWithConfig(alloc, "{{remoteText url=missing_url}}", json_doc, .{
        .remote_content = &remote_content,
    });
    defer alloc.free(rendered);

    const directives = try template_mod.parseErrorDirectives(alloc, rendered);
    defer template_mod.freeErrorDirectives(alloc, directives);

    try std.testing.expectEqual(@as(usize, 1), directives.len);
    try std.testing.expectEqual(@as(u16, 404), directives[0].status);
    try std.testing.expectEqualStrings("remote fetch failed", directives[0].message);
}

test "template remote validated text rejects oversized remote media directive" {
    const alloc = std.testing.allocator;

    const ListenerApp = struct {
        fn executor() @import("raft/transport/http_common.zig").RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, req_alloc: Allocator, req: @import("raft/transport/http_common.zig").HttpRequest) !@import("raft/transport/http_common.zig").HttpResponse {
            try std.testing.expectEqual(@import("raft/transport/http_common.zig").Method.GET, req.method);
            return .{
                .status = 200,
                .content_type = try req_alloc.dupe(u8, "image/png"),
                .body = try req_alloc.dupe(u8, "0123456789abcdef"),
            };
        }
    };

    var listener = @import("raft/transport/std_http_listener.zig").StdHttpListener.init(alloc, .{}, ListenerApp.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);
    const photo_url = try std.fmt.allocPrint(alloc, "{s}/photo.png", .{base_uri});
    defer alloc.free(photo_url);

    const json_doc = try std.fmt.allocPrint(alloc, "{{\"photo\":{f}}}", .{std.json.fmt(photo_url, .{})});
    defer alloc.free(json_doc);
    var remote_content = scraping.RemoteContentConfig{
        .security = .{ .block_private_ips = false, .max_download_size_bytes = 4 },
    };
    defer remote_content.deinit(alloc);

    try std.testing.expectError(
        RenderError.PermanentPromptFailure,
        renderJsonToValidatedTextWithConfig(alloc, "{{remoteMedia url=photo}} fallback text", json_doc, .{
            .remote_content = &remote_content,
        }),
    );
}
