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
const platform_time = @import("antfly_platform").time;

const Allocator = std.mem.Allocator;

pub const RenderError = error{
    PermanentPromptFailure,
    TransientPromptFailure,
};

const FatalRenderError = error{
    Canceled,
    Timeout,
    OutOfMemory,
};

const RenderContext = struct {
    alloc: Allocator,
    pdf_backend: pdf_mod.Backend,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    secret_store: ?*common_secrets.FileStore = null,
    io: ?std.Io = null,
    deadline_ns: ?u64 = null,
    remote_bytes_remaining: u64,
    max_media_parts: ?usize = null,
    emitted_media_parts: usize = 0,
    fatal_error: ?FatalRenderError = null,
};

pub const RenderConfig = struct {
    pdf_backend: pdf_mod.Backend = pdf_mod.Backend.system(),
    remote_content: ?*const scraping.RemoteContentConfig = null,
    secret_store: ?*common_secrets.FileStore = null,
    /// Request-scoped I/O used by remote template helpers when provided.
    io: ?std.Io = null,
    /// Absolute monotonic request deadline shared by every remote helper.
    deadline_ns: ?u64 = null,
    max_media_parts: ?usize = null,
};

pub const default_remote_fetch_max_download_size_bytes: u64 = 100 * 1024 * 1024;

const remote_fetch_security = scraping.ContentSecurityConfig{
    .block_private_ips = true,
    .max_download_size_bytes = default_remote_fetch_max_download_size_bytes,
};

fn remoteByteBudget(remote_content: ?*const scraping.RemoteContentConfig) u64 {
    var snapshot = if (remote_content) |cfg| cfg.acquire() else null;
    defer if (snapshot) |*held| held.deinit();

    const configured = if (snapshot) |held|
        if (held.config.security) |security| security.max_download_size_bytes else null
    else
        null;
    return @min(default_remote_fetch_max_download_size_bytes, configured orelse default_remote_fetch_max_download_size_bytes);
}

test "template remote byte budget reads the reloadable config snapshot" {
    const Harness = struct {
        config: scraping.RemoteContentConfig,
        releases: usize = 0,

        fn acquire(context: *anyopaque) ?scraping.RemoteContentConfig.Snapshot {
            const self: *@This() = @ptrCast(@alignCast(context));
            return .{
                .config = &self.config,
                .context = context,
                .release_fn = release,
            };
        }

        fn release(context: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.releases += 1;
        }

        fn health(_: *anyopaque) scraping.RemoteContentConfig.RuntimeHealth {
            return .{
                .generation = 1,
                .hash = [_]u8{0} ** 32,
                .last_reload_failed = false,
                .stale_snapshot = false,
                .reload_successes = 0,
                .reload_failures = 0,
            };
        }
    };

    var harness = Harness{
        .config = .{ .security = .{ .max_download_size_bytes = 4096 } },
    };
    var facade = scraping.RemoteContentConfig{
        .security = .{ .max_download_size_bytes = 8192 },
        .runtime = .{
            .context = &harness,
            .acquire_fn = Harness.acquire,
            .health_fn = Harness.health,
        },
    };

    try std.testing.expectEqual(@as(u64, 4096), remoteByteBudget(&facade));
    try std.testing.expectEqual(@as(usize, 1), harness.releases);
}

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
    var render_ctx = RenderContext{
        .alloc = alloc,
        .pdf_backend = config.pdf_backend,
        .remote_content = config.remote_content,
        .secret_store = config.secret_store,
        .io = config.io,
        .deadline_ns = config.deadline_ns,
        .remote_bytes_remaining = remoteByteBudget(config.remote_content),
        .max_media_parts = config.max_media_parts,
    };
    try ensureRenderActive(&render_ctx);

    var helper_arena_state = std.heap.ArenaAllocator.init(alloc);
    defer helper_arena_state.deinit();
    const helper_arena = helper_arena_state.allocator();

    var extra_helpers: hbs.HelperMap = .{};
    try extra_helpers.put(helper_arena, "remoteMedia", hbs.Helper.withData(&remoteMediaHelper, @ptrCast(&render_ctx)));
    try extra_helpers.put(helper_arena, "remotePDF", hbs.Helper.withData(&remotePdfHelper, @ptrCast(&render_ctx)));
    try extra_helpers.put(helper_arena, "remoteText", hbs.Helper.withData(&remoteTextHelper, @ptrCast(&render_ctx)));

    const rendered = try template_mod.renderDocumentWithHelpers(alloc, template_source, json_doc, &extra_helpers);
    return try completeRenderedText(alloc, rendered, &render_ctx);
}

fn completeRenderedText(alloc: Allocator, rendered: []const u8, render_ctx: *const RenderContext) ![]const u8 {
    ensureRenderActive(render_ctx) catch |err| {
        alloc.free(rendered);
        return err;
    };
    return rendered;
}

fn ensureRenderActive(render_ctx: *const RenderContext) !void {
    if (render_ctx.fatal_error) |err| return err;
    if (render_ctx.io) |io| try io.checkCancel();
    if (render_ctx.deadline_ns) |deadline_ns| {
        if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
    }
}

fn latchFatalRenderError(render_ctx: *RenderContext, err: anyerror) bool {
    const fatal: FatalRenderError = switch (err) {
        error.Canceled => error.Canceled,
        error.Timeout => error.Timeout,
        error.OutOfMemory => error.OutOfMemory,
        else => return false,
    };
    if (render_ctx.fatal_error == null) render_ctx.fatal_error = fatal;
    return true;
}

fn beginRemoteHelper(ctx: hbs.HelperContext) !void {
    const render_ctx = renderContext(ctx) orelse return;
    ensureRenderActive(render_ctx) catch |err| {
        _ = latchFatalRenderError(render_ctx, err);
        return err;
    };
}

fn finishRemoteHelperError(ctx: hbs.HelperContext, err: anyerror) void {
    if (renderContext(ctx)) |render_ctx| _ = latchFatalRenderError(render_ctx, err);
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
    try beginRemoteHelper(ctx);
    return remoteMediaHelperImpl(ctx) catch |err| {
        finishRemoteHelperError(ctx, err);
        return err;
    };
}

fn remoteMediaHelperImpl(ctx: hbs.HelperContext) anyerror!hbs.Value {
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

    const render_ctx = renderContext(ctx) orelse {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remoteMedia missing HTTP context");
        return .{ .safe_string = result };
    };
    if (url_str.len >= "data:".len and std.ascii.eqlIgnoreCase(url_str[0.."data:".len], "data:")) {
        if (mediaPartLimitReached(render_ctx)) return .{ .safe_string = "" };
        const decoded_size = scraping.dataUriDecodedSize(url_str) catch |err| {
            if (latchFatalRenderError(render_ctx, err)) return err;
            const result = try formatRemoteFetchErrorDirective(ctx.arena, err);
            return .{ .safe_string = result };
        };
        chargeRemoteBytes(render_ctx, @intCast(decoded_size)) catch |err| {
            if (latchFatalRenderError(render_ctx, err)) return err;
            const result = try formatRemoteFetchErrorDirective(ctx.arena, err);
            return .{ .safe_string = result };
        };
        const result = try std.fmt.allocPrint(ctx.arena, "<<<dotprompt:media:url {s}>>>", .{url_str});
        noteEmittedMediaPart(render_ctx);
        return .{ .safe_string = result };
    }

    // `mode=extract` produces text for PDFs and therefore does not consume a
    // media part. Other modes can be rejected before I/O once the cap is hit.
    const extract_mode = std.mem.eql(u8, mode, "extract");
    if (!extract_mode and mediaPartLimitReached(render_ctx)) return .{ .safe_string = "" };

    const credential_name = credentialName(ctx) catch |err| {
        const result = try formatRemoteFetchErrorDirective(ctx.arena, err);
        return .{ .safe_string = result };
    };
    const fetched = downloadRemoteContentOutcomeAlloc(render_ctx, url_str, credential_name) catch |err| {
        if (latchFatalRenderError(render_ctx, err)) return err;
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
    if (is_pdf and extract_mode) {
        const extracted = render_ctx.pdf_backend.extractText(render_ctx.alloc, response.data) catch |err| {
            if (latchFatalRenderError(render_ctx, err)) return err;
            const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
            return .{ .safe_string = result };
        };
        defer render_ctx.alloc.free(extracted);
        return .{ .string = try ctx.arena.dupe(u8, extracted) };
    }
    if (mediaPartLimitReached(render_ctx)) return .{ .safe_string = "" };
    if (is_pdf and std.mem.eql(u8, mode, "render")) {
        const png_bytes = render_ctx.pdf_backend.renderFirstPagePng(render_ctx.alloc, response.data) catch |err| {
            if (latchFatalRenderError(render_ctx, err)) return err;
            const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
            return .{ .safe_string = result };
        };
        defer render_ctx.alloc.free(png_bytes);

        const encoded_len = std.base64.standard.Encoder.calcSize(png_bytes.len);
        const encoded = try ctx.arena.alloc(u8, encoded_len);
        _ = std.base64.standard.Encoder.encode(encoded, png_bytes);
        const result = try std.fmt.allocPrint(ctx.arena, "<<<dotprompt:media:url data:image/png;base64,{s}>>>", .{encoded});
        noteEmittedMediaPart(render_ctx);
        return .{ .safe_string = result };
    }

    const encoded_len = std.base64.standard.Encoder.calcSize(response.data.len);
    const encoded = try ctx.arena.alloc(u8, encoded_len);
    _ = std.base64.standard.Encoder.encode(encoded, response.data);

    const result = try std.fmt.allocPrint(ctx.arena, "<<<dotprompt:media:url data:{s};base64,{s}>>>", .{
        response.content_type,
        encoded,
    });
    noteEmittedMediaPart(render_ctx);
    return .{ .safe_string = result };
}

fn chargeRemoteBytes(render_ctx: *RenderContext, bytes: u64) !void {
    if (bytes > render_ctx.remote_bytes_remaining) return error.StreamTooLong;
    render_ctx.remote_bytes_remaining -= bytes;
}

fn mediaPartLimitReached(render_ctx: *const RenderContext) bool {
    const limit = render_ctx.max_media_parts orelse return false;
    return render_ctx.emitted_media_parts >= limit;
}

fn noteEmittedMediaPart(render_ctx: *RenderContext) void {
    if (render_ctx.max_media_parts != null) render_ctx.emitted_media_parts += 1;
}

fn renderContext(ctx: hbs.HelperContext) ?*RenderContext {
    const userdata = ctx.userdata orelse return null;
    return @ptrCast(@alignCast(userdata));
}

fn remoteTextHelper(ctx: hbs.HelperContext) anyerror!hbs.Value {
    try beginRemoteHelper(ctx);
    return remoteTextHelperImpl(ctx) catch |err| {
        finishRemoteHelperError(ctx, err);
        return err;
    };
}

fn remoteTextHelperImpl(ctx: hbs.HelperContext) anyerror!hbs.Value {
    const url = ctx.hash.get("url") orelse return .{ .string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .string = "" },
    };
    if (url_str.len == 0) return .{ .string = "" };

    const render_ctx = renderContext(ctx) orelse {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remoteText missing HTTP context");
        return .{ .safe_string = result };
    };

    const credential_name = credentialName(ctx) catch |err| {
        const result = try formatRemoteFetchErrorDirective(ctx.arena, err);
        return .{ .safe_string = result };
    };
    const fetched = downloadRemoteContentOutcomeAlloc(render_ctx, url_str, credential_name) catch |err| {
        if (latchFatalRenderError(render_ctx, err)) return err;
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
    try beginRemoteHelper(ctx);
    return remotePdfHelperImpl(ctx) catch |err| {
        finishRemoteHelperError(ctx, err);
        return err;
    };
}

fn remotePdfHelperImpl(ctx: hbs.HelperContext) anyerror!hbs.Value {
    const url = ctx.hash.get("url") orelse return .{ .safe_string = "" };
    const url_str = switch (url) {
        .string => |s| s,
        else => return .{ .safe_string = "" },
    };
    if (url_str.len == 0) return .{ .safe_string = "" };

    const render_ctx = renderContext(ctx) orelse {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remotePDF missing HTTP context");
        return .{ .safe_string = result };
    };

    const credential_name = credentialName(ctx) catch |err| {
        const result = try formatRemoteFetchErrorDirective(ctx.arena, err);
        return .{ .safe_string = result };
    };
    const fetched = downloadRemoteContentOutcomeAlloc(render_ctx, url_str, credential_name) catch |err| {
        if (latchFatalRenderError(render_ctx, err)) return err;
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
            if (latchFatalRenderError(render_ctx, err)) return err;
            const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
            return .{ .safe_string = result };
        };
        defer render_ctx.alloc.free(extracted);
        return .{ .string = try ctx.arena.dupe(u8, extracted) };
    }

    const result = try template_mod.formatErrorDirective(ctx.arena, 0, "remotePDF requires an application/pdf response");
    return .{ .safe_string = result };
}

fn credentialName(ctx: hbs.HelperContext) !?[]const u8 {
    const value = ctx.hash.get("credentials") orelse return null;
    return switch (value) {
        .string => |text| if (text.len > 0) text else error.InvalidCredentialName,
        else => error.InvalidCredentialName,
    };
}

fn formatRemoteFetchErrorDirective(alloc: Allocator, err: anyerror) ![]const u8 {
    return switch (err) {
        error.StreamTooLong => try template_mod.formatErrorDirective(alloc, 413, @errorName(err)),
        error.HttpCredentialNotFoundOrOutOfScope,
        error.S3CredentialNotFoundOrOutOfScope,
        error.InvalidCredentialName,
        => try template_mod.formatErrorDirective(alloc, 403, @errorName(err)),
        else => try template_mod.formatErrorDirective(alloc, 0, @errorName(err)),
    };
}

fn downloadRemoteContentOutcomeAlloc(
    render_ctx: *RenderContext,
    url: []const u8,
    credential_name: ?[]const u8,
) !scraping.DownloadOutcome {
    if (render_ctx.remote_bytes_remaining == 0) return error.StreamTooLong;
    var snapshot = if (render_ctx.remote_content) |remote_content| remote_content.acquire() else null;
    defer if (snapshot) |*held| held.deinit();
    const remote_content = if (snapshot) |*held| held.config else null;
    var resolved = try resolveRemoteContentFetchOptions(render_ctx.alloc, remote_content, render_ctx.secret_store, url, credential_name);
    defer resolved.deinit(render_ctx.alloc);
    const fetch_budget = @min(
        resolved.security.max_download_size_bytes orelse default_remote_fetch_max_download_size_bytes,
        render_ctx.remote_bytes_remaining,
    );
    resolved.security.max_download_size_bytes = fetch_budget;
    const outcome = (if (try remoteFetchDownloadContext(render_ctx, &resolved.security)) |download_context|
        scraping.downloadContentOutcomeAllocWithHeadersAndContext(
            render_ctx.alloc,
            download_context,
            url,
            &resolved.security,
            if (resolved.s3_credentials) |*creds| creds else null,
            resolved.http_headers,
        )
    else
        scraping.downloadContentOutcomeAllocWithHeaders(
            render_ctx.alloc,
            url,
            &resolved.security,
            if (resolved.s3_credentials) |*creds| creds else null,
            resolved.http_headers,
        )) catch |err| {
        switch (err) {
            error.StreamTooLong, error.ResponseTooLarge => render_ctx.remote_bytes_remaining = 0,
            else => {},
        }
        return err;
    };
    errdefer if (outcome == .ok) {
        var response = outcome.ok;
        response.deinit(render_ctx.alloc);
    };
    try chargeDownloadOutcome(render_ctx, outcome, fetch_budget);
    return outcome;
}

fn chargeDownloadOutcome(
    render_ctx: *RenderContext,
    outcome: scraping.DownloadOutcome,
    fetch_budget: u64,
) !void {
    const downloaded_bytes: u64 = switch (outcome) {
        .ok => |downloaded| @intCast(downloaded.data.len),
        .http_error => |http_error| http_error.downloaded_bytes,
    };
    if (downloaded_bytes > fetch_budget) {
        render_ctx.remote_bytes_remaining = 0;
        return error.StreamTooLong;
    }
    render_ctx.remote_bytes_remaining -= downloaded_bytes;
}

fn remoteFetchDownloadContext(
    render_ctx: *const RenderContext,
    security: *const scraping.ContentSecurityConfig,
) !?scraping.DownloadContext {
    if (render_ctx.io == null and render_ctx.deadline_ns == null) return null;
    return .{
        .io = render_ctx.io orelse std.Io.Threaded.global_single_threaded.io(),
        // Leave this null when there is no request deadline. HTTP/S3 still
        // apply their configured ceiling; file:// can safely use caller-owned
        // cancellation without pretending std.Io files support a deadline.
        .timeout_ms = if (render_ctx.deadline_ns != null)
            try requestBoundedRemoteFetchTimeoutMs(
                security.download_timeout_seconds,
                render_ctx.deadline_ns,
                platform_time.monotonicNs(),
            )
        else
            null,
    };
}

fn requestBoundedRemoteFetchTimeoutMs(
    configured_seconds: ?u32,
    deadline_ns: ?u64,
    now_ns: u64,
) !u64 {
    const configured_ms: u64 = if (configured_seconds) |seconds|
        if (seconds == 0) 0 else @as(u64, seconds) * 1000
    else
        30_000;
    const deadline = deadline_ns orelse return configured_ms;
    if (now_ns >= deadline) return error.Timeout;
    const remaining_ns = deadline - now_ns;
    const remaining_ms = @max(
        @as(u64, 1),
        (remaining_ns +| std.time.ns_per_ms - 1) / std.time.ns_per_ms,
    );
    if (configured_ms == 0) return remaining_ms;
    return @min(configured_ms, remaining_ms);
}

test "template remote request deadline bounds configured download timeout" {
    const now_ns: u64 = 10 * std.time.ns_per_s;

    try std.testing.expectEqual(
        @as(u64, 30_000),
        try requestBoundedRemoteFetchTimeoutMs(null, null, now_ns),
    );
    try std.testing.expectEqual(
        @as(u64, 7_000),
        try requestBoundedRemoteFetchTimeoutMs(7, null, now_ns),
    );
    try std.testing.expectEqual(
        @as(u64, 1_500),
        try requestBoundedRemoteFetchTimeoutMs(7, now_ns + 1_500 * std.time.ns_per_ms, now_ns),
    );
    try std.testing.expectEqual(
        @as(u64, 30_000),
        try requestBoundedRemoteFetchTimeoutMs(null, now_ns + 40 * std.time.ns_per_s, now_ns),
    );
    try std.testing.expectEqual(
        @as(u64, 1_500),
        try requestBoundedRemoteFetchTimeoutMs(0, now_ns + 1_500 * std.time.ns_per_ms, now_ns),
    );
    try std.testing.expectEqual(
        @as(u64, 1),
        try requestBoundedRemoteFetchTimeoutMs(7, now_ns + 1, now_ns),
    );
    try std.testing.expectError(
        error.Timeout,
        requestBoundedRemoteFetchTimeoutMs(7, now_ns, now_ns),
    );
}

test "fatal helper errors survive Handlebars error swallowing" {
    const alloc = std.testing.allocator;
    var render_ctx = RenderContext{
        .alloc = alloc,
        .pdf_backend = pdf_mod.Backend.system(),
        .remote_bytes_remaining = default_remote_fetch_max_download_size_bytes,
    };

    const FatalHelper = struct {
        fn call(ctx: hbs.HelperContext) anyerror!hbs.Value {
            const active = renderContext(ctx).?;
            _ = latchFatalRenderError(active, error.Timeout);
            return error.Timeout;
        }
    };

    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    var helpers: hbs.HelperMap = .{};
    try helpers.put(arena_state.allocator(), "fatal", hbs.Helper.withData(&FatalHelper.call, @ptrCast(&render_ctx)));

    const rendered = try template_mod.renderDocumentWithHelpers(alloc, "{{fatal}}", "{}", &helpers);
    try std.testing.expectEqualStrings("", rendered);
    try std.testing.expectError(error.Timeout, completeRenderedText(alloc, rendered, &render_ctx));
}

test "render completion rechecks the absolute deadline" {
    const alloc = std.testing.allocator;
    const render_ctx = RenderContext{
        .alloc = alloc,
        .pdf_backend = pdf_mod.Backend.system(),
        .deadline_ns = platform_time.monotonicNs(),
        .remote_bytes_remaining = default_remote_fetch_max_download_size_bytes,
    };
    const rendered = try alloc.dupe(u8, "rendered");
    try std.testing.expectError(error.Timeout, completeRenderedText(alloc, rendered, &render_ctx));
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
    const parsed = std.Uri.parse(url) catch {
        const cfg = remote_content orelse return .{};
        return .{ .security = effectiveRemoteContentSecurity(cfg, null) };
    };
    const cfg = remote_content orelse {
        if (credential_name != null and isUriScheme(parsed, "s3")) return error.S3CredentialNotFoundOrOutOfScope;
        if (credential_name != null and (isUriScheme(parsed, "http") or isUriScheme(parsed, "https"))) {
            return error.HttpCredentialNotFoundOrOutOfScope;
        }
        return .{};
    };
    if (isUriScheme(parsed, "s3")) {
        const selected = try selectS3Credential(alloc, cfg, secret_store, parsed, credential_name);
        if (credential_name != null and selected == null) return error.S3CredentialNotFoundOrOutOfScope;
        if (selected) |value| {
            return .{
                .security = effectiveRemoteContentSecurity(cfg, value.credential.security),
                .s3_credentials = try resolveSelectedS3Credential(alloc, secret_store, value),
            };
        }
        return .{
            .security = effectiveRemoteContentSecurity(cfg, null),
        };
    }
    if (isUriScheme(parsed, "http") or isUriScheme(parsed, "https")) {
        const credential = selectHttpCredential(cfg, url, credential_name);
        if (credential_name != null and credential == null) return error.HttpCredentialNotFoundOrOutOfScope;
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

const SelectedS3Credential = struct {
    credential: *const scraping.S3CredentialConfig,
    /// Resolved exactly once so secret rotation cannot change endpoint-style
    /// bucket interpretation between scope validation and the actual fetch.
    endpoint: ?[]u8,
};

fn selectS3Credential(
    alloc: Allocator,
    cfg: *const scraping.RemoteContentConfig,
    secret_store: ?*common_secrets.FileStore,
    parsed: std.Uri,
    credential_name: ?[]const u8,
) !?SelectedS3Credential {
    if (credential_name) |name| {
        const credential = cfg.getS3(name) orelse return null;
        return try selectScopedS3Credential(alloc, secret_store, credential, parsed);
    }
    var it = cfg.s3.iterator();
    while (it.next()) |entry| {
        const credential = entry.value_ptr;
        if (credential.buckets == null) continue;
        const maybe_selected = selectScopedS3Credential(alloc, secret_store, credential, parsed) catch |err| switch (err) {
            error.OutOfMemory, error.Canceled => return err,
            else => continue,
        };
        if (maybe_selected) |selected| return selected;
    }
    if (cfg.default_s3) |name| {
        const credential = cfg.getS3(name) orelse return null;
        return try selectScopedS3Credential(alloc, secret_store, credential, parsed);
    }
    return null;
}

fn selectScopedS3Credential(
    alloc: Allocator,
    secret_store: ?*common_secrets.FileStore,
    credential: *const scraping.S3CredentialConfig,
    parsed: std.Uri,
) !?SelectedS3Credential {
    const endpoint = if (credential.endpoint) |endpoint_ref|
        try common_secrets.resolveReferenceOwned(alloc, secret_store, endpoint_ref)
    else
        null;
    errdefer if (endpoint) |value| alloc.free(value);
    const bucket = if (endpoint) |value| blk: {
        break :blk try scraping.s3BucketAlloc(alloc, parsed, value, credential.use_ssl);
    } else try alloc.dupe(u8, (parsed.host orelse return error.InvalidS3Url).percent_encoded);
    defer alloc.free(bucket);
    if (!s3CredentialScopeMatches(credential, bucket)) {
        if (endpoint) |value| alloc.free(value);
        return null;
    }
    return .{ .credential = credential, .endpoint = endpoint };
}

fn s3CredentialScopeMatches(credential: *const scraping.S3CredentialConfig, bucket: []const u8) bool {
    const patterns = credential.buckets orelse return true;
    for (patterns) |pattern| {
        if (bucketPatternMatches(pattern, bucket)) return true;
    }
    return false;
}

fn selectHttpCredential(
    cfg: *const scraping.RemoteContentConfig,
    url: []const u8,
    credential_name: ?[]const u8,
) ?*const scraping.HTTPCredentialConfig {
    if (credential_name) |name| {
        const credential = cfg.getHttp(name) orelse return null;
        const base_url = credential.base_url orelse return credential;
        return if (httpCredentialScopeMatches(base_url, url)) credential else null;
    }
    var selected: ?*const scraping.HTTPCredentialConfig = null;
    var selected_name: ?[]const u8 = null;
    var selected_scope_len: usize = 0;
    var it = cfg.http.iterator();
    while (it.next()) |entry| {
        const credential = entry.value_ptr;
        const base_url = credential.base_url orelse continue;
        if (!httpCredentialScopeMatches(base_url, url)) continue;
        const scope_len = httpCredentialScopeLength(base_url) orelse continue;
        const prefer = selected == null or
            scope_len > selected_scope_len or
            (scope_len == selected_scope_len and std.mem.order(u8, entry.key_ptr.*, selected_name.?) == .lt);
        if (prefer) {
            selected = credential;
            selected_name = entry.key_ptr.*;
            selected_scope_len = scope_len;
        }
    }
    return selected;
}

fn httpCredentialScopeLength(base_url: []const u8) ?usize {
    const base = std.Uri.parse(base_url) catch return null;
    if (!isHttpScheme(base.scheme)) return null;
    if (base.user != null or base.password != null or base.query != null or base.fragment != null) return null;
    const path = httpScopePath(base.path);
    if (!credentialScopePathIsSafe(path)) return null;
    return path.len;
}

fn httpCredentialScopeMatches(base_url: []const u8, url: []const u8) bool {
    const base = std.Uri.parse(base_url) catch return false;
    const target = std.Uri.parse(url) catch return false;

    if (!isHttpScheme(base.scheme) or !isHttpScheme(target.scheme)) return false;
    if (!std.ascii.eqlIgnoreCase(base.scheme, target.scheme)) return false;
    // A credential scope is an origin and optional path, never userinfo, a
    // query, or a fragment. Reject ambiguous configuration instead of turning
    // it into a broader scope than the operator intended.
    if (base.user != null or base.password != null or base.query != null or base.fragment != null) return false;
    if (target.user != null or target.password != null) return false;

    var base_host_buffer: [std.Io.net.HostName.max_len]u8 = undefined;
    var target_host_buffer: [std.Io.net.HostName.max_len]u8 = undefined;
    const base_host = (base.getHost(&base_host_buffer) catch return false).bytes;
    const target_host = (target.getHost(&target_host_buffer) catch return false).bytes;
    if (!std.ascii.eqlIgnoreCase(base_host, target_host)) return false;
    if (effectiveHttpPort(base) != effectiveHttpPort(target)) return false;

    const base_path = httpScopePath(base.path);
    const target_path = httpScopePath(target.path);
    if (!credentialScopePathIsSafe(base_path) or !credentialScopePathIsSafe(target_path)) return false;
    if (std.mem.eql(u8, base_path, target_path)) return true;
    if (!std.mem.startsWith(u8, target_path, base_path)) return false;
    return base_path[base_path.len - 1] == '/' or target_path[base_path.len] == '/';
}

fn isHttpScheme(scheme: []const u8) bool {
    return std.ascii.eqlIgnoreCase(scheme, "http") or std.ascii.eqlIgnoreCase(scheme, "https");
}

fn effectiveHttpPort(uri: std.Uri) u16 {
    return uri.port orelse if (std.ascii.eqlIgnoreCase(uri.scheme, "https")) 443 else 80;
}

fn httpScopePath(component: std.Uri.Component) []const u8 {
    const path = switch (component) {
        .raw, .percent_encoded => |value| value,
    };
    return if (path.len == 0) "/" else path;
}

fn credentialScopePathIsSafe(path: []const u8) bool {
    var segments = std.mem.splitScalar(u8, path, '/');
    while (segments.next()) |segment| {
        var decoded_len: usize = 0;
        var dots_only = true;
        var i: usize = 0;
        while (i < segment.len) {
            const decoded = if (segment[i] == '%') blk: {
                if (i + 2 >= segment.len) return false;
                const high = std.fmt.charToDigit(segment[i + 1], 16) catch return false;
                const low = std.fmt.charToDigit(segment[i + 2], 16) catch return false;
                i += 3;
                break :blk (high << 4) | low;
            } else blk: {
                const value = segment[i];
                i += 1;
                break :blk value;
            };
            // Some HTTP stacks and origin servers normalize backslashes or
            // encoded separators before routing. They cannot safely participate
            // in a syntactic credential path scope.
            if (decoded == '/' or decoded == '\\') return false;
            dots_only = dots_only and decoded == '.';
            decoded_len += 1;
        }
        if (dots_only and (decoded_len == 1 or decoded_len == 2)) return false;
    }
    return true;
}

fn resolveSelectedS3Credential(
    alloc: Allocator,
    secret_store: ?*common_secrets.FileStore,
    selected: SelectedS3Credential,
) !scraping.S3CredentialsConfig {
    const credential = selected.credential;
    var resolved = scraping.S3CredentialsConfig{
        .endpoint = selected.endpoint,
        .use_ssl = credential.use_ssl,
    };
    errdefer resolved.deinit(alloc);
    resolved.access_key_id = if (credential.access_key_id) |value|
        try common_secrets.resolveReferenceOwned(alloc, secret_store, value)
    else
        common_secrets.envValueOwned(alloc, "AWS_ACCESS_KEY_ID");
    resolved.secret_access_key = if (credential.secret_access_key) |value|
        try common_secrets.resolveReferenceOwned(alloc, secret_store, value)
    else
        common_secrets.envValueOwned(alloc, "AWS_SECRET_ACCESS_KEY");
    resolved.session_token = if (credential.session_token) |value|
        try common_secrets.resolveReferenceOwned(alloc, secret_store, value)
    else
        common_secrets.envValueOwned(alloc, "AWS_SESSION_TOKEN");
    return resolved;
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

test "HTTP credential scope requires the same origin and a path boundary" {
    try std.testing.expect(httpCredentialScopeMatches(
        "https://docs.internal.com/api",
        "https://docs.internal.com/api/v1/document?id=1",
    ));
    try std.testing.expect(httpCredentialScopeMatches(
        "https://docs.internal.com",
        "https://DOCS.INTERNAL.COM:443/anything",
    ));
    try std.testing.expect(!httpCredentialScopeMatches(
        "https://docs.internal.com",
        "https://docs.internal.com.evil.test/steal",
    ));
    try std.testing.expect(!httpCredentialScopeMatches(
        "https://docs.internal.com",
        "https://docs.internal.com@evil.test/steal",
    ));
    try std.testing.expect(!httpCredentialScopeMatches(
        "https://docs.internal.com",
        "https://docs.internal.com:444/anything",
    ));
    try std.testing.expect(!httpCredentialScopeMatches(
        "https://docs.internal.com/api",
        "https://docs.internal.com/api-v2/document",
    ));
    try std.testing.expect(!httpCredentialScopeMatches(
        "https://docs.internal.com/api?scope=unexpected",
        "https://docs.internal.com/api",
    ));
    try std.testing.expect(!httpCredentialScopeMatches(
        "https://docs.internal.com/api",
        "https://docs.internal.com/api/../secret",
    ));
    try std.testing.expect(!httpCredentialScopeMatches(
        "https://docs.internal.com/api",
        "https://docs.internal.com/api/%2e%2e/secret",
    ));
    try std.testing.expect(!httpCredentialScopeMatches(
        "https://docs.internal.com/api",
        "https://docs.internal.com/api/%2E./secret",
    ));
    try std.testing.expect(!httpCredentialScopeMatches(
        "https://docs.internal.com/api",
        "https://docs.internal.com/api/%2f../secret",
    ));
    try std.testing.expect(!httpCredentialScopeMatches(
        "https://docs.internal.com/api",
        "https://docs.internal.com/api\\..\\secret",
    ));
}

test "explicit HTTP credentials remain inside their configured scope" {
    const alloc = std.testing.allocator;
    var cfg = scraping.RemoteContentConfig{};
    defer cfg.deinit(alloc);

    {
        const name = try alloc.dupe(u8, "internal");
        errdefer alloc.free(name);
        const base_url = try alloc.dupe(u8, "https://docs.internal.com/api");
        errdefer alloc.free(base_url);
        try cfg.http.put(alloc, name, .{ .base_url = base_url });
    }

    try std.testing.expect(selectHttpCredential(
        &cfg,
        "https://docs.internal.com/api/document",
        "internal",
    ) != null);
    try std.testing.expect(selectHttpCredential(
        &cfg,
        "https://docs.internal.com.evil.test/api/document",
        "internal",
    ) == null);
}

test "explicit remote credentials fail closed when missing or outside scope" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var cfg = scraping.RemoteContentConfig{};
    try cfg.http.put(
        arena,
        try arena.dupe(u8, "internal"),
        .{ .base_url = try arena.dupe(u8, "https://docs.internal.test/api") },
    );
    const bucket_patterns = [_][]u8{try arena.dupe(u8, "allowed-*")};
    try cfg.s3.put(
        arena,
        try arena.dupe(u8, "archive"),
        .{ .buckets = &bucket_patterns },
    );

    try std.testing.expectError(
        error.HttpCredentialNotFoundOrOutOfScope,
        resolveRemoteContentFetchOptions(alloc, &cfg, null, "https://docs.internal.test/api/doc", "missing"),
    );
    try std.testing.expectError(
        error.HttpCredentialNotFoundOrOutOfScope,
        resolveRemoteContentFetchOptions(alloc, &cfg, null, "https://other.internal.test/api/doc", "internal"),
    );
    try std.testing.expectError(
        error.S3CredentialNotFoundOrOutOfScope,
        resolveRemoteContentFetchOptions(alloc, &cfg, null, "s3://allowed-bucket/doc", "missing"),
    );
    try std.testing.expectError(
        error.S3CredentialNotFoundOrOutOfScope,
        resolveRemoteContentFetchOptions(alloc, &cfg, null, "s3://denied-bucket/doc", "archive"),
    );
    try std.testing.expectError(
        error.HttpCredentialNotFoundOrOutOfScope,
        resolveRemoteContentFetchOptions(alloc, null, null, "https://docs.internal.test/api/doc", "internal"),
    );
    try std.testing.expectError(
        error.S3CredentialNotFoundOrOutOfScope,
        resolveRemoteContentFetchOptions(alloc, null, null, "s3://allowed-bucket/doc", "archive"),
    );
}

test "automatic HTTP credential selection is longest-scope and deterministic" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var cfg = scraping.RemoteContentConfig{};
    try cfg.http.put(
        arena,
        try arena.dupe(u8, "broad"),
        .{ .base_url = try arena.dupe(u8, "https://docs.internal.test/") },
    );
    try cfg.http.put(
        arena,
        try arena.dupe(u8, "z-narrow"),
        .{ .base_url = try arena.dupe(u8, "https://docs.internal.test/api") },
    );
    try cfg.http.put(
        arena,
        try arena.dupe(u8, "a-narrow"),
        .{ .base_url = try arena.dupe(u8, "https://docs.internal.test/api") },
    );

    const selected = selectHttpCredential(&cfg, "https://docs.internal.test/api/v1/doc", null).?;
    try std.testing.expect(selected == cfg.getHttp("a-narrow").?);
}

test "S3 credential scopes use the canonical bucket for endpoint-style URLs" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var cfg = scraping.RemoteContentConfig{};
    const allowed_buckets = [_][]u8{try arena.dupe(u8, "allowed-*")};
    try cfg.s3.put(
        arena,
        try arena.dupe(u8, "archive"),
        .{
            .endpoint = try arena.dupe(u8, "http://LOCALHOST:9000"),
            .buckets = &allowed_buckets,
        },
    );
    const endpoint_named_bucket = [_][]u8{try arena.dupe(u8, "localhost")};
    try cfg.s3.put(
        arena,
        try arena.dupe(u8, "host-is-not-bucket"),
        .{
            .endpoint = try arena.dupe(u8, "http://LOCALHOST:9000"),
            .buckets = &endpoint_named_bucket,
        },
    );

    var resolved = try resolveRemoteContentFetchOptions(
        alloc,
        &cfg,
        null,
        "s3://localhost:9000/allowed-bucket/doc.txt",
        "archive",
    );
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("http://LOCALHOST:9000", resolved.s3_credentials.?.endpoint.?);

    try std.testing.expectError(
        error.S3CredentialNotFoundOrOutOfScope,
        resolveRemoteContentFetchOptions(
            alloc,
            &cfg,
            null,
            "s3://localhost:9000/denied-bucket/doc.txt",
            "archive",
        ),
    );
    try std.testing.expectError(
        error.S3CredentialNotFoundOrOutOfScope,
        resolveRemoteContentFetchOptions(
            alloc,
            &cfg,
            null,
            "s3://localhost:9000/denied-bucket/doc.txt",
            "host-is-not-bucket",
        ),
    );
}

test "automatic S3 selection skips broken nonmatches and resolves only selected auth" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var cfg = scraping.RemoteContentConfig{};
    const allowed_buckets = [_][]u8{try arena.dupe(u8, "allowed-*")};
    try cfg.s3.put(
        arena,
        try arena.dupe(u8, "broken-endpoint"),
        .{
            .endpoint = try arena.dupe(u8, "${secret:CODEX_TEST_MISSING_S3_ENDPOINT_7C91}"),
            .buckets = &allowed_buckets,
        },
    );
    const other_buckets = [_][]u8{try arena.dupe(u8, "other-*")};
    try cfg.s3.put(
        arena,
        try arena.dupe(u8, "broken-unselected-auth"),
        .{
            .endpoint = try arena.dupe(u8, "http://localhost:9000"),
            .secret_access_key = try arena.dupe(u8, "${secret:CODEX_TEST_MISSING_S3_AUTH_7C91}"),
            .buckets = &other_buckets,
        },
    );
    try cfg.s3.put(
        arena,
        try arena.dupe(u8, "selected"),
        .{
            .endpoint = try arena.dupe(u8, "http://localhost:9000"),
            .access_key_id = try arena.dupe(u8, "access"),
            .secret_access_key = try arena.dupe(u8, "secret"),
            .buckets = &allowed_buckets,
        },
    );

    var resolved = try resolveRemoteContentFetchOptions(
        alloc,
        &cfg,
        null,
        "s3://localhost:9000/allowed-bucket/doc.txt",
        null,
    );
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("access", resolved.s3_credentials.?.access_key_id.?);
    try std.testing.expectEqualStrings("secret", resolved.s3_credentials.?.secret_access_key.?);
}

test "explicit credential and invalid credential inputs render permanent 403 failures" {
    const alloc = std.testing.allocator;
    var cfg = scraping.RemoteContentConfig{};
    defer cfg.deinit(alloc);
    const json_doc =
        \\{"http":"https://docs.internal.test/doc","s3":"s3://bucket/doc","empty":"","number":7}
    ;

    const rendered = try renderJsonToTextWithConfig(
        alloc,
        "{{remoteText url=http credentials=\"missing\"}}",
        json_doc,
        .{ .remote_content = &cfg },
    );
    defer alloc.free(rendered);
    const directives = try template_mod.parseErrorDirectives(alloc, rendered);
    defer template_mod.freeErrorDirectives(alloc, directives);
    try std.testing.expectEqual(@as(usize, 1), directives.len);
    try std.testing.expectEqual(@as(u16, 403), directives[0].status);
    try std.testing.expect(directives[0].isPermanent());

    try std.testing.expectError(
        RenderError.PermanentPromptFailure,
        renderJsonToValidatedTextWithConfig(
            alloc,
            "{{remoteText url=s3 credentials=\"missing\"}}",
            json_doc,
            .{ .remote_content = &cfg },
        ),
    );
    try std.testing.expectError(
        RenderError.PermanentPromptFailure,
        renderJsonToValidatedTextWithConfig(
            alloc,
            "{{remoteText url=http credentials=empty}}",
            json_doc,
            .{ .remote_content = &cfg },
        ),
    );
    try std.testing.expectError(
        RenderError.PermanentPromptFailure,
        renderJsonToValidatedTextWithConfig(
            alloc,
            "{{remoteText url=http credentials=number}}",
            json_doc,
            .{ .remote_content = &cfg },
        ),
    );
}

test "HTTP error bodies consume the aggregate render byte budget" {
    var render_ctx = RenderContext{
        .alloc = std.testing.allocator,
        .pdf_backend = pdf_mod.Backend.system(),
        .remote_bytes_remaining = 4,
    };
    const error_outcome = scraping.DownloadOutcome{ .http_error = .{
        .status = 500,
        .message = "remote fetch failed",
        .downloaded_bytes = 2,
    } };
    try chargeDownloadOutcome(&render_ctx, error_outcome, 4);
    try chargeDownloadOutcome(&render_ctx, error_outcome, 2);
    try std.testing.expectEqual(@as(u64, 0), render_ctx.remote_bytes_remaining);

    render_ctx.remote_bytes_remaining = 4;
    const oversized_error = scraping.DownloadOutcome{ .http_error = .{
        .status = 500,
        .message = "remote fetch failed",
        .downloaded_bytes = 5,
    } };
    try std.testing.expectError(error.StreamTooLong, chargeDownloadOutcome(&render_ctx, oversized_error, 4));
    try std.testing.expectEqual(@as(u64, 0), render_ctx.remote_bytes_remaining);
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

test "template remote S3 credential resolution survives every allocation failure" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var credential = scraping.S3CredentialConfig{};
            defer credential.deinit(alloc);
            credential.access_key_id = try alloc.dupe(u8, "access-key");
            credential.secret_access_key = try alloc.dupe(u8, "secret-key");
            credential.session_token = try alloc.dupe(u8, "session-token");
            const endpoint = try alloc.dupe(u8, "s3.example.test");
            var resolved = try resolveSelectedS3Credential(alloc, null, .{
                .credential = &credential,
                .endpoint = endpoint,
            });
            defer resolved.deinit(alloc);
            try std.testing.expectEqualStrings("s3.example.test", resolved.endpoint.?);
            try std.testing.expectEqualStrings("access-key", resolved.access_key_id.?);
            try std.testing.expectEqualStrings("secret-key", resolved.secret_access_key.?);
            try std.testing.expectEqualStrings("session-token", resolved.session_token.?);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
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
        .security = .{
            .block_private_ips = false,
            .max_download_size_bytes = "missing".len,
        },
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

    const twice = try renderJsonToTextWithConfig(
        alloc,
        "{{remoteText url=missing_url}}{{remoteText url=missing_url}}",
        json_doc,
        .{ .remote_content = &remote_content },
    );
    defer alloc.free(twice);
    const twice_directives = try template_mod.parseErrorDirectives(alloc, twice);
    defer template_mod.freeErrorDirectives(alloc, twice_directives);
    try std.testing.expectEqual(@as(usize, 2), twice_directives.len);
    try std.testing.expectEqual(@as(u16, 404), twice_directives[0].status);
    try std.testing.expectEqual(@as(u16, 413), twice_directives[1].status);
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

test "template remote enforces one aggregate byte budget across helpers" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "first.txt", .data = "ab" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "exact.txt", .data = "cd" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "over.txt", .data = "cde" });

    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const first_path = try tmp.dir.realPathFileAlloc(std.testing.io, "first.txt", alloc);
    defer alloc.free(first_path);
    const exact_path = try tmp.dir.realPathFileAlloc(std.testing.io, "exact.txt", alloc);
    defer alloc.free(exact_path);
    const over_path = try tmp.dir.realPathFileAlloc(std.testing.io, "over.txt", alloc);
    defer alloc.free(over_path);

    const json_doc = try std.fmt.allocPrint(alloc,
        \\{{"first":"file://{s}","exact":"file://{s}","over":"file://{s}"}}
    , .{ first_path, exact_path, over_path });
    defer alloc.free(json_doc);

    const allowed_paths = [_][]u8{root};
    const remote_content = scraping.RemoteContentConfig{
        .security = .{
            .allowed_paths = &allowed_paths,
            .max_download_size_bytes = 4,
        },
    };
    try std.testing.expectEqual(@as(u64, 4), remoteByteBudget(&remote_content));

    const exact = try renderJsonToTextWithConfig(
        alloc,
        "{{remoteText url=first}}{{remoteText url=exact}}",
        json_doc,
        .{ .remote_content = &remote_content },
    );
    defer alloc.free(exact);
    try std.testing.expectEqualStrings("abcd", exact);

    try std.testing.expectError(
        RenderError.PermanentPromptFailure,
        renderJsonToValidatedTextWithConfig(
            alloc,
            "{{remoteText url=first}}{{remoteText url=over}}",
            json_doc,
            .{ .remote_content = &remote_content },
        ),
    );
}

test "template remote media limit skips later fetches without changing the default" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "first.png", .data = "first" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "second.png", .data = "second" });

    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const first_path = try tmp.dir.realPathFileAlloc(std.testing.io, "first.png", alloc);
    defer alloc.free(first_path);
    const second_path = try tmp.dir.realPathFileAlloc(std.testing.io, "second.png", alloc);
    defer alloc.free(second_path);
    const missing_path = try std.fs.path.join(alloc, &.{ root, "missing.png" });
    defer alloc.free(missing_path);

    const json_doc = try std.fmt.allocPrint(alloc,
        \\{{"first":"file://{s}","second":"file://{s}","missing":"file://{s}"}}
    , .{ first_path, second_path, missing_path });
    defer alloc.free(json_doc);

    const allowed_paths = [_][]u8{root};
    const remote_content = scraping.RemoteContentConfig{
        .security = .{
            .allowed_paths = &allowed_paths,
            .max_download_size_bytes = 1024,
        },
    };

    const default_parts = try renderJsonToPartsWithConfig(
        alloc,
        "{{remoteMedia url=first}}{{remoteMedia url=second}}",
        json_doc,
        .{ .remote_content = &remote_content },
    );
    defer template_mod.freeContentParts(alloc, default_parts);
    try std.testing.expectEqual(@as(usize, 2), default_parts.len);

    const bounded_parts = try renderJsonToPartsWithConfig(
        alloc,
        "{{remoteMedia url=first}}{{remoteMedia url=missing}}",
        json_doc,
        .{
            .remote_content = &remote_content,
            .max_media_parts = 1,
        },
    );
    defer template_mod.freeContentParts(alloc, bounded_parts);
    try std.testing.expectEqual(@as(usize, 1), bounded_parts.len);
}

test "data URI media consumes the aggregate decoded-byte budget" {
    const alloc = std.testing.allocator;
    const remote_content = scraping.RemoteContentConfig{
        .security = .{ .max_download_size_bytes = 3 },
    };
    const json_doc =
        \\{"image":"data:image/png;base64,YWI="}
    ;

    const one = try renderJsonToPartsWithConfig(
        alloc,
        "{{remoteMedia url=image}}",
        json_doc,
        .{ .remote_content = &remote_content },
    );
    defer template_mod.freeContentParts(alloc, one);
    try std.testing.expectEqual(@as(usize, 1), one.len);

    try std.testing.expectError(
        RenderError.PermanentPromptFailure,
        renderJsonToValidatedTextWithConfig(
            alloc,
            "{{remoteMedia url=image}}{{remoteMedia url=image}}",
            json_doc,
            .{ .remote_content = &remote_content },
        ),
    );
}

test "PDF extract mode emits text even when the media-part limit is exhausted" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "doc.pdf", .data = "%PDF-fake" });

    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const pdf_path = try tmp.dir.realPathFileAlloc(std.testing.io, "doc.pdf", alloc);
    defer alloc.free(pdf_path);
    const json_doc = try std.fmt.allocPrint(alloc, "{{\"pdf\":\"file://{s}\"}}", .{pdf_path});
    defer alloc.free(json_doc);

    const FakePdfBackend = struct {
        fn extract(_: *const anyopaque, a: Allocator, _: []const u8) ![]u8 {
            return try a.dupe(u8, "extracted text");
        }

        fn render(_: *const anyopaque, a: Allocator, _: []const u8) ![]u8 {
            return try a.dupe(u8, "png");
        }
    };
    const backend = pdf_mod.Backend{
        .ptr = undefined,
        .extract_text_fn = FakePdfBackend.extract,
        .render_first_page_png_fn = FakePdfBackend.render,
    };
    const allowed_paths = [_][]u8{root};
    const remote_content = scraping.RemoteContentConfig{
        .security = .{
            .allowed_paths = &allowed_paths,
            .max_download_size_bytes = 1024,
        },
    };

    const rendered = try renderJsonToTextWithConfig(
        alloc,
        "{{remoteMedia url=pdf mode=\"extract\"}}",
        json_doc,
        .{
            .pdf_backend = backend,
            .remote_content = &remote_content,
            .max_media_parts = 0,
        },
    );
    defer alloc.free(rendered);
    try std.testing.expectEqualStrings("extracted text", rendered);
}
