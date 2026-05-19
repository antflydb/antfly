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

const Allocator = std.mem.Allocator;

pub const RenderError = error{
    PermanentPromptFailure,
    TransientPromptFailure,
};

const RenderContext = struct {
    alloc: Allocator,
    pdf_backend: pdf_mod.Backend,
};

pub const RenderConfig = struct {
    pdf_backend: pdf_mod.Backend = pdf_mod.Backend.system(),
};

const remote_fetch_security = scraping.ContentSecurityConfig{
    .max_download_size_bytes = 4 << 20,
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
    };
    const prev_ctx = active_render_context;
    active_render_context = &render_ctx;
    defer active_render_context = prev_ctx;

    return try template_mod.renderDocumentWithHelpers(alloc, template_source, json_doc, &extra_helpers);
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

    const fetched = scraping.downloadContentOutcomeAlloc(render_ctx.alloc, url_str, &remote_fetch_security, null) catch |err| {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
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

    const fetched = scraping.downloadContentOutcomeAlloc(render_ctx.alloc, url_str, &remote_fetch_security, null) catch |err| {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
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

    const fetched = scraping.downloadContentOutcomeAlloc(render_ctx.alloc, url_str, &remote_fetch_security, null) catch |err| {
        const result = try template_mod.formatErrorDirective(ctx.arena, 0, @errorName(err));
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

test "template remote renders remotePDF extract with injected pdf backend" {
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
                return .{
                    .status = 200,
                    .content_type = try req_alloc.dupe(u8, "application/pdf"),
                    .body = try req_alloc.dupe(u8, "%PDF-fake"),
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

    const rendered = try renderJsonToTextWithConfig(alloc, "{{remotePDF url=pdf_url}}", json_doc, .{
        .pdf_backend = backend,
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

    const parts = try renderJsonToPartsWithConfig(alloc, "{{remoteMedia url=pdf_url mode=\"render\"}}", json_doc, .{
        .pdf_backend = backend,
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

    const rendered = try renderJsonToTextWithConfig(alloc, "{{remoteText url=missing_url}}", json_doc, .{});
    defer alloc.free(rendered);

    const directives = try template_mod.parseErrorDirectives(alloc, rendered);
    defer template_mod.freeErrorDirectives(alloc, directives);

    try std.testing.expectEqual(@as(usize, 1), directives.len);
    try std.testing.expectEqual(@as(u16, 404), directives[0].status);
    try std.testing.expectEqualStrings("remote fetch failed", directives[0].message);
}
