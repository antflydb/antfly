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
const build_options = @import("build_options");
const template_mod = @import("template_stub.zig");
const scraping = if (builtin.os.tag == .freestanding or build_options.bench_minimal_deps)
    @import("scraping_stub.zig")
else
    @import("antfly_scraping");
const common_secrets = @import("../../common/secrets.zig");

const Allocator = std.mem.Allocator;

pub const RenderError = error{
    PermanentPromptFailure,
    TransientPromptFailure,
};

pub const RenderConfig = struct {};

pub const default_remote_fetch_max_download_size_bytes: u64 = 100 * 1024 * 1024;

const remote_fetch_security = scraping.ContentSecurityConfig{
    .block_private_ips = true,
    .max_download_size_bytes = default_remote_fetch_max_download_size_bytes,
};

pub const RenderJsonToTextFn = *const fn (
    ctx: ?*anyopaque,
    alloc: Allocator,
    template_source: []const u8,
    json_doc: []const u8,
    config: RenderConfig,
) anyerror![]const u8;

pub const RenderJsonToPartsFn = *const fn (
    ctx: ?*anyopaque,
    alloc: Allocator,
    template_source: []const u8,
    json_doc: []const u8,
    config: RenderConfig,
) anyerror![]template_mod.ContentPart;

pub const HostRenderer = struct {
    ctx: ?*anyopaque = null,
    render_json_to_text: ?RenderJsonToTextFn = null,
    render_json_to_parts: ?RenderJsonToPartsFn = null,
};

const unsupported_remote_helpers = [_][]const u8{
    "{{remoteMedia",
    "{{remotePDF",
    "{{remoteText",
    "{{transcribeAudio",
};

var host_renderer: ?HostRenderer = null;

pub fn setHostRenderer(renderer: ?HostRenderer) void {
    host_renderer = renderer;
}

fn requiresRemoteHelpers(template_source: []const u8) bool {
    inline for (unsupported_remote_helpers) |needle| {
        if (std.mem.indexOf(u8, template_source, needle) != null) {
            return true;
        }
    }
    return false;
}

fn callHostRenderJsonToText(
    alloc: Allocator,
    template_source: []const u8,
    json_doc: []const u8,
    config: RenderConfig,
) ![]const u8 {
    const renderer = host_renderer orelse return error.UnsupportedPlatform;
    const render_fn = renderer.render_json_to_text orelse return error.UnsupportedPlatform;
    return try render_fn(renderer.ctx, alloc, template_source, json_doc, config);
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
    if (requiresRemoteHelpers(template_source)) {
        return try callHostRenderJsonToText(alloc, template_source, json_doc, config);
    }
    return try template_mod.renderDocument(alloc, template_source, json_doc);
}

pub fn renderJsonToValidatedTextWithConfig(
    alloc: Allocator,
    template_source: []const u8,
    json_doc: []const u8,
    config: RenderConfig,
) ![]const u8 {
    const rendered = try renderJsonToTextWithConfig(alloc, template_source, json_doc, config);
    errdefer alloc.free(@constCast(rendered));
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
    if (requiresRemoteHelpers(template_source)) {
        const renderer = host_renderer orelse return error.UnsupportedPlatform;
        if (renderer.render_json_to_parts) |render_fn| {
            const parts = try render_fn(renderer.ctx, alloc, template_source, json_doc, config);
            errdefer template_mod.freeContentParts(alloc, parts);
            try validateRenderedParts(alloc, parts);
            return parts;
        }
        const rendered = try callHostRenderJsonToText(alloc, template_source, json_doc, config);
        defer alloc.free(@constCast(rendered));
        try validateRenderedTemplate(alloc, rendered);
        return try template_mod.textToParts(alloc, rendered);
    }

    const rendered = try template_mod.renderDocument(alloc, template_source, json_doc);
    defer alloc.free(@constCast(rendered));
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

fn validateRenderedParts(alloc: Allocator, parts: []const template_mod.ContentPart) !void {
    for (parts) |part| {
        switch (part) {
            .text => |text| try validateRenderedTemplate(alloc, text),
            else => {},
        }
    }
}

pub fn downloadRemoteContentOutcomeAllocWithConfig(
    alloc: Allocator,
    remote_content: ?*const scraping.RemoteContentConfig,
    secret_store: ?*common_secrets.FileStore,
    url: []const u8,
    credential_name: ?[]const u8,
) !scraping.DownloadOutcome {
    _ = secret_store;
    if (credential_name != null) return error.UnsupportedRemoteContentCredential;
    const security = try effectiveRemoteContentSecurity(remote_content);
    return try scraping.downloadContentOutcomeAllocWithHeaders(
        alloc,
        url,
        &security,
        null,
        null,
    );
}

fn effectiveRemoteContentSecurity(remote_content: ?*const scraping.RemoteContentConfig) !scraping.ContentSecurityConfig {
    const cfg = remote_content orelse return remote_fetch_security;
    if (cfg.default_s3 != null or cfg.s3.count() > 0 or cfg.http.count() > 0) return error.UnsupportedRemoteContentCredential;
    var effective = remote_fetch_security;
    if (cfg.security) |security| applyContentSecurityOverride(&effective, security);
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

test "template remote stub renders local template parts" {
    const alloc = std.testing.allocator;

    const parts = try renderJsonToParts(alloc, "{{title}} {{body}}",
        \\{"title":"Hello","body":"world"}
    );
    defer template_mod.freeContentParts(alloc, parts);

    try std.testing.expectEqual(@as(usize, 1), parts.len);
    try std.testing.expectEqualStrings("Hello world", parts[0].text);
}

test "template remote stub rejects remote helpers" {
    const alloc = std.testing.allocator;

    try std.testing.expectError(
        error.UnsupportedPlatform,
        renderJsonToText(
            alloc,
            "{{remoteText url=this}}",
            "\"https://example.com/doc.txt\"",
        ),
    );
}

fn testHostRenderJsonToText(
    _: ?*anyopaque,
    alloc: Allocator,
    _: []const u8,
    _: []const u8,
    _: RenderConfig,
) ![]const u8 {
    return try alloc.dupe(u8, "remote text");
}

test "template remote stub can use host text renderer for remote helpers" {
    const alloc = std.testing.allocator;
    setHostRenderer(.{
        .render_json_to_text = testHostRenderJsonToText,
    });
    defer setHostRenderer(null);

    const rendered = try renderJsonToText(
        alloc,
        "{{remoteText url=this}}",
        "\"https://example.com/doc.txt\"",
    );
    defer alloc.free(@constCast(rendered));

    try std.testing.expectEqualStrings("remote text", rendered);
}
