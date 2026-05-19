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
const template_mod = @import("template_stub.zig");

const Allocator = std.mem.Allocator;

pub const RenderError = error{
    PermanentPromptFailure,
    TransientPromptFailure,
};

pub const RenderConfig = struct {};

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
            return try render_fn(renderer.ctx, alloc, template_source, json_doc, config);
        }
        const rendered = try callHostRenderJsonToText(alloc, template_source, json_doc, config);
        defer alloc.free(@constCast(rendered));
        return try template_mod.textToParts(alloc, rendered);
    }

    const rendered = try template_mod.renderDocument(alloc, template_source, json_doc);
    defer alloc.free(@constCast(rendered));
    return try template_mod.textToParts(alloc, rendered);
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
