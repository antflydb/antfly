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

pub const impl = if (builtin.os.tag == .freestanding or build_options.bench_minimal_deps)
    @import("template_remote_stub.zig")
else
    struct {
        const template_remote = @import("../../template_remote.zig");

        pub const RenderConfig = template_remote.RenderConfig;
        pub const HostRenderer = struct {};

        pub fn setHostRenderer(_: ?@This().HostRenderer) void {}

        pub fn renderJsonToText(
            alloc: std.mem.Allocator,
            template_source: []const u8,
            json_doc: []const u8,
        ) ![]const u8 {
            return try template_remote.renderJsonToText(alloc, template_source, json_doc);
        }
    };

pub const RenderConfig = impl.RenderConfig;
pub const HostRenderer = impl.HostRenderer;

pub fn setHostRenderer(renderer: ?HostRenderer) void {
    impl.setHostRenderer(renderer);
}

pub fn renderJsonToText(
    alloc: std.mem.Allocator,
    template_source: []const u8,
    json_doc: []const u8,
) ![]const u8 {
    return try impl.renderJsonToText(alloc, template_source, json_doc);
}
