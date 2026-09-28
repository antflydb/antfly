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
