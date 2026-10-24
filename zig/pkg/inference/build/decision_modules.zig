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

/// Share a single source identity across SQL and inference consumers. This
/// std-only module inherits the consumer's target and optimization settings.
pub fn create(b: *std.Build, root: std.Build.LazyPath) *std.Build.Module {
    if (b.modules.get("antfly_decisions")) |existing| return existing;
    const module = b.addModule("antfly_decisions", .{ .root_source_file = root });
    const cloudflare = b.modules.get("antfly_cloudflare") orelse b.addModule("antfly_cloudflare", .{ .root_source_file = root.dirname().path(b, "../cloudflare/root.zig") });
    module.addImport("antfly_cloudflare", cloudflare);
    return module;
}

const WireBinding = struct { generator: *std.Build.Module, module: *std.Build.Module };
var wire_bindings: std.ArrayList(WireBinding) = .empty;

/// Wire types embed GeneratorConfig. Reuse their source identity within one
/// dependency graph, without mixing native and WASM generator modules.
pub fn createOpenApi(b: *std.Build, root: std.Build.LazyPath, generator: *std.Build.Module) *std.Build.Module {
    for (wire_bindings.items) |binding| if (binding.generator == generator) return binding.module;
    const module = b.createModule(.{ .root_source_file = root });
    module.addImport("antfly_generating_openapi", generator);
    wire_bindings.append(b.allocator, .{ .generator = generator, .module = module }) catch @panic("OOM");
    return module;
}
