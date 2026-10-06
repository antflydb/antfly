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

/// Generated paths are resolved during the make phase in Zig 0.17. Pass the
/// explicit test library path through env rather than baking a cache path into
/// production binaries or resolving the installation prefix at configure time.
pub fn configureTest(b: *std.Build, run: *std.Build.Step.Run, bridge: ?std.Build.LazyPath) void {
    if (bridge) |path| {
        const arguments = run.argv.toOwnedSlice(b.allocator) catch @panic("OOM");
        run.addArg("env");
        run.addPrefixedFileArg("ANTFLY_APPLE_BRIDGE_PATH=", path);
        run.argv.appendSlice(b.allocator, arguments) catch @panic("OOM");
        run.has_side_effects = true;
    }
}
