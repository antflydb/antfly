// Copyright 2026 Antfly, Inc.
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

// Compatibility WASM entry point for browser-based Termite inference.
//
// `build.zig` now selects `src/wasm_entry_wasm32.zig` or
// `src/wasm_entry_wasm64.zig` explicitly. This file remains as a generic root
// that dispatches to the matching profile shim for direct uses.

const std = @import("std");
const web_profile = @import("web/profile.zig");
const exports_profile = if (web_profile.is_wasm64)
    @import("web/exports_wasm64.zig")
else
    @import("web/exports_wasm32.zig");

comptime {
    _ = exports_profile;
}

export fn ldexpf(x: f32, exponent: c_int) f32 {
    var value = x;
    var e = exponent;
    while (e > 0) : (e -= 1) value *= 2.0;
    while (e < 0) : (e += 1) value *= 0.5;
    return value;
}

pub const os = struct {
    pub const PATH_MAX = 4096;
    pub const NAME_MAX = 255;
};

pub const std_options_debug_threaded_io: ?*std.Io.Threaded = null;
pub const std_options_debug_io: std.Io = undefined;
