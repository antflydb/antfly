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

const web_profile = @import("profile.zig");
const exports_core = @import("exports_core.zig");
const exports_generation = @import("exports_generation.zig");

comptime {
    if (!web_profile.is_wasm32) {
        @compileError("exports_wasm32.zig must only be built for wasm32");
    }
    _ = exports_core;
    _ = exports_generation;
}
