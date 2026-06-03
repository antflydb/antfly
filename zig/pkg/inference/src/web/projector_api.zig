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

const std = @import("std");
const projector_store_mod = @import("projector_store.zig");
const web_runtime = @import("runtime_state.zig");

pub fn loadGguf(
    allocator: std.mem.Allocator,
    runtime: *web_runtime.Runtime,
    gguf_data: []const u8,
) !u32 {
    const store = try projector_store_mod.ProjectorStore.initOwnedBytes(allocator, "web-projector.gguf", gguf_data);
    errdefer store.deinit();
    const kind = store.kind;
    return runtime.storeProjector(store, kind);
}
