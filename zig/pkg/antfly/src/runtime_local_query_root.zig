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

//! Independently compiled owner of complete local query operations.
//! Physical DB/index loops remain in the shared Apache storage engine.
pub const antfly_sources = @import("source_owner_storage.zig");
pub const runtime_impl = @import("capi_root.zig");
pub const storage_backend_erased = runtime_impl.storage_backend_erased;
pub const lsm_backend = runtime_impl.lsm_backend;
const provider = @import("storage/local_query_provider.zig");
comptime {
    @export(&provider.execute, .{ .name = "antfly_local_query_execute", .visibility = .hidden });
    @export(&provider.bufferDestroy, .{ .name = "antfly_local_query_buffer_destroy", .visibility = .hidden });
}
