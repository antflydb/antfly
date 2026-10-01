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

pub const backend = @import("backend.zig");
pub const capabilities = @import("capabilities.zig");
pub const connection = @import("connection.zig");
pub const docstore = @import("docstore.zig");
pub const index_storage = @import("index_storage.zig");
pub const native = @import("native.zig");
pub const secret_store = @import("secret_store.zig");
pub const paths = @import("paths.zig");
pub const restore_staging = if (@import("builtin").os.tag == .freestanding) @import("portable_restore.zig") else @import("restore_staging.zig");

test {
    _ = backend;
    _ = @import("bridge.zig");
    _ = capabilities;
    _ = connection;
    _ = docstore;
    _ = index_storage;
    _ = native;
    _ = secret_store;
    _ = paths;
    _ = restore_staging;
}
