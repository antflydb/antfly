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

pub const backend = @import("backend.zig");
pub const capabilities = @import("capabilities.zig");
pub const connection = @import("connection.zig");
pub const docstore = @import("docstore.zig");
pub const index_storage = @import("index_storage.zig");
pub const native = @import("native.zig");
pub const paths = @import("paths.zig");
pub const restore_staging = @import("restore_staging.zig");
pub const sql_source = @import("sql_source.zig");

test {
    _ = backend;
    _ = @import("bridge.zig");
    _ = capabilities;
    _ = connection;
    _ = docstore;
    _ = index_storage;
    _ = native;
    _ = paths;
    _ = restore_staging;
    _ = sql_source;
}
