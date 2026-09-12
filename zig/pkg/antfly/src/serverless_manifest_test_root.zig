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

const manifest_object_store = @import("serverless/manifest/object_store.zig");
const object_storage = @import("storage/object_storage.zig");

test {
    _ = manifest_object_store;
    _ = @import("serverless/manifest/fs_store.zig");
    _ = @import("serverless/manifest/remote_store.zig");
    _ = object_storage;
    _ = @import("serverless/build/retention.zig");
    _ = @import("serverless/catalog/fs_progress_store.zig");
    _ = @import("serverless/catalog/progress_store.zig");
}
