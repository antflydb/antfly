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

test {
    _ = @import("storage/lite/backend.zig");
    _ = @import("storage/lite/conformance_test.zig");
    _ = @import("storage/lite/docstore.zig");
    _ = @import("storage/lite/index_storage.zig");
    _ = @import("storage/lite/native.zig");
    _ = @import("storage/lite/paths.zig");
    _ = @import("storage/lite/restore_staging.zig");
}
