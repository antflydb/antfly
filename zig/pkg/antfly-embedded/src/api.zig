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

const embedded = @import("embedded_api_surface");

pub const OpenOptions = embedded.OpenOptions;
pub const Api = embedded.Api;
pub const checkLiteFileJson = embedded.checkLiteFileJson;
pub const copyStableLiteSnapshotFileJson = embedded.copyStableLiteSnapshotFileJson;

test "pkg antfly embedded api Lite surface compiles" {
    _ = OpenOptions;
    _ = Api.createLite;
    _ = Api.createLiteHosted;
    _ = Api.openLite;
    _ = Api.openLiteHosted;
    _ = Api.statusJson;
    _ = Api.checkLiteJson;
    _ = Api.checkLiteFileJson;
    _ = Api.copyStableLiteSnapshotFileJson;
    _ = checkLiteFileJson;
    _ = copyStableLiteSnapshotFileJson;
}
