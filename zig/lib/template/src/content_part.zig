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

/// Media content shared by template rendering and inference providers.
pub const ContentPart = union(enum) {
    text: []const u8,
    media_url: []const u8,
    binary: BinaryContent,

    pub const BinaryContent = struct {
        mime_type: []const u8,
        data: []const u8,
    };
};
