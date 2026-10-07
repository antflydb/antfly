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

/// Legacy tuple contributors use owner; explicit relationships use
/// owner_document (or implicitly source). Never feed an explicit identity
/// into the tuple-only membership directory.
pub fn validate(edge_id: []const u8, owner_document: []const u8, owner: []const u8) !void {
    if ((owner_document.len > 0 and edge_id.len == 0) or
        (owner.len > 0 and edge_id.len > 0)) return error.InvalidGraphEdges;
}
