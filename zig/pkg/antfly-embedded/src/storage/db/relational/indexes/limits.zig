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

//! A single admission contract for ordered keys and all their continuations.
//! Includes the physical generation prefix, tuple, escaped document ID, footer.
//! Enforced before publication for writes, backfill, and reverse reconstruction.
pub const max_stored_key_bytes: usize = 1024 * 1024;
pub const max_cursor_key_bytes = max_stored_key_bytes;

pub fn admit(bytes: usize) !void {
    if (bytes > max_stored_key_bytes) return error.RelationalIndexKeyTooLarge;
}
