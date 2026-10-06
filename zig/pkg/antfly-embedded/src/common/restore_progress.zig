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

/// Transport-neutral progress for a staged portable import. Callbacks borrow
/// this value synchronously; publication remains a separate commit point.
pub const Progress = struct {
    blocks_processed: u64,
    rows_validated: u64,
    payload_bytes_processed: u64,
    elapsed_ns: u64,
    rows_per_second: u64,
};
