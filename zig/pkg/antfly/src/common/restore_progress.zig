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

/// Transport-neutral progress for a staged portable import. Callbacks borrow
/// this value synchronously; publication remains a separate commit point.
pub const Progress = struct {
    blocks_processed: u64,
    rows_validated: u64,
    payload_bytes_processed: u64,
    elapsed_ns: u64,
    rows_per_second: u64,
};
