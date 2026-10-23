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

//! Data-only replication durability policy. Runtime slot selection and
//! acknowledgement evaluation belong to the publisher adapter.

pub const DurabilityMode = enum {
    async,
    remote_write,
    remote_apply,
};

pub const StandbySelection = enum {
    any,
    first,
    all,
};

pub const FailurePolicy = enum {
    block,
    fail_closed,
    degrade_to_async,
};

pub const SyncPolicy = struct {
    mode: DurabilityMode = .async,
    selection: StandbySelection = .any,
    required: usize = 1,
    standby_names: []const []const u8 = &.{},
    failure_policy: FailurePolicy = .block,
};

pub const DurabilityStatus = enum {
    satisfied,
    would_block,
    fail_closed,
    degraded_to_async,
};

pub const DurabilityDecision = struct {
    status: DurabilityStatus,
    mode: DurabilityMode,
    selection: StandbySelection,
    target_lsn: u64,
    progress_lsn: u64,
    missing_lsn_count: u64,
    satisfied_count: usize,
    required_count: usize,
    candidate_count: usize,
};
