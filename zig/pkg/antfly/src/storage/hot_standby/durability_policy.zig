// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
