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

//! Preserve per-owner slice attribution but publish process totals from the
//! common host root. Summing a child's bytes again would double-count them;
//! using only storage's child would omit host executors and bootstrap headers.
const resources = @import("antfly_local_sources").storage_resource_manager;
pub fn apply(stats: *resources.Stats, root: *resources.ResourceManager) void {
    stats.memory = root.snapshot().memory;
}
