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

//! Hidden initial-child Raft failover, distinct from ALTER publication.
test "mounted initial FK hidden three voter owner transfers before release" {
    // Keep the three-voter fault scenario, and exercise the SQL preparation
    // boundary as well. Explicit REST creation remains covered by the cold
    // restart and offline-owner fixtures sharing this publication machinery.
    try @import("hosted_initial_fk_fault_integration_test.zig").mountedInitialScenario(.generated_transfer);
}
