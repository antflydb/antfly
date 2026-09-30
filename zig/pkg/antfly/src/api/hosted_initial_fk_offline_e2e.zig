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

//! Physical cleanup after an offline initial child misses cancellation.
test "mounted initial FK canceled offline replica returns and signs exact unlink ACK" {
    try @import("hosted_initial_fk_fault_e2e.zig").mountedInitialScenario(.cancel_offline);
}

test "mounted initial FK published obsolete released replica retires without canceling current owner" {
    try @import("hosted_initial_fk_fault_e2e.zig").mountedInitialScenario(.published_obsolete);
}
