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

//! Index protocol compatibility names. Domains and wire fields remain exact;
//! shared signing machinery must not make another control family interchangeable.
const roots = @import("store_root_attestation.zig");
pub const Authority = roots.Authority;
pub const Proof = roots.ProofFor(
    "antfly/index-build-owner-receipt/v2",
    "antfly/index-owner-accepted-root/v1",
);
