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

//! Index protocol compatibility names. Domains and wire fields remain exact;
//! shared signing machinery must not make another control family interchangeable.
const roots = @import("store_root_attestation.zig");
pub const Authority = roots.Authority;
pub const Proof = roots.ProofFor(
    "antfly/index-build-owner-receipt/v2",
    "antfly/index-owner-accepted-root/v1",
);
