// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy
// of the License at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

/// Durable, monotonic phases for replacing the dense-index LSM projection
/// with native WAL-backed generations. The ordinal order is part of the
/// metadata wire format and supports conservative least-phase aggregation.
pub const DenseNativeStoragePhase = enum(u8) {
    legacy = 0,
    native_building = 1,
    native_validating = 2,
    native_authoritative = 3,
};
