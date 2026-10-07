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

/// Durable, monotonic phases for replacing the dense-index LSM projection
/// with native WAL-backed generations. The ordinal order is part of the
/// metadata wire format and supports conservative least-phase aggregation.
pub const DenseNativeStoragePhase = enum(u8) {
    legacy = 0,
    native_building = 1,
    native_validating = 2,
    native_authoritative = 3,
};
