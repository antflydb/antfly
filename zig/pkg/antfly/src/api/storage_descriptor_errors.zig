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

//! Descriptor exhaustion has one availability and reclamation policy across
//! local writer opens and routed reads. Never include unrelated I/O failures.
pub const failures = [_]anyerror{
    error.PersistentDescriptorAdmissionExhausted,
    error.StorageDescriptorPressure,
    error.ProcessFdQuotaExceeded,
    error.SystemFdQuotaExceeded,
};

pub fn isPressure(err: anyerror) bool {
    inline for (failures) |failure| if (err == failure) return true;
    return false;
}
