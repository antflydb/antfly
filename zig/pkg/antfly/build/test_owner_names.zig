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

// Compiler labels participate in both resource admission and unit ownership.
// Share their identity so changing a label cannot silently bypass exclusions.
pub const root_unit = "root-unit-tests";
pub const capi_contract = "capi-contract-tests";
pub const api_storage_authority = "api-storage-authority-tests";
