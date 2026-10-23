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

//! Durable standalone catalog keys shared by the server and offline operators.
pub const head_key = "catalog-v2/head";
pub const row_prefix = "catalog-v2/row/";
pub const Head = struct {
    version: u16 = 1,
    epoch: u64,
    revision: u64,
    next_id: u64,
};
