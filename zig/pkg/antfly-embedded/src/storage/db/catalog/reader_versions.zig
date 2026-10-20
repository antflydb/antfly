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

//! Durable feature epochs shared by catalog admission and peer negotiation.
//! Keep this module independent of physical storage and producer implementations.

pub const graph_expiration_namespace_reader_version: u32 = 4;
pub const adjudication_reader_version: u32 = 5;
pub const curation_reader_version: u32 = 6;
pub const generated_producers_reader_version: u32 = 7;
pub const grouped_sources_reader_version: u32 = 8;
pub const namespace_curation_reader_version: u32 = 9;
/// Required preparation retains exact participant and route bindings.
pub const prepared_transaction_reader_version: u32 = 10;
/// Per-transaction replay retention claims and atomic inventory adjustment.
pub const replay_credit_reader_version: u32 = 11;
pub const supported_reader_version: u32 = replay_credit_reader_version;
pub const minimum_reader_key = "\x00\x00__metadata__:minimum_reader_version";
pub const replacement_reader_base_key = "\x00\x00__metadata__:replacement_reader_base_v1";
