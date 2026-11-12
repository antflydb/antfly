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

//! Shared durable dataset contract; native producers and object serving use the same types.
const contract = @import("antfly_local_sources").retained_dataset_types;
pub const Id = contract.Id;
pub const max_public_counter = contract.max_public_counter;
pub const Kind = contract.Kind;
pub const Access = contract.Access;
pub const Law = contract.Law;
pub const Measure = contract.Measure;
pub const Summary = contract.Summary;
pub const Definition = contract.Definition;
pub const Range = contract.Range;
pub const SourcePosition = contract.SourcePosition;
pub const Segment = contract.Segment;
pub const RequiredSummary = contract.RequiredSummary;
pub const requireSourceReclamation = contract.requireSourceReclamation;
pub const zero = contract.zero;
pub const writeFields = contract.writeFields;
pub const RowKey = contract.RowKey;
pub const KeySpan = contract.KeySpan;
pub const rowKey = contract.rowKey;
