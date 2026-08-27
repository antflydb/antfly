// Copyright 2026 Antfly, Inc.
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

/// Wire capability required to decode atomic table-topology transitions.
pub const current_version: u16 = 1;

/// Creating thousands of Raft groups is an operational workflow, not one
/// catalog request. Keep one create bounded in CPU, memory, and log growth.
pub const max_initial_ranges: u32 = 1024;

/// Canonical table definitions are copied into the Raft log and replication
/// messages. A small explicit ceiling prevents one request from monopolizing
/// the metadata runtime while retaining ample room for schemas and indexes.
pub const max_create_definition_bytes: usize = 2 * 1024 * 1024;

/// Defense in depth on the final encoded Raft command. This includes the
/// definition, generated range records, and codec overhead.
pub const max_transition_command_bytes: usize = 3 * 1024 * 1024;
