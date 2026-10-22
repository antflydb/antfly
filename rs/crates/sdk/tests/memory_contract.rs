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

use antfly_sdk::types::{MemoryRecallRequest, MemorySpaceConfig};

#[test]
fn memory_native_uint64_configuration_round_trips() {
    let config: MemorySpaceConfig = serde_json::from_value(serde_json::json!({
        "retention_ms": u64::MAX,
    }))
    .unwrap();
    let encoded = serde_json::to_value(config).unwrap();
    assert_eq!(encoded["retention_ms"].as_u64(), Some(u64::MAX));
}

#[test]
fn memory_provisional_filter_distinguishes_false_from_the_native_default() {
    let defaults: MemoryRecallRequest = serde_json::from_str("{}").unwrap();
    let excluded: MemoryRecallRequest =
        serde_json::from_str(r#"{"include_provisional":false}"#).unwrap();
    let default_json = serde_json::to_value(defaults).unwrap();
    let excluded_json = serde_json::to_value(excluded).unwrap();
    assert_eq!(default_json["include_provisional"].as_bool(), Some(true));
    assert_eq!(excluded_json["include_provisional"].as_bool(), Some(false));
}

#[test]
fn memory_generated_read_types_decode_native_service_fixtures() {
    use antfly_sdk::types::{
        MemoryEntityCandidatePage, MemoryEntityProposalDetail, MemoryEntityProposalPage,
    };
    let candidates: MemoryEntityCandidatePage = serde_json::from_str(include_str!(
        "../../../../zig/pkg/antfly-embedded/src/memory/fixtures/wire/candidates.json"
    ))
    .unwrap();
    let proposals: MemoryEntityProposalPage = serde_json::from_str(include_str!(
        "../../../../zig/pkg/antfly-embedded/src/memory/fixtures/wire/proposals.json"
    ))
    .unwrap();
    let details: MemoryEntityProposalDetail = serde_json::from_str(include_str!(
        "../../../../zig/pkg/antfly-embedded/src/memory/fixtures/wire/proposal.json"
    ))
    .unwrap();
    let candidates = serde_json::to_value(candidates).unwrap();
    let proposals = serde_json::to_value(proposals).unwrap();
    let details = serde_json::to_value(details).unwrap();
    assert_eq!(candidates["seed"]["entity"]["id"], "a");
    assert_eq!(candidates["items"][0]["view"]["canonical_id"], "b");
    assert_eq!(details["proposal"]["id"], proposals["items"][0]["id"]);
    assert_eq!(details["inputs"]["actor"], "codex/consolidation");
    assert_eq!(details["inputs"]["entities"][0]["revision"], 3);
    assert_eq!(details["stale_configuration"], false);
}
