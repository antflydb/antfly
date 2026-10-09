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

use antfly_sdk::types::{InferenceDecideAnswer, InferenceDecideRequest, InferenceDecideResponse};
use serde_json::json;

#[test]
fn decision_requests_use_named_arrays_and_reject_private_aliases() {
    let wire = json!({
        "model": "embeddinggemma2",
        "inputs": [{"id": "first", "input": "Duplicate charge"}],
        "embedding_options": {"dimensions": 128, "task_type": "CLASSIFICATION"},
        "questions": [
            {"name": "route", "type": "choice", "instructions": "Route",
             "choices": [{"value": "billing"}, {"value": "support"}],
             "embedding_options": {"min_margin": 0.1}},
            {"name": "tags", "type": "multi_choice", "instructions": "Tags",
             "choices": [{"value": "refund"}, {"value": "urgent"}],
             "similarity_thresholds": {"refund": 0.4, "urgent": 0.5}}
        ]
    });
    let request: InferenceDecideRequest = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(request).unwrap(), wire);
    let mut old = wire.clone();
    old["questions"] = json!({"refund": {"type": "noul", "criteria": "Refund?"}});
    assert!(serde_json::from_value::<InferenceDecideRequest>(old).is_err());
    let mut old = wire;
    old["state"] = json!("Duplicate charge");
    assert!(serde_json::from_value::<InferenceDecideRequest>(old).is_err());
}

#[test]
fn decision_answer_unions_preserve_diagnostics_and_raw_similarity_boundaries() {
    let hash = "a".repeat(64);
    let wire = json!({
        "model": "fixture",
        "answers": [
            {"name": "route", "type": "choice", "decision_method": "typed",
             "choice": "billing", "probabilities": [{"value": "billing", "probability": 0.7}, {"value": "support", "probability": 0.3}],
             "confidence": 0.7, "confidence_method": "max_probability", "act_probability": 0.8},
            {"name": "urgency", "type": "score", "decision_method": "typed",
             "score": 0.5, "probabilities": [{"value": 0, "label": "Routine", "probability": 0.5}, {"value": 1, "label": "Soon", "probability": 0.5}],
             "confidence": 0.0, "confidence_method": "normalized_inverse_entropy"},
            {"name": "refund", "type": "predicate", "decision_method": "typed", "probability": 0.6},
            {"name": "similar_route", "type": "choice", "decision_method": "embedding_similarity",
             "choice": null, "status": "abstained", "abstention_reason": "tie", "margin": 0.0,
             "similarity_metric": "cosine", "prototype_set_hash": hash,
             "similarities": [{"value": "billing", "similarity": 0.3}, {"value": "support", "similarity": 0.3}]},
            {"name": "tags", "type": "multi_choice", "decision_method": "embedding_similarity",
             "choices": [], "status": "empty", "margin": 0.2,
             "similarity_metric": "cosine", "prototype_set_hash": hash,
             "similarity_thresholds": {"refund": 0.5, "urgent": 0.5},
             "similarities": [{"value": "refund", "similarity": 0.3}, {"value": "urgent", "similarity": 0.1}]}
        ],
        "usage": {"input_tokens": 12, "output_tokens": 0}
    });
    let response: InferenceDecideResponse = serde_json::from_value(wire.clone()).unwrap();
    assert!(matches!(
        response.answers[0],
        InferenceDecideAnswer::TrainedChoiceAnswer(_)
    ));
    assert!(matches!(
        response.answers[1],
        InferenceDecideAnswer::TrainedScoreAnswer(_)
    ));
    assert!(matches!(
        response.answers[2],
        InferenceDecideAnswer::TrainedPredicateAnswer(_)
    ));
    assert!(matches!(
        response.answers[3],
        InferenceDecideAnswer::EmbeddingChoiceAnswer(_)
    ));
    assert!(matches!(
        response.answers[4],
        InferenceDecideAnswer::EmbeddingMultiChoiceAnswer(_)
    ));
    assert_eq!(serde_json::to_value(response).unwrap(), wire);
    for field in ["confidence", "probability"] {
        let mut invalid = wire["answers"][3].clone();
        invalid[field] = json!(0.3);
        assert!(serde_json::from_value::<InferenceDecideAnswer>(invalid).is_err());
    }
}
