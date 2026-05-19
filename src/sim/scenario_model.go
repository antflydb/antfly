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

package sim

import "fmt"

type ScenarioKind string

const (
	ScenarioKindDocuments    ScenarioKind = "documents"
	ScenarioKindTransactions ScenarioKind = "transactions"
)

type ScenarioAction string

const (
	ActionWrite             ScenarioAction = "write"
	ActionReallocate        ScenarioAction = "reallocate"
	ActionCrash             ScenarioAction = "crash"
	ActionRestart           ScenarioAction = "restart"
	ActionPartition         ScenarioAction = "partition"
	ActionHeal              ScenarioAction = "heal"
	ActionMetadataCrash     ScenarioAction = "metadata_crash"
	ActionMetadataRestart   ScenarioAction = "metadata_restart"
	ActionMetadataPartition ScenarioAction = "metadata_partition"
	ActionMetadataHeal      ScenarioAction = "metadata_heal"
	ActionTick              ScenarioAction = "tick"
	ActionDropNextMsg       ScenarioAction = "drop_next_msg"
	ActionDuplicateNextMsg  ScenarioAction = "duplicate_next_msg"
	ActionCutLink           ScenarioAction = "cut_link"
	ActionHealLink          ScenarioAction = "heal_link"
	ActionSetLinkLatency    ScenarioAction = "set_link_latency"
	ActionResetLinkLatency  ScenarioAction = "reset_link_latency"
	ActionTxn               ScenarioAction = "txn"
	ActionTxnCommitCrash    ScenarioAction = "txn_commit_crash"
	ActionTxnResolveCrash   ScenarioAction = "txn_resolve_crash"
)

type ScenarioRecord struct {
	Kind    ScenarioKind     `json:"kind"`
	Seed    int64            `json:"seed"`
	Actions []ScenarioAction `json:"actions"`
}

func (r ScenarioRecord) Validate() error {
	if r.Kind == "" {
		return fmt.Errorf("scenario kind is required")
	}
	for i, action := range r.Actions {
		if action == "" {
			return fmt.Errorf("scenario action %d is empty", i)
		}
	}
	return nil
}
