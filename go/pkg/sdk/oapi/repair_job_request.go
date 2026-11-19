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

package oapi

import "encoding/json"

// RepairJobStartOptions controls a bounded table repair job. Its boolean
// Advance field retains the source contract of TableRepairJobStartRequest.
// Use WithAdvance(false) to disable automatic advancement explicitly.
type RepairJobStartOptions struct {
	Advance bool               `json:"advance,omitempty"`
	Cursor  string             `json:"cursor,omitempty"`
	Force   bool               `json:"force,omitempty"`
	Index   string             `json:"index,omitempty"`
	Kind    ArtifactRepairKind `json:"kind,omitempty"`
	Limit   uint32             `json:"limit,omitempty"`
	Target  RepairTarget       `json:"target,omitempty"`

	explicitAdvance bool
}

// WithAdvance returns a copy with an explicit automatic-advancement policy.
func (request RepairJobStartOptions) WithAdvance(advance bool) RepairJobStartOptions {
	request.Advance = advance
	request.explicitAdvance = true
	return request
}

type repairJobStartOptionsWire RepairJobStartOptions

func (request RepairJobStartOptions) MarshalJSON() ([]byte, error) {
	var advance *bool
	if request.Advance || request.explicitAdvance {
		advance = &request.Advance
	}
	return json.Marshal(struct {
		Advance *bool `json:"advance,omitempty"`
		repairJobStartOptionsWire
	}{Advance: advance, repairJobStartOptionsWire: repairJobStartOptionsWire(request)})
}

func (request *RepairJobStartOptions) UnmarshalJSON(body []byte) error {
	wire := struct {
		Advance *bool `json:"advance,omitempty"`
		*repairJobStartOptionsWire
	}{repairJobStartOptionsWire: (*repairJobStartOptionsWire)(request)}
	if err := json.Unmarshal(body, &wire); err != nil {
		return err
	}
	if wire.Advance != nil {
		request.Advance = *wire.Advance
		request.explicitAdvance = true
	}
	return nil
}
