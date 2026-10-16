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

package sdk

import (
	"encoding/json"
	"testing"
)

func TestIndexPublicationRecoveryPublicTypes(t *testing.T) {
	const body = `{"index_type":"relational","index_name":"by_email","publication_token":"recovery-handle","table_id":"18446744073709551615","publication_id":"22222222222222222222222222222222","plan_digest":"3333333333333333333333333333333333333333333333333333333333333333","metadata_incarnation":"11111111111111111111111111111111","state":"admission_unknown","replication_pending":true,"phase":"published","owner_count":2}`
	var status IndexPublicationStatus
	if err := json.Unmarshal([]byte(body), &status); err != nil {
		t.Fatal(err)
	}
	if status.State != IndexPublicationStateAdmissionUnknown || status.Phase != IndexPublicationPhasePublished || !status.ReplicationPending {
		t.Fatalf("unmirrored historical publication must remain unknown: %+v", status)
	}
	if status.TableId != "18446744073709551615" {
		t.Fatalf("table identity lost precision: %q", status.TableId)
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip IndexPublicationStatus
	if err := json.Unmarshal(encoded, &roundTrip); err != nil || roundTrip != status {
		t.Fatalf("publication round trip: %v, %+v", err, roundTrip)
	}
	var receipt SQLDDLReceipt
	if err := json.Unmarshal([]byte(`{"index_publication_token":"recovery-handle","fk_generation_publication_id":"22222222222222222222222222222222"}`), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.IndexPublicationToken != status.PublicationToken || receipt.FkGenerationPublicationId != status.PublicationId {
		t.Fatalf("receipt lost recovery identities: %+v", receipt)
	}
}
