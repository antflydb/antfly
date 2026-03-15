// Copyright 2025 Antfly, Inc.
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

package common

import (
	"fmt"
	"strings"

	"github.com/antflydb/antfly/lib/types"
)

type Peer struct {
	ID types.ID `json:"id"`
	// Address is the address of the peer.
	URL string `json:"url"`
}
type Peers []Peer

func (rp Peers) String() string {
	r := "Peers{"
	var rSb18 strings.Builder
	for i, peer := range rp {
		if i > 0 {
			fmt.Fprintf(&rSb18, ", %s", types.ID(peer.ID))
		} else {
			rSb18.WriteString(types.ID(peer.ID).String())
		}
	}
	r += rSb18.String()
	r += "}"
	return r
}
