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

package proxy

import (
	"fmt"
	"strings"
)

const (
	ViewPublished = "published"
	ViewLatest    = "latest"
)

func NormalizePolicy(policy RequestPolicy) RequestPolicy {
	if strings.TrimSpace(policy.View) == "" {
		policy.View = ViewPublished
	}
	return policy
}

func ValidatePolicy(req RequestContext) error {
	req.Policy = NormalizePolicy(req.Policy)

	switch req.Policy.View {
	case ViewPublished, ViewLatest:
	default:
		return fmt.Errorf("unsupported view %q", req.Policy.View)
	}

	if req.Operation == OperationWrite {
		if req.Policy.View == ViewLatest {
			return fmt.Errorf("write requests cannot require latest published view")
		}
		if req.Policy.MaxLagRecords > 0 {
			return fmt.Errorf("write requests cannot set max_lag_records")
		}
	}

	if req.Policy.RequiredVersion != nil && req.Policy.View == ViewLatest {
		return fmt.Errorf("required_version cannot be combined with latest view")
	}

	return nil
}
