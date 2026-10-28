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
	"github.com/antflydb/antfly/go/pkg/sdk/admission"
	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

// RequestAdmissionConfig bounds local operations and queue waiting.
type RequestAdmissionConfig = admission.Config

// ClientCapacityError identifies a refusal before transmission.
type ClientCapacityError = admission.CapacityError

// BoundedHTTPClient shares bounded HTTP capacity until response body retirement.
type BoundedHTTPClient = admission.Client

// NewBoundedHTTPClient creates or shares one admission owner. Nil policy selects
// 100 active requests, 100 waiters and a one-second queue ceiling.
func NewBoundedHTTPClient(next oapi.HttpRequestDoer, policy *RequestAdmissionConfig) (*BoundedHTTPClient, error) {
	return admission.New(next, policy)
}

func withRequestAdmission(out **BoundedHTTPClient) oapi.ClientOption {
	return func(client *oapi.Client) error {
		bounded, err := NewBoundedHTTPClient(client.Client, nil)
		if err != nil {
			return err
		}
		client.Client = bounded
		*out = bounded
		return nil
	}
}
