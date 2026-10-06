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

package memoryaf

import (
	"context"

	client "github.com/antflydb/antfly/go/pkg/sdk"
)

// Client is the minimal Antfly client interface required by memoryaf.
// Callers provide an implementation backed by their Antfly connection
// (e.g. a direct HTTP client or a cluster-aware wrapper).
type Client interface {
	CreateTable(ctx context.Context, tableName string, config *client.CreateTableRequest) error
	Batch(ctx context.Context, tableID string, batchRequest client.BatchRequest) (*client.BatchResult, error)
	QueryWithBody(ctx context.Context, requestBody []byte) ([]byte, error)
}
