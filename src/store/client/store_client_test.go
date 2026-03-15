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

package client

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestStoreClientBatch_PropagatesTimestamp(t *testing.T) {
	var received db.BatchOp
	httpClient := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.NoError(t, proto.Unmarshal(body, &received))
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       http.NoBody,
				Header:     make(http.Header),
			}, nil
		}),
	}

	sc := NewStoreClient(httpClient, types.ID(1), "http://store.test")
	timestamp := uint64(123456789)
	ctx := storeutils.WithTimestamp(context.Background(), timestamp)

	err := sc.Batch(
		ctx,
		types.ID(42),
		[][2][]byte{{[]byte("doc-1"), []byte(`{"name":"test"}`)}},
		nil,
		nil,
		db.Op_SyncLevelWrite,
	)
	require.NoError(t, err)
	assert.Equal(t, timestamp, received.GetTimestamp())
}
