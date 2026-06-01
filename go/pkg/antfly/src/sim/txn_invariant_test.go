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

import (
	"context"
	"testing"
	"time"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	storedb "github.com/antflydb/antfly/go/pkg/antfly/src/store/db"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestChecker_StableRejectsPendingTransactionState(t *testing.T) {
	txnID := uuid.MustParse("00000000-0000-0000-0000-0000000000d4")
	h := newTransactionHarness(t, txnID)
	checker := NewChecker(CheckerConfig{SplitLivenessTimeout: 30 * time.Second})

	lowKey := "1/acct/alice"
	highKey := "a/acct/bob"
	lowShard, err := h.ShardForKey("accounts", lowKey)
	require.NoError(t, err)
	highShard, err := h.ShardForKey("accounts", highKey)
	require.NoError(t, err)

	coordinatorShard := pickCoordinatorForTest(txnID, lowShard, highShard)
	participantShard := highShard
	if coordinatorShard == highShard {
		participantShard = lowShard
	}

	blockedResolve := make(chan struct{}, 1)
	releaseResolve := make(chan struct{})
	h.AddBeforeRPCFault(1, func(event RPCEvent) bool {
		return event.Method == "ResolveIntent" && event.ShardID == participantShard
	}, func(ctx context.Context, event RPCEvent) error {
		select {
		case blockedResolve <- struct{}{}:
		default:
		}
		select {
		case <-releaseResolve:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	done := make(chan error, 1)
	go func() {
		done <- h.ExecuteTransaction(
			simulatedTxnOpTimeout,
			map[types.ID][][2][]byte{
				lowShard:  {{[]byte(lowKey), []byte(`{"owner":"alice","balance":100}`)}},
				highShard: {{[]byte(highKey), []byte(`{"owner":"bob","balance":50}`)}},
			},
			nil,
			nil,
			nil,
			storedb.Op_SyncLevelWrite,
		)
	}()

	select {
	case <-blockedResolve:
	case err := <-done:
		require.NoError(t, err)
		t.Fatal("transaction completed before resolve block")
	case <-time.After(5 * time.Second):
		t.Fatal("resolve-intent fault was not reached")
	}

	err = checker.CheckStable(context.Background(), h)
	require.Error(t, err)
	require.Contains(t, err.Error(), "transaction")

	close(releaseResolve)
	require.NoError(t, <-done)
	require.NoError(t, h.WaitFor(60*time.Second, func() error {
		return checker.CheckStable(context.Background(), h)
	}))
}

func TestChecker_StableRejectsOrphanedIntents(t *testing.T) {
	checker := NewChecker(CheckerConfig{SplitLivenessTimeout: 30 * time.Second})
	txnID := uuid.MustParse("00000000-0000-0000-0000-0000000000e5")
	err := checker.checkTransactionStabilitySnapshot(&ClusterTxnSnapshot{
		Records: map[string]TxnRecordRef{},
		Intents: map[string][]TxnIntentRef{
			txnKeyString(txnID[:]): {{
				StoreID: 1,
				ShardID: 3001,
				Intent: storedb.TxnIntent{
					TxnID:   txnID[:],
					UserKey: []byte("1/acct/orphan"),
					Status:  storedb.TxnStatusPending,
				},
			}},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "orphaned intents")
}
