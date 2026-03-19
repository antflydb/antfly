package sim

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	storedb "github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestHarness_TransactionDelayedResolveIntentDelivery(t *testing.T) {
	txnID := uuid.MustParse("00000000-0000-0000-0000-0000000000a1")
	h := newTransactionHarness(t, txnID)

	lowKey := "1/acct/alice"
	highKey := "a/acct/bob"
	lowShard, err := h.ShardForKey("accounts", lowKey)
	require.NoError(t, err)
	highShard, err := h.ShardForKey("accounts", highKey)
	require.NoError(t, err)
	require.NotEqual(t, lowShard, highShard)

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
			60*time.Second,
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
		t.Fatal("transaction completed before resolve-intent fault blocked it")
	case <-time.After(5 * time.Second):
		t.Fatal("resolve-intent fault was not reached")
	}

	select {
	case err := <-done:
		require.NoError(t, err)
		t.Fatal("transaction completed while resolve-intent delivery was blocked")
	default:
	}

	close(releaseResolve)
	require.NoError(t, <-done)

	lowValue, err := h.LookupKey("accounts", lowKey)
	require.NoError(t, err)
	require.NotEmpty(t, lowValue)

	highValue, err := h.LookupKey("accounts", highKey)
	require.NoError(t, err)
	require.NotEmpty(t, highValue)
}

func TestHarness_TransactionCoordinatorCrashAfterCommitStillCompletes(t *testing.T) {
	txnID := uuid.MustParse("00000000-0000-0000-0000-0000000000b2")
	h := newTransactionHarness(t, txnID)

	lowKey := "1/acct/alice"
	highKey := "a/acct/bob"
	lowShard, err := h.ShardForKey("accounts", lowKey)
	require.NoError(t, err)
	highShard, err := h.ShardForKey("accounts", highKey)
	require.NoError(t, err)
	require.NotEqual(t, lowShard, highShard)

	coordinatorShard := pickCoordinatorForTest(txnID, lowShard, highShard)
	crashedStore := make(chan types.ID, 1)

	h.AddAfterRPCFault(1, func(event RPCEvent) bool {
		return event.Method == "CommitTransaction" && event.ShardID == coordinatorShard
	}, func(ctx context.Context, event RPCEvent) error {
		if err := h.CrashStore(event.NodeID); err != nil {
			return err
		}
		select {
		case crashedStore <- event.NodeID:
		default:
		}
		return nil
	})

	require.NoError(t, h.ExecuteTransaction(
		45*time.Second,
		map[types.ID][][2][]byte{
			lowShard:  {{[]byte(lowKey), []byte(`{"owner":"alice","balance":101}`)}},
			highShard: {{[]byte(highKey), []byte(`{"owner":"bob","balance":51}`)}},
		},
		nil,
		nil,
		nil,
		storedb.Op_SyncLevelWrite,
	))

	select {
	case crashedNodeID := <-crashedStore:
		require.NotZero(t, crashedNodeID)
	default:
		t.Fatal("coordinator crash fault did not trigger")
	}

	require.NoError(t, h.WaitFor(60*time.Second, func() error {
		lowValue, err := h.LookupKey("accounts", lowKey)
		if err != nil {
			return err
		}
		if len(lowValue) == 0 {
			return fmt.Errorf("coordinator key %q not visible yet", lowKey)
		}

		highValue, err := h.LookupKey("accounts", highKey)
		if err != nil {
			return err
		}
		if len(highValue) == 0 {
			return fmt.Errorf("participant key %q not visible yet", highKey)
		}
		return nil
	}))
}

func TestHarness_TransactionParticipantCrashDuringWriteIntentStillCompletes(t *testing.T) {
	txnID := uuid.MustParse("00000000-0000-0000-0000-0000000000c3")
	h := newTransactionHarness(t, txnID)

	lowKey := "1/acct/alice"
	highKey := "a/acct/bob"
	lowShard, err := h.ShardForKey("accounts", lowKey)
	require.NoError(t, err)
	highShard, err := h.ShardForKey("accounts", highKey)
	require.NoError(t, err)
	require.NotEqual(t, lowShard, highShard)

	coordinatorShard := pickCoordinatorForTest(txnID, lowShard, highShard)
	participantShard := highShard
	if coordinatorShard == highShard {
		participantShard = lowShard
	}

	crashedStore := make(chan types.ID, 1)
	h.AddAfterRPCFault(1, func(event RPCEvent) bool {
		return event.Method == "ResolveIntent" && event.ShardID == participantShard
	}, func(ctx context.Context, event RPCEvent) error {
		if err := h.CrashStore(event.NodeID); err != nil {
			return err
		}
		select {
		case crashedStore <- event.NodeID:
		default:
		}
		return nil
	})

	require.NoError(t, h.ExecuteTransaction(
		45*time.Second,
		map[types.ID][][2][]byte{
			lowShard:  {{[]byte(lowKey), []byte(`{"owner":"alice","balance":102}`)}},
			highShard: {{[]byte(highKey), []byte(`{"owner":"bob","balance":52}`)}},
		},
		nil,
		nil,
		nil,
		storedb.Op_SyncLevelWrite,
	))

	select {
	case crashedNodeID := <-crashedStore:
		require.NotZero(t, crashedNodeID)
	default:
		t.Fatal("participant crash fault did not trigger")
	}

	require.NoError(t, h.WaitFor(30*time.Second, func() error {
		lowValue, err := h.LookupKey("accounts", lowKey)
		if err != nil {
			return err
		}
		if len(lowValue) == 0 {
			return fmt.Errorf("coordinator key %q not visible yet", lowKey)
		}

		highValue, err := h.LookupKey("accounts", highKey)
		if err != nil {
			return err
		}
		if len(highValue) == 0 {
			return fmt.Errorf("participant key %q not visible yet", highKey)
		}
		return nil
	}))
}

func newTransactionHarness(t *testing.T, txnID uuid.UUID) *Harness {
	t.Helper()

	h, err := NewHarness(HarnessConfig{
		BaseDir:           t.TempDir(),
		Start:             time.Unix(1_700_200_000, 0).UTC(),
		MetadataID:        100,
		StoreIDs:          []types.ID{1, 2, 3},
		ReplicationFactor: 3,
		TxnIDGenerator: func() uuid.UUID {
			return txnID
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, h.Close())
	})

	_, err = h.CreateTable("accounts", tablemgr.TableConfig{
		NumShards: 4,
		StartID:   0x3000,
	})
	require.NoError(t, err)

	startTableOnAllStores(t, h, "accounts")
	return h
}
