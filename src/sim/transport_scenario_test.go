package sim

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRandomScenario_WithTransportFaultActions(t *testing.T) {
	actions := []ScenarioAction{
		ActionSetLinkLatency,
		ActionWrite,
		ActionDropNextMsg,
		ActionDuplicateNextMsg,
		ActionCutLink,
		ActionWrite,
		ActionHealLink,
		ActionResetLinkLatency,
		ActionTick,
	}

	result, err := RunRandomScenarioWithActions(context.Background(), RandomScenarioConfig{
		Seed:                     301,
		BaseDir:                  filepath.Join(t.TempDir(), "docs-transport"),
		Start:                    time.Unix(1_700_700_000, 0).UTC(),
		Steps:                    len(actions),
		SplitTimeout:             30 * time.Second,
		SplitFinalizeGracePeriod: time.Second,
		ActionSettle:             1200 * time.Millisecond,
		StabilizeEvery:           len(actions) + 1,
	}, ScenarioRecord{
		Kind:    ScenarioKindDocuments,
		Seed:    301,
		Actions: actions,
	})
	require.NoError(t, err)
	require.Equal(t, actions, result.Actions)
	requireTransportFaultEvents(t, result.Events, ActionSetLinkLatency, ActionDropNextMsg, ActionDuplicateNextMsg, ActionCutLink)
}

func TestRandomTransactionScenario_WithTransportFaultActions(t *testing.T) {
	actions := []ScenarioAction{
		ActionSetLinkLatency,
		ActionTxn,
		ActionDropNextMsg,
		ActionDuplicateNextMsg,
		ActionCutLink,
		ActionHealLink,
		ActionResetLinkLatency,
		ActionTxn,
	}

	result, err := RunRandomTransactionScenarioWithActions(context.Background(), RandomTransactionScenarioConfig{
		Seed:           302,
		BaseDir:        filepath.Join(t.TempDir(), "txn-transport"),
		Start:          time.Unix(1_700_800_000, 0).UTC(),
		Steps:          len(actions),
		ActionSettle:   time.Second,
		StabilizeEvery: len(actions) + 1,
	}, ScenarioRecord{
		Kind:    ScenarioKindTransactions,
		Seed:    302,
		Actions: actions,
	})
	if err == nil {
		require.Equal(t, actions, result.Actions)
		requireTransportFaultEvents(t, result.Events, ActionSetLinkLatency, ActionDropNextMsg, ActionDuplicateNextMsg, ActionCutLink)
		return
	}

	var runErr *ScenarioRunError
	require.ErrorAs(t, err, &runErr)
	require.Equal(t, actions, runErr.Actions)
	require.Contains(t, runErr.Trace, "[transport_fault]")
}

func requireTransportFaultEvents(t *testing.T, events []TraceEvent, actions ...ScenarioAction) {
	t.Helper()

	require.NotEmpty(t, events)
	matched := make(map[ScenarioAction]bool, len(actions))
	for _, event := range events {
		if event.Kind != "transport_fault" {
			continue
		}
		for _, action := range actions {
			if matched[action] {
				continue
			}
			if containsTransportAction(event.Message, action) {
				matched[action] = true
			}
		}
	}
	for _, action := range actions {
		require.Truef(t, matched[action], "missing transport fault event for %s", action)
	}
}

func containsTransportAction(message string, action ScenarioAction) bool {
	return strings.Contains(message, "action="+string(action))
}
