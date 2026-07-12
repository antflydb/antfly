package oapi

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestParseRestoreTableAcceptedResponses(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		committed bool
	}{
		{
			name: "asynchronous restore triggered",
			body: `{"restore":"triggered"}`,
		},
		{
			name:      "committed restore durability pending",
			body:      `{"restore":"committed","durability":"pending"}`,
			committed: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := ParseRestoreTableResponse(&http.Response{
				StatusCode: http.StatusAccepted,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(test.body)),
			})
			if err != nil {
				t.Fatalf("parse restore response: %v", err)
			}
			if response.JSON202 == nil {
				t.Fatal("accepted restore response was not decoded")
			}
			if test.committed {
				accepted, err := response.JSON202.AsRestoreCommittedPendingResponse()
				if err != nil {
					t.Fatalf("decode committed accepted restore: %v", err)
				}
				if accepted.Restore != RestoreCommittedPendingResponseRestoreCommitted {
					t.Fatalf("restore = %q, want committed", accepted.Restore)
				}
				if accepted.Durability != RestoreCommittedPendingResponseDurabilityPending {
					t.Fatalf("durability = %q, want pending", accepted.Durability)
				}
				return
			}
			accepted, err := response.JSON202.AsRestoreTriggeredResponse()
			if err != nil {
				t.Fatalf("decode triggered accepted restore: %v", err)
			}
			if accepted.Restore != RestoreTriggeredResponseRestoreTriggered {
				t.Fatalf("restore = %q, want triggered", accepted.Restore)
			}
		})
	}
}
