package oapi

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestParseRestoreTableAcceptedResponses(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		restore    RestoreAcceptedResponseRestore
		durability RestoreAcceptedResponseDurability
	}{
		{
			name:    "asynchronous restore triggered",
			body:    `{"restore":"triggered"}`,
			restore: RestoreAcceptedResponseRestoreTriggered,
		},
		{
			name:       "committed restore durability pending",
			body:       `{"restore":"committed","durability":"pending"}`,
			restore:    RestoreAcceptedResponseRestoreCommitted,
			durability: RestoreAcceptedResponseDurabilityPending,
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
			if response.JSON202.Restore != test.restore {
				t.Fatalf("restore = %q, want %q", response.JSON202.Restore, test.restore)
			}
			if response.JSON202.Durability != test.durability {
				t.Fatalf("durability = %q, want %q", response.JSON202.Durability, test.durability)
			}
		})
	}
}
