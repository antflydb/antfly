package oapi

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

func TestParseBackupMetadataUnavailableResponses(t *testing.T) {
	tests := []struct {
		name               string
		body               string
		retryAfter         string
		notLeader          string
		assertTypedVariant func(*testing.T, BackupMetadataUnavailable)
	}{
		{
			name:       "capability",
			body:       `{"code":"metadata_capability_unavailable","error":"metadata_capability_unavailable","message":"upgrade metadata nodes","required_capability":"linearizable_snapshot","retryable":true,"retry_after_ms":5000}`,
			retryAfter: "5",
			assertTypedVariant: func(t *testing.T, value BackupMetadataUnavailable) {
				decoded, err := value.AsMetadataCapabilityUnavailableError()
				if err != nil {
					t.Fatalf("decode capability response: %v", err)
				}
				if decoded.RequiredCapability != MetadataCapabilityUnavailableErrorRequiredCapabilityLinearizableSnapshot {
					t.Fatalf("required capability = %q", decoded.RequiredCapability)
				}
			},
		},
		{
			name:       "leader",
			body:       `{"code":"metadata_leader_unavailable","error":"metadata_leader_unavailable","message":"metadata leader unavailable","retryable":true,"retry_after_ms":1000}`,
			retryAfter: "1",
			notLeader:  "true",
			assertTypedVariant: func(t *testing.T, value BackupMetadataUnavailable) {
				decoded, err := value.AsMetadataLeaderUnavailableError()
				if err != nil {
					t.Fatalf("decode leader response: %v", err)
				}
				if decoded.Error != MetadataLeaderUnavailableErrorErrorMetadataLeaderUnavailable {
					t.Fatalf("leader error = %q", decoded.Error)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			newResponse := func() *http.Response {
				header := http.Header{
					"Content-Type": []string{"application/json"},
					"Retry-After":  []string{test.retryAfter},
				}
				if test.notLeader != "" {
					header.Set("X-Antfly-Metadata-Not-Leader", test.notLeader)
				}
				return &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Header:     header,
					Body:       io.NopCloser(strings.NewReader(test.body)),
				}
			}

			response, err := ParseBackupResponse(newResponse())
			if err != nil {
				t.Fatalf("parse backup response: %v", err)
			}
			if response.JSON503 == nil || response.Headers503 == nil {
				t.Fatal("typed 503 response was not decoded")
			}
			expectedRetryAfter, err := strconv.Atoi(test.retryAfter)
			if err != nil {
				t.Fatalf("parse Retry-After fixture: %v", err)
			}
			if response.Headers503.RetryAfter != expectedRetryAfter {
				t.Fatalf("Retry-After = %d", response.Headers503.RetryAfter)
			}
			if response.Headers503.XAntflyMetadataNotLeader != test.notLeader {
				t.Fatalf("metadata leader header = %q", response.Headers503.XAntflyMetadataNotLeader)
			}
			test.assertTypedVariant(t, *response.JSON503)

			tableResponse, err := ParseBackupTableResponse(newResponse())
			if err != nil {
				t.Fatalf("parse table backup response: %v", err)
			}
			if tableResponse.JSON503 == nil || tableResponse.Headers503 == nil {
				t.Fatal("typed table backup 503 response was not decoded")
			}
			if tableResponse.Headers503.RetryAfter != expectedRetryAfter {
				t.Fatalf("table backup Retry-After = %d", tableResponse.Headers503.RetryAfter)
			}
			if tableResponse.Headers503.XAntflyMetadataNotLeader != test.notLeader {
				t.Fatalf("table backup metadata leader header = %q", tableResponse.Headers503.XAntflyMetadataNotLeader)
			}
			test.assertTypedVariant(t, *tableResponse.JSON503)
		})
	}
}
