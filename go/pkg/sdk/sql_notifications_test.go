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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSQLNotificationsRetainOpaqueCursorsAndNeverExecuteSQL(t *testing.T) {
	const cursor = "ffffffffffffffff007f"
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/db/v1/sql/connections/"+strings.Repeat("a", 32)+"/notifications" {
			t.Errorf("unexpected poll: %s %s", r.Method, r.URL.Path)
		}
		var request SQLNotificationPollRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		if calls == 1 {
			if request.Acknowledge != "" || request.Limit != 1 {
				t.Errorf("unexpected request: %+v", request)
			}
			_, _ = w.Write([]byte(`{"notifications":[{"channel":"events","payload":"changed","sender_pid":42}],"pending":false,"cursor":"` + cursor + `"}`))
		} else {
			if request.Acknowledge != cursor {
				t.Errorf("lost cursor: %+v", request)
			}
			_, _ = w.Write([]byte(`{"notifications":[],"pending":false}`))
		}
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.PollSQLNotifications(context.Background(), strings.Repeat("a", 32), SQLNotificationPollRequest{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if first.Cursor != cursor || len(first.Notifications) != 1 || first.Notifications[0].SenderPid != 42 {
		t.Fatalf("unexpected page: %+v", first)
	}
	if _, err := client.PollSQLNotifications(context.Background(), strings.Repeat("a", 32), SQLNotificationPollRequest{Acknowledge: first.Cursor}); err != nil {
		t.Fatal(err)
	}
	for _, request := range []SQLNotificationPollRequest{{Limit: 129}, {Limit: -1}, {Acknowledge: "invalid"}} {
		if _, err := client.PollSQLNotifications(context.Background(), strings.Repeat("a", 32), request); err == nil {
			t.Fatal("accepted invalid poll")
		}
	}
	if calls != 2 {
		t.Fatalf("invalid input reached transport: %d calls", calls)
	}
}

func TestSQLNotificationsRejectMalformedOrOversizedPages(t *testing.T) {
	for _, body := range []string{
		`{"notifications":[],"pending":null}`,
		`{"notifications":null,"pending":false}`,
		`{"notifications":[],"pending":false,"cursor":null}`,
		`{"notifications":[],"pending":false,"cursor":"bad"}`,
		`{"notifications":[{"channel":"events","sender_pid":42}],"pending":false,"cursor":"00000000000000010000"}`,
		`{"notifications":[{"channel":"events","payload":"x","sender_pid":0}],"pending":false,"cursor":"00000000000000010000"}`,
		strings.Repeat(" ", (1<<20)+1),
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
		client, err := NewAntflyClient(server.URL, server.Client())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.PollSQLNotifications(context.Background(), strings.Repeat("a", 32), SQLNotificationPollRequest{}); err == nil {
			t.Errorf("accepted invalid page of %d bytes", len(body))
		}
		server.Close()
	}
}

func TestSQLNotificationRedirectDoesNotFollowOrReplay(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Location", "/trap")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client, err := NewAntflyClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PollSQLNotifications(context.Background(), strings.Repeat("a", 32), SQLNotificationPollRequest{}); err == nil {
		t.Fatal("accepted redirect")
	}
	if calls != 1 {
		t.Fatalf("poll replayed %d times", calls)
	}
}
