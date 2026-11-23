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
	"fmt"
	"strings"
	"unicode/utf8"
)

func notificationHex(value string, size int) bool {
	if len(value) != size {
		return false
	}
	for _, byte := range []byte(value) {
		if !(byte >= '0' && byte <= '9') && !(byte >= 'a' && byte <= 'f') {
			return false
		}
	}
	return true
}

// PollSQLNotifications receives committed LISTEN events without replaying SQL.
// Acknowledge a returned cursor only after consuming every event in its page.
func (c *AntflyClient) PollSQLNotifications(ctx context.Context, connection string, request SQLNotificationPollRequest) (*SQLNotificationPage, error) {
	if !notificationHex(connection, 32) || request.Limit < 0 || request.Limit > 128 || (request.Acknowledge != "" && !notificationHex(request.Acknowledge, 20)) {
		return nil, fmt.Errorf("invalid SQL notification connection, limit or acknowledgement")
	}
	response, err := c.client.PollSQLNotifications(ctx, connection, request)
	if err != nil {
		return nil, err
	}
	body, err := readSQLResourceResponse(response)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(body) {
		return nil, fmt.Errorf("invalid UTF-8 SQL notification page")
	}
	// Presence-aware decoding retains generated public types without accepting
	// missing mandatory fields as zero values or parsing the response twice.
	var raw struct {
		Notifications []struct {
			Channel   *string `json:"channel"`
			Payload   *string `json:"payload"`
			SenderPID *int32  `json:"sender_pid"`
		} `json:"notifications"`
		Cursor  json.RawMessage `json:"cursor"`
		Pending *bool           `json:"pending"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decoding SQL notifications: %w", err)
	}
	maximum := request.Limit
	if maximum == 0 {
		maximum = 128
	}
	if raw.Pending == nil || raw.Notifications == nil || len(raw.Notifications) > maximum {
		return nil, fmt.Errorf("invalid SQL notification page")
	}
	result := SQLNotificationPage{Notifications: make([]SQLNotification, len(raw.Notifications)), Pending: *raw.Pending}
	if len(raw.Cursor) != 0 {
		if json.Unmarshal(raw.Cursor, &result.Cursor) != nil || !notificationHex(result.Cursor, 20) {
			return nil, fmt.Errorf("invalid SQL notification cursor")
		}
	}
	if len(raw.Notifications) > 0 && result.Cursor == "" {
		return nil, fmt.Errorf("SQL notifications missing cursor")
	}
	total := 0
	for index, event := range raw.Notifications {
		if event.Channel == nil || event.Payload == nil || event.SenderPID == nil || *event.SenderPID <= 0 || len(*event.Channel) == 0 || len(*event.Channel) > 63 || len(*event.Payload) > 7999 || strings.IndexByte(*event.Channel, 0) >= 0 || strings.IndexByte(*event.Payload, 0) >= 0 {
			return nil, fmt.Errorf("invalid SQL notification event")
		}
		total += len(*event.Channel) + len(*event.Payload)
		if total > 64<<10 {
			return nil, fmt.Errorf("SQL notification page exceeds 64 KiB of logical data")
		}
		result.Notifications[index] = SQLNotification{Channel: *event.Channel, Payload: *event.Payload, SenderPid: *event.SenderPID}
	}
	return &result, nil
}
