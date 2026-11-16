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

package sources

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"time"
)

// Only explicitly read-only operations enter this policy. POST /query is safe
// to repeat with the identical bounds; batch/merge/create must never enter it.
func (p *Publisher) callRead(ctx context.Context, method, path string, body any) ([]byte, error) {
	wait, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for attempt := 0; ; attempt++ {
		if err := wait.Err(); err != nil {
			return nil, err
		}
		data, err := p.callContentType(wait, method, path, body, "application/json")
		if wait.Err() != nil {
			return nil, wait.Err()
		}
		if err == nil {
			return data, nil
		}
		if attempt >= 2 || !retryableSourceRead(err) {
			return nil, err
		}
		delay := (100 * time.Millisecond) << attempt
		var status *PublishHTTPError
		if errors.As(err, &status) && !status.RetryAt.IsZero() {
			delay = max(delay, time.Until(status.RetryAt))
		}
		deadline, _ := wait.Deadline()
		if delay >= time.Until(deadline) {
			return nil, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-wait.Done():
			timer.Stop()
			return nil, wait.Err()
		case <-timer.C:
		}
	}
}

func retryableSourceRead(err error) bool {
	var status *PublishHTTPError
	if errors.As(err, &status) {
		switch status.Status {
		case 408, 425, 429, 500, 502, 503, 504:
			return true
		}
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var network net.Error
	return errors.As(err, &network) && network.Timeout()
}
