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
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors returned by the store client. Use errors.Is to check
// whether an error (possibly wrapped) matches a specific category.
// These are classified from HTTP response status codes and body content
// via ResponseError.Is.
var (
	ErrNotFound          = errors.New("not found")
	ErrAlreadyExists     = errors.New("already exists")
	ErrKeyOutOfRange     = errors.New("key out of range")
	ErrVersionConflict   = errors.New("version conflict")
	ErrIntentConflict    = errors.New("intent conflict")
	ErrShardInitializing = errors.New("shard is initializing")
	ErrProposalDropped   = errors.New("proposal dropped")
	ErrNotLeader         = errors.New("not leader")
	ErrShardNotReady     = errors.New("shard not ready")
	ErrNoHealthyPeer     = errors.New("no healthy peer")
	ErrNoRaftStatus      = errors.New("no raft status")
)

// ResponseError represents an error response from a store node.
// It carries the HTTP status code and response body, and implements
// errors.Is to classify the error against the sentinel errors above.
type ResponseError struct {
	StatusCode int
	Body       string
}

func (e *ResponseError) Error() string {
	return fmt.Sprintf("error response from node: %d %s", e.StatusCode, e.Body)
}

// Is implements errors.Is matching against sentinel errors by inspecting
// the HTTP status code and response body content.
func (e *ResponseError) Is(target error) bool {
	bodyLower := strings.ToLower(e.Body)
	switch {
	case errors.Is(target, ErrNotFound):
		return e.StatusCode == 404 || strings.Contains(bodyLower, "not found")
	case errors.Is(target, ErrAlreadyExists):
		return strings.Contains(bodyLower, "already exists")
	case errors.Is(target, ErrKeyOutOfRange):
		return strings.Contains(bodyLower, "out of range")
	case errors.Is(target, ErrVersionConflict):
		return strings.Contains(bodyLower, "version predicate check failed") ||
			strings.Contains(bodyLower, "version conflict on key")
	case errors.Is(target, ErrIntentConflict):
		return strings.Contains(bodyLower, "intent conflict")
	case errors.Is(target, ErrShardInitializing):
		return strings.Contains(bodyLower, "shard is initializing")
	case errors.Is(target, ErrProposalDropped):
		return strings.Contains(bodyLower, "proposal dropped")
	case errors.Is(target, ErrNotLeader):
		return strings.Contains(bodyLower, "not leader") || strings.Contains(bodyLower, "no leader")
	case errors.Is(target, ErrShardNotReady):
		return strings.Contains(bodyLower, "shard not ready")
	case errors.Is(target, ErrNoHealthyPeer):
		return strings.Contains(bodyLower, "no healthy peer")
	case errors.Is(target, ErrNoRaftStatus):
		return strings.Contains(bodyLower, "no raft status")
	}
	return false
}
