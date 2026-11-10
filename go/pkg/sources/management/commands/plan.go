// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

package commands

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

type PlanOptions struct {
	Identity           func(key string) (string, error)
	ResolveConnection  func(ctx context.Context, ref string) (string, error)
	DefaultPermissions func(ctx context.Context) (management.Permissions, error)
	Extensions         func(key string, ext map[string]json.RawMessage, c management.Capabilities) error
}

type PlanAction string

const (
	PlanCreate    PlanAction = "create"
	PlanUpdate    PlanAction = "update"
	PlanUnchanged PlanAction = "unchanged"
)

type PlanItem struct {
	Key      string            `json:"key"`
	SourceID string            `json:"source_id"`
	Action   PlanAction        `json:"action"`
	Changes  []string          `json:"changes,omitempty"`
	Revision string            `json:"revision,omitempty"`
	Desired  management.Source `json:"-"`
}

type Plan struct {
	Items []PlanItem `json:"items"`
}

type ApplyResult struct {
	Key      string     `json:"key"`
	SourceID string     `json:"source_id"`
	Action   PlanAction `json:"action"`
	Outcome  string     `json:"outcome"`
	Revision string     `json:"revision,omitempty"`
	Error    string     `json:"error,omitempty"`
}

func PlanManifest(ctx context.Context, env Env, m Manifest, opts PlanOptions) (Plan, error) {
	return Plan{}, errors.New("not implemented")
}

func ApplyManifest(ctx context.Context, env Env, m Manifest, opts PlanOptions) ([]ApplyResult, error) {
	return nil, errors.New("not implemented")
}
