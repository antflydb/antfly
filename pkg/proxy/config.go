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

package proxy

import (
	"encoding/json"
	"fmt"
	"strings"
)

type routeConfig struct {
	Tenant             string `json:"tenant"`
	Table              string `json:"table"`
	Namespace          string `json:"namespace"`
	ServingNamespace   string `json:"serving_namespace"`
	PreferredBackend   string `json:"preferred_backend"`
	AllowStateful      bool   `json:"allow_stateful"`
	AllowServerless    bool   `json:"allow_serverless"`
	StatefulURL        string `json:"stateful_url"`
	ServerlessQueryURL string `json:"serverless_query_url"`
	ServerlessAPIURL   string `json:"serverless_api_url"`
}

func ParseRoutesJSON(raw string) ([]NamespaceRoute, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var decoded []routeConfig
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, fmt.Errorf("parse routes json: %w", err)
	}
	routes := make([]NamespaceRoute, 0, len(decoded))
	for _, route := range decoded {
		table := firstNonEmpty(route.Table, route.Namespace)
		namespace := firstNonEmpty(route.ServingNamespace, route.Namespace, route.Table)
		routes = append(routes, NamespaceRoute{
			Tenant:             route.Tenant,
			Table:              table,
			Namespace:          namespace,
			PreferredBackend:   BackendKind(route.PreferredBackend),
			AllowStateful:      route.AllowStateful,
			AllowServerless:    route.AllowServerless,
			StatefulURL:        route.StatefulURL,
			ServerlessQueryURL: route.ServerlessQueryURL,
			ServerlessAPIURL:   route.ServerlessAPIURL,
		})
	}
	return routes, nil
}
