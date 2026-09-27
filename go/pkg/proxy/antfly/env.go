// Copyright 2026 Antfly, Inc.
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

package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type GatewayEnvConfig struct {
	PublicAddr  string
	RequireAuth bool
	RoutesFile  string
	TokensFile  string
	Routes      []NamespaceRoute
	Tokens      map[string]Principal
}

func LoadGatewayEnvConfig(getenv func(string) string) (GatewayEnvConfig, error) {
	if getenv == nil {
		getenv = os.Getenv
	}

	routesFile := strings.TrimSpace(getenv("ANTFLY_PROXY_ROUTES_FILE"))
	routesJSON := getenv("ANTFLY_PROXY_ROUTES_JSON")
	if routesFile != "" {
		data, readErr := os.ReadFile(routesFile)
		if readErr != nil {
			return GatewayEnvConfig{}, fmt.Errorf("read ANTFLY_PROXY_ROUTES_FILE: %w", readErr)
		}
		routesJSON = string(data)
	}

	routes, err := ParseRoutesJSON(routesJSON)
	if err != nil {
		return GatewayEnvConfig{}, err
	}

	requireAuth := false
	if raw := strings.TrimSpace(getenv("ANTFLY_PROXY_REQUIRE_AUTH")); raw != "" {
		requireAuth, err = strconv.ParseBool(raw)
		if err != nil {
			return GatewayEnvConfig{}, fmt.Errorf("parse ANTFLY_PROXY_REQUIRE_AUTH: %w", err)
		}
	}

	tokensFile := strings.TrimSpace(getenv("ANTFLY_PROXY_BEARER_TOKENS_FILE"))
	tokensJSON := getenv("ANTFLY_PROXY_BEARER_TOKENS_JSON")
	if tokensFile != "" {
		data, readErr := os.ReadFile(tokensFile)
		if readErr != nil {
			return GatewayEnvConfig{}, fmt.Errorf("read ANTFLY_PROXY_BEARER_TOKENS_FILE: %w", readErr)
		}
		tokensJSON = string(data)
	}

	tokens, err := parseBearerTokensJSON(tokensJSON)
	if err != nil {
		return GatewayEnvConfig{}, err
	}

	return GatewayEnvConfig{
		PublicAddr:  firstNonEmpty(getenv("ANTFLY_PROXY_PUBLIC_ADDR"), ":8080"),
		RequireAuth: requireAuth,
		RoutesFile:  routesFile,
		TokensFile:  tokensFile,
		Routes:      routes,
		Tokens:      tokens,
	}, nil
}

func NewGatewayFromEnv(getenv func(string) string) (*Gateway, error) {
	cfg, err := LoadGatewayEnvConfig(getenv)
	if err != nil {
		return nil, err
	}
	var catalog Catalog
	if cfg.RoutesFile != "" {
		catalog = NewChainedCatalog(
			NewFileCatalog(cfg.RoutesFile),
			NewStaticCatalog(cfg.Routes),
		)
	} else {
		catalog = NewStaticCatalog(cfg.Routes)
	}
	router := NewRouterWithCatalog(catalog)
	var authenticator Authenticator
	if cfg.TokensFile != "" || len(cfg.Tokens) > 0 {
		authenticator = &ReloadingBearerAuthenticator{
			Required:     cfg.RequireAuth,
			Path:         cfg.TokensFile,
			StaticTokens: cfg.Tokens,
		}
	} else {
		authenticator = StaticBearerAuthenticator{
			Required: cfg.RequireAuth,
			Tokens:   cfg.Tokens,
		}
	}
	return NewGatewayFromConfig(GatewayConfig{
		Router:        router,
		Authenticator: authenticator,
	}), nil
}

type bearerTokenPrincipal struct {
	Subject    string                     `json:"subject"`
	Tenant     string                     `json:"tenant"`
	Admin      bool                       `json:"admin"`
	Tables     []string                   `json:"tables"`
	Namespaces []string                   `json:"namespaces"`
	Operations []string                   `json:"operations"`
	RowFilter  map[string]json.RawMessage `json:"row_filter,omitempty"`
}

func parseBearerTokensJSON(raw string) (map[string]Principal, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var decoded map[string]bearerTokenPrincipal
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, fmt.Errorf("parse bearer tokens json: %w", err)
	}
	tokens := make(map[string]Principal, len(decoded))
	for token, principal := range decoded {
		tokens[token] = Principal{
			Subject:           principal.Subject,
			Tenant:            principal.Tenant,
			Admin:             principal.Admin,
			AllowedTables:     append([]string(nil), principal.Tables...),
			AllowedNamespaces: append([]string(nil), principal.Namespaces...),
			AllowedOperations: parseOperationKinds(principal.Operations),
			RowFilter:         principal.RowFilter,
		}
	}
	return tokens, nil
}

func parseOperationKinds(values []string) []OperationKind {
	if len(values) == 0 {
		return nil
	}
	out := make([]OperationKind, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, OperationKind(trimmed))
		}
	}
	return out
}
