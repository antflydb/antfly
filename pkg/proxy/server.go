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

import "net/http"

type Server struct {
	Addr    string
	Gateway *Gateway
}

func NewServerFromEnv(getenv func(string) string) (*Server, error) {
	cfg, err := LoadGatewayEnvConfig(getenv)
	if err != nil {
		return nil, err
	}
	gateway, err := NewGatewayFromEnv(getenv)
	if err != nil {
		return nil, err
	}
	return &Server{
		Addr:    cfg.PublicAddr,
		Gateway: gateway,
	}, nil
}

func (s *Server) Handler() http.Handler {
	return s.Gateway
}

func (s *Server) ListenAndServe() error {
	return http.ListenAndServe(s.Addr, s.Handler())
}
