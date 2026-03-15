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

package a2a

import (
	"context"

	"github.com/a2aproject/a2a-go/a2a"
)

// CardProducer implements a2asrv.AgentCardProducer, building a dynamic
// AgentCard from the dispatcher's registered skills.
type CardProducer struct {
	dispatcher *Dispatcher
	baseURL    string
}

// NewCardProducer creates a new CardProducer.
func NewCardProducer(dispatcher *Dispatcher, baseURL string) *CardProducer {
	return &CardProducer{
		dispatcher: dispatcher,
		baseURL:    baseURL,
	}
}

// Card implements a2asrv.AgentCardProducer.
func (p *CardProducer) Card(_ context.Context) (*a2a.AgentCard, error) {
	return &a2a.AgentCard{
		Name:               "Antfly",
		Description:        "Distributed hybrid search and RAG engine. Supports semantic, full-text, and metadata search with LLM-powered retrieval agents.",
		URL:                p.baseURL + "/a2a",
		Version:            "1.0.0",
		ProtocolVersion:    string(a2a.Version),
		PreferredTransport: a2a.TransportProtocolJSONRPC,
		Capabilities: a2a.AgentCapabilities{
			Streaming:              true,
			StateTransitionHistory: true,
		},
		Skills:             p.dispatcher.Skills(),
		DefaultInputModes:  []string{"text", "data"},
		DefaultOutputModes: []string{"text", "data"},
	}, nil
}
