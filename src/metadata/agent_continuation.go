// Copyright 2026 Antfly, Inc.
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

package metadata

import (
	"fmt"
	"strings"

	"github.com/rs/xid"
)

func ensureAgentSessionID(sessionID, prefix string) string {
	if sessionID != "" {
		return sessionID
	}
	return prefix + xid.New().String()
}

func appendDecisionContext(base string, decisions []AgentDecision) string {
	if len(decisions) == 0 {
		return base
	}

	var b strings.Builder
	b.WriteString(base)
	b.WriteString("\n\nResolved user decisions:\n")
	for _, decision := range decisions {
		b.WriteString("- ")
		if decision.QuestionId != "" {
			b.WriteString(decision.QuestionId)
			b.WriteString(": ")
		}
		switch {
		case decision.Approved:
			b.WriteString("approved")
		case decision.Answer != nil:
			fmt.Fprint(&b, decision.Answer)
		default:
			b.WriteString("answered")
		}
		b.WriteString("\n")
	}

	return b.String()
}

func normalizeAgentStep(step AgentStep) AgentStep {
	if step.Name == "" {
		step.Name = "agent_step"
	}
	if step.Kind == "" {
		step.Kind = AgentStepKindToolCall
	}
	return step
}

func normalizeAgentSteps(steps []AgentStep) []AgentStep {
	if len(steps) == 0 {
		return nil
	}
	normalized := make([]AgentStep, 0, len(steps))
	for _, step := range steps {
		normalized = append(normalized, normalizeAgentStep(step))
	}
	return normalized
}

func clarificationQuestionKind(options []string) AgentQuestionKind {
	if len(options) > 0 {
		return AgentQuestionKindSingleChoice
	}
	return AgentQuestionKindFreeText
}
