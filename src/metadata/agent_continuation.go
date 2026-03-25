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
