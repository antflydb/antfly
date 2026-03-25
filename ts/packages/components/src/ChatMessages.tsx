import type { AgentQuestion, GenerationConfidence, QueryHit } from "@antfly/sdk";
import type { ReactNode } from "react";
import { useEffect, useRef } from "react";
import { useChatContext } from "./ChatContext";
import type { ChatTurn } from "./hooks/useChatStream";
import { SafeRender } from "./SafeRender";

export interface ChatMessagesProps {
  /** Show search hits for each turn */
  showHits?: boolean;
  /** Show follow-up questions on the last turn */
  showFollowUpQuestions?: boolean;
  /** Show confidence assessment */
  showConfidence?: boolean;

  /** Custom renderer for user messages */
  renderUserMessage?: (message: string, turn: ChatTurn) => ReactNode;
  /** Custom renderer for assistant messages */
  renderAssistantMessage?: (message: string, isStreaming: boolean, turn: ChatTurn) => ReactNode;
  /** Custom renderer for search hits */
  renderHits?: (hits: QueryHit[], turn: ChatTurn) => ReactNode;
  /** Custom renderer for follow-up questions */
  renderFollowUpQuestions?: (
    questions: string[],
    onSelect: (question: string) => void,
    turn: ChatTurn
  ) => ReactNode;
  /** Custom renderer for clarification requests */
  renderClarification?: (
    clarification: AgentQuestion,
    onRespond: (response: string) => void,
    turn: ChatTurn
  ) => ReactNode;
  /** Custom renderer for confidence data */
  renderConfidence?: (confidence: GenerationConfidence, turn: ChatTurn) => ReactNode;
  /** Custom renderer for the streaming indicator */
  renderStreamingIndicator?: () => ReactNode;
  /** Custom renderer for errors */
  renderError?: (error: string, turn: ChatTurn) => ReactNode;
}

function defaultRenderUserMessage(message: string) {
  return <div className="react-af-chat-message-user">{message}</div>;
}

function defaultRenderAssistantMessage(message: string, isStreaming: boolean) {
  return (
    <div className="react-af-chat-message-assistant">
      {message}
      {isStreaming && <span className="react-af-chat-streaming-cursor" />}
    </div>
  );
}

function defaultRenderHits(hits: QueryHit[]) {
  return (
    <details className="react-af-chat-hits">
      <summary>Sources ({hits.length})</summary>
      <ul>
        {hits.map((hit, idx) => (
          <li key={hit._id || idx}>
            <strong>Score:</strong> {hit._score.toFixed(3)}
            <pre>{JSON.stringify(hit._source, null, 2)}</pre>
          </li>
        ))}
      </ul>
    </details>
  );
}

function defaultRenderFollowUpQuestions(questions: string[], onSelect: (q: string) => void) {
  return (
    <div className="react-af-chat-followups">
      {questions.map((q) => (
        <button
          key={q}
          type="button"
          className="react-af-chat-followup-button"
          onClick={() => onSelect(q)}
        >
          {q}
        </button>
      ))}
    </div>
  );
}

function defaultRenderClarification(
  clarification: AgentQuestion,
  onRespond: (r: string) => void
) {
  return (
    <div className="react-af-chat-clarification">
      <p>{clarification.question}</p>
      {clarification.options?.map((option) => (
        <button
          key={option}
          type="button"
          className="react-af-chat-clarification-option"
          onClick={() => onRespond(option)}
        >
          {option}
        </button>
      ))}
    </div>
  );
}

function defaultRenderConfidence(confidence: GenerationConfidence) {
  return (
    <div className="react-af-chat-confidence">
      <small>
        Confidence: {(confidence.generation_confidence * 100).toFixed(0)}% | Relevance:{" "}
        {(confidence.context_relevance * 100).toFixed(0)}%
      </small>
    </div>
  );
}

function defaultRenderStreamingIndicator() {
  return <div className="react-af-chat-streaming-indicator">Thinking...</div>;
}

function defaultRenderError(error: string) {
  return <div className="react-af-chat-error">Error: {error}</div>;
}

export default function ChatMessages({
  showHits = false,
  showFollowUpQuestions = true,
  showConfidence = false,
  renderUserMessage,
  renderAssistantMessage,
  renderHits,
  renderFollowUpQuestions,
  renderClarification,
  renderConfidence,
  renderStreamingIndicator,
  renderError,
}: ChatMessagesProps) {
  const { turns, sendFollowUp, respondToClarification } = useChatContext();
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const lastTurnIndex = turns.length - 1;

  useEffect(() => {
    // Reference turns to trigger scroll on new messages and streaming updates
    void turns;
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [turns]);

  return (
    <div className="react-af-chat-messages" role="log" aria-live="polite">
      {turns.map((turn, index) => (
        <div key={turn.id} className="react-af-chat-turn">
          {/* User message */}
          {renderUserMessage
            ? <SafeRender render={renderUserMessage} args={[turn.userMessage, turn] as const} />
            : defaultRenderUserMessage(turn.userMessage)}

          {/* Error */}
          {turn.error &&
            (renderError ? (
              <SafeRender render={renderError} args={[turn.error, turn] as const} />
            ) : (
              defaultRenderError(turn.error)
            ))}

          {/* Streaming indicator (before any content appears) */}
          {turn.isStreaming && !turn.assistantMessage && !turn.error && (
            <div role="status">
              {renderStreamingIndicator ? (
                <SafeRender render={renderStreamingIndicator} args={[] as const} />
              ) : (
                defaultRenderStreamingIndicator()
              )}
            </div>
          )}

          {/* Assistant message */}
          {turn.assistantMessage &&
            (renderAssistantMessage ? (
              <SafeRender
                render={renderAssistantMessage}
                args={[turn.assistantMessage, turn.isStreaming, turn] as const}
              />
            ) : (
              defaultRenderAssistantMessage(turn.assistantMessage, turn.isStreaming)
            ))}

          {/* Confidence */}
          {showConfidence &&
            turn.confidence &&
            !turn.isStreaming &&
            (renderConfidence ? (
              <SafeRender render={renderConfidence} args={[turn.confidence, turn] as const} />
            ) : (
              defaultRenderConfidence(turn.confidence)
            ))}

          {/* Hits */}
          {showHits &&
            turn.hits.length > 0 &&
            (renderHits ? (
              <SafeRender render={renderHits} args={[turn.hits, turn] as const} />
            ) : (
              defaultRenderHits(turn.hits)
            ))}

          {/* Clarification */}
          {turn.clarification &&
            !turn.isStreaming &&
            (renderClarification ? (
              <SafeRender
                render={renderClarification}
                args={[turn.clarification, respondToClarification, turn] as const}
              />
            ) : (
              defaultRenderClarification(turn.clarification, respondToClarification)
            ))}

          {/* Follow-up questions (only on last turn) */}
          {showFollowUpQuestions &&
            index === lastTurnIndex &&
            turn.followUpQuestions.length > 0 &&
            !turn.isStreaming &&
            (renderFollowUpQuestions ? (
              <SafeRender
                render={renderFollowUpQuestions}
                args={[turn.followUpQuestions, sendFollowUp, turn] as const}
              />
            ) : (
              defaultRenderFollowUpQuestions(turn.followUpQuestions, sendFollowUp)
            ))}
        </div>
      ))}
      <div ref={messagesEndRef} />
    </div>
  );
}
