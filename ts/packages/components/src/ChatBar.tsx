import type { ChatToolsConfig, GeneratorConfig, RetrievalAgentSteps } from "@antfly/sdk";
import type { ReactNode } from "react";
import { useCallback, useEffect, useMemo, useRef } from "react";
import { ChatContext, type ChatContextValue } from "./ChatContext";
import ChatInput, { type ChatInputProps } from "./ChatInput";
import ChatMessages, { type ChatMessagesProps } from "./ChatMessages";
import type { ChatConfig } from "./hooks/useChatStream";
import { useChatStream } from "./hooks/useChatStream";
import { useSharedContext } from "./SharedContext";
import { resolveTable } from "./utils";

export interface ChatBarProps extends ChatMessagesProps, ChatInputProps {
  /** Unique identifier for this chat bar */
  id: string;
  /** Generator configuration (provider, model, temperature) */
  generator: GeneratorConfig;
  /** Table override */
  table?: string;
  /** Semantic indexes to use */
  semanticIndexes?: string[];
  /** Domain-specific knowledge for the agent */
  agentKnowledge?: string;
  /** System prompt override */
  systemPrompt?: string;
  /** Maximum tool iterations per turn */
  maxIterations?: number;
  /** Number of follow-up questions to generate */
  followUpCount?: number;
  /** Results limit per search */
  limit?: number;
  /** Retrieval agent steps configuration */
  steps?: RetrievalAgentSteps;
  /** Tool configuration for agentic mode */
  tools?: ChatToolsConfig;
  /** Fields to include in results */
  fields?: string[];
  /** Filter query to constrain results */
  filterQuery?: Record<string, unknown>;
  /** Exclusion query to exclude matches */
  exclusionQuery?: Record<string, unknown>;

  /** Called when streaming starts */
  onStreamStart?: () => void;
  /** Called when streaming ends */
  onStreamEnd?: () => void;
  /** Called on error */
  onError?: (error: string) => void;

  /** Additional children rendered inside the chat bar */
  children?: ReactNode;
}

export default function ChatBar({
  id,
  generator,
  table,
  semanticIndexes,
  agentKnowledge,
  systemPrompt,
  maxIterations,
  followUpCount,
  limit,
  steps,
  tools,
  fields,
  filterQuery,
  exclusionQuery,
  onStreamStart,
  onStreamEnd,
  onError,
  // ChatMessagesProps
  showHits,
  showFollowUpQuestions,
  showConfidence,
  renderUserMessage,
  renderAssistantMessage,
  renderHits,
  renderFollowUpQuestions,
  renderClarification,
  renderConfidence,
  renderStreamingIndicator,
  renderError,
  // ChatInputProps
  placeholder,
  renderInput,
  children,
}: ChatBarProps) {
  const [{ url, table: defaultTable, headers }, dispatch] = useSharedContext();
  const { turns, isStreaming, sendMessage: rawSendMessage, abort, reset } = useChatStream();

  const resolvedTable = resolveTable(table, defaultTable);

  const config = useMemo<ChatConfig>(
    () => ({
      url: url || "",
      headers: headers || {},
      generator,
      table: resolvedTable,
      semanticIndexes,
      agentKnowledge,
      systemPrompt,
      maxIterations,
      followUpCount,
      limit,
      steps,
      tools,
      fields,
      filterQuery,
      exclusionQuery,
    }),
    [
      url,
      headers,
      generator,
      resolvedTable,
      semanticIndexes,
      agentKnowledge,
      systemPrompt,
      maxIterations,
      followUpCount,
      limit,
      steps,
      tools,
      fields,
      filterQuery,
      exclusionQuery,
    ]
  );

  // Wrap sendMessage to include lifecycle callbacks
  const sendMessage = useCallback(
    (text: string) => {
      if (!url) {
        onError?.("ChatBar: Missing API URL in context");
        return;
      }
      onStreamStart?.();
      rawSendMessage(text, config);
    },
    [url, config, rawSendMessage, onStreamStart, onError]
  );

  // Track streaming state for lifecycle callbacks
  const prevStreamingRef = useRef(false);
  useEffect(() => {
    if (prevStreamingRef.current && !isStreaming) {
      // Streaming just ended — check for errors
      const lastTurn = turns[turns.length - 1];
      if (lastTurn?.error) {
        onError?.(lastTurn.error);
      }
      onStreamEnd?.();
    }
    prevStreamingRef.current = isStreaming;
  }, [isStreaming, turns, onStreamEnd, onError]);

  // Register as widget for consistency
  useEffect(() => {
    dispatch({
      type: "setWidget",
      key: id,
      needsQuery: false,
      needsConfiguration: false,
      isFacet: false,
      wantResults: false,
      table,
    });
  }, [dispatch, id, table]);

  // Cleanup on unmount
  useEffect(
    () => () => {
      dispatch({ type: "deleteWidget", key: id });
      abort();
    },
    [dispatch, id, abort]
  );

  const contextValue = useMemo<ChatContextValue>(
    () => ({
      turns,
      isStreaming,
      sendMessage,
      sendFollowUp: sendMessage,
      respondToClarification: sendMessage,
      abort,
      reset,
      config,
    }),
    [turns, isStreaming, sendMessage, abort, reset, config]
  );

  return (
    <ChatContext.Provider value={contextValue}>
      <div className="react-af-chat-bar">
        <ChatMessages
          showHits={showHits}
          showFollowUpQuestions={showFollowUpQuestions}
          showConfidence={showConfidence}
          renderUserMessage={renderUserMessage}
          renderAssistantMessage={renderAssistantMessage}
          renderHits={renderHits}
          renderFollowUpQuestions={renderFollowUpQuestions}
          renderClarification={renderClarification}
          renderConfidence={renderConfidence}
          renderStreamingIndicator={renderStreamingIndicator}
          renderError={renderError}
        />
        <ChatInput placeholder={placeholder} renderInput={renderInput} />
        {children}
      </div>
    </ChatContext.Provider>
  );
}
