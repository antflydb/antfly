import type { QueryHit } from "@antfly/sdk";
import type { ReactNode } from "react";
import type { ChatInputProps } from "../ChatInput";
import type { ChatMessagesProps } from "../ChatMessages";
import type { ChatTurn } from "../hooks/useChatStream";
import type { AIElementsComponents } from "./ai-elements-types";
import { hitToSourceProps, turnToStatus } from "./transforms";

/** The render prop set returned by createAIElementsRenderers. */
export type AIElementsRenderers = Pick<
  ChatMessagesProps,
  | "renderUserMessage"
  | "renderAssistantMessage"
  | "renderHits"
  | "renderFollowUpQuestions"
  | "renderStreamingIndicator"
> &
  Pick<ChatInputProps, "renderInput">;

/** Options for customizing the AI Elements renderers. */
export interface AIElementsRenderersOptions {
  /** Custom class names to merge onto AI Elements components */
  classNames?: {
    userMessage?: string;
    assistantMessage?: string;
    sources?: string;
    suggestions?: string;
    promptInput?: string;
  };
}

/**
 * Creates ChatBar render props that delegate to AI Elements components.
 *
 * Usage:
 * ```tsx
 * import { createAIElementsRenderers } from "@antfly/components";
 * import { Message, MessageContent, MessageResponse } from "@/components/ai/message";
 * import { Sources, SourcesTrigger, SourcesContent, Source } from "@/components/ai/sources";
 * import { Suggestions, Suggestion } from "@/components/ai/suggestion";
 * import { PromptInput, PromptInputTextarea, PromptInputSubmit, PromptInputFooter } from "@/components/ai/prompt-input";
 *
 * const renderers = createAIElementsRenderers({
 *   Message, MessageContent, MessageResponse,
 *   Sources, SourcesTrigger, SourcesContent, Source,
 *   Suggestions, Suggestion,
 *   PromptInput, PromptInputTextarea, PromptInputSubmit, PromptInputFooter,
 * });
 *
 * <ChatBar {...renderers} />
 * ```
 *
 * Renderers not covered (renderClarification, renderConfidence, renderError)
 * fall back to ChatBar defaults. Override them via spread after the factory:
 * ```tsx
 * <ChatBar {...renderers} renderError={(err) => <MyError>{err}</MyError>} />
 * ```
 */
export function createAIElementsRenderers(
  components: AIElementsComponents,
  options: AIElementsRenderersOptions = {}
): AIElementsRenderers {
  const {
    Message,
    MessageContent,
    MessageResponse,
    Sources,
    SourcesTrigger,
    SourcesContent,
    Source,
    Suggestions,
    Suggestion,
    PromptInput,
    PromptInputTextarea,
    PromptInputSubmit,
    PromptInputFooter,
  } = components;
  const { classNames = {} } = options;

  return {
    renderUserMessage(message: string, _turn: ChatTurn): ReactNode {
      return (
        <Message from="user" className={classNames.userMessage}>
          <MessageContent>
            <MessageResponse>{message}</MessageResponse>
          </MessageContent>
        </Message>
      );
    },

    renderAssistantMessage(message: string, isStreaming: boolean, _turn: ChatTurn): ReactNode {
      return (
        <Message from="assistant" className={classNames.assistantMessage}>
          <MessageContent>
            <MessageResponse parseIncompleteMarkdown={isStreaming}>
              {message || " "}
            </MessageResponse>
          </MessageContent>
        </Message>
      );
    },

    renderHits(hits: QueryHit[], _turn: ChatTurn): ReactNode {
      if (hits.length === 0) return null;
      return (
        <Sources className={classNames.sources}>
          <SourcesTrigger count={hits.length} />
          <SourcesContent>
            {hits.map((hit, i) => {
              const { key, href, title } = hitToSourceProps(hit, i);
              return <Source key={key} href={href} title={title} />;
            })}
          </SourcesContent>
        </Sources>
      );
    },

    renderFollowUpQuestions(
      questions: string[],
      onSelect: (question: string) => void,
      _turn: ChatTurn
    ): ReactNode {
      if (questions.length === 0) return null;
      return (
        <Suggestions className={classNames.suggestions}>
          {questions.map((q) => (
            <Suggestion key={q} suggestion={q} onClick={onSelect} />
          ))}
        </Suggestions>
      );
    },

    renderStreamingIndicator(): ReactNode {
      // Return null to let the assistant message's parseIncompleteMarkdown
      // handle the streaming state via MessageResponse
      return null;
    },

    renderInput({ value, onChange, onSubmit, isStreaming, placeholder, abort }): ReactNode {
      return (
        <PromptInput
          className={classNames.promptInput}
          onSubmit={(_msg, e) => {
            e.preventDefault();
            onSubmit();
          }}
        >
          <PromptInputTextarea
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder={placeholder}
            disabled={isStreaming}
          />
          <PromptInputFooter>
            <PromptInputSubmit
              status={turnToStatus(isStreaming, !!value)}
              onClick={isStreaming ? () => abort() : undefined}
            />
          </PromptInputFooter>
        </PromptInput>
      );
    },
  };
}
