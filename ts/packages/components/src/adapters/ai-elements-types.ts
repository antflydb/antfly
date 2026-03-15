import type { ComponentType, HTMLAttributes, ReactNode } from "react";

/**
 * Structural interface types matching AI Elements component signatures.
 *
 * These are duck-typed contracts — @antfly/components has zero runtime
 * dependency on AI Elements. Consumers pass the concrete components
 * from their local AI Elements installation to the factory.
 */

export interface MessageProps extends HTMLAttributes<HTMLDivElement> {
  from: "user" | "assistant";
}

export interface MessageContentProps extends HTMLAttributes<HTMLDivElement> {}

export interface MessageResponseProps {
  children: string;
  parseIncompleteMarkdown?: boolean;
  className?: string;
}

export interface SourcesProps extends HTMLAttributes<HTMLDivElement> {}

export interface SourcesTriggerProps {
  count: number;
  className?: string;
}

export interface SourcesContentProps extends HTMLAttributes<HTMLDivElement> {}

export interface SourceProps {
  href?: string;
  title?: string;
  className?: string;
}

export interface SuggestionsProps extends HTMLAttributes<HTMLDivElement> {}

export interface SuggestionProps {
  suggestion: string;
  onClick?: (suggestion: string) => void;
  className?: string;
}

export interface PromptInputProps {
  onSubmit: (message: { text?: string }, event: React.FormEvent) => void;
  children?: ReactNode;
  className?: string;
}

export interface PromptInputTextareaProps
  extends React.TextareaHTMLAttributes<HTMLTextAreaElement> {}

export interface PromptInputSubmitProps {
  status: "awaiting-message" | "submitted" | "streaming";
  className?: string;
  children?: ReactNode;
  onClick?: React.MouseEventHandler;
}

export interface PromptInputFooterProps extends HTMLAttributes<HTMLDivElement> {}

/** The full set of AI Elements components the adapter uses. */
export interface AIElementsComponents {
  Message: ComponentType<MessageProps>;
  MessageContent: ComponentType<MessageContentProps>;
  MessageResponse: ComponentType<MessageResponseProps>;
  Sources: ComponentType<SourcesProps>;
  SourcesTrigger: ComponentType<SourcesTriggerProps>;
  SourcesContent: ComponentType<SourcesContentProps>;
  Source: ComponentType<SourceProps>;
  Suggestions: ComponentType<SuggestionsProps>;
  Suggestion: ComponentType<SuggestionProps>;
  PromptInput: ComponentType<PromptInputProps>;
  PromptInputTextarea: ComponentType<PromptInputTextareaProps>;
  PromptInputSubmit: ComponentType<PromptInputSubmitProps>;
  PromptInputFooter: ComponentType<PromptInputFooterProps>;
}
