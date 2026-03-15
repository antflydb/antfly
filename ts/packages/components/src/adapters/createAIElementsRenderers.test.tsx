import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { ChatTurn } from "../hooks/useChatStream";
import type {
  AIElementsComponents,
  MessageContentProps,
  MessageProps,
  MessageResponseProps,
  PromptInputFooterProps,
  PromptInputProps,
  PromptInputSubmitProps,
  PromptInputTextareaProps,
  SourceProps,
  SourcesContentProps,
  SourcesProps,
  SourcesTriggerProps,
  SuggestionProps,
  SuggestionsProps,
} from "./ai-elements-types";
import { createAIElementsRenderers } from "./createAIElementsRenderers";

// Stub AI Elements components — plain HTML elements that expose props for assertions
function createMockComponents(): AIElementsComponents {
  return {
    Message: ({ from, children, className, ...rest }: MessageProps) => (
      <div data-testid={`message-${from}`} data-from={from} className={className} {...rest}>
        {children}
      </div>
    ),
    MessageContent: ({ children, ...rest }: MessageContentProps) => (
      <div data-testid="message-content" {...rest}>
        {children}
      </div>
    ),
    MessageResponse: ({ children, parseIncompleteMarkdown, ...rest }: MessageResponseProps) => (
      <div data-testid="message-response" data-parse-incomplete={parseIncompleteMarkdown} {...rest}>
        {children}
      </div>
    ),
    Sources: ({ children, className, ...rest }: SourcesProps) => (
      <div data-testid="sources" className={className} {...rest}>
        {children}
      </div>
    ),
    SourcesTrigger: ({ count, ...rest }: SourcesTriggerProps) => (
      <button data-testid="sources-trigger" data-count={count} {...rest}>
        {count} sources
      </button>
    ),
    SourcesContent: ({ children, ...rest }: SourcesContentProps) => (
      <div data-testid="sources-content" {...rest}>
        {children}
      </div>
    ),
    Source: ({ href, title, ...rest }: SourceProps) => (
      <a data-testid="source" href={href} {...rest}>
        {title}
      </a>
    ),
    Suggestions: ({ children, className, ...rest }: SuggestionsProps) => (
      <div data-testid="suggestions" className={className} {...rest}>
        {children}
      </div>
    ),
    Suggestion: ({ suggestion, onClick, ...rest }: SuggestionProps) => (
      <button
        data-testid="suggestion"
        type="button"
        onClick={() => onClick?.(suggestion)}
        {...rest}
      >
        {suggestion}
      </button>
    ),
    PromptInput: ({ onSubmit, children, className, ...rest }: PromptInputProps) => (
      <form
        data-testid="prompt-input"
        className={className}
        onSubmit={(e) => {
          e.preventDefault();
          onSubmit?.({ text: "" }, e);
        }}
        {...rest}
      >
        {children}
      </form>
    ),
    PromptInputTextarea: (props: PromptInputTextareaProps) => (
      <textarea data-testid="prompt-textarea" {...props} />
    ),
    PromptInputSubmit: ({ status, onClick, ...rest }: PromptInputSubmitProps) => (
      <button
        data-testid="prompt-submit"
        data-status={status}
        type="submit"
        onClick={onClick}
        {...rest}
      >
        {status}
      </button>
    ),
    PromptInputFooter: ({ children, ...rest }: PromptInputFooterProps) => (
      <div data-testid="prompt-footer" {...rest}>
        {children}
      </div>
    ),
  };
}

function makeTurn(overrides: Partial<ChatTurn> = {}): ChatTurn {
  return {
    id: "turn-1",
    userMessage: "hello",
    assistantMessage: "",
    hits: [],
    followUpQuestions: [],
    classification: null,
    confidence: null,
    clarification: null,
    appliedFilters: [],
    reasoningText: "",
    reasoningChain: [],
    activeSteps: [],
    toolCallsMade: 0,
    error: null,
    isStreaming: false,
    ...overrides,
  };
}

describe("createAIElementsRenderers", () => {
  const components = createMockComponents();

  describe("renderUserMessage", () => {
    it("renders Message with from='user' and message text", () => {
      const renderers = createAIElementsRenderers(components);
      const turn = makeTurn();
      render(renderers.renderUserMessage?.("Hello world", turn));

      const msg = screen.getByTestId("message-user");
      expect(msg).toHaveAttribute("data-from", "user");
      expect(screen.getByTestId("message-response")).toHaveTextContent("Hello world");
    });

    it("applies custom className", () => {
      const renderers = createAIElementsRenderers(components, {
        classNames: { userMessage: "custom-user" },
      });
      render(renderers.renderUserMessage?.("test", makeTurn()));
      expect(screen.getByTestId("message-user")).toHaveClass("custom-user");
    });
  });

  describe("renderAssistantMessage", () => {
    it("renders Message with from='assistant'", () => {
      const renderers = createAIElementsRenderers(components);
      render(renderers.renderAssistantMessage?.("Response text", false, makeTurn()));

      const msg = screen.getByTestId("message-assistant");
      expect(msg).toHaveAttribute("data-from", "assistant");
      expect(screen.getByTestId("message-response")).toHaveTextContent("Response text");
    });

    it("sets parseIncompleteMarkdown when streaming", () => {
      const renderers = createAIElementsRenderers(components);
      render(renderers.renderAssistantMessage?.("partial...", true, makeTurn()));

      expect(screen.getByTestId("message-response")).toHaveAttribute(
        "data-parse-incomplete",
        "true"
      );
    });

    it("does not set parseIncompleteMarkdown when not streaming", () => {
      const renderers = createAIElementsRenderers(components);
      render(renderers.renderAssistantMessage?.("complete", false, makeTurn()));

      expect(screen.getByTestId("message-response")).toHaveAttribute(
        "data-parse-incomplete",
        "false"
      );
    });
  });

  describe("renderHits", () => {
    it("returns null for empty hits", () => {
      const renderers = createAIElementsRenderers(components);
      const result = renderers.renderHits?.([], makeTurn());
      expect(result).toBeNull();
    });

    it("renders sources with correct count", () => {
      const renderers = createAIElementsRenderers(components);
      const hits = [
        { _id: "doc-1", _score: 0.9, _source: { title: "First", url: "https://a.com" } },
        { _id: "doc-2", _score: 0.8, _source: { title: "Second", url: "https://b.com" } },
      ];
      render(renderers.renderHits?.(hits, makeTurn()));

      expect(screen.getByTestId("sources-trigger")).toHaveAttribute("data-count", "2");
      const sources = screen.getAllByTestId("source");
      expect(sources).toHaveLength(2);
      expect(sources[0]).toHaveTextContent("First");
      expect(sources[0]).toHaveAttribute("href", "https://a.com");
      expect(sources[1]).toHaveTextContent("Second");
    });
  });

  describe("renderFollowUpQuestions", () => {
    it("returns null for empty questions", () => {
      const renderers = createAIElementsRenderers(components);
      const result = renderers.renderFollowUpQuestions?.([], vi.fn(), makeTurn());
      expect(result).toBeNull();
    });

    it("renders suggestion buttons", () => {
      const renderers = createAIElementsRenderers(components);
      render(
        renderers.renderFollowUpQuestions?.(["What else?", "Tell me more"], vi.fn(), makeTurn())
      );

      const suggestions = screen.getAllByTestId("suggestion");
      expect(suggestions).toHaveLength(2);
      expect(suggestions[0]).toHaveTextContent("What else?");
      expect(suggestions[1]).toHaveTextContent("Tell me more");
    });

    it("calls onSelect when suggestion clicked", async () => {
      const user = userEvent.setup();
      const onSelect = vi.fn();
      const renderers = createAIElementsRenderers(components);
      render(renderers.renderFollowUpQuestions?.(["Follow up"], onSelect, makeTurn()));

      await user.click(screen.getByTestId("suggestion"));
      expect(onSelect).toHaveBeenCalledWith("Follow up");
    });
  });

  describe("renderStreamingIndicator", () => {
    it("returns null (delegates to MessageResponse parseIncompleteMarkdown)", () => {
      const renderers = createAIElementsRenderers(components);
      expect(renderers.renderStreamingIndicator?.()).toBeNull();
    });
  });

  describe("renderInput", () => {
    it("renders prompt input with correct status when idle", () => {
      const renderers = createAIElementsRenderers(components);
      render(
        renderers.renderInput?.({
          value: "",
          onChange: vi.fn(),
          onSubmit: vi.fn(),
          isStreaming: false,
          placeholder: "Type here...",
          abort: vi.fn(),
        })
      );

      expect(screen.getByTestId("prompt-submit")).toHaveAttribute(
        "data-status",
        "awaiting-message"
      );
      expect(screen.getByTestId("prompt-textarea")).toHaveAttribute("placeholder", "Type here...");
    });

    it("shows streaming status when streaming with content", () => {
      const renderers = createAIElementsRenderers(components);
      render(
        renderers.renderInput?.({
          value: "some content",
          onChange: vi.fn(),
          onSubmit: vi.fn(),
          isStreaming: true,
          placeholder: "",
          abort: vi.fn(),
        })
      );

      expect(screen.getByTestId("prompt-submit")).toHaveAttribute("data-status", "streaming");
    });

    it("shows submitted status when streaming without content", () => {
      const renderers = createAIElementsRenderers(components);
      render(
        renderers.renderInput?.({
          value: "",
          onChange: vi.fn(),
          onSubmit: vi.fn(),
          isStreaming: true,
          placeholder: "",
          abort: vi.fn(),
        })
      );

      expect(screen.getByTestId("prompt-submit")).toHaveAttribute("data-status", "submitted");
    });

    it("disables textarea when streaming", () => {
      const renderers = createAIElementsRenderers(components);
      render(
        renderers.renderInput?.({
          value: "",
          onChange: vi.fn(),
          onSubmit: vi.fn(),
          isStreaming: true,
          placeholder: "",
          abort: vi.fn(),
        })
      );

      expect(screen.getByTestId("prompt-textarea")).toBeDisabled();
    });

    it("calls abort when submit button clicked during streaming", async () => {
      const user = userEvent.setup();
      const abort = vi.fn();
      const renderers = createAIElementsRenderers(components);
      render(
        renderers.renderInput?.({
          value: "text",
          onChange: vi.fn(),
          onSubmit: vi.fn(),
          isStreaming: true,
          placeholder: "",
          abort,
        })
      );

      await user.click(screen.getByTestId("prompt-submit"));
      expect(abort).toHaveBeenCalled();
    });
  });
});
