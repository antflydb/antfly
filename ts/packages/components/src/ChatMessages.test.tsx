import { render, screen } from "@testing-library/react";
import { useMemo } from "react";
import { describe, expect, it, vi } from "vitest";
import { ChatContext, type ChatContextValue } from "./ChatContext";
import ChatMessages from "./ChatMessages";
import type { ChatTurn } from "./hooks/useChatStream";

function createTurn(overrides: Partial<ChatTurn> = {}): ChatTurn {
  return {
    id: "turn-1",
    userMessage: "What is Antfly?",
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
    isStreaming: true,
    ...overrides,
  };
}

function TestProvider({
  turns,
  children,
}: {
  turns: ChatTurn[];
  children: React.ReactNode;
}) {
  const value: ChatContextValue = {
    turns,
    isStreaming: turns.some((turn) => turn.isStreaming),
    sendMessage: () => {},
    sendFollowUp: () => {},
    respondToClarification: () => {},
    abort: () => {},
    reset: () => {},
    config: {
      url: "http://localhost:8082/api/v1",
      table: "test",
    },
  };

  return <ChatContext.Provider value={value}>{children}</ChatContext.Provider>;
}

describe("ChatMessages", () => {
  it("does not throw when renderStreamingIndicator uses hooks and then stops rendering", () => {
    const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const scrollIntoViewMock = vi.fn();
    Element.prototype.scrollIntoView = scrollIntoViewMock;

    const renderStreamingIndicator = () => {
      // eslint-disable-next-line react-hooks/rules-of-hooks
      const label = useMemo(() => "Custom thinking...", []);
      return <div data-testid="custom-streaming-indicator">{label}</div>;
    };

    const view = render(
      <TestProvider turns={[createTurn()]}>
        <ChatMessages renderStreamingIndicator={renderStreamingIndicator} />
      </TestProvider>
    );

    expect(screen.getByTestId("custom-streaming-indicator").textContent).toContain(
      "Custom thinking"
    );

    view.rerender(
      <TestProvider
        turns={[
          createTurn({
            assistantMessage: "Antfly is a search system.",
            isStreaming: false,
          }),
        ]}
      >
        <ChatMessages renderStreamingIndicator={renderStreamingIndicator} />
      </TestProvider>
    );

    expect(screen.getByText("Antfly is a search system.")).toBeTruthy();

    const reactHookErrors = consoleErrorSpy.mock.calls.filter(
      (call) =>
        typeof call[0] === "string" &&
        (call[0].includes("Rendered more hooks") || call[0].includes("error #310"))
    );
    expect(reactHookErrors).toHaveLength(0);
    expect(scrollIntoViewMock).toHaveBeenCalled();

    consoleErrorSpy.mockRestore();
  });
});
