import type { EvalResult, GeneratorConfig } from "@antfly/sdk";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type React from "react";
import { useMemo } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import AnswerResults from "./AnswerResults";
import { useAnswerResultsContext } from "./AnswerResultsContext";
import Antfly from "./Antfly";
import QueryBox from "./QueryBox";
import * as utils from "./utils";

// Wrapper component to provide required context
const TestWrapper = ({ children }: { children: React.ReactNode }) => {
  return (
    <Antfly url="http://localhost:8082/api/v1" table="test">
      {children}
    </Antfly>
  );
};

// Mock generator config for testing
const mockGenerator: GeneratorConfig = {
  provider: "openai",
  model: "gpt-4",
  api_key: "test-key",
};

// Mock eval result for testing
const mockEvalResult: EvalResult = {
  summary: {
    total: 3,
    passed: 2,
    failed: 1,
    average_score: 0.75,
  },
  scores: {
    retrieval: {
      recall: { score: 0.8, pass: true, reason: "Good recall" },
      precision: { score: 0.7, pass: true, reason: "Decent precision" },
    },
    generation: {
      faithfulness: { score: 0.5, pass: false, reason: "Some hallucination detected" },
    },
  },
  duration_ms: 1500,
};

// Mock the streamAnswer function from utils
vi.mock("./utils", async () => {
  const actual = await vi.importActual<typeof utils>("./utils");
  return {
    ...actual,
    streamAnswer: vi.fn(),
  };
});

describe("AnswerResults", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("basic rendering", () => {
    it("should render without crashing", () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
          />
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should show empty state when no question submitted", () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
          />
        </TestWrapper>
      );

      const emptyMessage = container.querySelector(".react-af-answer-empty");
      expect(emptyMessage).toBeTruthy();
      expect(emptyMessage?.textContent).toContain("No results yet");
    });
  });

  describe("streaming behavior", () => {
    it("should stream answer chunks progressively", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, callbacks) => {
        callbacks.onGeneration?.("Hello ");
        callbacks.onGeneration?.("world");
        callbacks.onComplete?.();
        return new AbortController();
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(
        () => {
          expect(mockStreamAnswer).toHaveBeenCalled();
        },
        { timeout: 3000 }
      );

      await waitFor(
        () => {
          const answer = container.querySelector(".react-af-answer-text");
          expect(answer).toBeTruthy();
          expect(answer?.textContent).toContain("Hello world");
        },
        { timeout: 1000 }
      );
    });
  });

  describe("eval configuration", () => {
    it("should pass eval config to streamAnswer request", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      mockStreamAnswer.mockImplementation(async (_url, request, _headers, callbacks) => {
        // Verify eval config is in the request
        expect(request.steps?.eval).toEqual({
          evaluators: ["relevance", "faithfulness"],
        });
        callbacks.onComplete?.();
        return new AbortController();
      });

      render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
            eval={{
              evaluators: ["relevance", "faithfulness"],
            }}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(() => {
        expect(mockStreamAnswer).toHaveBeenCalled();
      });
    });

    it("should display eval results when eval config is provided", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, callbacks) => {
        callbacks.onGeneration?.("Test answer");
        callbacks.onEvalResult?.(mockEvalResult);
        callbacks.onComplete?.();
        return new AbortController();
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
            eval={{
              evaluators: ["relevance", "faithfulness"],
            }}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(() => {
        const evalElement = container.querySelector(".react-af-answer-eval");
        expect(evalElement).toBeTruthy();
      });

      // Check summary is displayed
      const evalElement = container.querySelector(".react-af-answer-eval");
      expect(evalElement?.textContent).toContain("2/3 passed");
      expect(evalElement?.textContent).toContain("75%");
    });

    it("should not display eval results when no eval config provided", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, callbacks) => {
        callbacks.onGeneration?.("Test answer");
        callbacks.onComplete?.();
        return new AbortController();
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(() => {
        const answer = container.querySelector(".react-af-answer-text");
        expect(answer).toBeTruthy();
      });

      // No eval element should exist
      const evalElement = container.querySelector(".react-af-answer-eval");
      expect(evalElement).toBeNull();
    });

    it("should use custom renderEvalResult when provided", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, callbacks) => {
        callbacks.onGeneration?.("Test answer");
        callbacks.onEvalResult?.(mockEvalResult);
        callbacks.onComplete?.();
        return new AbortController();
      });

      const customRenderEval = vi.fn((result: EvalResult) => (
        <div className="custom-eval">
          Custom eval: {result.summary?.passed}/{result.summary?.total}
        </div>
      ));

      const { container } = render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
            eval={{
              evaluators: ["relevance"],
            }}
            renderEvalResult={customRenderEval}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(() => {
        const customEval = container.querySelector(".custom-eval");
        expect(customEval).toBeTruthy();
        expect(customEval?.textContent).toContain("Custom eval: 2/3");
      });

      expect(customRenderEval).toHaveBeenCalledWith(mockEvalResult);
    });

    it("should not show eval results while streaming", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      let completeCallback: (() => void) | undefined;

      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, callbacks) => {
        callbacks.onGeneration?.("Streaming...");
        callbacks.onEvalResult?.(mockEvalResult);
        // Don't call onComplete yet - keep streaming
        completeCallback = callbacks.onComplete;
        return new AbortController();
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
            eval={{
              evaluators: ["relevance"],
            }}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      // Wait for answer to start streaming
      await waitFor(() => {
        const answer = container.querySelector(".react-af-answer-text");
        expect(answer).toBeTruthy();
      });

      // Eval should NOT be visible while streaming
      let evalElement = container.querySelector(".react-af-answer-eval");
      expect(evalElement).toBeNull();

      // Complete the stream
      await act(async () => {
        completeCallback?.();
      });

      // Now eval should be visible
      await waitFor(() => {
        evalElement = container.querySelector(".react-af-answer-eval");
        expect(evalElement).toBeTruthy();
      });
    });

    it("should display retrieval and generation metrics separately", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, callbacks) => {
        callbacks.onGeneration?.("Test answer");
        callbacks.onEvalResult?.(mockEvalResult);
        callbacks.onComplete?.();
        return new AbortController();
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
            eval={{
              evaluators: ["recall", "precision", "faithfulness"],
            }}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(() => {
        const evalElement = container.querySelector(".react-af-answer-eval");
        expect(evalElement).toBeTruthy();
      });

      // Check retrieval metrics category
      const retrievalCategory = container.querySelector(".react-af-answer-eval-category");
      expect(retrievalCategory?.textContent).toContain("Retrieval Metrics");
      expect(retrievalCategory?.textContent).toContain("recall");
      expect(retrievalCategory?.textContent).toContain("80.0%");

      // Check generation metrics
      const evalContent = container.querySelector(".react-af-answer-eval-content");
      expect(evalContent?.textContent).toContain("Generation Metrics");
      expect(evalContent?.textContent).toContain("faithfulness");
      expect(evalContent?.textContent).toContain("50.0%");
    });

    it("should display eval duration when available", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, callbacks) => {
        callbacks.onGeneration?.("Test answer");
        callbacks.onEvalResult?.(mockEvalResult);
        callbacks.onComplete?.();
        return new AbortController();
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
            eval={{
              evaluators: ["relevance"],
            }}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(() => {
        const duration = container.querySelector(".react-af-answer-eval-duration");
        expect(duration).toBeTruthy();
        expect(duration?.textContent).toContain("1500ms");
      });
    });
  });

  describe("error handling", () => {
    it("should display error when fetch fails", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, callbacks) => {
        callbacks.onError?.(new Error("Network error"));
        return new AbortController();
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(() => {
        const error = container.querySelector(".react-af-answer-error");
        expect(error).toBeTruthy();
        expect(error?.textContent).toContain("Network error");
      });
    });
  });

  describe("callbacks", () => {
    it("should call onStreamStart and onStreamEnd", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, callbacks) => {
        callbacks.onGeneration?.("Test");
        callbacks.onComplete?.();
        return new AbortController();
      });

      const onStreamStart = vi.fn();
      const onStreamEnd = vi.fn();

      render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
            onStreamStart={onStreamStart}
            onStreamEnd={onStreamEnd}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(() => {
        expect(onStreamStart).toHaveBeenCalled();
        expect(onStreamEnd).toHaveBeenCalled();
      });
    });
  });

  describe("renderAnswer with hooks (regression: React error #310)", () => {
    it("should not throw when renderLoading itself uses hooks before the first answer chunk", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      let generateCallback: ((chunk: string) => void) | undefined;

      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, callbacks) => {
        generateCallback = callbacks.onGeneration;
        return new AbortController();
      });

      const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

      const renderLoading = () => {
        // eslint-disable-next-line react-hooks/rules-of-hooks
        const loadingText = useMemo(() => "Custom loading", []);
        return <div data-testid="custom-loading">{loadingText}</div>;
      };

      render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
            renderLoading={renderLoading}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(() => {
        expect(screen.getByTestId("custom-loading")).toBeTruthy();
      });

      await act(async () => {
        generateCallback?.("Hello world");
      });

      await waitFor(() => {
        expect(screen.getByText("Hello world")).toBeTruthy();
      });

      const reactHookErrors = consoleErrorSpy.mock.calls.filter(
        (call) =>
          typeof call[0] === "string" &&
          (call[0].includes("Rendered more hooks") || call[0].includes("error #310"))
      );
      expect(reactHookErrors).toHaveLength(0);

      consoleErrorSpy.mockRestore();
    });

    it("should not throw when renderAnswer itself uses hooks and answer streams progressively", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      let generateCallback: ((chunk: string) => void) | undefined;
      let completeCallback: (() => void) | undefined;

      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, callbacks) => {
        generateCallback = callbacks.onGeneration;
        completeCallback = callbacks.onComplete;
        return new AbortController();
      });

      const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

      const renderAnswer = (answer: string, isStreaming: boolean) => {
        // These hooks execute inside the render prop itself.
        // eslint-disable-next-line react-hooks/rules-of-hooks
        const ctx = useAnswerResultsContext();
        // eslint-disable-next-line react-hooks/rules-of-hooks
        const displayText = useMemo(() => `Custom: ${answer}`, [answer]);
        return (
          <div data-testid="custom-answer-with-hooks">
            {displayText}
            {isStreaming && <span data-testid="streaming-indicator">streaming</span>}
            {ctx.hits.length > 0 && <span data-testid="hit-count">{ctx.hits.length} hits</span>}
          </div>
        );
      };

      render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
            renderAnswer={renderAnswer}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      // Submit query — triggers streaming, but no answer yet
      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(() => {
        expect(mockStreamAnswer).toHaveBeenCalled();
      });

      expect(screen.queryByTestId("custom-answer-with-hooks")).toBeNull();

      await act(async () => {
        generateCallback?.("Hello ");
        generateCallback?.("world");
      });

      await waitFor(() => {
        expect(screen.getByTestId("custom-answer-with-hooks").textContent).toContain(
          "Custom: Hello world"
        );
      });

      // Complete the stream
      await act(async () => {
        completeCallback?.();
      });

      // Verify no React errors were logged
      const reactHookErrors = consoleErrorSpy.mock.calls.filter(
        (call) =>
          typeof call[0] === "string" &&
          (call[0].includes("Rendered more hooks") || call[0].includes("error #310"))
      );
      expect(reactHookErrors).toHaveLength(0);

      consoleErrorSpy.mockRestore();
    });

    it("should not call renderAnswer until an answer exists", async () => {
      const mockStreamAnswer = vi.mocked(utils.streamAnswer);
      mockStreamAnswer.mockImplementation(async (_url, _request, _headers, _callbacks) => {
        return new AbortController();
      });

      const renderAnswer = vi.fn((answer: string, isStreaming: boolean) => (
        <div data-testid="custom-answer">{answer || (isStreaming ? "Waiting..." : "")}</div>
      ));

      render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
            renderAnswer={renderAnswer}
          />
        </TestWrapper>
      );

      const input = screen.getByPlaceholderText(/ask a question/i);
      const button = screen.getByRole("button", { name: /submit/i });

      await act(async () => {
        await userEvent.type(input, "test question");
        await userEvent.click(button);
      });

      await waitFor(() => {
        expect(mockStreamAnswer).toHaveBeenCalled();
      });

      expect(screen.queryByTestId("custom-answer")).toBeNull();
      expect(renderAnswer).not.toHaveBeenCalled();
    });

    it("should keep renderEmpty separate from renderAnswer before submission", () => {
      const renderEmpty = vi.fn(() => <div data-testid="custom-empty">Empty</div>);
      const renderAnswer = vi.fn(() => <div data-testid="custom-answer">Answer</div>);

      render(
        <TestWrapper>
          <QueryBox id="question" mode="submit" />
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            fields={["content"]}
            renderEmpty={renderEmpty}
            renderAnswer={renderAnswer}
          />
        </TestWrapper>
      );

      expect(screen.getByTestId("custom-empty")).toBeTruthy();
      expect(screen.queryByTestId("custom-answer")).toBeNull();
      expect(renderEmpty).toHaveBeenCalled();
      expect(renderAnswer).not.toHaveBeenCalled();
    });
  });
});
