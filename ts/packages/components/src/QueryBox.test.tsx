import { act, fireEvent, render, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { setupServer } from "msw/node";
import type React from "react";
import { useEffect } from "react";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import Antfly from "./Antfly";
import Autosuggest from "./Autosuggest";
import QueryBox from "./QueryBox";
import { useSharedContext, type Widget } from "./SharedContext";

// Mock scrollIntoView which isn't available in jsdom
beforeEach(() => {
  Element.prototype.scrollIntoView = vi.fn();
});

// MSW server setup
const server = setupServer(
  http.post("http://localhost:8082/api/v1/multiquery", () => {
    return HttpResponse.json({
      responses: [
        {
          status: 200,
          took: 1,
          hits: {
            hits: [
              { _id: "1", _score: 1.0, _source: { title: "Test Item 1", name: "First" } },
              { _id: "2", _score: 0.9, _source: { title: "Test Item 2", name: "Second" } },
            ],
            total: 2,
          },
          aggregations: {},
        },
      ],
    });
  })
);

// Start server before all tests
beforeAll(() => server.listen({ onUnhandledRequest: "bypass" }));

// Reset handlers after each test
afterEach(() => server.resetHandlers());

// Close server after all tests
afterAll(() => server.close());

// Helper component to spy on widget state
interface WidgetSpyProps {
  widgetId: string;
  onWidgetUpdate: (widget: Widget | undefined) => void;
}

function WidgetSpy({ widgetId, onWidgetUpdate }: WidgetSpyProps) {
  const [{ widgets }] = useSharedContext();
  const widget = widgets.get(widgetId);

  useEffect(() => {
    onWidgetUpdate(widget);
  }, [widget, onWidgetUpdate]);

  return null;
}

// Wrapper component to provide required context
const TestWrapper = ({ children }: { children: React.ReactNode }) => {
  return (
    <Antfly url="http://localhost:8082/api/v1" table="test">
      {children}
    </Antfly>
  );
};

describe("QueryBox", () => {
  describe("basic functionality", () => {
    it("should render with input element", () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" />
        </TestWrapper>
      );

      const input = container.querySelector("input");
      expect(input).toBeTruthy();
    });

    it("should accept initial value", () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" initialValue="hello" />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      expect(input?.value).toBe("hello");
    });

    it("should show submit button in submit mode", () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="submit" />
        </TestWrapper>
      );

      const submitButton = container.querySelector('button[type="submit"]');
      expect(submitButton).toBeTruthy();
    });

    it("should not show submit button in live mode", () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="live" />
        </TestWrapper>
      );

      const submitButton = container.querySelector('button[type="submit"]');
      expect(submitButton).toBeNull();
    });
  });

  describe("onInputChange callback", () => {
    it("should call onInputChange when user types", async () => {
      const onInputChange = vi.fn();

      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="submit" onInputChange={onInputChange} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      expect(onInputChange).toHaveBeenCalled();
      expect(onInputChange).toHaveBeenLastCalledWith("test");
    });

    it("should call onInputChange when clear button is clicked", async () => {
      const onInputChange = vi.fn();

      const { container } = render(
        <TestWrapper>
          <QueryBox
            id="test-search"
            mode="submit"
            initialValue="hello"
            onInputChange={onInputChange}
          />
        </TestWrapper>
      );

      // Reset mock since initial render might trigger it
      onInputChange.mockClear();

      const clearButton = container.querySelector(".react-af-querybox-clear");
      expect(clearButton).toBeTruthy();

      if (clearButton) {
        await userEvent.click(clearButton);
        expect(onInputChange).toHaveBeenCalledWith("");
      }
    });
  });

  describe("submit mode behavior", () => {
    it("should NOT set submittedAt when typing (submit mode)", async () => {
      let capturedWidget: Widget | undefined;
      const onWidgetUpdate = vi.fn((widget: Widget | undefined) => {
        capturedWidget = widget;
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="submit" />
          <WidgetSpy widgetId="test-search" onWidgetUpdate={onWidgetUpdate} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      // In submit mode, the local input value is updated...
      expect(input.value).toBe("test");
      // ...but the widget state in context is NOT updated until submit
      // Widget value stays at initial value (empty string)
      expect(capturedWidget?.value).toBe("");
      // And submittedAt should not be set
      expect(capturedWidget?.submittedAt).toBeUndefined();
    });

    it("should set submittedAt when form is submitted", async () => {
      let capturedWidget: Widget | undefined;
      const onWidgetUpdate = vi.fn((widget: Widget | undefined) => {
        capturedWidget = widget;
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="submit" />
          <WidgetSpy widgetId="test-search" onWidgetUpdate={onWidgetUpdate} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      // Submit the form
      const form = container.querySelector("form");
      expect(form).toBeTruthy();
      if (form) {
        await act(async () => {
          fireEvent.submit(form);
        });
      }

      await waitFor(() => {
        expect(capturedWidget?.submittedAt).toBeDefined();
      });
    });

    it("should set submittedAt when Enter key is pressed", async () => {
      let capturedWidget: Widget | undefined;
      const onWidgetUpdate = vi.fn((widget: Widget | undefined) => {
        capturedWidget = widget;
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="submit" />
          <WidgetSpy widgetId="test-search" onWidgetUpdate={onWidgetUpdate} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      // Reset to capture the submit
      const beforeSubmit = capturedWidget?.submittedAt;

      // Press Enter
      await userEvent.keyboard("{Enter}");

      await waitFor(() => {
        expect(capturedWidget?.submittedAt).toBeDefined();
        expect(capturedWidget?.submittedAt).not.toBe(beforeSubmit);
      });
    });
  });

  describe("live mode behavior", () => {
    it("should NOT set submittedAt when typing (live mode)", async () => {
      let capturedWidget: Widget | undefined;
      const onWidgetUpdate = vi.fn((widget: Widget | undefined) => {
        capturedWidget = widget;
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="live" />
          <WidgetSpy widgetId="test-search" onWidgetUpdate={onWidgetUpdate} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      // In live mode, typing updates the widget but doesn't set submittedAt
      await waitFor(() => {
        expect(capturedWidget?.value).toBe("test");
      });
      // Note: live mode typing doesn't set submittedAt - only autosuggest selection does
      expect(capturedWidget?.submittedAt).toBeUndefined();
    });
  });

  describe("autosuggest selection - auto-submit fix (commit a4759fc)", () => {
    it("should NOT auto-submit when selecting autosuggest in submit mode", async () => {
      let capturedWidget: Widget | undefined;
      const onWidgetUpdate = vi.fn((widget: Widget | undefined) => {
        capturedWidget = widget;
      });
      const onInputChange = vi.fn();

      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="submit" onInputChange={onInputChange}>
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <WidgetSpy widgetId="test-search" onWidgetUpdate={onWidgetUpdate} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Type to trigger autosuggest
      await userEvent.type(input, "test");

      // Wait for autosuggest to appear and results to load
      await waitFor(
        () => {
          const suggestions = container.querySelector(".react-af-autosuggest");
          expect(suggestions).toBeTruthy();
        },
        { timeout: 2000 }
      );

      // Record the state before selection
      const submittedAtBeforeSelection = capturedWidget?.submittedAt;
      onInputChange.mockClear();

      // Find and click the first suggestion
      const suggestionItems = container.querySelectorAll(".react-af-autosuggest-item");
      if (suggestionItems.length > 0) {
        await userEvent.click(suggestionItems[0]);

        // Wait for state update
        await waitFor(() => {
          // The input value should be updated with the suggestion
          expect(input.value).not.toBe("test");
        });

        // CRITICAL: submittedAt should NOT be set in submit mode
        // This is the fix from commit a4759fc
        expect(capturedWidget?.submittedAt).toBe(submittedAtBeforeSelection);
        expect(capturedWidget?.submittedAt).toBeUndefined();

        // onInputChange should have been called with the selected value
        expect(onInputChange).toHaveBeenCalled();
      }
    });

    it("should auto-update widget when selecting autosuggest in live mode", async () => {
      let capturedWidget: Widget | undefined;
      const onWidgetUpdate = vi.fn((widget: Widget | undefined) => {
        capturedWidget = widget;
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <WidgetSpy widgetId="test-search" onWidgetUpdate={onWidgetUpdate} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Type to trigger autosuggest
      await userEvent.type(input, "test");

      // Wait for autosuggest to appear
      await waitFor(
        () => {
          const suggestions = container.querySelector(".react-af-autosuggest");
          expect(suggestions).toBeTruthy();
        },
        { timeout: 2000 }
      );

      // Find and click the first suggestion
      const suggestionItems = container.querySelectorAll(".react-af-autosuggest-item");
      if (suggestionItems.length > 0) {
        await userEvent.click(suggestionItems[0]);

        // Wait for state update
        await waitFor(() => {
          // In live mode, submittedAt SHOULD be set when selecting a suggestion
          expect(capturedWidget?.submittedAt).toBeDefined();
        });
      }
    });

    it("should call onInputChange when autosuggest populates input", async () => {
      const onInputChange = vi.fn();

      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="submit" onInputChange={onInputChange}>
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Type to trigger autosuggest
      await userEvent.type(input, "test");

      // Wait for autosuggest to appear
      await waitFor(
        () => {
          const suggestions = container.querySelector(".react-af-autosuggest");
          expect(suggestions).toBeTruthy();
        },
        { timeout: 2000 }
      );

      // Clear the mock to only capture selection
      onInputChange.mockClear();

      // Find and click the first suggestion
      const suggestionItems = container.querySelectorAll(".react-af-autosuggest-item");
      if (suggestionItems.length > 0) {
        await userEvent.click(suggestionItems[0]);

        // onInputChange should have been called when autosuggest populated the input
        await waitFor(() => {
          expect(onInputChange).toHaveBeenCalled();
        });

        // The value should be from the suggestion (title field)
        const lastCall = onInputChange.mock.calls[onInputChange.mock.calls.length - 1];
        expect(lastCall[0]).toBeTruthy(); // Should have a value
        expect(lastCall[0]).not.toBe("test"); // Should be different from typed value
      }
    });

    it("should allow user to review and then submit in submit mode", async () => {
      let capturedWidget: Widget | undefined;
      const onWidgetUpdate = vi.fn((widget: Widget | undefined) => {
        capturedWidget = widget;
      });
      const onSubmit = vi.fn();

      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="submit" onSubmit={onSubmit}>
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <WidgetSpy widgetId="test-search" onWidgetUpdate={onWidgetUpdate} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Type to trigger autosuggest
      await userEvent.type(input, "test");

      // Wait for autosuggest to appear
      await waitFor(
        () => {
          const suggestions = container.querySelector(".react-af-autosuggest");
          expect(suggestions).toBeTruthy();
        },
        { timeout: 2000 }
      );

      // Find and click the first suggestion
      const suggestionItems = container.querySelectorAll(".react-af-autosuggest-item");
      if (suggestionItems.length > 0) {
        await userEvent.click(suggestionItems[0]);

        // Wait for input to be populated
        await waitFor(() => {
          expect(input.value).not.toBe("test");
        });

        // At this point, NO submission should have occurred
        expect(capturedWidget?.submittedAt).toBeUndefined();
        expect(onSubmit).not.toHaveBeenCalled();

        // Focus the input and press Enter to submit
        input.focus();
        await userEvent.type(input, "{Enter}");

        // NOW submission should occur
        await waitFor(() => {
          expect(capturedWidget?.submittedAt).toBeDefined();
          expect(onSubmit).toHaveBeenCalled();
        });
      }
    });
  });

  describe("custom onSuggestionSelect handler", () => {
    it("should use custom handler instead of default when provided", async () => {
      const customHandler = vi.fn();

      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="submit">
            <Autosuggest
              fields={["title__keyword"]}
              minChars={1}
              onSuggestionSelect={customHandler}
            />
          </QueryBox>
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Type to trigger autosuggest
      await userEvent.type(input, "test");

      // Wait for autosuggest to appear
      await waitFor(
        () => {
          const suggestions = container.querySelector(".react-af-autosuggest");
          expect(suggestions).toBeTruthy();
        },
        { timeout: 2000 }
      );

      // Find and click the first suggestion
      const suggestionItems = container.querySelectorAll(".react-af-autosuggest-item");
      if (suggestionItems.length > 0) {
        await userEvent.click(suggestionItems[0]);

        // Custom handler should be called
        await waitFor(() => {
          expect(customHandler).toHaveBeenCalled();
        });

        // Input value should NOT change (custom handler decides what to do)
        expect(input.value).toBe("test");
      }
    });
  });

  describe("escape key behavior", () => {
    it("should close autosuggest on Escape in live mode", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Type to trigger autosuggest
      await userEvent.type(input, "test");

      // Wait for autosuggest to appear
      await waitFor(
        () => {
          const suggestions = container.querySelector(".react-af-autosuggest");
          expect(suggestions).toBeTruthy();
        },
        { timeout: 2000 }
      );

      // Press Escape
      await userEvent.keyboard("{Escape}");

      // Autosuggest should close
      await waitFor(() => {
        const suggestions = container.querySelector(".react-af-autosuggest");
        expect(suggestions).toBeNull();
      });

      // Input value should remain
      expect(input.value).toBe("test");
    });

    it("should clear input on second Escape in live mode", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="test-search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Type to trigger autosuggest
      await userEvent.type(input, "test");

      // Wait for autosuggest to appear
      await waitFor(
        () => {
          const suggestions = container.querySelector(".react-af-autosuggest");
          expect(suggestions).toBeTruthy();
        },
        { timeout: 2000 }
      );

      // First Escape closes autosuggest
      await userEvent.keyboard("{Escape}");

      await waitFor(() => {
        const suggestions = container.querySelector(".react-af-autosuggest");
        expect(suggestions).toBeNull();
      });

      // Second Escape clears input
      await userEvent.keyboard("{Escape}");

      await waitFor(() => {
        expect(input.value).toBe("");
      });
    });
  });
});
