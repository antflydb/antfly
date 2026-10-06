import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { useGeneratorPreference } from "@/hooks/use-generator-preference";
import { GeneratorPreferenceProvider } from "./generator-preference-provider";

function Consumer() {
  const { dashboardGenerator, setDashboardGenerator } = useGeneratorPreference();

  return (
    <>
      <div data-testid="generator">
        {dashboardGenerator
          ? "model" in dashboardGenerator
            ? dashboardGenerator.model
            : dashboardGenerator.provider
          : "none"}
      </div>
      <button
        type="button"
        onClick={() =>
          setDashboardGenerator({
            provider: "openai",
            model: "gpt-4.1",
            temperature: 0.7,
          })
        }
      >
        Set Generator
      </button>
    </>
  );
}

function renderWithProvider(children: ReactNode) {
  return render(<GeneratorPreferenceProvider>{children}</GeneratorPreferenceProvider>);
}

const originalLocalStorageDescriptor = Object.getOwnPropertyDescriptor(window, "localStorage");

describe("GeneratorPreferenceProvider", () => {
  it("restores an Apple preference without a model", () => {
    window.localStorage.setItem(
      "antfarm-dashboard-generator",
      JSON.stringify({ provider: "apple", max_tokens: 256 })
    );
    try {
      renderWithProvider(<Consumer />);
      expect(screen.getByTestId("generator").textContent).toBe("apple");
    } finally {
      window.localStorage.removeItem("antfarm-dashboard-generator");
    }
  });

  afterEach(() => {
    cleanup();
    if (originalLocalStorageDescriptor) {
      Object.defineProperty(window, "localStorage", originalLocalStorageDescriptor);
    }
  });

  it("falls back to in-memory state when localStorage access throws during load", () => {
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      get() {
        throw new DOMException("blocked", "SecurityError");
      },
    });

    expect(() => renderWithProvider(<Consumer />)).not.toThrow();
    expect(screen.getByTestId("generator").textContent).toBe("none");
  });

  it("does not crash when persisting the preference fails", () => {
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      value: {
        getItem: () => null,
        setItem: () => {
          throw new DOMException("quota exceeded", "QuotaExceededError");
        },
        removeItem: () => undefined,
      },
    });

    renderWithProvider(<Consumer />);
    expect(() =>
      fireEvent.click(screen.getByRole("button", { name: "Set Generator" }))
    ).not.toThrow();
    expect(screen.getByTestId("generator").textContent).toBe("gpt-4.1");
  });
});

function ConfigurationConsumer() {
  const { dashboardGenerator } = useGeneratorPreference();
  return <pre data-testid="configuration">{JSON.stringify(dashboardGenerator)}</pre>;
}
it("loads only an opaque connection and model for a stored personal plan", () => {
  const raw = JSON.stringify({
    provider: "chatgpt",
    model: "catalog-slug",
    connection_id: "one",
    api_key: "should-not-survive",
    refresh_token: "should-not-survive",
  });
  Object.defineProperty(window, "localStorage", {
    configurable: true,
    value: { getItem: () => raw },
  });
  try {
    renderWithProvider(<ConfigurationConsumer />);
    expect(JSON.parse(screen.getByTestId("configuration").textContent || "null")).toEqual({
      provider: "chatgpt",
      model: "catalog-slug",
      connection_id: "one",
    });
  } finally {
    cleanup();
    if (originalLocalStorageDescriptor)
      Object.defineProperty(window, "localStorage", originalLocalStorageDescriptor);
  }
});
