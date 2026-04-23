import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useApiConfig } from "@/hooks/use-api-config";
import { ApiConfigProvider } from "./api-config-provider";

const { antflyClientMock, isProductEnabledMock } = vi.hoisted(() => ({
  antflyClientMock: vi.fn(function thisMock(this: { baseUrl?: string }, {
    baseUrl,
  }: {
    baseUrl: string;
  }) {
    this.baseUrl = baseUrl;
  }),
  isProductEnabledMock: vi.fn((product: string) => product === "antfly"),
}));

vi.mock("@antfly/sdk", () => ({
  AntflyClient: antflyClientMock,
}));

vi.mock("@/config/products", () => ({
  isProductEnabled: isProductEnabledMock,
}));

const originalLocalStorageDescriptor = Object.getOwnPropertyDescriptor(window, "localStorage");

function createLocalStorageMock() {
  const store = new Map<string, string>();
  return {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => {
      store.set(key, value);
    },
    removeItem: (key: string) => {
      store.delete(key);
    },
    clear: () => {
      store.clear();
    },
  };
}

function Consumer() {
  const {
    apiUrl,
    termiteApiUrl,
    client,
    setApiUrl,
    setTermiteApiUrl,
    resetToDefault,
    resetTermiteApiUrl,
  } = useApiConfig();

  return (
    <>
      <div data-testid="api-url">{apiUrl}</div>
      <div data-testid="termite-url">{termiteApiUrl}</div>
      <div data-testid="client-url">{String((client as { baseUrl?: string }).baseUrl)}</div>
      <button type="button" onClick={() => setApiUrl("  http://localhost:9000/api  ")}>
        Set API URL
      </button>
      <button type="button" onClick={() => setTermiteApiUrl("  https://cloud.example/termite  ")}>
        Set Termite URL
      </button>
      <button type="button" onClick={resetToDefault}>
        Reset API URL
      </button>
      <button type="button" onClick={resetTermiteApiUrl}>
        Reset Termite URL
      </button>
    </>
  );
}

describe("ApiConfigProvider", () => {
  beforeEach(() => {
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      value: createLocalStorageMock(),
    });
    window.localStorage.clear();
    antflyClientMock.mockClear();
    isProductEnabledMock.mockReset();
    isProductEnabledMock.mockImplementation((product: string) => product === "antfly");
  });

  afterEach(() => {
    cleanup();
    if (originalLocalStorageDescriptor) {
      Object.defineProperty(window, "localStorage", originalLocalStorageDescriptor);
    }
  });

  it("uses embedded-dashboard defaults when antfly is enabled", () => {
    render(
      <ApiConfigProvider>
        <Consumer />
      </ApiConfigProvider>
    );

    expect(screen.getByTestId("api-url").textContent).toBe("/api/v1");
    expect(screen.getByTestId("termite-url").textContent).toBe("/termite");
    expect(screen.getByTestId("client-url").textContent).toBe("/api/v1");
  });

  it("uses a same-origin termite URL in termite-only mode", () => {
    isProductEnabledMock.mockReturnValue(false);

    render(
      <ApiConfigProvider>
        <Consumer />
      </ApiConfigProvider>
    );

    expect(screen.getByTestId("termite-url").textContent).toBe("");
  });

  it("persists trimmed URLs and updates the sdk client", () => {
    render(
      <ApiConfigProvider>
        <Consumer />
      </ApiConfigProvider>
    );

    fireEvent.click(screen.getByRole("button", { name: "Set API URL" }));
    fireEvent.click(screen.getByRole("button", { name: "Set Termite URL" }));

    expect(screen.getByTestId("api-url").textContent).toBe("http://localhost:9000/api");
    expect(screen.getByTestId("termite-url").textContent).toBe("https://cloud.example/termite");
    expect(screen.getByTestId("client-url").textContent).toBe("http://localhost:9000/api");
    expect(localStorage.getItem("antfarm-api-url")).toBe("http://localhost:9000/api");
    expect(localStorage.getItem("antfarm-termite-api-url")).toBe("https://cloud.example/termite");
  });

  it("resets persisted URLs back to defaults", () => {
    render(
      <ApiConfigProvider>
        <Consumer />
      </ApiConfigProvider>
    );

    fireEvent.click(screen.getByRole("button", { name: "Set API URL" }));
    fireEvent.click(screen.getByRole("button", { name: "Set Termite URL" }));
    fireEvent.click(screen.getByRole("button", { name: "Reset API URL" }));
    fireEvent.click(screen.getByRole("button", { name: "Reset Termite URL" }));

    expect(screen.getByTestId("api-url").textContent).toBe("/api/v1");
    expect(screen.getByTestId("termite-url").textContent).toBe("/termite");
    expect(screen.getByTestId("client-url").textContent).toBe("/api/v1");
    expect(localStorage.getItem("antfarm-api-url")).toBeNull();
    expect(localStorage.getItem("antfarm-termite-api-url")).toBeNull();
  });
});
