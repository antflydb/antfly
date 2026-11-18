import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ConnectionStatusBanner } from "./connection-status-banner";

vi.mock("@/hooks/use-api-config", () => ({
  useApiConfig: () => ({ apiUrl: "/hosted/instance/db/v1", inferenceApiUrl: "/hosted/instance" }),
}));
vi.mock("@/config/products", () => ({ isProductEnabled: () => true }));
const fetchMock = vi.fn<typeof fetch>();

beforeEach(() => {
  fetchMock.mockReset();
  vi.stubGlobal("fetch", fetchMock);
  window.__ANTFARM_CONFIG__ = { authMode: "external" };
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  delete window.__ANTFARM_CONFIG__;
});

describe("connection failure guidance", () => {
  it.each([
    [401, "Sign-in required", "hosting dashboard"],
    [403, "Access denied", "permissions"],
    [503, "Service unavailable", "hosting dashboard"],
  ])("explains hosted HTTP %i without local startup advice", async (status, title, guidance) => {
    fetchMock.mockImplementation(async () => new Response(null, { status }));
    render(<ConnectionStatusBanner />);
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain(title);
    expect(alert.textContent).toContain(guidance);
    expect(alert.textContent).not.toMatch(/not running|standalone|Settings/);
  });

  it("shows a known auth failure while the other check is still pending", async () => {
    fetchMock.mockImplementation((url) =>
      String(url).endsWith("/status")
        ? Promise.resolve(new Response(null, { status: 401 }))
        : new Promise(() => {})
    );
    render(<ConnectionStatusBanner />);
    expect((await screen.findByRole("alert")).textContent).toContain("Sign-in required");
  });

  it("keeps mixed authentication and availability failures distinct", async () => {
    fetchMock.mockImplementation(
      async (url) =>
        new Response(null, {
          status: String(url).endsWith("/status") ? 401 : 503,
        })
    );
    render(<ConnectionStatusBanner />);
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("Antfly data: Sign-in required");
    expect(alert.textContent).toContain("Antfly inference: Service unavailable");
  });

  it("retains local connection guidance for a transport failure", async () => {
    window.__ANTFARM_CONFIG__ = { authMode: "local" };
    fetchMock.mockRejectedValue(new TypeError("Failed to fetch"));
    render(<ConnectionStatusBanner />);
    expect((await screen.findByRole("alert")).textContent).toContain("./antfly standalone");
  });

  it("uses local sign-in guidance for local authentication failures", async () => {
    window.__ANTFARM_CONFIG__ = { authMode: "local" };
    fetchMock.mockImplementation(async () => new Response(null, { status: 401 }));
    render(<ConnectionStatusBanner />);
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("Sign in to Antfarm");
    expect(alert.textContent).not.toContain("standalone");
  });

  it("clears the failure when Retry reconnects both services", async () => {
    fetchMock.mockImplementation(async () => new Response(null, { status: 401 }));
    render(<ConnectionStatusBanner />);
    await screen.findByRole("alert");
    fetchMock.mockImplementation(async () => new Response(null, { status: 200 }));
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Retry" })));
    await waitFor(() => expect(screen.queryByRole("alert")).toBeNull());
  });
});
