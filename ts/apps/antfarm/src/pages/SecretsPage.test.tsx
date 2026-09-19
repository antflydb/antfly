import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SecretsPage } from "./SecretsPage";

vi.mock("../hooks/use-api-config", () => ({
  useApiConfig: () => ({ apiUrl: "http://localhost:8080" }),
}));

const entries = [
  { key: "native.token", status: "configured_file", source: "native", managed: true },
  { key: "external.token", status: "configured_file", source: "platform", managed: false },
];

describe("SecretsPage source ownership", () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("shows external provenance and hides writes when no native store exists", async () => {
    const fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ writable: false, secrets: [entries[1]] }),
    });
    vi.stubGlobal("fetch", fetch);
    render(<SecretsPage />);
    expect(await screen.findByText("platform")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Add Secret" })).toBeNull();
    expect(screen.queryByRole("button", { name: /Delete native override/ })).toBeNull();
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0][0]).toBe("http://localhost:8080/secrets");
  });

  it("allows deleting only native overrides and explains fallback behavior", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ writable: true, secrets: entries }),
      })
    );
    render(<SecretsPage />);
    expect(await screen.findByRole("button", { name: "Add Secret" })).toBeTruthy();
    expect(
      screen.queryByRole("button", { name: "Delete native override for external.token" })
    ).toBeNull();
    fireEvent.click(
      screen.getByRole("button", { name: "Delete native override for native.token" })
    );
    expect(await screen.findByText(/environment may become active again/)).toBeTruthy();
  });
});
