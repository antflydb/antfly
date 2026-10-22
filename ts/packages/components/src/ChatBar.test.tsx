import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import Antfly from "./Antfly";
import ChatBar from "./ChatBar";
import * as utils from "./utils";

vi.mock("./utils", async () => ({
  ...(await vi.importActual<typeof utils>("./utils")),
  streamAnswer: vi.fn(),
}));

beforeEach(() => {
  vi.clearAllMocks();
  Element.prototype.scrollIntoView = vi.fn();
});
describe("ChatBar source changes", () => {
  it("cancels the old conversation and keeps the unsent draft for the new table", async () => {
    let signal: AbortSignal | undefined;
    vi.mocked(utils.streamAnswer).mockImplementation(
      async (_url, _request, _headers, _callbacks, options) => {
        signal = options?.signal;
        return new AbortController();
      }
    );
    const content = (table: string) => (
      <Antfly url="http://localhost:8082/db/v1" table={table}>
        <ChatBar
          id="chat"
          generator={{ provider: "antfly", model: "test" }}
          renderInput={({ value, onChange, onSubmit }) => (
            <>
              <input
                aria-label="draft"
                value={value}
                onChange={(event) => onChange(event.target.value)}
              />
              <button type="button" onClick={onSubmit}>
                Send message
              </button>
            </>
          )}
        />
      </Antfly>
    );
    const { rerender } = render(content("docs"));
    fireEvent.change(screen.getByLabelText("draft"), { target: { value: "first question" } });
    fireEvent.click(screen.getByRole("button", { name: "Send message" }));
    await waitFor(() => expect(utils.streamAnswer).toHaveBeenCalledOnce());
    fireEvent.change(screen.getByLabelText("draft"), { target: { value: "unsent draft" } });
    await act(async () => rerender(content("other")));
    expect(signal?.aborted).toBe(true);
    expect(screen.queryByText("first question")).toBeNull();
    expect((screen.getByLabelText("draft") as HTMLInputElement).value).toBe("unsent draft");
    fireEvent.click(screen.getByRole("button", { name: "Send message" }));
    await waitFor(() => expect(utils.streamAnswer).toHaveBeenCalledTimes(2));
    expect(vi.mocked(utils.streamAnswer).mock.calls[1][1].queries?.[0].table).toBe("other");
  });
});
