import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AIWorkflowSubmitButton } from "./ai-workflow";

afterEach(cleanup);

describe("AIWorkflowSubmitButton", () => {
  it("runs the cancellation handler instead of the submit handler while running", () => {
    const onCancel = vi.fn();
    const onSubmit = vi.fn();
    render(
      <AIWorkflowSubmitButton running onCancel={onCancel} onClick={onSubmit}>
        Run
      </AIWorkflowSubmitButton>
    );

    fireEvent.click(screen.getByRole("button", { name: "Stop" }));

    expect(onCancel).toHaveBeenCalledOnce();
    expect(onSubmit).not.toHaveBeenCalled();
  });
});
