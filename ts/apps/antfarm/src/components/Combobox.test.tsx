import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Combobox } from "./Combobox";

beforeEach(() => {
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    }
  );
  Element.prototype.scrollIntoView = vi.fn();
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

function openPicker(value = "original") {
  const onChange = vi.fn();
  render(
    <Combobox
      options={[
        { value: "original", label: "Original" },
        { value: "invoice-extractor", label: "Invoice extractor" },
      ]}
      value={value}
      onChange={onChange}
      allowCustomValue
    />
  );
  fireEvent.click(screen.getByRole("combobox"));
  return { onChange, input: screen.getByPlaceholderText("Search options...") };
}

describe("Combobox model selection", () => {
  it("keeps typing separate from selection and commits an exact matching option", () => {
    const { onChange, input } = openPicker();
    fireEvent.change(input, { target: { value: "invoice-extractor" } });
    expect(onChange).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("option", { name: "Invoice extractor" }));
    expect(onChange).toHaveBeenCalledExactlyOnceWith("invoice-extractor");
  });

  it("does not clear an already selected option", () => {
    const { onChange } = openPicker();
    fireEvent.click(screen.getByRole("option", { name: "Original" }));
    expect(onChange).toHaveBeenCalledExactlyOnceWith("original");
  });

  it("commits a custom value only through explicit confirmation", () => {
    const { onChange, input } = openPicker();
    fireEvent.change(input, { target: { value: "custom-model" } });
    expect(onChange).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("option", { name: "Use “custom-model”" }));
    expect(onChange).toHaveBeenCalledExactlyOnceWith("custom-model");
  });

  it("lets Enter select a searched option instead of committing the search fragment", () => {
    const { onChange, input } = openPicker();
    fireEvent.change(input, { target: { value: "invoice" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onChange).toHaveBeenCalledExactlyOnceWith("invoice-extractor");
  });
});
