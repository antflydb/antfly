import type { QueryHit } from "@antfly/sdk";
import React, { type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSharedContext } from "./SharedContext";

/**
 * Props passed to custom input components via renderInput.
 * Custom inputs must call onChange when value changes and onSubmit for form submission.
 */
export interface CustomInputProps {
  /** Current input value */
  value: string;
  /** Called when the input value changes */
  onChange: (value: string) => void;
  /** Called to submit the current value */
  onSubmit: (value: string) => void;
  /** Keyboard event handler - custom input should forward keyboard events */
  onKeyDown: (event: React.KeyboardEvent) => void;
  /** Whether the autosuggest dropdown is open */
  isSuggestOpen: boolean;
  /** Close the autosuggest dropdown */
  onSuggestClose: () => void;
  /** Placeholder text */
  placeholder?: string;
  /** Whether the input is disabled */
  disabled?: boolean;
  /** ID for the input element */
  id?: string;
}

export interface QueryBoxProps {
  id: string;
  mode?: "live" | "submit";
  initialValue?: string;
  placeholder?: string;
  children?: ReactNode;
  buttonLabel?: string;
  onSubmit?: (value: string) => void;
  onInputChange?: (value: string) => void;
  onEscape?: (clearInput: () => void) => boolean; // Return true to prevent default clear behavior
  /**
   * Custom input renderer. When provided, replaces the default <input> element.
   * The custom component receives props for value management, submission, and keyboard handling.
   * Note: When renderInput is provided, QueryBox does not wrap content in a <form> element,
   * as custom inputs like PromptInput may have their own form handling.
   */
  renderInput?: (props: CustomInputProps) => React.ReactNode;
}

export default function QueryBox({
  id,
  mode = "live",
  initialValue,
  placeholder,
  children,
  buttonLabel = "Submit",
  onSubmit,
  onInputChange,
  onEscape,
  renderInput,
}: QueryBoxProps) {
  const [{ widgets }, dispatch] = useSharedContext();
  const [value, setValue] = useState(initialValue || "");
  const isExternalUpdate = useRef(false);
  const [isSuggestOpen, setIsSuggestOpen] = useState(false);
  const [containerRefObject] = useState<{ current: HTMLDivElement | null }>({ current: null });
  const containerRef = useCallback(
    (node: HTMLDivElement | null) => {
      // eslint-disable-next-line react-hooks/immutability
      containerRefObject.current = node;
    },
    [containerRefObject]
  );

  // Update widget state in context
  const updateWidget = useCallback(
    (v: string, shouldSubmit = false) => {
      dispatch({
        type: "setWidget",
        key: id,
        needsQuery: false, // QueryBox doesn't contribute a query
        needsConfiguration: false,
        isFacet: false,
        rootQuery: true, // Still a root query widget for isolation logic
        wantResults: false,
        value: v,
        submittedAt: shouldSubmit ? Date.now() : undefined,
      });
    },
    [dispatch, id]
  );

  // Initialize on mount
  useEffect(() => {
    const valueToSet = initialValue || "";
    setValue(valueToSet);
    updateWidget(valueToSet);
  }, [initialValue, updateWidget]); // eslint-disable-line react-hooks/exhaustive-deps

  // Sync with external updates (e.g., from ActiveFilters)
  // NOTE: Only sync in "live" mode - in "submit" mode, local state is authoritative until form submission
  const widgetValue = widgets.get(id)?.value;
  useEffect(() => {
    // Skip sync in submit mode - local state is authoritative
    if (mode === "submit") {
      return;
    }
    if (widgetValue !== undefined && widgetValue !== value && !isExternalUpdate.current) {
      isExternalUpdate.current = true;
      setValue(String(widgetValue || ""));
      isExternalUpdate.current = false;
    }
  }, [widgetValue, value, mode]);

  // Handle input changes
  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const newValue = e.target.value;
      setValue(newValue);

      // Call onInputChange callback if provided
      if (onInputChange) {
        onInputChange(newValue);
      }

      // Update widget immediately for live mode
      if (mode === "live") {
        updateWidget(newValue);
      }

      // Open autosuggest if there are children
      if (newValue.trim()) {
        setIsSuggestOpen(true);
      }
    },
    [mode, updateWidget, onInputChange]
  );

  // Handle form submission (submit mode only)
  const handleSubmit = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault();

      // Close autosuggest dropdown on submit
      setIsSuggestOpen(false);

      // Call custom onSubmit callback if provided
      if (onSubmit) {
        onSubmit(value);
      }

      // Update widget state with submission timestamp
      updateWidget(value, true);
    },
    [value, updateWidget, onSubmit]
  );

  // Handle clear button click
  const handleClear = useCallback(() => {
    setValue("");
    if (onInputChange) {
      onInputChange("");
    }

    if (mode === "live") {
      updateWidget("");
    } else {
      // For submit mode, clear and submit
      updateWidget("", true);
    }

    setIsSuggestOpen(false);
  }, [mode, updateWidget, onInputChange]);

  // Simple function to clear just the input value (no widget state reset)
  const clearInputOnly = useCallback(() => {
    setValue("");
    if (onInputChange) {
      onInputChange("");
    }
  }, [onInputChange]);

  // Handle keyboard events (supports both input and textarea elements for custom inputs)
  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      // For custom inputs like PromptInput, Shift+Enter is used for newlines
      if (e.key === "Enter" && !e.shiftKey && mode === "submit") {
        e.preventDefault();

        // Close autosuggest dropdown on submit
        setIsSuggestOpen(false);

        // Call custom onSubmit callback if provided
        if (onSubmit) {
          onSubmit(value);
        }

        // Update widget state and trigger query
        updateWidget(value, true);
      } else if (e.key === "Escape") {
        e.preventDefault();

        // Allow parent to handle escape if onEscape is provided
        if (onEscape) {
          const shouldPreventDefault = onEscape(clearInputOnly);
          if (shouldPreventDefault) {
            return;
          }
        }

        // Default behavior for live mode: close autosuggest first, then clear
        if (mode === "live") {
          if (isSuggestOpen) {
            setIsSuggestOpen(false);
          } else if (value) {
            handleClear();
          }
        } else {
          // Submit mode: clear the input
          handleClear();
        }
      }
    },
    [mode, value, isSuggestOpen, onSubmit, onEscape, updateWidget, clearInputOnly, handleClear]
  );

  // Handle suggestion selection from Autosuggest
  const handleSuggestionSelect = useCallback(
    (suggestion: QueryHit) => {
      // Close autosuggest when a suggestion is selected
      setIsSuggestOpen(false);

      // If the child component had its own onSuggestionSelect, call it first
      const childProps = React.isValidElement(children)
        ? (children as React.ReactElement<{ onSuggestionSelect?: (hit: QueryHit) => void }>).props
        : {};
      const originalHandler = childProps.onSuggestionSelect;

      if (originalHandler) {
        // Let the custom handler decide what to do (e.g., navigate away)
        originalHandler(suggestion);
        // Don't update search box value if custom handler provided
        return;
      }

      // Default behavior: update search box with selected value
      // For now, we'll use a simple approach - get the first field value or _id
      let valueToSet = "";

      if (suggestion._source) {
        // Try common text fields first
        const commonTextFields = [
          "title",
          "name",
          "label",
          "text",
          "description",
          "question",
          "content",
        ];
        const sourceFields = Object.keys(suggestion._source);

        for (const commonField of commonTextFields) {
          if (suggestion._source[commonField]) {
            valueToSet = String(suggestion._source[commonField]);
            break;
          }
        }

        // If no common field found, use first available field
        if (!valueToSet && sourceFields.length > 0) {
          const firstAvailableField = sourceFields[0];
          const fieldValue = suggestion._source[firstAvailableField];
          if (fieldValue && typeof fieldValue !== "object") {
            valueToSet = String(fieldValue);
          }
        }
      }

      // Last resort: use the document ID
      if (!valueToSet) {
        valueToSet = suggestion._id || "";
      }

      setValue(valueToSet);

      // Call onInputChange to notify parent of the new value
      if (onInputChange) {
        onInputChange(valueToSet);
      }

      // Update widget state with the new value but DON'T auto-submit in submit mode
      // This allows the user to review the selected value before pressing Enter to submit
      updateWidget(valueToSet, mode === "live");
    },
    [mode, updateWidget, onInputChange, children]
  );

  // Cleanup on unmount
  useEffect(() => () => dispatch({ type: "deleteWidget", key: id }), [dispatch, id]);

  // Handler for custom input onChange
  const handleCustomInputChange = useCallback(
    (newValue: string) => {
      setValue(newValue);

      if (onInputChange) {
        onInputChange(newValue);
      }

      if (mode === "live") {
        updateWidget(newValue);
      }

      if (newValue.trim()) {
        setIsSuggestOpen(true);
      }
    },
    [mode, updateWidget, onInputChange]
  );

  // Handler for custom input onSubmit
  const handleCustomInputSubmit = useCallback(
    (submittedValue: string) => {
      setIsSuggestOpen(false);

      if (onSubmit) {
        onSubmit(submittedValue);
      }

      updateWidget(submittedValue, true);
    },
    [onSubmit, updateWidget]
  );

  // Build props for custom input
  const customInputProps: CustomInputProps = useMemo(
    () => ({
      value,
      onChange: handleCustomInputChange,
      onSubmit: handleCustomInputSubmit,
      onKeyDown: handleKeyDown,
      isSuggestOpen,
      onSuggestClose: () => setIsSuggestOpen(false),
      placeholder: placeholder || (mode === "submit" ? "Ask a question..." : "search…"),
      id: `${id}-input`,
    }),
    [
      value,
      handleCustomInputChange,
      handleCustomInputSubmit,
      handleKeyDown,
      isSuggestOpen,
      placeholder,
      mode,
      id,
    ]
  );

  const inputElement = (
    <input
      type="text"
      value={value}
      onChange={handleChange}
      onKeyDown={handleKeyDown}
      placeholder={placeholder || (mode === "submit" ? "Ask a question..." : "search…")}
    />
  );

  const clearButton = value && (
    <button
      type="button"
      className="react-af-querybox-clear"
      onClick={handleClear}
      aria-label="Clear"
    >
      ×
    </button>
  );

  // Render either custom input or default input element
  const renderedInput = renderInput ? renderInput(customInputProps) : inputElement;

  const content = (
    <>
      {renderedInput}
      {/* Only show clear button and submit button for default input */}
      {!renderInput && clearButton}
      {!renderInput && mode === "submit" && (
        <button type="submit" className="react-af-querybox-submit" disabled={!value.trim()}>
          {buttonLabel}
        </button>
      )}
      {children &&
        React.Children.map(children, (child) => {
          if (React.isValidElement(child)) {
            // Only clone props onto custom components (not native DOM elements)
            if (typeof child.type === "function" || typeof child.type === "object") {
              // Preserve the original onSuggestionSelect if it exists
              const originalOnSuggestionSelect = (child.props as Record<string, unknown>)
                ?.onSuggestionSelect as ((hit: QueryHit) => void) | undefined;

              return React.cloneElement(child as React.ReactElement<Record<string, unknown>>, {
                searchValue: mode === "submit" && !isSuggestOpen ? "" : value,
                onSuggestionSelect: originalOnSuggestionSelect || handleSuggestionSelect,
                containerRef: containerRefObject,
                isOpen: isSuggestOpen,
                onClose: () => setIsSuggestOpen(false),
              });
            }
          }
          return child;
        })}
    </>
  );

  // When renderInput is provided, skip the form wrapper as custom inputs (like PromptInput)
  // may have their own form handling
  const shouldWrapInForm = mode === "submit" && !renderInput;

  return (
    <div className="react-af-querybox" ref={containerRef}>
      {shouldWrapInForm ? <form onSubmit={handleSubmit}>{content}</form> : content}
    </div>
  );
}
