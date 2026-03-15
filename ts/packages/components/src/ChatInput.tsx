import type { ReactNode } from "react";
import { useCallback, useState } from "react";
import { useChatContext } from "./ChatContext";

export interface ChatInputProps {
  /** Placeholder text for the input */
  placeholder?: string;
  /** Custom renderer for the input area */
  renderInput?: (props: {
    value: string;
    onChange: (value: string) => void;
    onSubmit: () => void;
    isStreaming: boolean;
    placeholder: string;
    /** Abort the current stream */
    abort: () => void;
  }) => ReactNode;
}

export default function ChatInput({
  placeholder = "Type a message...",
  renderInput,
}: ChatInputProps) {
  const { sendMessage, isStreaming, abort } = useChatContext();
  const [value, setValue] = useState("");

  const handleSubmit = useCallback(() => {
    const trimmed = value.trim();
    if (!trimmed || isStreaming) return;
    sendMessage(trimmed);
    setValue("");
  }, [value, isStreaming, sendMessage]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        handleSubmit();
      }
    },
    [handleSubmit]
  );

  if (renderInput) {
    return (
      <div className="react-af-chat-input">
        {renderInput({
          value,
          onChange: setValue,
          onSubmit: handleSubmit,
          isStreaming,
          placeholder,
          abort,
        })}
      </div>
    );
  }

  return (
    <div className="react-af-chat-input">
      <form
        className="react-af-chat-input-form"
        onSubmit={(e) => {
          e.preventDefault();
          handleSubmit();
        }}
      >
        <input
          type="text"
          className="react-af-chat-input-field"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          disabled={isStreaming}
        />
        <button
          type="submit"
          className="react-af-chat-input-submit"
          disabled={isStreaming || !value.trim()}
        >
          Send
        </button>
      </form>
    </div>
  );
}
