import { createContext, useContext } from "react";
import type { ChatConfig, ChatTurn } from "./hooks/useChatStream";

/** Context value for chat components */
export interface ChatContextValue {
  /** All conversation turns */
  turns: ChatTurn[];
  /** Whether any turn is currently streaming */
  isStreaming: boolean;
  /** Send a new message to the agent */
  sendMessage: (text: string) => void;
  /** Send a follow-up question */
  sendFollowUp: (question: string) => void;
  /** Respond to a clarification request */
  respondToClarification: (response: string) => void;
  /** Abort the current stream */
  abort: () => void;
  /** Reset the conversation */
  reset: () => void;
  /** Current chat configuration */
  config: ChatConfig;
}

export const ChatContext = createContext<ChatContextValue | null>(null);

export function useChatContext(): ChatContextValue {
  const context = useContext(ChatContext);
  if (!context) {
    throw new Error("useChatContext must be used within a ChatBar component");
  }
  return context;
}
