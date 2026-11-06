import { type AntflyClient, AntflyHTTPError } from "@antfly/sdk";
import type { UIMessage } from "ai";

export interface AntflyChatHistoryOptions {
  client: AntflyClient;
  /** Table used to store chat history rows. Fixed in code. Must already exist. */
  table: string;
  /** Key prefix for history rows, to share a table with other data. Default "chat_history/". */
  keyPrefix?: string;
}

interface ChatHistoryRow {
  messages: UIMessage[];
  updatedAt: string;
}

/**
 * Persists `ai` v7 `UIMessage[]` chat history in a fixed Antfly table, for
 * use from `streamText(...).toUIMessageStreamResponse({ onFinish })` (or
 * `generateText`'s own completion) and for loading history back for
 * `useChat`.
 *
 * Writes use `sync_level: "full_index"`. On Lite storage, a write at the
 * default `sync_level` may not be visible for a few milliseconds and an
 * ordered query can return `live_read: true` with no cursor; chat history
 * must read its own writes (the very next request may reload the chat it
 * just saved), so this trades a touch of write latency for that guarantee
 * rather than adding a read-side wait.
 */
export function createChatHistory(options: AntflyChatHistoryOptions) {
  const prefix = options.keyPrefix ?? "chat_history/";
  const keyFor = (chatId: string) => `${prefix}${chatId}`;

  async function save(chatId: string, messages: UIMessage[]): Promise<void> {
    const row: ChatHistoryRow = { messages, updatedAt: new Date().toISOString() };
    const result = await options.client.tables.batchWithOptions(options.table, {
      inserts: { [keyFor(chatId)]: row },
      sync_level: "full_index",
    });
    if (result.status !== "committed") {
      throw new Error(`Antfly chat history save did not commit: ${JSON.stringify(result)}`);
    }
  }

  async function load(chatId: string): Promise<UIMessage[]> {
    try {
      const data = (await options.client.tables.lookup(options.table, keyFor(chatId))) as
        | ChatHistoryRow
        | undefined;
      return data?.messages ?? [];
    } catch (error) {
      if (error instanceof AntflyHTTPError && error.status === 404) return [];
      throw error;
    }
  }

  /**
   * Builds an `onFinish` callback for `toUIMessageStreamResponse({ onFinish })`,
   * bound to one chat ID.
   */
  function onFinish(chatId: string) {
    return async ({ messages }: { messages: UIMessage[] }) => {
      await save(chatId, messages);
    };
  }

  return { save, load, onFinish };
}
