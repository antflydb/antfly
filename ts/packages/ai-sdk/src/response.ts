import type {
  JSONObject,
  LanguageModelV4Content,
  LanguageModelV4FinishReason,
  LanguageModelV4Usage,
} from "@ai-sdk/provider";
import type { GenerateResponse, ToolCall } from "@antfly/sdk";

/** Maps the server's OpenAI-style finish reason to the AI SDK's unified reason. */
export function toFinishReason(raw: string | undefined): LanguageModelV4FinishReason {
  const unified: LanguageModelV4FinishReason["unified"] =
    raw === "stop"
      ? "stop"
      : raw === "length"
        ? "length"
        : raw === "tool_calls" || raw === "function_call"
          ? "tool-calls"
          : raw === "content_filter"
            ? "content-filter"
            : "other";
  return { unified, raw };
}

export function toUsage(usage: GenerateResponse["usage"]): LanguageModelV4Usage {
  if (!usage) {
    return {
      inputTokens: {
        total: undefined,
        noCache: undefined,
        cacheRead: undefined,
        cacheWrite: undefined,
      },
      outputTokens: { total: undefined, text: undefined, reasoning: undefined },
    };
  }
  const cacheRead = usage.cached_prompt_tokens;
  return {
    inputTokens: {
      total: usage.prompt_tokens,
      noCache: cacheRead === undefined ? usage.prompt_tokens : usage.prompt_tokens - cacheRead,
      cacheRead,
      cacheWrite: undefined,
    },
    outputTokens: {
      total: usage.completion_tokens,
      text: usage.completion_tokens,
      reasoning: undefined,
    },
    raw: usage as unknown as JSONObject,
  };
}

function toolCallContent(call: ToolCall): LanguageModelV4Content {
  return {
    type: "tool-call",
    toolCallId: call.id,
    toolName: call.function.name,
    input: call.function.arguments,
  };
}

/** Converts one non-streaming `GenerateResponse` choice into AI SDK content parts. */
export function toContent(response: GenerateResponse): LanguageModelV4Content[] {
  const choice = response.choices[0];
  if (!choice) throw new Error("Antfly generate response contained no choices");
  const content: LanguageModelV4Content[] = [];
  if (choice.message.reasoning_content) {
    content.push({ type: "reasoning", text: choice.message.reasoning_content });
  }
  if (choice.message.content) {
    content.push({ type: "text", text: choice.message.content });
  }
  for (const call of choice.message.tool_calls ?? []) {
    content.push(toolCallContent(call));
  }
  return content;
}
