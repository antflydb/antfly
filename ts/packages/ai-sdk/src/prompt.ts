import type {
  LanguageModelV4CallOptions,
  LanguageModelV4FunctionTool,
  LanguageModelV4Message,
  LanguageModelV4Prompt,
  LanguageModelV4ToolChoice,
  LanguageModelV4ToolResultOutput,
  SharedV4Warning,
} from "@ai-sdk/provider";
import { UnsupportedFunctionalityError } from "@ai-sdk/provider";
import type { ContentPart, GenerateRequest, InferenceChatMessage } from "@antfly/sdk";
import type { CatalogResource } from "./asset/catalog.js";
import { antflyAssetReferenceFrom } from "./asset/reference.js";

/** Not exported by `@antfly/sdk`; derived from the request schema it already exposes. */
type InferenceTool = NonNullable<GenerateRequest["tools"]>[number];
type InferenceToolChoice = NonNullable<GenerateRequest["tool_choice"]>;

/** Resolves an `{ antfly: <id> }` provider reference to its catalog `content.resource`. */
export type ResolveAssetReference = (id: string, signal?: AbortSignal) => Promise<CatalogResource>;

/**
 * Converts the AI SDK's provider-neutral prompt into the OpenAI-compatible
 * message array that `/ai/v1/generate` (aliased at `/ai/v1/chat/completions`)
 * expects. Antfly has no file-upload API for chat, so every file part is
 * inlined: images become `image_url` parts (URL or `data:` URI), and every
 * other media type becomes a base64 `media` part. A file part carrying
 * `providerReference: { antfly: <id> }` becomes the typed `source` content
 * part (antfly#1505's proposed asset flow): this descriptor goes to the
 * gateway, which fetches pinned bytes server-side before calling the model.
 * That requires `resolveSource`, supplied by
 * `AntflyChatLanguageModel` when it was constructed with a `client`.
 */
export async function toInferenceMessages(
  prompt: LanguageModelV4Prompt,
  warnings: SharedV4Warning[],
  resolveSource?: ResolveAssetReference
): Promise<InferenceChatMessage[]> {
  const messages = await Promise.all(
    prompt.map(async (message) => {
      // AI SDK groups parallel results; Antfly's wire format needs one message per call.
      if (message.role === "tool") {
        return message.content.flatMap((part): InferenceChatMessage[] => {
          if (part.type !== "tool-result") return [];
          return [
            { role: "tool", tool_call_id: part.toolCallId, content: toolOutputToText(part.output) },
          ];
        });
      }
      return [await toInferenceMessage(message, warnings, resolveSource)];
    })
  );
  return messages.flat();
}

async function toInferenceMessage(
  message: LanguageModelV4Message,
  warnings: SharedV4Warning[],
  resolveSource: ResolveAssetReference | undefined
): Promise<InferenceChatMessage> {
  switch (message.role) {
    case "system":
      return { role: "system", content: message.content };
    case "user":
      return {
        role: "user",
        content: await Promise.all(
          message.content.map((part) =>
            part.type === "text"
              ? ({ type: "text", text: part.text } satisfies ContentPart as ContentPart)
              : fileToContentPart(part, resolveSource)
          )
        ),
      };
    case "assistant": {
      const text: string[] = [];
      const toolCalls: NonNullable<InferenceChatMessage["tool_calls"]> = [];
      for (const part of message.content) {
        if (part.type === "text") {
          text.push(part.text);
        } else if (part.type === "tool-call") {
          if (part.providerExecuted) {
            warnings.push({
              type: "unsupported",
              feature: "providerExecuted",
              details: "Provider-executed tool calls are not supported; the call was dropped.",
            });
            continue;
          }
          toolCalls.push({
            id: part.toolCallId,
            type: "function",
            function: {
              name: part.toolName,
              arguments:
                typeof part.input === "string" ? part.input : JSON.stringify(part.input ?? {}),
            },
          });
        } else if (part.type === "reasoning" || part.type === "reasoning-file") {
          // The server has no slot for replaying prior reasoning as input; drop it.
          warnings.push({
            type: "other",
            message: `Dropped a '${part.type}' part when replaying assistant history.`,
          });
        } else if (part.type === "tool-result") {
          // Tool results belong on role:tool messages, not role:assistant; the
          // AI SDK should not produce this shape, but guard against it anyway.
          warnings.push({
            type: "other",
            message: "Dropped an assistant-role tool-result part.",
          });
        } else {
          warnings.push({
            type: "other",
            message: `Dropped an unsupported assistant content part: ${part.type}.`,
          });
        }
      }
      return {
        role: "assistant",
        ...(text.length ? { content: text.join("") } : {}),
        ...(toolCalls.length ? { tool_calls: toolCalls } : {}),
      };
    }
    default:
      throw new UnsupportedFunctionalityError({
        functionality: `message role ${(message as { role: string }).role}`,
      });
  }
}

async function fileToContentPart(
  part: Extract<LanguageModelV4Message, { role: "user" }>["content"][number] & { type: "file" },
  resolveSource: ResolveAssetReference | undefined
): Promise<ContentPart> {
  if (part.data.type === "reference") {
    const reference = antflyAssetReferenceFrom(part.data.reference);
    if (reference) {
      if (!resolveSource) {
        throw new UnsupportedFunctionalityError({
          functionality: "providerReference file parts",
          message:
            "This file part carries providerReference.antfly, but the model has no authorizeAsset. References can come from a browser, so they are only resolved with the app's own access decision. Pass authorizeAsset to the provider or model.",
        });
      }
      const resource = await resolveSource(part.data.reference.antfly as string);
      const { content_sha256: _digest, ...source } = resource;
      return { type: "source", source };
    }
  }
  const topLevel = part.mediaType.split("/")[0];
  if (topLevel === "image") {
    if (part.data.type === "url") {
      return { type: "image_url", image_url: { url: part.data.url.toString() } };
    }
    if (part.data.type === "data") {
      const base64 = typeof part.data.data === "string" ? part.data.data : toBase64(part.data.data);
      return { type: "image_url", image_url: { url: `data:${part.mediaType};base64,${base64}` } };
    }
  }
  if (part.data.type === "data") {
    const base64 = typeof part.data.data === "string" ? part.data.data : toBase64(part.data.data);
    return { type: "media", data: base64, mime_type: part.mediaType };
  }
  if (part.data.type === "url") {
    throw new UnsupportedFunctionalityError({
      functionality: "remote URL file parts for non-image media",
      message:
        "Antfly's AI SDK adapter does not fetch remote media on the client's behalf. Pass audio/video as inline bytes, or as a providerReference.antfly asset reference.",
    });
  }
  throw new UnsupportedFunctionalityError({
    functionality: `file part data variant '${part.data.type}'`,
  });
}

function toBase64(data: Uint8Array): string {
  return Buffer.from(data).toString("base64");
}

function toolOutputToText(output: LanguageModelV4ToolResultOutput): string {
  switch (output.type) {
    case "text":
    case "error-text":
      return output.value;
    case "json":
    case "error-json":
      return JSON.stringify(output.value);
    case "execution-denied":
      return JSON.stringify({ error: "execution-denied", reason: output.reason });
    case "content":
      return output.value
        .map((item) => (item.type === "text" ? item.text : `[${item.type}]`))
        .join("\n");
    default:
      return JSON.stringify(output);
  }
}

export function toInferenceTools(
  tools: LanguageModelV4CallOptions["tools"],
  warnings: SharedV4Warning[]
): InferenceTool[] | undefined {
  if (!tools?.length) return undefined;
  const converted: InferenceTool[] = [];
  for (const tool of tools) {
    if (tool.type === "provider") {
      warnings.push({
        type: "unsupported",
        feature: "tools",
        details: `Provider-defined tool '${tool.name}' is not supported and was dropped.`,
      });
      continue;
    }
    converted.push(toInferenceTool(tool));
  }
  return converted.length ? converted : undefined;
}

function toInferenceTool(tool: LanguageModelV4FunctionTool): InferenceTool {
  return {
    type: "function",
    function: {
      name: tool.name,
      description: tool.description,
      parameters: tool.inputSchema as Record<string, unknown>,
      strict: tool.strict,
    },
  };
}

export function toInferenceToolChoice(
  toolChoice: LanguageModelV4ToolChoice | undefined
): InferenceToolChoice | undefined {
  if (!toolChoice) return undefined;
  switch (toolChoice.type) {
    case "auto":
      return "auto";
    case "none":
      return "none";
    case "required":
      return "required";
    case "tool":
      return { type: "function", function: { name: toolChoice.toolName } };
    default:
      return undefined;
  }
}
