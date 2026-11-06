import type {
  LanguageModelV4,
  LanguageModelV4CallOptions,
  LanguageModelV4FinishReason,
  LanguageModelV4GenerateResult,
  LanguageModelV4StreamPart,
  LanguageModelV4StreamResult,
  LanguageModelV4Usage,
  SharedV4Warning,
} from "@ai-sdk/provider";
import type { GenerateRequest, InferenceClient } from "@antfly/sdk";
import { toInferenceMessages, toInferenceToolChoice, toInferenceTools } from "./prompt.js";
import { toContent, toFinishReason, toUsage } from "./response.js";

export interface AntflyChatLanguageModelOptions {
  inference: InferenceClient;
  modelId: string;
  /** Provider ID reported on the model, for AI SDK logging/telemetry only. */
  providerId?: string;
}

/**
 * `LanguageModelV4` over Antfly's `/ai/v1/generate` (aliased at
 * `/ai/v1/chat/completions`; both accept the same OpenAI-compatible
 * `GenerateRequest`/`GenerateResponse` contract). Built on
 * `@antfly/sdk`'s `InferenceClient` rather than raw `fetch`, so inference
 * errors (including gateway limits such as Local Gemma 4's one-video-per-request
 * cap) surface as `InferenceAPIError`/`InferenceCapacityError` instead of being
 * swallowed.
 *
 * Known gaps, honestly documented rather than silently ignored:
 * - `stopSequences` and `seed` are not supported by `InferenceGenerateRequest`
 *   and are reported as warnings rather than applied.
 * - `supportedUrls` is empty: the server's handling of non-`data:` URLs is
 *   route-dependent and still being qualified (antfly#1505), so this adapter
 *   never passes a remote URL through for non-image media and instead asks
 *   the caller for inline bytes.
 */
export class AntflyChatLanguageModel implements LanguageModelV4 {
  readonly specificationVersion = "v4" as const;
  readonly provider: string;
  readonly modelId: string;
  readonly supportedUrls: Record<string, RegExp[]> = {};

  private readonly inference: InferenceClient;

  constructor(options: AntflyChatLanguageModelOptions) {
    this.inference = options.inference;
    this.modelId = options.modelId;
    this.provider = options.providerId ?? "antfly";
  }

  async doGenerate(options: LanguageModelV4CallOptions): Promise<LanguageModelV4GenerateResult> {
    const warnings: SharedV4Warning[] = [];
    const request = this.buildRequest(options, warnings);
    const response = await this.inference.generate(request, { signal: options.abortSignal });
    return {
      content: toContent(response),
      finishReason: toFinishReason(response.choices[0]?.finish_reason),
      usage: toUsage(response.usage),
      warnings,
      response: {
        id: response.id,
        modelId: response.model,
        timestamp: new Date(response.created * 1000),
      },
    };
  }

  async doStream(options: LanguageModelV4CallOptions): Promise<LanguageModelV4StreamResult> {
    const warnings: SharedV4Warning[] = [];
    const request = this.buildRequest(options, warnings);
    const parts = this.streamParts(request, warnings, options.abortSignal);
    return {
      stream: new ReadableStream<LanguageModelV4StreamPart>({
        async pull(controller) {
          const next = await parts.next();
          if (next.done) {
            controller.close();
          } else {
            controller.enqueue(next.value);
          }
        },
        async cancel() {
          await parts.return?.(undefined);
        },
      }),
    };
  }

  private buildRequest(
    options: LanguageModelV4CallOptions,
    warnings: SharedV4Warning[]
  ): Omit<GenerateRequest, "stream"> {
    if (options.stopSequences?.length) {
      warnings.push({
        type: "unsupported",
        feature: "stopSequences",
        details: "Antfly's generate endpoint has no stop-sequence parameter; ignored.",
      });
    }
    if (options.seed !== undefined) {
      warnings.push({
        type: "unsupported",
        feature: "seed",
        details: "Antfly's generate endpoint has no seed parameter; ignored.",
      });
    }
    const responseFormat = toResponseFormat(options.responseFormat, warnings);
    const tools = toInferenceTools(options.tools, warnings);
    return {
      model: this.modelId,
      messages: toInferenceMessages(options.prompt, warnings),
      max_tokens: options.maxOutputTokens,
      temperature: options.temperature,
      top_p: options.topP,
      top_k: options.topK,
      presence_penalty: options.presencePenalty,
      frequency_penalty: options.frequencyPenalty,
      ...(tools ? { tools, tool_choice: toInferenceToolChoice(options.toolChoice) } : {}),
      ...(responseFormat ? { response_format: responseFormat } : {}),
    };
  }

  private async *streamParts(
    request: Omit<GenerateRequest, "stream">,
    warnings: SharedV4Warning[],
    signal: AbortSignal | undefined
  ): AsyncGenerator<LanguageModelV4StreamPart> {
    yield { type: "stream-start", warnings };
    let sentResponseMetadata = false;
    let textOpen = false;
    let reasoningOpen = false;
    const textId = "text-0";
    const reasoningId = "reasoning-0";
    const toolCalls = new Map<
      number,
      { id: string; name: string; args: string; started: boolean }
    >();
    let finishReason: LanguageModelV4FinishReason = { unified: "other", raw: undefined };
    let usage: LanguageModelV4Usage = toUsage(undefined);
    try {
      for await (const chunk of this.inference.generateStream(request, { signal })) {
        if (!sentResponseMetadata) {
          yield {
            type: "response-metadata",
            id: chunk.id,
            modelId: chunk.model,
            timestamp: new Date(chunk.created * 1000),
          };
          sentResponseMetadata = true;
        }
        if (chunk.usage) usage = toUsage(chunk.usage);
        const choice = chunk.choices[0];
        if (!choice) continue;
        const delta = choice.delta;
        if (delta.reasoning_content) {
          if (!reasoningOpen) {
            yield { type: "reasoning-start", id: reasoningId };
            reasoningOpen = true;
          }
          yield { type: "reasoning-delta", id: reasoningId, delta: delta.reasoning_content };
        }
        if (delta.content) {
          if (!textOpen) {
            yield { type: "text-start", id: textId };
            textOpen = true;
          }
          yield { type: "text-delta", id: textId, delta: delta.content };
        }
        for (const toolDelta of delta.tool_calls ?? []) {
          const index = toolDelta.index ?? 0;
          let state = toolCalls.get(index);
          if (!state) {
            state = {
              id: toolDelta.id ?? `call_${index}`,
              name: toolDelta.function?.name ?? "",
              args: "",
              started: false,
            };
            toolCalls.set(index, state);
          }
          if (!state.started && state.name) {
            yield { type: "tool-input-start", id: state.id, toolName: state.name };
            state.started = true;
          }
          if (toolDelta.function?.arguments) {
            state.args += toolDelta.function.arguments;
            if (state.started) {
              yield { type: "tool-input-delta", id: state.id, delta: toolDelta.function.arguments };
            }
          }
        }
        if (choice.finish_reason) finishReason = toFinishReason(choice.finish_reason);
      }
    } catch (error) {
      yield { type: "error", error };
      return;
    }
    if (reasoningOpen) yield { type: "reasoning-end", id: reasoningId };
    if (textOpen) yield { type: "text-end", id: textId };
    for (const state of toolCalls.values()) {
      if (state.started) yield { type: "tool-input-end", id: state.id };
      yield { type: "tool-call", toolCallId: state.id, toolName: state.name, input: state.args };
    }
    yield { type: "finish", usage, finishReason };
  }
}

function toResponseFormat(
  responseFormat: LanguageModelV4CallOptions["responseFormat"],
  warnings: SharedV4Warning[]
): GenerateRequest["response_format"] | undefined {
  if (!responseFormat || responseFormat.type === "text") return undefined;
  if (!responseFormat.schema) return { type: "json_object" };
  if (typeof responseFormat.schema !== "object") {
    warnings.push({
      type: "unsupported",
      feature: "responseFormat",
      details: "Non-object JSON schemas are not supported; falling back to json_object.",
    });
    return { type: "json_object" };
  }
  return {
    type: "json_schema",
    json_schema: {
      name: responseFormat.name,
      strict: true,
      schema: responseFormat.schema as Record<string, unknown>,
    },
  };
}
