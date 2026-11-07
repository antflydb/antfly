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
import type { AntflyClient, GenerateChunk, GenerateRequest, InferenceClient } from "@antfly/sdk";
import { abortable, combinedSignal } from "./abort.js";
import { resolveAuthorizedAsset } from "./asset/catalog.js";
import { type AuthorizeAsset, decodeAssetReference } from "./asset/reference.js";
import { providerInferenceError } from "./inference-error.js";
import {
  type ResolveAssetReference,
  toInferenceMessages,
  toInferenceToolChoice,
  toInferenceTools,
} from "./prompt.js";
import { toContent, toFinishReason, toUsage } from "./response.js";

export interface AntflyChatLanguageModelOptions {
  inference: InferenceClient;
  modelId: string;
  /** Provider ID reported on the model, for AI SDK logging/telemetry only. */
  providerId?: string;
  /**
   * Required to accept file parts carrying `providerReference: { antfly: <id> }`
   * (antfly#1505's typed `source` asset flow). Without it, such a part throws.
   */
  client?: AntflyClient;
  /**
   * The app's decision on whether the current user may use a referenced asset.
   * References aren't signed and can come from a browser, so without this they
   * are refused even when `client` is set. Bind it to the current user, for
   * example by creating the model per request.
   */
  authorizeAsset?: AuthorizeAsset;
}

/**
 * `LanguageModelV4` over Antfly's `/ai/v1/generate` (aliased at
 * `/ai/v1/chat/completions`; both accept the same OpenAI-compatible
 * `GenerateRequest`/`GenerateResponse` contract). Built on
 * `@antfly/sdk`'s `InferenceClient` rather than raw `fetch`, so inference
 * errors (including gateway limits such as Local Gemma 4's one-video-per-request
 * cap) surface as AI SDK provider errors with the original
 * `InferenceAPIError`/`InferenceCapacityError` preserved as their cause.
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
  private readonly resolveSource: ResolveAssetReference | undefined;

  constructor(options: AntflyChatLanguageModelOptions) {
    this.inference = options.inference;
    this.modelId = options.modelId;
    this.provider = options.providerId ?? "antfly";
    const { client, authorizeAsset } = options;
    this.resolveSource =
      client && authorizeAsset
        ? (id: string, signal?: AbortSignal) =>
            resolveAuthorizedAsset(client, authorizeAsset, decodeAssetReference(id), signal)
        : undefined;
  }

  async doGenerate(options: LanguageModelV4CallOptions): Promise<LanguageModelV4GenerateResult> {
    const warnings: SharedV4Warning[] = [];
    const request = await abortable(this.buildRequest(options, warnings), options.abortSignal);
    const response = await this.inference
      .generate(request, {
        signal: options.abortSignal,
        headers: options.headers,
      })
      .catch((error) => {
        throw providerInferenceError(error, "generate");
      });
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
    const cancellation = new AbortController();
    const signal = combinedSignal(options.abortSignal, cancellation.signal);
    const request = await abortable(
      this.buildRequest({ ...options, abortSignal: signal }, warnings),
      signal
    );
    const chunks = await this.inference
      .openGenerateStream(request, { signal, headers: options.headers })
      .catch((error) => {
        throw providerInferenceError(error, "generate");
      });
    const parts = this.streamParts(chunks, warnings);
    let cancelled = false;
    return {
      stream: new ReadableStream<LanguageModelV4StreamPart>({
        async pull(controller) {
          try {
            const next = await parts.next();
            // cancel() may run while next() is waiting for a network read.
            if (cancelled) return;
            if (next.done) controller.close();
            else controller.enqueue(next.value);
          } catch (error) {
            if (!cancelled) controller.error(error);
          }
        },
        cancel(reason) {
          cancelled = true;
          cancellation.abort(reason);
          // Abort interrupts a pending read. Do not queue cancellation behind
          // an uncooperative iterator; observe its eventual cleanup instead.
          void parts.return?.(undefined).catch(() => {});
        },
      }),
    };
  }

  private async buildRequest(
    options: LanguageModelV4CallOptions,
    warnings: SharedV4Warning[]
  ): Promise<Omit<GenerateRequest, "stream">> {
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
    const resolver = this.resolveSource;
    return {
      model: this.modelId,
      messages: await toInferenceMessages(
        options.prompt,
        warnings,
        resolver ? (id) => resolver(id, options.abortSignal) : undefined
      ),
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
    chunks: ReadableStream<GenerateChunk>,
    warnings: SharedV4Warning[]
  ): AsyncGenerator<LanguageModelV4StreamPart> {
    yield { type: "stream-start", warnings };
    let sentResponseMetadata = false;
    let textOpen = false;
    let reasoningOpen = false;
    const textId = "text-0";
    const reasoningId = "reasoning-0";
    const toolCalls = new Map<
      number,
      { id?: string; name: string; args: string; started: boolean }
    >();
    let finishReason: LanguageModelV4FinishReason = { unified: "other", raw: undefined };
    let usage: LanguageModelV4Usage = toUsage(undefined);
    const reader = chunks.getReader();
    try {
      while (true) {
        const next = await reader.read();
        if (next.done) break;
        const chunk = next.value;
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
            state = { name: "", args: "", started: false };
            toolCalls.set(index, state);
          }
          if (toolDelta.id && !state.started) state.id = toolDelta.id;
          if (toolDelta.function?.name) state.name += toolDelta.function.name;
          const args = toolDelta.function?.arguments ?? "";
          state.args += args;
          // Names may span deltas. Arguments mark the boundary; buffer any
          // early arguments until the provider has supplied both ID and name.
          if (!state.started && state.id && state.name && state.args.length > 0) {
            yield { type: "tool-input-start", id: state.id, toolName: state.name };
            yield { type: "tool-input-delta", id: state.id, delta: state.args };
            state.started = true;
          } else if (state.started && state.id && args) {
            yield { type: "tool-input-delta", id: state.id, delta: args };
          }
        }
        if (choice.finish_reason) finishReason = toFinishReason(choice.finish_reason);
      }
    } catch (error) {
      yield { type: "error", error: providerInferenceError(error, "generate") };
      return;
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
    if (reasoningOpen) yield { type: "reasoning-end", id: reasoningId };
    if (textOpen) yield { type: "text-end", id: textId };
    for (const [index, state] of toolCalls) {
      if (!state.name) throw new Error("Antfly tool-call stream ended without a function name");
      const id = state.id ?? `call_${index}`;
      if (!state.started) {
        yield { type: "tool-input-start", id, toolName: state.name };
        if (state.args) yield { type: "tool-input-delta", id, delta: state.args };
      }
      yield { type: "tool-input-end", id };
      yield { type: "tool-call", toolCallId: id, toolName: state.name, input: state.args };
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
