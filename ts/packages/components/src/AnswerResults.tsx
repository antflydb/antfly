import type {
  ClassificationTransformationResult,
  EvalConfig,
  EvalResult,
  EvaluatorScore,
  GenerationConfidence,
  GeneratorConfig,
  QueryHit,
  RetrievalAgentRequest,
  RetrievalAgentResult,
} from "@antfly/sdk";
import { type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AnswerResultsContext, type AnswerResultsContextValue } from "./AnswerResultsContext";
import { useSharedContext } from "./SharedContext";
import { resolveTable, streamAnswer } from "./utils";

export interface AnswerResultsProps {
  id: string;
  searchBoxId: string; // Links to the QueryBox that provides the search value
  generator?: GeneratorConfig;
  agentKnowledge?: string; // Additional context for the answer agent
  table?: string; // Optional table override - auto-inherits from QueryBox if not specified
  filterQuery?: Record<string, unknown>; // Filter query to constrain search results
  exclusionQuery?: Record<string, unknown>; // Exclusion query to exclude matches
  fields?: string[];
  semanticIndexes?: string[];

  // Eval configuration - enables inline evaluation of answer quality
  eval?: EvalConfig;

  // Visibility controls
  showClassification?: boolean;
  showReasoning?: boolean;
  showFollowUpQuestions?: boolean;
  showConfidence?: boolean;
  showHits?: boolean;

  // Custom renderers
  renderLoading?: () => ReactNode;
  renderEmpty?: () => ReactNode;
  renderClassification?: (data: ClassificationTransformationResult) => ReactNode;
  renderReasoning?: (reasoning: string, isStreaming: boolean) => ReactNode;
  renderAnswer?: (answer: string, isStreaming: boolean, hits?: QueryHit[]) => ReactNode;
  renderConfidence?: (confidence: GenerationConfidence) => ReactNode;
  renderFollowUpQuestions?: (questions: string[]) => ReactNode;
  renderHits?: (hits: QueryHit[]) => ReactNode;
  renderEvalResult?: (evalResult: EvalResult) => ReactNode;

  // Configuration overrides
  systemPrompt?: string; // → steps.generation.system_prompt
  generationContext?: string; // → steps.generation.generation_context
  limit?: number; // → queries[0].limit (default: 10)
  followUpCount?: number; // → steps.followup.count

  // Callbacks
  onStreamStart?: () => void;
  onStreamEnd?: () => void;
  onError?: (error: string) => void;

  // Detailed streaming callbacks
  onClassification?: (data: ClassificationTransformationResult) => void;
  onHit?: (hit: QueryHit) => void;
  onGenerationChunk?: (chunk: string) => void;
  onConfidence?: (data: GenerationConfidence) => void;
  onFollowup?: (question: string) => void;

  children?: ReactNode;
}

interface CustomAnswerRendererProps {
  answer: string;
  isStreaming: boolean;
  hits: QueryHit[];
  renderAnswer: NonNullable<AnswerResultsProps["renderAnswer"]>;
}

function CustomAnswerRenderer({
  answer,
  isStreaming,
  hits,
  renderAnswer,
}: CustomAnswerRendererProps) {
  return <>{renderAnswer(answer, isStreaming, hits)}</>;
}

export default function AnswerResults({
  id,
  searchBoxId,
  generator,
  agentKnowledge,
  table,
  filterQuery,
  exclusionQuery,
  fields,
  semanticIndexes,
  eval: evalConfig,
  systemPrompt,
  generationContext,
  limit,
  followUpCount,
  showClassification = false,
  showReasoning = false,
  showFollowUpQuestions = true,
  showConfidence = false,
  showHits = false,
  renderLoading,
  renderEmpty,
  renderClassification,
  renderReasoning,
  renderAnswer,
  renderConfidence,
  renderFollowUpQuestions,
  renderHits,
  renderEvalResult,
  onStreamStart,
  onStreamEnd,
  onError: onErrorCallback,
  onClassification: onClassificationCallback,
  onHit: onHitCallback,
  onGenerationChunk: onGenerationChunkCallback,
  onConfidence: onConfidenceCallback,
  onFollowup: onFollowupCallback,
  children,
}: AnswerResultsProps) {
  const [{ widgets, url, table: defaultTable, headers }, dispatch] = useSharedContext();

  // Answer agent state
  const [classification, setClassification] = useState<ClassificationTransformationResult | null>(
    null
  );
  const [hits, setHits] = useState<QueryHit[]>([]);
  const [reasoning, setReasoning] = useState("");
  const [answer, setAnswer] = useState("");
  const [confidence, setConfidence] = useState<GenerationConfidence | null>(null);
  const [followUpQuestions, setFollowUpQuestions] = useState<string[]>([]);
  const [evalResult, setEvalResult] = useState<EvalResult | null>(null);
  const [isStreaming, setIsStreaming] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const abortControllerRef = useRef<AbortController | null>(null);
  const previousSubmissionRef = useRef<number | undefined>(undefined);

  // Watch for changes in the QueryBox widget
  const searchBoxWidget = widgets.get(searchBoxId);
  const currentQuery = searchBoxWidget?.value as string | undefined;
  const submittedAt = searchBoxWidget?.submittedAt;

  // Trigger Answer Agent request when QueryBox is submitted (based on timestamp, not just query value)
  useEffect(() => {
    // Only trigger if we have a query and a submission timestamp
    if (!currentQuery || !submittedAt) {
      return;
    }

    // Check if this is a new submission (different timestamp from previous)
    if (submittedAt === previousSubmissionRef.current) {
      return;
    }

    // Validation check - don't proceed if URL is missing
    if (!url) {
      console.error("AnswerResults: Missing API URL in context");
      return;
    }

    // Cancel any previous stream
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
    }

    previousSubmissionRef.current = submittedAt;

    // Resolve table: prop > QueryBox widget > default
    const widgetTable = table || searchBoxWidget?.table;
    const resolvedTable = resolveTable(widgetTable, defaultTable);

    // Build the Retrieval Agent request with generation steps
    // QueryBox only provides the text value, AnswerResults owns the query configuration
    const retrievalRequest: RetrievalAgentRequest = {
      query: currentQuery,
      ...(generator ? { generator } : {}),
      agent_knowledge: agentKnowledge,
      stream: true,
      queries: [
        {
          table: resolvedTable,
          semantic_search: currentQuery,
          ...(semanticIndexes?.length ? { indexes: semanticIndexes } : {}),
          ...(fields?.length ? { fields: fields } : {}),
          ...(filterQuery ? { filter_query: filterQuery } : {}),
          ...(exclusionQuery ? { exclusion_query: exclusionQuery } : {}),
          limit: limit ?? 10,
        },
      ],
      steps: {
        generation: {
          enabled: true,
          ...(systemPrompt ? { system_prompt: systemPrompt } : {}),
          ...(generationContext ? { generation_context: generationContext } : {}),
        },
        classification: {
          enabled: true,
          with_reasoning: showReasoning,
        },
        followup: {
          enabled: showFollowUpQuestions,
          ...(followUpCount ? { count: followUpCount } : {}),
        },
        confidence: {
          enabled: showConfidence,
        },
        eval: evalConfig,
      },
    };

    // Start streaming
    const startStream = async () => {
      // Reset state at the start of the async operation
      setClassification(null);
      setHits([]);
      setReasoning("");
      setAnswer("");
      setConfidence(null);
      setFollowUpQuestions([]);
      setEvalResult(null);
      setError(null);
      setIsStreaming(true);

      if (onStreamStart) {
        onStreamStart();
      }

      try {
        const controller = await streamAnswer(url, retrievalRequest, headers || {}, {
          onClassification: (data) => {
            setClassification(data);
            onClassificationCallback?.(data);
          },
          onReasoning: (chunk) => {
            setReasoning((prev) => prev + chunk);
          },
          onHit: (hit) => {
            setHits((prev) => [...prev, hit]);
            onHitCallback?.(hit);
          },
          onGeneration: (chunk) => {
            setAnswer((prev) => prev + chunk);
            onGenerationChunkCallback?.(chunk);
          },
          onConfidence: (data) => {
            setConfidence(data);
            onConfidenceCallback?.(data);
          },
          onFollowup: (question) => {
            setFollowUpQuestions((prev) => [...prev, question]);
            onFollowupCallback?.(question);
          },
          onEvalResult: (data) => {
            setEvalResult(data);
          },
          onComplete: () => {
            setIsStreaming(false);
            if (onStreamEnd) {
              onStreamEnd();
            }
          },
          onError: (err) => {
            const message = err instanceof Error ? err.message : String(err);
            setError(message);
            setIsStreaming(false);
            if (onErrorCallback) {
              onErrorCallback(message);
            }
          },
          onRetrievalAgentResult: (result) => {
            // Non-streaming response
            setClassification(result.classification || null);
            setAnswer(result.generation || "");
            setFollowUpQuestions(result.followup_questions || []);
            setHits(result.hits || []);
            if (
              result.generation_confidence !== undefined &&
              result.context_relevance !== undefined
            ) {
              setConfidence({
                generation_confidence: result.generation_confidence,
                context_relevance: result.context_relevance,
              });
            }
            // Handle eval result from non-streaming response
            if (result.eval_result) {
              setEvalResult(result.eval_result);
            }
            setIsStreaming(false);
            if (onStreamEnd) {
              onStreamEnd();
            }
          },
        });

        abortControllerRef.current = controller;
      } catch (err) {
        const message = err instanceof Error ? err.message : "Unknown error";
        setError(message);
        setIsStreaming(false);
        if (onErrorCallback) {
          onErrorCallback(message);
        }
      }
    };

    startStream();

    // Cleanup on unmount or when submission changes
    return () => {
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
    };
  }, [
    submittedAt,
    currentQuery,
    searchBoxWidget,
    url,
    table,
    defaultTable,
    headers,
    generator,
    agentKnowledge,
    fields,
    semanticIndexes,
    filterQuery,
    exclusionQuery,
    evalConfig,
    systemPrompt,
    generationContext,
    limit,
    followUpCount,
    showReasoning,
    showFollowUpQuestions,
    showConfidence,
    onStreamStart,
    onStreamEnd,
    onErrorCallback,
    onClassificationCallback,
    onHitCallback,
    onGenerationChunkCallback,
    onConfidenceCallback,
    onFollowupCallback,
  ]);

  // Register this component as a widget (for consistency with other components)
  useEffect(() => {
    dispatch({
      type: "setWidget",
      key: id,
      needsQuery: false,
      needsConfiguration: false,
      isFacet: false,
      wantResults: false,
      table: table,
      value: answer,
    });
  }, [dispatch, id, table, answer]);

  // Cleanup on unmount
  useEffect(
    () => () => {
      dispatch({ type: "deleteWidget", key: id });
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
    },
    [dispatch, id]
  );

  // Default renderers
  const defaultRenderClassification = useCallback(
    (data: ClassificationTransformationResult) => (
      <div className="react-af-answer-classification">
        <strong>Classification:</strong> {data.route_type} (confidence:{" "}
        {(data.confidence * 100).toFixed(1)}%)
        <div>
          <strong>Strategy:</strong> {data.strategy}, <strong>Semantic Mode:</strong>{" "}
          {data.semantic_mode}
        </div>
        <div>
          <strong>Improved Query:</strong> {data.improved_query}
        </div>
        <div>
          <strong>Semantic Query:</strong> {data.semantic_query}
        </div>
        {data.reasoning && (
          <div>
            <strong>Reasoning:</strong> {data.reasoning}
          </div>
        )}
      </div>
    ),
    []
  );

  const defaultRenderReasoning = useCallback(
    (reasoningText: string, streaming: boolean) => (
      <div className="react-af-answer-reasoning">
        <strong>Reasoning:</strong>
        <p>
          {reasoningText}
          {streaming && <span className="react-af-answer-streaming"> ...</span>}
        </p>
      </div>
    ),
    []
  );

  const defaultRenderAnswer = useCallback(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    (answerText: string, streaming: boolean, _hits?: QueryHit[]) => (
      <div className="react-af-answer-text">
        {answerText}
        {streaming && <span className="react-af-answer-streaming"> ...</span>}
      </div>
    ),
    []
  );

  const defaultRenderConfidence = useCallback(
    (confidenceData: GenerationConfidence) => (
      <div className="react-af-answer-confidence">
        <strong>Confidence Assessment:</strong>
        <div>
          <strong>Generation Confidence:</strong>{" "}
          {(confidenceData.generation_confidence * 100).toFixed(1)}%
        </div>
        <div>
          <strong>Context Relevance:</strong> {(confidenceData.context_relevance * 100).toFixed(1)}%
        </div>
      </div>
    ),
    []
  );

  const defaultRenderFollowUpQuestions = useCallback(
    (questions: string[]) => (
      <div className="react-af-answer-follow-up">
        <strong>Follow-up Questions:</strong>
        <ul>
          {questions.map((q) => (
            <li key={q}>{q}</li>
          ))}
        </ul>
      </div>
    ),
    []
  );

  const defaultRenderHits = useCallback(
    (hitList: QueryHit[]) => (
      <details className="react-af-answer-hits">
        <summary>Search Results ({hitList.length})</summary>
        <ul>
          {hitList.map((hit, idx) => (
            <li key={hit._id || idx}>
              <strong>Score:</strong> {hit._score.toFixed(3)}
              <pre>{JSON.stringify(hit._source, null, 2)}</pre>
            </li>
          ))}
        </ul>
      </details>
    ),
    []
  );

  const defaultRenderEvalResult = useCallback((result: EvalResult) => {
    const summary = result.summary;
    const scores = result.scores;

    const renderScoreEntry = ([name, score]: [string, EvaluatorScore]) => (
      <li key={name}>
        {name}: {((score.score ?? 0) * 100).toFixed(1)}% {score.pass ? "✓" : "✗"}
        {score.reason && <span className="eval-reason"> - {score.reason}</span>}
      </li>
    );

    return (
      <details className="react-af-answer-eval">
        <summary>
          Evaluation Results
          {summary && (
            <span>
              {" "}
              - {summary.passed}/{summary.total} passed (avg:{" "}
              {((summary.average_score ?? 0) * 100).toFixed(0)}%)
            </span>
          )}
        </summary>
        <div className="react-af-answer-eval-content">
          {scores?.retrieval && Object.keys(scores.retrieval).length > 0 && (
            <div className="react-af-answer-eval-category">
              <strong>Retrieval Metrics:</strong>
              <ul>
                {(Object.entries(scores.retrieval) as [string, EvaluatorScore][]).map(
                  renderScoreEntry
                )}
              </ul>
            </div>
          )}
          {scores?.generation && Object.keys(scores.generation).length > 0 && (
            <div className="react-af-answer-eval-category">
              <strong>Generation Metrics:</strong>
              <ul>
                {(Object.entries(scores.generation) as [string, EvaluatorScore][]).map(
                  renderScoreEntry
                )}
              </ul>
            </div>
          )}
          {result.duration_ms !== undefined && (
            <div className="react-af-answer-eval-duration">
              <small>Evaluation took {result.duration_ms}ms</small>
            </div>
          )}
        </div>
      </details>
    );
  }, []);

  // Build context value for child components (e.g., AnswerFeedback)
  const contextValue = useMemo<AnswerResultsContextValue>(() => {
    const result: RetrievalAgentResult | null = answer
      ? ({
          generation: answer,
          hits,
          followup_questions: followUpQuestions,
          status: "completed",
        } as RetrievalAgentResult)
      : null;

    return {
      query: currentQuery || "",
      agentKnowledge,
      classification,
      hits,
      reasoning,
      answer,
      followUpQuestions,
      isStreaming,
      result,
      confidence,
      evalResult,
    };
  }, [
    currentQuery,
    agentKnowledge,
    classification,
    hits,
    reasoning,
    answer,
    followUpQuestions,
    isStreaming,
    confidence,
    evalResult,
  ]);

  return (
    <AnswerResultsContext.Provider value={contextValue}>
      <div className="react-af-answer-results">
        {error && (
          <div className="react-af-answer-error" style={{ color: "red" }}>
            Error: {error}
          </div>
        )}
        {!error &&
          !answer &&
          !reasoning &&
          isStreaming &&
          (renderLoading ? (
            renderLoading()
          ) : (
            <div className="react-af-answer-loading">Loading answer...</div>
          ))}
        {!error &&
          !answer &&
          !isStreaming &&
          (renderEmpty ? (
            renderEmpty()
          ) : (
            <div className="react-af-answer-empty">
              No results yet. Submit a question to get started.
            </div>
          ))}
        {showClassification &&
          classification &&
          (renderClassification
            ? renderClassification(classification)
            : defaultRenderClassification(classification))}
        {showReasoning &&
          reasoning &&
          (renderReasoning
            ? renderReasoning(reasoning, isStreaming)
            : defaultRenderReasoning(reasoning, isStreaming))}
        {!error &&
          answer &&
          (renderAnswer ? (
            <CustomAnswerRenderer
              answer={answer}
              isStreaming={isStreaming}
              hits={hits}
              renderAnswer={renderAnswer}
            />
          ) : (
            defaultRenderAnswer(answer, isStreaming, hits)
          ))}
        {showConfidence &&
          confidence &&
          !isStreaming &&
          (renderConfidence ? renderConfidence(confidence) : defaultRenderConfidence(confidence))}
        {showFollowUpQuestions &&
          followUpQuestions.length > 0 &&
          !isStreaming &&
          (renderFollowUpQuestions
            ? renderFollowUpQuestions(followUpQuestions)
            : defaultRenderFollowUpQuestions(followUpQuestions))}
        {showHits && hits.length > 0 && (renderHits ? renderHits(hits) : defaultRenderHits(hits))}
        {evalConfig &&
          evalResult &&
          !isStreaming &&
          (renderEvalResult ? renderEvalResult(evalResult) : defaultRenderEvalResult(evalResult))}
      </div>
      {children}
    </AnswerResultsContext.Provider>
  );
}
