import type { RetrievalAgentResult } from "@antfly/sdk";
import { type ReactNode, useCallback, useContext, useEffect, useRef, useState } from "react";
import { AnswerResultsContext } from "./AnswerResultsContext";

export interface FeedbackResult {
  rating: number;
  scale: number;
  comment?: string;
}

export interface AnswerFeedbackProps {
  /**
   * Maximum value of the rating scale. The rating will range from 0 to scale.
   * Examples:
   * - scale=1 for thumbs up/down (0 or 1)
   * - scale=4 for 5-star rating (0-4)
   * - scale=3 for 4-point scale (0-3)
   */
  scale: number;

  /**
   * Custom render function for the rating UI.
   * If not provided, developers must supply their own UI.
   *
   * @param currentRating - Currently selected rating (0-scale) or null if not rated
   * @param onRate - Callback to set a rating value (must be 0-scale)
   * @returns ReactNode to render the rating UI
   *
   * @example Using a default renderer
   * ```tsx
   * import { renderThumbsUpDown } from '@antfly/components';
   * <AnswerFeedback scale={1} renderRating={renderThumbsUpDown} onFeedback={...} />
   * ```
   */
  renderRating?: (currentRating: number | null, onRate: (rating: number) => void) => ReactNode;

  /**
   * Custom render function for the comment input.
   * If not provided, a default textarea is rendered.
   *
   * @param comment - Current comment value
   * @param setComment - Callback to update the comment value
   * @returns ReactNode to render the comment input
   */
  renderComment?: (comment: string, setComment: (value: string) => void) => ReactNode;

  /**
   * Custom render function for the submit button.
   * If not provided, a default button is rendered.
   *
   * @param onSubmit - Callback to submit the feedback
   * @returns ReactNode to render the submit button
   */
  renderSubmit?: (onSubmit: () => void) => ReactNode;

  /**
   * Custom render function for the submitted state.
   * If not provided, a default thank you message is rendered.
   *
   * @returns ReactNode to render after submission
   */
  renderSubmitted?: () => ReactNode;

  /**
   * Callback invoked when the user submits feedback.
   * Provides the feedback result along with the complete context (RAG or Answer Agent).
   *
   * @param data.feedback - The user's feedback (rating, scale, optional comment)
   * @param data.result - The result (RAG or Answer Agent)
   * @param data.query - The original query string
   * @param data.context - Additional context from Answer Agent (classification, reasoning, agentKnowledge) if available
   */
  onFeedback: (data: {
    feedback: FeedbackResult;
    result: RetrievalAgentResult;
    query: string;
    context?: {
      classification?: { route_type: "question" | "search"; confidence: number };
      reasoning?: string;
      agentKnowledge?: string;
    };
  }) => void;
}

export default function AnswerFeedback({
  scale,
  renderRating,
  renderComment,
  renderSubmit,
  renderSubmitted,
  onFeedback,
}: AnswerFeedbackProps) {
  const answerContext = useContext(AnswerResultsContext);

  if (!answerContext) {
    throw new Error("AnswerFeedback must be used within an AnswerResults component");
  }

  const { query, isStreaming } = answerContext;
  const result = answerContext.result;

  const [rating, setRating] = useState<number | null>(null);
  const [comment, setComment] = useState("");
  const [showCommentField, setShowCommentField] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const previousQueryRef = useRef<string>(query);

  // Reset feedback state when query changes (new question submitted)
  useEffect(() => {
    if (previousQueryRef.current !== query) {
      previousQueryRef.current = query;
      // eslint-disable-next-line react-hooks/set-state-in-effect -- Resetting form state when query changes is intentional
      setRating(null);
      setComment("");
      setShowCommentField(false);
      setSubmitted(false);
    }
  }, [query]);

  // Validate and set rating
  const handleRate = useCallback(
    (value: number) => {
      if (value < 0 || value > scale) {
        console.error(`AnswerFeedback: Invalid rating ${value}. Must be between 0 and ${scale}.`);
        return;
      }
      setRating(value);
      if (renderComment) {
        setShowCommentField(true);
      }
    },
    [scale, renderComment]
  );

  // Handle feedback submission
  const handleSubmit = useCallback(() => {
    if (rating === null) {
      console.warn("AnswerFeedback: Cannot submit without a rating");
      return;
    }

    if (!result) {
      console.warn("AnswerFeedback: Cannot submit feedback without a result");
      return;
    }

    const feedbackData: FeedbackResult = {
      rating,
      scale,
      ...(comment && { comment }),
    };

    // Build context object for Answer Agent (if available)
    const contextData = answerContext
      ? {
          classification: answerContext.classification || undefined,
          reasoning: answerContext.reasoning || undefined,
          agentKnowledge: answerContext.agentKnowledge || undefined,
        }
      : undefined;

    onFeedback({
      feedback: feedbackData,
      result,
      query,
      ...(contextData && { context: contextData }),
    });

    setSubmitted(true);
  }, [rating, scale, comment, result, query, answerContext, onFeedback]);

  // Don't show feedback UI while streaming
  if (isStreaming) {
    return null;
  }

  // Show thank you message after submission
  if (submitted) {
    if (renderSubmitted) {
      return <>{renderSubmitted()}</>;
    }
    return <h4 className="react-af-answer-feedback-submitted">Thank you for your feedback!</h4>;
  }

  // Don't show feedback if there's no result yet
  if (!result) {
    return null;
  }

  // Check if answer has actual content before showing feedback
  if (!answerContext.answer || answerContext.answer.trim() === "") {
    return null;
  }

  return (
    <div className="react-af-answer-feedback">
      {/* Rating UI */}
      <div className="react-af-feedback-rating">
        {renderRating ? renderRating(rating, handleRate) : null}
      </div>

      {/* Optional comment field (shown after rating) */}
      {renderComment && showCommentField && (
        <div className="react-af-feedback-comment">{renderComment(comment, setComment)}</div>
      )}

      {/* Submit button (shown after rating) */}
      {rating !== null && (
        <div className="react-af-feedback-actions">
          {renderSubmit ? (
            renderSubmit(handleSubmit)
          ) : (
            <button type="button" className="react-af-feedback-submit" onClick={handleSubmit}>
              Submit Feedback
            </button>
          )}
        </div>
      )}
    </div>
  );
}
