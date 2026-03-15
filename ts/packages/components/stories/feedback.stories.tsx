import type { RetrievalAgentResult } from "@antfly/sdk";
import {
  AnswerFeedback,
  AnswerResults,
  Antfly,
  type FeedbackResult,
  type GeneratorConfig,
  QueryBox,
  renderNumeric,
  renderStars,
  renderThumbsUpDown,
} from "../src";
import { tableName, url } from "./utils";

export default {
  title: "Answer Feedback",
  component: AnswerFeedback,
};

// Mock summarizer configuration
const mockSummarizer: GeneratorConfig = {
  provider: "ollama",
  model: "gemma3:4b",
};

export const ThumbsUpDown = () => {
  const handleFeedback = (data: {
    feedback: FeedbackResult;
    result: RetrievalAgentResult;
    query: string;
  }) => {
    console.log("Feedback received:", data);
    alert(
      `Thank you for your feedback!\n\n` +
        `Rating: ${data.feedback.rating === 1 ? "👍 Thumbs Up" : "👎 Thumbs Down"}\n` +
        `Query: "${data.query}"\n` +
        `Comment: ${data.feedback.comment || "(none)"}`
    );
  };

  return (
    <Antfly url={url} table={tableName}>
      <h1>Thumbs Up/Down Feedback</h1>
      <p>Simple binary feedback with thumbs up or thumbs down.</p>
      <style>{`
        .feedback-demo {
          max-width: 800px;
          margin: 0 auto;
        }
        .feedback-demo input {
          width: 100%;
          padding: 12px;
          border: 2px solid #4a90e2;
          border-radius: 8px;
          font-size: 16px;
          margin-bottom: 10px;
        }
        .feedback-demo button[type="submit"] {
          padding: 12px 24px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 8px;
          font-size: 16px;
          cursor: pointer;
        }
        .react-af-rag-summary {
          padding: 20px;
          background: #f8f9fa;
          border-radius: 8px;
          margin: 20px 0;
          line-height: 1.6;
        }
        .react-af-answer-feedback {
          margin-top: 20px;
          padding: 20px;
          background: white;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
        }
        .react-af-feedback-thumbs {
          display: flex;
          gap: 10px;
          justify-content: center;
        }
        .react-af-feedback-thumbs button {
          padding: 12px 24px;
          font-size: 32px;
          background: white;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
          cursor: pointer;
          transition: all 0.2s;
        }
        .react-af-feedback-thumbs button:hover {
          transform: scale(1.1);
          border-color: #4a90e2;
        }
        .react-af-feedback-thumbs button.active {
          background: #4a90e2;
          border-color: #4a90e2;
          transform: scale(1.1);
        }
        .react-af-feedback-comment {
          margin-top: 15px;
        }
        .react-af-feedback-comment-input {
          width: 100%;
          padding: 12px;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
          font-family: inherit;
          font-size: 14px;
          resize: vertical;
        }
        .react-af-feedback-actions {
          margin-top: 15px;
          text-align: right;
        }
        .react-af-feedback-submit {
          padding: 10px 20px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 6px;
          font-size: 14px;
          cursor: pointer;
        }
        .react-af-feedback-submit:hover {
          background: #357abd;
        }
      `}</style>

      <div className="feedback-demo">
        <pre
          style={{ background: "#f5f5f5", padding: "16px", borderRadius: "8px" }}
        >{`<AnswerResults id="answer" searchBoxId="question" generator={generator}>
  <AnswerFeedback
    scale={1}
    renderRating={renderThumbsUpDown}
    onFeedback={handleFeedback}
  />
</AnswerResults>`}</pre>

        <QueryBox id="question" placeholder="Ask a question..." />

        <AnswerResults id="answer" searchBoxId="question" generator={mockSummarizer}>
          <AnswerFeedback scale={1} renderRating={renderThumbsUpDown} onFeedback={handleFeedback} />
        </AnswerResults>
      </div>
    </Antfly>
  );
};

export const StarRating = () => {
  const handleFeedback = (data: {
    feedback: FeedbackResult;
    result: RetrievalAgentResult;
    query: string;
  }) => {
    console.log("Feedback received:", data);
    alert(
      `Thank you for your ${data.feedback.rating + 1}-star rating!\n\n` +
        `Query: "${data.query}"\n` +
        `Comment: ${data.feedback.comment || "(none)"}`
    );
  };

  return (
    <Antfly url={url} table={tableName}>
      <h1>Star Rating Feedback</h1>
      <p>Five-star rating system (0-4 scale = 1-5 stars displayed).</p>
      <style>{`
        .feedback-demo {
          max-width: 800px;
          margin: 0 auto;
        }
        .feedback-demo input {
          width: 100%;
          padding: 12px;
          border: 2px solid #4a90e2;
          border-radius: 8px;
          font-size: 16px;
          margin-bottom: 10px;
        }
        .feedback-demo button[type="submit"] {
          padding: 12px 24px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 8px;
          font-size: 16px;
          cursor: pointer;
        }
        .react-af-rag-summary {
          padding: 20px;
          background: #f8f9fa;
          border-radius: 8px;
          margin: 20px 0;
          line-height: 1.6;
        }
        .react-af-answer-feedback {
          margin-top: 20px;
          padding: 20px;
          background: white;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
        }
        .react-af-feedback-stars {
          display: flex;
          gap: 8px;
          justify-content: center;
        }
        .react-af-feedback-star {
          padding: 8px 12px;
          font-size: 36px;
          background: transparent;
          border: none;
          cursor: pointer;
          transition: transform 0.2s;
        }
        .react-af-feedback-star:hover {
          transform: scale(1.2);
        }
        .react-af-feedback-comment {
          margin-top: 15px;
        }
        .react-af-feedback-comment-input {
          width: 100%;
          padding: 12px;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
          font-family: inherit;
          font-size: 14px;
          resize: vertical;
        }
        .react-af-feedback-actions {
          margin-top: 15px;
          text-align: right;
        }
        .react-af-feedback-submit {
          padding: 10px 20px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 6px;
          font-size: 14px;
          cursor: pointer;
        }
        .react-af-feedback-submit:hover {
          background: #357abd;
        }
      `}</style>

      <div className="feedback-demo">
        <pre
          style={{ background: "#f5f5f5", padding: "16px", borderRadius: "8px" }}
        >{`<AnswerResults id="answer" searchBoxId="question" generator={generator}>
  <AnswerFeedback
    scale={4}
    renderRating={renderStars}
    onFeedback={handleFeedback}
  />
</AnswerResults>`}</pre>

        <QueryBox id="question" placeholder="Ask a question..." />

        <AnswerResults id="answer" searchBoxId="question" generator={mockSummarizer}>
          <AnswerFeedback scale={4} renderRating={renderStars} onFeedback={handleFeedback} />
        </AnswerResults>
      </div>
    </Antfly>
  );
};

export const NumericScale = () => {
  const handleFeedback = (data: {
    feedback: FeedbackResult;
    result: RetrievalAgentResult;
    query: string;
  }) => {
    console.log("Feedback received:", data);
    alert(
      `Thank you for rating ${data.feedback.rating}/${data.feedback.scale}!\n\n` +
        `Query: "${data.query}"\n` +
        `Comment: ${data.feedback.comment || "(none)"}`
    );
  };

  return (
    <Antfly url={url} table={tableName}>
      <h1>Numeric Scale Feedback</h1>
      <p>Four-point scale (0-3) commonly used for LLM evaluation.</p>
      <style>{`
        .feedback-demo {
          max-width: 800px;
          margin: 0 auto;
        }
        .feedback-demo input {
          width: 100%;
          padding: 12px;
          border: 2px solid #4a90e2;
          border-radius: 8px;
          font-size: 16px;
          margin-bottom: 10px;
        }
        .feedback-demo button[type="submit"] {
          padding: 12px 24px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 8px;
          font-size: 16px;
          cursor: pointer;
        }
        .react-af-rag-summary {
          padding: 20px;
          background: #f8f9fa;
          border-radius: 8px;
          margin: 20px 0;
          line-height: 1.6;
        }
        .react-af-answer-feedback {
          margin-top: 20px;
          padding: 20px;
          background: white;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
        }
        .react-af-feedback-numeric {
          display: flex;
          gap: 10px;
          justify-content: center;
        }
        .react-af-feedback-number {
          width: 60px;
          height: 60px;
          font-size: 24px;
          font-weight: bold;
          background: white;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
          cursor: pointer;
          transition: all 0.2s;
        }
        .react-af-feedback-number:hover {
          border-color: #4a90e2;
          background: #f0f8ff;
        }
        .react-af-feedback-number.active {
          background: #4a90e2;
          color: white;
          border-color: #4a90e2;
        }
        .react-af-feedback-comment {
          margin-top: 15px;
        }
        .react-af-feedback-comment-input {
          width: 100%;
          padding: 12px;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
          font-family: inherit;
          font-size: 14px;
          resize: vertical;
        }
        .react-af-feedback-actions {
          margin-top: 15px;
          text-align: right;
        }
        .react-af-feedback-submit {
          padding: 10px 20px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 6px;
          font-size: 14px;
          cursor: pointer;
        }
        .react-af-feedback-submit:hover {
          background: #357abd;
        }
      `}</style>

      <div className="feedback-demo">
        <pre
          style={{ background: "#f5f5f5", padding: "16px", borderRadius: "8px" }}
        >{`<AnswerResults id="answer" searchBoxId="question" generator={generator}>
  <AnswerFeedback
    scale={3}
    renderRating={(rating, onRate) => renderNumeric(rating, onRate, 3)}
    onFeedback={handleFeedback}
  />
</AnswerResults>`}</pre>

        <QueryBox id="question" placeholder="Ask a question..." />

        <AnswerResults id="answer" searchBoxId="question" generator={mockSummarizer}>
          <AnswerFeedback
            scale={3}
            renderRating={(rating, onRate) => renderNumeric(rating, onRate, 3)}
            onFeedback={handleFeedback}
          />
        </AnswerResults>
      </div>
    </Antfly>
  );
};

export const WithoutComments = () => {
  const handleFeedback = (data: {
    feedback: FeedbackResult;
    result: RetrievalAgentResult;
    query: string;
  }) => {
    console.log("Feedback received:", data);
    alert(`Rating: ${data.feedback.rating}/${data.feedback.scale}\n` + `Query: "${data.query}"`);
  };

  return (
    <Antfly url={url} table={tableName}>
      <h1>Feedback Without Comments</h1>
      <p>Disable the comment field for quick feedback collection.</p>
      <style>{`
        .feedback-demo {
          max-width: 800px;
          margin: 0 auto;
        }
        .feedback-demo input {
          width: 100%;
          padding: 12px;
          border: 2px solid #4a90e2;
          border-radius: 8px;
          font-size: 16px;
          margin-bottom: 10px;
        }
        .feedback-demo button[type="submit"] {
          padding: 12px 24px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 8px;
          font-size: 16px;
          cursor: pointer;
        }
        .react-af-rag-summary {
          padding: 20px;
          background: #f8f9fa;
          border-radius: 8px;
          margin: 20px 0;
          line-height: 1.6;
        }
        .react-af-answer-feedback {
          margin-top: 20px;
          padding: 20px;
          background: white;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
        }
        .react-af-feedback-stars {
          display: flex;
          gap: 8px;
          justify-content: center;
        }
        .react-af-feedback-star {
          padding: 8px 12px;
          font-size: 36px;
          background: transparent;
          border: none;
          cursor: pointer;
          transition: transform 0.2s;
        }
        .react-af-feedback-star:hover {
          transform: scale(1.2);
        }
        .react-af-feedback-actions {
          margin-top: 15px;
          text-align: center;
        }
        .react-af-feedback-submit {
          padding: 10px 20px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 6px;
          font-size: 14px;
          cursor: pointer;
        }
        .react-af-feedback-submit:hover {
          background: #357abd;
        }
      `}</style>

      <div className="feedback-demo">
        <pre
          style={{ background: "#f5f5f5", padding: "16px", borderRadius: "8px" }}
        >{`<AnswerFeedback
  scale={4}
  renderRating={renderStars}
  enableComments={false}
  onFeedback={handleFeedback}
/>`}</pre>

        <QueryBox id="question" placeholder="Ask a question..." />

        <AnswerResults id="answer" searchBoxId="question" generator={mockSummarizer}>
          <AnswerFeedback scale={4} renderRating={renderStars} onFeedback={handleFeedback} />
        </AnswerResults>
      </div>
    </Antfly>
  );
};

export const CustomRenderer = () => {
  const handleFeedback = (data: {
    feedback: FeedbackResult;
    result: RetrievalAgentResult;
    query: string;
  }) => {
    console.log("Feedback received:", data);
    const labels = ["Poor", "Fair", "Good", "Great", "Excellent"];
    alert(
      `Rating: ${labels[data.feedback.rating]} (${data.feedback.rating}/${data.feedback.scale})\n` +
        `Query: "${data.query}"\n` +
        `Comment: ${data.feedback.comment || "(none)"}`
    );
  };

  // Custom renderer with emoji and labels
  const customRender = (currentRating: number | null, onRate: (rating: number) => void) => {
    const options = [
      { emoji: "😞", label: "Poor", value: 0 },
      { emoji: "😐", label: "Fair", value: 1 },
      { emoji: "🙂", label: "Good", value: 2 },
      { emoji: "😊", label: "Great", value: 3 },
      { emoji: "🤩", label: "Excellent", value: 4 },
    ];

    return (
      <div
        style={{
          display: "flex",
          gap: "12px",
          justifyContent: "center",
          flexWrap: "wrap",
        }}
      >
        {options.map((option) => (
          <button
            type="button"
            key={option.value}
            onClick={() => onRate(option.value)}
            style={{
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              gap: "6px",
              padding: "12px 16px",
              background: currentRating === option.value ? "#4a90e2" : "white",
              color: currentRating === option.value ? "white" : "#333",
              border: `2px solid ${currentRating === option.value ? "#4a90e2" : "#e0e0e0"}`,
              borderRadius: "8px",
              cursor: "pointer",
              transition: "all 0.2s",
              fontSize: "14px",
            }}
          >
            <span style={{ fontSize: "32px" }}>{option.emoji}</span>
            <span style={{ fontWeight: 600 }}>{option.label}</span>
          </button>
        ))}
      </div>
    );
  };

  return (
    <Antfly url={url} table={tableName}>
      <h1>Custom Feedback Renderer</h1>
      <p>Create your own rating UI with emojis, labels, or any custom design.</p>
      <style>{`
        .feedback-demo {
          max-width: 900px;
          margin: 0 auto;
        }
        .feedback-demo input {
          width: 100%;
          padding: 12px;
          border: 2px solid #4a90e2;
          border-radius: 8px;
          font-size: 16px;
          margin-bottom: 10px;
        }
        .feedback-demo button[type="submit"] {
          padding: 12px 24px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 8px;
          font-size: 16px;
          cursor: pointer;
        }
        .react-af-rag-summary {
          padding: 20px;
          background: #f8f9fa;
          border-radius: 8px;
          margin: 20px 0;
          line-height: 1.6;
        }
        .react-af-answer-feedback {
          margin-top: 20px;
          padding: 20px;
          background: white;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
        }
        .react-af-feedback-comment {
          margin-top: 15px;
        }
        .react-af-feedback-comment-input {
          width: 100%;
          padding: 12px;
          border: 2px solid #e0e0e0;
          border-radius: 8px;
          font-family: inherit;
          font-size: 14px;
          resize: vertical;
        }
        .react-af-feedback-actions {
          margin-top: 15px;
          text-align: right;
        }
        .react-af-feedback-submit {
          padding: 10px 20px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 6px;
          font-size: 14px;
          cursor: pointer;
        }
        .react-af-feedback-submit:hover {
          background: #357abd;
        }
      `}</style>

      <div className="feedback-demo">
        <pre
          style={{ background: "#f5f5f5", padding: "16px", borderRadius: "8px" }}
        >{`const customRender = (currentRating, onRate) => {
  const options = [
    { emoji: "😞", label: "Poor", value: 0 },
    { emoji: "😐", label: "Fair", value: 1 },
    // ...
  ];

  return (
    <div style={{ display: "flex", gap: "12px" }}>
      {options.map(option => (
        <button onClick={() => onRate(option.value)}>
          <span>{option.emoji}</span>
          <span>{option.label}</span>
        </button>
      ))}
    </div>
  );
};

<AnswerFeedback
  scale={4}
  renderRating={customRender}
  onFeedback={handleFeedback}
/>`}</pre>

        <QueryBox id="question" placeholder="Ask a question..." />

        <AnswerResults id="answer" searchBoxId="question" generator={mockSummarizer}>
          <AnswerFeedback
            scale={4}
            renderRating={customRender}
            onFeedback={handleFeedback}
            renderComment={(comment, setComment) => (
              <textarea
                className="react-af-feedback-comment-input"
                placeholder="Tell us more about your experience..."
                value={comment}
                onChange={(e) => setComment(e.target.value)}
              />
            )}
            renderSubmit={(onSubmit) => (
              <button type="button" className="react-af-feedback-submit" onClick={onSubmit}>
                Send Feedback
              </button>
            )}
          />
        </AnswerResults>
      </div>
    </Antfly>
  );
};
