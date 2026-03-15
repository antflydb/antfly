# Answer Feedback

Collect user feedback on AI-generated answers using the `AnswerFeedback` component.

## Basic Usage

The `AnswerFeedback` component is designed to be used as a child of `RAGResults`. It automatically accesses the query, summary, and search results context from its parent.

```jsx
import {
  Antfly,
  AnswerBox,
  RAGResults,
  AnswerFeedback,
  renderThumbsUpDown
} from '@antfly/components';

const summarizer = {
  provider: "ollama",
  model: "gemma3:4b"
};

const MyApp = () => (
  <Antfly url="http://localhost:8082/api/v1" table="docs">
    <AnswerBox id="question" fields={["content"]} placeholder="Ask a question..." />

    <RAGResults id="answer" answerBoxId="question" summarizer={summarizer}>
      <AnswerFeedback
        scale={1}
        renderRating={renderThumbsUpDown}
        onFeedback={({ feedback, result, query }) => {
          // Store feedback in your database
          console.log('Rating:', feedback.rating);
          console.log('Query:', query);
          console.log('Summary:', result.summary_result.summary);
        }}
      />
    </RAGResults>
  </Antfly>
);
```

## Rating Types

### Thumbs Up/Down (Binary)

Simple positive/negative feedback:

```jsx
import { renderThumbsUpDown } from '@antfly/components';

<AnswerFeedback
  scale={1}
  renderRating={renderThumbsUpDown}
  onFeedback={handleFeedback}
/>
```

### Star Rating (5 Stars)

Standard 5-star rating using scale 0-4:

```jsx
import { renderStars } from '@antfly/components';

<AnswerFeedback
  scale={4}
  renderRating={renderStars}
  onFeedback={handleFeedback}
/>
```

### Numeric Scale

Numeric scale for any range (commonly 0-3 for LLM evaluation):

```jsx
import { renderNumeric } from '@antfly/components';

<AnswerFeedback
  scale={3}
  renderRating={(rating, onRate) => renderNumeric(rating, onRate, 3)}
  onFeedback={handleFeedback}
/>
```

## Custom Renderer

Create completely custom feedback UI with the render prop pattern:

```jsx
const customRender = (currentRating, onRate) => {
  const options = [
    { emoji: "😞", label: "Poor", value: 0 },
    { emoji: "😐", label: "Fair", value: 1 },
    { emoji: "🙂", label: "Good", value: 2 },
    { emoji: "😊", label: "Great", value: 3 },
    { emoji: "🤩", label: "Excellent", value: 4 }
  ];

  return (
    <div className="my-feedback-ui">
      {options.map(option => (
        <button
          key={option.value}
          onClick={() => onRate(option.value)}
          className={currentRating === option.value ? 'active' : ''}
        >
          <span className="emoji">{option.emoji}</span>
          <span className="label">{option.label}</span>
        </button>
      ))}
    </div>
  );
};

<AnswerFeedback
  scale={4}
  renderRating={customRender}
  onFeedback={handleFeedback}
/>
```

## Props

| Prop | Type | Default | Description |
|------|------|---------|-------------|
| `scale` | `number` | Required | Maximum rating value. Rating range is 0 to scale (e.g., scale=1 for binary 0/1, scale=4 for 5-star 0-4) |
| `renderRating` | `function` | undefined | Render function: `(currentRating: number \| null, onRate: (rating: number) => void) => ReactNode` |
| `onFeedback` | `function` | Required | Callback invoked on submit: `({ feedback, result, query }) => void` |
| `enableComments` | `boolean` | `true` | Show optional comment field after rating selection |
| `commentPlaceholder` | `string` | `"Add a comment (optional)"` | Placeholder text for comment textarea |
| `submitLabel` | `string` | `"Submit Feedback"` | Text for submit button |

## Feedback Data Structure

The `onFeedback` callback receives an object with complete context:

```typescript
{
  feedback: {
    rating: number,      // User's rating (0 to scale)
    scale: number,       // Maximum value (same as scale prop)
    comment?: string     // Optional comment text (if enableComments=true)
  },
  result: {
    summary_result: {
      summary: string    // The AI-generated summary
    },
    query_results: [{
      hits: {
        hits: QueryHit[] // Search results that informed the answer
      }
    }]
  },
  query: string          // Original user question
}
```

### Example Callback

```jsx
const handleFeedback = ({ feedback, result, query }) => {
  // Send to your analytics/feedback API
  fetch('/api/feedback', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      rating: feedback.rating,
      scale: feedback.scale,
      percentage: (feedback.rating / feedback.scale) * 100, // Normalize to percentage
      comment: feedback.comment,
      query: query,
      summary: result.summary_result.summary,
      num_sources: result.query_results[0].hits.hits.length,
      timestamp: new Date().toISOString()
    })
  });

  // Show confirmation to user
  alert('Thank you for your feedback!');
};
```

## Options

### Without Comments

Disable the comment field for quick feedback:

```jsx
<AnswerFeedback
  scale={4}
  renderRating={renderStars}
  enableComments={false}
  onFeedback={handleFeedback}
/>
```

### Custom Labels

Customize placeholder and button text:

```jsx
<AnswerFeedback
  scale={1}
  renderRating={renderThumbsUpDown}
  commentPlaceholder="Tell us what could be improved..."
  submitLabel="Send Feedback"
  onFeedback={handleFeedback}
/>
```

## Styling

The component provides CSS classes for custom styling. By design, the library is unstyled, so you control the appearance:

### CSS Classes

- `.react-af-answer-feedback` - Main container
- `.react-af-feedback-rating` - Rating UI wrapper
- `.react-af-feedback-thumbs` - Thumbs up/down container
- `.react-af-feedback-thumb-up` - Thumbs up button
- `.react-af-feedback-thumb-down` - Thumbs down button
- `.react-af-feedback-stars` - Stars container
- `.react-af-feedback-star` - Individual star button
- `.react-af-feedback-numeric` - Numeric scale container
- `.react-af-feedback-number` - Individual number button
- `.react-af-feedback-comment` - Comment field wrapper
- `.react-af-feedback-comment-input` - Textarea element
- `.react-af-feedback-actions` - Submit button wrapper
- `.react-af-feedback-submit` - Submit button

### Example CSS

```css
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

.react-af-feedback-comment-input {
  width: 100%;
  padding: 12px;
  border: 2px solid #e0e0e0;
  border-radius: 8px;
  font-family: inherit;
  font-size: 14px;
  resize: vertical;
  margin-top: 15px;
}

.react-af-feedback-submit {
  padding: 10px 20px;
  background: #4a90e2;
  color: white;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  cursor: pointer;
  margin-top: 15px;
}

.react-af-feedback-submit:hover {
  background: #357abd;
}
```

## Behavior

### Visibility

The feedback component automatically:
- Hides while the answer is still streaming
- Hides if there's no result yet
- Hides after the user submits feedback

### Comment Field

When `enableComments={true}` (default):
- Comment field appears after user selects a rating
- User can optionally add text feedback
- Comment is included in the `onFeedback` callback if provided

### Validation

The component validates:
- Rating must be within 0 to scale range
- Cannot submit without selecting a rating
- Must be used within a `RAGResults` component (throws error otherwise)

## Examples

See the Storybook for interactive examples:

```bash
npm run storybook
```

Then navigate to "Answer Feedback" in the sidebar to see:
- Thumbs up/down example
- Star rating example
- Numeric scale example
- Custom renderer example
- Without comments example
