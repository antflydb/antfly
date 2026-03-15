/**
 * Default renderer for thumbs up/down feedback (scale=1)
 * @param currentRating - The currently selected rating (0 or 1), or null if not rated
 * @param onRate - Callback to set the rating
 */
export function renderThumbsUpDown(currentRating: number | null, onRate: (rating: number) => void) {
  return (
    <div className="react-af-feedback-thumbs">
      <button
        type="button"
        className={`react-af-feedback-thumb-down ${currentRating === 0 ? "active" : ""}`}
        onClick={() => onRate(0)}
        aria-label="Thumbs down"
      >
        👎
      </button>
      <button
        type="button"
        className={`react-af-feedback-thumb-up ${currentRating === 1 ? "active" : ""}`}
        onClick={() => onRate(1)}
        aria-label="Thumbs up"
      >
        👍
      </button>
    </div>
  );
}

/**
 * Default renderer for star rating (scale=4 for 5 stars: 0-4)
 * @param currentRating - The currently selected rating (0-4), or null if not rated
 * @param onRate - Callback to set the rating
 */
export function renderStars(currentRating: number | null, onRate: (rating: number) => void) {
  const stars = [0, 1, 2, 3, 4];

  return (
    <div className="react-af-feedback-stars">
      {stars.map((starValue) => (
        <button
          key={starValue}
          type="button"
          className={`react-af-feedback-star ${currentRating !== null && starValue <= currentRating ? "active" : ""}`}
          onClick={() => onRate(starValue)}
          aria-label={`${starValue + 1} star${starValue === 0 ? "" : "s"}`}
        >
          {currentRating !== null && starValue <= currentRating ? "⭐" : "☆"}
        </button>
      ))}
    </div>
  );
}

/**
 * Default renderer for numeric scale (works with any scale value)
 * @param currentRating - The currently selected rating (0-scale), or null if not rated
 * @param onRate - Callback to set the rating
 * @param scale - Maximum value of the scale (e.g., 4 for 0-4)
 */
export function renderNumeric(
  currentRating: number | null,
  onRate: (rating: number) => void,
  scale: number
) {
  const values = Array.from({ length: scale + 1 }, (_, i) => i);

  return (
    <div className="react-af-feedback-numeric">
      {values.map((value) => (
        <button
          key={value}
          type="button"
          className={`react-af-feedback-number ${currentRating === value ? "active" : ""}`}
          onClick={() => onRate(value)}
          aria-label={`Rating ${value}`}
        >
          {value}
        </button>
      ))}
    </div>
  );
}
