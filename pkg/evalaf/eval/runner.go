package eval

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Runner orchestrates the evaluation process.
type Runner struct {
	config      Config
	evaluators  []Evaluator
	rateLimiter *rate.Limiter
}

// NewRunner creates a new evaluation runner.
func NewRunner(config Config, evaluators []Evaluator) *Runner {
	var limiter *rate.Limiter
	if config.Execution.RateLimitPerMinute > 0 {
		// Convert requests per minute to requests per second
		rps := float64(config.Execution.RateLimitPerMinute) / 60.0
		// Allow burst of up to 5 requests or 1/4 of the rate limit, whichever is smaller
		burst := max(config.Execution.RateLimitPerMinute/4, 1)
		if burst > 5 {
			burst = 5
		}
		limiter = rate.NewLimiter(rate.Limit(rps), burst)
	}
	return &Runner{
		config:      config,
		evaluators:  evaluators,
		rateLimiter: limiter,
	}
}

// Run executes evaluation on a dataset using a target function.
// The target function produces output for each example, which is then evaluated.
func (r *Runner) RunWithTarget(ctx context.Context, dataset Dataset, target TargetFunc) (*Report, error) {
	startTime := time.Now()

	examples, err := dataset.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load dataset: %w", err)
	}

	var results []ExampleResult
	evaluatorStats := make(map[string]*EvaluatorStats)

	// Initialize evaluator stats
	for _, ev := range r.evaluators {
		evaluatorStats[ev.Name()] = &EvaluatorStats{
			Name: ev.Name(),
		}
	}

	// Determine execution strategy
	if r.config.Execution.Parallel {
		results, err = r.runParallel(ctx, examples, target, evaluatorStats)
	} else {
		results, err = r.runSequential(ctx, examples, target, evaluatorStats)
	}

	if err != nil {
		return nil, err
	}

	// Calculate summary statistics
	summary := r.calculateSummary(results, evaluatorStats, time.Since(startTime))

	return &Report{
		Summary:   summary,
		Results:   results,
		Config:    r.config,
		Timestamp: startTime,
	}, nil
}

// Run executes evaluation on a dataset where the output is already included in the examples.
// This is useful for evaluating pre-collected outputs (e.g., from production traces).
func (r *Runner) Run(ctx context.Context, dataset Dataset) (*Report, error) {
	// Create a target function that returns the output from metadata
	target := func(ctx context.Context, example Example) (any, error) {
		// Check if output is in metadata
		if example.Metadata != nil {
			if output, ok := example.Metadata["output"]; ok {
				return output, nil
			}
		}
		return nil, fmt.Errorf("no output found in example metadata")
	}

	return r.RunWithTarget(ctx, dataset, target)
}

// runSequential evaluates examples sequentially.
func (r *Runner) runSequential(ctx context.Context, examples []Example, target TargetFunc, evaluatorStats map[string]*EvaluatorStats) ([]ExampleResult, error) {
	results := make([]ExampleResult, 0, len(examples))

	for _, example := range examples {
		result, err := r.evaluateExample(ctx, example, target, evaluatorStats)
		if err != nil {
			// Record the error as a failed result instead of failing the entire run
			results = append(results, ExampleResult{
				Example:   example,
				Output:    nil,
				Results:   map[string]*EvalResult{"error": {Pass: false, Score: 0.0, Error: err}},
				Duration:  0,
				Timestamp: time.Now(),
			})
		} else {
			results = append(results, result)
		}
	}

	return results, nil
}

// runParallel evaluates examples in parallel.
func (r *Runner) runParallel(ctx context.Context, examples []Example, target TargetFunc, evaluatorStats map[string]*EvaluatorStats) ([]ExampleResult, error) {
	results := make([]ExampleResult, len(examples))

	// Create semaphore for concurrency control
	maxConcurrency := r.config.Execution.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = 10
	}
	sem := make(chan struct{}, maxConcurrency)

	var wg sync.WaitGroup
	var mu sync.Mutex

	for i, example := range examples {
		wg.Add(1)
		go func(idx int, ex Example) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			result, err := r.evaluateExample(ctx, ex, target, evaluatorStats)
			mu.Lock()
			if err != nil {
				// Record the error as a failed result instead of failing the entire run
				results[idx] = ExampleResult{
					Example:   ex,
					Output:    nil,
					Results:   map[string]*EvalResult{"error": {Pass: false, Score: 0.0, Error: err}},
					Duration:  0,
					Timestamp: time.Now(),
				}
			} else {
				results[idx] = result
			}
			mu.Unlock()
		}(i, example)
	}

	wg.Wait()

	return results, nil
}

// evaluateExample evaluates a single example.
func (r *Runner) evaluateExample(ctx context.Context, example Example, target TargetFunc, evaluatorStats map[string]*EvaluatorStats) (ExampleResult, error) {
	startTime := time.Now()

	// Get output from target function
	output, err := target(ctx, example)
	if err != nil {
		return ExampleResult{}, fmt.Errorf("target function failed: %w", err)
	}

	// Run all evaluators
	evalResults := make(map[string]*EvalResult)
	var mu sync.Mutex

	if r.config.Execution.Parallel {
		// Run evaluators in parallel
		var wg sync.WaitGroup
		for _, evaluator := range r.evaluators {
			wg.Add(1)
			go func(ev Evaluator) {
				defer wg.Done()

				// Apply rate limiting if configured
				if r.rateLimiter != nil {
					if err := r.rateLimiter.Wait(ctx); err != nil {
						mu.Lock()
						evalResults[ev.Name()] = &EvalResult{
							Pass:  false,
							Score: 0.0,
							Error: fmt.Errorf("rate limiter wait failed: %w", err),
						}
						mu.Unlock()
						return
					}
				}

				evalStart := time.Now()

				result, err := ev.Evaluate(ctx, EvalInput{
					Input:     example.Input,
					Output:    output,
					Reference: example.Reference,
					Context:   example.Context,
					Metadata:  example.Metadata,
				})

				mu.Lock()
				defer mu.Unlock()

				if err != nil {
					evalResults[ev.Name()] = &EvalResult{
						Pass:  false,
						Score: 0.0,
						Error: err,
					}
				} else {
					evalResults[ev.Name()] = result
				}

				// Update stats
				r.updateEvaluatorStats(evaluatorStats, ev.Name(), result, err, time.Since(evalStart))
			}(evaluator)
		}
		wg.Wait()
	} else {
		// Run evaluators sequentially
		for _, evaluator := range r.evaluators {
			// Apply rate limiting if configured
			if r.rateLimiter != nil {
				if err := r.rateLimiter.Wait(ctx); err != nil {
					evalResults[evaluator.Name()] = &EvalResult{
						Pass:  false,
						Score: 0.0,
						Error: fmt.Errorf("rate limiter wait failed: %w", err),
					}
					continue
				}
			}

			evalStart := time.Now()

			result, err := evaluator.Evaluate(ctx, EvalInput{
				Input:     example.Input,
				Output:    output,
				Reference: example.Reference,
				Context:   example.Context,
				Metadata:  example.Metadata,
			})

			if err != nil {
				evalResults[evaluator.Name()] = &EvalResult{
					Pass:  false,
					Score: 0.0,
					Error: err,
				}
			} else {
				evalResults[evaluator.Name()] = result
			}

			// Update stats
			r.updateEvaluatorStats(evaluatorStats, evaluator.Name(), result, err, time.Since(evalStart))
		}
	}

	return ExampleResult{
		Example:   example,
		Output:    output,
		Results:   evalResults,
		Duration:  time.Since(startTime),
		Timestamp: startTime,
	}, nil
}

// updateEvaluatorStats updates statistics for an evaluator (thread-safe).
func (r *Runner) updateEvaluatorStats(statsMap map[string]*EvaluatorStats, name string, result *EvalResult, err error, duration time.Duration) {
	stats, ok := statsMap[name]
	if !ok {
		return
	}

	stats.TotalEvaluations++

	if err != nil {
		stats.Errors++
	} else if result != nil {
		if result.Pass {
			stats.Passed++
		} else {
			stats.Failed++
		}
		stats.AverageScore = (stats.AverageScore*float64(stats.TotalEvaluations-1) + result.Score) / float64(stats.TotalEvaluations)
	}

	stats.AverageDuration = (stats.AverageDuration*time.Duration(stats.TotalEvaluations-1) + duration) / time.Duration(stats.TotalEvaluations)
}

// calculateSummary calculates aggregate statistics.
func (r *Runner) calculateSummary(results []ExampleResult, evaluatorStats map[string]*EvaluatorStats, totalDuration time.Duration) Summary {
	totalExamples := len(results)
	passedExamples := 0
	totalScore := 0.0
	scoreCount := 0

	for _, result := range results {
		// An example passes if all evaluators pass
		examplePassed := true
		for _, evalResult := range result.Results {
			if evalResult.Error != nil || !evalResult.Pass {
				examplePassed = false
			}
			totalScore += evalResult.Score
			scoreCount++
		}

		if examplePassed {
			passedExamples++
		}
	}

	failedExamples := totalExamples - passedExamples

	var averageScore float64
	if scoreCount > 0 {
		averageScore = totalScore / float64(scoreCount)
	}

	var passRate float64
	if totalExamples > 0 {
		passRate = float64(passedExamples) / float64(totalExamples)
	}

	// Calculate pass rates for each evaluator
	for _, stats := range evaluatorStats {
		total := stats.Passed + stats.Failed
		if total > 0 {
			stats.PassRate = float64(stats.Passed) / float64(total)
		}
	}

	return Summary{
		TotalExamples:  totalExamples,
		PassedExamples: passedExamples,
		FailedExamples: failedExamples,
		AverageScore:   averageScore,
		PassRate:       passRate,
		TotalDuration:  totalDuration,
		EvaluatorStats: evaluatorStats,
	}
}
