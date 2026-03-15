package ui

import (
	"encoding/json"
	"time"

	"github.com/antflydb/antfly/pkg/evalaf/eval"
)

// DashboardData represents evaluation data formatted for UI display.
type DashboardData struct {
	Summary   SummaryData  `json:"summary"`
	Charts    []ChartData  `json:"charts"`
	Results   []ResultData `json:"results"`
	Timestamp time.Time    `json:"timestamp"`
}

// SummaryData contains high-level summary statistics.
type SummaryData struct {
	TotalExamples  int     `json:"total_examples"`
	PassedExamples int     `json:"passed_examples"`
	FailedExamples int     `json:"failed_examples"`
	PassRate       float64 `json:"pass_rate"`
	AverageScore   float64 `json:"average_score"`
	Duration       string  `json:"duration"`
}

// ChartData represents data for visualization.
type ChartData struct {
	Type   string    `json:"type"` // "bar", "line", "pie", "gauge"
	Title  string    `json:"title"`
	Labels []string  `json:"labels,omitempty"`
	Values []float64 `json:"values"`
	Color  string    `json:"color,omitempty"`
}

// ResultData represents a single evaluation result for display.
type ResultData struct {
	ExampleID   int                       `json:"example_id"`
	Input       string                    `json:"input"`
	Output      string                    `json:"output"`
	Reference   string                    `json:"reference,omitempty"`
	Status      string                    `json:"status"` // "passed", "failed", "error"
	Evaluations map[string]EvaluationData `json:"evaluations"`
	Duration    string                    `json:"duration"`
}

// EvaluationData represents a single evaluator's result.
type EvaluationData struct {
	Name   string  `json:"name"`
	Pass   bool    `json:"pass"`
	Score  float64 `json:"score"`
	Reason string  `json:"reason"`
	Error  string  `json:"error,omitempty"`
}

// FormatForDashboard converts an evaluation report to dashboard-friendly format.
func FormatForDashboard(report *eval.Report) *DashboardData {
	// Create summary
	summary := SummaryData{
		TotalExamples:  report.Summary.TotalExamples,
		PassedExamples: report.Summary.PassedExamples,
		FailedExamples: report.Summary.FailedExamples,
		PassRate:       report.Summary.PassRate,
		AverageScore:   report.Summary.AverageScore,
		Duration:       report.Summary.TotalDuration.String(),
	}

	// Create charts
	charts := []ChartData{
		// Pass/Fail pie chart
		{
			Type:   "pie",
			Title:  "Pass/Fail Distribution",
			Labels: []string{"Passed", "Failed"},
			Values: []float64{float64(report.Summary.PassedExamples), float64(report.Summary.FailedExamples)},
		},
		// Overall pass rate gauge
		{
			Type:   "gauge",
			Title:  "Overall Pass Rate",
			Values: []float64{report.Summary.PassRate * 100},
			Color:  getColorForPassRate(report.Summary.PassRate),
		},
	}

	// Add evaluator performance chart
	if len(report.Summary.EvaluatorStats) > 0 {
		labels := []string{}
		values := []float64{}
		for name, stats := range report.Summary.EvaluatorStats {
			labels = append(labels, name)
			values = append(values, stats.PassRate*100)
		}
		charts = append(charts, ChartData{
			Type:   "bar",
			Title:  "Evaluator Pass Rates (%)",
			Labels: labels,
			Values: values,
		})
	}

	// Convert results
	results := make([]ResultData, 0, len(report.Results))
	for i, result := range report.Results {
		// Determine overall status
		status := "passed"
		for _, evalResult := range result.Results {
			if evalResult.Error != nil {
				status = "error"
				break
			} else if !evalResult.Pass {
				status = "failed"
				break
			}
		}

		// Convert evaluations
		evaluations := make(map[string]EvaluationData)
		for evalName, evalResult := range result.Results {
			errStr := ""
			if evalResult.Error != nil {
				errStr = evalResult.Error.Error()
			}

			evaluations[evalName] = EvaluationData{
				Name:   evalName,
				Pass:   evalResult.Pass,
				Score:  evalResult.Score,
				Reason: evalResult.Reason,
				Error:  errStr,
			}
		}

		results = append(results, ResultData{
			ExampleID:   i + 1,
			Input:       formatValue(result.Example.Input),
			Output:      formatValue(result.Output),
			Reference:   formatValue(result.Example.Reference),
			Status:      status,
			Evaluations: evaluations,
			Duration:    result.Duration.String(),
		})
	}

	return &DashboardData{
		Summary:   summary,
		Charts:    charts,
		Results:   results,
		Timestamp: report.Timestamp,
	}
}

// ToJSON converts dashboard data to JSON.
func (d *DashboardData) ToJSON(pretty bool) ([]byte, error) {
	if pretty {
		return json.MarshalIndent(d, "", "  ")
	}
	return json.Marshal(d)
}

// formatValue converts any value to a string for display.
func formatValue(value any) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(data)
	}
}

// getColorForPassRate returns a color based on pass rate.
func getColorForPassRate(passRate float64) string {
	if passRate >= 0.9 {
		return "green"
	} else if passRate >= 0.7 {
		return "yellow"
	}
	return "red"
}

// MetricTrend represents a metric's trend over time.
type MetricTrend struct {
	MetricName string      `json:"metric_name"`
	Timestamps []time.Time `json:"timestamps"`
	Values     []float64   `json:"values"`
	Trend      string      `json:"trend"` // "improving", "declining", "stable"
}

// CompareReports compares two evaluation reports.
func CompareReports(baseline, current *eval.Report) *ComparisonData {
	return &ComparisonData{
		Baseline: FormatForDashboard(baseline),
		Current:  FormatForDashboard(current),
		Delta: DeltaData{
			PassRateDelta:     current.Summary.PassRate - baseline.Summary.PassRate,
			AverageScoreDelta: current.Summary.AverageScore - baseline.Summary.AverageScore,
		},
	}
}

// ComparisonData represents a comparison between two reports.
type ComparisonData struct {
	Baseline *DashboardData `json:"baseline"`
	Current  *DashboardData `json:"current"`
	Delta    DeltaData      `json:"delta"`
}

// DeltaData represents the change between two reports.
type DeltaData struct {
	PassRateDelta     float64 `json:"pass_rate_delta"`
	AverageScoreDelta float64 `json:"average_score_delta"`
}
