# ui - UI Integration Helpers

The `ui` package provides utilities for embedding evaluation results in user interfaces, including data formatting and visualization helpers.

## Features

- Dashboard-friendly data formatting
- Chart data generation for visualizations
- Report comparison utilities
- JSON export for frontend consumption

## Quick Start

```go
import "github.com/antflydb/antfly/pkg/evalaf/ui"

// Run evaluation
report, _ := runner.RunWithTarget(ctx, dataset, target)

// Convert to dashboard format
dashboardData := ui.FormatForDashboard(report)

// Send to frontend
jsonData, _ := dashboardData.ToJSON(true)
sendToFrontend(jsonData)
```

## Data Structures

### DashboardData

Main structure for UI consumption:

```go
type DashboardData struct {
    Summary   SummaryData      // High-level statistics
    Charts    []ChartData      // Visualization data
    Results   []ResultData     // Detailed results
    Timestamp time.Time        // Evaluation timestamp
}
```

### SummaryData

Aggregate statistics:

```go
type SummaryData struct {
    TotalExamples  int      // Total number of examples
    PassedExamples int      // Number that passed
    FailedExamples int      // Number that failed
    PassRate       float64  // Pass rate (0-1)
    AverageScore   float64  // Average score
    Duration       string   // Total duration
}
```

### ChartData

Data for visualizations:

```go
type ChartData struct {
    Type   string    // "bar", "line", "pie", "gauge"
    Title  string    // Chart title
    Labels []string  // Data labels (for bar/line charts)
    Values []float64 // Data values
    Color  string    // Color hint (for gauges)
}
```

### ResultData

Individual evaluation results:

```go
type ResultData struct {
    ExampleID   int                      // Example number
    Input       string                   // Input text
    Output      string                   // Output text
    Reference   string                   // Expected output
    Status      string                   // "passed", "failed", "error"
    Evaluations map[string]EvaluationData // Per-evaluator results
    Duration    string                   // Evaluation duration
}
```

## Complete Example

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"

    "github.com/antflydb/antfly/pkg/evalaf/eval"
    "github.com/antflydb/antfly/pkg/evalaf/ui"
)

func handleEvaluation(w http.ResponseWriter, r *http.Request) {
    ctx := context.Background()

    // Run evaluation
    config := eval.DefaultConfig()
    dataset := eval.NewJSONDatasetFromExamples("test", examples)
    evaluators := []eval.Evaluator{
        eval.NewExactMatchEvaluator("exact"),
    }

    runner := eval.NewRunner(*config, evaluators)
    report, err := runner.RunWithTarget(ctx, dataset, targetFunc)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // Convert to dashboard format
    dashboardData := ui.FormatForDashboard(report)

    // Send to frontend
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(dashboardData)
}
```

## Integration with Antfly UI

```go
// In Antfly metadata server
func (s *Server) handleEvaluationResults(w http.ResponseWriter, r *http.Request) {
    // Run Antfly RAG evaluation
    report := runAntflyEvaluation()

    // Format for UI
    dashboardData := ui.FormatForDashboard(report)

    // Embed in Antfly response
    response := map[string]any{
        "evaluation": dashboardData,
        "metadata": map[string]any{
            "version": "1.0",
        },
    }

    json.NewEncoder(w).Encode(response)
}
```

## Integration with searchaf

```go
// In searchaf dashboard
func (s *SearchafServer) getCustomerEvaluation(customerID string) (*ui.DashboardData, error) {
    // Run evaluation for customer
    report := runCustomerEvaluation(customerID)

    // Format for dashboard
    return ui.FormatForDashboard(report), nil
}

// API endpoint
func (s *SearchafServer) handleCustomerEvaluation(w http.ResponseWriter, r *http.Request) {
    customerID := extractCustomerID(r)

    dashboardData, err := s.getCustomerEvaluation(customerID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    // Send to frontend
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(dashboardData)
}
```

## Chart Types

### Pie Chart (Pass/Fail Distribution)

```json
{
  "type": "pie",
  "title": "Pass/Fail Distribution",
  "labels": ["Passed", "Failed"],
  "values": [85, 15]
}
```

### Gauge Chart (Pass Rate)

```json
{
  "type": "gauge",
  "title": "Overall Pass Rate",
  "values": [85.0],
  "color": "green"
}
```

### Bar Chart (Evaluator Performance)

```json
{
  "type": "bar",
  "title": "Evaluator Pass Rates (%)",
  "labels": ["faithfulness", "relevance", "citations"],
  "values": [90.5, 85.2, 78.3]
}
```

## Report Comparison

Compare two evaluation runs:

```go
import "github.com/antflydb/antfly/pkg/evalaf/ui"

// Run baseline evaluation
baselineReport, _ := runEvaluation(baselinePrompt)

// Run new evaluation
currentReport, _ := runEvaluation(newPrompt)

// Compare
comparison := ui.CompareReports(baselineReport, currentReport)

// Check improvement
if comparison.Delta.PassRateDelta > 0 {
    fmt.Printf("Improvement: +%.2f%% pass rate\n", comparison.Delta.PassRateDelta*100)
}
```

## Frontend Integration

### React Example

```typescript
interface DashboardData {
  summary: {
    totalExamples: number;
    passedExamples: number;
    failedExamples: number;
    passRate: number;
    averageScore: number;
    duration: string;
  };
  charts: ChartData[];
  results: ResultData[];
  timestamp: string;
}

function EvaluationDashboard() {
  const [data, setData] = useState<DashboardData | null>(null);

  useEffect(() => {
    fetch('/api/evaluation/results')
      .then(res => res.json())
      .then(setData);
  }, []);

  if (!data) return <div>Loading...</div>;

  return (
    <div>
      <h1>Evaluation Results</h1>
      <SummaryCard summary={data.summary} />
      {data.charts.map(chart => (
        <Chart key={chart.title} data={chart} />
      ))}
      <ResultsTable results={data.results} />
    </div>
  );
}
```

### Rendering Charts

The chart data is designed to work with popular charting libraries:

**Chart.js Example:**
```javascript
// For bar chart
new Chart(ctx, {
  type: 'bar',
  data: {
    labels: chartData.labels,
    datasets: [{
      data: chartData.values,
      label: chartData.title
    }]
  }
});
```

**Recharts Example:**
```jsx
import { BarChart, Bar, XAxis, YAxis } from 'recharts';

<BarChart data={chartData.labels.map((label, i) => ({
  name: label,
  value: chartData.values[i]
}))}>
  <Bar dataKey="value" />
  <XAxis dataKey="name" />
  <YAxis />
</BarChart>
```

## Color Coding

Pass rate colors:
- **Green**: ≥90%
- **Yellow**: 70-89%
- **Red**: <70%

Status indicators:
- **passed**: Green
- **failed**: Red
- **error**: Orange

## Best Practices

1. **Real-time Updates**: Poll evaluation endpoint periodically for live updates
2. **Filtering**: Allow users to filter results by evaluator, status, etc.
3. **Pagination**: Paginate large result sets in the UI
4. **Drill-down**: Allow clicking on summary cards to see detailed results
5. **Export**: Provide CSV/Excel export for detailed analysis
6. **Trends**: Show historical trends using MetricTrend data structure
7. **Alerts**: Notify users when pass rate drops below threshold

## See Also

- [eval package](../eval/README.md) - Core evaluation library
- [genkit package](../genkit/README.md) - LLM-as-judge evaluators
- [rag package](../rag/README.md) - RAG-specific evaluators
- [agent package](../agent/README.md) - Agent-specific evaluators
