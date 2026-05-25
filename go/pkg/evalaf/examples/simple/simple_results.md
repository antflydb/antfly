# Evaluation Report

**Timestamp**: 2025-11-11T14:38:56-08:00

**Duration**: 151.834µs

## Summary

- **Total Examples**: 3
- **Passed**: 2 (66.7%)
- **Failed**: 1 (33.3%)
- **Average Score**: 0.781

## Evaluator Statistics

| Evaluator | Total | Passed | Failed | Errors | Avg Score | Avg Duration |
|-----------|-------|--------|--------|--------|-----------|-------------|
| regex_match | 3 | 3 (100.0%) | 0 | 0 | 1.000 | 15.124µs |
| contains | 3 | 2 (66.7%) | 1 | 0 | 0.667 | 6.319µs |
| fuzzy_match | 3 | 2 (66.7%) | 1 | 0 | 0.792 | 26.999µs |
| exact_match | 3 | 2 (66.7%) | 1 | 0 | 0.667 | 15.471µs |

## Failed Examples

### Example 3

**Input**: `What color is the sky?`

**Reference**: `(?i)blue`

**Output**: `Blue`

**Results**:

- **fuzzy_match**: ❌ FAIL (score: 0.375) - Similarity: 37.50% (threshold: 80.00%)
- **exact_match**: ❌ FAIL (score: 0.000) - Output does not match reference (got "Blue", expected "(?i)blue")
- **contains**: ❌ FAIL (score: 0.000) - Output does not contain "(?i)blue"
- **regex_match**: ✅ PASS (score: 1.000)

