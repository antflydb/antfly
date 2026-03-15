// Types for the Evals Playground

export interface EvalItem {
  id: string;
  question: string;
  referenceAnswer: string;
  addedAt: string; // ISO date string
}

export interface EvalSet {
  name: string;
  createdAt: string; // ISO date string
  items: EvalItem[];
}

export interface EvalSets {
  [setName: string]: EvalSet;
}

// Result types for running evaluations
export interface EvalItemResult {
  itemId: string;
  question: string;
  referenceAnswer: string;
  actualAnswer: string;
  score: number;
  pass: boolean;
  reason: string;
  durationMs: number;
  error?: string;
}

export interface EvalRunResult {
  setName: string;
  tableName: string;
  startedAt: string;
  completedAt: string;
  results: EvalItemResult[];
  summary: {
    total: number;
    passed: number;
    failed: number;
    errors: number;
    averageScore: number;
    totalDurationMs: number;
  };
}
