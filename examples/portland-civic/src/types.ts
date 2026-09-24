export interface Permit {
  id: string;
  source_id: number;
  application: string;
  address: string;
  description: string;
  neighborhood: string;
  permit_type: string;
  status: string;
  created_at?: string;
  issued_at?: string;
  finaled_at?: string;
  valuation?: number;
  new_units?: number;
  location?: { lat: number; lon: number };
  source_url: string;
  search_text: string;
}
export interface Point {
  date: string;
  value: number;
  label?: string;
}
export interface Housing {
  fetched_at: string;
  source_url: string;
  data_status: string;
  series: { id: string; title: string; unit: string; points: Point[] }[];
}
export interface Snapshot {
  version: 1;
  fetched_at: string;
  source_url: string;
  since: string;
  source_count: number;
  selection: string;
  permits: Permit[];
  housing: Housing | null;
}
export interface Manifest extends Omit<Snapshot, "permits"> {
  snapshot_id: string;
  imported_count: number;
  semantic: boolean;
  neighborhoods: string[];
  statuses: string[];
  permit_types: string[];
}
export interface Filters {
  q: string;
  neighborhood: string;
  status: string;
  permit_type: string;
  from: string;
  to: string;
  mode: "keyword" | "hybrid";
  offset: number;
}
export interface SearchResult {
  permits: Permit[];
  total: number;
  exact: boolean;
  took: number;
  scope: "all_matches" | "ranked_candidates";
  neighborhoods: { key: string; doc_count: number }[];
  manifest: Manifest;
  generation_enabled: boolean;
}
export interface Explanation {
  text: string;
  generated: boolean;
  sources: { id: string; title: string; url: string }[];
}
