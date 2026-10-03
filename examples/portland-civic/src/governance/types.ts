export interface SourceDocument {
  id: string;
  title: string;
  publisher: string;
  source_url: string;
  sha256: string;
  fetched_at: string;
  published: string;
  version: string;
  supersedes?: string;
  page_count: number;
  indexed_pages: number[];
  extraction: "pdf-text" | "atlas-structured-data";
  review: "unreviewed";
  atlas_sha256?: string;
  atlas_hash_matches?: boolean;
}
export interface Passage {
  id: string;
  document_id: string;
  page: number;
  start: number;
  end: number;
  text: string;
}
export interface EvidencePage {
  document_id: string;
  page: number;
  text: string;
  extraction?: "native-text" | "florence-ocr" | "unreadable";
  quality_flags?: string[];
  caption?: string;
  reader_model?: string;
}
export interface DecisionEvent {
  id: string;
  date: string;
  chamber: string;
  text: string;
  vote?: string;
  source_url: string;
  evidence: string[];
  involves: string[];
}
export interface Claim {
  id: string;
  question: string;
  statement: string;
  evidence: string[];
  contrary: string[];
  limitation: string;
  involves: string[];
}
export interface ResearchGap {
  id: string;
  question: string;
  reason: string;
  needed: string;
  query: string;
}
export interface EvidenceNode {
  id: string;
  kind: "document" | "passage" | "event" | "claim" | "entity" | "gap";
  title: string;
  search_text: string;
  evidence?: string[];
  contrary?: string[];
  involves?: string[];
  supersedes?: string[];
  document_id?: string;
  passage_id?: string;
}
export interface GovernanceSnapshot {
  version: 1;
  fetched_at: string;
  atlas_url: string;
  atlas_sha256: string;
  documents: SourceDocument[];
  pages: EvidencePage[];
  passages: Passage[];
  events: DecisionEvent[];
  claims: Claim[];
  gaps: ResearchGap[];
  nodes: EvidenceNode[];
}
export interface GovernanceManifest {
  snapshot_id: string;
  documents: SourceDocument[];
  events: DecisionEvent[];
  claims: Claim[];
  gaps: ResearchGap[];
  nodes: EvidenceNode[];
  passage_count: number;
  fetched_at: string;
  atlas_url: string;
  semantic: boolean;
  semantic_ready: boolean;
  graph_ready: boolean;
}
export interface PassageHit extends Passage {
  score: number;
}
export interface EvidenceSearch {
  hits: PassageHit[];
  captions?: {
    document_id: string;
    page: number;
    caption: string;
    score: number;
  }[];
  mode: "keyword" | "hybrid";
  took: number;
}
export interface EvidenceBrief {
  status: "source-supported" | "conflicting-sources" | "not-established";
  statement: string;
  limitation: string;
  evidence: Passage[];
  contrary: Passage[];
  candidates: PassageHit[];
  curated: boolean;
  generator?: string;
  citation_validation?: "valid" | "rejected";
  claims?: {
    text: string;
    quote?: string;
    evidence: string[];
    contrary: string[];
  }[];
}
