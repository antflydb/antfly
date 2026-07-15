from enum import Enum


class SortProfileCandidateSource(str, Enum):
    COMPOSED = "composed"
    DISTRIBUTED_SHARDS = "distributed_shards"
    EXISTING_HITS = "existing_hits"
    MATCH_ALL = "match_all"
    NATIVE_FILTER = "native_filter"
    NONE = "none"
    PRIMARY_KEY = "primary_key"
    SORTED_SEGMENT_MEMBERSHIP = "sorted_segment_membership"
    TEXT_POSTINGS = "text_postings"
    VECTOR_TOP_K = "vector_top_k"

    def __str__(self) -> str:
        return str(self.value)
