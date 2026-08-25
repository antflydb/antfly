from enum import Enum


class GraphQueryModeUnsupportedErrorReason(str, Enum):
    DEDUPLICATE_NODES_MUST_BE_TRUE = "deduplicate_nodes_must_be_true"
    DIRECTION_MUST_BE_OUT = "direction_must_be_out"
    EXPAND_STRATEGY_NOT_SUPPORTED = "expand_strategy_not_supported"
    EXTERNAL_ALIAS_DOCUMENT_FILTER_NOT_SUPPORTED = "external_alias_document_filter_not_supported"
    EXTERNAL_ALIAS_SOURCE_NOT_SUPPORTED = "external_alias_source_not_supported"
    K_MUST_EQUAL_ONE = "k_must_equal_one"
    PATTERN_REQUIRED = "pattern_required"
    REVERSE_VARIABLE_PATH_NOT_SUPPORTED = "reverse_variable_path_not_supported"
    START_SELECTOR_NOT_SUPPORTED = "start_selector_not_supported"
    TARGET_REQUIRED = "target_required"
    TARGET_SELECTOR_NOT_SUPPORTED = "target_selector_not_supported"
    UNSUPPORTED_MODE = "unsupported_mode"
    WEIGHT_MODE_MUST_BE_MIN_HOPS = "weight_mode_must_be_min_hops"

    def __str__(self) -> str:
        return str(self.value)
