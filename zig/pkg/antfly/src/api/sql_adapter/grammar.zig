// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub fn sqlKeywordIsAnyOrSome(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "any") or std.ascii.eqlIgnoreCase(text, "some");
}

pub fn sqlKeywordStartsScalarPredicate(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "is") or
        std.ascii.eqlIgnoreCase(text, "isnull") or
        std.ascii.eqlIgnoreCase(text, "in") or
        std.ascii.eqlIgnoreCase(text, "between") or
        std.ascii.eqlIgnoreCase(text, "like") or
        std.ascii.eqlIgnoreCase(text, "ilike") or
        std.ascii.eqlIgnoreCase(text, "notnull") or
        sqlKeywordIsAnyOrSome(text) or
        std.ascii.eqlIgnoreCase(text, "all");
}

pub fn sqlJoinedSourceAliasTerminator(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn sqlAssignmentTailKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "from") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for");
}

pub fn sqlKeywordIsLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "length") or
        std.ascii.eqlIgnoreCase(text, "char_length") or
        std.ascii.eqlIgnoreCase(text, "character_length");
}

pub fn sqlKeywordIsOctetLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "octet_length");
}

pub fn sqlKeywordIsBitLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "bit_length");
}

pub fn sqlKeywordIsJsonArrayLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_array_length") or
        std.ascii.eqlIgnoreCase(text, "jsonb_array_length");
}

pub fn sqlKeywordIsCardinalityFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "cardinality");
}

pub fn sqlKeywordIsArrayLengthFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_length") or
        sqlKeywordIsCardinalityFunction(text);
}

pub fn sqlKeywordIsArrayPositionFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_position") or
        std.ascii.eqlIgnoreCase(text, "array_positions");
}

pub fn sqlKeywordIsArrayToStringFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "array_to_string");
}

pub fn arrayLengthDefaultOutput(keyword: []const u8) []const u8 {
    if (sqlKeywordIsCardinalityFunction(keyword)) return "cardinality";
    return "array_length";
}

pub fn sqlKeywordIsJsonTypeofFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_typeof") or
        std.ascii.eqlIgnoreCase(text, "jsonb_typeof");
}

pub fn sqlKeywordIsJsonExtractPathFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_extract_path") or
        std.ascii.eqlIgnoreCase(text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path_text");
}

pub fn sqlKeywordIsJsonBuildObjectFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_build_object") or
        std.ascii.eqlIgnoreCase(text, "jsonb_build_object");
}

pub fn sqlJsonExtractPathFunctionAsText(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "json_extract_path_text") or
        std.ascii.eqlIgnoreCase(text, "jsonb_extract_path_text");
}

pub fn sqlKeywordIsAsciiFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "ascii");
}

pub fn sqlKeywordIsChrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "chr");
}

pub fn sqlKeywordIsSubstringFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "substring") or
        std.ascii.eqlIgnoreCase(text, "substr");
}

pub fn sqlKeywordIsOverlayFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "overlay");
}

pub fn sqlKeywordIsTranslateFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "translate");
}

pub fn sqlKeywordIsSplitPartFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "split_part");
}

pub fn sqlKeywordIsStrposFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "strpos");
}

pub fn sqlKeywordIsLeftRightFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "left") or
        std.ascii.eqlIgnoreCase(text, "right");
}

pub fn sqlKeywordIsPadFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "lpad") or
        std.ascii.eqlIgnoreCase(text, "rpad");
}

pub fn sqlKeywordIsRepeatFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "repeat");
}

pub fn sqlKeywordIsReverseFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "reverse");
}

pub fn sqlKeywordIsInitcapFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "initcap");
}

pub fn sqlKeywordIsMd5Function(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "md5");
}

pub fn sqlKeywordIsStartsWithFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "starts_with");
}

pub fn sqlKeywordIsEndsWithFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "ends_with");
}

pub fn sqlKeywordIsDateTruncFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_trunc");
}

pub fn sqlKeywordIsDateBinFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_bin");
}

pub fn sqlKeywordIsDatePartFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "date_part") or
        std.ascii.eqlIgnoreCase(text, "extract");
}

pub fn sqlKeywordIsTrimVariantFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "btrim") or
        std.ascii.eqlIgnoreCase(text, "ltrim") or
        std.ascii.eqlIgnoreCase(text, "rtrim");
}

pub fn sqlKeywordIsUuidV4Function(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "gen_random_uuid") or
        std.ascii.eqlIgnoreCase(text, "uuid_generate_v4");
}

pub fn sqlKeywordIsRegexpMatchFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_match") or
        std.ascii.eqlIgnoreCase(text, "regexp_like");
}

pub fn sqlKeywordIsRegexpCountFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_count");
}

pub fn sqlKeywordIsRegexpSubstrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_substr");
}

pub fn sqlKeywordIsRegexpInstrFunction(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "regexp_instr");
}

pub fn rowExpressionBoundaryKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "as") or
        std.ascii.eqlIgnoreCase(text, "from") or
        std.ascii.eqlIgnoreCase(text, "where") or
        std.ascii.eqlIgnoreCase(text, "and") or
        std.ascii.eqlIgnoreCase(text, "or") or
        std.ascii.eqlIgnoreCase(text, "group") or
        std.ascii.eqlIgnoreCase(text, "having") or
        std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "fetch") or
        std.ascii.eqlIgnoreCase(text, "for") or
        std.ascii.eqlIgnoreCase(text, "asc") or
        std.ascii.eqlIgnoreCase(text, "desc") or
        std.ascii.eqlIgnoreCase(text, "nulls") or
        std.ascii.eqlIgnoreCase(text, "then") or
        std.ascii.eqlIgnoreCase(text, "else") or
        std.ascii.eqlIgnoreCase(text, "end") or
        std.ascii.eqlIgnoreCase(text, "when") or
        std.ascii.eqlIgnoreCase(text, "filter") or
        std.ascii.eqlIgnoreCase(text, "over");
}

pub fn sqlWhereTailClauseKeyword(text: []const u8) bool {
    return std.ascii.eqlIgnoreCase(text, "order") or
        std.ascii.eqlIgnoreCase(text, "limit") or
        std.ascii.eqlIgnoreCase(text, "offset") or
        std.ascii.eqlIgnoreCase(text, "for") or
        std.ascii.eqlIgnoreCase(text, "group") or
        std.ascii.eqlIgnoreCase(text, "having") or
        std.ascii.eqlIgnoreCase(text, "returning") or
        std.ascii.eqlIgnoreCase(text, "join") or
        std.ascii.eqlIgnoreCase(text, "left") or
        std.ascii.eqlIgnoreCase(text, "inner") or
        std.ascii.eqlIgnoreCase(text, "with") or
        std.ascii.eqlIgnoreCase(text, "over") or
        std.ascii.eqlIgnoreCase(text, "lateral");
}
