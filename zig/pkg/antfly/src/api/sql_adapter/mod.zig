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

pub const ast = @import("ast.zig");
pub const classifier = @import("classifier.zig");
pub const corpus = @import("corpus.zig");
pub const diagnostics = @import("diagnostics.zig");
pub const grammar = @import("grammar.zig");
pub const lexer = @import("lexer.zig");
pub const parser = @import("parser.zig");
pub const token = @import("token.zig");
pub const value = @import("value.zig");

pub const SelectOutputKind = ast.SelectOutputKind;
pub const SelectOutputRef = ast.SelectOutputRef;
pub const SelectSetOperation = ast.SelectSetOperation;
pub const SqlAdapterClassificationReason = diagnostics.SqlAdapterClassificationReason;
pub const SqlStatementFamily = classifier.SqlStatementFamily;
pub const SqlPatternQuantifier = ast.SqlPatternQuantifier;
pub const SqlRowClaimClause = ast.SqlRowClaimClause;
pub const SqlValue = value.SqlValue;
pub const SqlWriteStatementKind = classifier.SqlWriteStatementKind;
pub const Token = token.Token;
pub const TokenKind = token.TokenKind;
pub const UnsupportedPlanFamily = corpus.UnsupportedPlanFamily;
pub const adapterNoopFingerprintAlloc = corpus.adapterNoopFingerprintAlloc;
pub const adapterNoopPlanMatchesReason = corpus.adapterNoopPlanMatchesReason;
pub const arrayLengthDefaultOutput = grammar.arrayLengthDefaultOutput;
pub const atEnd = parser.atEnd;
pub const classificationReasonFromToken = diagnostics.classificationReasonFromToken;
pub const classificationReasonIsAdapterNoop = diagnostics.classificationReasonIsAdapterNoop;
pub const classificationReasonIsUnsupportedRequirement = diagnostics.classificationReasonIsUnsupportedRequirement;
pub const classificationReasonToken = diagnostics.classificationReasonToken;
pub const classificationReasonTokenIsKnown = diagnostics.classificationReasonTokenIsKnown;
pub const classifyStatementFamily = classifier.classifyStatementFamily;
pub const classifyWriteStatement = classifier.classifyWriteStatement;
pub const expectKeyword = parser.expectKeyword;
pub const expectToken = parser.expectToken;
pub const findMatchingRParenIndex = parser.findMatchingRParenIndex;
pub const findTopLevelKeyword = parser.findTopLevelKeyword;
pub const freeTokens = lexer.freeTokens;
pub const matchKeyword = parser.matchKeyword;
pub const matchToken = parser.matchToken;
pub const peekKeyword = parser.peekKeyword;
pub const peekKind = parser.peekKind;
pub const rowExpressionBoundaryKeyword = grammar.rowExpressionBoundaryKeyword;
pub const sqlAssignmentTailKeyword = grammar.sqlAssignmentTailKeyword;
pub const sqlJoinedSourceAliasTerminator = grammar.sqlJoinedSourceAliasTerminator;
pub const sqlJsonExtractPathFunctionAsText = grammar.sqlJsonExtractPathFunctionAsText;
pub const sqlKeywordIsAnyOrSome = grammar.sqlKeywordIsAnyOrSome;
pub const sqlKeywordIsArrayLengthFunction = grammar.sqlKeywordIsArrayLengthFunction;
pub const sqlKeywordIsArrayPositionFunction = grammar.sqlKeywordIsArrayPositionFunction;
pub const sqlKeywordIsArrayToStringFunction = grammar.sqlKeywordIsArrayToStringFunction;
pub const sqlKeywordIsAsciiFunction = grammar.sqlKeywordIsAsciiFunction;
pub const sqlKeywordIsBitLengthFunction = grammar.sqlKeywordIsBitLengthFunction;
pub const sqlKeywordIsCardinalityFunction = grammar.sqlKeywordIsCardinalityFunction;
pub const sqlKeywordIsChrFunction = grammar.sqlKeywordIsChrFunction;
pub const sqlKeywordIsDateBinFunction = grammar.sqlKeywordIsDateBinFunction;
pub const sqlKeywordIsDatePartFunction = grammar.sqlKeywordIsDatePartFunction;
pub const sqlKeywordIsDateTruncFunction = grammar.sqlKeywordIsDateTruncFunction;
pub const sqlKeywordIsEndsWithFunction = grammar.sqlKeywordIsEndsWithFunction;
pub const sqlKeywordIsInitcapFunction = grammar.sqlKeywordIsInitcapFunction;
pub const sqlKeywordIsJsonArrayLengthFunction = grammar.sqlKeywordIsJsonArrayLengthFunction;
pub const sqlKeywordIsJsonBuildObjectFunction = grammar.sqlKeywordIsJsonBuildObjectFunction;
pub const sqlKeywordIsJsonExtractPathFunction = grammar.sqlKeywordIsJsonExtractPathFunction;
pub const sqlKeywordIsJsonTypeofFunction = grammar.sqlKeywordIsJsonTypeofFunction;
pub const sqlKeywordIsLeftRightFunction = grammar.sqlKeywordIsLeftRightFunction;
pub const sqlKeywordIsLengthFunction = grammar.sqlKeywordIsLengthFunction;
pub const sqlKeywordIsMd5Function = grammar.sqlKeywordIsMd5Function;
pub const sqlKeywordIsOctetLengthFunction = grammar.sqlKeywordIsOctetLengthFunction;
pub const sqlKeywordIsOverlayFunction = grammar.sqlKeywordIsOverlayFunction;
pub const sqlKeywordIsPadFunction = grammar.sqlKeywordIsPadFunction;
pub const sqlKeywordIsRegexpCountFunction = grammar.sqlKeywordIsRegexpCountFunction;
pub const sqlKeywordIsRegexpInstrFunction = grammar.sqlKeywordIsRegexpInstrFunction;
pub const sqlKeywordIsRegexpMatchFunction = grammar.sqlKeywordIsRegexpMatchFunction;
pub const sqlKeywordIsRegexpSubstrFunction = grammar.sqlKeywordIsRegexpSubstrFunction;
pub const sqlKeywordIsRepeatFunction = grammar.sqlKeywordIsRepeatFunction;
pub const sqlKeywordIsReverseFunction = grammar.sqlKeywordIsReverseFunction;
pub const sqlKeywordIsSplitPartFunction = grammar.sqlKeywordIsSplitPartFunction;
pub const sqlKeywordIsStartsWithFunction = grammar.sqlKeywordIsStartsWithFunction;
pub const sqlKeywordIsStrposFunction = grammar.sqlKeywordIsStrposFunction;
pub const sqlKeywordIsSubstringFunction = grammar.sqlKeywordIsSubstringFunction;
pub const sqlKeywordIsTranslateFunction = grammar.sqlKeywordIsTranslateFunction;
pub const sqlKeywordIsTrimVariantFunction = grammar.sqlKeywordIsTrimVariantFunction;
pub const sqlKeywordIsUuidV4Function = grammar.sqlKeywordIsUuidV4Function;
pub const sqlKeywordStartsScalarPredicate = grammar.sqlKeywordStartsScalarPredicate;
pub const tokenizeAlloc = lexer.tokenizeAlloc;
pub const unsupportedFingerprintAlloc = corpus.unsupportedFingerprintAlloc;
pub const unsupportedPlanFamilyToken = corpus.unsupportedPlanFamilyToken;
pub const unsupportedPlanMatchesFamily = corpus.unsupportedPlanMatchesFamily;
pub const unsupportedPlanMatchesReason = corpus.unsupportedPlanMatchesReason;
