// Vendored Yuku 0.20.1; upstream revision e68066ff291d2fe43c727054f03636f4a633596b.
// SPDX-License-Identifier: MIT
//
// MIT License
//
// Copyright (c) 2026 Yuku
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

const core = @import("types/core.zig");
const object = @import("types/object.zig");
const generics = @import("types/generics.zig");
const predicate = @import("types/predicate.zig");
const arrows = @import("arrows.zig");
const ts_expressions = @import("expressions.zig");

pub const parseTypeAliasBody = core.parseTypeAliasBody;
pub const extendQualifiedName = core.extendQualifiedName;

pub const parseTypeParameters = generics.parseTypeParameters;
pub const parseTypeArguments = generics.parseTypeArguments;
pub const isAngleOpen = generics.isAngleOpen;

pub const parseTypeAnnotation = predicate.parseTypeAnnotation;
pub const parseReturnTypeAnnotation = predicate.parseReturnTypeAnnotation;
pub const parsePatternTypeAnnotation = predicate.parsePatternTypeAnnotation;
pub const applyDecoratorsToPattern = predicate.applyDecoratorsToPattern;
pub const markPatternOptional = predicate.markPatternOptional;
pub const checkDefiniteAssignment = predicate.checkDefiniteAssignment;

pub const parseObjectTypeMembers = object.parseObjectTypeMembers;
pub const isIndexSignatureStart = object.isIndexSignatureStart;
pub const parseIndexSignature = object.parseIndexSignature;

pub const classifyArrowHead = arrows.classifyArrowHead;
pub const parseArrow = arrows.parseArrow;
pub const tryParseArrow = arrows.tryParseArrow;
pub const tryParseGenericArrow = arrows.tryParseGenericArrow;

pub const parseTypeAssertion = ts_expressions.parseTypeAssertion;
pub const parseAsOrSatisfiesExpression = ts_expressions.parseAsOrSatisfiesExpression;
pub const parseNonNullExpression = ts_expressions.parseNonNullExpression;
pub const parseTypeArgumentedCallOrInstantiation =
    ts_expressions.parseTypeArgumentedCallOrInstantiation;
pub const tryParseTypeArgumentsInExpression = ts_expressions.tryParseTypeArgumentsInExpression;
