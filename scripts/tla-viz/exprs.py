#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tiny safe expression evaluator for display invariants.

Bindings declare per-step assertions over flattened trace fields, e.g.
    identityRows == visible
    !restoreIntentCleared || runtimeRepairComplete
    visible >= baseVisible

Grammar (loosest binding first):
    or:      and ("||" and)*
    and:     cmp ("&&" cmp)*
    cmp:     add (("=="|"!="|"<="|">="|"<"|">") add)?
    add:     unary (("+"|"-") unary)*
    unary:   "!" unary | "-" unary | atom
    atom:    INT | STRING | "true" | "false" | IDENT | "(" or ")"

Identifiers resolve against the step's flattened field map. A reference to a
missing field makes the whole expression evaluate to None ("not assessable at
this step") rather than raising — display invariants must never break a
render. These are legibility aids; TLC remains the authority.
"""

from __future__ import annotations

import re

_TOKEN_RE = re.compile(r"""
    \s*(?:
        (?P<num>-?\d+)
      | (?P<str>'[^']*'|"[^"]*")
      | (?P<op>\|\||&&|==|!=|<=|>=|[!<>+\-()])
      | (?P<ident>[A-Za-z_][A-Za-z0-9_.]*)
    )""", re.VERBOSE)


class _Missing:
    pass


MISSING = _Missing()


def tokenize(src: str) -> list[tuple[str, str]]:
    tokens, pos = [], 0
    while pos < len(src):
        m = _TOKEN_RE.match(src, pos)
        if m is None or m.end() == pos:
            if src[pos:].strip():
                raise ValueError(f"bad token at {src[pos:]!r}")
            break
        pos = m.end()
        for kind in ("num", "str", "op", "ident"):
            val = m.group(kind)
            if val is not None:
                tokens.append((kind, val))
                break
    return tokens


class Expr:
    """Compiled expression: parse once, evaluate per step."""

    def __init__(self, src: str):
        self.src = src
        self._tokens = tokenize(src)
        self._pos = 0
        self._ast = self._or()
        if self._pos != len(self._tokens):
            raise ValueError(f"trailing tokens in {src!r}")

    # ---- parser (produces nested tuples) ----

    def _peek(self):
        return self._tokens[self._pos] if self._pos < len(self._tokens) else (None, None)

    def _take(self, kind=None, val=None):
        tok = self._peek()
        if (kind and tok[0] != kind) or (val and tok[1] != val):
            raise ValueError(f"expected {val or kind} in {self.src!r}")
        self._pos += 1
        return tok

    def _or(self):
        node = self._and()
        while self._peek() == ("op", "||"):
            self._take()
            node = ("or", node, self._and())
        return node

    def _and(self):
        node = self._cmp()
        while self._peek() == ("op", "&&"):
            self._take()
            node = ("and", node, self._cmp())
        return node

    def _cmp(self):
        node = self._add()
        kind, val = self._peek()
        if kind == "op" and val in ("==", "!=", "<=", ">=", "<", ">"):
            self._take()
            node = ("cmp", val, node, self._add())
        return node

    def _add(self):
        node = self._unary()
        while True:
            kind, val = self._peek()
            if kind == "op" and val in ("+", "-"):
                self._take()
                node = ("arith", val, node, self._unary())
            else:
                return node

    def _unary(self):
        kind, val = self._peek()
        if kind == "op" and val == "!":
            self._take()
            return ("not", self._unary())
        if kind == "op" and val == "-":
            self._take()
            return ("neg", self._unary())
        return self._atom()

    def _atom(self):
        kind, val = self._take()
        if kind == "num":
            return ("lit", int(val))
        if kind == "str":
            return ("lit", val[1:-1])
        if kind == "ident":
            if val == "true":
                return ("lit", True)
            if val == "false":
                return ("lit", False)
            return ("field", val)
        if kind == "op" and val == "(":
            node = self._or()
            self._take("op", ")")
            return node
        raise ValueError(f"unexpected {val!r} in {self.src!r}")

    # ---- evaluator ----

    def evaluate(self, env: dict) -> bool | None:
        """True/False, or None when a referenced field is absent this step."""
        val = self._eval(self._ast, env)
        if val is MISSING:
            return None
        return bool(val)

    def _eval(self, node, env):
        op = node[0]
        if op == "lit":
            return node[1]
        if op == "field":
            return env.get(node[1], MISSING)
        if op == "not":
            v = self._eval(node[1], env)
            return MISSING if v is MISSING else not v
        if op == "neg":
            v = self._eval(node[1], env)
            return MISSING if v is MISSING else -v
        a = self._eval(node[2], env)
        b = self._eval(node[3], env) if op in ("cmp", "arith") else None
        if op == "or":
            left = self._eval(node[1], env)
            if left is not MISSING and left:
                return True
            right = self._eval(node[2], env)
            if right is MISSING or left is MISSING:
                return MISSING
            return bool(left or right)
        if op == "and":
            left = self._eval(node[1], env)
            if left is not MISSING and not left:
                return False
            right = self._eval(node[2], env)
            if right is MISSING or left is MISSING:
                return MISSING
            return bool(left and right)
        if a is MISSING or b is MISSING:
            return MISSING
        if op == "arith":
            try:
                return a + b if node[1] == "+" else a - b
            except TypeError:
                return MISSING
        # cmp
        try:
            return {"==": a == b, "!=": a != b,
                    "<": a < b, "<=": a <= b,
                    ">": a > b, ">=": a >= b}[node[1]]
        except TypeError:
            return MISSING
