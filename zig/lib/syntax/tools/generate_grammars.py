# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

"""Convert pinned Tree-sitter LR tables and lexical automata into native Zig.

This is a maintenance tool; normal builds use the checked-in Zig and binary
tables and require neither Tree-sitter nor a C/Rust compiler. After generation,
run zig fmt on src/grammars and run the upstream corpus tests in grammar.zig.
"""

import argparse
import ast
import hashlib
import json
import re
import struct
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
ROOT = REPO / "zig/lib/syntax/src/grammars"
PINS = {
    "go": (
        "0.25.0",
        "1547678a9da59885853f5f5cc8a99cc203fa2e2c",
        "3dbf6ed1238b5dfcf2be4d2f2d4cb27a14d34f34d7784eccccbfd532fd4a6d85",
    ),
    "python": (
        "0.25.0",
        "293fdc02038ee2bf0e2e206711b69c90ac0d413f",
        "a895f10b3cf7b2608f3283b43cd5cfed70971c7ee4a0136abbaaccbc4a7a25e0",
    ),
    "rust": (
        "0.24.2",
        "77a3747266f4d621d0757825e6b11edcbf991ca5",
        "9602518f9e57919910bf0e777e52f6bfc9325d4c182e998bdb4efd5682b76e4a",
    ),
}


def body(s, start):
    opening = s.index("{", start)
    begin = opening + 1
    depth = 1
    quote = None
    esc = False
    for p in range(opening + 1, len(s)):
        c = s[p]
        if quote:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == quote:
                quote = None
        elif c in "\"'":
            quote = c
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if not depth:
                return s[begin:p]
    raise ValueError("unclosed")


def section(s, name):
    return body(s, s.index(name))


def cvalue(x, ids):
    x = x.strip()
    if x in ids:
        return ids[x]
    if x.startswith("'"):
        v = ast.literal_eval(x)
        assert len(v) == 1
        return ord(v)
    return int(x, 0)


def expressions(s, ids):
    pat = r"'(?:\\.|[^'\\])*'|\b(?:sym_\w+|anon_sym_\w+|aux_sym_\w+|ts_builtin_sym_\w+)\b|&&|\|\||!(?!=)"

    def conv(m):
        x = m[0]
        if x == "&&":
            return " and "
        if x == "||":
            return " or "
        if x == "!":
            return "!"
        return str(cvalue(x, ids)) if x.startswith("'") or x in ids else x

    s = re.sub(pat, conv, s)
    s = re.sub(r"\b00\b", "0", s)
    return re.sub(
        r"set_contains\((\w+),\s*\d+,\s*lookahead\)", r"contains(&\1, lookahead)", s
    )


def lexfun(s, name, ids):
    b = section(s, "static bool " + name + "(")
    out = []
    for match in re.finditer(
        r"case (\d+):\s*(.*?)(?=\n\s*case |\n\s*default:)", b, re.DOTALL
    ):
        n = int(match[1])
        block = match[2].strip()
        result = []
        block = re.sub(r"/\*.*?\*/", "", block, flags=re.DOTALL)
        while block:
            block = block.lstrip()
            m = re.match(r"ACCEPT_TOKEN\((\w+)\);", block)
            if m:
                result.append(
                    f"accepted = .{{ .symbol = {ids[m[1]]}, .start = input.start, .end = input.at }};"
                )
                block = block[m.end() :]
                continue
            m = re.match(r"END_STATE\(\);", block)
            if m:
                result.append("return accepted;")
                block = block[m.end() :]
                continue
            m = re.match(r"ADVANCE_MAP\((.*?)\);", block, re.DOTALL)
            if m:
                pieces = re.findall(r"'(?:\\.|[^'\\])*'|0x[0-9a-fA-F]+|\d+", m[1])
                assert len(pieces) % 2 == 0, m[1]
                result.append("switch (lookahead) {")
                for x, y in zip(pieces[::2], pieces[1::2]):
                    result.append(
                        f"{cvalue(x, ids)} => {{ input.advance(false); state = {y}; continue :scan; }},"
                    )
                result.append("else => {},\n}")
                block = block[m.end() :]
                continue
            if block.startswith("if ("):
                # Find the closing parenthesis outside C character literals.
                at = block.index("(") + 1
                depth = 1
                quote = False
                esc = False
                p = at
                while depth:
                    c = block[p]
                    if quote:
                        if esc:
                            esc = False
                        elif c == "\\":
                            esc = True
                        elif c == "'":
                            quote = False
                    elif c == "'":
                        quote = True
                    elif c == "(":
                        depth += 1
                    elif c == ")":
                        depth -= 1
                    p += 1
                expr = expressions(block[at : p - 1], ids)
                m = re.match(r"\s*(ADVANCE|SKIP)\((\d+)\);", block[p:])
                assert m, block[p : p + 200]
                result.append(
                    f"if ({expr}) {{ input.advance({str(m[1] == 'SKIP').lower()}); state = {m[2]}; continue :scan; }}"
                )
                block = block[p + m.end() :]
                continue
            raise ValueError(block[:300])
        out.append(f"{n} => {{\n" + "\n".join(result) + "\n},")
    eof = (
        "const eof = input.at == input.bytes.len;"
        if re.search(r"\bif \(eof\)", b)
        else ""
    )
    return (
        f"""pub fn {name}(input: *Lexer, initial: u16) ?Token {{
 var state = initial;
 var accepted: ?Token = null;
 scan: while (true) {{
 const lookahead = input.codepoint();
 {eof}
 switch (state) {{
"""
        + "\n".join(out)
        + """\nelse => return accepted,\n}\n}\n}\n"""
    )


def generate(language, path):
    _version, revision, digest = PINS[language]
    parser_bytes = (path / "src/parser.c").read_bytes()
    if hashlib.sha256(parser_bytes).hexdigest() != digest:
        raise ValueError(f"{language}: parser.c differs from pinned {revision}")
    s = parser_bytes.decode()
    ids = {"ts_builtin_sym_end": 0, "ts_builtin_sym_error": 65535}
    defs = dict(re.findall(r"#define (\w+) (\d+)", s))
    defs = {k: int(v) for k, v in defs.items()}
    for enum in re.finditer(r"enum\s+\w+\s*\{(.*?)\};", s, re.DOTALL):
        value = 0
        for item in enum[1].split(","):
            if not item.strip():
                continue
            parts = item.strip().split("=")
            key = parts[0].strip()
            if len(parts) > 1:
                value = int(parts[1])
            ids[key] = value
            value += 1

    def val(x):
        x = x.strip()
        x = re.sub(r"(?:STATE|ACTIONS)\((\d+)\)", r"\1", x)
        return cvalue(x, ids)

    def indexed(name):
        b = section(s, name)
        return {val(i): v for i, v in re.findall(r"\[([^\]]+)\]\s*=\s*([^\n]+)", b)}

    # Normalize both dense and sparse LR rows to sorted symbol/value pairs.
    states = [{} for _ in range(defs["STATE_COUNT"])]
    for n, b in re.findall(
        r"\[STATE\((\d+)\)\] = \{(.*?)\n  \}", section(s, "ts_parse_table["), re.DOTALL
    ):
        states[int(n)] = {
            val(i): val(v)
            for i, v in re.findall(r"\[(\w+)\] = ((?:ACTIONS|STATE)\(\d+\))", b)
        }
    small = section(s, "ts_small_parse_table[]")
    items = re.sub(r"\[\d+\]\s*=", "", small)
    items = [val(x) for x in items.split(",") if x.strip()]
    maps = {
        int(i): int(v)
        for i, v in re.findall(
            r"\[SMALL_STATE\((\d+)\)\] = (\d+)",
            section(s, "ts_small_parse_table_map[]"),
        )
    }
    for state, offset in maps.items():
        p = offset
        groups = items[p]
        p += 1
        row = {}
        for _ in range(groups):
            value, n = items[p : p + 2]
            p += 2
            for symbol in items[p : p + n]:
                row[symbol] = value
            p += n
        states[state] = row
    actions = {}
    b = section(s, "ts_parse_actions[]")
    for m in re.finditer(
        r"\[(\d+)\] = \{\.entry = \{\.count = (\d+),.*?\}\},(.*?)(?=\n  \[|\Z)",
        b,
        re.DOTALL,
    ):
        seq = []
        for op, args in re.findall(
            r"(RECOVER|SHIFT_EXTRA|SHIFT_REPEAT|SHIFT|REDUCE|ACCEPT_INPUT)\(([^)]*)\)",
            m[3],
        ):
            args = [val(x) for x in args.split(",") if x.strip()]
            if op.startswith("SHIFT"):
                seq.append(
                    (
                        1,
                        (
                            1
                            if op == "SHIFT_EXTRA"
                            else 2
                            if op == "SHIFT_REPEAT"
                            else 0
                        ),
                        0,
                        0,
                        args[0] if args else 0,
                        0,
                        0,
                    )
                )
            elif op == "REDUCE":
                symbol, children, prec, prod = args
                seq.append((2, False, children, symbol, 0, prod, prec))
            elif op == "ACCEPT_INPUT":
                seq.append((3, False, 0, 0, 0, 0, 0))
            else:
                seq.append((0, False, 0, 0, 0, 0, 0))
        assert len(seq) == int(m[2]), m[0][:200]
        actions[int(m[1])] = seq
    modes = []
    mode_body = section(s, "ts_lex_modes[")
    for i in range(len(states)):
        mode = re.search(r"\[" + str(i) + r"\] = \{(.*?)\}", mode_body).group(1)
        d = dict(re.findall(r"\.(\w+) = (\d+)", mode))
        if "(TSStateId)(-1)" in mode:
            d["lex_state"] = "65535"
        modes.append(
            [
                int(d.get(k, 0))
                for k in ["lex_state", "external_lex_state", "reserved_word_set_id"]
            ]
        )
    symbols = []
    names = indexed("ts_symbol_names[]")
    metadata = section(s, "ts_symbol_metadata[]")
    for i in range(defs["SYMBOL_COUNT"] + defs["ALIAS_COUNT"]):
        name = ast.literal_eval(names[i].strip().rstrip(",").replace(r"\?", "?"))
        symbols.append(name)
    flags = [0] * len(symbols)
    for key, info in re.findall(r"\[(\w+)\] = \{(.*?)\}", metadata, re.DOTALL):
        flags[ids[key]] = (1 if ".visible = true" in info else 0) | (
            2 if ".named = true" in info else 0
        )
    symmap = [0] * len(symbols)
    for key, value in indexed("ts_symbol_map[]").items():
        symmap[key] = val(value.rstrip(","))
    aliases = [
        [0] * defs["MAX_ALIAS_SEQUENCE_LENGTH"]
        for _ in range(defs["PRODUCTION_ID_COUNT"])
    ]
    for prod, b in re.findall(
        r"\[(\d+)\] = \{(.*?)\}", section(s, "ts_alias_sequences["), re.DOTALL
    ):
        for child, symbol in re.findall(r"\[(\d+)\] = (\w+)", b):
            aliases[int(prod)][int(child)] = ids[symbol]
    fields = []
    fieldnames = {0: ""}
    if defs["FIELD_COUNT"]:
        fieldnames.update(
            {
                key: (
                    "" if value.rstrip(",") == "NULL" else json.loads(value.rstrip(","))
                )
                for key, value in indexed("ts_field_names[]").items()
            }
        )
    entries = []
    if defs["FIELD_COUNT"]:
        for field, child, inherited in re.findall(
            r"\{(field_\w+), (\d+)(, \.inherited = true)?\}",
            section(s, "ts_field_map_entries[]"),
        ):
            entries.append((ids[field], int(child), bool(inherited)))
    slices = (
        {
            int(i): (int(index), int(n))
            for i, index, n in re.findall(
                r"\[(\d+)\] = \{\.index = (\d+), \.length = (\d+)\}",
                section(s, "ts_field_map_slices["),
            )
        }
        if defs["FIELD_COUNT"]
        else {}
    )
    for prod in range(defs["PRODUCTION_ID_COUNT"]):
        at, n = slices.get(prod, (0, 0))
        fields.append(entries[at : at + n])
    reserved = []
    if "ts_reserved_words[" in s:
        r = section(s, "ts_reserved_words[")
        d = {
            int(n): [val(x) for x in b.split(",") if x.strip()]
            for n, b in re.findall(r"\[(\d+)\] = \{(.*?)\}", r, re.DOTALL)
        }
        reserved = [d.get(i, []) for i in range(max(d, default=0) + 1)]
    else:
        reserved = [[]]
    extmap = []
    external = []
    if defs["EXTERNAL_TOKEN_COUNT"]:
        d = indexed("ts_external_scanner_symbol_map[")
        extmap = [val(d[i].rstrip(",")) for i in range(defs["EXTERNAL_TOKEN_COUNT"])]
        r = section(s, "ts_external_scanner_states[")
        d = {
            int(n): [ids[x] for x in re.findall(r"\[(\w+)\] = true", b)]
            for n, b in re.findall(r"\[(\d+)\] = \{(.*?)\}", r, re.DOTALL)
        }
        external = [
            sum(1 << x for x in d.get(i, [])) for i in range(max(d, default=0) + 1)
        ]
    # Little-endian indexed records: row directory+entries; actions; modes; aliases; fields; reserved; external.
    data = bytearray()
    offsets = {}

    def u16(x):
        data.extend(struct.pack("<H", x))

    def u32(x):
        data.extend(struct.pack("<I", x))

    def start(name):
        offsets[name] = len(data)

    start("rows")
    p = 0
    for row in states:
        u32(p)
        u16(len(row))
        p += len(row)
    start("entries")
    for row in states:
        for symbol, value in sorted(row.items()):
            u16(symbol)
            u16(value)
    start("actions")
    na = max(actions) + 1
    for i in range(na):
        u32(0)
        u16(0)
    start("action_data")
    for i, seq in actions.items():
        struct.pack_into("<IH", data, offsets["actions"] + i * 6, len(data), len(seq))
        for tag, extra, children, symbol, state, prod, prec in seq:
            data.extend(
                struct.pack(
                    "<BBBBHHHh", tag, extra, children, 0, symbol, state, prod, prec
                )
            )
    start("modes")
    for mode in modes:
        for x in mode:
            u16(x)
    start("aliases")
    for alias in aliases:
        for x in alias:
            u16(x)
    start("fields")
    p = 0
    for fields_ in fields:
        u32(p)
        u16(len(fields_))
        p += len(fields_)
    start("field_entries")
    for fields_ in fields:
        for field, child, inherited in fields_:
            u16(field)
            data.extend(bytes([child, inherited]))
    start("reserved")
    p = 0
    for words in reserved:
        u32(p)
        u16(len(words))
        p += len(words)
    start("reserved_data")
    for words in reserved:
        for x in words:
            u16(x)
    start("external")
    for x in external:
        u32(x)
    start("external_symbols")
    for x in extmap:
        u16(x)
    (ROOT / (language + ".bin")).write_bytes(data)
    license = (path / "LICENSE").read_text()
    header = (
        "// Generated from tree-sitter-"
        + language
        + " "
        + revision
        + "; upstream MIT notice follows.\n"
        + "".join("// " + line + "\n" for line in license.splitlines())
        + "\n"
    )
    out = (
        header
        + """const runtime = @import("../grammar.zig");
const Lexer = runtime.Lexer;
const Token = runtime.Token;
const contains = runtime.contains;
"""
    )
    out += (
        "pub const grammar: runtime.Grammar = .{\n.scanner = ."
        + (language if language != "go" else "none")
        + ',\n.data = @embedFile("'
        + language
        + '.bin"),\n.lex = ts_lex,\n.keywords = ts_lex_keywords,\n'
    )
    out += (
        " .tokens = "
        + str(defs["TOKEN_COUNT"])
        + ",\n .state_count = "
        + str(len(states))
        + ",\n.alias_width = "
        + str(defs["MAX_ALIAS_SEQUENCE_LENGTH"])
        + ",\n"
    )
    word = re.search(r"\.keyword_capture_token = (\w+)", s)
    out += " .word = " + str(ids[word[1]] if word else 0) + ",\n"
    if language == "go":
        out += (
            " .builtin_identifiers = &.{"
            + ",".join(str(ids[name]) for name in ("anon_sym_new", "anon_sym_make"))
            + "},\n"
        )
    for key, value in offsets.items():
        out += " ." + key + " = " + str(value) + ",\n"
    out += (
        " .names = &.{"
        + ",".join(
            json.dumps(x, ensure_ascii=False).replace(r"\u0000", r"\x00")
            for x in symbols
        )
        + "},\n"
    )
    out += " .flags = &.{" + ",".join(map(str, flags)) + "},\n"
    out += " .symbol_map = &.{" + ",".join(map(str, symmap)) + "},\n"
    out += (
        " .field_names = &.{"
        + ",".join(json.dumps(fieldnames[i]) for i in range(len(fieldnames)))
        + "},\n};\n"
    )
    for name, b in re.findall(
        r"static const TSCharacterRange (\w+)\[\] = \{(.*?)\};", s, re.DOTALL
    ):
        ranges = re.findall(
            r"\{\s*('(?:\\.|[^'\\])*'|0x[\da-fA-F]+|\d+),\s*('(?:\\.|[^'\\])*'|0x[\da-fA-F]+|\d+)\s*\}",
            b,
        )
        out += (
            "const "
            + name
            + " = [_][2]u32{"
            + ",".join(".{" + str(val(x)) + "," + str(val(y)) + "}" for x, y in ranges)
            + "};\n"
        )
    out += lexfun(s, "ts_lex", ids) + lexfun(s, "ts_lex_keywords", ids)
    (ROOT / (language + ".zig")).write_text(out)
    print(language, len(data), "data bytes", len(out), "source bytes")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "sources",
        type=Path,
        help="Directory containing tree-sitter-{go,python,rust}-VERSION upstream source trees",
    )
    args = parser.parse_args()
    for lang, (version, _, _) in PINS.items():
        path = args.sources / f"tree-sitter-{lang}-{version}"
        generate(lang, path)
        generate_corpus(lang, path)


def compiler_tree_choices(language, title, text):
    """Resolve two upstream snapshot ambiguities using language compiler ASTs.

    CPython ast.parse makes Starred contain Attribute/Subscript/Call, and Go's
    go/parser produces CallExpr with IndexExpr/IndexListExpr for generic calls.
    These choices preserve actual callees for source indexing. Grammar acceptance
    is unchanged. All other snapshots are used directly from upstream.
    """
    if language == "python" and title.endswith(": Lists"):
        for old, new in [
            (
                "(attribute(list_splat(identifier))(identifier))",
                "(list_splat(attribute(identifier)(identifier)))",
            ),
            (
                "(attribute(subscript(list_splat(identifier))(identifier))(identifier))",
                "(list_splat(attribute(subscript(identifier)(identifier))(identifier)))",
            ),
            (
                "(call(list_splat(identifier))(argument_list))",
                "(list_splat(call(identifier)(argument_list)))",
            ),
        ]:
            text = text.replace(old, new)
    if language == "go" and title.endswith(": Generic call expressions"):
        # Parse the normalized tree so nested type arguments remain intact.
        tokens = re.findall(r"\(|\)|[^()]+", text)
        at = 0

        def read():
            nonlocal at
            assert tokens[at] == "("
            name = tokens[at + 1]
            at += 2
            children = []
            while tokens[at] != ")":
                children.append(read())
            at += 1
            if (
                name == "type_conversion_expression"
                and children[0][0] == "generic_type"
            ):
                generic, argument = children
                arguments = generic[1][1][1]
                if len(arguments) == 1:
                    callee = (
                        "index_expression",
                        [("identifier", []), ("identifier", [])],
                    )
                    return ("call_expression", [callee, ("argument_list", [argument])])
                return (
                    "call_expression",
                    [
                        ("identifier", []),
                        ("type_arguments", arguments),
                        ("argument_list", [argument]),
                    ],
                )
            return name, children

        def render(n):
            return "(" + n[0] + "".join(map(render, n[1])) + ")"

        text = render(read())
    return text


def normalize_tree(text):
    tokens = re.findall(r'\(|\)|"(?:\\.|[^"\\])*"|[^\s()]+', text)
    at = 0

    def node():
        nonlocal at
        assert tokens[at] == "("
        at += 1
        name = tokens[at]
        at += 1
        children = []
        while tokens[at] != ")":
            if tokens[at] == "(":
                children.append(node())
            else:
                at += 1  # Field labels and anonymous terminals.
        at += 1
        if name in {"comment", "line_comment", "block_comment", "line_continuation"}:
            return ""
        return "(" + name + "".join(children) + ")"

    return node().encode()


def generate_corpus(language, path):
    data = bytearray()
    for source_path in sorted((path / "test/corpus").rglob("*.txt")):
        parts = re.split(r"(?m)^={3,}\r?\n", source_path.read_text())
        for i in range(1, len(parts) - 1, 2):
            name = parts[i].strip()
            halves = re.split(r"(?m)^-{3,}\r?\n", parts[i + 1], maxsplit=1)
            if len(halves) != 2 or ":skip" in name:
                continue
            source, expected = halves
            source = (source.strip("\r\n") + "\n").encode()
            title = (str(source_path.relative_to(path / "test")) + ": " + name).encode()
            valid = "(ERROR" not in expected and "(MISSING" not in expected
            expected = (
                compiler_tree_choices(
                    language, title.decode(), normalize_tree(expected).decode()
                ).encode()
                if valid
                else b""
            )
            data += (
                struct.pack("<IIIB", len(title), len(source), len(expected), valid)
                + title
                + source
                + expected
            )
    (ROOT / (language + "_corpus.bin")).write_bytes(data)


if __name__ == "__main__":
    main()
