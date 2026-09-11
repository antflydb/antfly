// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Small model-free seeds derived from the v2 wire/Unicode/long-document
//! regression contracts. These are request bytes, not learned-output fixtures.
pub const basic =
    \\{"schema_version":2,"model":"fuzz","schema":{"entities":["person"]},"inputs":[{"content":"Ada met Grace in 東京. 😀 é"}]}
;
pub const replacements =
    \\{"schema_version":2,"model":"fuzz","schema":{"entities":["person"]},"options":{"threshold":0.9,"include_spans":true,"word_splitter":"char"},"inputs":[{"id":"first","content":[{"type":"text","text":"Ada"},{"type":"text","text":"Lovelace"}]},{"id":"second","content":"東京 😀","schema":{"classifications":[{"name":"t","labels":["a","b"],"max_labels":null,"ordered":false}]},"options":{}}]}
;
pub const mixed =
    \\{"schema_version":2,"model":"fuzz","schema":{"entities":["person","org"],"entity_attributes":{"tone":{"labels":["positive","negative"],"applies_to":[]}},"classifications":[{"name":"topic","labels":["work","other"],"mode":"multi","min_labels":0,"max_labels":null,"prompt":"Choose a topic","examples":[{"input":"A job","label":"work"}]}],"structures":{"deal":{"mode":"natural","anchor":"buyer","fields":{"buyer":{"dtype":"str","cardinality":"required_one"},"status":{"choices":["paid","unpaid"]},"tags":{"dtype":"list","cardinality":"zero_or_more"}}}},"relations":[{"type":"works_for","source":"person","target":"org","description":"employment"}],"classification_constraints":[{"type":"Or","children":[{"type":"LabelRef","task":"topic","label":"work"},{"type":"Not","child":{"type":"AnySelected","task":"topic"}}]}]},"inputs":[{"content":"Ada met Grace at Antfly and paid for a new job in London yesterday."}]}
;
pub const ordinal =
    \\{"schema_version":2,"model":"fuzz","schema":{"classifications":[{"name":"priority","labels":["low","medium","high"],"mode":"ordinal"}],"classification_constraints":[{"type":"MinLevel","task":"priority","level":"medium"}]},"inputs":[{"content":"Escalate this issue."}],"options":{"decoder":{"algorithm":"beam","max_search_nodes":1,"best_effort":true},"long_document":{"mode":"window","window_words":8,"overlap_words":2,"record_identity":"semantic"}}}
;
pub const joint =
    \\{"schema_version":2,"model":"fuzz","schema":{"joint_ie":{"entities":{"person":{},"org":{}},"relations":{"works_for":{"head":["person"],"tail":["org"],"allow_self":false,"max_per_head":1}},"constraints":[{"type":"NoSelfLoops"},{"type":"AcyclicRelation","relation":"works_for"}]}},"inputs":[{"content":"Ada works for Antfly."}],"options":{"joint_ie":{"candidate_threshold":0.1},"offset_unit":"utf16_codeunits"}}
;
pub const regex =
    \\{"schema_version":2,"model":"fuzz","schema":{"entities":["person"],"entity_definitions":{"person":{"validators":[{"type":"regex","pattern":"[A-Z][a-z]+","mode":"partial","flags":0}]}}},"inputs":[{"content":"Ada Lovelace"}]}
;
// Unicode case closure traverses the pinned equivalence table for each
// character class and deliberately exceeds this harness's tiny step ceiling.
pub const regex_casefold_limit =
    \\{"schema_version":2,"model":"fuzz","schema":{"entities":["person"],"entity_definitions":{"person":{"validators":[{"type":"regex","pattern":"[A-Z][a-z]+","mode":"partial","flags":2}]}}},"inputs":[{"content":"Ada Lovelace"}]}
;
pub const unicode =
    \\{"schema_version":2,"model":"fuzz","schema":{"entities":["name"]},"options":{"word_splitter":"char","offset_unit":"unicode_codepoints"},"inputs":[{"content":"  東京\tİ ΟΣ ẞ 😀 👩‍💻 é\nhttps://example.org/a?b=1&c=2   "},{"content":"\r\n\t \u0000"},{"content":""}]}
;
pub const valid = [_][]const u8{ basic, replacements, mixed, ordinal, joint, regex, unicode };
pub const invalid = [_][]const u8{
    "",                                                                                                                "{",                                                                                                                     "[]",                                                                                                                      "\xff\xfe",                                                                                                        "{\"schema_version\":2}",
    "{\"schema_version\":2,\"schema_version\":1}",                                                                     "{\"schema_version\":2,\"model\":\"m\",\"schema\":{\"entities\":[\"p\"]},\"inputs\":[{\"id\":null,\"content\":\"x\"}]}", "{\"schema_version\":2,\"model\":\"m\",\"schema\":{\"entities\":[\"p\"]},\"inputs\":[{\"content\":\"x\",\"schema\":{}}]}", "{\"schema_version\":2,\"model\":\"m\",\"schema\":{\"entities\":[\"p\"]},\"inputs\":[{\"content\":\"\\ud800\"}]}", "{\"schema_version\":2,\"model\":\"m\",\"schema\":{\"entities\":[\"p\"]},\"inputs\":[{\"content\":\"x\"}],\"options\":{\"threshold\":1e999}}",
    "{\"schema_version\":2,\"model\":\"m\",\"schema\":{\"entities\":[\"p\",\"p\"]},\"inputs\":[{\"content\":\"x\"}]}",
};
pub const all = valid ++ invalid ++ [_][]const u8{regex_casefold_limit};
