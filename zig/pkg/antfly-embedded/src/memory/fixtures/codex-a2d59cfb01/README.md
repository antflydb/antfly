# Codex external-judgment fixtures

Copied unchanged from [antflydb/codex at a2d59cfb01](https://github.com/antflydb/codex/tree/a2d59cfb01/codex-rs/antfly/tests/fixtures/knowledge_eval), under that repository's Apache-2.0 license. See [the integration review](https://github.com/antflydb/antfly/pull/1227#issuecomment-6327590919).

Native tests replay the labelled verdicts through service policy. They validate admission, application, distinct pins, typed relationships and supersession; they do not measure a model's accuracy. Hosted inference and credentials stay with the client. The 64-case merge corpus is distinct from the earlier 30-case concept-pair corpus.
