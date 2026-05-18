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

import assert from "node:assert/strict";
import {
    createGoParityRemoteTemplateRenderer,
    formatDotpromptMediaUrl,
} from "./wasm_client.mjs";

const renderer = createGoParityRemoteTemplateRenderer({
    remoteText({ url, credentials }) {
        return `text:${url}:${credentials}`;
    },
    remoteMedia({ url, mode, credentials }) {
        if (mode === "extract") return `pdf-text:${url}:${credentials}`;
        return formatDotpromptMediaUrl(`media:${mode}:${url}:${credentials}`);
    },
    transcribeAudio({ url, language }) {
        return `transcribed:${url}:${language}`;
    },
});

assert.equal(
    renderer({
        templateSource: "{{title}} {{body}}",
        jsonDoc: { title: "Hello", body: "world" },
    }),
    "Hello world",
);

assert.equal(
    renderer({
        templateSource: "{{author.name}}",
        jsonDoc: { author: { name: "Ada" } },
    }),
    "Ada",
);

assert.equal(
    renderer({
        templateSource: "{{scrubHtml body}}",
        jsonDoc: { body: "<p>Hello</p><script>evil()</script><p>World</p>" },
    }),
    "HelloWorld",
);

assert.equal(
    renderer({
        templateSource: "{{remoteText url=this credentials=\"primary\"}}",
        jsonDoc: "\"https://example.com/doc.txt\"",
    }),
    "text:https://example.com/doc.txt:primary",
);

assert.equal(
    renderer({
        templateSource: "{{remoteMedia url=this}}",
        jsonDoc: "\"https://example.com/image.png\"",
    }),
    "<<<dotprompt:media:url media:raw:https://example.com/image.png:>>>",
);

assert.equal(
    renderer({
        templateSource: "{{remoteMedia url=this mode=\"render\" credentials=\"thumbs\"}}",
        jsonDoc: "\"https://example.com/doc.pdf\"",
    }),
    "<<<dotprompt:media:url media:render:https://example.com/doc.pdf:thumbs>>>",
);

assert.equal(
    renderer({
        templateSource: "{{remotePDF url=this credentials=\"primary\"}}",
        jsonDoc: "\"https://example.com/doc.pdf\"",
    }),
    "pdf-text:https://example.com/doc.pdf:primary",
);

assert.equal(
    renderer({
        templateSource: "{{transcribeAudio url=this language=\"en\"}}",
        jsonDoc: "\"https://example.com/audio.mp3\"",
    }),
    "transcribed:https://example.com/audio.mp3:en",
);

console.log("antfly embedded wasm client parity helpers passed: ok");
