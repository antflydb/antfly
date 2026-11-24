// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Optional real-browser companion to TestLocalNativeGCS*, using an installed
// Playwright package. No mocked browser requests: queries reach native Antfly.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {join} from 'node:path';
import {writeFile} from 'node:fs/promises';
const require = createRequire(import.meta.url);
const {chromium} = require(process.env.ANTFLY_GREPSTEIN_PLAYWRIGHT_MODULE || 'playwright');
const [url, profile, artifacts] = process.argv.slice(2);
assert.ok(['pdf', 'transcription', 'images'].includes(profile));
const query = profile === 'images' ? 'A red vehicle' : 'Native GCS extraction';
const browser = await chromium.launch({headless:true, executablePath:process.env.ANTFLY_GREPSTEIN_BROWSER_EXECUTABLE || undefined});
try {
 const page = await browser.newPage();
 const errors = [];
 page.on('pageerror', e => errors.push(e.message));
 await page.goto(url);
 await page.getByRole('combobox', {name:'Record collection'}).selectOption('fixture');
 await page.getByRole('textbox', {name:'Search records'}).fill(query);
 const firstSearch = page.waitForResponse(r => new URL(r.url()).pathname === '/api/search');
 await page.getByRole('button', {name:'Search',exact:true}).click();
 await writeFile(join(artifacts, 'search.json'), await (await firstSearch).text());
 await page.waitForFunction(() => document.querySelector('#status').textContent.includes('results.'));
 const cards = page.locator('#results article');
 assert.ok(await cards.count() >= 2, 'search must include both native records');
 assert.ok((await cards.first().textContent()).includes(query));
 const link = cards.first().getByRole('link', {name:/Open original/});
 const href = await link.getAttribute('href');
 assert.ok(href.includes('collection=fixture'));
 if (profile === 'images') assert.equal(await link.textContent(), 'Open original image');
 else assert.ok(href.includes(profile === 'transcription' ? '#t=' : '#page='));
 const original = await page.request.get(new URL(href, url).href);
 assert.equal(original.status(),200);
 assert.ok((await original.body()).length > 0);
 assert.match(original.headers()['content-type'], profile === 'transcription' ? /^audio\// : profile === 'images' ? /^image\// : /^application\/pdf/);
 if(profile === 'transcription') {
  const player = cards.first().locator('audio');
  assert.equal(await player.getAttribute('preload'),'none');
  await cards.first().getByRole('button', {name:/^Play /}).first().click();
  await page.waitForFunction(() => {const p=document.querySelector('audio');return p && p.readyState >= 1;});
  assert.ok(await player.evaluate(p=>Number.isFinite(p.duration) && p.duration>0));
 }
 await cards.first().locator('summary').click();
 assert.ok(await cards.first().locator('pre').isVisible());
 await cards.first().locator('summary').click();
 await page.screenshot({path:join(artifacts,'native-'+profile+'.png'),fullPage:true});
 await page.getByRole('textbox', {name:'Search records'}).fill('zzznomatchfixture');
 await page.getByRole('button', {name:'Search',exact:true}).click();
 await page.waitForFunction(()=>document.querySelector('#status').textContent.startsWith('0 results.'));
 assert.equal(await cards.count(),0);
 for (const mode of ['semantic', 'graph']) {
  await page.getByRole('combobox', {name:'Search mode'}).selectOption(mode);
  await page.getByRole('textbox', {name:'Search records'}).fill(query);
  await page.getByRole('button', {name:'Search',exact:true}).click();
  await page.waitForFunction(()=> {const s=document.querySelector('#status').textContent;return s!=='Searching…';});
  assert.ok(!(await page.locator('#status').textContent()).includes('results.'), 'unconfigured '+mode+' search must show error');
 }
 assert.deepEqual(errors,[]);
 console.log(JSON.stringify({profile,search:true,source:true,citations:true,audio:profile==='transcription',empty:true,error:true,screenshot:join(artifacts,'native-'+profile+'.png')}));
} finally { await browser.close(); }
