// Copyright 2026 Antfly, Inc.
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

import { app, BrowserWindow, dialog, ipcMain } from 'electron';
import { readdir, stat } from 'node:fs/promises';
import { join, relative, resolve, sep } from 'node:path';
import { homedir } from 'node:os';

import { createStaticServer, pathIsInside } from './file_server.mjs';

const webRoot = resolve(new URL('../web/', import.meta.url).pathname);
const defaultModelsDir = resolve(join(homedir(), '.termite/models'));

let serverOrigin = null;
let mainWindow = null;

function normalizeRelativePath(baseDir, absolutePath) {
  return relative(baseDir, absolutePath).split(sep).join('/');
}

function isInterestingModelFile(entryName) {
  const lowered = entryName.toLowerCase();
  return lowered.endsWith('.gguf') ||
    lowered.endsWith('.safetensors') ||
    lowered === 'config.json' ||
    lowered === 'gliner_config.json' ||
    lowered === 'rebel_config.json' ||
    lowered === 'tokenizer.json' ||
    lowered === 'tokenizer_config.json';
}

function toFileUrl(filePath) {
  const url = new URL('/__termite__/file', serverOrigin);
  url.searchParams.set('path', filePath);
  return url.href;
}

async function collectModelFiles(rootDir, currentDir = rootDir, out = []) {
  const entries = await readdir(currentDir, { withFileTypes: true });
  for (const entry of entries) {
    const absPath = join(currentDir, entry.name);
    if (entry.isDirectory()) {
      await collectModelFiles(rootDir, absPath, out);
      continue;
    }
    if (!entry.isFile() || !isInterestingModelFile(entry.name)) continue;
    const fileStat = await stat(absPath);
    out.push({
      path: normalizeRelativePath(rootDir, absPath),
      sizeBytes: fileStat.size,
      url: toFileUrl(absPath),
    });
  }
  return out;
}

const allowedModelRoots = new Set([defaultModelsDir]);

function rememberAllowedModelRoot(dirPath) {
  allowedModelRoots.add(resolve(dirPath));
}

function modelRootIsAllowed(dirPath) {
  const resolved = resolve(dirPath);
  for (const root of allowedModelRoots) {
    if (pathIsInside(resolved, root)) return true;
  }
  return false;
}

async function startServer() {
  const server = createStaticServer({
    webRoot,
    getAllowedFileRoots: () => Array.from(allowedModelRoots),
  });
  await new Promise((resolvePromise, rejectPromise) => {
    server.once('error', rejectPromise);
    server.listen(0, '127.0.0.1', () => resolvePromise());
  });
  const address = server.address();
  serverOrigin = `http://127.0.0.1:${address.port}`;
}

async function createWindow() {
  if (!serverOrigin) {
    await startServer();
  }

  mainWindow = new BrowserWindow({
    width: 1440,
    height: 980,
    webPreferences: {
      preload: join(app.getAppPath(), 'preload.mjs'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  });

  await mainWindow.loadURL(`${serverOrigin}/index.html`);
}

ipcMain.handle('termite-electron:get-default-models-dir', async () => defaultModelsDir);

ipcMain.handle('termite-electron:choose-models-dir', async () => {
  const result = await dialog.showOpenDialog(mainWindow, {
    properties: ['openDirectory'],
    defaultPath: defaultModelsDir,
  });
  if (result.canceled || result.filePaths.length === 0) return null;
  const chosen = resolve(result.filePaths[0]);
  rememberAllowedModelRoot(chosen);
  return chosen;
});

ipcMain.handle('termite-electron:scan-models-dir', async (_event, dirPath) => {
  const resolved = resolve(dirPath || defaultModelsDir);
  if (!modelRootIsAllowed(resolved)) {
    throw new Error(`models directory ${resolved} has not been selected by the user`);
  }
  const files = await collectModelFiles(resolved);
  return {
    directoryPath: resolved,
    files,
  };
});

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});
