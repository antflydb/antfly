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

// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import koffi from "koffi";
import { AntflyInferenceOptions, AntflyOpenOptions, loadNative } from "./native.js";
import { SUPPORTED_ABI_VERSION } from "./types.js";

/** Thrown by validateAbi() when the loaded libantfly does not match the header this binding was written against. */
export class AbiMismatchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AbiMismatchError";
  }
}

/** Returns the loaded Antfly C ABI version (antfly_abi_version()). */
export function abiVersion(): number {
  return loadNative().abiVersion();
}

/**
 * Reports the loaded libantfly threading contract, like sqlite3_threadsafe().
 * Always THREADING_SERIALIZED for the current libantfly.
 */
export function threadingMode(): number {
  return loadNative().threadingMode();
}

/**
 * Verifies that the loaded C library matches the header this binding was
 * compiled against: antfly_abi_version() and the antfly_open_options struct
 * size. Called automatically by every open/create/checkFile call; exposed so
 * applications can fail fast at startup too.
 */
export function validateAbi(): void {
  const native = loadNative();
  const gotVersion = native.abiVersion();
  if (gotVersion !== SUPPORTED_ABI_VERSION) {
    throw new AbiMismatchError(
      `lite: unsupported C ABI version ${gotVersion}, want ${SUPPORTED_ABI_VERSION}`
    );
  }
  const gotSize = native.openOptionsSize();
  const wantSize = koffi.sizeof(AntflyOpenOptions);
  if (gotSize !== wantSize) {
    throw new AbiMismatchError(
      `lite: C ABI open options size ${gotSize}, compiled header size ${wantSize}`
    );
  }
}

/**
 * Verifies that the loaded C library's antfly_inference_options struct size
 * matches the header this binding was compiled against, like validateAbi()
 * does for antfly_open_options. Called automatically by Inference.open();
 * exposed so applications can fail fast at startup too.
 */
export function validateInferenceAbi(): void {
  const native = loadNative();
  const gotSize = native.inferenceOptionsSize();
  const wantSize = koffi.sizeof(AntflyInferenceOptions);
  if (gotSize !== wantSize) {
    throw new AbiMismatchError(
      `lite: C ABI inference options size ${gotSize}, compiled header size ${wantSize}`
    );
  }
}
