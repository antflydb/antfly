// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import CryptoKit
import Foundation

// The private seed is runtime input, never an argument or compiled source.
guard let line = readLine(), let seed = Data(base64Encoded: line), seed.count == 32 else {
  throw NSError(
    domain: "dev.antfly.dictation.signing", code: 1,
    userInfo: [NSLocalizedDescriptionKey: "Expected a modern 32-byte Sparkle private seed."])
}
let key = try Curve25519.Signing.PrivateKey(rawRepresentation: seed)
print(key.publicKey.rawRepresentation.base64EncodedString())
