// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// An incremental SSE data decoder. Foundation's AsyncBytes.lines discards
/// empty lines, so it cannot preserve the event boundaries required by SSE.
public struct SSEDecoder: Sendable {
  private var line = Data()
  private var data: [String] = []
  private var eventBytes = 0
  private var carriageReturn = false
  public init() {}
  public mutating func push(_ byte: UInt8) throws -> String? {
    if byte == 10 && carriageReturn {
      carriageReturn = false
      return nil
    }
    carriageReturn = byte == 13
    if byte == 10 || byte == 13 {
      defer { line.removeAll(keepingCapacity: true) }
      if line.isEmpty {
        guard !data.isEmpty else { return nil }
        let result = data.joined(separator: "\n")
        data.removeAll(keepingCapacity: true)
        eventBytes = 0
        return result
      }
      let text = String(decoding: line, as: UTF8.self)
      if text.hasPrefix("data:") {
        var fragment = text.dropFirst(5)
        if fragment.first == " " { fragment = fragment.dropFirst() }
        eventBytes += fragment.utf8.count + 1
        guard eventBytes <= 2_097_152 else {
          throw DictationError.invalid("Dictation event exceeds 2 MiB.")
        }
        data.append(String(fragment))
      }
    } else {
      guard line.count < 2_097_152 else {
        throw DictationError.invalid("Dictation event line exceeds 2 MiB.")
      }
      line.append(byte)
    }
    return nil
  }
}
