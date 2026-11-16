// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

/// A cheap COW snapshot. Encoding is deferred until an upload, retry, or archive needs it.
public struct PCMRecording: Sendable {
  public let samples: Data
  public let sampleRate: Double
  public init(samples: Data, sampleRate: Double) {
    self.samples = samples
    self.sampleRate = sampleRate
  }
  public func encoded() async throws -> Data {
    let worker = Task.detached(priority: .userInitiated) { try wav() }
    let data = try await withTaskCancellationHandler {
      try await worker.value
    } onCancel: {
      worker.cancel()
    }
    try Task.checkCancellation()
    return data
  }
  public func wav() throws -> Data {
    guard samples.count % 4 == 0, sampleRate.isFinite, (8000...192000).contains(sampleRate),
      samples.count <= 120 * 1024 * 1024
    else {
      throw DictationError.invalid("Invalid or oversized PCM recording.")
    }
    var data = Data()
    func ascii(_ text: String) { data.append(contentsOf: text.utf8) }
    func u16(_ value: UInt16) {
      var v = value.littleEndian
      withUnsafeBytes(of: &v) { data.append(contentsOf: $0) }
    }
    func u32(_ value: UInt32) {
      var v = value.littleEndian
      withUnsafeBytes(of: &v) { data.append(contentsOf: $0) }
    }
    var pcm = Data(capacity: samples.count / 2)
    try samples.withUnsafeBytes { raw in
      for (index, value) in raw.bindMemory(to: Float.self).enumerated() {
        if index % 4096 == 0 { try Task.checkCancellation() }
        let bounded = value.isFinite ? max(-1, min(1, value)) : 0
        var encoded = Int16((bounded * 32767).rounded()).littleEndian
        withUnsafeBytes(of: &encoded) { pcm.append(contentsOf: $0) }
      }
    }
    ascii("RIFF")
    u32(UInt32(pcm.count + 36))
    ascii("WAVEfmt ")
    u32(16)
    u16(1)
    u16(1)
    u32(UInt32(sampleRate))
    u32(UInt32(sampleRate) * 2)
    u16(2)
    u16(16)
    ascii("data")
    u32(UInt32(pcm.count))
    data.append(pcm)
    return data
  }
}
