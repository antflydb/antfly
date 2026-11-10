// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import Foundation

/// Per-recording cleanup state. Keep recent output editable so a correction in
/// the next batch can retract words from the preceding batch. Rewind the latest
/// input window when a live recognition prefix grows; older output is reused.
/// Reuse only within one recording with the same preset, dictionary and options.
public actor CleanupContext {
  nonisolated let namespace = UUID()
  static let editableBytes = 1024
  static let newInputBytes = VoiceFormatting.Plan.maxCleanupBatchBytes - editableBytes
  private struct Snapshot {
    var outputCount: Int
    var tail: String
    var inputBytes: Int
  }
  private var source = ""
  private var output: [String] = []
  private var tail = ""
  private var rewind: Snapshot?
  private var busy = false
  public init() {}

  func process(
    _ plan: VoiceFormatting.Plan,
    clean: @Sendable (String) async throws -> String
  ) async throws -> String {
    guard !busy else { throw DictationError.invalid("Cleanup context is already in use.") }
    busy = true
    defer { busy = false }
    do {
      try Task.checkCancellation()
      if source == plan.input, rewind != nil { return output.joined() + tail }
      var snapshot = Snapshot(outputCount: 0, tail: "", inputBytes: 0)
      if let rewind,
        plan.input.utf8.starts(with: source.utf8.prefix(rewind.inputBytes))
      {
        snapshot = rewind
      }
      output.removeSubrange(snapshot.outputCount..<output.count)
      tail = snapshot.tail
      let start = plan.input.utf8.index(plan.input.utf8.startIndex, offsetBy: snapshot.inputBytes)
      guard let index = String.Index(start, within: plan.input) else {
        throw DictationError.invalid("Invalid cleanup context boundary.")
      }
      let suffix = String(plan.input[index...])
      let limit =
        plan.input.utf8.count <= VoiceFormatting.Plan.maxCleanupBatchBytes
        ? VoiceFormatting.Plan.maxCleanupBatchBytes : Self.newInputBytes
      let batches = try plan.partition(suffix, maxBytes: limit)
      var inputBytes = snapshot.inputBytes
      for batch in batches {
        try Task.checkCancellation()
        let before = Snapshot(outputCount: output.count, tail: tail, inputBytes: inputBytes)
        let window = tail + batch.text
        let parts = try plan.partition(window, maxBytes: VoiceFormatting.Plan.maxCleanupBatchBytes)
        guard parts.count == 1 else {
          throw DictationError.invalid("Cleanup context exceeds capacity.")
        }
        let cleaned = parts[0].needsCleanup ? try await clean(window) : window
        try Task.checkCancellation()
        let revised = try plan.partition(cleaned, maxBytes: Self.editableBytes, fromEnd: true)
        output.append(contentsOf: revised.dropLast().map(\.text))
        tail = revised.last?.text ?? ""
        inputBytes += batch.text.utf8.count
        snapshot = before
      }
      let result = output.joined() + tail
      // A failed window cannot poison a later retry or bypass marker validation.
      _ = try plan.restore(result)
      source = plan.input
      rewind = snapshot
      return result
    } catch {
      source = ""
      output = []
      tail = ""
      rewind = nil
      throw error
    }
  }
}
