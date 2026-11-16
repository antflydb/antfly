// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public enum RecordingArchive {
  /// Remove uncommitted candidates left by a crash, never the retained recording.
  public static func removeStaleCandidates(in directory: URL) throws {
    let prefix = ".recording-"
    for file in try FileManager.default.contentsOfDirectory(
      at: directory, includingPropertiesForKeys: nil)
    {
      let name = file.lastPathComponent
      guard name.hasPrefix(prefix), name.hasSuffix(".wav"),
        UUID(uuidString: String(name.dropFirst(prefix.count).dropLast(4))) != nil
      else { continue }
      try FileManager.default.removeItem(at: file)
    }
  }
  /// Write a unique candidate off the UI actor. The caller commits its rename
  /// only after checking that the recording is still current, and always removes
  /// the candidate afterward. Cancellation can never publish stale audio.
  public static func stage(_ data: Data, in directory: URL) async throws -> URL {
    let file = directory.appendingPathComponent(".recording-" + UUID().uuidString + ".wav")
    let worker = Task.detached(priority: .utility) {
      try Task.checkCancellation()
      try data.write(to: file, options: .atomic)
      try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
      try Task.checkCancellation()
    }
    do {
      try await withTaskCancellationHandler {
        try await worker.value
      } onCancel: {
        worker.cancel()
      }
      try Task.checkCancellation()
      return file
    } catch {
      try? FileManager.default.removeItem(at: file)
      throw error
    }
  }
}
