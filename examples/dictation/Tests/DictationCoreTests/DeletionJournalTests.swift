// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation
import Testing

@testable import DictationCore

@Test func deletionJournalPersistsTargetsAcknowledgmentsAndUnfinishedLocalCleanup() async throws {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: directory) }
  let file = directory.appendingPathComponent("deletions.json")
  let a = HistoryDestination(server: "https://a.example", table: "dictation")
  let b = HistoryDestination(server: "https://b.example", table: "dictation")
  let journal = try HistoryDeletionJournal(file: file)
  try await journal.begin([a: ["entry"], b: ["entry"]])
  try await journal.acknowledge(a, ids: ["entry"])
  let recovered = try HistoryDeletionJournal(file: file)
  #expect(recovered.recoveredIDs == ["entry"])
  #expect(await recovered.pending() == [b: ["entry"]])
  // Repeated preparation must not resurrect an already acknowledged target.
  try await recovered.begin([a: ["entry"], b: ["entry"]])
  #expect(await recovered.pending() == [b: ["entry"]])
  do {
    try await recovered.complete(["entry"])
    Issue.record("Forgot an unacknowledged destination")
  } catch is DictationError {}
  try await recovered.acknowledge(b, ids: ["entry"])
  let beforeLocalCommit = try HistoryDeletionJournal(file: file)
  #expect(beforeLocalCommit.recoveredIDs == ["entry"])
  #expect(await beforeLocalCommit.pending().isEmpty)
  try await beforeLocalCommit.complete(["entry"])
  #expect(!FileManager.default.fileExists(atPath: file.path))
  #expect(try HistoryDeletionJournal(file: file).recoveredIDs.isEmpty)
}
