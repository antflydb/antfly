// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public struct HistoryDestination: Codable, Hashable, Sendable {
  public var server: String
  public var table: String
  public init(server: String, table: String) {
    self.server = server
    self.table = table
  }
}

/// A durable, bounded, single-writer outbox. Each job keeps its original destination
/// and resolves credentials at dispatch time. Disk/network work never runs on the UI actor.
public actor HistoryOutbox {
  public struct Status: Sendable {
    public var pending: Int
    public var error: String?
    public var paused: Bool
  }
  public typealias Upload = @Sendable (HistoryEntry, HistoryDestination) async throws -> Void
  private struct Job: Codable, Sendable {
    var id = UUID().uuidString
    var entry: HistoryEntry
    var destination: HistoryDestination
  }
  private let directory: URL
  private let upload: Upload
  private let sleep: @Sendable (UInt64) async throws -> Void
  private let changed: @Sendable (Status) async -> Void
  private var jobs: [Job]
  private var retainedBytes: Int
  private var jobSizes: [String: Int]
  private var worker: Task<Void, Never>?
  private var uploading = false
  private var enabled: Bool
  private var holds: Set<UUID> = []
  private var metadataHolds: Set<UUID> = []
  private var paused: Bool { !enabled || !holds.isEmpty }
  private var lastError: String?
  private let archive: HistoryArchive
  private var idleWaiters: [CheckedContinuation<Void, Never>] = []
  private let maxJobs = 500
  private let maxBytes = 64 * 1024 * 1024

  public init(
    directory: URL, paused: Bool = false, archive: HistoryArchive? = nil, upload: @escaping Upload,
    sleep: @escaping @Sendable (UInt64) async throws -> Void = {
      try await Task.sleep(nanoseconds: $0)
    },
    changed: @escaping @Sendable (Status) async -> Void = { _ in }
  ) throws {
    self.directory = directory
    self.archive =
      archive
      ?? HistoryArchive(
        file: directory.deletingLastPathComponent().appendingPathComponent("history.json"))
    self.upload = upload
    self.sleep = sleep
    self.changed = changed
    self.enabled = !paused
    try FileManager.default.createDirectory(
      at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
    var loaded: [Job] = []
    var bytes = 0
    var sizes: [String: Int] = [:]
    for file in try FileManager.default.contentsOfDirectory(
      at: directory, includingPropertiesForKeys: [.fileSizeKey]
    ).sorted(by: { $0.lastPathComponent < $1.lastPathComponent }) where file.pathExtension == "json"
    {
      let size = try file.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0
      guard size <= 8 * 1024 * 1024, bytes + size <= maxBytes, loaded.count < maxJobs else {
        throw DictationError.invalid(
          "History outbox exceeds its storage limit. Local transcripts remain available.")
      }
      do {
        let job = try JSONDecoder().decode(Job.self, from: Data(contentsOf: file))
        guard UUID(uuidString: job.id) != nil, file.lastPathComponent == job.id + ".json" else {
          throw DictationError.invalid("Invalid history job.")
        }
        var config = Configuration()
        config.server = job.destination.server
        config.table = job.destination.table
        _ = try config.validated()
        loaded.append(job)
        sizes[job.id] = size
        bytes += size
      } catch {
        // A damaged optional history record must not disable dictation or discard the other jobs.
        try FileManager.default.moveItem(at: file, to: file.appendingPathExtension("invalid"))
      }
    }
    jobs = loaded
    retainedBytes = bytes
    jobSizes = sizes
  }
  public func status() -> Status { Status(pending: jobs.count, error: lastError, paused: paused) }
  /// Pending jobs are authoritative for archives created before destinations
  /// were stored with entries. A transcript may have more than one queued copy.
  public func destinations(for ids: Set<String>) -> [String: Set<HistoryDestination>] {
    var result: [String: Set<HistoryDestination>] = [:]
    for job in jobs where ids.contains(job.entry.id) {
      result[job.entry.id, default: []].insert(job.destination)
    }
    return result
  }
  public func enqueue(_ entry: HistoryEntry, destination: HistoryDestination) async throws {
    var config = Configuration()
    config.server = destination.server
    config.table = destination.table
    _ = try config.validated()
    if jobs.contains(where: { $0.entry.id == entry.id && $0.destination == destination }) {
      start()
      return
    }
    let job = Job(entry: entry, destination: destination)
    let data = try JSONEncoder().encode(job)
    guard jobs.count < maxJobs, data.count <= 8 * 1024 * 1024,
      retainedBytes + data.count <= maxBytes
    else {
      throw DictationError.invalid(
        "History sync queue is full; transcript is saved locally. Retry upload after the server recovers."
      )
    }
    let file = path(job)
    try data.write(to: file, options: .atomic)
    try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
    jobs.append(job)
    retainedBytes += data.count
    jobSizes[job.id] = data.count
    start()
    await notify()
  }
  public func resume(_ token: UUID? = nil) async {
    if let token {
      holds.remove(token)
      metadataHolds.remove(token)
    } else {
      enabled = true
    }
    start()
    await notify()
  }
  public func setEnabled(_ value: Bool) async {
    enabled = value
    if value {
      start()
    } else {
      let active = worker
      if !uploading { active?.cancel() }
      await active?.value
    }
    await notify()
  }
  public struct Barrier: Sendable {
    public var token: UUID
    public var destinations: [String: Set<HistoryDestination>]
  }
  @discardableResult public func pause() async -> UUID {
    let barrier = holdForDeletion(ids: [], preserveDestinations: false)
    await waitForBarrier(barrier.token)
    return barrier.token
  }
  /// Snapshot destinations before awaiting an active upload. Its response may
  /// finish or another retention transition may purge jobs during that await.
  public func holdForDeletion(ids: Set<String>, preserveDestinations: Bool = true) -> Barrier {
    let token = UUID()
    holds.insert(token)
    if preserveDestinations { metadataHolds.insert(token) }
    let snapshot = destinations(for: ids)
    if !uploading { worker?.cancel() }
    return Barrier(token: token, destinations: snapshot)
  }
  public func waitForBarrier(_ token: UUID) async {
    guard holds.contains(token) else { return }
    let active = worker
    await active?.value
    await notify()
  }
  public func pauseForDeletion(ids: Set<String>) async -> Barrier {
    let barrier = holdForDeletion(ids: ids)
    await waitForBarrier(barrier.token)
    return barrier
  }
  /// Call while paused to synchronize explicit history deletion with pending uploads.
  public func discard(ids: Set<String>, destination: HistoryDestination? = nil) async throws {
    guard paused, worker == nil else {
      throw DictationError.invalid("Pause history sync before deleting pending records.")
    }
    let matches: (Job) -> Bool = {
      ids.contains($0.entry.id) && (destination == nil || $0.destination == destination)
    }
    for job in jobs where matches(job) {
      do { try FileManager.default.removeItem(at: path(job)) } catch {
        let failure = error as NSError
        guard failure.domain == NSCocoaErrorDomain, failure.code == NSFileNoSuchFileError else {
          throw error
        }
      }
      // Keep memory consistent after every unlink. A partial disk failure can
      // be retried without getting stuck on an already removed job file.
      retainedBytes -= jobSizes.removeValue(forKey: job.id) ?? 0
      jobs.removeAll { $0.id == job.id }
    }
    if jobs.isEmpty {
      lastError = nil
      finishWaiters()
    }
    await notify()
  }
  public func discardAll() async throws {
    try await discard(ids: Set(jobs.map { $0.entry.id }))
  }
  /// Revision fencing keeps a delayed write from resurrecting locally cleared history.
  public func storeArchive(_ entries: [HistoryEntry], revision: UInt64) async throws {
    try await archive.store(entries, revision: revision)
  }
  public func waitUntilIdle() async {
    if jobs.isEmpty && worker == nil { return }
    await withCheckedContinuation { idleWaiters.append($0) }
  }
  private func path(_ job: Job) -> URL { directory.appendingPathComponent(job.id + ".json") }
  private func start() {
    guard !paused, worker == nil, !jobs.isEmpty else { return }
    worker = Task(priority: .utility) { await self.drain() }
  }
  private func notify() async { await changed(status()) }
  private func finishWaiters() {
    let waiting = idleWaiters
    idleWaiters = []
    for waiter in waiting { waiter.resume() }
  }
  private func drain() async {
    var failures: UInt64 = 0
    defer {
      worker = nil
      if jobs.isEmpty { finishWaiters() } else if !paused { start() }
    }
    while !paused && !Task.isCancelled, let job = jobs.first {
      do {
        uploading = true
        try await upload(job.entry, job.destination)
        uploading = false
        try Task.checkCancellation()
        // A deletion owner may be persisting its intent while this response
        // arrives. Keep the job's destinations durable until that owner discards
        // it. Resuming another hold may replay the same idempotent upsert.
        if !metadataHolds.isEmpty {
          await notify()
          return
        }
        try FileManager.default.removeItem(at: path(job))
        jobs.removeFirst()
        retainedBytes -= jobSizes.removeValue(forKey: job.id) ?? 0
        failures = 0
        lastError = nil
        await notify()
      } catch {
        uploading = false
        if paused || Task.isCancelled { return }
        failures = min(failures + 1, 7)
        lastError = error.localizedDescription
        await notify()
        do { try await sleep(min(300, UInt64(1) << failures) * 1_000_000_000) } catch { return }
      }
    }
  }
}
