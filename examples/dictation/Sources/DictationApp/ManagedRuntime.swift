// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import Darwin
import DictationCore
import Foundation
import Security

/// The app owns one supervisor, which owns exactly one Antfly child. Closing the
/// control pipe (including on app crash) stops that child; no PID scanning or
/// process-name based termination is used.
@MainActor final class ManagedRuntime: ObservableObject {
  nonisolated static let credentialAccount = "managed-runtime-admin-v1"
  private(set) var password = ""
  var supportsAppleSpeech: Bool {
    guard let root = Bundle.main.resourceURL,
      let data = try? Data(contentsOf: root.appendingPathComponent("Runtime/release.json")),
      let metadata = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
    else { return false }
    if #available(macOS 26, *) { return metadata["apple_providers"] as? Bool == true }
    return false
  }
  nonisolated static let origin = Configuration.managedServerOrigin
  @Published private(set) var status = "Local engine stopped."
  @Published private(set) var ready = false
  @Published private(set) var busy = false
  private let directory: URL
  private let runtimeOverride: URL?
  private let launcherOverride: URL?
  private let credentialRead: @MainActor () -> String
  private let credentialWrite: @MainActor (String) throws -> Void
  private var process: Process?
  private var control: Pipe?
  private var startup: Task<Void, Never>?
  private var generation = UUID()
  private var desired = false
  private var failures = 0
  private var download: Process?
  private var downloadControl: Pipe?
  var changed: (@MainActor () -> Void)?
  var logURL: URL { directory.appendingPathComponent("runtime.log") }
  private var runtime: URL? {
    runtimeOverride ?? Bundle.main.resourceURL?.appendingPathComponent("Runtime/antfly")
  }
  init(
    directory: URL, runtime: URL? = nil, launcher: URL? = nil,
    credentialRead: @escaping @MainActor () -> String = {
      Credentials.read(server: ManagedRuntime.credentialAccount)
    },
    credentialWrite: @escaping @MainActor (String) throws -> Void = {
      try Credentials.save($0, server: ManagedRuntime.credentialAccount)
    }
  ) {
    self.directory = directory.appendingPathComponent("Runtime")
    runtimeOverride = runtime
    launcherOverride = launcher
    self.credentialRead = credentialRead
    self.credentialWrite = credentialWrite
  }

  func start(resetFailures: Bool = true) {
    desired = true
    guard process == nil, startup == nil else { return }
    if resetFailures { failures = 0 }
    let revision = UUID()
    generation = revision
    ready = false
    busy = true
    status = "Starting local engine…"
    startup = Task { [self] in
      defer {
        if generation == revision {
          startup = nil
          busy = false
        }
      }
      do {
        guard let runtime, FileManager.default.isExecutableFile(atPath: runtime.path),
          let launcher = launcherOverride ?? Bundle.main.executableURL
        else {
          throw DictationError.invalid(
            "Bundled Antfly is missing. Install an official app, or build with BUNDLE_RUNTIME=1.")
        }
        try FileManager.default.createDirectory(
          at: directory, withIntermediateDirectories: true,
          attributes: [.posixPermissions: 0o700])
        password = credentialRead()
        if password.isEmpty {
          guard
            !FileManager.default.fileExists(atPath: directory.appendingPathComponent("data").path)
          else {
            throw DictationError.invalid(
              "The local engine’s Keychain credential is unavailable. Unlock Keychain or restore the original credential; engine data has been preserved."
            )
          }
          var bytes = [UInt8](repeating: 0, count: 32)
          guard SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes) == errSecSuccess else {
            throw DictationError.invalid("Could not generate the local engine credential.")
          }
          password = Data(bytes).base64EncodedString()
          try credentialWrite(password)
        }
        var config: [String: Any] = [
          "enable_auth": true,
          "public_inference": ["enabled": true],
          "transcribers": [
            "whisper": ["provider": "antfly", "model": "openai/whisper-tiny"],
            "apple-speech": [
              "provider": "apple", "language_code": "en-US",
              "download_assets": FileManager.default.fileExists(
                atPath: directory.appendingPathComponent("apple-assets-enabled").path),
            ],
          ],
          "generators": ["cleanup": ["provider": "apple", "max_tokens": 1024]],
        ]
        if !supportsAppleSpeech {
          config["transcribers"] = [
            "whisper": ["provider": "antfly", "model": "openai/whisper-tiny"]
          ]
          config.removeValue(forKey: "generators")
        }
        try JSONSerialization.data(withJSONObject: config, options: [.sortedKeys])
          .write(to: directory.appendingPathComponent("antfly.json"), options: .atomic)
        let pipe = Pipe()
        let child = Process()
        child.executableURL = launcher
        child.arguments = ["--managed-runtime", runtime.path, directory.path]
        child.standardInput = pipe
        child.standardOutput = FileHandle.nullDevice
        child.standardError = FileHandle.nullDevice
        child.terminationHandler = { [weak self] child in
          let code = child.terminationStatus
          Task { @MainActor in self?.exited(revision: revision, code: code) }
        }
        _ = fcntl(pipe.fileHandleForWriting.fileDescriptor, F_SETNOSIGPIPE, 1)
        try child.run()
        process = child
        control = pipe
        // Only the parent write end must keep the supervisor alive.
        try pipe.fileHandleForReading.close()
        try pipe.fileHandleForWriting.write(contentsOf: Data((password + "\n").utf8))
        let settings = URLSessionConfiguration.ephemeral
        settings.timeoutIntervalForRequest = 1
        settings.timeoutIntervalForResource = 2
        let session = URLSession(configuration: settings)
        defer { session.invalidateAndCancel() }
        let deadline = ContinuousClock.now.advanced(by: .seconds(60))
        for _ in 0..<120 {
          if ContinuousClock.now >= deadline { break }
          try Task.checkCancellation()
          guard generation == revision, child.isRunning else {
            throw DictationError.invalid("Local engine exited. See runtime log.")
          }
          var request = URLRequest(url: URL(string: Self.origin + "/ai/v1/models")!)
          request.setValue(
            "Basic " + Data(("admin:" + password).utf8).base64EncodedString(),
            forHTTPHeaderField: "Authorization")
          if let (_, response) = try? await session.data(for: request),
            (response as? HTTPURLResponse)?.statusCode == 200, child.isRunning
          {
            ready = true
            status =
              "Local engine ready. Discover models to check recognition and cleanup readiness."
            changed?()
            return
          }
          try await Task.sleep(for: .milliseconds(500))
        }
        throw DictationError.invalid(
          "Local engine did not become ready within one minute. See runtime log.")
      } catch {
        guard generation == revision, !Task.isCancelled else { return }
        status = error.localizedDescription
        desired = false
        try? control?.fileHandleForWriting.close()
      }
    }
  }
  private func exited(revision: UUID, code: Int32) {
    guard revision == generation else { return }
    process = nil
    control = nil
    ready = false
    guard desired else { return }
    startup?.cancel()
    generation = UUID()
    let restartRevision = generation
    failures += 1
    guard failures <= 3 else {
      startup = nil
      busy = false
      status =
        "Local engine stopped after repeated failures (exit \(code)). Open its log, then retry."
      changed?()
      return
    }
    status = "Local engine exited (\(code)); restarting (\(failures)/3)…"
    startup = Task { [self] in
      try? await Task.sleep(for: .seconds(1 << failures))
      guard !Task.isCancelled, desired, generation == restartRevision else { return }
      startup = nil
      start(resetFailures: false)
    }
    changed?()
  }
  func stop() async {
    desired = false
    generation = UUID()
    startup?.cancel()
    startup = nil
    ready = false
    busy = true
    try? downloadControl?.fileHandleForWriting.close()
    if let download { await Task.detached { download.waitUntilExit() }.value }
    download = nil
    downloadControl = nil
    try? control?.fileHandleForWriting.close()
    let child = process
    // Supervisor enforces its own ten-second termination deadline.
    if let child { await Task.detached { child.waitUntilExit() }.value }
    process = nil
    control = nil
    busy = false
    status = "Local engine stopped."
  }
  func ensureReady(timeout: Duration = .seconds(90)) async throws {
    try Task.checkCancellation()
    if !ready && !busy {
      // A failed startup may still own a child awaiting termination. Drain it
      // before starting a fresh attempt rather than waiting on a stopped engine.
      await stop()
      try Task.checkCancellation()
      start()
    }
    try await waitUntilReady(timeout: timeout)
  }
  func waitUntilReady(timeout: Duration = .seconds(90)) async throws {
    let deadline = ContinuousClock.now.advanced(by: timeout)
    while !ready {
      try Task.checkCancellation()
      guard busy else { throw DictationError.invalid(status) }
      guard ContinuousClock.now < deadline else {
        throw DictationError.invalid(
          "The local engine did not start in time. Retry setup or open the engine log.")
      }
      try await Task.sleep(for: .milliseconds(200))
    }
    try Task.checkCancellation()
  }
  func cancelDownload() { try? downloadControl?.fileHandleForWriting.close() }
  func enableAppleAssets() async throws {
    try Data().write(to: directory.appendingPathComponent("apple-assets-enabled"), options: .atomic)
    await stop()
    try Task.checkCancellation()
    start()
  }
  func installWhisper() async throws {
    guard ready, !busy, let runtime else {
      throw DictationError.invalid("Start the local engine before downloading a model.")
    }
    busy = true
    defer {
      if status == "Downloading Whisper Tiny…" {
        status = Task.isCancelled ? "Model download canceled." : "Model download stopped."
      }
      if startup == nil { busy = false }
      download = nil
      try? downloadControl?.fileHandleForWriting.close()
      downloadControl = nil
    }
    status = "Downloading Whisper Tiny…"
    let child = Process()
    guard let launcher = launcherOverride ?? Bundle.main.executableURL else {
      throw DictationError.invalid("App launcher missing.")
    }
    let pipe = Pipe()
    downloadControl = pipe
    child.executableURL = launcher
    child.arguments = ["--managed-runtime-pull", runtime.path, directory.path]
    child.standardInput = pipe
    child.standardOutput = FileHandle.nullDevice
    child.standardError = FileHandle.nullDevice
    download = child
    try child.run()
    try pipe.fileHandleForReading.close()
    while child.isRunning {
      if Task.isCancelled { try? pipe.fileHandleForWriting.close() }
      // Keep ownership until the supervisor has reaped its download process.
      await Task.detached { try? await Task.sleep(for: .milliseconds(200)) }.value
    }
    try Task.checkCancellation()
    guard child.terminationStatus == 0 else {
      status = "Whisper download failed. Check the connection and available disk space, then retry."
      throw DictationError.invalid(status)
    }
    status = "Whisper downloaded. Restarting the local engine…"
    await stop()
    try Task.checkCancellation()
    start()
  }
}

/// Runs before SwiftUI initialization. A second app cannot share the data
/// directory, and a busy port never causes us to attach to another server.
func superviseManagedRuntime(arguments: [String]) -> Int32 {
  guard arguments.count == 4 else { return 64 }
  let pulling = arguments[1] == "--managed-runtime-pull"
  let runtime = arguments[2]
  let root = URL(fileURLWithPath: arguments[3], isDirectory: true)
  let lock = open(
    root.appendingPathComponent(pulling ? "download.lock" : "owner.lock").path, O_CREAT | O_RDWR,
    0o600)
  guard lock >= 0 else { return 73 }
  defer { close(lock) }
  guard flock(lock, LOCK_EX | LOCK_NB) == 0 else { return 75 }
  let logger = RuntimeLog(root.appendingPathComponent(pulling ? "download.log" : "runtime.log"))
  for port: UInt16 in (pulling ? [] : [47831, 47832]) {
    let socket = Darwin.socket(AF_INET, SOCK_STREAM, 0)
    guard socket >= 0 else { return 71 }
    // Match the server listener's reuse policy so a normal restart is not
    // blocked by completed HTTP connections still in TCP TIME_WAIT.
    var reuse: Int32 = 1
    _ = withUnsafePointer(to: &reuse) {
      setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, $0, socklen_t(MemoryLayout<Int32>.size))
    }
    var address = sockaddr_in()
    address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
    address.sin_family = sa_family_t(AF_INET)
    address.sin_port = port.bigEndian
    address.sin_addr.s_addr = inet_addr("127.0.0.1")
    let bound = withUnsafePointer(to: &address) {
      $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
        Darwin.bind(socket, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
      }
    }
    close(socket)
    guard bound == 0 else {
      logger.write(
        Data(
          "Local engine port \(port) is occupied. Quit the other Dictation copy or use an external server.\n"
            .utf8))
      return 78
    }
  }
  var bootstrapPassword = Data()
  if !pulling {
    // Read a bounded credential from the private pipe, never arguments or disk.
    while bootstrapPassword.count <= 72 {
      let byte = FileHandle.standardInput.readData(ofLength: 1)
      if byte == Data([10]) { break }
      if byte.isEmpty { return 77 }
      bootstrapPassword.append(byte)
    }
    guard (12...72).contains(bootstrapPassword.count) else { return 77 }
  }
  let child = Process()
  child.executableURL = Bundle.main.executableURL
  child.arguments = [
    "standalone", "--host", "127.0.0.1", "--port", "47831", "--health-port", "47832",
    "--config", root.appendingPathComponent("antfly.json").path,
    "--models-dir", root.appendingPathComponent("models").path,
    "--data-dir", root.appendingPathComponent("data").path,
  ]
  if pulling {
    child.arguments = [
      "inference", "pull", "openai/whisper-tiny", "--tasks", "transcribe",
      "--models-dir", root.appendingPathComponent("models").path,
    ]
  }
  child.arguments = ["--managed-runtime-child", runtime] + (child.arguments ?? [])
  // Never inherit developer DYLD overrides or provider credentials.
  child.environment = [
    "HOME": NSHomeDirectory(), "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
    "TMPDIR": NSTemporaryDirectory(),
  ]
  if !pulling {
    child.environment?["ANTFLY_BOOTSTRAP_ADMIN_PASSWORD"] = String(
      decoding: bootstrapPassword, as: UTF8.self)
  }
  let output = Pipe()
  child.standardOutput = output
  child.standardError = output
  output.fileHandleForReading.readabilityHandler = { handle in
    let data = handle.availableData
    if data.isEmpty { handle.readabilityHandler = nil } else { logger.write(data) }
  }
  do { try child.run() } catch {
    logger.write(Data((error.localizedDescription + "\n").utf8))
    return 70
  }
  // The exec trampoline puts Antfly and any inference workers in a new
  // process group before exec. Never signal the app's or another server's group.
  while child.isRunning {
    var descriptor = pollfd(fd: STDIN_FILENO, events: Int16(POLLIN | POLLHUP), revents: 0)
    let result = poll(&descriptor, 1, 200)
    if result < 0 && errno != EINTR { break }
    if result > 0 {
      if descriptor.revents & Int16(POLLHUP | POLLERR) != 0 { break }
      if FileHandle.standardInput.readData(ofLength: 4096).isEmpty { break }
    }
  }
  let group = -child.processIdentifier
  if kill(group, SIGTERM) != 0, child.isRunning { child.terminate() }
  // Scheduler delays must not turn the ten-second grace period into an
  // unbounded count of coalesced sleeps on a busy host.
  let deadline = ContinuousClock.now.advanced(by: .seconds(10))
  while kill(group, 0) == 0 && ContinuousClock.now < deadline {
    Thread.sleep(forTimeInterval: 0.05)
  }
  if kill(group, 0) == 0 { kill(group, SIGKILL) }
  child.waitUntilExit()
  // Foundation can publish termination before its asynchronous child reaper
  // finishes. Reap our direct child synchronously, or accept ECHILD if it won.
  var waitStatus: Int32 = 0
  while waitpid(child.processIdentifier, &waitStatus, 0) < 0 && errno == EINTR {}
  output.fileHandleForReading.readabilityHandler = nil
  return child.terminationStatus
}

/// No application state is initialized in this mode. exec preserves the child's
/// PID as its process-group identity, including for a runtime that forks workers.
func executeManagedRuntimeChild(arguments: [String]) -> Never {
  guard arguments.count >= 3, getpgrp() == getpid() || setpgid(0, 0) == 0 else { exit(71) }
  var values = arguments.dropFirst(2).map { strdup($0) }
  values.append(nil)
  values.withUnsafeMutableBufferPointer { buffer in
    _ = execv(arguments[2], buffer.baseAddress!)
  }
  exit(70)
}

private final class RuntimeLog: @unchecked Sendable {
  private let lock = NSLock()
  private var handle: FileHandle?
  private var bytes = 0
  init(_ url: URL) {
    if !FileManager.default.fileExists(atPath: url.path) {
      _ = FileManager.default.createFile(
        atPath: url.path, contents: nil, attributes: [.posixPermissions: 0o600])
    }
    handle = try? FileHandle(forWritingTo: url)
    bytes = Int((try? handle?.seekToEnd()) ?? 0)
  }
  deinit { try? handle?.close() }
  func write(_ data: Data) {
    lock.lock()
    defer { lock.unlock() }
    // Append in O(chunk size), rotating one bounded log instead of rewriting
    // the entire buffer for every engine message.
    let chunk = Data(data.suffix(1_048_576))
    if bytes + chunk.count > 1_048_576 {
      try? handle?.truncate(atOffset: 0)
      try? handle?.seek(toOffset: 0)
      bytes = 0
    }
    try? handle?.write(contentsOf: chunk)
    bytes += chunk.count
  }
}
