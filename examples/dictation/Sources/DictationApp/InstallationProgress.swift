// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit

/// The modal window pumps AppKit events while a worker owns all installer I/O.
/// Cancellation does not dismiss it until the worker has stopped and cleaned up.
@MainActor final class InstallationProgress: NSObject {
  private let work = InstallationWork()
  private let label = NSTextField(labelWithString: "Preparing…")
  private let button = NSButton(title: "Cancel", target: nil, action: nil)
  @objc private func cancel() {
    work.cancel()
    button.isEnabled = false
    label.stringValue = "Canceling…"
  }
  static func run(
    title: String, cancellable: Bool = true,
    operation: @escaping @Sendable (InstallationWork) throws -> URL
  ) throws -> URL {
    let controller = InstallationProgress()
    let window = NSPanel(
      contentRect: NSRect(x: 0, y: 0, width: 420, height: 130),
      styleMask: [.titled], backing: .buffered, defer: false)
    window.title = title
    let spinner = NSProgressIndicator()
    spinner.style = .spinning
    spinner.startAnimation(nil)
    let content = NSStackView(views: [spinner, controller.label, controller.button])
    content.orientation = .vertical
    content.spacing = 12
    content.translatesAutoresizingMaskIntoConstraints = false
    window.contentView!.addSubview(content)
    NSLayoutConstraint.activate([
      content.centerXAnchor.constraint(equalTo: window.contentView!.centerXAnchor),
      content.centerYAnchor.constraint(equalTo: window.contentView!.centerYAnchor),
      content.widthAnchor.constraint(lessThanOrEqualToConstant: 390),
    ])
    controller.button.target = controller
    controller.button.action = #selector(cancel)
    controller.button.isHidden = !cancellable
    let work = controller.work
    DispatchQueue.global(qos: .userInitiated).async {
      work.finish(Result { try operation(work) })
    }
    // A modal-mode timer also works before NSApplication.run during first launch.
    let timer = Timer(timeInterval: 0.05, repeats: true) { _ in
      MainActor.assumeIsolated {
        if work.result != nil {
          NSApp.stopModal()
        } else if !work.isCanceled {
          controller.label.stringValue = work.stage
        }
      }
    }
    RunLoop.main.add(timer, forMode: .modalPanel)
    window.center()
    NSApp.runModal(for: window)
    timer.invalidate()
    window.orderOut(nil)
    guard let result = work.result else { throw CancellationError() }
    return try result.get()
  }
}

/// Owns subprocess cancellation and the publication boundary under one lock.
final class InstallationWork: @unchecked Sendable {
  private let lock = NSLock()
  private var canceled = false
  private var published = false
  private var process: Process?
  private var outcome: Result<URL, Error>?
  private var message = "Preparing…"
  var isCanceled: Bool { lock.withLock { canceled } }
  var hasRunningProcess: Bool { lock.withLock { process?.isRunning == true } }
  var result: Result<URL, Error>? { lock.withLock { outcome } }
  var stage: String { lock.withLock { message } }
  func stage(_ value: String) { lock.withLock { message = value } }
  func checkCancellation() throws {
    if isCanceled { throw CancellationError() }
  }
  func cancel() {
    lock.withLock {
      guard !published, outcome == nil else { return }
      canceled = true
      if let process, process.isRunning { process.terminate() }
    }
  }
  func finish(_ result: Result<URL, Error>) {
    lock.withLock { outcome = canceled && !published ? .failure(CancellationError()) : result }
  }
  func publish(_ operation: () throws -> Void) throws {
    try lock.withLock {
      guard !canceled else { throw CancellationError() }
      try operation()
      published = true
    }
  }
  func run(_ executable: String, arguments: [String]) throws {
    let child = Process()
    child.executableURL = URL(fileURLWithPath: executable)
    child.arguments = arguments
    child.standardOutput = FileHandle.nullDevice
    child.standardError = FileHandle.nullDevice
    try lock.withLock {
      guard !canceled else { throw CancellationError() }
      try child.run()
      process = child
    }
    child.waitUntilExit()  // worker only; cancel() terminates this exact child
    lock.withLock { process = nil }
    try checkCancellation()
    guard child.terminationReason == .exit, child.terminationStatus == 0 else {
      throw InstallationError.command(URL(fileURLWithPath: executable).lastPathComponent)
    }
  }
}
