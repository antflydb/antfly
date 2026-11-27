// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import DictationCore

/// Installation uses the running bundle, including an App Translocation mount.
/// No private translocation API or access to the original Downloads path is needed.
@MainActor enum AppInstallation {
  static let declinedKey = "AntflyInstallationPromptDeclined"
  static var applicationsDirectories: [URL] {
    FileManager.default.urls(for: .applicationDirectory, in: .localDomainMask)
      + FileManager.default.urls(for: .applicationDirectory, in: .userDomainMask)
  }
  static var installed: Bool {
    InstallationLocation.isInstalled(Bundle.main.bundleURL, directories: applicationsDirectories)
  }
  static var available: Bool {
    !installed
      && UpdateConfiguration(info: Bundle.main.infoDictionary ?? [:], installed: true) != nil
  }

  /// Called before SwiftUI constructs Store, so the original copy starts no engine
  /// and requests no permissions when the user elects to install.
  static func offerAtLaunch(defaults: UserDefaults = .standard) -> Bool {
    guard available, !defaults.bool(forKey: declinedKey) else { return false }
    NSApplication.shared.setActivationPolicy(.regular)
    NSApp.activate(ignoringOtherApps: true)
    let alert = NSAlert()
    alert.messageText = "Move Antfly Dictation to Applications?"
    alert.informativeText =
      "Installing keeps the app in one place and enables automatic updates. Your settings will be preserved."
    alert.addButton(withTitle: "Move to Applications")
    alert.addButton(withTitle: "Not Now")
    guard alert.runModal() == .alertFirstButtonReturn else {
      defaults.set(true, forKey: declinedKey)
      return false
    }
    guard let target = prepareInstallation() else { return false }
    return relaunch(target)
  }

  static func prepareInstallation() -> URL? {
    do {
      let source = Bundle.main.bundleURL
      let directories = applicationsDirectories
      // Prefer an existing installation, including ~/Applications. Never replace
      // it: the user can update that copy through Sparkle without downgrading it.
      let existing = directories.map { $0.appendingPathComponent("Antfly Dictation.app") }
        .first { FileManager.default.fileExists(atPath: $0.path) }
      let target: URL
      if let existing {
        try InstallationLocation.requireInstalled(existing, directories: directories)
        _ = try InstallationProgress.run(title: "Checking installed app") { work in
          try SignedApplicationInstaller.verify(existing, work: work)
          return existing
        }
        let alert = NSAlert()
        alert.messageText = "Antfly Dictation is already installed"
        alert.informativeText =
          "Open the installed copy and use Check for Updates to update it. This copy will stay where it is."
        alert.addButton(withTitle: "Open Installed App")
        alert.addButton(withTitle: "Cancel")
        guard alert.runModal() == .alertFirstButtonReturn else { return nil }
        target = existing
      } else {
        guard
          let directory = directories.first(where: {
            FileManager.default.isWritableFile(atPath: $0.path)
          }) ?? directories.last
        else { throw InstallationError.noDestination }
        target = try InstallationProgress.run(title: "Installing Antfly Dictation") { work in
          try SignedApplicationInstaller.copy(source: source, directory: directory, work: work)
        }
      }
      return target
    } catch is CancellationError {
      return nil
    } catch {
      let alert = NSAlert()
      alert.alertStyle = .warning
      alert.messageText = "Antfly Dictation could not be installed"
      alert.informativeText =
        error.localizedDescription
        + "\n\nYou can also quit the app, drag it to Applications in Finder, and open the installed copy."
      alert.addButton(withTitle: "Continue Here")
      alert.addButton(withTitle: "Show Applications in Finder")
      if alert.runModal() == .alertSecondButtonReturn,
        let directory = applicationsDirectories.first
      {
        NSWorkspace.shared.open(directory)
      }
      return nil
    }
  }

  static func relaunch(_ target: URL) -> Bool {
    do {
      try InstallationLocation.requireInstalled(target, directories: applicationsDirectories)
      if let running = NSWorkspace.shared.runningApplications.first(where: {
        $0.processIdentifier != ProcessInfo.processInfo.processIdentifier
          && $0.bundleURL?.resolvingSymlinksInPath().standardizedFileURL.path
            == target.resolvingSymlinksInPath().standardizedFileURL.path
      }) {
        running.activate(options: [])
      } else {
        _ = try InstallationProgress.run(title: "Opening installed app", cancellable: false) {
          work in
          try work.run("/usr/bin/open", arguments: ["-n", target.path])
          return target
        }
      }
      return true
    } catch {
      let alert = NSAlert()
      alert.messageText = "Open the installed app in Applications"
      alert.informativeText =
        "Installation succeeded, but the installed copy could not be opened automatically. Quit this copy and open it from Applications."
      alert.runModal()
      NSWorkspace.shared.activateFileViewerSelecting([target])
      return false
    }
  }
}

enum InstallationLocation {
  static func requireInstalled(_ bundle: URL, directories: [URL]) throws {
    guard isInstalled(bundle, directories: directories) else {
      throw InstallationError.externalLink(bundle)
    }
  }
  static func isInstalled(_ bundle: URL, directories: [URL]) -> Bool {
    let parent = bundle.resolvingSymlinksInPath().deletingLastPathComponent().standardizedFileURL
    return directories.contains {
      parent.path == $0.resolvingSymlinksInPath().standardizedFileURL.path
    }
  }
}

enum InstallationError: LocalizedError {
  case externalLink(URL)
  case noDestination
  case command(String)
  var errorDescription: String? {
    switch self {
    case .externalLink(let url):
      return
        "The item at \(url.path) links to an app outside Applications. Remove that link in Finder, then install the actual app bundle."
    case .noDestination: return "No writable Applications folder is available."
    case .command(let name):
      return "The \(name) operation failed. The existing installation has not been replaced."
    }
  }
}

enum SignedApplicationInstaller {
  private static let requirement =
    "anchor apple generic and certificate leaf[subject.OU] = \"8Z2RNB767D\" and identifier \"dev.antfly.dictation\""
  static func verify(_ bundle: URL, work: InstallationWork = InstallationWork()) throws {
    try work.run(
      "/usr/bin/codesign",
      arguments: ["--verify", "--deep", "--strict", "-R", "=" + requirement, bundle.path])
  }
  static func copy(
    source: URL, directory: URL, work: InstallationWork = InstallationWork(),
    verifyBundle: ((URL) throws -> Void)? = nil
  ) throws -> URL {
    let manager = FileManager.default
    let verifyBundle = verifyBundle ?? { try verify($0, work: work) }
    work.stage("Checking app signature…")
    try work.checkCancellation()
    try verifyBundle(source)
    try work.checkCancellation()
    try manager.createDirectory(at: directory, withIntermediateDirectories: true)
    let staging = directory.appendingPathComponent(".antfly-install-" + UUID().uuidString)
    try manager.createDirectory(at: staging, withIntermediateDirectories: false)
    defer { try? manager.removeItem(at: staging) }
    let staged = staging.appendingPathComponent("Antfly Dictation.app")
    work.stage("Copying app…")
    try work.run(
      "/usr/bin/ditto", arguments: ["--rsrc", "--extattr", "--acl", source.path, staged.path])
    work.stage("Verifying installed copy…")
    try work.checkCancellation()
    try verifyBundle(staged)
    try work.checkCancellation()
    let destination = directory.appendingPathComponent("Antfly Dictation.app")
    // moveItem fails if another installer won the race. It never replaces a bundle.
    try work.publish { try manager.moveItem(at: staged, to: destination) }
    return destination
  }
}
