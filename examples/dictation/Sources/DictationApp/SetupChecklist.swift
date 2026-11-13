// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import DictationCore
import SwiftUI

struct SetupChecklist: View {
  @ObservedObject var store: Store
  @ObservedObject var runtime: ManagedRuntime
  @StateObject private var presentation = SetupPresentation()
  var body: some View {
    DisclosureGroup(isExpanded: $presentation.expanded) {
      VStack(alignment: .leading, spacing: 8) {
        HStack {
          readiness("Microphone", ready: store.microphoneAllowed)
          Text("Record your voice.").foregroundStyle(.secondary)
          Spacer()
          if !store.microphoneAllowed {
            Button("Allow microphone") { store.requestMicrophone() }
            Button("Open permission settings") { store.openMicrophoneSettings() }
          }
        }
        HStack {
          readiness("Accessibility", ready: store.accessibilityAllowed)
          Text("Use a shortcut across apps and paste text.").foregroundStyle(.secondary)
          Spacer()
          if !store.accessibilityAllowed { Button("Enable access…") { store.setUpShortcut() } }
        }
        HStack {
          readiness("Global shortcut", ready: store.shortcutReady)
          Text(store.config.effectiveShortcut.label)
          Spacer()
          if !store.shortcutReady { Button("Activate shortcut") { store.setUpShortcut() } }
        }
        HStack {
          readiness("Speech engine", ready: speechReady)
          Text(engineStatus).font(.caption).lineLimit(2)
          Spacer()
          Button("Check engine") { store.refreshModels() }.disabled(store.working)
        }
        Text(
          "You can test recording without Accessibility access. To dictate into another app, enable access and focus its text field first."
        )
        .font(.caption).foregroundStyle(.secondary)
      }.disabled(store.state != .idle || store.working)
    } label: {
      Label("Getting ready", systemImage: "checklist")
    }
    .padding(10).background(.quaternary, in: RoundedRectangle(cornerRadius: 8))
    .onAppear { store.refreshPermissions() }
    .onReceive(NotificationCenter.default.publisher(for: NSApplication.didBecomeActiveNotification))
    { _ in
      store.refreshPermissions()
    }
  }
  private var model: DiscoveredModel? {
    ModelCatalog(data: store.models).transcriber(
      store.config.presets.first(where: { $0.id == store.config.selectedPreset })?.model ?? "")
  }
  private var speechReady: Bool {
    (!store.config.usesManagedRuntime || runtime.ready) && model != nil
      && model?.dictation?.readiness_error == nil
  }
  private var engineStatus: String {
    if store.config.usesManagedRuntime && !runtime.ready { return runtime.status }
    if let error = model?.dictation?.readiness_error {
      return error.contains("AppleSpeechAssetsMissing")
        ? "Language assets needed. Enable Apple speech downloads in Speech & text, then test dictation."
        : "Speech needs attention: " + error
    }
    return model == nil
      ? "Check the engine to discover your speech model."
      : "Speech model available. Test dictation to confirm language readiness."
  }
  private func readiness(_ title: String, ready: Bool) -> some View {
    Label(title, systemImage: ready ? "checkmark.circle.fill" : "circle")
      .foregroundStyle(ready ? Color.green : Color.secondary)
      .accessibilityLabel(title + (ready ? ": ready" : ": needs setup"))
  }
}

struct DictionaryEditor: View {
  let title: String
  @Binding var words: [String]
  @StateObject private var input = DictionaryInput()
  var body: some View {
    VStack(alignment: .leading, spacing: 6) {
      Text(title).font(.headline)
      ForEach(Array(words.enumerated()), id: \.offset) { index, word in
        HStack {
          Text(word).textSelection(.enabled)
          Spacer()
          Button("Remove", systemImage: "minus.circle") {
            words.remove(at: index)
          }.labelStyle(.iconOnly).accessibilityLabel("Remove " + word)
        }
      }
      HStack {
        TextField("Add a word or phrase", text: $input.word).onSubmit { add() }
        Button("Add word") { add() }.disabled(
          input.word.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
      }
    }
  }
  private func add() {
    let word = input.word.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !word.isEmpty else { return }
    if !words.contains(word) { words.append(word) }
    input.word = ""
  }
}

@MainActor private final class SetupPresentation: ObservableObject {
  @Published var expanded = true
}
@MainActor private final class DictionaryInput: ObservableObject {
  @Published var word = ""
}
