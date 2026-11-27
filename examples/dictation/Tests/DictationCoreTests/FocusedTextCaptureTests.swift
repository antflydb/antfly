// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Testing

@testable import DictationApp

struct FocusedTextCaptureTests {
  private func resolver(
    editable: Set<Int>, secure: Set<Int> = [], disabled: Set<Int> = [], links: [Int: Int] = [:]
  ) -> FocusedTextResolver<Int> {
    .init(
      traits: {
        .init(
          editable: editable.contains($0), secure: secure.contains($0),
          enabled: !disabled.contains($0))
      },
      next: { links[$0] }, equal: { $0 == $1 })
  }
  @Test func capturesFocusedRichEditorThroughItsEditableAncestor() throws {
    let result = resolver(editable: [3], links: [1: 2, 2: 3]).resolve(1)
    #expect(try result.get() == 3)
  }
  @Test func secureOrDisabledFocusedDescendantsNeverEscapeToAnEditor() {
    for result in [
      resolver(editable: [2], secure: [1], links: [1: 2]).resolve(1),
      resolver(editable: [2], disabled: [1], links: [1: 2]).resolve(1),
      resolver(editable: [2], secure: [2], links: [1: 2]).resolve(1),
    ] {
      if case .success = result { Issue.record("Unsafe editor accepted") }
    }
  }
  @Test func nonEditorFocusAndCyclicOrDeepChainsStayUnsupported() {
    for result in [
      resolver(editable: [99]).resolve(1),
      resolver(editable: [99], links: [1: 2, 2: 1]).resolve(1),
      resolver(
        editable: [10], links: Dictionary(uniqueKeysWithValues: (1..<10).map { ($0, $0 + 1) })
      ).resolve(1),
    ] {
      if case .success = result { Issue.record("Unfocused or unbounded editor accepted") }
    }
  }
  @Test func validationResolvesTheSameRootWhenFocusedEditorDescendantsDiffer() throws {
    let lookup = resolver(editable: [3, 5], links: [1: 3, 2: 3, 4: 5])
    #expect(try lookup.resolve(1).get() == lookup.resolve(2).get())
    #expect(try lookup.resolve(1).get() != lookup.resolve(4).get())
  }
}
