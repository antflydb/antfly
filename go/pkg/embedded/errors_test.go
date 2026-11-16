// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embedded

import "testing"
import "strings"
import "errors"
import "fmt"

func TestNativeErrorOwnsDetailAndPreservesCode(t *testing.T) {
	err := &NativeError{Code: InvalidArgument, Detail: "sparse must be boolean"}
	if !errors.Is(err, InvalidArgument) {
		t.Fatal("native detail lost stable code")
	}
	if !strings.Contains(err.Error(), "sparse must be boolean") {
		t.Fatal("missing detail")
	}
}

func TestOutcomeReceiptAttachesToOutcomeUnknown(t *testing.T) {
	const receipt = "or1:000102030405060708090a0b0c0d0e0f:3"
	err := withOutcomeReceipt(OutcomeUnknown, OutcomeUnknown, receipt)
	if !errors.Is(err, OutcomeUnknown) {
		t.Fatal("receipt lost stable code")
	}
	if got, ok := OutcomeReceiptOf(err); !ok || got != receipt {
		t.Fatalf("receipt = %q, %v", got, ok)
	}
	if !strings.Contains(err.Error(), receipt) {
		t.Fatalf("error text %q missing receipt", err.Error())
	}

	detailed := withOutcomeReceipt(&NativeError{Code: OutcomeUnknown, Detail: "commit"}, OutcomeUnknown, receipt)
	var native *NativeError
	if !errors.As(detailed, &native) || native.Detail != "commit" || native.OutcomeReceipt != receipt {
		t.Fatalf("detailed error = %#v", detailed)
	}

	if err := withOutcomeReceipt(OutcomeUnknown, OutcomeUnknown, ""); err != OutcomeUnknown {
		t.Fatalf("missing receipt changed error to %#v", err)
	}
	if _, ok := OutcomeReceiptOf(OutcomeUnknown); ok {
		t.Fatal("bare code reported a receipt")
	}
	if _, ok := OutcomeReceiptOf(fmt.Errorf("wrapped: %w", &NativeError{Code: Busy})); ok {
		t.Fatal("receiptless native error reported a receipt")
	}
}

func TestOutcomeReceiptStatusString(t *testing.T) {
	if OutcomeReceiptApplied.String() != "applied" || OutcomeReceiptNotApplied.String() != "not_applied" {
		t.Fatal("unexpected status names")
	}
}
