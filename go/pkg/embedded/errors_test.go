package embedded

import "testing"
import "strings"
import "errors"

func TestNativeErrorOwnsDetailAndPreservesCode(t *testing.T) {
	err := &NativeError{Code: InvalidArgument, Detail: "sparse must be boolean"}
	if !errors.Is(err, InvalidArgument) {
		t.Fatal("native detail lost stable code")
	}
	if !strings.Contains(err.Error(), "sparse must be boolean") {
		t.Fatal("missing detail")
	}
}
