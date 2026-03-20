package pebbleutils

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestRecoverPebbleClosed_RecognizesRawPebbleClosedError(t *testing.T) {
	var err error

	func() {
		defer RecoverPebbleClosed(&err)
		panic(errors.New("pebble: closed"))
	}()

	require.ErrorIs(t, err, pebble.ErrClosed)
}

func TestRecoverPebbleClosed_RecognizesClosedLogWriterPanic(t *testing.T) {
	var err error

	func() {
		defer RecoverPebbleClosed(&err)
		panic(errors.New("pebble/record: closed LogWriter"))
	}()

	require.ErrorIs(t, err, pebble.ErrClosed)
}

func TestRecoverPebbleClosed_RepanicsUnknownErrors(t *testing.T) {
	require.Panics(t, func() {
		var err error
		func() {
			defer RecoverPebbleClosed(&err)
			panic(errors.New("boom"))
		}()
	})
}
