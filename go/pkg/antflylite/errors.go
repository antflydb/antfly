// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

package antflylite

// ErrorCode is a stable Antfly C ABI error code.
type ErrorCode uint32

const (
	OK              ErrorCode = 0
	InvalidArgument ErrorCode = 1
	NotFound        ErrorCode = 2
	VersionConflict ErrorCode = 3
	IntentConflict  ErrorCode = 4
	TxnNotFound     ErrorCode = 5
	Busy            ErrorCode = 6
	Internal        ErrorCode = 255
)

var errorCodeNames = map[ErrorCode]string{
	OK:              "ANTFLY_OK",
	InvalidArgument: "ANTFLY_INVALID_ARGUMENT",
	NotFound:        "ANTFLY_NOT_FOUND",
	VersionConflict: "ANTFLY_VERSION_CONFLICT",
	IntentConflict:  "ANTFLY_INTENT_CONFLICT",
	TxnNotFound:     "ANTFLY_TXN_NOT_FOUND",
	Busy:            "ANTFLY_BUSY",
	Internal:        "ANTFLY_INTERNAL",
}

var errorCodeDescriptions = map[ErrorCode]string{
	OK:              "operation completed successfully",
	InvalidArgument: "an argument, request, path, or open mode is invalid",
	NotFound:        "the requested database object was not found",
	VersionConflict: "a version predicate did not match the current document version",
	IntentConflict:  "a transaction intent conflicts with the requested operation",
	TxnNotFound:     "the requested transaction was not found",
	Busy:            "the database is temporarily busy or a writer is already active",
	Internal:        "an internal error occurred",
}

func (code ErrorCode) Error() string {
	return code.Name() + ": " + code.Description()
}

// Name returns the stable symbolic C ABI name for code.
func (code ErrorCode) Name() string {
	if name, ok := errorCodeNames[code]; ok {
		return name
	}
	return "ANTFLY_UNKNOWN_ERROR"
}

// Description returns a short stable description for code.
func (code ErrorCode) Description() string {
	if description, ok := errorCodeDescriptions[code]; ok {
		return description
	}
	return "unknown Antfly error code"
}
