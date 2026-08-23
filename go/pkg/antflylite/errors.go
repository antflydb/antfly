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
	// OutcomeUnknown means publication succeeded in the running process, but
	// crash durability could not be confirmed. Inspect the destination and do
	// not retry the operation automatically.
	OutcomeUnknown ErrorCode = 7
	// Unsupported means the operation requires a platform or filesystem
	// capability that is unavailable. Retrying unchanged will not succeed.
	Unsupported ErrorCode = 8
	Internal    ErrorCode = 255
)

var errorCodeNames = map[ErrorCode]string{
	OK:              "ANTFLY_OK",
	InvalidArgument: "ANTFLY_INVALID_ARGUMENT",
	NotFound:        "ANTFLY_NOT_FOUND",
	VersionConflict: "ANTFLY_VERSION_CONFLICT",
	IntentConflict:  "ANTFLY_INTENT_CONFLICT",
	TxnNotFound:     "ANTFLY_TXN_NOT_FOUND",
	Busy:            "ANTFLY_BUSY",
	OutcomeUnknown:  "ANTFLY_OUTCOME_UNKNOWN",
	Unsupported:     "ANTFLY_UNSUPPORTED",
	Internal:        "ANTFLY_INTERNAL",
}

var errorCodeDescriptions = map[ErrorCode]string{
	OK:              "operation completed successfully",
	InvalidArgument: "an argument, request, path, or open mode is invalid",
	NotFound:        "the requested database object was not found",
	VersionConflict: "a version predicate did not match the current document version",
	IntentConflict:  "a transaction intent conflicts with the requested operation",
	TxnNotFound:     "the requested transaction was not found",
	Busy:            "the requested resource is temporarily busy or changed during streaming; stabilize it and retry",
	OutcomeUnknown:  "the operation was published, but crash durability could not be confirmed; inspect the destination and do not retry automatically",
	Unsupported:     "the operation requires a capability that is not supported by this platform or filesystem",
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
