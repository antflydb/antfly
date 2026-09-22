// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// Apply checked SQL operation hooks after oapi-codegen. Fail closed if an
// upstream generator changes the expected functions instead of silently
// dropping transport safety from regenerated clients.
package main

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"regexp"
)

func main() {
	path := "oapi/client.gen.go"
	source, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	for _, change := range []struct{ function, from, to string }{
		{`func \(c \*Client\) ExecuteSQLWithBody\(`, "return c.Client.Do(req)", "return executeSQLRequest(c.Client, req)"},
		{`func \(c \*Client\) ExecuteSQL\(`, "return c.Client.Do(req)", "return executeSQLRequest(c.Client, req)"},
		{`func NewExecuteSQLRequest\(`, "json.Marshal(body)", "marshalSQLRequest(body)"},
		{`func ParseExecuteSQLResponse\(`, "io.ReadAll(rsp.Body)", "readSQLResponseBody(rsp.Body)"},
	} {
		re := regexp.MustCompile(`(?s)` + change.function + `.*?\n}`)
		matches := re.FindAllIndex(source, -1)
		if len(matches) != 1 {
			panic(fmt.Sprintf("SQL generator hook matched %d functions: %s", len(matches), change.function))
		}
		start, end := matches[0][0], matches[0][1]
		function := source[start:end]
		if bytes.Count(function, []byte(change.from)) != 1 {
			panic("SQL generator hook no longer matches: " + change.function)
		}
		replacement := bytes.Replace(function, []byte(change.from), []byte(change.to), 1)
		updated := append([]byte{}, source[:start]...)
		updated = append(updated, replacement...)
		source = append(updated, source[end:]...)
	}
	source, err = format.Source(source)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(path, source, 0o644); err != nil {
		panic(err)
	}
}
