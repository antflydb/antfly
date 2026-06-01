package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
)

const antflyCloudBinary = "antfly-cloud"

type commandExitError struct {
	code int
}

func (e commandExitError) Error() string { return fmt.Sprintf("command exited with status %d", e.code) }
func (e commandExitError) ExitCode() int { return e.code }

var cloudCmd = &cobra.Command{
	Use:                "cloud [args...]",
	Short:              "Delegate to the separate Antfly Cloud CLI",
	DisableFlagParsing: true,
	SilenceErrors:      true,
	RunE: func(cmd *cobra.Command, args []string) error {
		code := runAntflyCloud(args, os.Stdin, cmd.OutOrStdout(), cmd.ErrOrStderr())
		if code != 0 {
			return commandExitError{code: code}
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(cloudCmd)
}

func runAntflyCloud(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	path, err := exec.LookPath(antflyCloudBinary)
	if err != nil {
		fmt.Fprintf(stderr, `%s is not installed.

The `+"`antfly cloud`"+` command delegates to the separate Antfly Cloud CLI.
Install it with:

  brew install antflydb/taps/antfly-cloud

Then rerun this command.
`+"\n", antflyCloudBinary)
		return 127
	}
	return runExternalCommand(path, args, stdin, stdout, stderr)
}

func runExternalCommand(path string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	cmd := exec.Command(path, args...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.ExitCode()
		}
		fmt.Fprintf(stderr, "failed to run %s: %v\n", path, err)
		return 1
	}
	return 0
}
