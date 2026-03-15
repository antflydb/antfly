/*
Copyright 2025 The Antfly Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/antflydb/antfly/lib/secrets"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/term"
)

var keystorePath string

// keystoreCmd represents the keystore command
var keystoreCmd = &cobra.Command{
	Use:   "keystore",
	Short: "Manage encrypted secrets in the Antfly keystore",
	Long: `The keystore command provides subcommands for managing encrypted secrets
in the Antfly keystore. Secrets are stored in an encrypted file and can be
referenced in configuration files using ${secret:key.name} syntax.

Examples:
  # Create a new keystore
  antfly keystore create

  # Add a secret interactively
  antfly keystore add aws.access_key_id

  # Add a secret from stdin (for automation)
  echo "sk-..." | antfly keystore add openai.api_key --stdin

  # Add a file (for service account JSON, etc)
  antfly keystore add-file gcp.credentials /path/to/service-account.json

  # List all secret keys
  antfly keystore list

  # Show a secret value
  antfly keystore show openai.api_key

  # Remove a secret
  antfly keystore remove aws.access_key_id`,
}

// keystoreCreateCmd creates a new keystore
var keystoreCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new encrypted keystore",
	Long: `Creates a new encrypted keystore file. If a keystore already exists,
this command will fail unless --force is specified.`,
	Run: func(cmd *cobra.Command, args []string) {
		force, _ := cmd.Flags().GetBool("force")

		// Check if keystore already exists
		if secrets.KeystoreExists(keystorePath) && !force {
			fmt.Fprintf(os.Stderr, "Error: Keystore already exists at %s\n", keystorePath)
			fmt.Fprintf(os.Stderr, "Use --force to overwrite\n")
			os.Exit(1)
		}

		// Get password from flag/env or prompt
		password := keystorePassword("Enter keystore password (leave empty for no password): ")

		// Create keystore
		ks, err := secrets.NewKeystore(keystorePath, password)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating keystore: %v\n", err)
			os.Exit(1)
		}

		// Save to disk
		if err := ks.Save(); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving keystore: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Created keystore at %s\n", keystorePath)
	},
}

// keystoreAddCmd adds a secret to the keystore
var keystoreAddCmd = &cobra.Command{
	Use:   "add <key>",
	Short: "Add or update a secret in the keystore",
	Long: `Adds or updates a secret in the keystore. The value is prompted interactively
unless --stdin is specified.

Examples:
  # Add interactively
  antfly keystore add aws.access_key_id

  # Add from stdin
  echo "secret-value" | antfly keystore add openai.api_key --stdin`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		key := args[0]
		stdin, _ := cmd.Flags().GetBool("stdin")

		// Load keystore
		ks := loadKeystore()

		// Get value
		var value string
		if stdin {
			scanner := bufio.NewScanner(os.Stdin)
			if scanner.Scan() {
				value = scanner.Text()
			}
			if err := scanner.Err(); err != nil {
				fmt.Fprintf(os.Stderr, "Error reading from stdin: %v\n", err)
				os.Exit(1)
			}
		} else {
			value = getPassword(fmt.Sprintf("Enter value for '%s': ", key), true)
		}

		// Add to keystore
		if err := ks.Add(key, []byte(value)); err != nil {
			fmt.Fprintf(os.Stderr, "Error adding secret: %v\n", err)
			os.Exit(1)
		}

		// Save
		if err := ks.Save(); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving keystore: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Added '%s' to keystore\n", key)
	},
}

// keystoreAddFileCmd adds a file as a secret to the keystore
var keystoreAddFileCmd = &cobra.Command{
	Use:   "add-file <key> <file-path>",
	Short: "Add a file's contents as a secret",
	Long: `Reads a file and stores its contents as a secret in the keystore.
This is useful for service account JSON files, certificates, etc.

Example:
  antfly keystore add-file gcp.credentials /path/to/service-account.json`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		key := args[0]
		filePath := args[1]

		// Load keystore
		ks := loadKeystore()

		// Read file
		fileContent, err := os.ReadFile(filePath) //nolint:gosec // G304: internal file I/O, not user-controlled
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
			os.Exit(1)
		}

		// Add to keystore
		if err := ks.Add(key, fileContent); err != nil {
			fmt.Fprintf(os.Stderr, "Error adding secret: %v\n", err)
			os.Exit(1)
		}

		// Save
		if err := ks.Save(); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving keystore: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Added '%s' (from %s) to keystore\n", key, filePath)
	},
}

// keystoreListCmd lists all keys in the keystore
var keystoreListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all secret keys in the keystore",
	Long:  `Lists all secret keys (not values) stored in the keystore.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Load keystore
		ks := loadKeystore()

		// List keys
		keys := ks.List()
		if len(keys) == 0 {
			fmt.Println("Keystore is empty")
			return
		}

		fmt.Printf("Keystore contains %d secret(s):\n", len(keys))
		for _, key := range keys {
			fmt.Printf("  - %s\n", key)
		}
	},
}

// keystoreShowCmd shows a secret value
var keystoreShowCmd = &cobra.Command{
	Use:   "show <key>",
	Short: "Show a secret value",
	Long: `Displays the decrypted value of a secret from the keystore.
WARNING: This will print the secret in plain text to the terminal.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		key := args[0]

		// Load keystore
		ks := loadKeystore()

		// Get value
		value, err := ks.GetString(key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error retrieving secret: %v\n", err)
			os.Exit(1)
		}

		fmt.Println(value)
	},
}

// keystoreRemoveCmd removes a secret from the keystore
var keystoreRemoveCmd = &cobra.Command{
	Use:   "remove <key>",
	Short: "Remove a secret from the keystore",
	Long:  `Removes a secret from the keystore permanently.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		key := args[0]

		// Load keystore
		ks := loadKeystore()

		// Remove
		if err := ks.Remove(key); err != nil {
			fmt.Fprintf(os.Stderr, "Error removing secret: %v\n", err)
			os.Exit(1)
		}

		// Save
		if err := ks.Save(); err != nil {
			fmt.Fprintf(os.Stderr, "Error saving keystore: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Removed '%s' from keystore\n", key)
	},
}

func init() {
	rootCmd.AddCommand(keystoreCmd)

	// Add subcommands
	keystoreCmd.AddCommand(keystoreCreateCmd)
	keystoreCmd.AddCommand(keystoreAddCmd)
	keystoreCmd.AddCommand(keystoreAddFileCmd)
	keystoreCmd.AddCommand(keystoreListCmd)
	keystoreCmd.AddCommand(keystoreShowCmd)
	keystoreCmd.AddCommand(keystoreRemoveCmd)

	// Persistent flags for all keystore commands
	keystoreCmd.PersistentFlags().StringVarP(&keystorePath, "path", "p", secrets.DefaultKeystorePath, "Path to keystore file")

	// create command flags
	keystoreCreateCmd.Flags().Bool("force", false, "Overwrite existing keystore")

	// add command flags
	keystoreAddCmd.Flags().Bool("stdin", false, "Read value from stdin instead of prompting")
}

// keystorePassword returns the keystore password from --keystore-password / ANTFLY_KEYSTORE_PASSWORD
// if set, otherwise prompts interactively.
func keystorePassword(prompt string) string {
	if pw := viper.GetString("keystore_password"); pw != "" {
		return pw
	}
	return getPassword(prompt, false)
}

// loadKeystore loads the keystore with password prompt
func loadKeystore() *secrets.Keystore {
	// Check if keystore exists
	if !secrets.KeystoreExists(keystorePath) {
		fmt.Fprintf(os.Stderr, "Error: Keystore does not exist at %s\n", keystorePath)
		fmt.Fprintf(os.Stderr, "Create one with: antfly keystore create\n")
		os.Exit(1)
	}

	// Get password from flag/env or prompt
	password := keystorePassword("Enter keystore password: ")

	// Load keystore
	ks, err := secrets.LoadKeystore(keystorePath, password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading keystore: %v\n", err)
		fmt.Fprintf(os.Stderr, "Make sure the password is correct\n")
		os.Exit(1)
	}

	return ks
}

// getPassword prompts for a password (or any hidden input)
func getPassword(prompt string, required bool) string {
	fmt.Fprint(os.Stderr, prompt)

	// Read from terminal with echo disabled
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Fprintln(os.Stderr) // Print newline after hidden input

	if err != nil {
		if err == io.EOF || !term.IsTerminal(int(syscall.Stdin)) {
			fmt.Fprintln(os.Stderr, "Warning: not running in a terminal, password input not available")
			return ""
		}
		fmt.Fprintf(os.Stderr, "Error reading password: %v\n", err)
		os.Exit(1)
	}

	password := strings.TrimSpace(string(bytePassword))

	if required && password == "" {
		fmt.Fprintf(os.Stderr, "Error: Password cannot be empty\n")
		os.Exit(1)
	}

	return password
}
