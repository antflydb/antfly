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
package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func addInternalCommands(parent *cobra.Command) {
	internalCmd := &cobra.Command{
		Use:   "internal",
		Short: "Internal cluster management commands",
		Long:  `Commands for managing internal cluster operations like raft membership.`,
	}

	metadataCmd := &cobra.Command{
		Use:   "metadata",
		Short: "Manage metadata raft cluster",
		Long:  `Commands for managing the metadata raft cluster members.`,
	}

	metadataAddPeerCmd := &cobra.Command{
		Use:   "add-peer",
		Short: "Add a peer to the metadata raft cluster",
		Long:  `Add a new node to the metadata raft cluster by specifying its node ID and raft URL.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeID, err := cmd.Flags().GetUint64("id")
			if err != nil {
				return fmt.Errorf("failed to get node ID: %w", err)
			}

			raftURL, err := cmd.Flags().GetString("raft")
			if err != nil {
				return fmt.Errorf("failed to get raft URL: %w", err)
			}

			if raftURL == "" {
				return fmt.Errorf("--raft flag is required")
			}

			fmt.Fprintf(os.Stderr, "Adding peer %d with raft URL %s...\n", nodeID, raftURL)

			if err := antflyClient.AddMetadataPeer(nodeID, raftURL); err != nil {
				return fmt.Errorf("failed to add peer: %w", err)
			}

			fmt.Fprintln(os.Stderr, "Successfully added peer to metadata cluster")
			return nil
		},
	}

	metadataRemovePeerCmd := &cobra.Command{
		Use:   "remove-peer",
		Short: "Remove a peer from the metadata raft cluster",
		Long:  `Remove a node from the metadata raft cluster by specifying its node ID.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeID, err := cmd.Flags().GetUint64("id")
			if err != nil {
				return fmt.Errorf("failed to get node ID: %w", err)
			}

			force, err := cmd.Flags().GetBool("force")
			if err != nil {
				return fmt.Errorf("failed to get force flag: %w", err)
			}

			// Prompt for confirmation unless --force is specified
			if !force {
				fmt.Fprintf(os.Stderr, "Are you sure you want to remove peer %d from the metadata cluster? (yes/no): ", nodeID)
				reader := bufio.NewReader(os.Stdin)
				response, err := reader.ReadString('\n')
				if err != nil {
					return fmt.Errorf("failed to read confirmation: %w", err)
				}

				response = strings.TrimSpace(strings.ToLower(response))
				if response != "yes" && response != "y" {
					fmt.Fprintln(os.Stderr, "Operation cancelled")
					return nil
				}
			}

			fmt.Fprintf(os.Stderr, "Removing peer %d...\n", nodeID)

			if err := antflyClient.RemoveMetadataPeer(nodeID); err != nil {
				return fmt.Errorf("failed to remove peer: %w", err)
			}

			fmt.Fprintln(os.Stderr, "Successfully removed peer from metadata cluster")
			return nil
		},
	}

	metadataStatusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show metadata raft cluster status",
		Long:  `Display the current leader and member information for the metadata raft cluster.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			status, err := antflyClient.GetMetadataStatus()
			if err != nil {
				return fmt.Errorf("failed to get metadata status: %w", err)
			}

			fmt.Fprintf(os.Stderr, "Leader: %d\n", status.Leader)
			fmt.Fprintln(os.Stderr, "\nCluster Members:")
			for nodeID, raftURL := range status.Members {
				leaderMark := ""
				if nodeID == status.Leader {
					leaderMark = " (leader)"
				}
				fmt.Fprintf(os.Stderr, "  Node %d: %s%s\n", nodeID, raftURL, leaderMark)
			}
			return nil
		},
	}

	// Register internal command
	parent.AddCommand(internalCmd)

	// Register metadata subcommand under internal
	internalCmd.AddCommand(metadataCmd)

	// Register metadata operations
	metadataCmd.AddCommand(metadataAddPeerCmd)
	metadataCmd.AddCommand(metadataRemovePeerCmd)
	metadataCmd.AddCommand(metadataStatusCmd)

	// Flags for add-peer
	metadataAddPeerCmd.Flags().Uint64("id", 0, "Node ID of the peer to add (required)")
	metadataAddPeerCmd.Flags().String("raft", "", "Raft URL of the peer (required)")
	_ = metadataAddPeerCmd.MarkFlagRequired("id")
	_ = metadataAddPeerCmd.MarkFlagRequired("raft")

	// Flags for remove-peer
	metadataRemovePeerCmd.Flags().Uint64("id", 0, "Node ID of the peer to remove (required)")
	metadataRemovePeerCmd.Flags().Bool("force", false, "Skip confirmation prompt")
	_ = metadataRemovePeerCmd.MarkFlagRequired("id")
}
