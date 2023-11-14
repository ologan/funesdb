// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package admin provides a cobra command for the funes admin listener.
package admin

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/funes/admin/brokers"
	configcmd "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/funes/admin/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/funes/admin/partitions"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// NewCommand returns the funes admin command.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Talk to the Funes admin listener",
		Args:  cobra.ExactArgs(0),
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	cmd.AddCommand(
		brokers.NewCommand(fs, p),
		partitions.NewCommand(fs, p),
		configcmd.NewCommand(fs, p),
	)
	return cmd
}
