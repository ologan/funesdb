// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package funes

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/funes/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/funes/tune"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rp "github.com/redpanda-data/redpanda/src/go/rpk/pkg/funes"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params, launcher rp.Launcher) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "funes",
		Short: "Interact with a local Funes process",
	}

	cmd.AddCommand(
		NewStartCommand(fs, p, launcher),
		NewStopCommand(fs, p),
		NewCheckCommand(fs, p),
		NewModeCommand(fs, p),
		NewConfigCommand(fs, p),

		tune.NewCommand(fs, p),
		admin.NewCommand(fs, p),
	)

	return cmd
}
