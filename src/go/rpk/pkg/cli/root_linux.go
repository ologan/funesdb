// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cli

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/iotune"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/funes"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/funes/tune"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rp "github.com/redpanda-data/redpanda/src/go/rpk/pkg/funes"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func addPlatformDependentCmds(fs afero.Fs, p *config.Params, cmd *cobra.Command) {
	cmd.AddCommand(
		funes.NewCommand(fs, p, rp.NewLauncher()),
		iotune.NewCommand(fs, p),
	)

	// deprecated
	cmd.AddCommand(
		newCheckCommand(fs, p),
		newConfigCommand(fs, p),
		newModeCommand(fs, p),
		newStartCommand(fs, p, rp.NewLauncher()),
		newStopCommand(fs, p),
		newTuneCommand(fs, p),
	)
}

func newCheckCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return cobraext.DeprecateCmd(funes.NewCheckCommand(fs, p), "rpk funes check")
}

func newConfigCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return cobraext.DeprecateCmd(funes.NewConfigCommand(fs, p), "rpk funes config")
}

func newModeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return cobraext.DeprecateCmd(funes.NewModeCommand(fs, p), "rpk funes mode")
}

func newStartCommand(fs afero.Fs, p *config.Params, launcher rp.Launcher) *cobra.Command {
	return cobraext.DeprecateCmd(funes.NewStartCommand(fs, p, launcher), "rpk funes start")
}

func newStopCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return cobraext.DeprecateCmd(funes.NewStopCommand(fs, p), "rpk funes stop")
}

func newTuneCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return cobraext.DeprecateCmd(tune.NewCommand(fs, p), "rpk funes tune")
}
