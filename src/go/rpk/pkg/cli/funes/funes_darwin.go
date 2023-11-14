// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build darwin
// +build darwin

package funes

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/funes/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewFunesDarwinCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "funes",
		Short: "Interact with a local or remote Funes process",
	}
	cmd.AddCommand(admin.NewCommand(fs, p))
	return cmd
}
