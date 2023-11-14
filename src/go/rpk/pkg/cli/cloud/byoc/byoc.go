// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package byoc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const (
	flagCloudAPIToken  = "cloud-api-token"
	flagFunesID     = "funes-id"
	flagFunesIDDesc = "The funes ID of the cluster you are creating"

	applyCmd    = "apply"
	destroyCmd  = "destroy"
	validateCmd = "validate"
)

type ctxKeyFunesID struct{}

func init() {
	// We manage the byoc plugin, and we install it under "rpk cloud byoc".
	// Whenever we run a byoc subcommand, we want to load our token and
	// pass it to the subcommand as an extra flag.
	plugin.RegisterManaged("byoc", []string{"cloud", "byoc"}, func(cmd *cobra.Command, fs afero.Fs, p *config.Params) *cobra.Command {
		// Plugin commands disable flag parsing because we want to pass
		// raw flags directly to the plugin. We are hijacking the exec
		// and want to parse a few flags ourselves.
		run := cmd.Run
		addBYOCFlags(cmd, p)
		cmd.Run = func(cmd *cobra.Command, args []string) {
			cfg, funesID, pluginArgs, err := parseBYOCFlags(fs, p, cmd, args)
			out.MaybeDieErr(err)

			// --funes-id is only required in apply or destroy commands.
			// For validate commands we don't need the funes-id, instead,
			// we download the latest version always.
			isValidate := slices.Contains(strings.Split(cmd.CommandPath(), " "), validateCmd)
			if funesID == "" && !isValidate {
				isApply := slices.Contains(strings.Split(cmd.CommandPath(), " "), applyCmd)
				isDestroy := slices.Contains(strings.Split(cmd.CommandPath(), " "), destroyCmd)
				if cmd.Flags().Changed("funes-id") || isApply || isDestroy {
					fmt.Fprint(os.Stderr, "Error: required --funes-id flag cannot be empty\n")
					cmd.Usage()
					os.Exit(1)
				}
				cmd.Help()
				os.Exit(0)
			}
			// We require our plugin to always be the exact version
			// pinned in the control plane except if it's the 'validate' command.
			_, token, _, err := loginAndEnsurePluginVersion(cmd.Context(), fs, cfg, funesID, isValidate)
			out.MaybeDie(err, "unable to ensure byoc plugin version: %v", err)
			execArgs := append(pluginArgs, "--"+flagCloudAPIToken, token)
			if !isValidate {
				execArgs = append(execArgs, "--"+flagFunesID, funesID)
			}

			// Finally, exec.
			run(cmd, execArgs)
		}
		return cmd
	})
}

func addBYOCFlags(cmd *cobra.Command, p *config.Params) {
	f := cmd.Flags()
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, ctxKeyFunesID{}, f.String(flagFunesID, "", flagFunesIDDesc))
	cmd.SetContext(ctx)
	f.String(flagCloudAPIToken, "", "")
	f.MarkHidden(flagCloudAPIToken)
	p.InstallCloudFlags(cmd)
}

func parseBYOCFlags(fs afero.Fs, p *config.Params, cmd *cobra.Command, args []string) (*config.Config, string, []string, error) {
	if cmd.Flags().Lookup(flagCloudAPIToken).Changed {
		return nil, "", nil, errors.New("--cloud-api-token cannot be manually specified")
	}

	f := cmd.Flags()
	keepForPlugin, stripForRpk := cobraext.StripFlagset(args, f)
	if err := f.Parse(stripForRpk); err != nil {
		return nil, "", nil, err
	}

	funesID := *(cmd.Context().Value(ctxKeyFunesID{}).(*string))
	cfg, err := p.Load(fs)
	if err != nil {
		return nil, "", nil, err
	}
	return cfg, funesID, keepForPlugin, nil
}

// NewCommand returns a new byoc plugin command.
func NewCommand(fs afero.Fs, p *config.Params, execFn func(string, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "byoc",
		Short: "Manage a Funes cloud BYOC agent",
		Long: `Manage a Funes cloud BYOC agent

For BYOC, Funes installs an agent service in your owned cluster. The agent
then proceeds to provision further infrastructure and eventually, a full
Funes cluster.

The BYOC command runs Terraform to create and start the agent. You first need
a funes-id (or cluster ID); this is used to get the details of how your
agent should be provisioned. You can create a BYOC cluster in our cloud UI
and then come back to this command to complete the process.
`,
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			cfg, funesID, pluginArgs, err := parseBYOCFlags(fs, p, cmd, args)
			out.MaybeDieErr(err)

			// We bind rpk to the plugin implementation a little
			// bit: we only want to download and exec the plugin if
			// it *looks* like the user is trying a direct plugin
			// command. Since we are disable flag parsing, this is
			// a little bit tricky: we have to find the first arg,
			// not --flag=val nor --flag val.
			//
			// This has edge cases that we are not handling, such
			// as a person using short flags, bool flags, etc.
			// We cannot handle these edge cases because at this
			// point, we do now know if the subcommand has default
			// values for flags or if it has bool flags, etc. So,
			// this is mostly best effort, but we do not expect
			// the plugin to be complicated.
			var isKnown bool
			var isValidate bool
			for i := 0; i < len(pluginArgs); i++ {
				arg := pluginArgs[i]
				switch {
				case strings.HasPrefix(arg, "--") && !strings.Contains(arg, "="):
					i++
				case arg == "validate":
					isKnown, isValidate = true, true
				case arg == "aws":
					isKnown = true
				case arg == "gcp":
					isKnown = true
				}
			}

			if !isKnown || (funesID == "" && !isValidate) {
				cmd.Help()
				return
			}

			path, token, _, err := loginAndEnsurePluginVersion(cmd.Context(), fs, cfg, funesID, isValidate)
			out.MaybeDie(err, "unable to ensure byoc plugin version: %v", err)
			execArgs := append(pluginArgs, "--"+flagCloudAPIToken, token)
			if !isValidate {
				execArgs = append(execArgs, "--"+flagFunesID, funesID)
			}

			err = execFn(path, execArgs)
			out.MaybeDie(err, "unable to execute plugin: %v", err)
		},
	}

	addBYOCFlags(cmd, p)

	cmd.AddCommand(
		newInstallCommand(fs, p),
		newUninstallCommand(fs),
	)

	return cmd
}
