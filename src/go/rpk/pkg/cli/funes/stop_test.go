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

package funes_test

import (
	"fmt"
	"os/exec"
	"strconv"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/funes"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestStopCommand(t *testing.T) {
	// simulate the funes process with an infinite loop.
	const baseCommand string = "while :; do sleep 1; done"
	tests := []struct {
		name           string
		ignoredSignals []string
		args           []string
	}{
		{
			name: "it should stop funes on SIGINT",
			args: []string{"--timeout", "100ms"},
		},
		{
			name:           "it should stop funes on SIGTERM if SIGINT was ignored",
			ignoredSignals: []string{"INT"},
			args:           []string{"--timeout", "100ms"},
		},
		{
			name:           "it should stop funes on SIGKILL if SIGINT and SIGTERM were ignored",
			ignoredSignals: []string{"TERM", "INT"},
			args:           []string{"--timeout", "100ms"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			conf := config.DevDefault()
			command := baseCommand
			// trap the signals we want to ignore, to check that the
			// signal escalation is working.
			for _, s := range tt.ignoredSignals {
				command = fmt.Sprintf(`trap "" %s; %s`, s, command)
			}
			ecmd := exec.Command("bash", "-c", command)
			// spawn the process asynchronously
			err := ecmd.Start()
			require.NoError(t, err)
			require.NotNil(t, ecmd.Process)

			pid := ecmd.Process.Pid
			_, err = utils.WriteBytes(
				fs,
				[]byte(strconv.Itoa(pid)),
				conf.PIDFile(),
			)
			require.NoError(t, err)
			err = conf.Write(fs)
			require.NoError(t, err)

			p := new(config.Params)
			c := funes.NewStopCommand(fs, p)
			c.Flags().StringVar(&p.ConfigFlag, "config", "", "this is done in root.go, but we need it here for the tests setting args")
			args := append([]string{"--config", conf.FileLocation()}, tt.args...)
			c.SetArgs(args)

			err = c.Execute()
			require.NoError(t, err)

			isStillRunning, err := os.IsRunningPID(
				fs,
				ecmd.Process.Pid,
			)
			require.NoError(t, err)
			require.False(t, isStillRunning)
		})
	}
}
