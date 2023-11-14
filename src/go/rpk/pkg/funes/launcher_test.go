// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package funes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_collectFunesArgs(t *testing.T) {
	tests := []struct {
		name string
		args FunesArgs
		want []string
	}{
		{
			name: "shall include config file path into args",
			args: FunesArgs{
				ConfigFilePath: "/etc/funes/funes.yaml",
			},
			want: []string{
				"funes",
				"--funes-cfg",
				"/etc/funes/funes.yaml",
			},
		},
		{
			name: "shall include memory setting into command line",
			args: FunesArgs{
				ConfigFilePath: "/etc/funes/funes.yaml",
				SeastarFlags: map[string]string{
					"memory": "1G",
				},
			},
			want: []string{
				"funes",
				"--funes-cfg",
				"/etc/funes/funes.yaml",
				"--memory=1G",
			},
		},
		{
			name: "shall include cpuset setting into command line",
			args: FunesArgs{
				ConfigFilePath: "/etc/funes/funes.yaml",
				SeastarFlags: map[string]string{
					"cpuset": "0-1",
				},
			},
			want: []string{
				"funes",
				"--funes-cfg",
				"/etc/funes/funes.yaml",
				"--cpuset=0-1",
			},
		},
		{
			name: "shall include io-properties-file setting into command line",
			args: FunesArgs{
				ConfigFilePath: "/etc/funes/funes.yaml",
				SeastarFlags: map[string]string{
					"io-properties-file": "/etc/funes/io-config.yaml",
				},
			},
			want: []string{
				"funes",
				"--funes-cfg",
				"/etc/funes/funes.yaml",
				"--io-properties-file=/etc/funes/io-config.yaml",
			},
		},
		{
			name: "shall include memory lock",
			args: FunesArgs{
				ConfigFilePath: "/etc/funes/funes.yaml",
				SeastarFlags: map[string]string{
					"lock-memory": fmt.Sprint(true),
				},
			},
			want: []string{
				"funes",
				"--funes-cfg",
				"/etc/funes/funes.yaml",
				"--lock-memory=true",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collectFunesArgs(&tt.args)
			require.Exactly(t, tt.want, got)
		})
	}
}
