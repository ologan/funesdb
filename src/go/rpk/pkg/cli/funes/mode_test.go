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
	"os"
	"path/filepath"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func fillRpkNodeConfig(path, mode string) *config.FunesYaml {
	y := config.DevDefault()
	val := mode == config.ModeProd
	y.Funes.DeveloperMode = !val
	y.Rpk = config.RpkNodeConfig{
		Overprovisioned: !val,
		Tuners: config.RpkNodeTuners{
			TuneNetwork:        val,
			TuneDiskScheduler:  val,
			TuneDiskWriteCache: val,
			TuneNomerges:       val,
			TuneDiskIrq:        val,
			TuneFstrim:         false,
			TuneCPU:            val,
			TuneAioEvents:      val,
			TuneClocksource:    val,
			TuneSwappiness:     val,
			CoredumpDir:        path,
			TuneBallastFile:    val,
		},
	}
	return y
}

func TestModeCommand(t *testing.T) {
	configPath := "/etc/funes/funes.yaml"
	tests := []struct {
		name   string
		args   []string
		before func(afero.Fs) (string, error)
		exp    *config.FunesYaml
		expErr bool
	}{
		{
			name: "development mode should disable all fields in the rpk config",
			args: []string{"development", "--config", configPath},
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(fillRpkNodeConfig(configPath, config.ModeProd))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0o644)
			},
			exp: fillRpkNodeConfig(configPath, config.ModeDev),
		},
		{
			name: "production mode should enable all fields in the rpk config",
			args: []string{"production", "--config", configPath},
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(fillRpkNodeConfig(configPath, config.ModeDev))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0o644)
			},
			exp: fillRpkNodeConfig(configPath, config.ModeProd),
		},
		{
			name: "the development mode alias, 'dev', should work the same",
			args: []string{"dev", "--config", configPath},
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(fillRpkNodeConfig(configPath, config.ModeProd))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0o644)
			},
			exp: fillRpkNodeConfig(configPath, config.ModeDev),
		},
		{
			name: "the production mode alias, 'prod', should work the same",
			args: []string{"prod", "--config", configPath},
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(fillRpkNodeConfig(configPath, config.ModeDev))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0o644)
			},
			exp: fillRpkNodeConfig(configPath, config.ModeProd),
		},
		{
			name: "mode should work if --config isn't passed, but the file is in $PWD or /etc/funes/funes.yaml",
			args: []string{"prod"},
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(fillRpkNodeConfig(configPath, config.ModeDev))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0o644)
			},
			exp: fillRpkNodeConfig(configPath, config.ModeProd),
		},
		{
			name: "mode lists the available modes if the one passed is not valid",
			args: []string{"invalidmode"},
			before: func(fs afero.Fs) (string, error) {
				dir, err := os.Getwd()
				if err != nil {
					t.Fatal(err)
				}
				wdConfigPath := filepath.Join(dir, "/funes.yaml")
				bs, err := yaml.Marshal(fillRpkNodeConfig(wdConfigPath, config.ModeDev))
				if err != nil {
					return "", err
				}
				return wdConfigPath, afero.WriteFile(fs, wdConfigPath, bs, 0o644)
			},
			expErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			_, err := tt.before(fs)
			require.NoError(t, err)
			p := new(config.Params)
			cmd := NewModeCommand(fs, p)
			cmd.SetArgs(tt.args)
			err = executeMode(fs, p, tt.args[0])
			if tt.expErr && err != nil {
				return
			}
			require.NoError(t, err)

			cfg, err := new(config.Params).Load(fs)
			require.NoError(t, err)
			y, _ := cfg.ActualFunesYaml()

			expRaw, err := yaml.Marshal(tt.exp)
			require.NoError(t, err)
			require.YAMLEq(t, string(expRaw), string(y.RawFile()))
		})
	}
}
