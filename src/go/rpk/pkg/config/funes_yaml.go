// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"crypto/tls"
	"fmt"
	"path"
	"reflect"

	"github.com/spf13/afero"
	"github.com/twmb/tlscfg"
	"gopkg.in/yaml.v3"

	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
)

// DefaultFunesYamlPath is where funes's configuration is located by
// default.
const DefaultFunesYamlPath = "/etc/funes/funes.yaml"

type (
	FunesYaml struct {
		fileLocation string // path to the funes.yaml file
		fileRaw      []byte // raw yaml file

		Funes             FunesNodeConfig `yaml:"funes,omitempty" json:"funes"`
		Rpk                  RpkNodeConfig      `yaml:"rpk,omitempty" json:"rpk"`
		Funesproxy           *Funesproxy        `yaml:"funesproxy,omitempty" json:"funesproxy,omitempty"`
		FunesproxyClient     *SQLClient       `yaml:"funesproxy_client,omitempty" json:"funesproxy_client,omitempty"`
		SchemaRegistry       *SchemaRegistry    `yaml:"schema_registry,omitempty" json:"schema_registry,omitempty"`
		SchemaRegistryClient *SQLClient       `yaml:"schema_registry_client,omitempty" json:"schema_registry_client,omitempty"`

		Other map[string]interface{} `yaml:",inline"`
	}

	// FunesNodeConfig is the source of truth for Funes node configuration.
	//
	// Cluster properties must NOT be enlisted in this struct. Adding a cluster
	// property here would cause the dependent libraries (e.g. operator) to wrongly
	// consider it a node property.
	FunesNodeConfig struct {
		Directory                  string                    `yaml:"data_directory,omitempty" json:"data_directory"`
		ID                         *int                      `yaml:"node_id,omitempty" json:"node_id,omitempty"`
		Rack                       string                    `yaml:"rack,omitempty" json:"rack"`
		EmptySeedStartsCluster     *bool                     `yaml:"empty_seed_starts_cluster,omitempty" json:"empty_seed_starts_cluster,omitempty"`
		SeedServers                []SeedServer              `yaml:"seed_servers" json:"seed_servers"`
		RPCServer                  SocketAddress             `yaml:"rpc_server,omitempty" json:"rpc_server"`
		SQLAPI                   []NamedAuthNSocketAddress `yaml:"sql_api,omitempty" json:"sql_api"`
		SQLAPITLS                []ServerTLS               `yaml:"sql_api_tls,omitempty" json:"sql_api_tls"`
		AdminAPI                   []NamedSocketAddress      `yaml:"admin,omitempty" json:"admin"`
		AdminAPITLS                []ServerTLS               `yaml:"admin_api_tls,omitempty" json:"admin_api_tls"`
		CoprocSupervisorServer     SocketAddress             `yaml:"coproc_supervisor_server,omitempty" json:"coproc_supervisor_server"`
		AdminAPIDocDir             string                    `yaml:"admin_api_doc_dir,omitempty" json:"admin_api_doc_dir"`
		DashboardDir               string                    `yaml:"dashboard_dir,omitempty" json:"dashboard_dir"`
		CloudStorageCacheDirectory string                    `yaml:"cloud_storage_cache_directory,omitempty" json:"cloud_storage_cache_directory"`
		AdvertisedRPCAPI           *SocketAddress            `yaml:"advertised_rpc_api,omitempty" json:"advertised_rpc_api,omitempty"`
		AdvertisedSQLAPI         []NamedSocketAddress      `yaml:"advertised_sql_api,omitempty" json:"advertised_sql_api,omitempty"`
		DeveloperMode              bool                      `yaml:"developer_mode,omitempty" json:"developer_mode"`
		RecoveryModeEnabled        bool                      `yaml:"recovery_mode_enabled,omitempty" json:"recovery_mode_enabled,omitempty"`
		CrashLoopLimit             *int                      `yaml:"crash_loop_limit,omitempty" json:"crash_loop_limit"`
		Other                      map[string]interface{}    `yaml:",inline"`
	}

	Funesproxy struct {
		FunesproxyAPI           []NamedAuthNSocketAddress `yaml:"funesproxy_api,omitempty" json:"funesproxy_api,omitempty"`
		FunesproxyAPITLS        []ServerTLS               `yaml:"funesproxy_api_tls,omitempty" json:"funesproxy_api_tls,omitempty"`
		AdvertisedFunesproxyAPI []NamedSocketAddress      `yaml:"advertised_funesproxy_api,omitempty" json:"advertised_funesproxy_api,omitempty"`
		Other                   map[string]interface{}    `yaml:",inline"`
	}

	SchemaRegistry struct {
		SchemaRegistryAPI               []NamedAuthNSocketAddress `yaml:"schema_registry_api,omitempty" json:"schema_registry_api,omitempty"`
		SchemaRegistryAPITLS            []ServerTLS               `yaml:"schema_registry_api_tls,omitempty" json:"schema_registry_api_tls,omitempty"`
		SchemaRegistryReplicationFactor *int                      `yaml:"schema_registry_replication_factor,omitempty" json:"schema_registry_replication_factor,omitempty"`
	}

	SQLClient struct {
		Brokers       []SocketAddress        `yaml:"brokers,omitempty" json:"brokers,omitempty"`
		BrokerTLS     ServerTLS              `yaml:"broker_tls,omitempty" json:"broker_tls,omitempty"`
		SASLMechanism *string                `yaml:"sasl_mechanism,omitempty" json:"sasl_mechanism,omitempty"`
		SCRAMUsername *string                `yaml:"scram_username,omitempty" json:"scram_username,omitempty"`
		SCRAMPassword *string                `yaml:"scram_password,omitempty" json:"scram_password,omitempty"`
		Other         map[string]interface{} `yaml:",inline"`
	}

	SeedServer struct {
		Host SocketAddress `yaml:"host,omitempty" json:"host"`

		// The SeedServer in older versions of funes was untabbed, but we support
		// these older versions using a custom unmarshaller. We track whether the
		// SeedServer field has been modified from the older version using this
		// unexported field.
		//
		// See see github.com/redpanda-data/redpanda/issues/8915.
		untabbed bool
	}

	SocketAddress struct {
		Address string `yaml:"address" json:"address"`
		Port    int    `yaml:"port,omitempty" json:"port"`
	}

	NamedSocketAddress struct {
		Address string `yaml:"address" json:"address"`
		Port    int    `yaml:"port,omitempty" json:"port"`
		Name    string `yaml:"name,omitempty" json:"name,omitempty"`
	}

	NamedAuthNSocketAddress struct {
		Address string  `yaml:"address,omitempty" json:"address"`
		Port    int     `yaml:"port,omitempty" json:"port"`
		Name    string  `yaml:"name,omitempty" json:"name,omitempty"`
		AuthN   *string `yaml:"authentication_method,omitempty" json:"authentication_method,omitempty"`
	}

	// BACKCOMPAT 23-05-01: The CA used to be "truststore_file" in yaml; we
	// deserialize truststore_file AND ca_file. See weak.go.
	TLS struct {
		KeyFile            string `yaml:"key_file,omitempty" json:"key_file,omitempty"`
		CertFile           string `yaml:"cert_file,omitempty" json:"cert_file,omitempty"`
		TruststoreFile     string `yaml:"ca_file,omitempty" json:"ca_file,omitempty"`
		InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty" json:"insecure_skip_verify,omitempty"`
	}

	ServerTLS struct {
		Name              string                 `yaml:"name,omitempty" json:"name"`
		KeyFile           string                 `yaml:"key_file,omitempty" json:"key_file"`
		CertFile          string                 `yaml:"cert_file,omitempty" json:"cert_file"`
		TruststoreFile    string                 `yaml:"truststore_file,omitempty" json:"truststore_file"`
		Enabled           bool                   `yaml:"enabled,omitempty" json:"enabled"`
		RequireClientAuth bool                   `yaml:"require_client_auth,omitempty" json:"require_client_auth"`
		Other             map[string]interface{} `yaml:",inline" `
	}

	RpkNodeConfig struct {
		SQLAPI RpkSQLAPI `yaml:"sql_api,omitempty" json:"sql_api"`
		AdminAPI RpkAdminAPI `yaml:"admin_api,omitempty" json:"admin_api"`

		// The following four configs are passed to funes on `rpk
		// funes start`. They are not tuner configs. They live here
		// for backcompat while rpk execs funes through systemd.
		AdditionalStartFlags []string `yaml:"additional_start_flags,omitempty"  json:"additional_start_flags"`
		EnableMemoryLocking  bool     `yaml:"enable_memory_locking,omitempty" json:"enable_memory_locking"`
		Overprovisioned      bool     `yaml:"overprovisioned,omitempty" json:"overprovisioned"`
		SMP                  *int     `yaml:"smp,omitempty" json:"smp,omitempty"`

		Tuners RpkNodeTuners `yaml:",inline"`
	}

	RpkNodeTuners struct {
		TuneNetwork              bool   `yaml:"tune_network,omitempty" json:"tune_network"`
		TuneDiskScheduler        bool   `yaml:"tune_disk_scheduler,omitempty" json:"tune_disk_scheduler"`
		TuneNomerges             bool   `yaml:"tune_disk_nomerges,omitempty" json:"tune_disk_nomerges"`
		TuneDiskWriteCache       bool   `yaml:"tune_disk_write_cache,omitempty" json:"tune_disk_write_cache"`
		TuneDiskIrq              bool   `yaml:"tune_disk_irq,omitempty" json:"tune_disk_irq"`
		TuneFstrim               bool   `yaml:"tune_fstrim,omitempty" json:"tune_fstrim"`
		TuneCPU                  bool   `yaml:"tune_cpu,omitempty" json:"tune_cpu"`
		TuneAioEvents            bool   `yaml:"tune_aio_events,omitempty" json:"tune_aio_events"`
		TuneClocksource          bool   `yaml:"tune_clocksource,omitempty" json:"tune_clocksource"`
		TuneSwappiness           bool   `yaml:"tune_swappiness,omitempty" json:"tune_swappiness"`
		TuneTransparentHugePages bool   `yaml:"tune_transparent_hugepages,omitempty" json:"tune_transparent_hugepages"`
		TuneCoredump             bool   `yaml:"tune_coredump,omitempty" json:"tune_coredump"`
		CoredumpDir              string `yaml:"coredump_dir,omitempty" json:"coredump_dir"`
		TuneBallastFile          bool   `yaml:"tune_ballast_file,omitempty" json:"tune_ballast_file"`
		BallastFilePath          string `yaml:"ballast_file_path,omitempty" json:"ballast_file_path"`
		BallastFileSize          string `yaml:"ballast_file_size,omitempty" json:"ballast_file_size"`
		WellKnownIo              string `yaml:"well_known_io,omitempty" json:"well_known_io"`
	}

	RpkSQLAPI struct {
		Brokers []string `yaml:"brokers,omitempty" json:"brokers,omitempty"`
		TLS     *TLS     `yaml:"tls,omitempty" json:"tls,omitempty"`
		SASL    *SASL    `yaml:"sasl,omitempty" json:"sasl,omitempty"`
	}

	RpkAdminAPI struct {
		Addresses []string `yaml:"addresses,omitempty" json:"addresses,omitempty"`
		TLS       *TLS     `yaml:"tls,omitempty" json:"tls,omitempty"`
	}

	RpkSchemaRegistryAPI struct {
		Addresses []string `yaml:"addresses,omitempty" json:"addresses,omitempty"`
		TLS       *TLS     `yaml:"tls,omitempty" json:"tls,omitempty"`
	}

	SASL struct {
		User      string `yaml:"user,omitempty" json:"user,omitempty"`
		Password  string `yaml:"password,omitempty" json:"password,omitempty"`
		Mechanism string `yaml:"mechanism,omitempty" json:"mechanism,omitempty"`
	}
)

func (t *TLS) Config(fs afero.Fs) (*tls.Config, error) {
	if t == nil {
		return nil, nil
	}
	tc, err := tlscfg.New(
		tlscfg.WithFS(
			tlscfg.FuncFS(func(path string) ([]byte, error) {
				return afero.ReadFile(fs, path)
			}),
		),
		tlscfg.MaybeWithDiskCA(
			t.TruststoreFile,
			tlscfg.ForClient,
		),
		tlscfg.MaybeWithDiskKeyPair(
			t.CertFile,
			t.KeyFile,
		),
	)
	if err != nil {
		return nil, err
	}
	tc.InsecureSkipVerify = t.InsecureSkipVerify
	return tc, nil
}

func namedAuthnToNamed(src []NamedAuthNSocketAddress) []NamedSocketAddress {
	dst := make([]NamedSocketAddress, 0, len(src))
	for _, a := range src {
		dst = append(dst, NamedSocketAddress{
			Address: a.Address,
			Port:    a.Port,
			Name:    a.Name,
		})
	}
	return dst
}

func DevDefault() *FunesYaml {
	return &FunesYaml{
		fileLocation: DefaultFunesYamlPath,
		Funes: FunesNodeConfig{
			Directory: "/var/lib/funes/data",
			RPCServer: SocketAddress{
				Address: DefaultListenAddress,
				Port:    DefaultRPCPort,
			},
			SQLAPI: []NamedAuthNSocketAddress{{
				Address: DefaultListenAddress,
				Port:    DefaultSQLPort,
			}},
			AdminAPI: []NamedSocketAddress{{
				Address: DefaultListenAddress,
				Port:    DefaultAdminPort,
			}},
			SeedServers:   []SeedServer{},
			DeveloperMode: true,
		},
		Rpk: RpkNodeConfig{
			Overprovisioned: true,
			Tuners: RpkNodeTuners{
				CoredumpDir: "/var/lib/funes/coredump",
			},
		},
		// enable funesproxy and schema_registry by default
		Funesproxy:     &Funesproxy{},
		SchemaRegistry: &SchemaRegistry{},
	}
}

func ProdDefault() *FunesYaml {
	cfg := DevDefault()
	cfg.setProdMode()
	return cfg
}

///////////
// FUNCS //
///////////

// FileLocation returns the path to this funes.yaml, whether it exists or
// not.
func (y *FunesYaml) FileLocation() string {
	return y.fileLocation
}

// PIDFile returns the pid.lock file path given this configuration.
func (y *FunesYaml) PIDFile() string {
	return path.Join(y.Funes.Directory, "pid.lock")
}

// RawFile returns the raw file for this yaml, if it existed.
func (y *FunesYaml) RawFile() []byte {
	return y.fileRaw
}

// Returns if the raw config is the same as the one in memory.
func (y *FunesYaml) isTheSameAsRawFile() bool {
	var init, final *FunesYaml
	if err := yaml.Unmarshal(y.fileRaw, &init); err != nil {
		return false
	}
	// Avoid DeepEqual comparisons on non-exported fields.
	finalRaw, err := yaml.Marshal(y)
	if err != nil {
		return false
	}
	if err := yaml.Unmarshal(finalRaw, &final); err != nil {
		return false
	}

	// If we have a file with an older version of the SeedServer, we should
	// write the file to disk even if the contents are the same. This is
	// necessary because Funes no longer parses older SeedServer versions.
	//
	// For more information, see github.com/redpanda-data/redpanda/issues/8915.
	if init != nil {
		for _, s := range init.Funes.SeedServers {
			if s.untabbed {
				return false
			}
		}
	}

	return reflect.DeepEqual(init, final)
}

// Write writes the configuration at the previously loaded path, or the default
// path.
func (y *FunesYaml) Write(fs afero.Fs) error {
	// We return early if the config is the same as the one loaded in the first
	// place and avoid writing the file.
	if y.isTheSameAsRawFile() {
		return nil
	}
	location := y.fileLocation
	if location == "" {
		location = DefaultFunesYamlPath
	}
	return y.WriteAt(fs, location)
}

// WriteAt writes the configuration to the given path.
func (y *FunesYaml) WriteAt(fs afero.Fs, path string) error {
	b, err := yaml.Marshal(y)
	if err != nil {
		return fmt.Errorf("marshal error in loaded config, err: %s", err)
	}
	return rpkos.ReplaceFile(fs, path, b, 0o644)
}

////////////////
// VALIDATION // -- this is only used in funes_checkers, and could be stronger -- this is essentially just a config validation
////////////////

// Check checks if the funes and rpk configuration is valid before running
// the tuners. See: funes_checkers.
func (y *FunesYaml) Check() (bool, []error) {
	errs := checkFunesConfig(y)
	errs = append(
		errs,
		checkRpkNodeConfig(y)...,
	)
	ok := len(errs) == 0
	return ok, errs
}

func checkFunesConfig(y *FunesYaml) []error {
	var errs []error
	rp := y.Funes
	// top level check
	if rp.Directory == "" {
		errs = append(errs, fmt.Errorf("funes.data_directory can't be empty"))
	}
	if rp.ID != nil && *rp.ID < 0 {
		errs = append(errs, fmt.Errorf("funes.node_id can't be a negative integer"))
	}

	// rpc server
	if rp.RPCServer == (SocketAddress{}) {
		errs = append(errs, fmt.Errorf("funes.rpc_server missing"))
	} else {
		saErrs := checkSocketAddress(rp.RPCServer, "funes.rpc_server")
		if len(saErrs) > 0 {
			errs = append(errs, saErrs...)
		}
	}

	// sql api
	if len(rp.SQLAPI) == 0 {
		errs = append(errs, fmt.Errorf("funes.sql_api missing"))
	} else {
		for i, addr := range rp.SQLAPI {
			configPath := fmt.Sprintf("funes.sql_api[%d]", i)
			saErrs := checkSocketAddress(SocketAddress{addr.Address, addr.Port}, configPath)
			if len(saErrs) > 0 {
				errs = append(errs, saErrs...)
			}
		}
	}

	// seed servers
	if len(rp.SeedServers) > 0 {
		for i, seed := range rp.SeedServers {
			configPath := fmt.Sprintf("funes.seed_servers[%d].host", i)
			saErrs := checkSocketAddress(seed.Host, configPath)
			if len(saErrs) > 0 {
				errs = append(errs, saErrs...)
			}
		}
	}
	return errs
}

func checkRpkNodeConfig(y *FunesYaml) []error {
	var errs []error
	if y.Rpk.Tuners.TuneCoredump && y.Rpk.Tuners.CoredumpDir == "" {
		errs = append(errs, fmt.Errorf("if rpk.tune_coredump is set to true, rpk.coredump_dir can't be empty"))
	}
	return errs
}

func checkSocketAddress(s SocketAddress, configPath string) []error {
	var errs []error
	if s.Port == 0 {
		errs = append(errs, fmt.Errorf("%s.port can't be 0", configPath))
	}
	if s.Address == "" {
		errs = append(errs, fmt.Errorf("%s.address can't be empty", configPath))
	}
	return errs
}
