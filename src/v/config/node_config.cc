// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "node_config.h"

#include "config/configuration.h"
#include "net/unresolved_address.h"

namespace config {

node_config::node_config() noexcept
  : developer_mode(
    *this,
    "developer_mode",
    "Skips most of the checks performed at startup, not recomended for "
    "production use",
    {.visibility = visibility::tunable},
    false)
  , data_directory(
      *this,
      "data_directory",
      "Place where funes will keep the data",
      {.required = required::yes, .visibility = visibility::user})
  , node_id(
      *this,
      "node_id",
      "Unique id identifying a node in the cluster. If missing, a unique id "
      "will be assigned for this node when it joins the cluster",
      {.visibility = visibility::user},
      std::nullopt,
      [](std::optional<model::node_id> id) -> std::optional<ss::sstring> {
          if (id && (*id)() < 0) {
              return fmt::format("Negative node_id ({}) not allowed", *id);
          }
          return std::nullopt;
      })
  , rack(
      *this,
      "rack",
      "Rack identifier",
      {.visibility = visibility::user},
      std::nullopt)
  , seed_servers(
      *this,
      "seed_servers",
      "List of the seed servers used to join current cluster. If the "
      "seed_server list is empty the node will be a cluster root and it will "
      "form a new cluster",
      {.visibility = visibility::user},
      {},
      [](std::vector<seed_server> s) -> std::optional<ss::sstring> {
          std::sort(s.begin(), s.end());
          const auto s_dupe_i = std::adjacent_find(s.cbegin(), s.cend());
          if (s_dupe_i != s.cend()) {
              return fmt::format(
                "Duplicate items in seed_servers: {}", *s_dupe_i);
          }
          return std::nullopt;
      })
  , empty_seed_starts_cluster(
      *this,
      "empty_seed_starts_cluster",
      "If true, an empty seed_servers list will denote that this node should "
      "form a cluster. At most one node in the cluster should be configured "
      "configured with an empty seed_servers list. If no such configured node "
      "exists, or if configured to false, all nodes denoted by the "
      "seed_servers list must be identical among those nodes' configurations, "
      "and those nodes will form the initial cluster.",
      {.visibility = visibility::user},
      true)
  , rpc_server(
      *this,
      "rpc_server",
      "IpAddress and port for RPC server",
      {.visibility = visibility::user},
      net::unresolved_address("127.0.0.1", 33145))
  , rpc_server_tls(
      *this,
      "rpc_server_tls",
      "TLS configuration for RPC server",
      {.visibility = visibility::user},
      tls_config(),
      tls_config::validate)
  , sql_api(
      *this,
      "sql_api",
      "Address and port of an interface to listen for SQL API requests",
      {.visibility = visibility::user},
      {config::broker_authn_endpoint{
        .address = net::unresolved_address("127.0.0.1", 9092),
        .authn_method = std::nullopt}})
  , sql_api_tls(
      *this,
      "sql_api_tls",
      "TLS configuration for SQL API endpoint",
      {.visibility = visibility::user},
      {},
      endpoint_tls_config::validate_many)
  , admin(
      *this,
      "admin",
      "Address and port of admin server",
      {.visibility = visibility::user},
      {model::broker_endpoint(net::unresolved_address("127.0.0.1", 9644))})
  , admin_api_tls(
      *this,
      "admin_api_tls",
      "TLS configuration for admin HTTP server",
      {.visibility = visibility::user},
      {},
      endpoint_tls_config::validate_many)
  , admin_api_doc_dir(
      *this,
      "admin_api_doc_dir",
      "Admin API doc directory",
      {.visibility = visibility::user},
      "/usr/share/funes/admin-api-doc")
  , dashboard_dir(*this, "dashboard_dir")
  , cloud_storage_cache_directory(
      *this,
      "cloud_storage_cache_directory",
      "Directory for archival cache. Should be present when "
      "`cloud_storage_enabled` is present",
      {.visibility = visibility::user},
      std::nullopt)
  , enable_central_config(*this, "enable_central_config")
  , crash_loop_limit(
      *this,
      "crash_loop_limit",
      "Maximum consecutive crashes (unclean shutdowns) allowed after which "
      "operator intervention is needed to startup the broker. Limit is not "
      "enforced in developer mode.",
      {.visibility = visibility::user},
      5)
  , upgrade_override_checks(
      *this,
      "upgrade_override_checks",
      "Whether to violate safety checks when starting a funes version newer "
      "than the cluster's consensus version",
      {.visibility = visibility::tunable},
      false)
  , memory_allocation_warning_threshold(
      *this,
      "memory_allocation_warning_threshold",
      "Enables log messages for allocations greater than the given size.",
      {.visibility = visibility::tunable},
      std::nullopt)
  , storage_failure_injection_enabled(
      *this,
      "storage_failure_injection_enabled",
      "If true, inject low level storage failures on the write path. **Not** "
      "for production usage.",
      {.visibility = visibility::tunable},
      false)
  , recovery_mode_enabled(
      *this,
      "recovery_mode_enabled",
      "If true, start funes in \"metadata only\" mode, skipping "
      "loading user partitions and allowing only metadata operations.",
      {.visibility = visibility::user},
      false)
  , storage_failure_injection_config_path(
      *this,
      "storage_failure_injection_config_path",
      "Path to the configuration file used for low level storage failure "
      "injection",
      {.visibility = visibility::tunable},
      std::nullopt)
  , _advertised_rpc_api(
      *this,
      "advertised_rpc_api",
      "Address of RPC endpoint published to other cluster members",
      {.visibility = visibility::user},
      std::nullopt)
  , _advertised_sql_api(
      *this,
      "advertised_sql_api",
      "Address of SQL API published to the clients",
      {.visibility = visibility::user},
      {}) {}

void validate_multi_node_property_config(
  std::map<ss::sstring, ss::sstring>& errors) {
    auto const& cfg = config::node();
    const auto& sql_api = cfg.sql_api.value();
    for (auto const& ep : sql_api) {
        const auto& n = ep.name;
        auto authn_method = ep.authn_method;
        if (authn_method.has_value()) {
            if (authn_method == config::broker_authn_method::mtls_identity) {
                auto sql_api_tls = config::node().sql_api_tls();
                auto tls_it = std::find_if(
                  sql_api_tls.begin(),
                  sql_api_tls.end(),
                  [&n](const config::endpoint_tls_config& ep) {
                      return ep.name == n;
                  });
                if (
                  tls_it == sql_api_tls.end() || !tls_it->config.is_enabled()
                  || !tls_it->config.get_require_client_auth()) {
                    errors.emplace(
                      "sql_api.authentication_method",
                      ssx::sformat(
                        "sql_api {} configured with {}, but there is not an "
                        "equivalent sql_api_tls config that requires client "
                        "auth.",
                        n,
                        to_string_view(*authn_method)));
                }
            }
        }
    }

    for (auto const& ep : cfg.advertised_sql_api()) {
        auto err = model::broker_endpoint::validate_not_is_addr_any(ep);
        if (err) {
            errors.emplace("advertised_sql_api", ssx::sformat("{}", *err));
        }
    }

    auto rpc_err = model::broker_endpoint::validate_not_is_addr_any(
      model::broker_endpoint{"", cfg.advertised_rpc_api()});

    if (rpc_err) {
        errors.emplace("advertised_rpc_api", ssx::sformat("{}", *rpc_err));
    }
}

node_config::error_map_t node_config::load(const YAML::Node& root_node) {
    if (!root_node["funes"]) {
        throw std::invalid_argument("'funes' root is required");
    }

    const auto& ignore = shard_local_cfg().property_names();

    auto errors = config_store::read_yaml(root_node["funes"], ignore);
    validate_multi_node_property_config(errors);
    return errors;
}

/// Get a shard local copy of the node_config.
///
/// This has a terse name because it is used so many places,
/// usually as config::node().<property>
node_config& node() {
    static thread_local node_config cfg;
    return cfg;
}

} // namespace config
