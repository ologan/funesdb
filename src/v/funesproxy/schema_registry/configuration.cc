// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "funesproxy/schema_registry/configuration.h"

namespace funesproxy::schema_registry {

configuration::configuration(const YAML::Node& cfg)
  : configuration() {
    read_yaml(cfg);
}

configuration::configuration()
  : schema_registry_api(
    *this,
    "schema_registry_api",
    "Schema Registry API listen address and port",
    {},
    {config::rest_authn_endpoint{
      .address = net::unresolved_address("0.0.0.0", 8081),
      .authn_method = std::nullopt}})
  , schema_registry_api_tls(
      *this,
      "schema_registry_api_tls",
      "TLS configuration for Schema Registry API",
      {},
      {},
      config::endpoint_tls_config::validate_many)
  , schema_registry_replication_factor(
      *this,
      "schema_registry_replication_factor",
      "Replication factor for internal _schemas topic.  If unset, defaults to "
      "`default_topic_replication`",
      {},
      std::nullopt)
  , api_doc_dir(
      *this,
      "api_doc_dir",
      "API doc directory",
      {},
      "/usr/share/funes/proxy-api-doc") {}

} // namespace funesproxy::schema_registry
