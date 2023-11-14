/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "sql/client/config_utils.h"

#include "cluster/controller.h"
#include "cluster/ephemeral_credential_frontend.h"
#include "config/configuration.h"
#include "sql/client/client.h"
#include "sql/client/configuration.h"
#include "seastarx.h"
#include "security/acl.h"
#include "utils/string_switch.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>

#include <exception>

namespace sql::client {

ss::future<std::unique_ptr<sql::client::configuration>>
create_client_credentials(
  cluster::controller& controller,
  config::configuration const& cluster_cfg,
  sql::client::configuration const& client_cfg,
  security::acl_principal principal) {
    auto new_cfg = std::make_unique<sql::client::configuration>(
      to_yaml(client_cfg, config::redact_secrets::no));

    // If AuthZ is not enabled, don't create credentials.
    if (!cluster_cfg.sql_enable_authorization().value_or(
          cluster_cfg.enable_sasl())) {
        co_return new_cfg;
    }

    // If the configuration is overriden, use it.
    if (
      client_cfg.scram_password.is_overriden()
      || client_cfg.scram_username.is_overriden()
      || client_cfg.sasl_mechanism.is_overriden()) {
        co_return new_cfg;
    }

    // Get the internal secret for user
    auto& frontend = controller.get_ephemeral_credential_frontend().local();
    auto pw = co_await frontend.get(principal);

    if (pw.err != cluster::errc::success) {
        co_return ss::coroutine::return_exception(
          std::runtime_error(fmt::format(
            "Failed to fetch credential for principal: {}", principal)));
    }

    new_cfg->sasl_mechanism.set_value(pw.credential.mechanism());
    new_cfg->scram_username.set_value(pw.credential.user()());
    new_cfg->scram_password.set_value(pw.credential.password()());

    co_return new_cfg;
}

void set_client_credentials(
  sql::client::configuration const& client_cfg,
  sql::client::client& client) {
    client.config().sasl_mechanism.set_value(client_cfg.sasl_mechanism());
    client.config().scram_username.set_value(client_cfg.scram_username());
    client.config().scram_password.set_value(client_cfg.scram_password());
}

ss::future<> set_client_credentials(
  sql::client::configuration const& client_cfg,
  ss::sharded<sql::client::client>& client) {
    co_await client.invoke_on_all([&client_cfg](sql::client::client& client) {
        client.config().sasl_mechanism.set_value(client_cfg.sasl_mechanism());
        client.config().scram_username.set_value(client_cfg.scram_username());
        client.config().scram_password.set_value(client_cfg.scram_password());
    });
}

model::compression compression_from_str(std::string_view v) {
    return string_switch<model::compression>(v)
      .match("none", model::compression::none)
      .match("gzip", model::compression::gzip)
      .match("snappy", model::compression::snappy)
      .match("lz4", model::compression::lz4)
      .match("zstd", model::compression::zstd)
      .default_match(model::compression::none);
}

} // namespace sql::client
