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

#pragma once

#include "cluster/fwd.h"
#include "config/fwd.h"
#include "sql/client/fwd.h"
#include "model/compression.h"
#include "seastarx.h"
#include "security/acl.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <vector>

namespace sql::client {

ss::future<std::unique_ptr<sql::client::configuration>>
create_client_credentials(
  cluster::controller& controller,
  config::configuration const& cluster_cfg,
  sql::client::configuration const& client_cfg,
  security::acl_principal principal);

void set_client_credentials(
  sql::client::configuration const& client_cfg,
  sql::client::client& client);

ss::future<> set_client_credentials(
  sql::client::configuration const& client_cfg,
  ss::sharded<sql::client::client>& client);

model::compression compression_from_str(std::string_view v);

} // namespace sql::client
