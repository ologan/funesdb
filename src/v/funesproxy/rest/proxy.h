/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "funesproxy/fwd.h"
#include "funesproxy/rest/configuration.h"
#include "funesproxy/server.h"
#include "funesproxy/util.h"
#include "seastarx.h"
#include "security/request_auth.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/socket_defs.hh>

#include <vector>

namespace funesproxy::rest {

class proxy : public ss::peering_sharded_service<proxy> {
public:
    using server = auth_ctx_server<proxy>;
    proxy(
      const YAML::Node& config,
      ss::smp_service_group smp_sg,
      size_t max_memory,
      ss::sharded<sql::client::client>& client,
      ss::sharded<sql_client_cache>& client_cache,
      cluster::controller* controller);

    ss::future<> start();
    ss::future<> stop();

    configuration& config();
    sql::client::configuration& client_config();
    ss::sharded<sql::client::client>& client() { return _client; }
    ss::sharded<sql_client_cache>& client_cache() { return _client_cache; }
    ss::future<> mitigate_error(std::exception_ptr);

private:
    ss::future<> do_start();
    ss::future<> configure();
    ss::future<> inform(model::node_id);
    ss::future<> do_inform(model::node_id);

    configuration _config;
    ssx::semaphore _mem_sem;
    ss::gate _gate;
    ss::sharded<sql::client::client>& _client;
    ss::sharded<sql_client_cache>& _client_cache;
    server::context_t _ctx;
    server _server;
    one_shot _ensure_started;
    cluster::controller* _controller;
    bool _has_ephemeral_credentials{false};
    bool _is_started{false};
};

} // namespace funesproxy::rest
