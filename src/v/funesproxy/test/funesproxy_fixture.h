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

#include "config/configuration.h"
#include "http/client.h"
#include "sql/client/client.h"
#include "sql/client/configuration.h"
#include "sql/protocol/metadata.h"
#include "funesproxy/rest/configuration.h"
#include "funes/tests/fixture.h"

class funesproxy_test_fixture : public funes_thread_fixture {
public:
    funesproxy_test_fixture()
      : funes_thread_fixture() {}

    funesproxy_test_fixture(funesproxy_test_fixture const&) = delete;
    funesproxy_test_fixture(funesproxy_test_fixture&&) = delete;
    funesproxy_test_fixture operator=(funesproxy_test_fixture const&) = delete;
    funesproxy_test_fixture operator=(funesproxy_test_fixture&&) = delete;
    ~funesproxy_test_fixture() = default;

    http::client make_proxy_client() {
        net::base_transport::configuration transport_cfg;
        transport_cfg.server_addr = net::unresolved_address{
          "localhost", proxy_port};
        return http::client(transport_cfg);
    }

    http::client make_schema_reg_client() {
        net::base_transport::configuration transport_cfg;
        transport_cfg.server_addr = net::unresolved_address{
          "localhost", schema_reg_port};
        return http::client(transport_cfg);
    }

    void set_config(ss::sstring name, std::any val) {
        app.set_proxy_config(std::move(name), std::move(val)).get();
    }

    void set_client_config(ss::sstring name, std::any val) {
        app.set_proxy_client_config(std::move(name), std::move(val)).get();
    }
};
