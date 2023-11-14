// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/server/handlers/api_versions.h"
#include "sql/types.h"
#include "funes/tests/fixture.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/smp.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

// https://github.com/apache/sql/blob/eaccb92/core/src/test/scala/unit/sql/server/ApiVersionsRequestTest.scala

FIXTURE_TEST(validate_latest_version, funes_thread_fixture) {
    auto client = make_sql_client().get0();
    client.connect().get();

    sql::api_versions_request request;
    request.data.client_software_name = "funes";
    request.data.client_software_version = "x.x.x";
    auto response = client.dispatch(request, sql::api_version(3)).get0();
    BOOST_TEST(response.data.error_code == sql::error_code::none);
    client.stop().then([&client] { client.shutdown(); }).get();

    auto expected = sql::get_supported_apis();
    BOOST_TEST(response.data.api_keys == expected);
}

FIXTURE_TEST(validate_v0, funes_thread_fixture) {
    auto client = make_sql_client().get0();
    client.connect().get();

    sql::api_versions_request request;
    auto response = client.dispatch(request, sql::api_version(0)).get0();
    BOOST_TEST(response.data.error_code == sql::error_code::none);
    client.stop().then([&client] { client.shutdown(); }).get();

    auto expected = sql::get_supported_apis();
    BOOST_TEST(response.data.api_keys == expected);
}

FIXTURE_TEST(unsupported_version, funes_thread_fixture) {
    auto client = make_sql_client().get0();
    client.connect().get();

    sql::api_versions_request request;
    auto max_version = sql::api_version(
      std::numeric_limits<sql::api_version::type>::max());
    auto response
      = client.dispatch(request, max_version, sql::api_version(0)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST(
      response.data.error_code == sql::error_code::unsupported_version);
    BOOST_REQUIRE(!response.data.api_keys.empty());

    // get the versions supported by the api versions request itself
    auto api = std::find_if(
      response.data.api_keys.cbegin(),
      response.data.api_keys.cend(),
      [](const sql::api_versions_response_key& api) {
          return api.api_key == sql::api_versions_api::key;
      });

    BOOST_REQUIRE(api != response.data.api_keys.cend());
    BOOST_TEST(api->api_key == sql::api_versions_api::key);
    BOOST_TEST(api->min_version == sql::api_versions_handler::min_supported);
    BOOST_TEST(api->max_version == sql::api_versions_handler::max_supported);
}

// Tests for bug that broke flex request parsing for null or empty client ids
FIXTURE_TEST(flex_with_empty_client_id, funes_thread_fixture) {
    auto client = make_sql_client("").get0();
    client.connect().get();

    sql::api_versions_request request;
    auto response = client.dispatch(request, sql::api_version(3)).get0();
    BOOST_TEST(response.data.error_code == sql::error_code::none);
    client.stop().then([&client] { client.shutdown(); }).get();
}

FIXTURE_TEST(flex_with_null_client_id, funes_thread_fixture) {
    auto client = make_sql_client(std::nullopt).get0();
    client.connect().get();

    sql::api_versions_request request;
    auto response = client.dispatch(request, sql::api_version(3)).get0();
    BOOST_TEST(response.data.error_code == sql::error_code::none);
    client.stop().then([&client] { client.shutdown(); }).get();
}
