// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller_api.h"
#include "features/feature_table.h"
#include "sql/client/client.h"
#include "sql/protocol/describe_groups.h"
#include "sql/protocol/errors.h"
#include "sql/protocol/find_coordinator.h"
#include "sql/protocol/join_group.h"
#include "sql/protocol/schemata/join_group_request.h"
#include "sql/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "funes/tests/fixture.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace sql;
join_group_request make_join_group_request(
  ss::sstring member_id,
  ss::sstring gr,
  std::vector<ss::sstring> protocols,
  ss::sstring protocol_type) {
    join_group_request req;
    req.data.group_id = sql::group_id(std::move(gr));
    req.data.member_id = sql::member_id(std::move(member_id));
    req.data.protocol_type = sql::protocol_type(std::move(protocol_type));
    for (auto& p : protocols) {
        req.data.protocols.push_back(join_group_request_protocol{
          .name = protocol_name(std::move(p)), .metadata = bytes{}});
    }
    req.data.session_timeout_ms = 10s;
    return req;
}
struct consumer_offsets_fixture : public funes_thread_fixture {
    void
    wait_for_consumer_offsets_topic(const sql::group_instance_id& group) {
        auto client = make_sql_client().get0();

        client.connect().get();
        sql::find_coordinator_request req(group);
        req.data.key_type = sql::coordinator_type::group;
        client.dispatch(std::move(req), sql::api_version(1)).get();

        app.controller->get_api()
          .local()
          .wait_for_topic(
            model::sql_consumer_offsets_nt, model::timeout_clock::now() + 30s)
          .get();

        tests::cooperative_spin_wait_with_timeout(30s, [&group, &client] {
            sql::describe_groups_request req;
            req.data.groups.emplace_back(group);
            return client.dispatch(std::move(req), sql::api_version(1))
              .then([](sql::describe_groups_response response) {
                  return response.data.groups.front().error_code
                         == sql::error_code::none;
              });
        }).get();

        client.stop().get();
        client.shutdown();
    }
};

FIXTURE_TEST(join_empty_group_static_member, consumer_offsets_fixture) {
    sql::group_instance_id gr("instance-1");
    wait_for_consumer_offsets_topic(gr);
    auto client = make_sql_client().get0();
    auto deferred = ss::defer([&client] {
        client.stop().then([&client] { client.shutdown(); }).get();
    });
    client.connect().get();

    tests::cooperative_spin_wait_with_timeout(30s, [&gr, &client] {
        auto req = make_join_group_request(
          unknown_member_id, "group-test", {"p1", "p2"}, "random");
        // set group instance id
        req.data.group_instance_id = gr;
        return client.dispatch(std::move(req), sql::api_version(5))
          .then([&](auto resp) {
              BOOST_REQUIRE(
                resp.data.error_code == sql::error_code::none
                || resp.data.error_code == sql::error_code::not_coordinator);
              return resp.data.error_code == sql::error_code::none
                     && resp.data.member_id != unknown_member_id;
          });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(consumer_group_decode) {
    {
        // snatched from a log message after a franz-go client joined
        auto data = bytes_to_iobuf(
          base64_to_bytes("AAEAAAADAAJ0MAACdDEAAnQyAAAACAAAAAAAAAAAAAAAAA=="));
        const auto topics = group::decode_consumer_subscriptions(
          std::move(data));
        BOOST_REQUIRE(
          topics
          == absl::node_hash_set<model::topic>(
            {model::topic("t0"), model::topic("t1"), model::topic("t2")}));
    }
}
