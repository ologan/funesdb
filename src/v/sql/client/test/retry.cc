// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/client/client.h"
#include "sql/client/test/fixture.h"
#include "sql/client/test/utils.h"
#include "sql/protocol/exceptions.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

inline const model::topic_partition unknown_tp{
  model::topic{"unknown"}, model::partition_id{0}};

FIXTURE_TEST(test_retry_list_offsets, sql_client_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    client.config().retry_base_backoff.set_value(10ms);
    client.config().retries.set_value(size_t(5));

    BOOST_REQUIRE_EXCEPTION(
      client.list_offsets(unknown_tp).get(),
      sql::exception_base,
      [](sql::exception_base ex) {
          return ex.error == sql::error_code::unknown_topic_or_partition;
      });
}

FIXTURE_TEST(test_retry_produce, sql_client_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    client.config().retry_base_backoff.set_value(10ms);
    client.config().retries.set_value(size_t(5));

    auto res = client
                 .produce_record_batch(
                   unknown_tp, make_batch(model::offset{0}, 12))
                 .get();
    BOOST_REQUIRE_EQUAL(
      res.error_code, sql::error_code::unknown_topic_or_partition);
}

class sql_client_create_topic_fixture : public sql_client_fixture {
public:
    sql_client_create_topic_fixture()
      : sql_client_fixture(std::make_optional<uint32_t>(1)) {}
};

FIXTURE_TEST(test_retry_create_topic, sql_client_create_topic_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    client.config().retry_base_backoff.set_value(10ms);
    client.config().retries.set_value(size_t(5));

    auto make_topic = [](ss::sstring name) {
        return sql::creatable_topic{
          .name = model::topic{name},
          .num_partitions = 1,
          .replication_factor = 1};
    };

    size_t num_topics = 20;
    for (int i = 0; i < num_topics; ++i) {
        auto creatable_topic = make_topic(fmt::format("topic-{}", i));
        try {
            auto res = client.create_topic(creatable_topic).get();
            BOOST_REQUIRE_EQUAL(res.data.topics.size(), 1);
            for (auto& topic : res.data.topics) {
                BOOST_REQUIRE_EQUAL(topic.name, creatable_topic.name);
                BOOST_REQUIRE_EQUAL(topic.error_code, sql::error_code::none);
            }
        } catch (const sql::client::topic_error& ex) {
            // If we do get an error, then it should be
            // throttling_quota_exceeded. Anything else is a problem
            BOOST_REQUIRE_EQUAL(ex.topic, creatable_topic.name);
            BOOST_REQUIRE_EQUAL(
              ex.error, sql::error_code::throttling_quota_exceeded);
        }
    }
}
