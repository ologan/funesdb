// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/protocol/create_topics.h"
#include "sql/protocol/delete_topics.h"
#include "sql/protocol/errors.h"
#include "sql/protocol/metadata.h"
#include "sql/protocol/schemata/create_topics_request.h"
#include "sql/server/handlers/topics/types.h"
#include "sql/types.h"
#include "funes/application.h"
#include "funes/tests/fixture.h"
#include "test_utils/async.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/test/tools/old/interface.hpp>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <optional>
#include <vector>

using namespace std::chrono_literals; // NOLINT

class delete_topics_request_fixture : public funes_thread_fixture {
public:
    void create_topic(ss::sstring tp, uint32_t partitions, uint16_t rf) {
        sql::creatable_topic topic;

        topic.name = model::topic(tp);
        topic.num_partitions = partitions;
        topic.replication_factor = rf;

        std::vector<sql::creatable_topic> topics;
        topics.push_back(std::move(topic));
        auto req = sql::create_topics_request{.data{
          .topics = std::move(topics),
          .timeout_ms = 10s,
          .validate_only = false,
        }};

        auto client = make_sql_client().get0();
        client.connect().get0();
        auto resp
          = client.dispatch(std::move(req), sql::api_version(2)).get0();
    }

    sql::delete_topics_response
    send_delete_topics_request(sql::delete_topics_request req) {
        auto client = make_sql_client().get0();
        client.connect().get0();

        return client.dispatch(std::move(req), sql::api_version(2)).get0();
    }

    void
    validate_valid_delete_topics_request(sql::delete_topics_request req) {
        auto resp = send_delete_topics_request(std::move(req));
        // response have no errors
        for (const auto& r : resp.data.responses) {
            BOOST_REQUIRE_EQUAL(r.error_code, sql::error_code::none);
        }
        // topics are deleted
        for (const auto& r : resp.data.responses) {
            BOOST_REQUIRE(r.name.has_value());
            validate_topic_is_deleteted(*r.name);
        }
    }

    sql::metadata_response get_topic_metadata(const model::topic& tp) {
        auto client = make_sql_client().get0();
        client.connect().get0();
        std::vector<sql::metadata_request_topic> topics;
        topics.push_back(sql::metadata_request_topic{tp});
        sql::metadata_request md_req{
          .data = {.topics = topics, .allow_auto_topic_creation = false},
          .list_all_topics = false};
        return client.dispatch(md_req).get0();
    }

    ss::future<sql::metadata_response> get_all_metadata() {
        return make_sql_client().then([](sql::client::transport c) {
            return ss::do_with(
              std::move(c), [](sql::client::transport& client) {
                  return client.connect().then([&client] {
                      sql::metadata_request md_req{
                        .data = {
                          .topics = std::nullopt,
                          .allow_auto_topic_creation = false}};
                      return client.dispatch(
                        std::move(md_req), sql::api_version(1));
                  });
              });
        });
    }

    // https://github.com/apache/sql/blob/8e161580b859b2fcd54c59625e232b99f3bb48d0/core/src/test/scala/unit/sql/server/DeleteTopicsRequestTest.scala#L126
    void validate_topic_is_deleteted(const model::topic& tp) {
        sql::metadata_response resp = get_topic_metadata(tp);
        auto it = std::find_if(
          std::cbegin(resp.data.topics),
          std::cend(resp.data.topics),
          [tp](const sql::metadata_response::topic& md_tp) {
              return md_tp.name == tp;
          });
        BOOST_CHECK(it != resp.data.topics.end());
        BOOST_REQUIRE_NE(it->error_code, sql::error_code::none);
    }

    sql::delete_topics_request make_delete_topics_request(
      std::vector<model::topic> topics, std::chrono::milliseconds timeout) {
        sql::delete_topics_request req;
        req.data.topic_names = std::move(topics);
        req.data.timeout_ms = timeout;
        return req;
    }

    void validate_error_delete_topic_request(
      sql::delete_topics_request req,
      absl::flat_hash_map<model::topic, sql::error_code> expected_response) {
        auto resp = send_delete_topics_request(std::move(req));

        BOOST_REQUIRE_EQUAL(
          resp.data.responses.size(), expected_response.size());

        for (const auto& tp_r : resp.data.responses) {
            BOOST_REQUIRE(tp_r.name.has_value());
            BOOST_REQUIRE_EQUAL(
              tp_r.error_code, expected_response.find(*tp_r.name)->second);
        }
    }
};

// https://github.com/apache/sql/blob/8e161580b859b2fcd54c59625e232b99f3bb48d0/core/src/test/scala/unit/sql/server/DeleteTopicsRequestTest.scala#L35
FIXTURE_TEST(delete_valid_topics, delete_topics_request_fixture) {
    wait_for_controller_leadership().get();

    create_topic("topic-1", 1, 1);
    // Single topic
    validate_valid_delete_topics_request(
      make_delete_topics_request({model::topic("topic-1")}, 10s));

    create_topic("topic-2", 5, 1);
    create_topic("topic-3", 1, 1);
    // Multi topic
    validate_valid_delete_topics_request(make_delete_topics_request(
      {model::topic("topic-2"), model::topic("topic-3")}, 10s));
}

#if 0
// TODO(michal) - fix test fixture.
//
// https://github.com/apache/sql/blob/8e161580b859b2fcd54c59625e232b99f3bb48d0/core/src/test/scala/unit/sql/server/DeleteTopicsRequestTest.scala#L62
FIXTURE_TEST(error_delete_topics_request, delete_topics_request_fixture) {
    wait_for_controller_leadership().get();

    // Basic
    validate_error_delete_topic_request(
      make_delete_topics_request({model::topic("invalid-topic")}, 10s),
      {{model::topic("invalid-topic"),
        sql::error_code::unknown_topic_or_partition}});

    // Partial
    create_topic("partial-topic-1", 1, 1);

    validate_error_delete_topic_request(
      make_delete_topics_request(
        {model::topic("partial-topic-1"),
         model::topic("partial-invalid-topic")},
        10s),
      {{model::topic("partial-topic-1"), sql::error_code::none},
       {model::topic("partial-invalid-topic"),
        sql::error_code::unknown_topic_or_partition}});

    // Timeout
    create_topic("timeout-topic", 1, 1);
    auto tp = model::topic("timeout-topic");
    validate_error_delete_topic_request(
      make_delete_topics_request({tp}, 0ms),
      {{tp, sql::error_code::request_timed_out}});

    tests::cooperative_spin_wait_with_timeout(5s, [this, tp] {
        return get_all_metadata().then(
          [](sql::metadata_response resp) { return resp.topics.empty(); });
    }).get0();

    validate_topic_is_deleteted(model::topic("timeout-topic"));
}
#endif
