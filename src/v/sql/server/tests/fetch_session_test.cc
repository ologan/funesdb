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
#include "sql/protocol/fetch.h"
#include "sql/server/fetch_session.h"
#include "sql/server/fetch_session_cache.h"
#include "sql/types.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "test_utils/fixture.h"

#include <seastar/core/sstring.hh>

#include <boost/range/iterator_range_core.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>
#include <boost/test/unit_test_suite.hpp>

#include <tuple>

using namespace std::chrono_literals; // NOLINT
struct fixture {
    static sql::fetch_session_partition make_fetch_partition(
      model::topic topic, model::partition_id p_id, model::offset offset) {
        return sql::fetch_session_partition{
          .topic_partition = {topic, p_id},
          .max_bytes = 1_MiB,
          .fetch_offset = offset,
          .high_watermark = offset};
    }

    static sql::fetch_request::topic
    make_fetch_request_topic(model::topic tp, int partitions_count) {
        sql::fetch_request::topic fetch_topic{
          .name = std::move(tp),
          .fetch_partitions = {},
        };

        for (int i = 0; i < partitions_count; ++i) {
            fetch_topic.fetch_partitions.push_back(
              sql::fetch_request::partition{
                .partition_index = model::partition_id(i),
                .fetch_offset = model::offset(i * 10),
                .max_bytes = 100_KiB,
              });
        }
        return fetch_topic;
    }
};

FIXTURE_TEST(test_next_epoch, fixture) {
    BOOST_REQUIRE_EQUAL(
      sql::next_epoch(sql::initial_fetch_session_epoch),
      sql::fetch_session_epoch(1));

    BOOST_REQUIRE_EQUAL(
      sql::next_epoch(sql::fetch_session_epoch(25)),
      sql::fetch_session_epoch(26));

    BOOST_REQUIRE_EQUAL(
      sql::next_epoch(sql::final_fetch_session_epoch),
      sql::final_fetch_session_epoch);
}

FIXTURE_TEST(test_fetch_session_basic_operations, fixture) {
    sql::fetch_session session(sql::fetch_session_id(123));
    struct tpo {
        model::ktp ktp;
        model::offset offset;
    };
    std::vector<tpo> expected;
    expected.reserve(20);

    for (int i = 0; i < 20; ++i) {
        expected.push_back(tpo{
          model::ktp{
            model::topic(random_generators::gen_alphanum_string(5)),
            model::partition_id(
              random_generators::get_int(i * 10, ((i + 1) * 10) - 1))},
          model::offset(random_generators::get_int(10000))});

        auto& t = expected.back();
        session.partitions().emplace(fixture::make_fetch_partition(
          t.ktp.get_topic(), t.ktp.get_partition(), t.offset));
    }

    BOOST_TEST_MESSAGE("test insertion order iteration");
    size_t i = 0;
    auto rng = boost::make_iterator_range(
      session.partitions().cbegin_insertion_order(),
      session.partitions().cend_insertion_order());

    for (auto fp : rng) {
        BOOST_REQUIRE_EQUAL(fp.topic_partition, expected[i].ktp);
        BOOST_REQUIRE_EQUAL(fp.fetch_offset, expected[i].offset);
        ++i;
    }

    BOOST_TEST_MESSAGE("test lookup");
    for (auto& t : expected) {
        const auto& key = t.ktp.as_tp_view();
        BOOST_REQUIRE(session.partitions().contains(key));
        BOOST_REQUIRE(
          session.partitions().find(key) != session.partitions().end());
    }

    auto not_existing = model::topic_partition(
      model::topic("123456"), model::partition_id(9999));

    BOOST_REQUIRE(!session.partitions().contains(not_existing));
    BOOST_REQUIRE(
      session.partitions().find(not_existing) == session.partitions().end());

    BOOST_TEST_MESSAGE("test erase");

    const auto& key = expected[0].ktp.as_tp_view();
    auto mem_usage_before = session.mem_usage();
    session.partitions().erase(key);
    BOOST_REQUIRE(!session.partitions().contains(key));
    BOOST_REQUIRE(session.partitions().find(key) == session.partitions().end());

    BOOST_REQUIRE_LT(session.mem_usage(), mem_usage_before);
}

FIXTURE_TEST(test_session_operations, fixture) {
    sql::fetch_session_cache cache(120s);
    sql::fetch_request req;
    req.data.session_epoch = sql::initial_fetch_session_epoch;
    req.data.session_id = sql::invalid_fetch_session_id;
    req.data.topics = {make_fetch_request_topic(model::topic("test"), 3)};
    {
        BOOST_TEST_MESSAGE("create new session");
        auto ctx = cache.maybe_get_session(req);

        BOOST_REQUIRE_EQUAL(ctx.error(), sql::error_code::none);
        BOOST_REQUIRE_EQUAL(ctx.has_error(), false);
        // first fetch has to be full fetch
        BOOST_REQUIRE_EQUAL(ctx.is_full_fetch(), true);
        BOOST_REQUIRE_EQUAL(ctx.is_sessionless(), false);
        BOOST_REQUIRE_NE(ctx.session().get(), nullptr);
        auto rng = boost::make_iterator_range(
          ctx.session()->partitions().cbegin_insertion_order(),
          ctx.session()->partitions().cend_insertion_order());
        auto i = 0;
        BOOST_REQUIRE_EQUAL(ctx.session()->partitions().size(), 3);
        for (const auto& fp : rng) {
            BOOST_REQUIRE_EQUAL(
              fp.topic_partition.get_topic(), req.data.topics[0].name);
            BOOST_REQUIRE_EQUAL(
              fp.topic_partition.get_partition(),
              req.data.topics[0].fetch_partitions[i].partition_index);
            BOOST_REQUIRE_EQUAL(
              fp.fetch_offset,
              req.data.topics[0].fetch_partitions[i].fetch_offset);
            BOOST_REQUIRE_EQUAL(
              fp.max_bytes, req.data.topics[0].fetch_partitions[i].max_bytes);
            i++;
        }

        req.data.session_id = ctx.session()->id();
        req.data.session_epoch = ctx.session()->epoch();
    }

    BOOST_TEST_MESSAGE("test updating session");
    {
        req.data.topics[0].fetch_partitions.erase(
          std::next(req.data.topics[0].fetch_partitions.begin()));
        // add 2 partitons from new topic, forget one from the first topic
        req.data.topics.push_back(
          make_fetch_request_topic(model::topic("test-new"), 2));
        req.data.forgotten.push_back(sql::fetch_request::forgotten_topic{
          .name = model::topic("test"), .forgotten_partition_indexes = {1}});

        auto ctx = cache.maybe_get_session(req);

        BOOST_REQUIRE_EQUAL(ctx.error(), sql::error_code::none);
        BOOST_REQUIRE_EQUAL(ctx.has_error(), false);
        // this is an incremental fetch
        BOOST_REQUIRE_EQUAL(ctx.is_full_fetch(), false);
        BOOST_REQUIRE_EQUAL(ctx.is_sessionless(), false);
        BOOST_REQUIRE_NE(ctx.session().get(), nullptr);

        BOOST_REQUIRE_EQUAL(ctx.session()->partitions().size(), 4);
        auto rng = boost::make_iterator_range(
          ctx.session()->partitions().cbegin_insertion_order(),
          ctx.session()->partitions().cend_insertion_order());

        auto i = 0;
        // check that insertion order is preserved
        for (const auto& fp : rng) {
            auto t_idx = i < 2 ? 0 : 1;
            auto p_idx = i < 2 ? i : i - 2;

            BOOST_REQUIRE_EQUAL(
              fp.topic_partition.get_topic(), req.data.topics[t_idx].name);
            BOOST_REQUIRE_EQUAL(
              fp.topic_partition.get_partition(),
              req.data.topics[t_idx].fetch_partitions[p_idx].partition_index);
            BOOST_REQUIRE_EQUAL(
              fp.fetch_offset,
              req.data.topics[t_idx].fetch_partitions[p_idx].fetch_offset);
            BOOST_REQUIRE_EQUAL(
              fp.max_bytes,
              req.data.topics[t_idx].fetch_partitions[p_idx].max_bytes);
            i++;
        }
    }
    BOOST_TEST_MESSAGE("removing session");
    {
        req.data.session_epoch = sql::final_fetch_session_epoch;
        req.data.topics = {};

        auto ctx = cache.maybe_get_session(req);

        BOOST_REQUIRE_EQUAL(ctx.is_sessionless(), true);
        BOOST_REQUIRE_EQUAL(ctx.has_error(), false);
        BOOST_REQUIRE(ctx.session().get() == nullptr);
        BOOST_REQUIRE(cache.size() == 0);
    }
}
