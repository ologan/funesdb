// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/client/fetch_session.h"

#include "sql/client/test/utils.h"
#include "sql/protocol/batch_consumer.h"
#include "sql/protocol/batch_reader.h"
#include "sql/protocol/errors.h"
#include "sql/protocol/fetch.h"
#include "sql/types.h"
#include "model/fundamental.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

namespace k = sql;
namespace kc = k::client;

std::optional<sql::batch_reader>
make_record_set(model::offset offset, std::optional<size_t> count) {
    if (!count) {
        return std::nullopt;
    }
    iobuf record_set;
    auto writer{sql::protocol::encoder(record_set)};
    sql::protocol::writer_serialize_batch(writer, make_batch(offset, *count));
    return sql::batch_reader{std::move(record_set)};
}

sql::fetch_response make_fetch_response(
  sql::fetch_session_id s_id,
  model::topic_partition_view tpv,
  std::optional<sql::batch_reader> record_set) {
    sql::fetch_response res{
      .data = {
        .throttle_time_ms = std::chrono::milliseconds{0},
        .error_code = sql::error_code::none,
        .session_id = s_id,
        .topics{}}};
    sql::fetch_response::partition p{.name = tpv.topic};
    p.partitions.push_back(sql::fetch_response::partition_response{
      .partition_index = tpv.partition,
      .error_code = sql::error_code::none,
      .high_watermark = model::offset{-1},
      .last_stable_offset = model::offset{-1},
      .log_start_offset = model::offset{-1},
      .aborted = {},
      .records{std::move(record_set)}});
    res.data.topics.push_back(std::move(p));
    return res;
}

struct context {
    const sql::fetch_session_id fetch_session_id{42};
    const model::topic_partition tp{
      model::topic{"test_topic"}, model::partition_id{2}};
    const size_t record_set_size{0};
    sql::fetch_session_epoch expected_epoch{
      sql::initial_fetch_session_epoch};
    model::offset expected_offset{0};

    bool
    apply_fetch_response(kc::fetch_session& s, std::optional<size_t> count) {
        auto res = make_fetch_response(
          fetch_session_id, tp, make_record_set(expected_offset, count));
        expected_offset += count.value_or(0);
        ++expected_epoch;
        return s.apply(res);
    }
};

SEASTAR_THREAD_TEST_CASE(test_fetch_session) {
    context ctx;
    kc::fetch_session s;

    BOOST_REQUIRE_EQUAL(s.id(), sql::invalid_fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), sql::initial_fetch_session_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), model::offset{0});

    // Apply some records
    BOOST_REQUIRE(ctx.apply_fetch_response(s, 8));
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);

    // Apply more records
    BOOST_REQUIRE(ctx.apply_fetch_response(s, 8));
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);
}

SEASTAR_THREAD_TEST_CASE(test_fetch_session_null_record_set) {
    context ctx;
    kc::fetch_session s;

    // Apply some records
    BOOST_REQUIRE(ctx.apply_fetch_response(s, 8));
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);

    // Apply nullopt record_set
    BOOST_REQUIRE(ctx.apply_fetch_response(s, std::nullopt));
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);
}

SEASTAR_THREAD_TEST_CASE(test_fetch_session_empty_record_set) {
    context ctx;
    kc::fetch_session s;

    // Apply some records
    BOOST_REQUIRE(ctx.apply_fetch_response(s, 8));
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);

    // Apply 0 records
    BOOST_REQUIRE(ctx.apply_fetch_response(s, 0));
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);
}

SEASTAR_THREAD_TEST_CASE(test_fetch_session_make_offset_commit_request_all) {
    context ctx;
    kc::fetch_session s;

    BOOST_REQUIRE_EQUAL(s.id(), sql::invalid_fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), sql::initial_fetch_session_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), model::offset{0});

    // Apply some records
    BOOST_REQUIRE(ctx.apply_fetch_response(s, 8));
    BOOST_REQUIRE_EQUAL(s.id(), ctx.fetch_session_id);
    BOOST_REQUIRE_EQUAL(s.epoch(), ctx.expected_epoch);
    BOOST_REQUIRE_EQUAL(s.offset(ctx.tp), ctx.expected_offset);

    auto req = s.make_offset_commit_request();
    BOOST_REQUIRE_EQUAL(req.size(), 1);
    BOOST_REQUIRE_EQUAL(req[0].name, ctx.tp.topic);
    BOOST_REQUIRE_EQUAL(req[0].partitions.size(), 1);
    const auto partition = req[0].partitions[0];
    BOOST_REQUIRE_EQUAL(partition.partition_index, ctx.tp.partition);
    BOOST_REQUIRE_EQUAL(
      partition.committed_leader_epoch, sql::invalid_leader_epoch);
    BOOST_REQUIRE_EQUAL(partition.committed_offset, ctx.expected_offset - 1);
}
