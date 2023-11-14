/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Funes Enterprise file under the Funes Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/funes/blob/master/licenses/rcl.md
 */
#include "sql/server/tests/offset_for_leader_epoch_utils.h"

#include "sql/client/transport.h"
#include "sql/protocol/errors.h"
#include "sql/protocol/offset_for_leader_epoch.h"
#include "sql/protocol/schemata/offset_for_leader_epoch_request.h"
#include "sql/protocol/schemata/offset_for_leader_epoch_response.h"
#include "sql/protocol/types.h"
#include "vlog.h"

#include <seastar/util/log.hh>

namespace tests {

ss::future<sql_offset_for_epoch_transport::pid_to_offset_map_t>
sql_offset_for_epoch_transport::offsets_for_leaders(
  model::topic topic_name, pid_to_term_map_t term_per_partition) {
    sql::offset_for_leader_epoch_request req;
    sql::offset_for_leader_topic t{topic_name, {}, {}};
    for (const auto [pid, term] : term_per_partition) {
        t.partitions.emplace_back(sql::offset_for_leader_partition{
          .partition = pid,
          .current_leader_epoch = sql::leader_epoch{-1},
          .leader_epoch = sql::leader_epoch{term()},
        });
    }
    req.data.topics.emplace_back(std::move(t));
    auto resp = co_await _transport.dispatch(std::move(req));
    if (resp.data.topics.size() != 1) {
        throw std::runtime_error(
          fmt::format("Expected 1 topic, got {}", resp.data.topics.size()));
    }
    pid_to_offset_map_t ret;
    for (const auto& p_res : resp.data.topics[0].partitions) {
        // NOTE: offset_out_of_range returns with a low watermark of -1
        if (p_res.error_code == sql::error_code::none) {
            ret.emplace(p_res.partition, p_res.end_offset);
            continue;
        }
        throw std::runtime_error(fmt::format(
          "Error for partition {}: {}", p_res.partition, p_res.error_code));
    }
    co_return ret;
}

} // namespace tests
