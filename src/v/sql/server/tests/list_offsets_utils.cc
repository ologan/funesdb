/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Funes Enterprise file under the Funes Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/funes/blob/master/licenses/rcl.md
 */
#include "sql/server/tests/list_offsets_utils.h"

#include "sql/client/transport.h"
#include "sql/protocol/errors.h"
#include "sql/protocol/list_offsets.h"
#include "sql/protocol/schemata/list_offset_request.h"
#include "sql/protocol/schemata/list_offset_response.h"

namespace tests {

ss::future<sql_list_offsets_transport::pid_to_offset_map_t>
sql_list_offsets_transport::list_offsets(
  model::topic topic_name, pid_to_timestamp_map_t ts_per_partition) {
    sql::list_offsets_request req;

    req.data.topics = {{
      .name = std::move(topic_name),
    }};
    for (const auto& [pid, ts] : ts_per_partition) {
        req.data.topics[0].partitions.emplace_back(sql::list_offset_partition{
          .partition_index = pid, .timestamp = ts});
    }
    auto resp = co_await _transport.dispatch(std::move(req));
    if (resp.data.topics.size() != 1) {
        throw std::runtime_error(
          fmt::format("Expected 1 topic, got {}", resp.data.topics.size()));
    }
    pid_to_offset_map_t ret;
    for (const auto& p_res : resp.data.topics[0].partitions) {
        if (p_res.error_code != sql::error_code::none) {
            throw std::runtime_error(fmt::format(
              "Error for partition {}: {}",
              p_res.partition_index,
              p_res.error_code));
        }
        ret[p_res.partition_index] = p_res.offset;
    }
    co_return ret;
}

ss::future<model::offset>
sql_list_offsets_transport::start_offset_for_partition(
  model::topic topic_name, model::partition_id pid) {
    return list_offset_for_partition(
      std::move(topic_name),
      pid,
      sql::list_offsets_request::earliest_timestamp);
}

ss::future<model::offset>
sql_list_offsets_transport::high_watermark_for_partition(
  model::topic topic_name, model::partition_id pid) {
    return list_offset_for_partition(
      std::move(topic_name),
      pid,
      sql::list_offsets_request::latest_timestamp);
}

} // namespace tests
