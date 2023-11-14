/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Funes Enterprise file under the Funes Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/funes/blob/master/licenses/rcl.md
 */
#include "sql/server/tests/delete_records_utils.h"

#include "sql/client/transport.h"
#include "sql/protocol/delete_records.h"
#include "sql/protocol/errors.h"
#include "sql/protocol/schemata/delete_records_request.h"
#include "sql/protocol/schemata/delete_records_response.h"
#include "vlog.h"

#include <seastar/util/log.hh>

namespace tests {

ss::future<sql_delete_records_transport::pid_to_offset_map_t>
sql_delete_records_transport::delete_records(
  model::topic topic_name,
  pid_to_offset_map_t offsets_per_partition,
  std::chrono::milliseconds timeout) {
    sql::delete_records_request req;
    req.data.timeout_ms = timeout;
    sql::delete_records_topic tp;
    tp.name = std::move(topic_name);
    for (auto& [pid, offset] : offsets_per_partition) {
        sql::delete_records_partition p;
        p.partition_index = pid;
        p.offset = offset;
        tp.partitions.emplace_back(std::move(p));
    }
    req.data.topics.emplace_back(std::move(tp));
    auto resp = co_await _transport.dispatch(std::move(req));
    if (resp.data.topics.size() != 1) {
        throw std::runtime_error(
          fmt::format("Expected 1 topic, got {}", resp.data.topics.size()));
    }
    pid_to_offset_map_t ret;
    for (const auto& p_res : resp.data.topics[0].partitions) {
        // NOTE: offset_out_of_range returns with a low watermark of -1
        if (
          p_res.error_code == sql::error_code::offset_out_of_range
          || p_res.error_code == sql::error_code::none) {
            ret.emplace(p_res.partition_index, p_res.low_watermark);
            continue;
        }
        throw std::runtime_error(fmt::format(
          "Error for partition {}: {}",
          p_res.partition_index,
          p_res.error_code));
    }
    co_return ret;
}

} // namespace tests
