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

#include "sql/protocol/errors.h"
#include "sql/protocol/offset_commit.h"
#include "sql/protocol/offset_fetch.h"
#include "sql/protocol/schemata/offset_commit_request.h"
#include "funesproxy/json/requests/partition_offsets.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

namespace funesproxy::json {

inline std::vector<sql::offset_commit_request_topic>
partition_offsets_request_to_offset_commit_request(
  std::vector<topic_partition_offset> tps) {
    std::vector<sql::offset_commit_request_topic> res;
    if (tps.empty()) {
        return res;
    }

    std::sort(tps.begin(), tps.end());
    res.push_back(sql::offset_commit_request_topic{tps.front().topic, {}});
    for (auto& tp : tps) {
        if (tp.topic != res.back().name) {
            res.push_back(
              sql::offset_commit_request_topic{std::move(tp.topic), {}});
        }
        res.back().partitions.push_back(sql::offset_commit_request_partition{
          .partition_index = tp.partition, .committed_offset = tp.offset});
    }
    return res;
}

} // namespace funesproxy::json
