/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "sql/server/partition_proxy.h"
#include "sql/types.h"

#include <compare>

namespace sql::details {

inline sql::error_code check_leader_epoch(
  sql::leader_epoch request_epoch, const sql::partition_proxy& p) {
    /**
     * no leader epoch provided, skip validation
     */
    if (request_epoch < 0) {
        return error_code::none;
    }
    auto const partition_epoch = p.leader_epoch();

    if (request_epoch > partition_epoch) {
        return error_code::unknown_leader_epoch;
    } else if (request_epoch < partition_epoch) {
        return error_code::fenced_leader_epoch;
    } else {
        return error_code::none;
    }
}

} // namespace sql::details
