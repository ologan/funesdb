/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Funes Enterprise file under the Funes Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/funes/blob/master/licenses/rcl.md
 */
#pragma once

#include "sql/client/transport.h"

#include <seastar/core/coroutine.hh>

namespace tests {

// Wrapper around a SQL transport that encapsulates fetching the latest
// offset for a given term.
//
// The primary goal of this is to allow tests to delete without dealing
// explicitly with the SQL schemata. To that end, it exposes a
// protocol-agnostic API.
class sql_offset_for_epoch_transport {
public:
    // NOTE: returned offsets are sql offsets
    using pid_to_offset_map_t
      = absl::flat_hash_map<model::partition_id, model::offset>;
    using pid_to_term_map_t
      = absl::flat_hash_map<model::partition_id, model::term_id>;

    explicit sql_offset_for_epoch_transport(sql::client::transport&& t)
      : _transport(std::move(t)) {}

    ss::future<> start() { return _transport.connect(); }

    ss::future<pid_to_offset_map_t> offsets_for_leaders(
      model::topic topic_name, pid_to_term_map_t term_per_partition);

    ss::future<model::offset> offset_for_leader_partition(
      model::topic topic_name, model::partition_id pid, model::term_id t) {
        pid_to_term_map_t m;
        m.emplace(pid, t);
        auto out_map = co_await offsets_for_leaders(
          std::move(topic_name), std::move(m));
        co_return out_map[pid];
    }

private:
    sql::client::transport _transport;
};

} // namespace tests
