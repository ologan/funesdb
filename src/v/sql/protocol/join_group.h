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
#include "sql/protocol/schemata/join_group_request.h"
#include "sql/protocol/schemata/join_group_response.h"
#include "sql/types.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <utility>

namespace sql {

struct join_group_request final {
    using api_type = join_group_api;

    join_group_request_data data;

    join_group_request() = default;
    join_group_request(const join_group_request&) = delete;
    join_group_request& operator=(const join_group_request&) = delete;
    join_group_request(join_group_request&&) = default;
    join_group_request& operator=(join_group_request&&) = delete;

    // extra context from request header set in decode
    api_version version;
    std::optional<sql::client_id> client_id;
    sql::client_host client_host;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    /**
     * Convert the request member protocol list into the type used internally to
     * group membership. We maintain two different types because the internal
     * type is also the type stored on disk and we do not want it to be tied to
     * the type produced by code generation.
     */
    std::vector<member_protocol> native_member_protocols() const {
        std::vector<member_protocol> res;
        std::transform(
          data.protocols.cbegin(),
          data.protocols.cend(),
          std::back_inserter(res),
          [](const join_group_request_protocol& p) {
              return member_protocol{p.name, p.metadata};
          });
        return res;
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const join_group_request& r) {
        return os << r.data;
    }
};

static inline const sql::member_id no_member("");
static inline const sql::member_id no_leader("");
static constexpr sql::generation_id no_generation(-1);
static inline const sql::protocol_name no_protocol("");

struct join_group_response final {
    using api_type = join_group_api;

    join_group_response_data data;

    join_group_response() = default;

    join_group_response(sql::member_id member_id, sql::error_code error)
      : join_group_response(
        error, no_generation, no_protocol, no_leader, member_id) {}

    explicit join_group_response(sql::error_code error)
      : join_group_response(no_member, error) {}

    join_group_response(const join_group_request& r, sql::error_code error)
      : join_group_response(r.data.member_id, error) {}

    join_group_response(
      sql::error_code error,
      sql::generation_id generation_id,
      sql::protocol_name protocol_name,
      sql::member_id leader_id,
      sql::member_id member_id,
      std::vector<join_group_response_member> members = {}) {
        data.throttle_time_ms = std::chrono::milliseconds(0);
        data.error_code = error;
        data.generation_id = generation_id;
        data.protocol_name = std::move(protocol_name);
        data.leader = std::move(leader_id);
        data.member_id = std::move(member_id);
        data.members = std::move(members);
    }

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const join_group_response& r) {
        return os << r.data;
    }
};

inline join_group_response
make_join_error(sql::member_id member_id, error_code error) {
    return join_group_response(
      error, no_generation, no_protocol, no_leader, std::move(member_id));
}

// group membership helper to compare a protocol set from the wire with our
// internal type without doing a full type conversion.
inline bool operator==(
  const std::vector<join_group_request_protocol>& a,
  const std::vector<member_protocol>& b) {
    return std::equal(
      a.cbegin(),
      a.cend(),
      b.cbegin(),
      b.cend(),
      [](const join_group_request_protocol& a, const member_protocol& b) {
          return a.name == b.name && a.metadata == b.metadata;
      });
}

// group membership helper to compare a protocol set from the wire with our
// internal type without doing a full type conversion.
inline bool operator!=(
  const std::vector<join_group_request_protocol>& a,
  const std::vector<member_protocol>& b) {
    return !(a == b);
}

} // namespace sql
