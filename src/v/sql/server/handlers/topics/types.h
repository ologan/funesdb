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
#include "cluster/types.h"
#include "sql/protocol/schemata/create_topics_request.h"
#include "sql/protocol/schemata/create_topics_response.h"
#include "sql/server/errors.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "utils/absl_sstring_hash.h"

#include <absl/container/flat_hash_map.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include <array>

namespace sql {

/**
 * We use `flat_hash_map` in here since those map are small and short lived.
 */
using config_map_t
  = absl::flat_hash_map<ss::sstring, ss::sstring, sstring_hash, sstring_eq>;
/**
 * Topic properties string names
 */
static constexpr std::string_view topic_property_compression
  = "compression.type";
static constexpr std::string_view topic_property_cleanup_policy
  = "cleanup.policy";
static constexpr std::string_view topic_property_compaction_strategy
  = "compaction.strategy";
static constexpr std::string_view topic_property_timestamp_type
  = "message.timestamp.type";
static constexpr std::string_view topic_property_segment_size = "segment.bytes";
static constexpr std::string_view topic_property_retention_bytes
  = "retention.bytes";
static constexpr std::string_view topic_property_retention_duration
  = "retention.ms";
static constexpr std::string_view topic_property_max_message_bytes
  = "max.message.bytes";
static constexpr std::string_view topic_property_recovery
  = "funes.remote.recovery";
static constexpr std::string_view topic_property_remote_write
  = "funes.remote.write";
static constexpr std::string_view topic_property_remote_read
  = "funes.remote.read";
static constexpr std::string_view topic_property_read_replica
  = "funes.remote.readreplica";
static constexpr std::string_view topic_property_retention_local_target_bytes
  = "retention.local.target.bytes";
static constexpr std::string_view topic_property_retention_local_target_ms
  = "retention.local.target.ms";
static constexpr std::string_view topic_property_replication_factor
  = "replication.factor";
static constexpr std::string_view topic_property_remote_delete
  = "funes.remote.delete";
static constexpr std::string_view topic_property_segment_ms = "segment.ms";

// Server side schema id validation
static constexpr std::string_view topic_property_record_key_schema_id_validation
  = "funes.key.schema.id.validation";
static constexpr std::string_view
  topic_property_record_key_subject_name_strategy
  = "funes.key.subject.name.strategy";
static constexpr std::string_view
  topic_property_record_value_schema_id_validation
  = "funes.value.schema.id.validation";
static constexpr std::string_view
  topic_property_record_value_subject_name_strategy
  = "funes.value.subject.name.strategy";

// Server side schema id validation (compat names)
static constexpr std::string_view
  topic_property_record_key_schema_id_validation_compat
  = "confluent.key.schema.validation";
static constexpr std::string_view
  topic_property_record_key_subject_name_strategy_compat
  = "confluent.key.subject.name.strategy";
static constexpr std::string_view
  topic_property_record_value_schema_id_validation_compat
  = "confluent.value.schema.validation";
static constexpr std::string_view
  topic_property_record_value_subject_name_strategy_compat
  = "confluent.value.subject.name.strategy";

static constexpr std::string_view
  topic_property_initial_retention_local_target_bytes
  = "initial.retention.local.target.bytes";
static constexpr std::string_view
  topic_property_initial_retention_local_target_ms
  = "initial.retention.local.target.ms";

// SQL topic properties that is not relevant for Funes
// Or cannot be altered with sql alter handler
static constexpr std::array<std::string_view, 20> allowlist_topic_noop_confs = {

  // Not used in Funes
  "unclean.leader.election.enable",
  "message.downconversion.enable",
  "segment.index.bytes",
  "segment.jitter.ms",
  "min.insync.replicas",
  "min.compaction.lag.ms",
  "min.cleanable.dirty.ratio",
  "message.timestamp.difference.max.ms",
  "message.format.version",
  "max.compaction.lag.ms",
  "leader.replication.throttled.replicas",
  "index.interval.bytes",
  "follower.replication.throttled.replicas",
  "flush.ms",
  "flush.messages",
  "file.delete.delay.ms",
  "delete.retention.ms",
  "preallocate",
};

/// \brief Type representing SQL protocol response from
/// CreateTopics, DeleteTopics and CreatePartitions requests
/// the types used here match the types used in SQL protocol specification
struct topic_op_result {
    model::topic topic;
    error_code ec;
    std::optional<ss::sstring> err_msg;
};

inline creatable_topic_result
from_cluster_topic_result(const cluster::topic_result& err) {
    return {.name = err.tp_ns.tp, .error_code = map_topic_error_code(err.ec)};
}

config_map_t config_map(const std::vector<createable_topic_config>& config);
config_map_t config_map(const std::vector<creatable_topic_configs>& config);

cluster::custom_assignable_topic_configuration
to_cluster_type(const creatable_topic& t);

config_map_t from_cluster_type(const cluster::topic_properties&);
} // namespace sql
