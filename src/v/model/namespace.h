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
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/smp.hh>

namespace model {

inline const model::ns funes_ns("funes");

inline const model::ntp controller_ntp(
  funes_ns, model::topic("controller"), model::partition_id(0));

/*
 * The kvstore is organized as an ntp with a partition per core.
 */
inline const model::topic kvstore_topic("kvstore");
inline model::ntp kvstore_ntp(ss::shard_id shard) {
    return {funes_ns, kvstore_topic, model::partition_id(shard)};
}

inline const model::ns sql_namespace("sql");

inline const model::ns sql_internal_namespace("sql_internal");

inline const model::topic sql_consumer_offsets_topic("__consumer_offsets");

inline const model::topic_namespace sql_consumer_offsets_nt(
  model::sql_namespace, sql_consumer_offsets_topic);

inline const model::topic sql_audit_logging_topic("__audit_log");

inline const model::topic tx_manager_topic("tx");
inline const model::topic_namespace
  tx_manager_nt(model::sql_internal_namespace, tx_manager_topic);
// Previously we had only one partition in tm.
// Now we support multiple partitions.
// legacy_tm_ntp exists to support previous behaviour
inline const model::ntp legacy_tm_ntp(
  model::sql_internal_namespace,
  model::tx_manager_topic,
  model::partition_id(0));

inline const model::topic id_allocator_topic("id_allocator");
inline const model::topic_namespace
  id_allocator_nt(model::sql_internal_namespace, id_allocator_topic);
inline const model::ntp id_allocator_ntp(
  model::sql_internal_namespace,
  model::id_allocator_topic,
  model::partition_id(0));

inline const model::topic tx_registry_topic("tx_registry");
inline const model::topic_namespace
  tx_registry_nt(model::sql_internal_namespace, tx_registry_topic);
inline const model::ntp tx_registry_ntp(
  model::sql_internal_namespace,
  model::tx_registry_topic,
  model::partition_id(0));

inline const model::topic_partition schema_registry_internal_tp{
  model::topic{"_schemas"}, model::partition_id{0}};

inline const model::ntp wasm_binaries_internal_ntp(
  model::funes_ns, model::topic("wasm_binaries"), model::partition_id(0));

inline bool is_user_topic(topic_namespace_view tp_ns) {
    return tp_ns.ns == sql_namespace
           && tp_ns.tp != sql_consumer_offsets_topic
           && tp_ns.tp != schema_registry_internal_tp.topic;
}

inline bool is_user_topic(const ntp& ntp) {
    return is_user_topic(topic_namespace_view{ntp});
}

} // namespace model
