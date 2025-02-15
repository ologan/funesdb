/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Funes Enterprise file under the Funes Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/funes/blob/master/licenses/rcl.md
 */

#include "cluster/tests/topic_properties_generator.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "security/acl.h"

// Test utilities to facilitate creating metadata.

namespace cluster::cloud_metadata {

inline security::license get_test_license() {
    const char* sample_valid_license = std::getenv("FUNES_SAMPLE_LICENSE");
    const ss::sstring license_str{sample_valid_license};
    return security::make_license(license_str);
}

inline security::acl_binding binding_for_user(const ss::sstring& user) {
    const security::acl_principal principal{
      security::principal_type::ephemeral_user, user};
    security::acl_entry acl_entry{
      principal,
      security::acl_host::wildcard_host(),
      security::acl_operation::all,
      security::acl_permission::allow};
    auto binding = security::acl_binding{
      security::resource_pattern{
        security::resource_type::topic,
        security::resource_pattern::wildcard,
        security::pattern_type::literal},
      acl_entry};
    return binding;
}

inline topic_properties uploadable_topic_properties() {
    auto props = random_topic_properties();
    if (
      !props.shadow_indexing.has_value()
      || !is_archival_enabled(props.shadow_indexing.value())) {
        props.shadow_indexing.emplace(model::shadow_indexing_mode::full);
    }
    // Explicitly fix revision ID. Topic recovery can't deserialize
    // negatives correctly.
    props.remote_topic_properties->remote_revision = model::initial_revision_id{
      0};
    props.recovery = false;
    props.read_replica = false;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    return props;
}

inline topic_properties non_remote_topic_properties() {
    auto props = random_topic_properties();
    props.shadow_indexing = model::shadow_indexing_mode::disabled;
    props.recovery = false;
    props.read_replica = false;
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    return props;
}

} // namespace cluster::cloud_metadata
