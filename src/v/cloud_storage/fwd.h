/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Funes Enterprise file under the Funes Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/funes/blob/master/licenses/rcl.md
 */

#pragma once

namespace cloud_storage {

class cache;
class partition_recovery_manager;
class remote;
class remote_partition;
class remote_segment;
class partition_manifest;
class topic_manifest;
class partition_probe;
class async_manifest_view;
class partition_probe;

struct log_recovery_result;
struct offset_range;
struct topic_recovery_service;

} // namespace cloud_storage
