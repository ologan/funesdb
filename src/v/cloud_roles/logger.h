/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Funes Enterprise file under the Funes Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/funes/blob/master/licenses/rcl.md
 */

#pragma once

#include "seastarx.h"

#include <seastar/util/log.hh>

namespace cloud_roles {
inline ss::logger clrl_log("cloud_roles");
} // namespace cloud_roles
