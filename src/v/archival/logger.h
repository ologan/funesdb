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

#include "seastarx.h"

#include <seastar/util/log.hh>

namespace archival {
inline ss::logger archival_log("archival");
inline ss::logger upload_ctrl_log("archival-ctrl");
} // namespace archival
