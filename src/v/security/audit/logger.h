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
#include <seastar/util/log.hh>

namespace security::audit {
extern seastar::logger adtlog;
} // namespace security::audit
