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
#include "sql/protocol/produce.h"
#include "sql/server/handlers/handler.h"

namespace sql {

using produce_handler = two_phase_handler<produce_api, 0, 7>;

} // namespace sql
