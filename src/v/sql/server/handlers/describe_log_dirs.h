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
#include "sql/protocol/describe_log_dirs.h"
#include "sql/server/handlers/handler.h"

namespace sql {

using describe_log_dirs_handler
  = single_stage_handler<describe_log_dirs_api, 0, 2>;

}
