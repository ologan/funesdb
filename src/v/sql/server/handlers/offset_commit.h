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
#include "sql/protocol/offset_commit.h"
#include "sql/server/handlers/handler.h"
#include "sql/server/response.h"

namespace sql {

// in version 0 sql stores offsets in zookeeper. if we ever need to
// support version 0 then we need to do some code review to see if this has
// any implications on semantics.
using offset_commit_handler = two_phase_handler<offset_commit_api, 1, 8>;

} // namespace sql
