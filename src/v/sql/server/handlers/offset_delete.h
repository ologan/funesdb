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
#include "sql/protocol/offset_delete.h"
#include "sql/server/handlers/handler.h"
#include "sql/server/response.h"

namespace sql {

using offset_delete_handler = single_stage_handler<offset_delete_api, 0, 0>;

} // namespace sql
