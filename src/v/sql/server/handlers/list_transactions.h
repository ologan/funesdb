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
#include "sql/protocol/list_transactions.h"
#include "sql/protocol/schemata/list_transactions_request.h"
#include "sql/server/handlers/handler.h"

namespace sql {

using list_transactions_handler
  = single_stage_handler<list_transactions_api, 0, 0>;

}
