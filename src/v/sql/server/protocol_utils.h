/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "sql/protocol/types.h"
#include "sql/server/request_context.h"
#include "sql/server/response.h"
#include "sql/types.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/temporary_buffer.hh>

#include <optional>

namespace sql {

// TODO: move to iobuf_parser
ss::future<std::optional<request_header>> parse_header(ss::input_stream<char>&);

ss::scattered_message<char> response_as_scattered(response_ptr response);

} // namespace sql
