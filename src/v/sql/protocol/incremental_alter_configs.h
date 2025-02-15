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
#include "bytes/iobuf.h"
#include "sql/protocol/errors.h"
#include "sql/protocol/schemata/incremental_alter_configs_request.h"
#include "sql/protocol/schemata/incremental_alter_configs_response.h"
#include "sql/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

namespace sql {

struct incremental_alter_configs_request final {
    using api_type = incremental_alter_configs_api;

    incremental_alter_configs_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const incremental_alter_configs_request& r) {
        return os << r.data;
    }
};

struct incremental_alter_configs_response final {
    using api_type = incremental_alter_configs_api;

    incremental_alter_configs_response_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const incremental_alter_configs_response& r) {
        return os << r.data;
    }
};

} // namespace sql
