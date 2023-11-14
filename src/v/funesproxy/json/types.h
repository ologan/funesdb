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

#include <cstdint>
#include <string>

namespace funesproxy::json {

enum class serialization_format : uint8_t {
    none = 0,
    v2,
    json_v2,
    binary_v2,
    schema_registry_v1_json,
    schema_registry_json,
    application_json,
    application_octet,
    unsupported
};

inline std::string_view name(serialization_format fmt) {
    switch (fmt) {
    case funesproxy::json::serialization_format::none:
        return "none";
    case funesproxy::json::serialization_format::v2:
        return "application/vnd.sql.v2+json";
    case funesproxy::json::serialization_format::json_v2:
        return "application/vnd.sql.json.v2+json";
    case funesproxy::json::serialization_format::binary_v2:
        return "application/vnd.sql.binary.v2+json";
    case funesproxy::json::serialization_format::schema_registry_v1_json:
        return "application/vnd.schemaregistry.v1+json";
    case funesproxy::json::serialization_format::schema_registry_json:
        return "application/vnd.schemaregistry.json";
    case funesproxy::json::serialization_format::application_json:
        return "application/json";
    case funesproxy::json::serialization_format::application_octet:
        return "application/octet-stream";
    case funesproxy::json::serialization_format::unsupported:
        return "unsupported";
    }
    return "(unknown format)";
}

template<typename T>
class rjson_parse_impl;

template<typename T>
class rjson_serialize_impl;

} // namespace funesproxy::json
