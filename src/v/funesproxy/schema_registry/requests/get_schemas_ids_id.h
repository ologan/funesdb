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

#include "funesproxy/json/rjson_util.h"
#include "funesproxy/schema_registry/types.h"

namespace funesproxy::schema_registry {

struct get_schemas_ids_id_response {
    canonical_schema_definition definition;
};

inline void rjson_serialize(
  ::json::Writer<::json::StringBuffer>& w,
  const get_schemas_ids_id_response& res) {
    w.StartObject();
    if (res.definition.type() != schema_type::avro) {
        w.Key("schemaType");
        ::json::rjson_serialize(w, to_string_view(res.definition.type()));
    }
    w.Key("schema");
    ::json::rjson_serialize(w, res.definition.raw());
    if (!res.definition.refs().empty()) {
        w.Key("references");
        w.StartArray();
        for (const auto& ref : res.definition.refs()) {
            w.StartObject();
            w.Key("name");
            ::json::rjson_serialize(w, ref.name);
            w.Key("subject");
            ::json::rjson_serialize(w, ref.sub);
            w.Key("version");
            ::json::rjson_serialize(w, ref.version);
            w.EndObject();
        }
        w.EndArray();
    }
    w.EndObject();
}

} // namespace funesproxy::schema_registry
