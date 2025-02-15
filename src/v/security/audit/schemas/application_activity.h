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

#include "security/audit/schemas/schemas.h"
#include "security/audit/schemas/types.h"

namespace security::audit {
// Describe generate CRUD API activities
// https://schema.ocsf.io/1.0.0/classes/api_activity?extensions=
class api_activity final : public ocsf_base_event<api_activity> {
public:
    enum class activity_id : uint8_t {
        unknown = 0,
        create = 1,
        read = 2,
        update = 3,
        delete_id = 4,
        other = 99
    };

    enum class status_id : uint8_t {
        unknown = 0,
        success = 1,
        failure = 2,
        other = 99
    };

    api_activity(
      activity_id activity_id,
      actor actor,
      api api,
      network_endpoint dst_endpoint,
      std::optional<http_request> http_request,
      std::vector<resource_detail> resources,
      severity_id severity_id,
      network_endpoint src_endpoint,
      status_id status_id,
      timestamp_t time,
      api_activity_unmapped unmapped)
      : ocsf_base_event(
        category_uid::application_activity,
        class_uid::api_activity,
        ocsf_funes_metadata_cloud_profile(),
        severity_id,
        time,
        activity_id)
      , _activity_id(activity_id)
      , _actor(std::move(actor))
      , _api(std::move(api))
      , _cloud(cloud{.provider = ""})
      , _dst_endpoint(std::move(dst_endpoint))
      , _http_request(std::move(http_request))
      , _resources(std::move(resources))
      , _src_endpoint(std::move(src_endpoint))
      , _status_id(status_id)
      , _unmapped(std::move(unmapped)) {}

    auto equality_fields() const {
        return std::tie(
          _activity_id,
          _actor,
          _api,
          _dst_endpoint.addr.host(),
          _http_request,
          _resources,
          _src_endpoint.addr.host(),
          _status_id,
          _unmapped);
    }

private:
    activity_id _activity_id;
    actor _actor;
    api _api;
    cloud _cloud;
    network_endpoint _dst_endpoint;
    std::optional<http_request> _http_request;
    std::vector<resource_detail> _resources;
    network_endpoint _src_endpoint;
    status_id _status_id;
    api_activity_unmapped _unmapped;

    size_t hash() const final { return std::hash<api_activity>()(*this); }

    friend inline void rjson_serialize(
      ::json::Writer<::json::StringBuffer>& w, const api_activity& a) {
        w.StartObject();
        a.rjson_serialize(w);
        w.Key("activity_id");
        ::json::rjson_serialize(w, a._activity_id);
        w.Key("actor");
        ::json::rjson_serialize(w, a._actor);
        w.Key("api");
        ::json::rjson_serialize(w, a._api);
        w.Key("cloud");
        ::json::rjson_serialize(w, a._cloud);
        w.Key("dst_endpoint");
        ::json::rjson_serialize(w, a._dst_endpoint);
        if (a._http_request) {
            w.Key("http_request");
            ::json::rjson_serialize(w, a._http_request);
        }
        if (!a._resources.empty()) {
            w.Key("resources");
            ::json::rjson_serialize(w, a._resources);
        }

        w.Key("src_endpoint");
        ::json::rjson_serialize(w, a._src_endpoint);
        w.Key("status_id");
        ::json::rjson_serialize(w, a._status_id);
        w.Key("unmapped");
        ::json::rjson_serialize(w, a._unmapped);
        w.EndObject();
    }
};

// Reports the installation, removal, start, or stop of an application or
// service
// https://schema.ocsf.io/1.0.0/classes/application_lifecycle?extensions=
class application_lifecycle final
  : public ocsf_base_event<application_lifecycle> {
public:
    enum class activity_id : uint8_t {
        unknown = 0,
        install = 1,
        remove = 2,
        start = 3,
        stop = 4,
        other = 99
    };

    application_lifecycle(
      activity_id activity_id,
      product app,
      severity_id severity_id,
      timestamp_t time)
      : ocsf_base_event(
        category_uid::application_activity,
        class_uid::application_lifecycle,
        severity_id,
        time,
        activity_id)
      , _activity_id(activity_id)
      , _app(std::move(app)) {}

    auto equality_fields() const { return std::tie(_activity_id, _app); }

private:
    activity_id _activity_id;
    product _app;

    size_t hash() const final {
        return std::hash<application_lifecycle>()(*this);
    }

    friend inline void rjson_serialize(
      ::json::Writer<::json::StringBuffer>& w, const application_lifecycle& a) {
        w.StartObject();
        a.rjson_serialize(w);

        w.Key("activity_id");
        ::json::rjson_serialize(w, a._activity_id);
        w.Key("app");
        ::json::rjson_serialize(w, a._app);

        w.EndObject();
    }
};
} // namespace security::audit
