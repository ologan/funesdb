// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/config_frontend.h"
#include "config/configuration.h"
#include "sql/protocol/alter_configs.h"
#include "sql/protocol/create_topics.h"
#include "sql/protocol/describe_configs.h"
#include "sql/protocol/errors.h"
#include "sql/protocol/incremental_alter_configs.h"
#include "sql/protocol/metadata.h"
#include "sql/protocol/schemata/alter_configs_request.h"
#include "sql/protocol/schemata/describe_configs_request.h"
#include "sql/protocol/schemata/describe_configs_response.h"
#include "sql/protocol/schemata/incremental_alter_configs_request.h"
#include "sql/server/handlers/topics/types.h"
#include "sql/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "funes/tests/fixture.h"

#include <seastar/core/loop.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <absl/container/flat_hash_map.h>

#include <optional>

using namespace std::chrono_literals; // NOLINT

inline ss::logger test_log("test"); // NOLINT

class alter_config_test_fixture : public funes_thread_fixture {
public:
    void create_topic(model::topic name, int partitions) {
        model::topic_namespace tp_ns(model::sql_namespace, std::move(name));

        add_topic(tp_ns, partitions).get();

        ss::parallel_for_each(
          boost::irange(0, partitions),
          [this, tp_ns](int i) {
              return wait_for_partition_offset(
                model::ntp(tp_ns.ns, tp_ns.tp, model::partition_id(i)),
                model::offset(0));
          })
          .get();
    }

    template<typename Func>
    auto do_with_client(Func&& f) {
        return make_sql_client().then(
          [f = std::forward<Func>(f)](sql::client::transport client) mutable {
              return ss::do_with(
                std::move(client),
                [f = std::forward<Func>(f)](
                  sql::client::transport& client) mutable {
                    return client.connect().then(
                      [&client, f = std::forward<Func>(f)]() mutable {
                          return f(client);
                      });
                });
          });
    }

    sql::alter_configs_resource make_alter_topic_config_resource(
      const model::topic& topic,
      const absl::flat_hash_map<ss::sstring, ss::sstring>& properties) {
        std::vector<sql::alterable_config> cfg_list;
        cfg_list.reserve(properties.size());
        for (auto& [k, v] : properties) {
            cfg_list.push_back(sql::alterable_config{.name = k, .value = v});
        }

        return sql::alter_configs_resource{
          .resource_type = static_cast<int8_t>(
            sql::config_resource_type::topic),
          .resource_name = topic,
          .configs = std::move(cfg_list),
        };
    }

    sql::incremental_alter_configs_resource
    make_incremental_alter_topic_config_resource(
      const model::topic& topic,
      const absl::flat_hash_map<
        ss::sstring,
        std::
          pair<std::optional<ss::sstring>, sql::config_resource_operation>>&
        operations) {
        std::vector<sql::incremental_alterable_config> cfg_list;
        cfg_list.reserve(operations.size());
        for (auto& [k, v] : operations) {
            cfg_list.push_back(sql::incremental_alterable_config{
              .name = k,
              .config_operation = static_cast<int8_t>(v.second),
              .value = v.first,
            });
        }

        return sql::incremental_alter_configs_resource{
          .resource_type = static_cast<int8_t>(
            sql::config_resource_type::topic),
          .resource_name = topic,
          .configs = std::move(cfg_list),
        };
    }

    sql::describe_configs_response describe_configs(
      const ss::sstring& resource_name,
      std::optional<std::vector<ss::sstring>> configuration_keys = std::nullopt,
      sql::config_resource_type resource_type
      = sql::config_resource_type::topic) {
        sql::describe_configs_request req;

        sql::describe_configs_resource res{
          .resource_type = resource_type,
          .resource_name = resource_name,
          .configuration_keys = configuration_keys,
        };
        req.data.resources.push_back(std::move(res));
        return do_with_client([req = std::move(req)](
                                sql::client::transport& client) mutable {
                   return client.dispatch(
                     std::move(req), sql::api_version(0));
               })
          .get();
    }

    sql::alter_configs_response
    alter_configs(std::vector<sql::alter_configs_resource> resources) {
        sql::alter_configs_request req;
        req.data.resources = std::move(resources);
        return do_with_client([req = std::move(req)](
                                sql::client::transport& client) mutable {
                   return client.dispatch(
                     std::move(req), sql::api_version(0));
               })
          .get();
    }

    sql::incremental_alter_configs_response incremental_alter_configs(
      std::vector<sql::incremental_alter_configs_resource> resources) {
        sql::incremental_alter_configs_request req;
        req.data.resources = std::move(resources);
        return do_with_client([req = std::move(req)](
                                sql::client::transport& client) mutable {
                   return client.dispatch(
                     std::move(req), sql::api_version(0));
               })
          .get();
    }

    void assert_property_presented(
      const ss::sstring& resource_name,
      const ss::sstring& key,
      const sql::describe_configs_response resp,
      const bool presented) {
        auto it = std::find_if(
          resp.data.results.begin(),
          resp.data.results.end(),
          [&resource_name](const sql::describe_configs_result& res) {
              return res.resource_name == resource_name;
          });
        BOOST_REQUIRE(it != resp.data.results.end());

        auto cfg_it = std::find_if(
          it->configs.begin(),
          it->configs.end(),
          [&key](const sql::describe_configs_resource_result& res) {
              return res.name == key;
          });
        if (presented) {
            BOOST_REQUIRE(cfg_it != it->configs.end());
        } else {
            BOOST_REQUIRE(cfg_it == it->configs.end());
        }
    }

    void assert_properties_amount(
      const ss::sstring& resource_name,
      const sql::describe_configs_response resp,
      const size_t amount) {
        auto it = std::find_if(
          resp.data.results.begin(),
          resp.data.results.end(),
          [&resource_name](const sql::describe_configs_result& res) {
              return res.resource_name == resource_name;
          });
        BOOST_REQUIRE(it != resp.data.results.end());
        vlog(test_log.trace, "amount: {}", amount);
        vlog(test_log.trace, "it->configs.size(): {}", it->configs.size());
        vlog(test_log.trace, "it->configs: {}", it->configs);
        BOOST_REQUIRE(it->configs.size() == amount);
    }

    void assert_property_value(
      const model::topic& topic,
      const ss::sstring& key,
      const ss::sstring& value,
      const sql::describe_configs_response resp) {
        auto it = std::find_if(
          resp.data.results.begin(),
          resp.data.results.end(),
          [&topic](const sql::describe_configs_result& res) {
              return res.resource_name == topic;
          });
        BOOST_REQUIRE(it != resp.data.results.end());

        auto cfg_it = std::find_if(
          it->configs.begin(),
          it->configs.end(),
          [&key](const sql::describe_configs_resource_result& res) {
              return res.name == key;
          });
        BOOST_REQUIRE(cfg_it != it->configs.end());

        BOOST_REQUIRE_EQUAL(cfg_it->value, value);
    }
};

FIXTURE_TEST(
  test_broker_describe_configs_requested_properties,
  alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    sql::metadata_request req;
    req.data.topics = std::nullopt;
    auto client = make_sql_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, sql::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();
    auto broker_id = std::to_string(resp.data.brokers[0].node_id());

    std::vector<ss::sstring> all_properties = {
      "listeners",
      "advertised.listeners",
      "log.segment.bytes",
      "log.retention.bytes",
      "log.retention.ms",
      "num.partitions",
      "default.replication.factor",
      "log.dirs",
      "auto.create.topics.enable"};

    // All properies_request
    auto all_describe_resp = describe_configs(
      broker_id, std::nullopt, sql::config_resource_type::broker);
    assert_properties_amount(
      broker_id, all_describe_resp, all_properties.size());
    for (const auto& property : all_properties) {
        assert_property_presented(broker_id, property, all_describe_resp, true);
    }

    // Single properies_request
    for (const auto& request_property : all_properties) {
        std::vector<ss::sstring> request_properties = {request_property};
        auto single_describe_resp = describe_configs(
          broker_id,
          std::make_optional(request_properties),
          sql::config_resource_type::broker);
        assert_properties_amount(broker_id, single_describe_resp, 1);
        for (const auto& property : all_properties) {
            assert_property_presented(
              broker_id,
              property,
              single_describe_resp,
              property == request_property);
        }
    }

    // Group properties_request
    std::vector<ss::sstring> first_group_config_properties = {
      "listeners",
      "advertised.listeners",
      "log.segment.bytes",
      "log.retention.bytes",
      "log.retention.ms"};

    std::vector<ss::sstring> second_group_config_properties = {
      "num.partitions",
      "default.replication.factor",
      "log.dirs",
      "auto.create.topics.enable"};

    auto first_group_describe_resp = describe_configs(
      broker_id,
      std::make_optional(first_group_config_properties),
      sql::config_resource_type::broker);
    assert_properties_amount(
      broker_id,
      first_group_describe_resp,
      first_group_config_properties.size());
    for (const auto& property : first_group_config_properties) {
        assert_property_presented(
          broker_id, property, first_group_describe_resp, true);
    }
    for (const auto& property : second_group_config_properties) {
        assert_property_presented(
          broker_id, property, first_group_describe_resp, false);
    }

    auto second_group_describe_resp = describe_configs(
      broker_id,
      std::make_optional(second_group_config_properties),
      sql::config_resource_type::broker);
    assert_properties_amount(
      broker_id,
      second_group_describe_resp,
      second_group_config_properties.size());
    for (const auto& property : first_group_config_properties) {
        assert_property_presented(
          broker_id, property, second_group_describe_resp, false);
    }
    for (const auto& property : second_group_config_properties) {
        assert_property_presented(
          broker_id, property, second_group_describe_resp, true);
    }
}

FIXTURE_TEST(
  test_topic_describe_configs_requested_properties, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    app.controller->get_config_frontend()
      .invoke_on_all([](cluster::config_frontend& cfg_frontend) {
          cluster::config_update_request r{
            .upsert = {{"enable_schema_id_validation", "compat"}}};
          return cfg_frontend.patch(r, model::timeout_clock::now() + 1s)
            .discard_result();
      })
      .get();

    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    std::vector<ss::sstring> all_properties = {
      "retention.ms",
      "retention.bytes",
      "segment.bytes",
      "cleanup.policy",
      "compression.type",
      "message.timestamp.type",
      "funes.remote.read",
      "funes.remote.write",
      "max.message.bytes",
      "retention.local.target.bytes",
      "retention.local.target.ms",
      "funes.remote.delete",
      "segment.ms",
      "funes.key.schema.id.validation",
      "confluent.key.schema.validation",
      "funes.key.subject.name.strategy",
      "confluent.key.subject.name.strategy",
      "funes.value.schema.id.validation",
      "confluent.value.schema.validation",
      "funes.value.subject.name.strategy",
      "confluent.value.subject.name.strategy",
      "initial.retention.local.target.bytes",
      "initial.retention.local.target.ms"};

    // All properties_request
    auto all_describe_resp = describe_configs(test_tp);
    assert_properties_amount(test_tp, all_describe_resp, all_properties.size());
    for (const auto& property : all_properties) {
        assert_property_presented(test_tp, property, all_describe_resp, true);
    }

    // Single properties_request
    for (const auto& request_property : all_properties) {
        std::vector<ss::sstring> request_properties = {request_property};
        auto single_describe_resp = describe_configs(
          test_tp, std::make_optional(request_properties));
        assert_properties_amount(test_tp, single_describe_resp, 1);
        for (const auto& property : all_properties) {
            assert_property_presented(
              test_tp,
              property,
              single_describe_resp,
              property == request_property);
        }
    }

    // Group properties_request
    std::vector<ss::sstring> first_group_config_properties = {
      "retention.ms",
      "retention.bytes",
      "segment.bytes",
      "funes.remote.read",
      "funes.remote.write"};

    std::vector<ss::sstring> second_group_config_properties = {
      "cleanup.policy", "compression.type", "message.timestamp.type"};

    auto first_group_describe_resp = describe_configs(
      test_tp, std::make_optional(first_group_config_properties));
    vlog(
      test_log.debug,
      "first_group_describe_resp: {}",
      first_group_describe_resp);
    assert_properties_amount(
      test_tp, first_group_describe_resp, first_group_config_properties.size());
    for (const auto& property : first_group_config_properties) {
        assert_property_presented(
          test_tp, property, first_group_describe_resp, true);
    }
    for (const auto& property : second_group_config_properties) {
        assert_property_presented(
          test_tp, property, first_group_describe_resp, false);
    }

    auto second_group_describe_resp = describe_configs(
      test_tp, std::make_optional(second_group_config_properties));
    vlog(
      test_log.debug,
      "second_group_describe_resp: {}",
      second_group_describe_resp);
    assert_properties_amount(
      test_tp,
      second_group_describe_resp,
      second_group_config_properties.size());
    for (const auto& property : first_group_config_properties) {
        assert_property_presented(
          test_tp, property, second_group_describe_resp, false);
    }
    for (const auto& property : second_group_config_properties) {
        assert_property_presented(
          test_tp, property, second_group_describe_resp, true);
    }
}

FIXTURE_TEST(test_alter_single_topic_config, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    absl::flat_hash_map<ss::sstring, ss::sstring> properties;
    properties.emplace("retention.ms", "1234");
    properties.emplace("cleanup.policy", "compact");
    properties.emplace("funes.remote.read", "true");
    properties.emplace("replication.factor", "1");

    auto resp = alter_configs(
      {make_alter_topic_config_resource(test_tp, properties)});

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, sql::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, test_tp);

    auto describe_resp = describe_configs(test_tp);
    assert_property_value(
      test_tp, "retention.ms", fmt::format("{}", 1234ms), describe_resp);
    assert_property_value(test_tp, "cleanup.policy", "compact", describe_resp);
    assert_property_value(
      test_tp, "funes.remote.read", "true", describe_resp);
}

FIXTURE_TEST(test_alter_multiple_topics_config, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic topic_1{"topic-1"};
    model::topic topic_2{"topic-2"};
    create_topic(topic_1, 1);
    create_topic(topic_2, 3);

    absl::flat_hash_map<ss::sstring, ss::sstring> properties_1;
    properties_1.emplace("retention.ms", "1234");
    properties_1.emplace("cleanup.policy", "compact");
    properties_1.emplace("replication.factor", "1");

    absl::flat_hash_map<ss::sstring, ss::sstring> properties_2;
    properties_2.emplace("retention.bytes", "4096");
    properties_2.emplace("replication.factor", "1");

    auto resp = alter_configs({
      make_alter_topic_config_resource(topic_1, properties_1),
      make_alter_topic_config_resource(topic_2, properties_2),
    });

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 2);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, sql::error_code::none);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[1].error_code, sql::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, topic_1);
    BOOST_REQUIRE_EQUAL(resp.data.responses[1].resource_name, topic_2);

    auto describe_resp_1 = describe_configs(topic_1);
    assert_property_value(
      topic_1, "retention.ms", fmt::format("{}", 1234ms), describe_resp_1);
    assert_property_value(
      topic_1, "cleanup.policy", "compact", describe_resp_1);

    auto describe_resp_2 = describe_configs(topic_2);
    assert_property_value(topic_2, "retention.bytes", "4096", describe_resp_2);
}

FIXTURE_TEST(
  test_alter_topic_sql_config_allowlist, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    absl::flat_hash_map<ss::sstring, ss::sstring> properties;
    properties.emplace("unclean.leader.election.enable", "true");
    properties.emplace("replication.factor", "1");

    auto resp = alter_configs(
      {make_alter_topic_config_resource(test_tp, properties)});

    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, sql::error_code::none);
}

FIXTURE_TEST(test_alter_topic_error, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    absl::flat_hash_map<ss::sstring, ss::sstring> properties;
    properties.emplace("not.exists", "1234");

    auto resp = alter_configs(
      {make_alter_topic_config_resource(test_tp, properties)});

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, sql::error_code::invalid_config);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, test_tp);
}

FIXTURE_TEST(
  test_alter_configuration_should_override, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);
    /**
     * Set custom retention.ms
     */
    absl::flat_hash_map<ss::sstring, ss::sstring> properties;
    properties.emplace("retention.ms", "1234");
    properties.emplace("replication.factor", "1");

    auto resp = alter_configs(
      {make_alter_topic_config_resource(test_tp, properties)});

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, sql::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, test_tp);

    auto describe_resp = describe_configs(test_tp);
    assert_property_value(
      test_tp, "retention.ms", fmt::format("{}", 1234ms), describe_resp);

    /**
     * Set custom retention.bytes, previous settings should be overriden
     */
    absl::flat_hash_map<ss::sstring, ss::sstring> new_properties;
    new_properties.emplace("retention.bytes", "4096");
    new_properties.emplace("replication.factor", "1");

    alter_configs({make_alter_topic_config_resource(test_tp, new_properties)});

    auto new_describe_resp = describe_configs(test_tp);
    // retention.ms should be set back to default
    assert_property_value(
      test_tp,
      "retention.ms",
      fmt::format(
        "{}", config::shard_local_cfg().log_retention_ms().value_or(-1ms)),
      new_describe_resp);
    assert_property_value(
      test_tp, "retention.bytes", "4096", new_describe_resp);
}

FIXTURE_TEST(test_incremental_alter_config, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);
    // set custom retention
    absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, sql::config_resource_operation>>
      properties;
    properties.emplace(
      "retention.ms",
      std::make_pair("1234", sql::config_resource_operation::set));

    auto resp = incremental_alter_configs(
      {make_incremental_alter_topic_config_resource(test_tp, properties)});

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, sql::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, test_tp);

    auto describe_resp = describe_configs(test_tp);
    assert_property_value(
      test_tp, "retention.ms", fmt::format("{}", 1234ms), describe_resp);

    /**
     * Set custom retention.bytes, only this property should be updated
     */
    absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, sql::config_resource_operation>>
      new_properties;
    new_properties.emplace(
      "retention.bytes",
      std::pair{"4096", sql::config_resource_operation::set});

    incremental_alter_configs(
      {make_incremental_alter_topic_config_resource(test_tp, new_properties)});

    auto new_describe_resp = describe_configs(test_tp);
    // retention.ms should stay untouched
    assert_property_value(
      test_tp, "retention.ms", fmt::format("{}", 1234ms), new_describe_resp);
    assert_property_value(
      test_tp, "retention.bytes", "4096", new_describe_resp);
}

FIXTURE_TEST(
  test_incremental_alter_config_sql_config_allowlist,
  alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);

    absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, sql::config_resource_operation>>
      properties;
    properties.emplace(
      "unclean.leader.election.enable",
      std::pair{"true", sql::config_resource_operation::set});

    auto resp = incremental_alter_configs(
      {make_incremental_alter_topic_config_resource(test_tp, properties)});

    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, sql::error_code::none);
}

FIXTURE_TEST(test_incremental_alter_config_remove, alter_config_test_fixture) {
    wait_for_controller_leadership().get();
    model::topic test_tp{"topic-1"};
    create_topic(test_tp, 6);
    // set custom retention
    absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, sql::config_resource_operation>>
      properties;
    properties.emplace(
      "retention.ms",
      std::make_pair("1234", sql::config_resource_operation::set));

    auto resp = incremental_alter_configs(
      {make_incremental_alter_topic_config_resource(test_tp, properties)});

    BOOST_REQUIRE_EQUAL(resp.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.responses[0].error_code, sql::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.responses[0].resource_name, test_tp);

    auto describe_resp = describe_configs(test_tp);
    assert_property_value(
      test_tp, "retention.ms", fmt::format("{}", 1234ms), describe_resp);

    /**
     * Remove retention.bytes
     */
    absl::flat_hash_map<
      ss::sstring,
      std::pair<std::optional<ss::sstring>, sql::config_resource_operation>>
      new_properties;
    new_properties.emplace(
      "retention.ms",
      std::pair{std::nullopt, sql::config_resource_operation::remove});

    incremental_alter_configs(
      {make_incremental_alter_topic_config_resource(test_tp, new_properties)});

    auto new_describe_resp = describe_configs(test_tp);
    // retention.ms should be set back to default
    assert_property_value(
      test_tp,
      "retention.ms",
      fmt::format(
        "{}", config::shard_local_cfg().log_retention_ms().value_or(-1ms)),
      new_describe_resp);
}
