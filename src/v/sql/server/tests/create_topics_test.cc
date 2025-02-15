// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/protocol/create_topics.h"
#include "sql/protocol/metadata.h"
#include "sql/server/handlers/topics/types.h"
#include "funes/tests/fixture.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

#include <algorithm>
#include <limits>

// rougly equivalent to the test harness:
//   https://github.com/apache/sql/blob/8e16158/core/src/test/scala/unit/sql/server/AbstractCreateTopicsRequestTest.scala
class create_topic_fixture : public funes_thread_fixture {
public:
    sql::create_topics_request make_req(
      std::vector<sql::creatable_topic> topics, bool validate_only = false) {
        return sql::create_topics_request{.data{
          .topics = std::move(topics),
          .timeout_ms = 10s,
          .validate_only = validate_only,
        }};
    }

    sql::creatable_topic make_topic(
      ss::sstring name,
      std::optional<int> num_partitions = std::nullopt,
      std::optional<int> replication_factor = std::nullopt,
      std::optional<std::map<ss::sstring, ss::sstring>> config = std::nullopt,
      std::optional<std::map<int, std::vector<int>>> assignment
      = std::nullopt) {
        sql::creatable_topic topic;

        topic.name = model::topic(name);

        if (num_partitions) {
            topic.num_partitions = *num_partitions;
        } else if (assignment) {
            topic.num_partitions = -1;
        } else {
            topic.num_partitions = 1;
        }

        if (replication_factor) {
            topic.replication_factor = *replication_factor;
        } else if (assignment) {
            topic.replication_factor = -1;
        } else {
            topic.replication_factor = 1;
        }

        if (config) {
            for (auto& c : *config) {
                topic.configs.push_back({c.first, c.second});
            }
        }

        if (assignment) {
            for (auto& a : *assignment) {
                sql::creatable_replica_assignment pa;
                pa.partition_index = model::partition_id(a.first);
                for (auto& b : a.second) {
                    pa.broker_ids.push_back(model::node_id(b));
                }
                topic.assignments.push_back(std::move(pa));
            }
        }

        return topic;
    }

    void test_create_topic(
      sql::create_topics_request req,
      sql::api_version version = sql::api_version(2)) {
        auto client = make_sql_client().get0();
        client.connect().get();
        auto resp = client.dispatch(req, version).get0();

        BOOST_REQUIRE_MESSAGE(
          std::all_of(
            std::cbegin(resp.data.topics),
            std::cend(resp.data.topics),
            [](const sql::creatable_topic_result& t) {
                return t.error_code == sql::error_code::none;
            }),
          fmt::format("expected no errors. received response: {}", resp));

        for (auto& topic : req.data.topics) {
            verify_metadata(client, req, topic);

            auto it = std::find_if(
              resp.data.topics.begin(),
              resp.data.topics.end(),
              [name = topic.name](const auto& t) { return t.name == name; });

            BOOST_CHECK(it != resp.data.topics.end());
            verify_response(topic, *it, version, req.data.validate_only);

            // TODO: one we combine the cluster fixture with the funes
            // fixture and enable multiple RP instances to run at the same time
            // in the test, then we should create two clients in this test where
            // one of the client is not connected to the controller, and verify
            // that the topic creation is correctly propogated to the
            // non-controller broker.
        }

        client.stop().then([&client] { client.shutdown(); }).get();
    }

    void verify_response(
      const sql::creatable_topic& req,
      const sql::creatable_topic_result& topic_res,
      sql::api_version version,
      bool validate_only) {
        if (version < sql::api_version(5)) {
            /// currently this method only verifies configurations in v5
            /// responses
            return;
        }
        if (validate_only) {
            /// Server should return default configs
            BOOST_TEST(topic_res.configs, "empty config response");
            auto cfg_map = config_map(*topic_res.configs);
            const auto default_topic_properties = sql::from_cluster_type(
              app.metadata_cache.local().get_default_properties());
            BOOST_TEST(
              cfg_map == default_topic_properties,
              "incorrect default properties");
            BOOST_CHECK_EQUAL(
              topic_res.topic_config_error_code, sql::error_code::none);
            return;
        }
        if (req.configs.empty()) {
            /// no custom configs were passed
            return;
        }
        BOOST_TEST(topic_res.configs, "Expecting configs");
        auto resp_cfgs = sql::config_map(*topic_res.configs);
        auto cfg = app.metadata_cache.local().get_topic_cfg(
          model::topic_namespace_view{model::sql_namespace, topic_res.name});
        BOOST_TEST(cfg, "missing topic config");
        auto config_map = sql::from_cluster_type(cfg->properties);
        BOOST_TEST(config_map == resp_cfgs, "configs didn't match");
        BOOST_CHECK_EQUAL(
          topic_res.topic_config_error_code, sql::error_code::none);
    }

    void verify_metadata(
      sql::client::transport& client,
      sql::create_topics_request& create_req,
      sql::creatable_topic& request_topic) {
        // query the server for this topic's metadata
        sql::metadata_request metadata_req;
        metadata_req.data.topics
          = std::make_optional<std::vector<sql::metadata_request_topic>>();
        metadata_req.data.topics->push_back(
          sql::metadata_request_topic{request_topic.name});
        auto metadata_resp
          = client.dispatch(metadata_req, sql::api_version(1)).get0();

        // yank out the metadata for the topic from the response
        auto topic_metadata = std::find_if(
          metadata_resp.data.topics.cbegin(),
          metadata_resp.data.topics.cend(),
          [&request_topic](const sql::metadata_response::topic& topic) {
              return topic.name == request_topic.name;
          });

        BOOST_TEST_REQUIRE(
          (topic_metadata != metadata_resp.data.topics.cend()),
          "expected topic not returned from metadata query");

        int partitions;
        if (!request_topic.assignments.empty()) {
            partitions = request_topic.assignments.size();
        } else {
            partitions = request_topic.num_partitions;
        }

        int replication;
        if (!request_topic.assignments.empty()) {
            replication = request_topic.assignments[0].broker_ids.size();
        } else {
            replication = request_topic.replication_factor;
        }

        if (create_req.data.validate_only) {
            BOOST_TEST(
              topic_metadata->error_code != sql::error_code::none,
              fmt::format(
                "error {} for topic {}",
                topic_metadata->error_code,
                request_topic.name));
            BOOST_TEST(
              topic_metadata->partitions.empty(),
              "topic should have no partitions");
        } else {
            BOOST_TEST(topic_metadata->error_code == sql::error_code::none);
            if (partitions == -1) {
                // FIXME: where does the default partition count come from?
                BOOST_TEST(topic_metadata->partitions.size() == 99999999);
            } else {
                BOOST_TEST(topic_metadata->partitions.size() == partitions);
            }

            // FIXME: this is a temporary fix. what we really want is to use
            // BOOST_TEST_REQUIRE for this condition, but there does seem to be
            // something preventing the partitions from being reported
            // reliabily.
            BOOST_TEST(!topic_metadata->partitions.empty());
            if (topic_metadata->partitions.empty()) {
                return;
            }

            if (replication == -1) {
                // FIXME: where does the default replication come from?
                BOOST_TEST(
                  topic_metadata->partitions[0].replica_nodes.size()
                  == 99999999);
            } else {
                BOOST_TEST(
                  topic_metadata->partitions[0].replica_nodes.size()
                  == replication);
            }
        }
    }

    void foo() { BOOST_TEST(false); }
};

// This is rougly equivalent to
//   https://github.com/apache/sql/blob/8e16158/core/src/test/scala/unit/sql/server/CreateTopicsRequestTest.scala#L27
FIXTURE_TEST(create_topics, create_topic_fixture) {
    wait_for_controller_leadership().get();

    test_create_topic(make_req({make_topic("topic1")}));

    // FIXME: these all crash with undefined behavior
#if 0
    // replication factor = 3
    test_create_topic(make_req({make_topic("topic2", std::nullopt, 3)}));

    test_create_topic(make_req({make_topic(
      "topic2",
      5,
      2,
      std::map<ss::sstring, ss::sstring>{{
        {"min.insync.replicas", "2"},
      }})}));

    // defaults
    test_create_topic(make_req({make_topic("topic12", -1, -1)}));
    test_create_topic(make_req({make_topic("topic13", -1, 2)}));
    test_create_topic(make_req({make_topic("topic13", 2, -1)}));
#endif

    // FIXME: funes does not currently support manual partition assignments.
    // however we should handle gracefully clients that try.
#if 0
    // Manual assignments
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic4", assignment = Map(0 -> List(0))))))
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic5",
      assignment = Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2)),
      config = Map("min.insync.replicas" -> "2")))))
    // Mixed
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic6"),
      topicReq("topic7", numPartitions = 5, replicationFactor = 2),
      topicReq("topic8", assignment = Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2))))))
    validateValidCreateTopicsRequests(topicsReq(Seq(topicReq("topic9"),
      topicReq("topic10", numPartitions = 5, replicationFactor = 2),
      topicReq("topic11", assignment = Map(0 -> List(0, 1), 1 -> List(1, 0), 2 -> List(1, 2)))),
      validateOnly = true))
#endif
}

FIXTURE_TEST(read_replica_and_remote_write, create_topic_fixture) {
    auto topic = make_topic(
      "topic1",
      std::nullopt,
      std::nullopt,
      std::map<ss::sstring, ss::sstring>{
        {"funes.remote.readreplica", "panda-bucket"},
        {"funes.remote.write", "true"}});

    auto req = make_req({topic});

    auto client = make_sql_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, sql::api_version(2)).get0();

    BOOST_CHECK(
      resp.data.topics[0].error_code == sql::error_code::invalid_config);
    BOOST_CHECK(
      resp.data.topics[0].error_message
      == "remote read and write are not supported for read replicas");
    BOOST_CHECK(resp.data.topics[0].name == "topic1");
}

FIXTURE_TEST(test_v5_validate_configs_resp, create_topic_fixture) {
    wait_for_controller_leadership().get();

    /// Test conditions in create_topic_fixture::verify_metadata will run
    test_create_topic(
      make_req({make_topic("topicA"), make_topic("topicB")}, true),
      sql::api_version(5));

    /// Test create topic with custom configs, verify that they have been set
    /// and correctly returned in response
    std::map<ss::sstring, ss::sstring> config_map{
      {ss::sstring(sql::topic_property_retention_bytes), "1234567"},
      {ss::sstring(sql::topic_property_segment_size), "7654321"}};

    test_create_topic(
      make_req(
        {make_topic("topicC", 3, 1, config_map),
         make_topic("topicD", 3, 1, config_map)},
        false),
      sql::api_version(5));
}
