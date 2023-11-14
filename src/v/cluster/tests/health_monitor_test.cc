// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/metadata_cache.h"
#include "cluster/node/types.h"
#include "cluster/shard_table.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/tests/health_monitor_test_utils.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "net/unresolved_address.h"
#include "outcome.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <seastar/core/sstring.hh>

#include <boost/test/tools/interface.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <unordered_set>
#include <vector>

static cluster::cluster_report_filter get_all{};

void check_reports_the_same(
  std::vector<cluster::node_health_report>& lhs,
  std::vector<cluster::node_health_report>& rhs) {
    BOOST_TEST_REQUIRE(lhs.size() == rhs.size());
    auto by_id = [](
                   const cluster::node_health_report& lr,
                   const cluster::node_health_report& rr) {
        return lr.id < rr.id;
    };
    std::sort(lhs.begin(), lhs.end(), by_id);
    std::sort(rhs.begin(), rhs.end(), by_id);

    for (auto i = 0; i < lhs.size(); ++i) {
        auto& lr = lhs[i];
        auto& rr = rhs[i];
        BOOST_TEST_REQUIRE(
          lr.local_state.funes_version == rr.local_state.funes_version);
        BOOST_TEST_REQUIRE(std::equal(
          lr.topics.cbegin(),
          lr.topics.cend(),
          rr.topics.cbegin(),
          rr.topics.cend()));
        BOOST_TEST_REQUIRE(
          lr.local_state.disks().size() == rr.local_state.disks().size());
        for (auto i = 0; i < lr.local_state.disks().size(); ++i) {
            BOOST_REQUIRE_EQUAL(
              lr.local_state.disks().at(i).alert,
              rr.local_state.disks().at(i).alert);
            BOOST_REQUIRE_EQUAL(
              lr.local_state.disks().at(i).free,
              rr.local_state.disks().at(i).free);
            BOOST_REQUIRE_EQUAL(
              lr.local_state.disks().at(i).path,
              rr.local_state.disks().at(i).path);
            BOOST_REQUIRE_EQUAL(
              lr.local_state.disks().at(i).total,
              rr.local_state.disks().at(i).total);
        }
        BOOST_TEST_REQUIRE(
          lr.local_state.get_disk_alert() == rr.local_state.get_disk_alert());
    }
}

void check_states_the_same(
  std::vector<cluster::node_state>& lhs,
  std::vector<cluster::node_state>& rhs) {
    BOOST_TEST_REQUIRE(lhs.size() == rhs.size());

    auto by_id = [](
                   const cluster::node_state& lr,
                   const cluster::node_state& rr) { return lr.id < rr.id; };
    std::sort(lhs.begin(), lhs.end(), by_id);
    std::sort(rhs.begin(), rhs.end(), by_id);

    for (auto i = 0; i < lhs.size(); ++i) {
        auto& lr = lhs[i];
        auto& rr = rhs[i];
        BOOST_TEST_REQUIRE(lr.is_alive == rr.is_alive);
        BOOST_TEST_REQUIRE(lr.membership_state == rr.membership_state);
    }
}

FIXTURE_TEST(data_are_consistent_across_nodes, cluster_test_fixture) {
    auto n1 = create_node_application(model::node_id{0});
    auto n2 = create_node_application(model::node_id{1});
    auto n3 = create_node_application(model::node_id{2});

    wait_for_all_members(3s).get();
    // wait for disk space report to be present
    tests::cooperative_spin_wait_with_timeout(10s, [&n1] {
        return n1->controller->get_health_monitor()
          .local()
          .get_cluster_health(
            get_all, cluster::force_refresh::yes, model::no_timeout)
          .then([](result<cluster::cluster_health_report> res) {
              if (!res) {
                  return false;
              }
              if (res.value().node_reports.empty()) {
                  return false;
              }
              return true;
          });
    }).get();

    // collect health report from node 1
    auto r_1 = n1->controller->get_health_monitor()
                 .local()
                 .get_cluster_health(
                   get_all, cluster::force_refresh::yes, model::no_timeout)
                 .get0();
    auto r_2 = n2->controller->get_health_monitor()
                 .local()
                 .get_cluster_health(
                   get_all, cluster::force_refresh::yes, model::no_timeout)
                 .get0();
    auto r_3 = n3->controller->get_health_monitor()
                 .local()
                 .get_cluster_health(
                   get_all, cluster::force_refresh::yes, model::no_timeout)
                 .get0();

    BOOST_TEST_REQUIRE(r_1.has_value());
    BOOST_TEST_REQUIRE(r_2.has_value());
    BOOST_TEST_REQUIRE(r_3.has_value());

    auto report_1 = r_1.value();
    auto report_2 = r_2.value();
    auto report_3 = r_3.value();
    BOOST_TEST_REQUIRE(report_1.raft0_leader == report_2.raft0_leader);
    BOOST_TEST_REQUIRE(report_2.raft0_leader == report_3.raft0_leader);

    check_reports_the_same(report_1.node_reports, report_2.node_reports);
    check_reports_the_same(report_2.node_reports, report_3.node_reports);
    check_states_the_same(report_1.node_states, report_2.node_states);
    check_states_the_same(report_2.node_states, report_3.node_states);
}

cluster::topic_configuration topic_cfg(
  const model::ns& ns,
  const ss::sstring& name,
  int16_t replication,
  int paritions) {
    return cluster::topic_configuration(
      ns, model::topic(name), paritions, replication);
}

bool contains_exactly_ntp_leaders(
  ss::logger& logger,
  const std::unordered_set<model::ntp>& expected,
  const ss::chunked_fifo<cluster::topic_status>& topics) {
    auto left = expected;
    for (const auto& t_l : topics) {
        for (const auto& p_l : t_l.partitions) {
            model::ntp ntp(t_l.tp_ns.ns, t_l.tp_ns.tp, p_l.id);
            if (left.erase(ntp) == 0) {
                vlog(
                  logger.debug,
                  "ntp {} is present in report, but is not expected",
                  ntp);
                return false;
            }
        }
    }
    if (!left.empty()) {
        for (auto& ntp : left) {
            vlog(logger.debug, "Missing ntp {} from topic status", ntp);
        }
    }
    return left.empty();
}

model::ntp ntp(model::ns ns, ss::sstring tp, int pid) {
    return model::ntp(
      std::move(ns), model::topic(std::move(tp)), model::partition_id(pid));
}

FIXTURE_TEST(test_ntp_filter, cluster_test_fixture) {
    auto n1 = create_node_application(model::node_id{0});
    create_node_application(model::node_id{1});
    create_node_application(model::node_id{2});

    wait_for_all_members(3s).get();
    // wait for disk space report to be present
    tests::cooperative_spin_wait_with_timeout(10s, [&n1] {
        return n1->controller->get_health_monitor()
          .local()
          .get_cluster_health(
            get_all, cluster::force_refresh::yes, model::no_timeout)
          .then([](result<cluster::cluster_health_report> res) {
              if (!res) {
                  return false;
              }
              if (res.value().node_reports.empty()) {
                  return false;
              }
              return true;
          });
    }).get();

    // create topics
    std::vector<cluster::topic_configuration> topics;
    topics.push_back(topic_cfg(model::sql_namespace, "tp-1", 3, 3));
    topics.push_back(topic_cfg(model::sql_namespace, "tp-2", 3, 2));
    topics.push_back(topic_cfg(model::sql_namespace, "tp-3", 3, 1));
    topics.push_back(
      topic_cfg(model::sql_internal_namespace, "internal-1", 3, 2));

    n1->controller->get_topics_frontend()
      .local()
      .autocreate_topics(std::move(topics), 30s)
      .get();

    static auto filter_template = [] {
        return cluster::cluster_report_filter{
          .node_report_filter = cluster::node_report_filter{
            .include_partitions = cluster::include_partitions_info::yes,
          }};
    };

    auto f_1 = filter_template();
    // test filtering by ntp
    cluster::partitions_filter::topic_map_t sql_topics_map;
    sql_topics_map.emplace(
      model::topic("tp-1"),
      cluster::partitions_filter::partitions_set_t{
        model::partition_id(0), model::partition_id(2)});
    sql_topics_map.emplace(
      model::topic("tp-2"),
      cluster::partitions_filter::partitions_set_t{model::partition_id(0)});

    f_1.node_report_filter.ntp_filters.namespaces.emplace(
      model::sql_namespace, std::move(sql_topics_map));

    // filter by namespace
    f_1.node_report_filter.ntp_filters.namespaces.emplace(
      model::funes_ns, cluster::partitions_filter::topic_map_t{});

    // filter by topic
    cluster::partitions_filter::topic_map_t internal_topic_map;
    internal_topic_map.emplace(
      model::topic("internal-1"),
      cluster::partitions_filter::partitions_set_t{});

    f_1.node_report_filter.ntp_filters.namespaces.emplace(
      model::sql_internal_namespace, std::move(internal_topic_map));

    /**
     * Requested sql/tp-1/0, sql/tp-1/2, sql/tp-2/0,
     * funes/controller/0, all partitions of sql-internal/internal-1
     */
    tests::cooperative_spin_wait_with_timeout(10s, [&] {
        return n1->controller->get_health_monitor()
          .local()
          .get_cluster_health(
            f_1, cluster::force_refresh::yes, model::no_timeout)
          .then([](result<cluster::cluster_health_report> report) {
              return report.has_value()
                     && report.value().node_reports.size() == 3
                     && contains_exactly_ntp_leaders(
                       g_seastar_test_log,
                       {
                         ntp(model::sql_namespace, "tp-1", 0),
                         ntp(model::sql_namespace, "tp-1", 2),
                         ntp(model::sql_namespace, "tp-2", 0),
                         ntp(model::sql_internal_namespace, "internal-1", 0),
                         ntp(model::sql_internal_namespace, "internal-1", 1),
                         ntp(model::funes_ns, "controller", 0),
                       },
                       report.value().node_reports.begin()->topics);
          });
    }).get();

    // check filtering in node report
    tests::cooperative_spin_wait_with_timeout(10s, [&] {
        return n1->controller->get_health_monitor()
          .local()
          .collect_node_health(f_1.node_report_filter)
          .then([](result<cluster::node_health_report> report) {
              return report.has_value()
                     && contains_exactly_ntp_leaders(
                       g_seastar_test_log,
                       {
                         ntp(model::sql_namespace, "tp-1", 0),
                         ntp(model::sql_namespace, "tp-1", 2),
                         ntp(model::sql_namespace, "tp-2", 0),
                         ntp(model::sql_internal_namespace, "internal-1", 0),
                         ntp(model::sql_internal_namespace, "internal-1", 1),
                         ntp(model::funes_ns, "controller", 0),
                       },
                       report.value().topics);
          });
    }).get();
}

FIXTURE_TEST(test_alive_status, cluster_test_fixture) {
    auto n1 = create_node_application(model::node_id{0});
    create_node_application(model::node_id{1});
    create_node_application(model::node_id{2});

    wait_for_all_members(3s).get();
    // wait for disk space report to be present
    tests::cooperative_spin_wait_with_timeout(10s, [&n1] {
        return n1->controller->get_health_monitor()
          .local()
          .get_cluster_health(
            get_all, cluster::force_refresh::yes, model::no_timeout)
          .then([](result<cluster::cluster_health_report> res) {
              if (!res) {
                  return false;
              }
              if (res.value().node_reports.empty()) {
                  return false;
              }
              return true;
          });
    }).get();

    // stop one of the nodes
    remove_node_application(model::node_id{1});

    // wait until the node will be reported as not alive
    tests::cooperative_spin_wait_with_timeout(10s, [&n1] {
        return n1->controller->get_health_monitor()
          .local()
          .get_cluster_health(
            get_all, cluster::force_refresh::yes, model::no_timeout)
          .then([](result<cluster::cluster_health_report> res) {
              if (!res) {
                  return false;
              }
              if (res.value().node_reports.empty()) {
                  return false;
              }
              auto it = std::find_if(
                res.value().node_states.begin(),
                res.value().node_states.end(),
                [](cluster::node_state& s) {
                    return s.id == model::node_id(1);
                });
              return it->is_alive == cluster::alive::no;
          });
    }).get();
}

// tests below are non-rp-fixture unit tests but we don't want to add another
// binary just for that

struct health_report_unit : cluster::health_report_accessor {};

namespace {
using namespace cluster;
using namespace model;

enum part_status { HEALTHY, LEADERLESS, URP };

topic_status
make_ts(ss::sstring name, const std::vector<part_status>& status_list) {
    ss::chunked_fifo<cluster::partition_status> statuses;

    partition_id pid{0};
    for (auto status : status_list) {
        partition_status s = [&]() -> partition_status {
            switch (status) {
            case HEALTHY:
                return partition_status{.leader_id = model::node_id(0)};
            case LEADERLESS:
                return partition_status{.leader_id = std::nullopt};
            case URP:
                return partition_status{
                  .leader_id = model::node_id(0),
                  .under_replicated_replicas = 1};
            default:
                BOOST_FAIL("huh");
            };
        }();

        s.id = pid++;
        statuses.emplace_back(s);
    }

    return {{model::sql_namespace, topic{name}}, std::move(statuses)};
}

node_health_report
make_nhr(int nid, const std::vector<topic_status>& statuses) {
    node_health_report nhr;
    nhr.id = node_id{nid};
    std::move(statuses.begin(), statuses.end(), std::back_inserter(nhr.topics));
    return nhr;
};

struct node_and_status {
    int nid;
    std::vector<topic_status> statuses;
};

auto make_reports(const std::vector<node_and_status>& statuses) {
    health_report_accessor::report_cache_t ret;
    for (auto& s : statuses) {
        ret[node_id{s.nid}] = make_nhr(s.nid, s.statuses);
    }
    return ret;
};

} // namespace

namespace cluster {

std::ostream& operator<<(
  std::ostream& os, const health_report_accessor::aggregated_report& r) {
    os << "{lcount: " << r.leaderless_count
       << ", ucount: " << r.under_replicated_count << ", leaderless: {";

    for (auto& e : r.leaderless) {
        os << e << ", ";
    }
    os << "}, under_replicated: {";
    for (auto& e : r.under_replicated) {
        os << e << ", ";
    }
    os << "}}";
    return os;
}
} // namespace cluster

FIXTURE_TEST(test_aggregate, health_report_unit) {
    using report = health_report_accessor::aggregated_report;

    const ss::sstring topic_a = "topic_a";
    auto healthy_a = make_ts(topic_a, {HEALTHY, HEALTHY});
    auto healthy_leaderless_a = make_ts(topic_a, {HEALTHY, LEADERLESS});
    auto healthy_urp_a = make_ts(topic_a, {HEALTHY, URP});
    auto urp_a = make_ts(topic_a, {URP});

    model::ntp ntp0_a{model::sql_namespace, topic_a, 0};
    model::ntp ntp1_a{model::sql_namespace, topic_a, 1};
    model::ntp ntp2_a{model::sql_namespace, topic_a, 2};

    const ss::sstring topic_b = "topic_b";
    auto healthy_b = make_ts(topic_b, {HEALTHY, HEALTHY});
    auto healthy_leaderless_b = make_ts(topic_b, {HEALTHY, LEADERLESS});
    auto healthy_urp_b = make_ts(topic_b, {HEALTHY, URP});
    auto urp_b = make_ts(topic_b, {URP});

    model::ntp ntp0_b{model::sql_namespace, topic_b, 0};
    model::ntp ntp1_b{model::sql_namespace, topic_b, 1};
    model::ntp ntp2_b{model::sql_namespace, topic_b, 2};

    report_cache_t empty_reports{{model::node_id(0), {}}};

    {
        // empty input, empty report
        auto r = aggregate(empty_reports);
        BOOST_CHECK_EQUAL(r, (report{}));
    }

    {
        // healthy input, empty report
        auto input_reports = make_reports({{0, {healthy_a}}});
        auto result = aggregate(input_reports);
        BOOST_CHECK_EQUAL(result, (report{}));
    }

    {
        // 1 node, 1 topic, HL
        auto input_reports = make_reports({{0, {healthy_leaderless_a}}});
        auto result = aggregate(input_reports);
        aggregated_report expected = {
          .leaderless = {ntp1_a}, .leaderless_count = 1};
        BOOST_CHECK_EQUAL(result, expected);
    }

    {
        // 2 identical nodes: 1 topic, HL
        auto input_reports = make_reports(
          {{0, {healthy_leaderless_a}}, {1, {healthy_leaderless_a}}});
        auto result = aggregate(input_reports);
        aggregated_report expected = {
          .leaderless = {ntp1_a}, .leaderless_count = 1};
        BOOST_CHECK_EQUAL(result, expected);
    }

    {
        // node 0: a: HL
        // node 1: b: HL
        auto input_reports = make_reports(
          {{0, {healthy_leaderless_a}}, {1, {healthy_leaderless_b}}});
        auto result = aggregate(input_reports);
        aggregated_report expected = {
          .leaderless = {ntp1_a, ntp1_b}, .leaderless_count = 2};
        BOOST_CHECK_EQUAL(result, expected);
    }

    {
        // node 0: a: HL
        // node 0: b: HU
        auto input_reports = make_reports(
          {{0, {healthy_leaderless_a, healthy_urp_b}}});
        auto result = aggregate(input_reports);
        aggregated_report expected = {
          .leaderless = {ntp1_a},
          .under_replicated = {ntp1_b},
          .leaderless_count = 1,
          .under_replicated_count = 1};
        BOOST_CHECK_EQUAL(result, expected);
    }

    {
        // node 0: a: HL
        // node 1: b: HU
        auto input_reports = make_reports(
          {{0, {healthy_leaderless_a}}, {1, {healthy_urp_b}}});
        auto result = aggregate(input_reports);
        aggregated_report expected = {
          .leaderless = {ntp1_a},
          .under_replicated = {ntp1_b},
          .leaderless_count = 1,
          .under_replicated_count = 1};
        BOOST_CHECK_EQUAL(result, expected);
    }
}

FIXTURE_TEST(test_report_truncation, health_report_unit) {
    constexpr size_t max_count
      = health_report_accessor::aggregated_report::max_partitions_report;

    auto test_unhealthy = [&](size_t unhealthy_count, part_status pstatus) {
        std::vector<topic_status> statuses;
        for (size_t i = 0; i < unhealthy_count; i++) {
            auto tn = fmt::format("topic_{}", i);
            auto status = make_ts(tn, {pstatus});
            statuses.emplace_back(status);
        }

        health_report_accessor::report_cache_t reports;
        reports[model::node_id(0)] = make_nhr(0, statuses);

        auto result = aggregate(reports);

        size_t expected_leaderless = pstatus == LEADERLESS ? unhealthy_count
                                                           : 0;
        size_t expected_urp = pstatus == URP ? unhealthy_count : 0;

        // now verify that the counts are as expected, except that the list size
        // is always clamped to the max
        BOOST_CHECK_EQUAL(
          result.leaderless.size(), std::min(expected_leaderless, max_count));
        BOOST_CHECK_EQUAL(result.leaderless_count, expected_leaderless);

        BOOST_CHECK_EQUAL(
          result.under_replicated.size(), std::min(expected_urp, max_count));
        BOOST_CHECK_EQUAL(result.under_replicated_count, expected_urp);
    };

    test_unhealthy(0, LEADERLESS);
    test_unhealthy(0, URP);

    test_unhealthy(max_count - 1, LEADERLESS);
    test_unhealthy(max_count - 1, URP);

    test_unhealthy(max_count, LEADERLESS);
    test_unhealthy(max_count, URP);

    test_unhealthy(max_count + 1, LEADERLESS);
    test_unhealthy(max_count + 1, URP);
}
