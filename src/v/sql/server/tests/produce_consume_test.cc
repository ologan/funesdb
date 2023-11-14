// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "sql/client/transport.h"
#include "sql/protocol/errors.h"
#include "sql/protocol/fetch.h"
#include "sql/protocol/offset_for_leader_epoch.h"
#include "sql/protocol/produce.h"
#include "sql/protocol/wire.h"
#include "sql/server/handlers/produce.h"
#include "sql/server/snc_quota_manager.h"
#include "sql/server/tests/delete_records_utils.h"
#include "sql/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "funes/tests/fixture.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/old/interface.hpp>

#include <vector>

using namespace std::chrono_literals;
using std::vector;
using tests::kv_t;

struct prod_consume_fixture : public funes_thread_fixture {
    void start() {
        consumer = std::make_unique<sql::client::transport>(
          make_sql_client().get0());
        producer = std::make_unique<sql::client::transport>(
          make_sql_client().get0());
        consumer->connect().get0();
        producer->connect().get0();
        model::topic_namespace tp_ns(model::ns("sql"), test_topic);
        add_topic(tp_ns).get0();
        model::ntp ntp(tp_ns.ns, tp_ns.tp, model::partition_id(0));
        tests::cooperative_spin_wait_with_timeout(2s, [ntp, this] {
            auto shard = app.shard_table.local().shard_for(ntp);
            if (!shard) {
                return ss::make_ready_future<bool>(false);
            }
            return app.partition_manager.invoke_on(
              *shard, [ntp](cluster::partition_manager& pm) {
                  return pm.get(ntp)->is_leader();
              });
        }).get0();
    }

    std::vector<sql::produce_request::partition> small_batches(size_t count) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        for (int i = 0; i < count; ++i) {
            iobuf v{};
            v.append("v", 1);
            builder.add_raw_kv(iobuf{}, std::move(v));
        }

        std::vector<sql::produce_request::partition> res;

        sql::produce_request::partition partition;
        partition.partition_index = model::partition_id(0);
        partition.records.emplace(std::move(builder).build());
        res.push_back(std::move(partition));
        return res;
    }

    ss::future<sql::produce_response>
    produce_raw(std::vector<sql::produce_request::partition>&& partitions) {
        sql::produce_request::topic tp;
        tp.partitions = std::move(partitions);
        tp.name = test_topic;
        std::vector<sql::produce_request::topic> topics;
        topics.push_back(std::move(tp));
        sql::produce_request req(std::nullopt, 1, std::move(topics));
        req.data.timeout_ms = std::chrono::seconds(2);
        req.has_idempotent = false;
        req.has_transactional = false;
        return producer->dispatch(std::move(req));
    }

    template<typename T>
    ss::future<model::offset> produce(T&& batch_factory) {
        const size_t count = random_generators::get_int(1, 20);
        return produce_raw(batch_factory(count))
          .then([count](sql::produce_response r) {
              return r.data.responses.begin()->partitions.begin()->base_offset
                     + model::offset(count - 1);
          });
    }

    ss::future<sql::fetch_response> fetch_next() {
        sql::fetch_request::partition partition;
        partition.fetch_offset = fetch_offset;
        partition.partition_index = model::partition_id(0);
        partition.log_start_offset = model::offset(0);
        partition.max_bytes = 1_MiB;
        sql::fetch_request::topic topic;
        topic.name = test_topic;
        topic.fetch_partitions.push_back(partition);

        sql::fetch_request req;
        req.data.min_bytes = 1;
        req.data.max_bytes = 10_MiB;
        req.data.max_wait_ms = 1000ms;
        req.data.topics.push_back(std::move(topic));

        return consumer->dispatch(std::move(req), sql::api_version(4))
          .then([this](sql::fetch_response resp) {
              if (resp.data.topics.empty()) {
                  return resp;
              }
              auto& part = *resp.data.topics.begin();

              for ([[maybe_unused]] auto& r : part.partitions) {
                  const auto& data = part.partitions.begin()->records;
                  if (data && !data->empty()) {
                      // update next fetch offset the same way as SQL clients
                      fetch_offset = ++data->last_offset();
                  }
              }
              return resp;
          });
    }

    model::offset fetch_offset{0};
    std::unique_ptr<sql::client::transport> consumer;
    std::unique_ptr<sql::client::transport> producer;
    ss::abort_source as;
    const model::topic test_topic = model::topic("test-topic");
};

/**
 * produce/consume test simulating Hazelcast benchmart workload with small
 * batches.
 */
FIXTURE_TEST(test_produce_consume_small_batches, prod_consume_fixture) {
    wait_for_controller_leadership().get0();
    start();
    auto offset_1 = produce([this](size_t cnt) {
                        return small_batches(cnt);
                    }).get0();
    auto resp_1 = fetch_next().get0();

    auto offset_2 = produce([this](size_t cnt) {
                        return small_batches(cnt);
                    }).get0();
    auto resp_2 = fetch_next().get0();

    BOOST_REQUIRE_EQUAL(resp_1.data.topics.empty(), false);
    BOOST_REQUIRE_EQUAL(resp_2.data.topics.empty(), false);
    BOOST_REQUIRE_EQUAL(resp_1.data.topics.begin()->partitions.empty(), false);
    BOOST_REQUIRE_EQUAL(
      resp_1.data.topics.begin()->partitions.begin()->error_code,
      sql::error_code::none);
    BOOST_REQUIRE_EQUAL(
      resp_1.data.topics.begin()->partitions.begin()->records->last_offset(),
      offset_1);
    BOOST_REQUIRE_EQUAL(resp_2.data.topics.begin()->partitions.empty(), false);
    BOOST_REQUIRE_EQUAL(
      resp_2.data.topics.begin()->partitions.begin()->error_code,
      sql::error_code::none);
    BOOST_REQUIRE_EQUAL(
      resp_2.data.topics.begin()->partitions.begin()->records->last_offset(),
      offset_2);
};

FIXTURE_TEST(test_version_handler, prod_consume_fixture) {
    wait_for_controller_leadership().get();
    start();
    std::vector<sql::produce_request::topic> topics;
    topics.push_back(sql::produce_request::topic{
      .name = model::topic{"abc123"}, .partitions = small_batches(10)});

    const auto unsupported_version = sql::api_version(
      sql::produce_handler::max_supported() + 1);
    BOOST_CHECK_THROW(
      producer
        ->dispatch(
          // NOLINTNEXTLINE(bugprone-use-after-move)
          sql::produce_request(std::nullopt, 1, std::move(topics)),
          unsupported_version)
        .get(),
      sql::client::sql_request_disconnected_exception);
}

static std::vector<sql::produce_request::partition>
single_batch(const size_t volume) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    {
        const ss::sstring data(volume, 's');
        iobuf v{};
        v.append(data.data(), data.size());
        builder.add_raw_kv(iobuf{}, std::move(v));
    }

    sql::produce_request::partition partition;
    partition.partition_index = model::partition_id(0);
    partition.records.emplace(std::move(builder).build());

    std::vector<sql::produce_request::partition> res;
    res.push_back(std::move(partition));
    return res;
}

namespace ch = std::chrono;

struct throughput_limits_fixure : prod_consume_fixture {
    ch::milliseconds _window_width;
    ch::milliseconds _balancer_period;
    int64_t _rate_minimum;

    void config_set(const std::string_view name, const std::any value) {
        ss::smp::invoke_on_all([&] {
            config::shard_local_cfg().get(name).set_value(value);
        }).get0();
    }

    void config_set_window_width(const ch::milliseconds window_width) {
        _window_width = window_width;
        ss::smp::invoke_on_all([window_width] {
            config::shard_local_cfg()
              .get("sql_quota_balancer_window_ms")
              .set_value(window_width);
        }).get0();
    }

    void config_set_balancer_period(const ch::milliseconds balancer_period) {
        _balancer_period = balancer_period;
        ss::smp::invoke_on_all([balancer_period] {
            config::shard_local_cfg()
              .get("sql_quota_balancer_node_period_ms")
              .set_value(balancer_period);
        }).get0();
    }

    void config_set_rate_minimum(const int64_t rate_minimum) {
        _rate_minimum = rate_minimum;
        ss::smp::invoke_on_all([rate_minimum] {
            config::shard_local_cfg()
              .get("sql_quota_balancer_min_shard_throughput_bps")
              .set_value(rate_minimum);
        }).get0();
    }

    int warmup_cycles(const size_t rate_limit, const size_t packet_size) const {
        // warmup is the number of iterations enough to exhaust the token bucket
        // at least twice, and for the balancer to run at least 4 times after
        // that.
        const int warmup_bytes = rate_limit
                                 * ch::duration_cast<ch::milliseconds>(
                                     // token bucket component
                                     2 * _window_width
                                     // balancer component
                                     + 4 * _balancer_period)
                                     .count()
                                 / 1000;
        return warmup_bytes / packet_size + 1;
    }

    size_t test_ingress(
      const size_t rate_limit_in,
      const size_t batch_size,
      const int tolerance_percent) {
        size_t sql_in_data_len = 0;
        constexpr size_t sql_packet_overhead = 127;
        // do not divide rate by smp::count because
        // - balanced case: TP will be balanced and  the entire quota will end
        // up in one shard
        // - static case: rate_limit is per shard
        const auto batches_cnt = /* 1s * */ rate_limit_in
                                 / (batch_size + sql_packet_overhead);
        ch::steady_clock::time_point start;
        ch::milliseconds throttle_time{};

        for (int k = -warmup_cycles(
               rate_limit_in, batch_size + sql_packet_overhead);
             k != batches_cnt;
             ++k) {
            if (k == 0) {
                start = ch::steady_clock::now();
                throttle_time = {};
                BOOST_TEST_WARN(
                  false,
                  "Ingress measurement starts. batches: " << batches_cnt);
            }
            throttle_time += produce_raw(single_batch(batch_size))
                               .then([](const sql::produce_response& r) {
                                   return r.data.throttle_time_ms;
                               })
                               .get0();
            sql_in_data_len += batch_size;
        }
        const auto stop = ch::steady_clock::now();
        const auto wire_data_length = (batch_size + sql_packet_overhead)
                                      * batches_cnt;
        const auto rate_estimated = rate_limit_in
                                    - _rate_minimum * (ss::smp::count - 1);
        const auto time_estimated = ch::milliseconds(
          wire_data_length * 1000 / rate_estimated);
        BOOST_TEST_CHECK(
          abs(stop - start - time_estimated)
            < time_estimated * tolerance_percent / 100,
          "Ingress time: stop-start["
            << stop - start << "] ≈ time_estimated[" << time_estimated << "] ±"
            << tolerance_percent << "%, error: " << std::setprecision(3)
            << (stop - start - time_estimated) * 100.0 / time_estimated << "%");
        return sql_in_data_len;
    }

    size_t test_egress(
      const size_t sql_data_available,
      const size_t rate_limit_out,
      const size_t batch_size,
      const int tolerance_percent) {
        size_t sql_out_data_len = 0;
        constexpr size_t sql_packet_overhead = 62;
        ch::steady_clock::time_point start;
        size_t total_size{};
        ch::milliseconds throttle_time{};
        // consume cannot be measured by the number of fetches because the size
        // of fetch payload is up to funes, "fetch_max_bytes" is merely a
        // guidance. Therefore the consume test runs as long as there is data
        // to fetch. We only can consume almost as much as have been produced:
        const auto sql_data_cap = sql_data_available - batch_size * 2;
        for (int k = -warmup_cycles(
               rate_limit_out, batch_size + sql_packet_overhead);
             sql_out_data_len < sql_data_cap;
             ++k) {
            if (k == 0) {
                start = ch::steady_clock::now();
                total_size = {};
                throttle_time = {};
                BOOST_TEST_WARN(
                  false,
                  "Egress measurement starts. sql_out_data_len: "
                    << sql_out_data_len
                    << ", sql_data_cap: " << sql_data_cap);
            }
            const auto fetch_resp = fetch_next().get0();
            BOOST_REQUIRE_EQUAL(fetch_resp.data.topics.size(), 1);
            BOOST_REQUIRE_EQUAL(fetch_resp.data.topics[0].partitions.size(), 1);
            BOOST_TEST_REQUIRE(
              fetch_resp.data.topics[0].partitions[0].records.has_value());
            const auto sql_data_len = fetch_resp.data.topics[0]
                                          .partitions[0]
                                          .records.value()
                                          .size_bytes();
            total_size += sql_data_len + sql_packet_overhead;
            throttle_time += fetch_resp.data.throttle_time_ms;
            sql_out_data_len += sql_data_len;
        }
        const auto stop = ch::steady_clock::now();
        const auto rate_estimated = rate_limit_out
                                    - _rate_minimum * (ss::smp::count - 1);
        const auto time_estimated = ch::milliseconds(
          total_size * 1000 / rate_estimated);
        BOOST_TEST_CHECK(
          abs(stop - start - time_estimated)
            < (time_estimated * tolerance_percent / 100),
          "Egress time: stop-start["
            << stop - start << "] ≈ time_estimated[" << time_estimated << "] ±"
            << tolerance_percent << "%, error: " << std::setprecision(3)
            << (stop - start - time_estimated) * 100.0 / time_estimated << "%");
        return sql_out_data_len;
    }
};

FIXTURE_TEST(test_node_throughput_limits_static, throughput_limits_fixure) {
    // configure
    constexpr int64_t pershard_rate_limit_in = 9_KiB;
    constexpr int64_t pershard_rate_limit_out = 7_KiB;
    constexpr size_t batch_size = 256;
    config_set(
      "sql_throughput_limit_node_in_bps",
      std::make_optional(pershard_rate_limit_in * ss::smp::count));
    config_set(
      "sql_throughput_limit_node_out_bps",
      std::make_optional(pershard_rate_limit_out * ss::smp::count));
    config_set("fetch_max_bytes", batch_size);
    config_set("max_sql_throttle_delay_ms", 30'000ms);
    config_set_window_width(200ms);
    config_set_balancer_period(0ms);
    config_set_rate_minimum(0);

    wait_for_controller_leadership().get();
    start();

    // PRODUCE 10 KiB in smaller batches, check throttle but do not honour it,
    // check that has to take 1 s
    const size_t sql_in_data_len = test_ingress(
      pershard_rate_limit_in, batch_size, 3 /*%*/);

    // CONSUME
    const size_t sql_out_data_len = test_egress(
      sql_in_data_len, pershard_rate_limit_out, batch_size, 3 /*%*/);

    // otherwise test is not valid:
    BOOST_REQUIRE_GT(sql_in_data_len, sql_out_data_len);
}

FIXTURE_TEST(test_node_throughput_limits_balanced, throughput_limits_fixure) {
    // configure
    constexpr int64_t rate_limit_in = 9_KiB;
    constexpr int64_t rate_limit_out = 7_KiB;
    constexpr size_t batch_size = 256;
    config_set(
      "sql_throughput_limit_node_in_bps", std::make_optional(rate_limit_in));
    config_set(
      "sql_throughput_limit_node_out_bps",
      std::make_optional(rate_limit_out));
    config_set("fetch_max_bytes", batch_size);
    config_set("max_sql_throttle_delay_ms", 30'000ms);
    config_set("sql_quota_balancer_min_shard_throughput_ratio", 0.);
    config_set_window_width(100ms);
    config_set_balancer_period(50ms);
    config_set_rate_minimum(250);

    wait_for_controller_leadership().get();
    start();

    // PRODUCE 10 KiB in smaller batches, check throttle but do not honour it,
    // check that has to take 1 s
    const size_t sql_in_data_len = test_ingress(
      rate_limit_in, batch_size, 8 /*%*/);

    // CONSUME
    size_t sql_out_data_len = test_egress(
      sql_in_data_len, rate_limit_out, batch_size, 8 /*%*/);

    // otherwise test is not valid:
    BOOST_REQUIRE_GT(sql_in_data_len, sql_out_data_len);

    if (ss::smp::count <= 2) {
        // the following tests are only valid when shards count is greater
        // than the # of partintions produced to / consumed from
        return;
    }

    const auto collect_quotas_minmax =
      [this](
        std::function<sql::snc_quota_manager::quota_t(
          const sql::snc_quota_manager&)> mapper,
        const char* const direction) {
          const auto quotas = app.snc_quota_mgr.map(std::move(mapper)).get0();
          info("quotas_{}: {}", direction, quotas);
          const auto quotas_minmax = std::minmax_element(
            quotas.cbegin(), quotas.cend());
          return std::pair{*quotas_minmax.first, *quotas_minmax.second};
      };

    const auto quotas_b_minmax_in = collect_quotas_minmax(
      [](const sql::snc_quota_manager& qm) { return qm.get_quota().in; },
      "in");
    const auto quotas_b_minmax_eg = collect_quotas_minmax(
      [](const sql::snc_quota_manager& qm) { return qm.get_quota().eg; },
      "eg");

    // verify that the effective quota is distributed very unevenly
    BOOST_CHECK_GT(quotas_b_minmax_in.second, quotas_b_minmax_in.first * 5);
    BOOST_CHECK_GT(quotas_b_minmax_eg.second, quotas_b_minmax_eg.first * 5);

    // verify that the minimum quota has been honoured
    BOOST_CHECK_GE(quotas_b_minmax_in.first, _rate_minimum);
    BOOST_CHECK_GE(quotas_b_minmax_in.first, _rate_minimum);

    // disable the balancer; that should reset effective quotas to default
    config_set_balancer_period(0ms);

    const auto quotas_e_minmax_in = collect_quotas_minmax(
      [](const sql::snc_quota_manager& qm) { return qm.get_quota().in; },
      "in");
    const auto quotas_e_minmax_eg = collect_quotas_minmax(
      [](const sql::snc_quota_manager& qm) { return qm.get_quota().eg; },
      "eg");

    // when the balancer is off, effective quotas is reset to default quotas
    // which are evenly distributed across shards: max-min<=1
    BOOST_CHECK_LE(quotas_e_minmax_in.second, quotas_e_minmax_in.first + 1);
    BOOST_CHECK_LE(quotas_e_minmax_eg.second, quotas_e_minmax_eg.first + 1);
}

FIXTURE_TEST(test_quota_balancer_config_balancer_period, prod_consume_fixture) {
    namespace ch = std::chrono;

    auto get_balancer_runs = [this] {
        return app.snc_quota_mgr
          .map_reduce0(
            [](const sql::snc_quota_manager& qm) {
                return qm.get_snc_quotas_probe().get_balancer_runs();
            },
            0,
            [](uint32_t lhs, uint32_t rhs) { return lhs + rhs; })
          .get0();
    };

    auto set_balancer_period = [](const ch::milliseconds d) {
        ss::smp::invoke_on_all([&] {
            auto& config = config::shard_local_cfg();
            config.get("sql_quota_balancer_node_period_ms").set_value(d);
        }).get0();
    };

    wait_for_controller_leadership().get();

    // Since the test is timing sensitive, allow 3 attempts before failing
    for (int attempt = 0; attempt != 3; ++attempt) {
        bool succ = true;

        set_balancer_period(25ms);
        int br_last = get_balancer_runs();
        ss::sleep(100ms).get0();
        int br = get_balancer_runs();
        BOOST_TEST_WARN(
          abs(br - br_last - 4) <= 1,
          "Expected 4±1 balancer runs, got " << br << ", attempt " << attempt);
        succ = succ && abs(br - br_last - 4) <= 1;

        set_balancer_period(0ms);
        br_last = get_balancer_runs();
        ss::sleep(100ms).get0();
        br = get_balancer_runs();
        BOOST_TEST_WARN(
          abs(br - br_last - 0) <= 1,
          "Expected 0±1 balancer runs, got " << br - br_last << ", attempt "
                                             << attempt);
        succ = succ && abs(br - br_last - 0) <= 1;

        set_balancer_period(15ms);
        br_last = get_balancer_runs();
        ss::sleep(100ms).get0();
        br = get_balancer_runs();
        BOOST_TEST_WARN(
          abs(br - br_last - 7) <= 1,
          "Expected 7±1 balancer runs, got " << br - br_last << ", attempt "
                                             << attempt);
        succ = succ && abs(br - br_last - 7) <= 1;

        if (succ) {
            break;
        }
        BOOST_TEST_CHECK(
          attempt < 2,
          "3 test attempts have failed, check test warnings above for details");
    }
}

// TODO: move producer utilities somewhere else and give this test a proper
// home.
FIXTURE_TEST(test_offset_for_leader_epoch, prod_consume_fixture) {
    producer = std::make_unique<sql::client::transport>(
      make_sql_client().get0());
    producer->connect().get0();
    model::topic_namespace tp_ns(model::ns("sql"), test_topic);
    add_topic(tp_ns).get0();
    model::ntp ntp(tp_ns.ns, tp_ns.tp, model::partition_id(0));
    tests::cooperative_spin_wait_with_timeout(10s, [ntp, this] {
        auto shard = app.shard_table.local().shard_for(ntp);
        if (!shard) {
            return ss::make_ready_future<bool>(false);
        }
        return app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& pm) {
              return pm.get(ntp)->is_leader();
          });
    }).get0();
    auto shard = app.shard_table.local().shard_for(ntp);
    for (int i = 0; i < 3; i++) {
        // Refresh leadership.
        app.partition_manager
          .invoke_on(
            *shard,
            [ntp](cluster::partition_manager& mgr) {
                auto raft = mgr.get(ntp)->raft();
                raft->step_down("force_step_down").get();
                tests::cooperative_spin_wait_with_timeout(10s, [raft] {
                    return raft->is_leader();
                }).get0();
            })
          .get();
        app.partition_manager
          .invoke_on(
            *shard,
            [this, ntp](cluster::partition_manager& mgr) {
                auto partition = mgr.get(ntp);
                produce([this](size_t cnt) {
                    return small_batches(cnt);
                }).get0();
            })
          .get();
    }
    // Prefix truncate the log so the beginning of the log moves forward.
    app.partition_manager
      .invoke_on(
        *shard,
        [ntp](cluster::partition_manager& mgr) {
            auto partition = mgr.get(ntp);
            storage::truncate_prefix_config cfg(
              model::offset(1), ss::default_priority_class());
            partition->log()->truncate_prefix(cfg).get();
        })
      .get();

    // Make a request getting the offset from a term below the start of the
    // log.
    auto client = make_sql_client().get0();
    client.connect().get();
    auto current_term = app.partition_manager
                          .invoke_on(
                            *shard,
                            [ntp](cluster::partition_manager& mgr) {
                                return mgr.get(ntp)->raft()->term();
                            })
                          .get();
    sql::offset_for_leader_epoch_request req;
    sql::offset_for_leader_topic t{
      test_topic,
      {{model::partition_id(0),
        sql::leader_epoch(current_term()),
        sql::leader_epoch(0)}},
      {},
    };
    req.data.topics.emplace_back(std::move(t));
    auto resp = client.dispatch(req, sql::api_version(2)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();
    BOOST_REQUIRE_EQUAL(1, resp.data.topics.size());
    const auto& topic_resp = resp.data.topics[0];
    BOOST_REQUIRE_EQUAL(1, topic_resp.partitions.size());
    const auto& partition_resp = topic_resp.partitions[0];

    BOOST_REQUIRE_NE(partition_resp.end_offset, model::offset(-1));

    // Check that the returned offset is the start of the log, since the
    // requested term has been truncated.
    auto earliest_sql_offset
      = app.partition_manager
          .invoke_on(
            *shard,
            [ntp](cluster::partition_manager& mgr) {
                auto partition = mgr.get(ntp);
                auto start_offset = partition->log()->offsets().start_offset;
                return partition->get_offset_translator_state()
                  ->from_log_offset(start_offset);
            })
          .get();
    BOOST_REQUIRE_EQUAL(earliest_sql_offset, partition_resp.end_offset);
}

FIXTURE_TEST(test_basic_delete_around_batch, prod_consume_fixture) {
    wait_for_controller_leadership().get0();
    start();
    const model::topic_namespace tp_ns(model::ns("sql"), test_topic);
    const model::partition_id pid(0);
    const model::ntp ntp(tp_ns.ns, tp_ns.tp, pid);
    auto partition = app.partition_manager.local().get(ntp);
    auto log = partition->log();

    tests::sql_produce_transport producer(make_sql_client().get());
    producer.start().get();
    producer
      .produce_to_partition(
        test_topic,
        model::partition_id(0),
        vector<kv_t>{
          {"key0", "val0"},
          {"key1", "val1"},
          {"key2", "val2"},
        })
      .get();
    producer
      .produce_to_partition(
        test_topic,
        model::partition_id(0),
        vector<kv_t>{
          {"key3", "val3"},
          {"key4", "val4"},
        })
      .get();
    producer
      .produce_to_partition(
        test_topic,
        model::partition_id(0),
        vector<kv_t>{
          {"key5", "val5"},
          {"key6", "val6"},
        })
      .get();
    log->flush().get();
    log->force_roll(ss::default_priority_class()).get();
    BOOST_REQUIRE_EQUAL(2, log->segments().size());

    tests::sql_consume_transport consumer(make_sql_client().get());
    consumer.start().get();
    tests::sql_delete_records_transport deleter(make_sql_client().get());
    deleter.start().get();

    // At this point, we have three batches:
    //  [0, 2] [3, 4], [5, 6]
    const auto check_consume_out_of_range = [&](model::offset sql_offset) {
        BOOST_REQUIRE_EXCEPTION(
          consumer.consume_from_partition(test_topic, pid, sql_offset).get(),
          std::runtime_error,
          [](std::runtime_error e) {
              return std::string(e.what()).find("out_of_range")
                     != std::string::npos;
          });
    };
    {
        // Delete in the middle of an offset.
        auto lwm = deleter
                     .delete_records_from_partition(
                       test_topic, pid, model::offset(1), 5s)
                     .get();
        BOOST_CHECK_EQUAL(model::offset(1), lwm);
        // We should fail to consume below the start offset.
        check_consume_out_of_range(model::offset(0));
        // But the data may still be returned, and is expected to be filtered
        // client-side.
        auto consumed_records
          = consumer.consume_from_partition(test_topic, pid, model::offset(1))
              .get();
        BOOST_REQUIRE(!consumed_records.empty());
        BOOST_REQUIRE_EQUAL("key0", consumed_records[0].key);
    }

    {
        // Delete at the start of a batch boundary.
        auto lwm = deleter
                     .delete_records_from_partition(
                       test_topic, pid, model::offset(3), 5s)
                     .get();
        BOOST_CHECK_EQUAL(model::offset(3), lwm);
        check_consume_out_of_range(model::offset(2));
        // No extraneous data should exist.
        auto consumed_records
          = consumer.consume_from_partition(test_topic, pid, model::offset(3))
              .get();
        BOOST_REQUIRE(!consumed_records.empty());
        BOOST_REQUIRE_EQUAL("key3", consumed_records[0].key);
    }

    {
        // Delete near the end.
        auto lwm = deleter
                     .delete_records_from_partition(
                       test_topic, pid, model::offset(6), 5s)
                     .get();
        BOOST_CHECK_EQUAL(model::offset(6), lwm);
        check_consume_out_of_range(model::offset(5));
        // The entire batch is read.
        auto consumed_records
          = consumer.consume_from_partition(test_topic, pid, model::offset(6))
              .get();
        BOOST_REQUIRE(!consumed_records.empty());
        BOOST_REQUIRE_EQUAL("key5", consumed_records[0].key);
    }
    auto lwm = deleter
                 .delete_records_from_partition(
                   test_topic, pid, model::offset(7), 5s)
                 .get();
    BOOST_REQUIRE_EQUAL(model::offset(7), lwm);
    producer
      .produce_to_partition(
        test_topic,
        model::partition_id(0),
        vector<kv_t>{
          {"key7", "val7"},
        })
      .get();
    log->flush().get();
    check_consume_out_of_range(model::offset(6));
    auto consumed_records
      = consumer.consume_from_partition(test_topic, pid, model::offset(7))
          .get();
    BOOST_REQUIRE(!consumed_records.empty());
    BOOST_REQUIRE_EQUAL("key7", consumed_records[0].key);
}

FIXTURE_TEST(test_produce_bad_timestamps, prod_consume_fixture) {
    /*
     * this tests produces messages with timestamps in the future and in the
     * past, and checks that the metric
     * vectorized_sql_rpc_produces_with_timestamps_out_of_bounds is correctly
     * incremented.
     */

    wait_for_controller_leadership().get0();
    start();
    auto ntp = model::ntp(
      model::ns("sql"), test_topic, model::partition_id(0));

    auto producer = tests::sql_produce_transport(make_sql_client().get());
    producer.start().get();

    // helper to produce a bunch of messages with some drift applied to the
    // timestamps. the drift is the same for all the messages, but a more
    // advaced test would be to have a range of drifts from start to finish
    auto produce_messages = [&](std::chrono::system_clock::duration drift) {
        producer
          .produce_to_partition(
            ntp.tp.topic,
            ntp.tp.partition,
            {
              {"key0", "val0"},
              {"key1", "val1"},
              {"key2", "val2"},
            },
            model::to_timestamp(std::chrono::system_clock::now() + drift))
          .get();
    };

    BOOST_TEST_INFO("expect produce_bad_create_time to be 0");
    auto bad_timestamps_metric
      = app._sql_server.local().probe().get_produce_bad_create_time();
    BOOST_CHECK_EQUAL(0, bad_timestamps_metric);

    BOOST_TEST_INFO("messages with no skew do not trigger the probe");
    produce_messages(0s);
    BOOST_CHECK_EQUAL(
      0, app._sql_server.local().probe().get_produce_bad_create_time());

    BOOST_TEST_INFO(
      "messages with a skew towards the future trigger the probe");
    config::shard_local_cfg().log_message_timestamp_alert_after_ms.set_value(
      std::chrono::duration_cast<std::chrono::milliseconds>(1h));
    produce_messages(2h);
    BOOST_CHECK_LT(
      bad_timestamps_metric,
      app._sql_server.local().probe().get_produce_bad_create_time());

    bad_timestamps_metric
      = app._sql_server.local().probe().get_produce_bad_create_time();

    BOOST_TEST_INFO("messages with a skew towards the past trigger the probe");
    config::shard_local_cfg().log_message_timestamp_alert_before_ms.set_value(
      std::optional{std::chrono::duration_cast<std::chrono::milliseconds>(1h)});
    produce_messages(-2h);
    BOOST_CHECK_LT(
      bad_timestamps_metric,
      app._sql_server.local().probe().get_produce_bad_create_time());

    bad_timestamps_metric
      = app._sql_server.local().probe().get_produce_bad_create_time();

    BOOST_TEST_INFO("messages within the bounds to not trigger the probe");
    produce_messages(-30min);
    produce_messages(30min);
    BOOST_CHECK_EQUAL(
      bad_timestamps_metric,
      app._sql_server.local().probe().get_produce_bad_create_time());

    BOOST_TEST_INFO("disabling the alert for the past allows messages in the "
                    "past without triggering the probe");
    config::shard_local_cfg().log_message_timestamp_alert_before_ms.set_value(
      std::optional<std::chrono::milliseconds>{});
    produce_messages(-365 * 24h);
    BOOST_CHECK_EQUAL(
      bad_timestamps_metric,
      app._sql_server.local().probe().get_produce_bad_create_time());
}
