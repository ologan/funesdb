/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Funes Enterprise file under the Funes Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/funes/blob/master/licenses/rcl.md
 */

#include "cloud_storage/tests/produce_utils.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "sql/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "funes/tests/fixture.h"
#include "storage/disk_log_impl.h"
#include "test_utils/scoped_config.h"

class cloud_storage_manual_multinode_test_base
  : public s3_imposter_fixture
  , public funes_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    cloud_storage_manual_multinode_test_base()
      : funes_thread_fixture(
        funes_thread_fixture::init_cloud_storage_tag{},
        httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});

        test_local_cfg.get("cloud_storage_enable_segment_merging")
          .set_value(false);
        test_local_cfg.get("cloud_storage_disable_upload_loop_for_tests")
          .set_value(true);

        wait_for_controller_leadership().get();
    }

    std::unique_ptr<funes_thread_fixture> start_second_fixture() {
        return std::make_unique<funes_thread_fixture>(
          model::node_id(2),
          9092 + 10,
          33145 + 10,
          8082 + 10,
          8081 + 10,
          std::vector<config::seed_server>{
            {.addr = net::unresolved_address("127.0.0.1", 33145)}},
          ssx::sformat("test.second_dir{}", time(0)),
          app.sched_groups,
          true,
          get_s3_config(httpd_port_number()),
          get_archival_config(),
          get_cloud_config(httpd_port_number()));
    }
    scoped_config test_local_cfg;
};
