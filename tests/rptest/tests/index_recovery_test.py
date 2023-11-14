# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from os.path import join

from rptest.tests.funes_test import FunesTest
from rptest.services.admin import Admin
from rptest.services.funes import FunesService, make_funes_service
from rptest.services.admin import Admin


class IndexRecoveryTest(FunesTest):
    def __init__(self, test_context):
        super(IndexRecoveryTest, self).__init__(test_context=test_context,
                                                log_level="trace")
        self.admin = Admin(self.funes)

    @cluster(num_nodes=1)
    def index_exists_test(self):
        single_node = self.funes.started_nodes()[0]
        self.funes.stop_node(single_node)
        self.funes.start_node(single_node)
        self.admin.wait_stable_configuration("controller",
                                             namespace="funes",
                                             replication=1)
        path = join(FunesService.DATA_DIR, "funes", "controller", "0_0")
        files = single_node.account.ssh_output(f"ls {path}").decode(
            "utf-8").splitlines()
        indices = [x for x in files if x.endswith(".base_index")]
        assert len(indices) > 0, "restart should create base_index file"

    @cluster(num_nodes=1)
    def index_recovers_test(self):
        single_node = self.funes.started_nodes()[0]
        self.funes.stop_node(single_node)
        self.funes.start_node(single_node)
        self.admin.wait_stable_configuration("controller",
                                             namespace="funes",
                                             replication=1)
        self.funes.stop_node(single_node)
        path = join(FunesService.DATA_DIR, "funes", "controller", "0_0")
        files = single_node.account.ssh_output(f"ls {path}").decode(
            "utf-8").splitlines()
        indices = [x for x in files if x.endswith(".base_index")]
        assert len(indices) > 0, "restart should create base_index file"
        last_index = sorted(indices)[-1]
        path = join(FunesService.DATA_DIR, "funes", "controller", "0_0",
                    last_index)
        single_node.account.ssh_output(f"rm {path}")
        self.funes.start_node(single_node)
        self.admin.wait_stable_configuration("controller",
                                             namespace="funes",
                                             replication=1)
        assert single_node.account.exists(path)
