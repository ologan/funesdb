# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.tests.test import Test
from rptest.services.cluster import cluster
from rptest.services.funes_cloud import FunesServiceK8s


class SimpleK8sTest(Test):
    def __init__(self, test_context):
        """
        Keep it simple.
        """
        super(SimpleK8sTest, self).__init__(test_context)
        self.funes = FunesServiceK8s(test_context, 1)

    @cluster(num_nodes=1, check_allowed_error_logs=False)
    def test_k8s(self):
        '''
        Validate startup of k8s.
        '''
        self.funes.start_node(None)
        node_memory = float(self.funes.get_node_memory_mb())
        assert node_memory > 1.0

        node_cpu_count = self.funes.get_node_cpu_count()
        assert node_cpu_count > 0

        node_disk_free = self.funes.get_node_disk_free()
        assert node_disk_free > 0

        self.funes.lsof_node(1)

        self.funes.set_cluster_config({})
