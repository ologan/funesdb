# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from rptest.clients.rpk import RpkTool
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.funes import RESTART_LOG_ALLOW_LIST
from rptest.tests.funes_test import FunesTest


class MetadataTest(FunesTest):
    def controller_present(self):
        return self.funes.controller() is not None

    @cluster(num_nodes=3)
    def test_metadata_request_contains_all_brokers(self):
        """
        Check if broker list returned from metadata request is complete
        """
        wait_until(lambda: self.controller_present, 10, 1)
        rpk = RpkTool(self.funes)
        nodes = rpk.cluster_info()
        assert len(nodes) == 3
        all_ids = [self.funes.idx(n) for n in self.funes.nodes]
        returned_node_ids = [n.id for n in nodes]
        assert sorted(all_ids) == sorted(returned_node_ids)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(failure=['isolate', 'stop'], node=['follower', 'controller'])
    def test_metadata_request_does_not_contain_failed_node(
            self, failure, node):
        """
        Check if broker list returned from metadata request does not contain node
        which is not alive
        """
        # validate initial conditions
        wait_until(lambda: self.controller_present, 10, 1)
        rpk = RpkTool(self.funes)
        nodes = rpk.cluster_info()
        assert len(nodes) == 3
        funes_ids = [self.funes.idx(n) for n in self.funes.nodes]
        node_ids = [n.id for n in nodes]
        assert sorted(funes_ids) == sorted(node_ids)

        def get_node():
            if node == 'controller':
                return self.funes.controller()
            else:
                n = self.funes.nodes[0]
                while n == self.funes.controller():
                    n = random.choice(self.funes.nodes)
                return n

        node = get_node()
        node_id = self.funes.idx(node)
        self.funes.logger.info(
            f"Injecting failure on node {node.account.hostname} with id: {node_id}",
        )
        with FailureInjector(self.funes) as fi:
            if failure == "isolate":
                fi.inject_failure(
                    FailureSpec(FailureSpec.FAILURE_ISOLATE, node))
            else:
                self.funes.stop_node(node)

            rpk = RpkTool(self.funes)

            def contains_only_alive_nodes():
                nodes = rpk.cluster_info()
                returned_ids = [n.id for n in nodes]
                return len(nodes) == 2 and node_id not in returned_ids

            wait_until(contains_only_alive_nodes, 60)
