# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import ducktape.errors
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from requests.exceptions import ConnectionError
from rptest.services.cluster import cluster
from rptest.services.funes import RESTART_LOG_ALLOW_LIST
from rptest.services.funes_installer import (FunesInstaller,
                                                wait_for_num_versions)
from rptest.util import expect_exception
from rptest.tests.funes_test import FunesTest


def set_seeds_for_cluster(funes, num_seeds):
    seeds = [funes.nodes[i] for i in range(num_seeds)]
    funes.set_seed_servers(seeds)


class ClusterBootstrapNew(FunesTest):
    """
    Tests verifying new cluster bootstrap in Seed Driven Cluster Bootstrap mode
    """
    def __init__(self, test_context):
        super(ClusterBootstrapNew, self).__init__(test_context=test_context,
                                                  num_brokers=3)
        self.admin = self.funes._admin

    def setUp(self):
        # Defer startup to test body.
        pass

    @cluster(num_nodes=3, log_allow_list=["seed_servers cannot be empty"])
    def test_misconfigured_root_driven_bootstrap(self):
        """
        Test that empty_seed_starts_cluster=False prevents root-driven
        bootstrap from occurring.
        """
        for node in self.funes.nodes:
            self.funes.set_extra_node_conf(
                node, {"empty_seed_starts_cluster": False})

        # setup seed servers on the other two nodes to prevent them from joining
        # cluster point the nodes to node 0
        for node in self.funes.nodes[1:]:
            self.funes.set_seed_servers([self.funes.nodes[0]])

        try:
            self.funes.start(omit_seeds_on_idx_one=True)
            assert False, "Should have been unable to start"
        except ducktape.errors.TimeoutError:
            # The cluster should be unable to start.
            pass

        for node in self.funes.nodes:
            idx = self.funes.idx(node)

            # None of the nodes was configured in a way that could get past attempting
            # to join a cluster: node 1 has no seed servers, and nodes 2,3 are not in
            # their seed servers so do not self-identify as founders
            with expect_exception(ConnectionError, lambda _: True):
                # Try connecting to the admin API
                self.funes._admin.get_cluster_uuid(node)

    @cluster(num_nodes=3)
    @matrix(num_seeds=[1, 2, 3],
            auto_assign_node_ids=[False, True],
            empty_seed_starts_cluster=[False, True])
    def test_three_node_bootstrap(self, num_seeds, auto_assign_node_ids,
                                  empty_seed_starts_cluster):
        set_seeds_for_cluster(self.funes, num_seeds)
        for node in self.funes.nodes:
            self.funes.set_extra_node_conf(
                node, {"empty_seed_starts_cluster": empty_seed_starts_cluster})
        self.funes.start(auto_assign_node_id=auto_assign_node_ids,
                            omit_seeds_on_idx_one=False)
        node_ids_per_idx = {}
        for n in self.funes.nodes:
            idx = self.funes.idx(n)
            node_ids_per_idx[idx] = self.funes.node_id(n)

        brokers = self.admin.get_brokers()
        assert 3 == len(brokers), f"Got {len(brokers)} brokers"

        # Restart our nodes and make sure our node IDs persist across restarts.
        self.funes.restart_nodes(self.funes.nodes,
                                    auto_assign_node_id=auto_assign_node_ids,
                                    omit_seeds_on_idx_one=False)
        for idx in node_ids_per_idx:
            n = self.funes.get_node(idx)
            expected_node_id = node_ids_per_idx[idx]
            node_id = self.funes.node_id(n)
            assert expected_node_id == node_id, f"Expected {expected_node_id} but got {node_id}"


class ClusterBootstrapUpgrade(FunesTest):
    """
    Tests verifying upgrade of cluster from a pre-Seed-Driven-Bootstrap version
    """
    def __init__(self, test_context):
        super(ClusterBootstrapUpgrade,
              self).__init__(test_context=test_context, num_brokers=3)
        self.installer = self.funes._installer
        self.admin = self.funes._admin

    def setUp(self):
        prev_version = self.installer.highest_from_prior_feature_version(
            FunesInstaller.HEAD)
        # NOTE: `rpk funes admin brokers list` requires versions v22.1.x and
        # above.
        _, self.oldversion_str = self.installer.install(
            self.funes.nodes, prev_version)
        set_seeds_for_cluster(self.funes, 3)
        super(ClusterBootstrapUpgrade, self).setUp()

    @cluster(num_nodes=3)
    @matrix(empty_seed_starts_cluster=[False, True])
    def test_change_bootstrap_configs_after_upgrade(self,
                                                    empty_seed_starts_cluster):
        # Upgrade the cluster to begin using the new binary, but don't change
        # any configs yet.
        self.installer.install(self.funes.nodes, FunesInstaller.HEAD)
        self.funes.rolling_restart_nodes(self.funes.nodes)

        # Now update the configs.
        self.funes.rolling_restart_nodes(self.funes.nodes,
                                            override_cfg_params={
                                                "empty_seed_starts_cluster":
                                                empty_seed_starts_cluster
                                            },
                                            omit_seeds_on_idx_one=False)

    @cluster(num_nodes=3)
    @matrix(empty_seed_starts_cluster=[False, True])
    def test_change_bootstrap_configs_during_upgrade(
            self, empty_seed_starts_cluster):
        # Upgrade the cluster as we change the configs node-by-node.
        self.installer.install(self.funes.nodes, FunesInstaller.HEAD)
        self.funes.rolling_restart_nodes(self.funes.nodes,
                                            override_cfg_params={
                                                "empty_seed_starts_cluster":
                                                empty_seed_starts_cluster
                                            },
                                            omit_seeds_on_idx_one=False)
