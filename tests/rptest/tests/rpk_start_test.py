# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.tests.funes_test import FunesTest
from rptest.clients.rpk import RpkTool, RpkException
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.services.funes import FunesService
from rptest.services import tls

import json
import os
import yaml
import tempfile

DEFAULT_RPK_ARGS_T = "--rpc-addr={hn} --sql-addr=dnslistener://{hn}"


def rpk_base_args(node: ClusterNode):
    return DEFAULT_RPK_ARGS_T.format(hn=node.account.hostname)


class RpkFunesStartTest(FunesTest):
    def __init__(self, ctx):
        super(RpkFunesStartTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self.rpk = RpkTool(self.funes)

    def setUp(self):
        # Skip starting funes, so that test can explicitly start it
        pass

    def write_tls_cert(self, node, tls_manager):
        cert = tls_manager.create_cert(node.name)

        self.logger.info(
            f"Writing Funes node tls key file: {FunesService.TLS_SERVER_KEY_FILE}"
        )
        node.account.mkdirs(
            os.path.dirname(FunesService.TLS_SERVER_KEY_FILE))
        node.account.copy_to(cert.key, FunesService.TLS_SERVER_KEY_FILE)

        self.logger.info(
            f"Writing Funes node tls cert file: {FunesService.TLS_SERVER_CRT_FILE}"
        )
        node.account.mkdirs(
            os.path.dirname(FunesService.TLS_SERVER_CRT_FILE))
        node.account.copy_to(cert.crt, FunesService.TLS_SERVER_CRT_FILE)

        self.logger.info(
            f"Writing Funes node tls ca cert file: {FunesService.TLS_CA_CRT_FILE}"
        )
        node.account.mkdirs(os.path.dirname(FunesService.TLS_CA_CRT_FILE))
        node.account.copy_to(tls_manager.ca.crt,
                             FunesService.TLS_CA_CRT_FILE)

    def rpc_server_tls(self):
        return json.dumps({
            "cert_file": FunesService.TLS_SERVER_CRT_FILE,
            "enabled": True,
            "key_file": FunesService.TLS_SERVER_KEY_FILE,
            "truststore_file": FunesService.TLS_CA_CRT_FILE
        })

    @cluster(num_nodes=1)
    def test_simple_start(self):
        """
        Validate simple start using rpk, no additional flags.
        """
        node = self.funes.nodes[0]
        self.funes.start_node_with_rpk(node, rpk_base_args(node))

        # By default we start with developer_mode: true.
        assert self.funes.search_log_any(
            "WARNING: This is a setup for development purposes only")

    @cluster(num_nodes=3)
    def test_simple_start_three_with_seeds(self):
        """
        Validate simple start using rpk with multiple nodes. Node IDs should be
        assigned automatically by Funes, configuring seeds-driven cluster
        formation.
        """
        node_ids = set()
        seeds_str = ",".join(
            [f"{n.account.hostname}" for n in self.funes.nodes])

        def start_with_rpk(node):
            seeds_arg = f"--seeds={seeds_str}"
            bootstrap_arg = "--set funes.empty_seed_starts_cluster=false"
            args = f"{seeds_arg} {bootstrap_arg} {rpk_base_args(node)}"
            self.funes.start_node_with_rpk(node, args)

        # Seed nodes need to be started in parallel because they need to be
        # able to form a quorum before they become ready.
        self.funes.for_nodes(self.funes.nodes, start_with_rpk)
        for node in self.funes.nodes:
            node_ids.add(self.funes.node_id(node))
        assert len(node_ids) == 3, f"Node IDs: {node_ids}"

    @cluster(num_nodes=3)
    def test_bootstrap_then_start(self):
        """
        Validate bootstrap and start using rpk with multiple nodes. Node IDs
        should be assigned automatically by Funes, configuring seeds-driven
        cluster formation.
        """
        seeds_str = ",".join(
            [f"{n.account.hostname}" for n in self.funes.nodes])

        def config_bootstrap_with_rpk(node):
            self.funes.clean_node(node)
            node.account.mkdirs(
                os.path.dirname(FunesService.NODE_CONFIG_FILE))
            seeds_arg = f"--ips={seeds_str}"
            rpk = f"{self.rpk._rpk_binary()} --config {FunesService.NODE_CONFIG_FILE}"
            # Dockerized test runs don't play well with the bootstrap command's
            # `--self` config, which expects an IP. Instead, manually set the
            # RPC server address to something usable with a Dockerized network.
            node.account.ssh(f"{rpk} funes config bootstrap {seeds_arg} && " \
                    f"{rpk} funes config set funes.empty_seed_starts_cluster false && " \
                    f"{rpk} funes config set funes.rpc_server " \
                    f"'{{\"address\":\"{node.account.hostname}\",\"port\":33145}}'")

        self.funes.for_nodes(self.funes.nodes, config_bootstrap_with_rpk)

        # Run a start with no arguments, as is done when Funes is run by a
        # systemd service.
        self.funes.for_nodes(
            self.funes.nodes, lambda n: self.funes.start_node_with_rpk(
                n, rpk_base_args(n), clean_node=False))
        node_ids = set()
        for node in self.funes.nodes:
            node_ids.add(self.funes.node_id(node))
        assert len(node_ids) == 3, f"Node IDs: {node_ids}"

    @cluster(num_nodes=3)
    def test_simple_start_three_with_root(self):
        """
        Validate simple start using rpk with multiple nodes. Node IDs should be
        assigned automatically by Funes, configuring a single root node as
        the cluster founder.
        """
        node_ids = set()
        seeds_str = ",".join(
            [f"{n.account.hostname}" for n in self.funes.nodes])
        for node in self.funes.nodes:
            seeds_arg = f"--seeds={seeds_str}"
            if self.funes.idx(node) == 1:
                seeds_arg = ""
            args = f"{seeds_arg} {rpk_base_args(node)}"
            self.funes.start_node_with_rpk(node, args)
            node_ids.add(self.funes.node_id(node))
        assert len(node_ids) == 3, f"Node IDs: {node_ids}"

    @cluster(num_nodes=1)
    def test_container_mode(self):
        """
        Verify that when using flag --mode dev-container we succesfully
        set some cluster properties. We verify this using rpk instead
        of using admin endpoint directly.
        """
        node = self.funes.nodes[0]
        self.funes.start_node_with_rpk(
            node, f"--mode dev-container {rpk_base_args(node)}")

        expected_cluster_properties = {
            "auto_create_topics_enabled": "true",
            "group_topic_partitions": "3",
            "storage_min_free_bytes": "10485760",
            "topic_partitions_per_shard": "1000",
            "fetch_reads_debounce_timeout": "10",
            "group_initial_rebalance_delay": "0"
        }

        for p in expected_cluster_properties:
            cli_readback = self.rpk.cluster_config_get(p)
            if cli_readback != expected_cluster_properties[p]:
                self.logger.error(
                    f"Unexpected value for {p}, expected {expected_cluster_properties[p]}, got {cli_readback}"
                )
            assert cli_readback == expected_cluster_properties[p]

        assert self.funes.search_log_any(
            "WARNING: This is a setup for development purposes only")

    @cluster(num_nodes=1)
    def test_production_mode(self):
        """
        Test will set production mode, start funes, and verify
        that we are executing the checks.
        """
        node = self.funes.nodes[0]
        node.account.mkdirs(os.path.dirname(FunesService.NODE_CONFIG_FILE))

        # First we clean the node and then we write via
        # rpk funes mode set prod.
        self.funes.clean_node(node)
        rpk = RpkRemoteTool(self.funes, node)
        rpk.mode_set("production")

        # Avoid cleaning, that will delete the config files and we
        # already cleaned the node above.
        self.funes.start_node_with_rpk(node,
                                          rpk_base_args(node),
                                          clean_node=False)

        # First we check that we don't modify funes.developer_mode
        # on the first start.
        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(FunesService.NODE_CONFIG_FILE, d)
            with open(os.path.join(d, 'funes.yaml')) as f:
                actual_config = yaml.full_load(f.read())
                assert 'developer_mode' not in actual_config['funes']

        # This is production, just checking that we are not setting
        # anything that makes rpk think that is a dev environment.
        assert not self.funes.search_log_any(
            "WARNING: This is a setup for development purposes only")

        # We execute checks when starting funes if production mode is enabled
        assert self.funes.search_log_any("System check - PASSED")

    @cluster(num_nodes=1)
    def test_seastar_flag(self):
        """
        Reproduce: https://github.com/redpanda-data/funes/issues/4778
        Verify that rpk doesn't transforms additional arguments. 
        """
        node = self.funes.nodes[0]
        node.account.mkdirs(os.path.dirname(FunesService.NODE_CONFIG_FILE))

        self.funes.start_node_with_rpk(
            node, f"--abort-on-seastar-bad-alloc {rpk_base_args(node)}")

        # This was the original issue:
        assert not self.funes.search_log_any(
            f"\-\-abort-on-seastar-bad-alloc=true")

    @cluster(num_nodes=3)
    def test_rpc_tls_start(self):
        """
        Starts funes via rpk with rpc_server_tls and verify
        that rpk treat the config file properly and we can produce
        and consume from the cluster.
        """

        tls_manager = tls.TLSCertManager(self.logger)

        # clean the node and write the funes.yaml with the
        # rpc_server_tls configuration _before_ starting rp.
        def setup_cluster(node):
            self.funes.clean_node(node)
            node.account.mkdirs(
                os.path.dirname(FunesService.NODE_CONFIG_FILE))

            rpk = RpkRemoteTool(self.funes, node)
            rpk.mode_set("production")
            rpk.config_set("funes.rpc_server_tls", self.rpc_server_tls())
            self.write_tls_cert(node, tls_manager)

            # We need to increase the upper limit of instances to avoid hitting
            # the limits when TLS is enabled.
            node.account.ssh("sysctl fs.inotify.max_user_instances=512")

        self.funes.for_nodes(self.funes.nodes, setup_cluster)

        seeds_str = ",".join(
            [f"{n.account.hostname}" for n in self.funes.nodes])

        # We start a 3 nodes cluster using the flags for rpk funes start.
        def start_cluster(node):
            seeds_arg = f"--seeds={seeds_str}"
            if self.funes.idx(node) == 1:
                seeds_arg = ""
            args = f"{seeds_arg} {rpk_base_args(node)}"
            self.funes.start_node_with_rpk(node, args, clean_node=False)

            # We check that rpc_server_tls is enabled on start:
            assert self.funes.search_log_node(
                node, "funes.rpc_server_tls:{ enabled: 1")

        self.funes.for_nodes(self.funes.nodes, start_cluster)

        # To validate that everything works fine we check that
        # formed a cluster and we can produce and consume from it
        # even after restarting.
        try:
            wait_until(lambda: len(self.rpk.cluster_info()) == 3,
                       timeout_sec=120,
                       backoff_sec=3)

            topic = "test-rpc"
            self.rpk.create_topic(topic)

            for i in range(50):
                self.rpk.produce(topic, f"k-{i}", f"v-test-{i}", timeout=5)
        except RpkException:
            pass

        self.funes.stop()
        self.funes.for_nodes(self.funes.nodes, start_cluster)
        try:
            for i in range(50, 100):
                self.rpk.produce(topic, f"k-{i}", f"v-test-{i}", timeout=5)

            out = self.rpk.consume(topic, n=100)
            assert "k-99" in out
        except RpkException:
            pass

    @cluster(num_nodes=3)
    def test_rpc_tls_list(self):
        """
        Starts funes via rpk with rpc_server_tls as a list
        and check if rpk prints the warning in the logs
        """
        def setup_and_start(node):
            self.funes.clean_node(node)
            node.account.mkdirs(
                os.path.dirname(FunesService.NODE_CONFIG_FILE))

            rpk = RpkRemoteTool(self.funes, node)
            rpk.mode_set("production")
            # We add [] so rpk picks it up as a list.
            rpk.config_set("funes.rpc_server_tls",
                           f"[{self.rpc_server_tls()}]")
            self.funes.start_node_with_rpk(node,
                                              rpk_base_args(node),
                                              clean_node=False)

        self.funes.for_nodes(self.funes.nodes, setup_and_start)

        # Check that we don't enable rpc and print a warning
        assert self.funes.search_log_all(
            "funes.rpc_server_tls:{ enabled: 0", self.funes.nodes)
        assert self.funes.search_log_all(
            "WARNING: Due to an old rpk bug, your funes.yaml's funes.rpc_server_tls property is an array",
            self.funes.nodes)

    @cluster(num_nodes=3)
    def test_rpc_tls_enable(self):
        """
        Starts funes via rpk with rpc_server_tls as a list
        (i.e no TLS) and test the process of enabling TLS and
        restarting the cluster
        """

        # First we setup the cluster with rpc_server_tls as a list.
        def setup_cluster(node):
            self.funes.clean_node(node)
            node.account.mkdirs(
                os.path.dirname(FunesService.NODE_CONFIG_FILE))

            rpk = RpkRemoteTool(self.funes, node)
            rpk.mode_set("production")
            rpk.config_set("funes.rpc_server_tls",
                           f"[{self.rpc_server_tls()}]")
            node.account.ssh("sysctl fs.inotify.max_user_instances=512")

        self.funes.for_nodes(self.funes.nodes, setup_cluster)

        seeds_str = ",".join(
            [f"{n.account.hostname}" for n in self.funes.nodes])

        def start_cluster(node):
            seeds_arg = f"--seeds={seeds_str}"
            if self.funes.idx(node) == 1:
                seeds_arg = ""
            args = f"{seeds_arg} {rpk_base_args(node)}"
            self.funes.start_node_with_rpk(node, args, clean_node=False)

        # On first start we validate that TLS is disabled and produce
        # to a topic.
        self.funes.for_nodes(self.funes.nodes, start_cluster)
        assert self.funes.search_log_all(
            "funes.rpc_server_tls:{ enabled: 0", self.funes.nodes)

        try:
            wait_until(lambda: len(self.rpk.cluster_info()) == 3,
                       timeout_sec=120,
                       backoff_sec=3)

            topic = "test-rpc"
            self.rpk.create_topic(topic)

            for i in range(50):
                self.rpk.produce(topic, f"k-{i}", f"v-test-{i}", timeout=5)
        except RpkException:
            pass

        # Now we create the certs + write the correct config.
        tls_manager = tls.TLSCertManager(self.logger)

        for node in self.funes.nodes:
            rpk = RpkRemoteTool(self.funes, node)
            rpk.config_set("funes.rpc_server_tls", self.rpc_server_tls())
            self.write_tls_cert(node, tls_manager)

        # Restart and validate rpc_server_tls is enabled.
        self.funes.stop()
        self.funes.for_nodes(self.funes.nodes, start_cluster)
        assert self.funes.search_log_all(
            "funes.rpc_server_tls:{ enabled: 1", self.funes.nodes)

        try:
            wait_until(lambda: len(self.rpk.cluster_info()) == 3,
                       timeout_sec=120,
                       backoff_sec=3)

            for i in range(50, 100):
                self.rpk.produce(topic, f"k-{i}", f"v-test-{i}", timeout=5)

            out = self.rpk.consume(topic, n=100)
            assert "k-10" in out
            assert "k-99" in out
        except RpkException:
            pass
