# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from concurrent.futures import ThreadPoolExecutor
from math import ceil, floor
import random
from threading import Thread

import requests
from rptest.clients.sql_cat import SQLCat
from time import sleep
from rptest.clients.default import DefaultClient
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.tests.prealloc_nodes import PreallocNodesTest

from rptest.clients.rpk import RpkTool
from rptest.tests.funes_test import FunesTest
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize
from ducktape.mark import matrix
from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.services.funes import CHAOS_LOG_ALLOW_LIST, RESTART_LOG_ALLOW_LIST, FunesService, make_funes_service
from rptest.utils.node_operations import NodeDecommissionWaiter


class NodePoolMigrationTest(PreallocNodesTest):
    """
    Basic nodes decommissioning test.
    """
    def __init__(self, test_context):
        self._topic = None

        super(NodePoolMigrationTest, self).__init__(test_context=test_context,
                                                    num_brokers=10,
                                                    node_prealloc_count=1)

    def setup(self):
        # defer starting funes to test body
        pass

    @property
    def admin(self):
        # retry on timeout and service unavailable
        return Admin(self.funes, retry_codes=[503, 504])

    def _create_topics(self, replication_factors=[1, 3]):
        """
        :return: total number of partitions in all topics
        """
        total_partitions = 0
        topics = []
        for i in range(10):
            partitions = random.randint(1, 10)
            spec = TopicSpec(
                name=f"migration-test-{i}",
                partition_count=partitions,
                replication_factor=random.choice(replication_factors))
            topics.append(spec)
            total_partitions += partitions

        for spec in topics:
            self.client().create_topic(spec)

        self._topic = random.choice(topics).name

        return total_partitions

    # after node was removed the state should be consistent on all other not removed nodes
    def _check_state_consistent(self, decommissioned_id):

        not_decommissioned = [
            n for n in self.funes.started_nodes()
            if self.funes.node_id(n) != decommissioned_id
        ]

        def _state_consistent():

            for n in not_decommissioned:
                cfg_status = self.admin.get_cluster_config_status(n)
                brokers = self.admin.get_brokers(n)
                config_ids = sorted([s['node_id'] for s in cfg_status])
                brokers_ids = sorted([b['node_id'] for b in brokers])
                self.logger.info(
                    f"brokers: {brokers_ids}, from config: {config_ids}")
                if brokers_ids != config_ids:
                    return False
                if decommissioned_id in brokers_ids:
                    return False

            return True

        wait_until(
            _state_consistent, 90, 1,
            "Error waiting for all the nodes to report consistent list of brokers in the cluster health and configuration."
        )

    def _wait_for_node_removed(self, node_id, decommissioned_ids):
        waiter = NodeDecommissionWaiter(
            self.funes,
            node_id,
            self.logger,
            progress_timeout=120,
            decommissioned_node_ids=decommissioned_ids)
        waiter.wait_for_removal()
        return True

    def _wait_for_nodes_removed(self, decommissioned_ids):

        with ThreadPoolExecutor(
                max_workers=len(decommissioned_ids)) as executor:
            result = executor.map(
                lambda id: self._wait_for_node_removed(id, decommissioned_ids),
                decommissioned_ids)

            return [r for r in result]

    def _decommission(self, node_id, decommissioned_ids=[]):
        def decommissioned():
            try:

                results = []
                for n in self.funes.nodes:
                    # do not query decommissioned nodes
                    if self.funes.node_id(n) in decommissioned_ids:
                        continue

                    brokers = self.admin.get_brokers(node=n)
                    for b in brokers:
                        if b['node_id'] == node_id:
                            results.append(
                                b['membership_status'] == 'draining')

                if all(results):
                    return True

                self.admin.decommission_broker(node_id)
            except requests.exceptions.RetryError:
                return False
            except requests.exceptions.ConnectionError:
                return False
            except requests.exceptions.HTTPError:
                return False

        wait_until(
            decommissioned,
            30,
            1,
            err_msg=
            f"Timeout waiting for node {node_id} to start decommissioning")

    @property
    def msg_size(self):
        return 64

    @property
    def msg_count(self):
        return int(20 * self.producer_throughput / self.msg_size)

    @property
    def producer_throughput(self):
        return 1024 if self.debug_mode else 1024 * 1024

    def start_producer(self):
        self.logger.info(
            f"starting kgo-verifier producer with {self.msg_count} messages of size {self.msg_size} and throughput: {self.producer_throughput} bps"
        )
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.funes,
            self._topic,
            self.msg_size,
            self.msg_count,
            custom_node=self.preallocated_nodes,
            rate_limit_bps=self.producer_throughput)

        self.producer.start(clean=False)

        wait_until(lambda: self.producer.produce_status.acked > 10,
                   timeout_sec=120,
                   backoff_sec=1)

    def start_consumer(self):
        self.consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.funes,
            self._topic,
            self.msg_size,
            readers=1,
            nodes=self.preallocated_nodes)
        self.consumer.start(clean=False)

    def verify(self):
        self.logger.info(
            f"verifying workload: topic: {self._topic}, with [rate_limit: {self.producer_throughput}, message size: {self.msg_size}, message count: {self.msg_count}]"
        )
        self.producer.wait()

        # Await the consumer that is reading only the subset of data that
        # was written before it started.
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, f"Invalid reads in topic: {self._topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"
        del self.consumer

        # Start a new consumer to read all data written
        self.start_consumer()
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, f"Invalid reads in topic: {self._topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"

    def _replicas_per_node(self):
        sqlcat = SQLCat(self.funes)
        node_replicas = {}
        md = sqlcat.metadata()
        self.funes.logger.debug(f"metadata: {md}")
        for topic in md['topics']:
            for p in topic['partitions']:
                for r in p['replicas']:
                    id = r['id']
                    if id not in node_replicas:
                        node_replicas[id] = 0
                    node_replicas[id] += 1

        return node_replicas

    @cluster(num_nodes=11, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(balancing_mode=['off', 'node_add'])
    def test_migrating_funes_nodes_to_new_pool(self, balancing_mode):
        initial_pool = self.funes.nodes[0:5]
        new_pool = self.funes.nodes[5:]
        self.funes.set_seed_servers(initial_pool)

        # start funes on initial pool of nodes
        self.funes.start(nodes=initial_pool,
                            auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)
        self.admin.patch_cluster_config(
            upsert={"partition_autobalancing_mode": balancing_mode})
        self._create_topics()

        self.start_producer()
        self.start_consumer()
        # add new nodes to the cluster
        self.funes.for_nodes(
            new_pool,
            lambda n: self.funes.start_node(n, auto_assign_node_id=True))

        def all_nodes_present():
            for n in self.funes.nodes:
                brokers = self.admin.get_brokers(node=n)
                if len(brokers) != len(initial_pool) + len(new_pool):
                    self.logger.info(
                        f"Node: {n.account.hostname}(node_id: {self.funes.node_id(n)}) contains {len(brokers)} while we expect it to have {len(initial_pool) + len(new_pool)} brokers"
                    )
                    return False
            return True

        wait_until(
            all_nodes_present,
            60,
            1,
            err_msg=
            "Not all nodes that were supposed to join the cluster are members")
        decommissioned_ids = [
            self.funes.node_id(to_decommission)
            for to_decommission in initial_pool
        ]

        for to_decommission_id in decommissioned_ids:

            self.logger.info(f"decommissioning node: {to_decommission_id}", )
            self._decommission(to_decommission_id,
                               decommissioned_ids=decommissioned_ids)

        self._wait_for_nodes_removed(decommissioned_ids)

        self.verify()

        def _all_nodes_balanced():
            r_per_node = self._replicas_per_node()
            self.logger.info(f"finished with {r_per_node} replicas per node")
            total_replicas = sum([r for r in r_per_node.values()])
            tolerance = total_replicas * 0.1
            min_expected = total_replicas / len(new_pool) - tolerance
            max_expected = total_replicas / len(new_pool) + tolerance

            return all([
                min_expected <= v <= max_expected for v in r_per_node.values()
            ])

        wait_until(_all_nodes_balanced, 60, 1,
                   f"Partitions are not balanced correctly")
