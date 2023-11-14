# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from rptest.clients.types import TopicSpec
from rptest.tests.funes_test import FunesTest
from rptest.services.cluster import cluster
from rptest.services.funes import RESTART_LOG_ALLOW_LIST
from rptest.services.funes_installer import FunesInstaller, wait_for_num_versions
from rptest.services.funes import FunesService
from ducktape.utils.util import wait_until
from typing import (Any, Optional)
from ducktape.tests.test import TestContext

from confluent_sql import (Producer, SQLException, Message)
from random import choice
from string import ascii_uppercase


def on_delivery(err: Optional[Any], msg: Message) -> None:
    if err is not None:
        raise SQLException(err)


PAYLOAD_1KB = ''.join(choice(ascii_uppercase) for i in range(1024))


class TxAbortSnapshotTest(FunesTest):
    topics = [TopicSpec()]
    """
    Checks that the abort indexes for deleted segment offsets are cleaned up.
    """
    def __init__(self, test_context: TestContext):
        # NOTE this should work with delete_retention_ms instead of log_retention_ms, but due to
        # https://github.com/redpanda-data/funes/issues/13362 this is not possible
        extra_rp_conf = {
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "log_segment_size": 1048576,
            "log_retention_ms": 1,
            "abort_index_segment_size": 2
        }
        super(TxAbortSnapshotTest, self).__init__(test_context=test_context,
                                                  num_brokers=3,
                                                  extra_rp_conf=extra_rp_conf)

    def fill_idx(self, topic: str) -> None:
        p = Producer({
            'bootstrap.servers': self.funes.brokers(),
            'transactional.id': '1',
        })
        p.init_transactions()
        for _ in range(0, 5):
            p.begin_transaction()
            p.produce(topic,
                      key="key1".encode('utf-8'),
                      value=PAYLOAD_1KB.encode('utf-8'),
                      callback=on_delivery)
            p.flush()
            p.abort_transaction()

    def fill_segment(self, topic: str) -> None:
        p = Producer({
            "bootstrap.servers": self.funes.brokers(),
            "enable.idempotence": True,
            "retries": 5
        })
        for _ in range(0, 4 * 1024):
            p.produce(topic,
                      key="key1".encode('utf-8'),
                      value=PAYLOAD_1KB.encode('utf-8'),
                      callback=on_delivery)
        p.flush()

    def find_indexes(self, topic: str) -> dict[str, str]:
        idxes = dict()
        for node in self.funes.nodes:
            idxes[node.account.hostname] = []
            cmd = f"find {FunesService.DATA_DIR}"
            out_iter = node.account.ssh_capture(cmd)
            for line in out_iter:
                m = re.match(
                    f"{FunesService.DATA_DIR}/sql/{topic}/\\d+_\\d+/(abort.idx.\\d+.\\d+)",
                    line)
                if m:
                    idxes[node.account.hostname].append(m.group(1))
        return idxes

    def find_segments(self, topic: str) -> dict[str, list[str]]:
        segments = dict()
        for node in self.funes.nodes:
            segments[node.account.hostname] = []
            cmd = f"find {FunesService.DATA_DIR}"
            out_iter = node.account.ssh_capture(cmd)
            for line in out_iter:
                m = re.match(
                    f"{FunesService.DATA_DIR}/sql/{topic}/\\d+_\\d+/(.+).log",
                    line)
                if m:
                    self.logger.info(f"{node.account.hostname} {line}")
                    segments[node.account.hostname].append(m.group(1))
        return segments

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_index_removal(self) -> None:
        self.fill_idx(self.topics[0].name)
        segments = self.find_segments(self.topics[0].name)
        for node in self.funes.nodes:
            assert len(segments[node.account.hostname]) == 1
        self.funes.restart_nodes(self.funes.nodes)
        idx = self.find_indexes(self.topics[0].name)
        for node in self.funes.nodes:
            assert len(idx[node.account.hostname]) > 0
        self.fill_segment(self.topics[0].name)

        def segments_gone() -> bool:
            current = self.find_segments(self.topics[0].name)
            for node in segments.keys():
                if node not in current:
                    continue
                if segments[node][0] in current[node]:
                    return False
            return True

        wait_until(segments_gone, timeout_sec=60, backoff_sec=1)
        self.fill_segment(self.topics[0].name)

        self.funes.restart_nodes(self.funes.nodes)

        def indices_gone():
            current_idx = self.find_indexes(self.topics[0].name)
            for node in self.funes.nodes:
                idxes = current_idx[node.account.hostname]
                if len(idxes) != 0:
                    self.logger.debug(
                        f"node: {node.account.hostname} has non empty indexes: {idxes}"
                    )
                    return False

            return True

        try:
            wait_until(indices_gone, timeout_sec=30, backoff_sec=1)
        except TimeoutError:
            all_indices = self.find_indexes(self.topics[0].name)
            self.logger.error(f"All uncleaned indexes: {all_indices}")
            raise
