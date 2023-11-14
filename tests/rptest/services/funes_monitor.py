# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import threading

from rptest.services.funes import FunesService

from ducktape.services.background_thread import BackgroundThreadService


class FunesMonitor(BackgroundThreadService):
    def __init__(self, context, funes: FunesService):
        super(FunesMonitor, self).__init__(context, num_nodes=1)
        self.funes = funes
        self.stopping = threading.Event()
        self.interval = 2  # seconds

    def _worker(self, idx, node):
        self.stopping.clear()

        self.funes.logger.info(
            f"Started FunesMonitor on {node.account.hostname}")

        while not self.stopping.is_set():
            started_dead_nodes = []
            for n in self.funes.started_nodes():
                try:
                    self.funes.logger.info(
                        f"FunesMonitor checking {n.account.hostname}")
                    pid = self.funes.funes_pid(n)
                    if pid is None:
                        started_dead_nodes.append(n)

                except Exception as e:
                    self.funes.logger.warn(
                        f"Failed to fetch PID for node {n.account.hostname}: {e}"
                    )

            for n in started_dead_nodes:
                self.funes.remove_from_started_nodes(n)

            for n in started_dead_nodes:
                try:
                    self.funes.logger.info(
                        f"FunesMonitor restarting {n.account.hostname}")
                    self.funes.start_node(node=n,
                                             write_config=False,
                                             timeout=60)
                except Exception as e:
                    self.funes.logger.error(
                        f"FunesMonitor failed to restart {n.account.hostname}: {e}"
                    )

            time.sleep(self.interval)

        self.funes.logger.info(
            f"Stopped FunesMonitor on {node.account.hostname}")

    def stop_node(self, node):
        self.funes.logger.info(
            f"Stopping FunesMonitor on {node.account.hostname}")
        self.stopping.set()

    def clean_node(self, nodes):
        pass
