# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import threading
from rptest.services.admin import Admin
from rptest.tests.prealloc_nodes import PreallocNodesTest

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer, FunesAdminOperation
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.funes import CHAOS_LOG_ALLOW_LIST, PREV_VERSION_LOG_ALLOW_LIST
from rptest.services.funes_installer import FunesInstaller
from rptest.tests.funes_test import FunesTest
from rptest.utils.mode_checks import cleanup_on_early_exit, skip_debug_mode
from rptest.utils.node_operations import FailureInjectorBackgroundThread, NodeOpsExecutor, generate_random_workload

from rptest.clients.offline_log_viewer import OfflineLogViewer


class AdminOperationsTest(FunesTest):
    def __init__(self, test_context, *args, **kwargs):
        self.admin_fuzz = None

        super().__init__(test_context=test_context,
                         num_brokers=3,
                         *args,
                         **kwargs)

    def tearDown(self):
        if self.admin_fuzz is not None:
            self.admin_fuzz.stop()

        return super().tearDown()

    @cluster(num_nodes=3)
    def test_admin_operations(self):

        self.admin_fuzz = AdminOperationsFuzzer(self.funes,
                                                min_replication=1,
                                                operations_interval=1)

        self.admin_fuzz.start()
        self.admin_fuzz.wait(50, 360)
        self.admin_fuzz.stop()
