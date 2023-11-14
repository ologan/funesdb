# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import sys
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.cluster.remoteaccount import RemoteCommandError
from threading import Event


class LibrdsqlTestcase(BackgroundThreadService):
    TEST_DIR = "/opt/librdsql/tests"
    ROOT = "/mnt/librdsql_tests"
    CONF_FILE = os.path.join(ROOT, "test.conf")
    LOG_FILE = os.path.join(ROOT, "rdsql_test.log")
    SQL_VERSION = "3.0.0"
    SQL_PATH = f"/opt/sql-{SQL_VERSION}"

    logs = {
        "test_case_output": {
            "path": LOG_FILE,
            "collect_default": False
        },
    }

    def __init__(self, context, funes, test_case_number):
        super(LibrdsqlTestcase, self).__init__(context, num_nodes=1)
        self.funes = funes

        self.test_case_number = test_case_number
        self.error = None

    def _worker(self, _idx, node):
        node.account.ssh("mkdir -p %s" % LibrdsqlTestcase.ROOT,
                         allow_fail=False)
        # configure test cases
        node.account.create_file(
            LibrdsqlTestcase.CONF_FILE,
            f"bootstrap.servers={self.funes.brokers()}")
        # compile test case
        self.logger.info(f"running librdsql test {self.test_case_number}")

        # environment to run the tests with
        env = {
            "TEST_SQL_VERSION": LibrdsqlTestcase.SQL_VERSION,
            "TESTS": f"{self.test_case_number:04}",
            "RDSQL_TEST_CONF": LibrdsqlTestcase.CONF_FILE,
            "SQL_VERSION": LibrdsqlTestcase.SQL_VERSION,
            "SQL_PATH": LibrdsqlTestcase.SQL_PATH,
            "BROKERS": self.funes.brokers(),
        }
        env_str = " ".join([f"{k}={v}" for k, v in env.items()])
        try:
            node.account.ssh(
                f"{env_str} {LibrdsqlTestcase.TEST_DIR}/test-runner > {LibrdsqlTestcase.LOG_FILE} 2>&1"
            )
        except RemoteCommandError as error:
            self.logger.info(
                f"Librdsql test case: {self.test_case_number} failed with error: {error}"
            )
            self.error = error

    def stop_node(self, node):
        pass

    def clean_node(self, node):
        node.account.ssh("rm -rf " + self.ROOT, allow_fail=False)
