# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster

from rptest.tests.funes_test import FunesTest


class FunesStartupTest(FunesTest):
    """
    Tests that Funes starts within 10 seconds
    """
    def __init__(self, test_context):
        super(FunesStartupTest, self).__init__(test_context=test_context,
                                                  node_ready_timeout_s=10)

    @cluster(num_nodes=3)
    def test_startup(self):
        pass
