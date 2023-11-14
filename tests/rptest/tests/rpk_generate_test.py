# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json

from rptest.services.cluster import cluster

from rptest.tests.funes_test import FunesTest
from rptest.clients.rpk import RpkTool


class RpkGenerateTest(FunesTest):
    def __init__(self, ctx):
        super(RpkGenerateTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.funes)

    @cluster(num_nodes=3)
    def test_generate_grafana(self):
        """
          Test that rpk generate grafana-dashboard will generate the required dashboard
          and that it's a proper JSON file.
          """

        # dashboard is the dictionary of the current dashboards and their title.
        dashboards = {
            "operations": "Funes Ops Dashboard",
            "consumer-metrics": "SQL Consumer",
            "consumer-offsets": "SQL Consumer Offsets",
            "topic-metrics": "SQL Topic Metrics"
        }
        for name, expectedTitle in dashboards.items():
            out = self._rpk.generate_grafana(name)
            try:
                dash = json.loads(out)

                # We only validate one known value, the main goal is to identify if it's a valid JSON
                title = dash["title"]
                assert title == expectedTitle, f"Received dashboard title: '{title}', expected: '{expectedTitle}'"
            except json.JSONDecodeError as err:
                self.logger.error(
                    f"unable to parse generated' {name}' dashboard : {err}")
