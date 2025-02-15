# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.utils.rpk_config import read_rpk_cfg, read_funes_cfg
from rptest.util import expect_exception
from rptest.services.cluster import cluster

from rptest.tests.funes_test import FunesTest
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.clients.rpk import RpkTool
from ducktape.cluster.remoteaccount import RemoteCommandError
from rptest.services.funes import FunesService


class RpkProfileTest(FunesTest):
    def __init__(self, ctx):
        super(RpkProfileTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.funes)

    @cluster(num_nodes=1)
    def test_e2e_profile(self):
        """
        Test an e2e flow of different operations using rpk profile:
        Create 2 -> List -> Use -> Delete -> Use deleted -> Rename
        """
        pr1 = "profile_1"
        pr2 = "profile_2"
        node = self.funes.get_node(0)
        rpk = RpkRemoteTool(self.funes, node)

        # Create profiles
        rpk.create_profile(pr1)
        rpk.create_profile(pr2)

        rpk_cfg = read_rpk_cfg(node)

        assert rpk_cfg["current_profile"] == pr2
        # rpk pushes to the top the last profile used
        assert rpk_cfg["profiles"][0]["name"] == pr2
        assert rpk_cfg["profiles"][1]["name"] == pr1

        # List profiles
        profile_list = rpk.list_profiles()
        assert len(profile_list) == 2
        assert pr1 in profile_list and pr2 in profile_list

        # Change selected profile
        rpk.use_profile(pr1)
        rpk_cfg = read_rpk_cfg(node)
        assert rpk_cfg["current_profile"] == pr1

        rpk.delete_profile(pr2)
        profile_list = rpk.list_profiles()
        assert len(profile_list) == 1

        # Now we try to use an already deleted profile
        with expect_exception(RemoteCommandError,
                              lambda e: "returned non-zero exit" in str(e)):
            rpk.use_profile(pr2)

        # Finally, we rename it
        rpk.rename_profile("new_name")
        rpk_cfg = read_rpk_cfg(node)
        assert rpk_cfg["current_profile"] == "new_name"

    @cluster(num_nodes=3)
    def test_use_profile(self):
        """
        Test that creates a profile, assign the brokers and create a 
        topic without using the --brokers flag that is used in every 
        ducktape test so far.
        """
        node = self.funes.get_node(0)
        rpk = RpkRemoteTool(self.funes, node)
        rpk.create_profile("noflag")

        rpk.set_profile("brokers=" + self.funes.brokers())
        rpk.create_topic_no_flags("no-flag-test")

        topic_list = self._rpk.list_topics()
        assert "no-flag-test" in topic_list

    @cluster(num_nodes=3)
    def test_create_profile_from_funes(self):
        """
        Create funes.yaml, use create rpk profile --from-funes
        """
        node = self.funes.get_node(0)
        rpk = RpkRemoteTool(self.funes, node)

        # We set the broker list in the funes.yaml
        rpk.config_set("rpk.sql_api.brokers", self.funes.brokers_list())

        # Then we create the profile based on the funes.yaml
        rpk.create_profile_funes("simple_test",
                                    FunesService.NODE_CONFIG_FILE)

        rpk_cfg = read_rpk_cfg(node)
        funes_cfg = read_funes_cfg(node)

        rpk_brokers = rpk_cfg["profiles"][0]["sql_api"]["brokers"]
        funes_brokers = funes_cfg["rpk"]["sql_api"]["brokers"]

        assert rpk_brokers == funes_brokers
