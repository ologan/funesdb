# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.utils.rpk_config import read_funes_cfg
from rptest.tests.funes_test import FunesTest
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.services.funes import FunesService, RESTART_LOG_ALLOW_LIST

import yaml
import random


class RpkConfigTest(FunesTest):
    def __init__(self, ctx):
        super(RpkConfigTest, self).__init__(test_context=ctx)
        self._ctx = ctx

    @cluster(num_nodes=3)
    def test_config_set_single_number(self):
        n = random.randint(1, len(self.funes.nodes))
        node = self.funes.get_node(n)
        rpk = RpkRemoteTool(self.funes, node)
        key = 'funes.admin.port'
        value = '9641'  # The default is 9644, so we will change it

        rpk.config_set(key, value, path=FunesService.NODE_CONFIG_FILE)
        actual_config = read_funes_cfg(node)

        if f"{actual_config['funes']['admin'][0]['port']}" != value:
            self.logger.error(
                "Configs differ\n" + f"Expected: {value}\n" +
                f"Actual: {yaml.dump(actual_config['funes']['admin'][0]['port'])}"
            )
        assert f"{actual_config['funes']['admin'][0]['port']}" == value

    @cluster(num_nodes=3)
    def test_config_set_yaml(self):
        n = random.randint(1, len(self.funes.nodes))
        node = self.funes.get_node(n)
        rpk = RpkRemoteTool(self.funes, node)
        key = 'funes.seed_servers'
        value = '''                                                      
- node_id: 1
  host:
    address: 192.168.10.1
    port: 33145
- node_id: 2
  host:
    address: 192.168.10.2
    port: 33145
- node_id: 3
  host:
    address: 192.168.10.3
    port: 33145
'''

        expected = '''                                                      
- host:
    address: 192.168.10.1
    port: 33145
- host:
    address: 192.168.10.2
    port: 33145
- host:
    address: 192.168.10.3
    port: 33145
'''

        rpk.config_set(key, value, format='yaml')

        expected_config = yaml.full_load(expected)
        actual_config = read_funes_cfg(node)
        if actual_config['funes']['seed_servers'] != expected_config:
            self.logger.error(
                "Configs differ\n" +
                f"Expected: {yaml.dump(expected_config)}\n" +
                f"Actual: {yaml.dump(actual_config['funes']['seed_servers'])}"
            )
        assert actual_config['funes']['seed_servers'] == expected_config

    @cluster(num_nodes=3)
    def test_config_set_json(self):
        n = random.randint(1, len(self.funes.nodes))
        node = self.funes.get_node(n)
        rpk = RpkRemoteTool(self.funes, node)
        key = 'rpk'
        value = '{"tune_aio_events":true,"tune_cpu":true,"tune_disk_irq":true}'

        rpk.config_set(key, value, format='json')

        expected_config = yaml.full_load('''
tune_aio_events: true
tune_cpu: true
tune_disk_irq: true
''')
        actual_config = read_funes_cfg(node)

        if actual_config['rpk'] != expected_config:
            self.logger.error("Configs differ\n" +
                              f"Expected: {yaml.dump(expected_config)}\n" +
                              f"Actual: {yaml.dump(actual_config['rpk'])}")
        assert actual_config['rpk'] == expected_config

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_config_change_then_restart_node(self):
        for node in self.funes.nodes:
            rpk = RpkRemoteTool(self.funes, node)
            key = 'funes.admin.port'
            value = '9641'  # The default is 9644, so we will change it

            rpk.config_set(key, value)

            self.funes.restart_nodes(node)

    @cluster(num_nodes=1)
    def test_config_change_mode_prod(self):
        """
        Verify that after running rpk funes mode prod, the 
        configuration values of the tuners change accordingly.
        """
        node = self.funes.nodes[0]
        rpk = RpkRemoteTool(self.funes, node)
        rpk.mode_set("prod")
        expected_config = yaml.full_load('''
    tune_network: true
    tune_disk_scheduler: true
    tune_disk_nomerges: true
    tune_disk_write_cache: true
    tune_disk_irq: true
    tune_cpu: true
    tune_aio_events: true
    tune_clocksource: true
    tune_swappiness: true
    coredump_dir: /var/lib/funes/coredump
    tune_ballast_file: true
''')

        actual_config = read_funes_cfg(node)

        if actual_config['rpk'] != expected_config:
            self.logger.error("Configs differ\n" +
                              f"Expected: {yaml.dump(expected_config)}\n" +
                              f"Actual: {yaml.dump(actual_config['rpk'])}")
        assert actual_config['rpk'] == expected_config
        assert 'developer_mode' not in actual_config['funes']
