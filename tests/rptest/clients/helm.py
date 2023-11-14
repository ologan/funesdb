# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import subprocess


class HelmTool:
    """
    Wrapper around helm.
    """
    def __init__(self,
                 funes,
                 release='funes',
                 chart='funes/funes',
                 namespace='funes'):
        self._funes = funes
        self._release = release
        self._chart = chart
        self._namespace = namespace

    def install(self):
        cmd = [
            'helm', 'install', self._release, self._chart, '--namespace',
            self._namespace, '--create-namespace', '--set',
            'external.domain=customfunesdomain.local', '--set',
            'statefulset.initContainers.setDataDirOwnership.enabled=true',
            '--wait'
        ]
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            # log but ignore for now
            self._funes.logger.info("helm error {}: {}".format(
                e.returncode, e.output))

    def uninstall(self):
        cmd = [
            'helm', 'uninstall', self._release, '--namespace', self._namespace,
            '--wait'
        ]
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            # log but ignore for now
            self._funes.logger.info("helm error {}: {}".format(
                e.returncode, e.output))

    def upgrade_config_cluster(self, values: dict = {}, timeout: int = 300):
        """
        Changes the funes cluster config settings in 'values',
        but leaves other values alone. Default timeout for helm is 5 minutes.
        """

        cmd = [
            'helm', 'upgrade', self._release, self._chart, '--namespace',
            self._namespace, '--wait', '--reuse-values', '--timeout',
            '{}s'.format(timeout), '--set-json',
            'config.cluster={}'.format(json.dumps(values))
        ]
        try:
            subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            # log but ignore for now
            self._funes.logger.info("helm error {}: {}".format(
                e.returncode, e.output))
