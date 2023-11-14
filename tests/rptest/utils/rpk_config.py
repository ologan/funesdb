# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import tempfile
import yaml

from rptest.services.funes import FunesService


def read_rpk_cfg(node):
    with tempfile.TemporaryDirectory() as d:
        node.account.copy_from(FunesService.RPK_CONFIG_FILE, d)
        with open(os.path.join(d, 'rpk.yaml')) as f:
            return yaml.full_load(f.read())


def read_funes_cfg(node):
    with tempfile.TemporaryDirectory() as d:
        node.account.copy_from(FunesService.NODE_CONFIG_FILE, d)
        with open(os.path.join(d, 'funes.yaml')) as f:
            return yaml.full_load(f.read())
