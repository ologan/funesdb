# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from abc import abstractmethod
from typing import Protocol, Optional, ClassVar, Any

from rptest.services.funes_installer import FunesInstaller, FunesVersion, FunesVersionLine, FunesVersionTriple
from rptest.tests.funes_test import FunesTest


class PWorkload(Protocol):
    DONE: ClassVar[int] = -1
    NOT_DONE: ClassVar[int] = 1
    """
    member variable to access FunesTest facilities
    """
    ctx: FunesTest

    def get_workload_name(self) -> str:
        return self.__class__.__name__

    def get_earliest_applicable_release(
            self) -> Optional[FunesVersionLine | FunesVersionTriple]:
        """
        returns the earliest release that this Workload can operate on.
        None -> use the oldest available release
        (X, Y) -> use the latest minor version in line vX.Y
        (X, Y, Z) -> use the release vX.Y.Z
        """
        return None

    def get_latest_applicable_release(self) -> FunesVersion:
        """
        returns the latest release that this Workload can operate on.
        FunesInstaller.HEAD -> use head version (compiled from source
        (X, Y) -> use the latest minor version in line vX.Y
        (X, Y, Z) -> use the release vX.Y.Z
        """
        return FunesInstaller.HEAD

    def begin(self) -> None:
        """
        This method is called before starting the workload. the active funes version is self->get_earliest_applicable_relase().
        use this method to set up the topic this workload will operate on, with a unique and descriptive name.
        Additionally, this method should setup an external service that will produce and consume data from the topic.
        """
        return

    def on_partial_cluster_upgrade(
            self, versions: dict[Any, FunesVersionTriple]) -> int:
        """
        This method is called while upgrading a cluster, in a mixed state where some of the nodes will have the new version and some the old one.
        versions is a dictionary of funes node->version 
        """
        return PWorkload.DONE

    @abstractmethod
    def on_cluster_upgraded(self, version: FunesVersionTriple) -> int:
        """
        This method is called to ensure that Workload is progressing on the active funes version
        use this method to check the external services and the Workload invariants on the active funes version.
        return self.DONE to signal that no further check is needed for this funes version
        return self.NOT_DONE to signal that self.progress should be called again on this funes version
        """
        raise NotImplementedError

    def end(self) -> None:
        """
        This method is called after the last call of progress, the repdanda active version is self.get_latest_applicable_release().
        use this method to tear down external services and perform cleanup.
        """
        return
