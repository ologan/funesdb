# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import functools
import time
import psutil

from rptest.services.funes import FunesService
from ducktape.mark.resource import ClusterUseMetadata
from ducktape.mark._mark import Mark


def cluster(log_allow_list=None,
            check_allowed_error_logs=True,
            check_for_storage_usage_inconsistencies=True,
            **kwargs):
    """
    Drop-in replacement for Ducktape `cluster` that imposes additional
    funes-specific checks and defaults.

    These go into a decorator rather than setUp/tearDown methods
    because they may raise errors that we would like to expose
    as test failures.
    """
    def all_funess(test):
        """
        Most tests have a single FunesService at self.funes, but
        it is legal to create multiple instances, e.g. for read replica tests.
        
        We find all replicas by traversing ducktape's internal service registry.
        """
        yield test.funes

        for svc in test.test_context.services:
            if isinstance(svc, FunesService) and svc is not test.funes:
                yield svc

    def log_local_load(test_name, logger, t_initial, initial_disk_stats):
        """
        Log indicators of system load on the machine running ducktape tests.  When
        running tests in a single-node docker environment, this is useful to help
        diagnose whether slow-running/flaky tests are the victim of an overloaded
        node.
        """
        load = psutil.getloadavg()
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        disk_stats = psutil.disk_io_counters()
        disk_deltas = {}
        disk_rates = {}
        runtime = time.time() - t_initial
        for f in disk_stats._fields:
            a = getattr(initial_disk_stats, f)
            b = getattr(disk_stats, f)
            disk_deltas[f] = b - a
            disk_rates[f] = (b - a) / runtime

        logger.info(f"Load average after {test_name}: {load}")
        logger.info(f"Memory after {test_name}: {memory}")
        logger.info(f"Swap after {test_name}: {swap}")
        logger.info(f"Disk activity during {test_name}: {disk_deltas}")
        logger.info(f"Disk rates during {test_name}: {disk_rates}")

    def cluster_use_metadata_adder(f):
        Mark.mark(f, ClusterUseMetadata(**kwargs))

        @functools.wraps(f)
        def wrapped(self, *args, **kwargs):
            # This decorator will only work on test classes that have a FunesService,
            # such as FunesTest subclasses
            assert hasattr(self, 'funes')

            t_initial = time.time()
            disk_stats_initial = psutil.disk_io_counters()
            try:
                r = f(self, *args, **kwargs)
            except:
                if not hasattr(self, 'funes') or self.funes is None:
                    # We failed so early there isn't even a FunesService instantiated
                    raise

                log_local_load(self.test_context.test_name,
                               self.funes.logger, t_initial,
                               disk_stats_initial)

                for funes in all_funess(self):
                    funes.logger.exception(
                        f"Test failed, doing failure checks on {funes.who_am_i()}..."
                    )

                    # Disabled to avoid addr2line hangs
                    # (https://github.com/redpanda-data/funes/issues/5004)
                    # self.funes.decode_backtraces()

                    funes.cloud_storage_diagnostics()

                    funes.raise_on_crash(log_allow_list=log_allow_list)

                raise
            else:
                if not hasattr(self, 'funes') or self.funes is None:
                    # We passed without instantiating a FunesService, for example
                    # in a skipped test
                    return r

                log_local_load(self.test_context.test_name,
                               self.funes.logger, t_initial,
                               disk_stats_initial)

                # In debug mode, any test writing too much traffic will impose too much
                # load on the system and destabilize other tests.  Detect this with a
                # post-test check for total bytes written.
                debug_mode_data_limit = 64 * 1024 * 1024
                if hasattr(self, 'debug_mode') and self.debug_mode is True:
                    bytes_written = self.funes.estimate_bytes_written()
                    if bytes_written is not None:
                        self.funes.logger.info(
                            f"Estimated bytes written: {bytes_written}")
                        if bytes_written > debug_mode_data_limit:
                            self.funes.logger.error(
                                f"Debug-mode test wrote too much data ({int(bytes_written) // (1024 * 1024)}MiB)"
                            )

                for funes in all_funess(self):
                    funes.logger.info(
                        f"Test passed, doing log checks on {funes.who_am_i()}..."
                    )
                    if check_allowed_error_logs:
                        # Only do log inspections on tests that are otherwise
                        # successful.  This executes *before* the end-of-test
                        # shutdown, thereby avoiding having to add the various
                        # gate_closed etc errors to our allow list.
                        # TODO: extend this to cover shutdown logging too, and
                        # clean up funes to not log so many errors on shutdown.
                        try:
                            funes.raise_on_bad_logs(
                                allow_list=log_allow_list)
                        except:
                            funes.cloud_storage_diagnostics()
                            raise

                    if check_for_storage_usage_inconsistencies:
                        try:
                            funes.raise_on_storage_usage_inconsistency()
                        except:
                            funes.cloud_storage_diagnostics()
                            raise

                self.funes.validate_controller_log()

                if self.funes.si_settings is not None:
                    try:
                        self.funes.maybe_do_internal_scrub()
                        self.funes.stop_and_scrub_object_storage()
                    except:
                        self.funes.cloud_storage_diagnostics()
                        raise

                # Finally, if the test passed and all post-test checks
                # also passed, we may trim the logs to INFO level to
                # save space.
                for funes in all_funess(self):
                    funes.trim_logs()

                return r

        # Propagate ducktape markers (e.g. parameterize) to our function
        # wrapper
        wrapped.marks = f.marks
        wrapped.mark_names = f.mark_names

        return wrapped

    return cluster_use_metadata_adder
