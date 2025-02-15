# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import random
import signal
import string
import threading
import confluent_sql as ck
import ducktape

from ducktape.mark import parametrize, matrix
from rptest.services.admin import Admin
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.tests.funes_test import FunesTest
from rptest.clients.sql_cli_tools import SQLCliTools
from rptest.clients.rpk import RpkTool, RpkException
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.util import produce_until_segments, wait_until_result, expect_exception
from rptest.services.funes import SISettings
from rptest.utils.si_utils import BucketView, NTP

TEST_TOPIC_NAME = "test-topic-1"
TEST_COMPACTED_TOPIC_NAME = "test-topic-2-compact"
TEST_DELETE_POLICY_CHANGE_TOPIC_NAME = "test-topic-3-compact-then-delete"


class DeleteRecordsTest(FunesTest, PartitionMovementMixin):
    """
    The purpose of this test is to exercise the delete-records API which
    prefix truncates the log at a user defined offset.
    """

    log_segment_size = 1048576

    topics = [
        TopicSpec(name=TEST_TOPIC_NAME,
                  partition_count=1,
                  replication_factor=3,
                  retention_bytes=-1,
                  cleanup_policy=TopicSpec.CLEANUP_DELETE),
        TopicSpec(name=TEST_COMPACTED_TOPIC_NAME,
                  partition_count=1,
                  replication_factor=3,
                  retention_bytes=-1,
                  cleanup_policy=TopicSpec.CLEANUP_COMPACT),
        TopicSpec(name=TEST_DELETE_POLICY_CHANGE_TOPIC_NAME,
                  partition_count=1,
                  replication_factor=3,
                  retention_bytes=-1,
                  cleanup_policy=TopicSpec.CLEANUP_COMPACT),
    ]

    def __init__(self, test_context):
        extra_rp_conf = dict(enable_leader_balancer=False,
                             log_compaction_interval_ms=5000,
                             log_segment_size=self.log_segment_size)
        super(DeleteRecordsTest, self).__init__(test_context=test_context,
                                                num_brokers=3,
                                                extra_rp_conf=extra_rp_conf)
        self._ctx = test_context

        self.rpk = RpkTool(self.funes)
        self.admin = Admin(self.funes)

    def setUp(self):
        # Defer cluster startup so we can tweak configs based on the type of
        # test run.
        pass

    def _start(self,
               cloud_storage_enabled,
               start_with_data=True,
               apply_retention_settings=True):
        # Tests will invoke this method to start funes with the correctly
        # applied test settings
        if cloud_storage_enabled:
            si_settings = SISettings(
                self._ctx,
                log_segment_size=self.log_segment_size,
                cloud_storage_housekeeping_interval_ms=2000,
                fast_uploads=True)
            extra_rp_conf = dict(
                log_compaction_interval_ms=2000,
                compacted_log_segment_size=self.log_segment_size)
            self.funes.set_extra_rp_conf(extra_rp_conf)
            self.funes.set_si_settings(si_settings)

        self.funes.start()
        self._create_initial_topics()

        if start_with_data is True:
            produce_until_segments(
                self.funes,
                topic=TEST_TOPIC_NAME,
                partition_idx=0,
                count=10,
                acks=-1,
            )

        local_snapshot = self.funes.storage().segments_by_node(
            "sql", TEST_TOPIC_NAME, 0)
        assert len(local_snapshot) > 0, "empty snapshot"
        self.funes.logger.info(f"Snapshot: {local_snapshot}")

        if cloud_storage_enabled and apply_retention_settings:
            self._apply_local_retention_settings()

        return local_snapshot

    def _apply_local_retention_settings(self):
        # Will evict local segments every 2 segments, the rational here is to
        # stress the TS path of delete-records where the data only exists in cloud
        # and not locally
        self.local_retention = self.log_segment_size * 2
        self.funes.logger.info(
            f"Turning on aggressive local retention, log_segment_size: {self.log_segment_size} retention.local.target.bytes: {self.local_retention}"
        )
        self.rpk.alter_topic_config(
            TEST_TOPIC_NAME, TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
            self.local_retention)

    def get_topic_info(self, topic_name):
        topics_info = list(self.rpk.describe_topic(topic_name))
        self.logger.info(topics_info)
        assert len(topics_info) == 1
        return topics_info[0]

    def wait_until_records(self,
                           topic_name,
                           offset,
                           timeout_sec=30,
                           backoff_sec=1):
        def expect_high_watermark():
            topic_info = self.get_topic_info(topic_name)
            return topic_info.high_watermark == offset

        wait_until(expect_high_watermark,
                   timeout_sec=timeout_sec,
                   backoff_sec=backoff_sec)

    def delete_records(self, topic, partition, truncate_offset):
        """
        Makes delete records call with 1 topic partition in the request body.

        Asserts that the response contains 1 element, with matching topic &
        partition contents.
        """
        response = self.rpk.trim_prefix(topic,
                                        truncate_offset,
                                        partitions=[partition])
        assert len(response) == 1
        assert response[0].topic == topic
        assert response[0].partition == partition
        assert response[0].error_msg == "", f"Err msg: {response[0].error}"
        return response[0].new_start_offset

    def retry_list_offset_request(self, fn, value_on_read):
        def check_bound():
            try:
                if fn():
                    return value_on_read
            except Exception as e:
                # Transient failure, desired to retry
                if 'unknown broker' in str(e):
                    raise e
            return not value_on_read

        return wait_until_result(
            check_bound,
            timeout_sec=10,
            backoff_sec=1,
            err_msg="Failed to make list_offsets request, unknown broker",
            retry_on_exc=True)

    def assert_start_partition_boundaries(self, topic_name, truncate_offset):
        def check_bound_start(offset, value_on_read=True):
            def attempt_consume_one():
                r = self.rpk.consume(topic_name,
                                     n=1,
                                     offset=f'{offset}-{offset+1}',
                                     quiet=True,
                                     timeout=10)
                return r.count('_') == 1

            return self.retry_list_offset_request(attempt_consume_one,
                                                  value_on_read)

        def check_bound_start_fails(offset):
            return check_bound_start(offset, value_on_read=False)

        assert check_bound_start(
            truncate_offset
        ), f"new log start: {truncate_offset} not consumable"
        assert check_bound_start_fails(
            truncate_offset -
            1), f"before log start: {truncate_offset - 1} is consumable"

    def assert_new_partition_boundaries(self, topic_name, truncate_offset,
                                        high_watermark):
        """
        Returns true if the partition contains records at the expected boundaries,
        ensuring the truncation worked at the exact requested point and that the
        number of remaining records is as expected.
        """
        def check_bound_end(offset, value_on_read=True):
            def attempt_consume_last():
                # Not timing out means data was available to read
                _ = self.rpk.consume(topic_name,
                                     n=1,
                                     offset=offset,
                                     timeout=10)
                return True

            return self.retry_list_offset_request(attempt_consume_last,
                                                  value_on_read)

        def check_bound_end_fails(offset):
            return check_bound_end(offset, value_on_read=False)

        assert truncate_offset <= high_watermark, f"Test malformed"

        if truncate_offset == high_watermark:
            # Assert no data at all can be read
            assert check_bound_end_fails(truncate_offset)
            return

        # truncate_offset is inclusive start of log
        # high_watermark is exclusive end of log
        # Readable offsets: [truncate_offset, high_watermark)
        self.assert_start_partition_boundaries(topic_name, truncate_offset)
        assert check_bound_end(
            high_watermark -
            1), f"log end: {high_watermark - 1} not consumable"
        assert check_bound_end_fails(
            high_watermark), f"high watermark: {high_watermark} is consumable"

    @cluster(num_nodes=3)
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_topic_start_delta(self, cloud_storage_enabled):
        """
        Test that delete-records moves forward the segment offset delta

        Perform this by creating only 1 segment and moving forward the start
        offset within that segment, performing verifications at each step
        """
        topic = TEST_TOPIC_NAME
        num_records = 10240
        records_size = 512
        truncate_offset_start = 100

        self._start(cloud_storage_enabled, start_with_data=False)

        # Produce some data, wait for it all to arrive
        sql_tools = SQLCliTools(self.funes)
        sql_tools.produce(topic, num_records, records_size)
        self.wait_until_records(topic,
                                num_records,
                                timeout_sec=10,
                                backoff_sec=1)

        # Call delete-records in a loop incrementing new point each time
        for truncate_offset in range(truncate_offset_start,
                                     truncate_offset_start + 5):
            # Perform truncation
            low_watermark = self.delete_records(topic, 0, truncate_offset)
            assert low_watermark == truncate_offset, f"Expected low watermark: {truncate_offset} observed: {low_watermark}"

            # Assert correctness of start and end offsets in topic metadata
            topic_info = self.get_topic_info(topic)
            assert topic_info.id == 0, f"Partition id: {topic_info.id}"
            assert topic_info.start_offset == truncate_offset, f"Start offset: {topic_info.start_offset}"
            assert topic_info.high_watermark == num_records, f"High watermark: {topic_info.high_watermark}"

            # ... and in actual fetch requests
            self.assert_new_partition_boundaries(topic, truncate_offset,
                                                 topic_info.high_watermark)

    # Disable checks for storage usage inconsistencies as orphaned log segments left
    # post node crash have been observed in this test. This is not something
    # specific to delete-records but will happen in all cases where segment deletion
    # occurs at the moment a failure is injected.
    @cluster(num_nodes=3, check_for_storage_usage_inconsistencies=False)
    @matrix(cloud_storage_enabled=[True, False],
            truncate_point=[
                "at_segment_boundary", "random_offset",
                "one_below_high_watermark", "at_high_watermark"
            ])
    def test_delete_records_segment_deletion(self, cloud_storage_enabled: bool,
                                             truncate_point: str):
        """
        Test that prefix truncation actually deletes data

        In the case the desired truncation point passes multiple segments it
        can be asserted that all of those segments will be deleted once the
        log_eviction_stm processes the deletion event
        """
        local_snapshot = self._start(cloud_storage_enabled,
                                     apply_retention_settings=False)

        def get_segment_boundaries_via_local_storage(node):
            def to_final_indicies(seg):
                if seg.data_file is not None:
                    return int(seg.data_file.split('-')[0])
                else:
                    # A segment with no data file indicates that an index or
                    # compaction index was left behind for a deleted segment, or
                    # deletion is currently in flight.
                    return -1

            boundaries = sorted([to_final_indicies(seg) for seg in node])
            boundaries = [idx for idx in boundaries if idx >= 0]
            self.funes.logger.debug(f"Local boundaries: {boundaries}")
            return boundaries

        def get_segment_boundaries_via_cloud_manifest(manifest):
            boundaries = sorted([
                int(idx.split('-')[0]) for idx in manifest['segments'].keys()
            ])
            self.funes.logger.debug(f"Cloud boundaries: {boundaries}")
            return boundaries

        # Tests for 4 different types of scenarios
        # 1. User wants to truncate all data - high_watermark
        # 2. User wants to truncate 1 before  the high watermark
        # 3. User wants to truncate at a random point
        # 4. User wants to truncate at a random segment boundary
        def obtain_test_parameters(local_snapshot):
            node = local_snapshot[list(local_snapshot.keys())[-1]]
            segment_boundaries = get_segment_boundaries_via_local_storage(node)
            truncate_offset = None
            high_watermark = int(
                self.get_topic_info(TEST_TOPIC_NAME).high_watermark)
            if truncate_point == "one_below_high_watermark":
                truncate_offset = high_watermark - 1  # Leave 1 record
            elif truncate_point == "at_high_watermark":
                truncate_offset = high_watermark  # Delete all data
            elif truncate_point == "random_offset":
                truncate_offset = random.randint(1, high_watermark)
            elif truncate_point == "at_segment_boundary":
                random_segment = random.randint(1, len(local_snapshot) - 1)
                truncate_offset = segment_boundaries[random_segment]
            else:
                assert False, "unknown test parameter encountered"

            # Cannot compare sql offsets to funes offsets returned by storage utilities,
            # so conversion must occur to compare
            response = self.admin.get_local_offsets_translated(
                [truncate_offset], TEST_TOPIC_NAME, 0, translate_to="funes")
            assert len(response) == 1 and 'rp_offset' in response[0], response
            rp_truncate_offset = response[0]['rp_offset']

            self.funes.logger.info(
                f"Truncate sql offset: {truncate_offset} truncate funes offset: {rp_truncate_offset} high watermark: {high_watermark}"
            )
            return (truncate_offset, rp_truncate_offset, high_watermark)

        (truncate_offset, rp_truncate_offset,
         high_watermark) = obtain_test_parameters(local_snapshot)

        # Apply local retention settings after the truncation offset has been translated, otherwise
        # the state within the offset translator may be truncated away
        self._apply_local_retention_settings()

        # Make delete-records call, assert response looks ok
        try:
            low_watermark = self.delete_records(TEST_TOPIC_NAME, 0,
                                                truncate_offset)
            assert low_watermark == truncate_offset, f"Expected low watermark: {truncate_offset} observed: {low_watermark}"
        except Exception as e:
            topic_info = self.get_topic_info(TEST_TOPIC_NAME)
            self.funes.logger.info(
                f"Start offset: {topic_info.start_offset}")
            raise e

        # Restart one node while the effect is propogating
        stopped_node = random.choice(self.funes.nodes)
        self.funes.signal_funes(stopped_node, signal=signal.SIGKILL)
        self.funes.start_node(stopped_node)

        # Assert start offset is correct and there aren't any off-by-one errors
        self.assert_new_partition_boundaries(TEST_TOPIC_NAME, low_watermark,
                                             high_watermark)

        def all_segments_removed(segments, lwm):
            num_below_watermark = len([seg for seg in segments if seg < lwm])
            return num_below_watermark <= 1

        try:
            self.funes.logger.info(
                f"Waiting until all segments below low watermark {low_watermark} are deleted"
            )

            def are_all_local_segments_removed():
                local_snapshot = self.funes.storage(
                    all_nodes=True).segments_by_node("sql", TEST_TOPIC_NAME,
                                                     0)
                return all([
                    all_segments_removed(
                        get_segment_boundaries_via_local_storage(node),
                        rp_truncate_offset)
                    for _, node in local_snapshot.items()
                ])

            wait_until(
                are_all_local_segments_removed,
                timeout_sec=30,
                backoff_sec=5,
                err_msg=
                f"Failed while waiting on all segments below lwm: {low_watermark} to be removed"
            )
        except ducktape.errors.TimeoutError as e:
            self.funes.logger.info(
                f"Timed out waiting for segments, ensure no orphaned segments exist nodes that didn't crash"
            )
            snapshot = self.funes.storage(all_nodes=True).segments_by_node(
                "sql", TEST_TOPIC_NAME, 0)
            for node_name, segments in snapshot.items():
                if node_name != stopped_node.name:
                    self.funes.logger.debug(
                        f"Verifying storage on node {node_name}...")
                    assert all_segments_removed(
                        get_segment_boundaries_via_local_storage(segments),
                        rp_truncate_offset)

        if cloud_storage_enabled:

            def are_all_cloud_segments_removed():
                bv = BucketView(self.funes)
                cloud_manifest = bv.get_partition_manifest(
                    NTP("sql", TEST_TOPIC_NAME, 0))
                return all_segments_removed(
                    get_segment_boundaries_via_cloud_manifest(cloud_manifest),
                    rp_truncate_offset)

            wait_until(
                are_all_cloud_segments_removed,
                timeout_sec=30,
                backoff_sec=5,
                err_msg=
                f"Failed while waiting on all cloud segments below lwm: {low_watermark} to be removed"
            )

    @cluster(num_nodes=3)
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_bounds_checking(self, cloud_storage_enabled):
        """
        Ensure that the delete-records handler returns the appropriate error
        code when a user attempts to truncate at an offset that is not within
        the boundaries of the partition.
        """
        out_of_range_prefix = "OFFSET_OUT_OF_RANGE"

        self._start(cloud_storage_enabled)

        num_records = self.get_topic_info(TEST_TOPIC_NAME).high_watermark

        def bad_truncation(truncate_offset):
            with expect_exception(RpkException,
                                  lambda e: out_of_range_prefix in str(e)):
                self.rpk.trim_prefix(TEST_TOPIC_NAME, truncate_offset, [0])

        # Try truncating past the end of the log
        bad_truncation(num_records + 1)

        # Truncate to attempt to truncate before new beginning
        truncate_offset = 125
        low_watermark = self.delete_records(TEST_TOPIC_NAME, 0,
                                            truncate_offset)
        assert low_watermark == truncate_offset

        # Try to truncate before and at the low_watermark
        bad_truncation(0)
        bad_truncation(low_watermark)

        # Try to truncate at a specific edge case where the start and end
        # are 1 offset away from eachother
        truncate_offset = num_records - 2
        for t_ofs in range(truncate_offset, num_records + 1):
            low_watermark = self.delete_records(TEST_TOPIC_NAME, 0, t_ofs)
            assert low_watermark == t_ofs

        # Assert that nothing is readable
        bad_truncation(truncate_offset)
        bad_truncation(num_records)
        bad_truncation(num_records + 1)

    @cluster(num_nodes=3)
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_empty_or_missing_topic_or_partition(
            self, cloud_storage_enabled):
        missing_topic = "foo_bar"
        out_of_range_prefix = "OFFSET_OUT_OF_RANGE"
        unknown_topic_or_partition = "UNKNOWN_TOPIC_OR_PARTITION"

        self._start(cloud_storage_enabled, start_with_data=False)

        # Assert failure condition on unknown topic
        with expect_exception(RpkException,
                              lambda e: unknown_topic_or_partition in str(e)):
            self.rpk.trim_prefix(missing_topic, 0, [0])

        # Assert failure condition on unknown partition index
        with expect_exception(RpkException,
                              lambda e: unknown_topic_or_partition in str(e)):
            missing_idx = 15
            self.rpk.trim_prefix(TEST_TOPIC_NAME, 0, [missing_idx])

        # Assert out of range occurs on an empty topic
        with expect_exception(RpkException,
                              lambda e: out_of_range_prefix in str(e)):
            self.rpk.trim_prefix(TEST_TOPIC_NAME, 0, [0])

        # Assert correct behavior on a topic with 1 record
        self.rpk.produce(TEST_TOPIC_NAME, "k", "v", partition=0)
        self.wait_until_records(TEST_TOPIC_NAME,
                                1,
                                timeout_sec=5,
                                backoff_sec=1)
        with expect_exception(RpkException,
                              lambda e: out_of_range_prefix in str(e)):
            self.rpk.trim_prefix(TEST_TOPIC_NAME, 0, [0])

        # ... truncating at high watermark 1 should delete all data
        low_watermark = self.delete_records(TEST_TOPIC_NAME, 0, 1)
        assert low_watermark == 1
        topic_info = self.get_topic_info(TEST_TOPIC_NAME)
        assert topic_info.high_watermark == 1

    @cluster(num_nodes=3)
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_compacted_topic(self, cloud_storage_enabled):
        """
        Tests that it is not allowed to delete records from a topic with cleanup.policy=compact
        """
        self._start(cloud_storage_enabled, start_with_data=False)

        with expect_exception(
                RpkException,
                lambda e:
                "Request parameters do not satisfy the configured policy" in
                str(e),
        ):
            self.rpk.trim_prefix(TEST_COMPACTED_TOPIC_NAME, 0, [0])

    @cluster(num_nodes=3)
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_topic_policy_change(self, cloud_storage_enabled):
        """
        Tests that it is allowed to delete records from a topic that was created
        initially with the `cleanup.policy=compact` and then updated to
        `cleanup.policy=compact,delete`.
        """
        num_records = 10240
        records_size = 512
        truncate_offset = 100
        topic = TEST_DELETE_POLICY_CHANGE_TOPIC_NAME

        self._start(cloud_storage_enabled, start_with_data=False)

        self.rpk.alter_topic_config(topic, TopicSpec.PROPERTY_CLEANUP_POLICY,
                                    TopicSpec.CLEANUP_COMPACT_DELETE)

        # Produce some data, wait for it all to arrive
        sql_tools = SQLCliTools(self.funes)
        sql_tools.produce(topic, num_records, records_size)
        self.wait_until_records(topic,
                                num_records,
                                timeout_sec=10,
                                backoff_sec=1)

        # Perform truncation
        low_watermark = self.delete_records(topic, 0, truncate_offset)
        assert low_watermark == truncate_offset, f"Expected low watermark: {truncate_offset} observed: {low_watermark}"

        # Assert correctness of start and end offsets in topic metadata
        topic_info = self.get_topic_info(topic)
        assert topic_info.id == 0, f"Partition id: {topic_info.id}"
        assert topic_info.start_offset == truncate_offset, f"Start offset: {topic_info.start_offset}"
        assert topic_info.high_watermark == num_records, f"High watermark: {topic_info.high_watermark}"

        # ... and in actual fetch requests
        self.assert_new_partition_boundaries(topic, truncate_offset,
                                             topic_info.high_watermark)

    @cluster(num_nodes=3)
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_with_transactions(self, cloud_storage_enabled):
        """
        Tests that the log_eviction_stm is respecting the max_collectible_offset
        """
        self._start(cloud_storage_enabled)

        payload = ''.join(
            random.choice(string.ascii_letters) for _ in range(512))
        producer = ck.Producer({
            'bootstrap.servers': self.funes.brokers(),
            'transactional.id': '0',
        })
        producer.init_transactions()

        def delete_records_within_transaction(reporter):
            try:
                high_watermark = int(
                    self.get_topic_info(TEST_TOPIC_NAME).high_watermark)
                response = self.rpk.trim_prefix(TEST_TOPIC_NAME,
                                                high_watermark, [0])
                assert len(response) == 1
                # Even though the on disk data may be late to evict, the start offset
                # should have been immediately updated
                assert response[0].new_start_offset == high_watermark
                assert response[0].error_msg == ""
            except Exception as e:
                reporter.exc = e

        # Write 20 records and leave the transaction open
        producer.begin_transaction()
        for idx in range(0, 20):
            producer.produce(TEST_TOPIC_NAME, '0', payload, 0)
            producer.flush()

        # The eviction_stm will be re-queuing the event until the max collectible
        # offset is moved ahead of the eviction offset, or until a timeout occurs
        class ThreadReporter:
            exc = None

        eviction_thread = threading.Thread(
            target=delete_records_within_transaction, args=(ThreadReporter, ))
        eviction_thread.start()

        # Committing the transaction will allow the eviction_stm to move forward
        # and process the event.
        time.sleep(1)
        producer.commit_transaction()
        eviction_thread.join()
        # Rethrow exception encountered in thread if observed
        if ThreadReporter.exc is not None:
            raise ThreadReporter.exc

    @cluster(num_nodes=5)
    @parametrize(cloud_storage_enabled=True)
    @parametrize(cloud_storage_enabled=False)
    def test_delete_records_concurrent_truncations(self,
                                                   cloud_storage_enabled):
        """
        This tests verifies that issuing DeleteRecords requests with concurrent
        producers and consumers has no effect on correctness
        """
        self._start(cloud_storage_enabled)

        # Should complete producing within 20s
        producer = KgoVerifierProducer(self._ctx,
                                       self.funes,
                                       TEST_TOPIC_NAME,
                                       msg_size=512,
                                       msg_count=20000,
                                       rate_limit_bps=500000)  # 0.5/mbs
        consumer = KgoVerifierConsumerGroupConsumer(self._ctx,
                                                    self.funes,
                                                    TEST_TOPIC_NAME,
                                                    512,
                                                    1,
                                                    max_msgs=20000)

        def issue_delete_records():
            topic_info = self.get_topic_info(TEST_TOPIC_NAME)
            start_offset = topic_info.start_offset + 1
            high_watermark = topic_info.high_watermark - 1
            if high_watermark - start_offset <= 0:
                self.funes.logger.info("Waiting on log to populate")
                return
            truncate_point = random.randint(start_offset, high_watermark)
            self.funes.logger.info(
                f"Issuing delete_records request at offset: {truncate_point}")
            response = self.rpk.trim_prefix(TEST_TOPIC_NAME, truncate_point,
                                            [0])
            assert len(response) == 1
            assert response[0].new_start_offset == truncate_point
            assert response[0].error_msg == ""
            # Cannot assert end boundaries as there is a concurrent producer
            # moving the hwm forward
            self.assert_start_partition_boundaries(TEST_TOPIC_NAME,
                                                   truncate_point)

        def issue_partition_move():
            self._dispatch_random_partition_move(TEST_TOPIC_NAME, 0)
            self._wait_for_move_in_progress(TEST_TOPIC_NAME, 0, timeout=5)

        def background_test_loop(reporter,
                                 fn,
                                 iterations=10,
                                 sleep_sec=1,
                                 allowable_retries=3):
            """
            Periodicially issue delete records requests within a loop. allowable_reties
            exists so that the test doesn't automatically fail when a leadership change occurs.
            The implementation guaratntees that the special prefix_truncation batch is
            replicated before the client is acked, it however does not guarantee that the effect
            has occured. Have the test retry on some failures to account for this.
            """
            try:
                retries = allowable_retries
                while iterations > 0:
                    try:
                        fn()
                        retries = allowable_retries
                    except Exception as e:
                        if retries == 0:
                            raise e
                    time.sleep(sleep_sec)
                    iterations -= 1
                    retries -= 1
            except Exception as e:
                reporter.exc = e

        class DeleteRecordsExceptionReporter:
            exc = None

        class PartitionMoveExceptionReporter:
            exc = None

        delete_records_thread = threading.Thread(
            target=background_test_loop,
            args=(DeleteRecordsExceptionReporter, issue_delete_records))

        partition_move_thread = threading.Thread(
            target=background_test_loop,
            args=(PartitionMoveExceptionReporter, issue_partition_move),
            kwargs={
                'iterations': 2,
                'sleep_sec': 7
            })

        # Start up producer/consumer and thread that periodically issues delete records requests
        delete_records_thread.start()
        partition_move_thread.start()
        producer.start()
        consumer.start()

        # Shut down all threads started above
        self.funes.logger.info("Joining on delete-records thread")
        delete_records_thread.join()
        self.funes.logger.info("Joining on partition move thread")
        partition_move_thread.join()
        self.funes.logger.info("Joining on producer thread")
        producer.wait()
        self.funes.logger.info("Calling consumer::stop")
        consumer.stop()
        self.funes.logger.info("Joining on consumer thread")
        consumer.wait()
        if DeleteRecordsExceptionReporter.exc is not None:
            raise DeleteRecordsExceptionReporter.exc
        if PartitionMoveExceptionReporter.exc is not None:
            raise PartitionMoveExceptionReporter.exc

        status = consumer.consumer_status
        assert status.validator.invalid_reads == 0
        assert status.validator.out_of_scope_invalid_reads == 0
