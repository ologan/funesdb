# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.services.compatibility.example_runner import ExampleRunner
import rptest.services.compatibility.sql_streams_examples as SQLStreamExamples
from rptest.services.kaf_producer import KafProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.funes_test import FunesTest
from rptest.clients.types import TopicSpec
from rptest.services.funes import FunesproxyConfig, SchemaRegistryConfig


class SQLStreamsTest(FunesTest):
    """
    Base class for SQLStreams tests that contains all
    shared objects between tests
    """

    # The example is the java program that uses SQL Streams.
    # The java program is represented by a wrapper in SQLStreamExamples.
    Example = None

    def __init__(self, test_context, funesproxy_config: FunesproxyConfig,
                 schema_registry_config: SchemaRegistryConfig):
        super(SQLStreamsTest,
              self).__init__(test_context=test_context,
                             funesproxy_config=funesproxy_config,
                             schema_registry_config=schema_registry_config)

        self._ctx = test_context

        # 300s timeout because sometimes the examples
        # take 4min+ to produce correct output
        self._timeout = 300

    def create_example(self):
        # This will raise TypeError if Example is undefined
        example_helper = self.Example(self.funes, False)
        example = ExampleRunner(self._ctx,
                                example_helper,
                                timeout_sec=self._timeout)

        return example


class SQLStreamsDriverBase(SQLStreamsTest):
    """
    Base class for SQLStreams tests that explicitly use a
    driver program to generate data.
    """

    # The driver is also a java program that uses SQL Streams.
    # The java program is also represented by the example's corresponding
    # wrapper in SQLStreamExamples.
    Driver = None

    def __init__(self, test_context, funesproxy_config: FunesproxyConfig,
                 schema_registry_config: SchemaRegistryConfig):
        super(SQLStreamsDriverBase,
              self).__init__(test_context=test_context,
                             funesproxy_config=funesproxy_config,
                             schema_registry_config=schema_registry_config)

    @cluster(num_nodes=5)
    def test_sql_streams(self):
        example = self.create_example()

        # This will raise TypeError if DriverHeler is undefined
        driver_helper = self.Driver(self.funes, True)
        driver = ExampleRunner(self._ctx,
                               driver_helper,
                               timeout_sec=self._timeout)

        # Start the example
        example.start()
        wait_until(example.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)

        # Start the driver
        driver.start()
        wait_until(driver.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)


class SQLStreamsProdConsBase(SQLStreamsTest):
    """
    Base class for SQLStreams tests that use a producer
    and consumer
    """

    # The producer should be an extension to KafProducer
    PRODUCER = None

    def __init__(self, test_context, funesproxy_config: FunesproxyConfig,
                 schema_registry_config: SchemaRegistryConfig):
        super(SQLStreamsProdConsBase,
              self).__init__(test_context=test_context,
                             funesproxy_config=funesproxy_config,
                             schema_registry_config=schema_registry_config)

    def is_valid_msg(self, msg):
        raise NotImplementedError("is_valid_msg() undefined.")

    @cluster(num_nodes=6)
    def test_sql_streams(self):
        example = self.create_example()

        # This will raise TypeError if PRODUCER is undefined
        producer = self.PRODUCER(self._ctx, self.funes, self.topics[0].name)
        consumer = RpkConsumer(self._ctx, self.funes, self.topics[1].name)

        # Start the example
        example.start()

        # Produce some data
        producer.start()
        producer.wait()

        # Consume the data
        consumer.start()

        def try_cons():
            i = 0
            msgs = consumer.messages
            while i < len(msgs) and not self.is_valid_msg(msgs[i]):
                i += 1

            return i < len(msgs)

        wait_until(
            try_cons,
            timeout_sec=self._timeout,
            backoff_sec=5,
            err_msg=f"sql-streams {self._ctx.cls_name} consumer failed")

        consumer.stop()
        producer.stop()


class SQLStreamsTopArticles(SQLStreamsDriverBase):
    """
    Test SQLStreams TopArticles which counts the top N articles
    from a stream of page views
    """
    topics = (
        TopicSpec(name="PageViews"),
        TopicSpec(name="TopNewsPerIndustry"),
    )

    Example = SQLStreamExamples.SQLStreamsTopArticles
    Driver = SQLStreamExamples.SQLStreamsTopArticles

    def __init__(self, test_context):
        super(SQLStreamsTopArticles,
              self).__init__(test_context=test_context,
                             funesproxy_config=FunesproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())


class SQLStreamsSessionWindow(SQLStreamsDriverBase):
    """
    Test SQLStreams SessionWindow counts user activity within
    a specific interval of time
    """
    topics = (
        TopicSpec(name="play-events"),
        TopicSpec(name="play-events-per-session"),
    )

    Example = SQLStreamExamples.SQLStreamsSessionWindow
    Driver = SQLStreamExamples.SQLStreamsSessionWindow

    def __init__(self, test_context):
        super(SQLStreamsSessionWindow,
              self).__init__(test_context=test_context,
                             funesproxy_config=FunesproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())


class SQLStreamsJsonToAvro(SQLStreamsDriverBase):
    """
    Test SQLStreams JsontoAvro example which converts records from JSON to AVRO
    using the schema registry.
    """
    topics = (
        TopicSpec(name="json-source"),
        TopicSpec(name="avro-sink"),
    )

    Example = SQLStreamExamples.SQLStreamsJsonToAvro
    Driver = SQLStreamExamples.SQLStreamsJsonToAvro

    def __init__(self, test_context):
        super(SQLStreamsJsonToAvro,
              self).__init__(test_context=test_context,
                             funesproxy_config=FunesproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())


class SQLStreamsPageView(FunesTest):
    """
    Test SQLStreams PageView example which performs a join between a 
    KStream and a KTable
    """
    topics = (
        TopicSpec(name="PageViews"),
        TopicSpec(name="UserProfiles"),
        TopicSpec(name="PageViewsByRegion"),
    )

    def __init__(self, test_context):
        super(SQLStreamsPageView,
              self).__init__(test_context=test_context,
                             funesproxy_config=FunesproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

        self._timeout = 300

    @cluster(num_nodes=5)
    def test_sql_streams_page_view(self):
        example_jar = SQLStreamExamples.SQLStreamsPageView(self.funes,
                                                               is_driver=False)
        example = ExampleRunner(self.test_context,
                                example_jar,
                                timeout_sec=self._timeout)

        driver_jar = SQLStreamExamples.SQLStreamsPageView(self.funes,
                                                              is_driver=True)
        driver = ExampleRunner(self.test_context,
                               driver_jar,
                               timeout_sec=self._timeout)

        # Start the example
        example.start()
        wait_until(example.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)

        # Start the driver
        driver.start()
        wait_until(driver.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)


class SQLStreamsWikipedia(FunesTest):
    """
    Test SQLStreams Wikipedia example which computes, for every minute the
    number of new user feeds from the Wikipedia feed irc stream.
    """
    topics = (
        TopicSpec(name="WikipediaFeed"),
        TopicSpec(name="WikipediaStats"),
    )

    def __init__(self, test_context):
        super(SQLStreamsWikipedia,
              self).__init__(test_context=test_context,
                             funesproxy_config=FunesproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

        self._timeout = 300

    @cluster(num_nodes=5)
    def test_sql_streams_wikipedia(self):
        example_jar = SQLStreamExamples.SQLStreamsWikipedia(
            self.funes, is_driver=False)
        example = ExampleRunner(self.test_context,
                                example_jar,
                                timeout_sec=self._timeout)

        driver_jar = SQLStreamExamples.SQLStreamsWikipedia(self.funes,
                                                               is_driver=True)
        driver = ExampleRunner(self.test_context,
                               driver_jar,
                               timeout_sec=self._timeout)

        # Start the example
        example.start()
        wait_until(example.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)

        # Start the driver
        driver.start()
        wait_until(driver.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)


class SQLStreamsSumLambda(SQLStreamsDriverBase):
    """
    Test SQLStreams SumLambda example that sums odd numbers
    using reduce
    """
    topics = (
        TopicSpec(name="numbers-topic"),
        TopicSpec(name="sum-of-odd-numbers-topic"),
    )

    Example = SQLStreamExamples.SQLStreamsSumLambda
    Driver = SQLStreamExamples.SQLStreamsSumLambda

    def __init__(self, test_context):
        super(SQLStreamsSumLambda,
              self).__init__(test_context=test_context,
                             funesproxy_config=FunesproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())


class AnomalyProducer(KafProducer):
    def __init__(self, context, funes, topic):
        super(AnomalyProducer, self).__init__(context,
                                              funes,
                                              topic,
                                              num_records=10)

    def value_gen(self):
        return "record-$((1 + $RANDOM % 3))"


class SQLStreamsAnomalyDetection(SQLStreamsProdConsBase):
    """
    Test SQLStreams SessionWindow example that counts the user clicks
    within a 1min window
    """
    topics = (
        TopicSpec(name="UserClicks"),
        TopicSpec(name="AnomalousUsers"),
    )

    Example = SQLStreamExamples.SQLStreamsAnomalyDetection
    PRODUCER = AnomalyProducer

    def __init__(self, test_context):
        super(SQLStreamsAnomalyDetection,
              self).__init__(test_context=test_context,
                             funesproxy_config=FunesproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

    def is_valid_msg(self, msg):
        key = msg["key"]
        return "record-1" in key or "record-2" in key or "record-3" in key


class RegionProducer(KafProducer):
    def __init__(self, context, funes, topic):
        super(RegionProducer, self).__init__(context,
                                             funes,
                                             topic,
                                             num_records=10)

    def value_gen(self):
        return "region-$((1 + $RANDOM % 3))"


class SQLStreamsUserRegion(SQLStreamsProdConsBase):
    """
    Test SQLStreams UserRegion example the demonstrates group-by ops and
    aggregations on a KTable
    """
    topics = (
        TopicSpec(name="UserRegions"),
        TopicSpec(name="LargeRegions"),
    )

    Example = SQLStreamExamples.SQLStreamsUserRegion
    PRODUCER = RegionProducer

    def __init__(self, test_context):
        super(SQLStreamsUserRegion,
              self).__init__(test_context=test_context,
                             funesproxy_config=FunesproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

    def is_valid_msg(self, msg):
        key = msg["key"]
        return "region-1" in key or "region-2" in key or "region-3" in key


class WordProducer(KafProducer):
    def __init__(self, context, funes, topic):
        super(WordProducer, self).__init__(context,
                                           funes,
                                           topic,
                                           num_records=10)

    def value_gen(self):
        return "\"funes is fast\""


class SQLStreamsWordCount(SQLStreamsProdConsBase):
    """
    Test SQLStreams WordCount example which does simple prod-cons with
    KStreams and computes a historgram for word occurence
    """

    topics = (
        TopicSpec(name="streams-plaintext-input"),
        TopicSpec(name="streams-wordcount-output"),
    )

    Example = SQLStreamExamples.SQLStreamsWordCount
    PRODUCER = WordProducer

    def __init__(self, test_context):
        super(SQLStreamsWordCount,
              self).__init__(test_context=test_context,
                             funesproxy_config=FunesproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

    def is_valid_msg(self, msg):
        key = msg["key"]
        return "funes" in key


class SQLStreamsMapFunction(SQLStreamsProdConsBase):
    """
    Test SQLStreams MapFunction example which does .upper() as a state-less
    transform on records
    """

    topics = (
        TopicSpec(name="TextLinesTopic"),
        TopicSpec(name="UppercasedTextLinesTopic"),
        TopicSpec(name="OriginalAndUppercasedTopic"),
    )

    Example = SQLStreamExamples.SQLStreamsMapFunc
    PRODUCER = WordProducer

    def __init__(self, test_context):
        super(SQLStreamsMapFunction,
              self).__init__(test_context=test_context,
                             funesproxy_config=FunesproxyConfig(),
                             schema_registry_config=SchemaRegistryConfig())

    def is_valid_msg(self, msg):
        value = msg["value"]
        return "FUNES IS FAST" in value
