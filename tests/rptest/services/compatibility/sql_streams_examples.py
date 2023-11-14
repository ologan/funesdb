# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import re
from .example_base import ExampleBase

# The sql-streams root directory which is made in the
# Dockerfile
TESTS_DIR = os.path.join("/opt", "sql-streams-examples")


class SQLStreams(ExampleBase):
    def __init__(self, funes, is_driver, jar_args):
        super(SQLStreams, self).__init__(funes)

        self._jar_arg = jar_args[0] if is_driver else jar_args[1]
        self._is_driver = is_driver

    # The internal condition to determine if the
    # example is successful. Returns boolean.
    def _condition(self, line):
        if self._is_driver:
            return self.driver_cond(line)
        else:
            return "Example started." in line

    # Return the command to call in the shell
    def cmd(self):
        cmd = f"java -cp {TESTS_DIR}/target/sql-streams-examples-6.2.0-standalone.jar io.confluent.examples.streams."
        cmd = cmd + self._jar_arg
        return cmd

    # Return the process name to kill
    def process_to_kill(self):
        return "java"

    def driver_cond(self, line):
        raise NotImplementedError("driver condition undefined")


class SQLStreamsWikipedia(SQLStreams):
    def __init__(self, funes, is_driver):
        schema_reg = funes.schema_reg()
        jar_args = (
            f"WikipediaFeedAvroExampleDriver {funes.brokers()} {schema_reg}",
            f"WikipediaFeedAvroLambdaExample {funes.brokers()} {schema_reg}"
        )

        super(SQLStreamsWikipedia, self).__init__(funes, is_driver,
                                                    jar_args)

        self.users_re = re.compile(
            "(erica|bob|joe|damian|tania|phil|sam|lauren|joseph)")

    def driver_cond(self, line):
        return self.users_re.search(line) is not None


class SQLStreamsTopArticles(SQLStreams):
    def __init__(self, funes, is_driver):
        schema_reg = funes.schema_reg()
        jar_args = (
            f"TopArticlesExampleDriver {funes.brokers()} {schema_reg}",
            f"TopArticlesLambdaExample {funes.brokers()} {schema_reg}")

        super(SQLStreamsTopArticles, self).__init__(funes, is_driver,
                                                      jar_args)

    def driver_cond(self, line):
        return "engineering" in line or "telco" in line or "finance" in line or "health" in line or "science" in line


class SQLStreamsSessionWindow(SQLStreams):
    def __init__(self, funes, is_driver):
        schema_reg = funes.schema_reg()
        jar_args = (
            f"SessionWindowsExampleDriver {funes.brokers()} {schema_reg}",
            f"SessionWindowsExample {funes.brokers()} {schema_reg}")

        super(SQLStreamsSessionWindow,
              self).__init__(funes, is_driver, jar_args)

    def driver_cond(self, line):
        return "sarah" in line or "bill" in line or "jo" in line


class SQLStreamsJsonToAvro(SQLStreams):
    def __init__(self, funes, is_driver):
        schema_reg = funes.schema_reg()
        jar_args = (
            f"JsonToAvroExampleDriver {funes.brokers()} {schema_reg}",
            f"JsonToAvroExample {funes.brokers()} {schema_reg}")

        super(SQLStreamsJsonToAvro, self).__init__(funes, is_driver,
                                                     jar_args)

        # The driver prints out 3 avro records
        self._count = 3

    def driver_cond(self, line):
        # sub 1 when Converted Avro Record is in the line, otherwise sub 0
        self._count -= "Converted Avro Record" in line
        return self._count <= 0


class SQLStreamsPageView(SQLStreams):
    def __init__(self, funes, is_driver):
        schema_reg = funes.schema_reg()
        jar_args = (
            f"PageViewRegionExampleDriver {funes.brokers()} {schema_reg}",
            f"PageViewRegionLambdaExample {funes.brokers()} {schema_reg}")

        super(SQLStreamsPageView, self).__init__(funes, is_driver,
                                                   jar_args)

    def driver_cond(self, line):
        return "europe@" in line or "usa@" in line or "asia@" in line or "africa@" in line


class SQLStreamsSumLambda(SQLStreams):
    def __init__(self, funes, is_driver):
        jar_args = (f"SumLambdaExampleDriver {funes.brokers()}",
                    f"SumLambdaExample {funes.brokers()}")

        super(SQLStreamsSumLambda, self).__init__(funes, is_driver,
                                                    jar_args)

    def driver_cond(self, line):
        return "Current sum of odd numbers is:" in line


class SQLStreamsAnomalyDetection(SQLStreams):
    def __init__(self, funes, is_driver):
        # 1st arg is None since this example does not have a driver
        jar_args = (None,
                    f"AnomalyDetectionLambdaExample {funes.brokers()}")

        super(SQLStreamsAnomalyDetection,
              self).__init__(funes, is_driver, jar_args)


class SQLStreamsUserRegion(SQLStreams):
    def __init__(self, funes, is_driver):
        # 1st arg is None since this example does not have a driver
        jar_args = (None, f"UserRegionLambdaExample {funes.brokers()}")

        super(SQLStreamsUserRegion, self).__init__(funes, is_driver,
                                                     jar_args)


class SQLStreamsWordCount(SQLStreams):
    def __init__(self, funes, is_driver):
        # 1st arg is None since this example does not have a driver
        jar_args = (None, f"WordCountLambdaExample {funes.brokers()}")

        super(SQLStreamsWordCount, self).__init__(funes, is_driver,
                                                    jar_args)


class SQLStreamsMapFunc(SQLStreams):
    def __init__(self, funes, is_driver):
        # 1st arg is None since this example does not have a driver
        jar_args = (None, f"MapFunctionLambdaExample {funes.brokers()}")

        super(SQLStreamsMapFunc, self).__init__(funes, is_driver,
                                                  jar_args)
