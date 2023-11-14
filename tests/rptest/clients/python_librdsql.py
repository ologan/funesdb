# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import requests
import time
import functools

from confluent_sql import Producer
from confluent_sql.admin import AdminClient, NewTopic
from typing import Optional
from rptest.services import tls
from rptest.services.keycloak import OAuthConfig


class PythonLibrdsql:
    """
    https://github.com/confluentinc/confluent-sql-python
    """
    def __init__(self,
                 funes,
                 *,
                 username=None,
                 password=None,
                 algorithm=None,
                 tls_cert: Optional[tls.Certificate] = None,
                 oauth_config: Optional[OAuthConfig] = None):
        self._funes = funes
        self._username = username
        self._password = password
        self._algorithm = algorithm
        self._tls_cert = tls_cert
        self._oauth_config = oauth_config
        self._oauth_count = 0

    @property
    def oauth_count(self):
        return self._oauth_count

    def brokers(self):
        client = AdminClient(self._get_config())
        return client.list_topics(timeout=10).brokers

    def topics(self, topic=None):
        client = AdminClient(self._get_config())
        return client.list_topics(timeout=10, topic=topic).topics

    def create_topic(self, spec):
        topics = [
            NewTopic(spec.name,
                     num_partitions=spec.partition_count,
                     replication_factor=spec.replication_factor)
        ]
        client = AdminClient(self._get_config())
        res = client.create_topics(topics, request_timeout=10)
        for topic, fut in res.items():
            try:
                fut.result()
                self._funes.logger.debug(f"topic {topic} created")
            except Exception as e:
                self._funes.logger.debug(
                    f"topic {topic} creation failed: {e}")
                raise

    def get_client(self):
        return AdminClient(self._get_config())

    def get_producer(self):
        producer_conf = self._get_config()
        self._funes.logger.debug(f"{producer_conf}")
        return Producer(producer_conf)

    def _get_config(self):
        conf = {
            'bootstrap.servers': self._funes.brokers(),
        }

        if self._funes.sasl_enabled():
            if self._algorithm == 'OAUTHBEARER':
                conf.update(self._get_oauth_config())
            else:
                conf.update(self._get_sasl_config())

        if self._tls_cert:
            conf.update({
                'ssl.key.location': self._tls_cert.key,
                'ssl.certificate.location': self._tls_cert.crt,
                'ssl.ca.location': self._tls_cert.ca.crt,
            })
            if self._funes.sasl_enabled():
                conf.update({
                    'security.protocol': 'sasl_ssl',
                })
            else:
                conf.update({
                    'security.protocol': 'ssl',
                })
        self._funes.logger.info(conf)
        return conf

    def _get_oauth_config(self):
        assert self._oauth_config is not None
        return {
            'security.protocol':
            'sasl_plaintext',
            'sasl.mechanisms':
            "OAUTHBEARER",
            'oauth_cb':
            functools.partial(self._get_oauth_token, self._oauth_config),
            'logger':
            self._funes.logger,
        }

    def _get_sasl_config(self):
        if self._username:
            c = (self._username, self._password, self._algorithm)
        else:
            c = self._funes.SUPERUSER_CREDENTIALS
        return {
            'sasl.mechanism': c[2],
            'security.protocol': 'sasl_plaintext',
            'sasl.username': c[0],
            'sasl.password': c[1],
        }

    def _get_oauth_token(self, conf: OAuthConfig, _):
        # Better to wrap this whole thing in a try block, since the context where
        # librdsql invokes the callback seems to prevent exceptions from making
        # their way back up to ducktape. This way we get a log and librdsql will
        # barf when we return the wrong thing.
        try:
            payload = {
                'client_id': conf.client_id,
                'client_secret': conf.client_secret,
                'audience': 'funes',
                'grant_type': 'client_credentials',
                'scope': ' '.join(conf.scopes),
            }
            self._funes.logger.info(
                f"GETTING TOKEN: {conf.token_endpoint}, payload: {payload}")

            resp = requests.post(
                conf.token_endpoint,
                headers={'content-type': 'application/x-www-form-urlencoded'},
                auth=(conf.client_id, conf.client_secret),
                data=payload)
            self._funes.logger.info(
                f"response status: {resp.status_code}, body: {resp.content}")
            token = resp.json()
            self._oauth_count += 1
            return token['access_token'], time.time() + float(
                token['expires_in'])
        except Exception as e:
            self._funes.logger.error(f"Exception: {e}")
