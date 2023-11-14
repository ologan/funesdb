# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import os
import time

from ducktape.cluster.remoteaccount import RemoteCommandError, RemoteAccountSSHConfig
from ducktape.cluster.windows_remoteaccount import WindowsRemoteAccount
from ducktape.errors import TimeoutError
from ducktape.mark import env, ok_to_fail, parametrize
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kerberos import KrbKdc, KrbClient, FunesKerberosNode, AuthenticationError, KRB5_CONF_PATH, render_krb5_config, ActiveDirectoryKdc
from rptest.services.funes import LoggingConfig, FunesService, SecurityConfig
from rptest.tests.sasl_reauth_test import get_sasl_metrics, REAUTH_METRIC, EXPIRATION_METRIC
from rptest.utils.rpenv import IsCIOrNotEmpty

LOG_CONFIG = LoggingConfig('info',
                           logger_levels={
                               'security': 'trace',
                               'sql': 'trace',
                               'admin_api_server': 'trace',
                           })

REALM = "EXAMPLE.COM"


class FunesKerberosTestBase(Test):
    """
    Base class for tests that use the Funes service with Kerberos
    """
    def __init__(
            self,
            test_context,
            num_nodes=5,
            sasl_mechanisms=["SCRAM", "GSSAPI"],
            keytab_file=f"{FunesService.PERSISTENT_ROOT}/funes.keytab",
            krb5_conf_path=KRB5_CONF_PATH,
            kdc=None,
            realm=REALM,
            conn_max_reauth_ms=None,
            **kwargs):
        super(FunesKerberosTestBase, self).__init__(test_context, **kwargs)

        num_brokers = num_nodes - 1
        if not kdc:
            kdc = KrbKdc(test_context, realm=realm)
            num_brokers = num_nodes - 2
        self.kdc = kdc

        security = SecurityConfig()
        security.enable_sasl = True
        security.sasl_mechanisms = sasl_mechanisms

        self.funes = FunesKerberosNode(
            test_context,
            kdc=self.kdc,
            realm=realm,
            keytab_file=keytab_file,
            krb5_conf_path=krb5_conf_path,
            num_brokers=num_brokers,
            log_config=LOG_CONFIG,
            security=security,
            extra_rp_conf={'sql_sasl_max_reauth_ms': conn_max_reauth_ms},
            **kwargs)

        self.client = KrbClient(test_context, self.kdc, self.funes)

    def setUp(self):
        self.funes.logger.info("Starting KDC")
        self.kdc.start()
        self.funes.logger.info("Starting Funes")
        self.funes.start()
        self.funes.logger.info("Starting Client")
        self.client.start()


class FunesKerberosTest(FunesKerberosTestBase):
    def __init__(self, test_context, **kwargs):
        super(FunesKerberosTest, self).__init__(test_context, **kwargs)

    @cluster(num_nodes=5)
    @parametrize(req_principal="client",
                 acl=True,
                 topics=["needs_acl", "always_visible"],
                 fail=False)
    @parametrize(req_principal="client",
                 acl=False,
                 topics=["always_visible"],
                 fail=False)
    @parametrize(req_principal="invalid", acl=False, topics={}, fail=True)
    def test_init(self, req_principal: str, acl: bool, topics: set[str],
                  fail: bool):

        self.client.add_primary(primary="client")

        username, password, mechanism = self.funes.SUPERUSER_CREDENTIALS
        super_rpk = RpkTool(self.funes,
                            username=username,
                            password=password,
                            sasl_mechanism=mechanism)

        client_user_principal = f"User:client"

        # Create a topic that's visible to "client" iff acl = True
        super_rpk.create_topic("needs_acl")
        if acl:
            super_rpk.sasl_allow_principal(client_user_principal,
                                           ["write", "read", "describe"],
                                           "topic", "needs_acl", username,
                                           password, mechanism)

        # Create a topic visible to anybody
        super_rpk.create_topic("always_visible")
        super_rpk.sasl_allow_principal("*", ["write", "read", "describe"],
                                       "topic", "always_visible", username,
                                       password, mechanism)

        expected_acls = 3 * (2 if acl else 1)
        wait_until(lambda: super_rpk.acl_list().count('\n') >= expected_acls,
                   5)

        for metadata_fn in [self.client.metadata, self.client.metadata_java]:
            try:
                wait_until(
                    lambda f=metadata_fn: self._have_expected_topics(
                        f, req_principal, 3, set(topics)),
                    timeout_sec=5,
                    backoff_sec=0.5,
                    err_msg=
                    f"Did not receive expected set of topics with {metadata_fn.__name__}"
                )
                assert not fail
            except AuthenticationError:
                assert fail
            except TimeoutError:
                assert fail

    def _have_expected_topics(self, metadata_fn, req_principal,
                              expected_broker_count, topics_set):
        metadata = metadata_fn(req_principal)
        self.funes.logger.info(
            f"{metadata_fn.__name__} (GSSAPI): {metadata}")
        assert len(metadata['brokers']) == expected_broker_count
        return {n['topic'] for n in metadata['topics']} == topics_set


class FunesKerberosLicenseTest(FunesKerberosTestBase):
    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, test_context, num_nodes=3, **kwargs):
        super(FunesKerberosLicenseTest,
              self).__init__(test_context,
                             num_nodes=num_nodes,
                             sasl_mechanisms=["SCRAM"],
                             **kwargs)
        self.funes.set_environment({
            '__FUNES_LICENSE_CHECK_INTERVAL_SEC':
            f'{self.LICENSE_CHECK_INTERVAL_SEC}'
        })

    def _has_license_nag(self):
        return self.funes.search_log_any("Enterprise feature(s).*")

    def _license_nag_is_set(self):
        return self.funes.search_log_all(
            f"Overriding default license log annoy interval to: {self.LICENSE_CHECK_INTERVAL_SEC}s"
        )

    @cluster(num_nodes=3)
    def test_license_nag(self):
        wait_until(self._license_nag_is_set,
                   timeout_sec=30,
                   err_msg="Failed to set license nag internal")

        self.logger.debug("Ensuring no license nag")
        time.sleep(self.LICENSE_CHECK_INTERVAL_SEC * 2)
        assert not self._has_license_nag()

        self.logger.debug("Setting cluster config")
        self.funes.set_cluster_config(
            {"sasl_mechanisms": ["GSSAPI", "SCRAM"]})

        self.logger.debug("Waiting for license nag")
        wait_until(self._has_license_nag,
                   timeout_sec=self.LICENSE_CHECK_INTERVAL_SEC * 2,
                   err_msg="License nag failed to appear")


class FunesKerberosRulesTesting(FunesKerberosTestBase):
    def __init__(self, test_context, **kwargs):
        super(FunesKerberosRulesTesting, self).__init__(test_context,
                                                           num_nodes=3,
                                                           **kwargs)

    @cluster(num_nodes=3)
    @parametrize(rules=["RULE:[1:$1test$0](client.*)", "DEFAULT"],
                 kerberos_principal="client",
                 rp_user=f"clienttest{REALM}",
                 expected_topics=["restricted", "always_visible"],
                 acl=[("restricted", f"clienttest{REALM}"),
                      ("always_visible", "*")])
    @parametrize(rules=[
        "RULE:[2:$1testbad$0](client.*)",
        "RULE:[1:$1testgood](client.*)s/client(.*)/$1funes/U", "DEFAULT"
    ],
                 kerberos_principal="client",
                 rp_user="TESTGOODFUNES",
                 expected_topics=["restricted", "always_visible"],
                 acl=[("restricted", "TESTGOODFUNES"),
                      ("always_visible", "*")])
    def test_kerberos_mapping_rules(self, rules: [str],
                                    kerberos_principal: str, rp_user: str,
                                    expected_topics: [str], acl: [(str, str)]):
        self.client.add_primary(primary=kerberos_principal)

        username, password, mechanism = self.funes.SUPERUSER_CREDENTIALS
        super_rpk = RpkTool(self.funes,
                            username=username,
                            password=password,
                            sasl_mechanism=mechanism)

        for topic, principal in acl:
            super_rpk.create_topic(topic)
            super_rpk.sasl_allow_principal(principal,
                                           ["write", "read", "describe"],
                                           "topic", topic, username, password,
                                           mechanism)

        rpk = RpkTool(self.funes)
        rpk.cluster_config_set("sasl_kerberos_principal_mapping",
                               json.dumps(rules))

        wait_until(lambda: self._have_expected_topics(kerberos_principal,
                                                      set(expected_topics)),
                   timeout_sec=5,
                   backoff_sec=0.5,
                   err_msg=f"Did not receive expected set of topics")

    def _have_expected_topics(self, req_principal, topics_set):
        metadata = self.client.metadata(req_principal)
        self.logger.debug(f"Metadata (GSSAPI): {metadata}")
        return {n['topic'] for n in metadata['topics']} == topics_set

    @cluster(num_nodes=3)
    @parametrize(rules=['default'], expected_error="default")
    @parametrize(rules=['RULE:[1:$1]', 'RUL'], expected_error="RUL")
    def test_invalid_kerberos_mapping_rules(self, rules: [str],
                                            expected_error: str):
        rpk = RpkTool(self.funes)
        try:
            rpk.cluster_config_set("sasl_kerberos_principal_mapping",
                                   json.dumps(rules))
            assert False
        except RpkException as e:
            self.logger.debug(f"Message: {e.stderr}")
            assert f"Invalid rule: {expected_error}" in e.stderr


class FunesKerberosConfigTest(FunesKerberosTestBase):
    KEYTAB_FILE = f"{FunesService.PERSISTENT_ROOT}/funes_not_default.keytab"
    KRB5_CONF_PATH = f"{FunesService.PERSISTENT_ROOT}/krb5_not_default.conf"

    def __init__(self, test_context, **kwargs):
        super(FunesKerberosConfigTest,
              self).__init__(test_context,
                             num_nodes=3,
                             keytab_file=self.KEYTAB_FILE,
                             krb5_conf_path=self.KRB5_CONF_PATH,
                             **kwargs)

    def setUp(self):
        super(FunesKerberosConfigTest, self).setUp()
        krb5_config = render_krb5_config(kdc_node=self.kdc.nodes[0],
                                         realm="INCORRECT.EXAMPLE")
        for node in self.funes.nodes:
            self.logger.debug(
                f"Rendering incorrect KRB5 config for {node.name} using KDC node {self.kdc.nodes[0].name}"
            )
            node.account.create_file(KRB5_CONF_PATH, krb5_config)

    @cluster(num_nodes=3)
    def test_non_default(self):
        req_principal = "client"
        self.client.add_primary(primary=req_principal)

        admin = Admin(self.funes)
        keytab = admin.get_cluster_config()['sasl_kerberos_keytab']

        def keytab_not_found():
            return self.funes.search_log_any(
                f"Key table file '{keytab}' not found")

        def log_has_default_realm(realm: str):
            return self.funes.search_log_any(f"Default realm: '{realm}'")

        def has_metadata():
            metadata = self.client.metadata(req_principal)
            self.logger.debug(f"Metadata (GSSAPI): {metadata}")
            return len(metadata['brokers']) != 0

        try:
            has_metadata()
            assert False
        except RemoteCommandError as err:
            self.logger.info(f"err: {err}")
            assert b"ccselect can't find appropriate cache for server principal funes/" in err.msg

        wait_until(keytab_not_found, 5)

        self.funes.set_cluster_config(
            {"sasl_kerberos_keytab": self.KEYTAB_FILE})

        try:
            has_metadata()
        except Exception:
            pass

        wait_until(lambda: log_has_default_realm("INCORRECT.EXAMPLE"), 5)

        self.funes.set_cluster_config(
            {"sasl_kerberos_config": self.KRB5_CONF_PATH})

        wait_until(has_metadata, 5)

        wait_until(lambda: log_has_default_realm(REALM), 5)


class FunesKerberosExternalActiveDirectoryTest(FunesKerberosTestBase):
    def __init__(self, test_context, **kwargs):
        ip = os.environ.get("ACTIVE_DIRECTORY_IP")
        realm = os.environ.get("ACTIVE_DIRECTORY_REALM")
        ssh_username = os.environ.get("ACTIVE_DIRECTORY_SSH_USER")
        ssh_password = os.environ.get("ACTIVE_DIRECTORY_SSH_PASSWORD")
        keytab_password = os.environ.get("ACTIVE_DIRECTORY_KEYTAB_PASSWORD")
        upn_user = os.environ.get("ACTIVE_DIRECTORY_UPN_USER")
        spn_user = os.environ.get("ACTIVE_DIRECTORY_SPN_USER")

        ssh_config = RemoteAccountSSHConfig(host=ip,
                                            hostname=ip,
                                            user=ssh_username,
                                            password=ssh_password)
        wra = WindowsRemoteAccount(ssh_config=ssh_config,
                                   externally_routable_ip=ip)
        kdc = ActiveDirectoryKdc(logger=test_context.logger,
                                 remote_account=wra,
                                 realm=realm,
                                 keytab_password=keytab_password,
                                 upn_user=upn_user,
                                 spn_user=spn_user)
        super(FunesKerberosExternalActiveDirectoryTest,
              self).__init__(test_context,
                             num_nodes=2,
                             kdc=kdc,
                             realm=realm,
                             **kwargs)

    def setUp(self):
        super(FunesKerberosExternalActiveDirectoryTest, self).setUp()

    @env(ACTIVE_DIRECTORY_REALM=IsCIOrNotEmpty())
    @ok_to_fail  # Not all CI builders have access to an ADDS - let's find out which ones.
    @cluster(num_nodes=2)
    def test_metadata(self):
        principal = f"client/localhost"
        self.client.add_primary(primary=principal)
        metadata = self.client.metadata(principal)
        self.logger.info(f"metadata: {metadata}")
        assert len(metadata['brokers']) == 1


class GSSAPIReauthTest(FunesKerberosTestBase):
    MAX_REAUTH_MS = 2000
    PRODUCE_DURATION_S = MAX_REAUTH_MS * 2 / 1000
    PRODUCE_INTERVAL_S = 0.1
    PRODUCE_ITER = int(PRODUCE_DURATION_S / PRODUCE_INTERVAL_S)

    EXAMPLE_TOPIC = "needs_acl"

    def __init__(self, test_context, **kwargs):
        super().__init__(test_context,
                         conn_max_reauth_ms=self.MAX_REAUTH_MS,
                         **kwargs)

    @cluster(num_nodes=5)
    def test_gssapi_reauth(self):
        req_principal = "client"
        fail = False
        self.client.add_primary(primary="client")
        username, password, mechanism = self.funes.SUPERUSER_CREDENTIALS
        super_rpk = RpkTool(self.funes,
                            username=username,
                            password=password,
                            sasl_mechanism=mechanism)

        client_user_principal = f"User:client"

        # Create a topic that's visible to "client" iff acl = True
        super_rpk.create_topic(self.EXAMPLE_TOPIC)
        super_rpk.sasl_allow_principal(client_user_principal,
                                       ["write", "read", "describe"], "topic",
                                       self.EXAMPLE_TOPIC, username, password,
                                       mechanism)

        try:
            self.client.produce(req_principal,
                                self.EXAMPLE_TOPIC,
                                num=self.PRODUCE_ITER,
                                interval_s=self.PRODUCE_INTERVAL_S)
            pass
        except AuthenticationError:
            assert fail
        except TimeoutError:
            assert fail

        metrics = get_sasl_metrics(self.funes)
        self.funes.logger.debug(f"SASL metrics: {metrics}")
        assert (EXPIRATION_METRIC in metrics.keys())
        assert (metrics[EXPIRATION_METRIC] == 0
                ), "Client should reauth before session expiry"
        assert (REAUTH_METRIC in metrics.keys())
        assert (metrics[REAUTH_METRIC]
                > 0), "Expected client reauth on some broker..."
