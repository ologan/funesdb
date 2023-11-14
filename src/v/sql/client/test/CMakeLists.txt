rp_test(
  UNIT_TEST
  BINARY_NAME test_sql_client_unit
  SOURCES
    partitioners.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::sql_client
  LABELS sql
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_sql_client_single_thread
  SOURCES
    fetch_session.cc
    produce_batcher.cc
    produce_partition.cc
    retry_with_mitigation.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::sql_client
  ARGS "-- -c 1"
  LABELS sql
)

rp_test(
  FIXTURE_TEST
  BINARY_NAME sql_client
  SOURCES
    consumer_group.cc
    fetch.cc
    produce.cc
    reconnect.cc
    retry.cc
    test_config_utils.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES
    v::seastar_testing_main
    v::sql_client
    v::application
    v::storage_test_utils
  LABELS sql
)
