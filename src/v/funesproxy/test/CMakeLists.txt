rp_test(
  UNIT_TEST
  BINARY_NAME funesproxy_unit
  SOURCES
    errors.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::funesproxy_common v::sql_protocol
  LABELS funesproxy
)

rp_test(
  UNIT_TEST
  BINARY_NAME funesproxy_single_thread
  SOURCES
    one_shot.cc
    sql_client_cache.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::rphashing v::http v::funesproxy_common v::sql_protocol
  ARGS "-- -c 1"
  LABELS funesproxy
)
