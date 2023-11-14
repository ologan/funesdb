rp_test(
  UNIT_TEST
  BINARY_NAME funesproxy_json_types
  SOURCES
    iobuf.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::funesproxy_json v::utils
  LABELS funesproxy
)
