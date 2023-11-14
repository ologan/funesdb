rp_test(
  UNIT_TEST
  BINARY_NAME funesproxy_json_requests
  SOURCES
    fetch.cc
    produce.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::funesproxy_rest v::utils
  LABELS funesproxy
)
