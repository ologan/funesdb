rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME io
  SOURCES
    cache_test.cc
    interval_map_test.cc
  LIBRARIES
    v::gtest_main
    absl::btree
)
