set(SQL_REQUEST_GENERATOR "${CMAKE_BINARY_DIR}/src/go/kreq-gen/sql-request-generator")

rp_test(
  UNIT_TEST
  BINARY_NAME
    test_sql_protocol_unit
  SOURCES
    security_test.cc
    protocol_test.cc
  DEFINITIONS
    BOOST_TEST_DYN_LINK
  LIBRARIES
  Boost::unit_test_framework
    v::sql
    v::storage_test_utils
  LABELS
    sql
    sql_protocol
  BUILD_DEPENDENCIES
    sql_request_generator
  ENV
    "GENERATOR_BIN=${SQL_REQUEST_GENERATOR}"
)

rp_test(
  UNIT_TEST
  BINARY_NAME
    test_sql_protocol_single_thread
  SOURCES
    field_parser_test.cc
    batch_reader_test.cc
  DEFINITIONS
    BOOST_TEST_DYN_LINK
  LIBRARIES
    v::seastar_testing_main
    v::sql
    v::storage_test_utils
  ARGS "-- -c 1"
  LABELS
    sql
    sql_protocol
)
