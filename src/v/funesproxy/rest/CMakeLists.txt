seastar_generate_swagger(
  TARGET rest_swagger
  VAR rest_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/../api/api-doc/rest.json
  OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/../api/api-doc/
)

v_cc_library(
  NAME funesproxy_rest
  SRCS
    api.cc
    configuration.cc
    handlers.cc
    proxy.cc
    ${rest_file}
  DEPS
    v::funesproxy_common
    v::funesproxy_parsing
    v::funesproxy_json
    v::sql_client
    v::sql_protocol
    v::syschecks
    v::ssx
    v::utils
  )

add_subdirectory(test)
