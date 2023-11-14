v_cc_library(
  NAME funesproxy_common
  SRCS
    error.cc
    logger.cc
    probe.cc
    server.cc
    sql_client_cache.cc
  DEPS
    v::funesproxy_parsing
    v::funesproxy_json
    v::sql_client
    v::sql_protocol
    v::syschecks
    v::ssx
    v::utils
  )

add_subdirectory(test)
add_subdirectory(rest)
add_subdirectory(schema_registry)
add_subdirectory(json)
add_subdirectory(parsing)
