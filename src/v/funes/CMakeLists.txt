set(swags
  config
  cluster_config
  raft
  security
  status
  features
  broker
  partition
  hbadger
  transaction
  cluster
  debug
  usage
  shadow_indexing
  transform)

set(swag_files)
foreach(swag ${swags})
  seastar_generate_swagger(
    TARGET ${swag}_swagger
    VAR ${swag}_swagger_files
    IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/${swag}.json
    OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/
  )
  list(APPEND swag_files ${${swag}_swagger_files})
endforeach()

# Client facing utilities
v_cc_library(
  NAME clientutil
  SRCS
    cluster_config_schema_util.cc
    ${cluster_config_swagger_files}
  DEPS
    v::config
)

v_cc_library(
  NAME application
  SRCS
    ${swag_files}
    cluster_config_schema_util.cc
    admin_server.cc
    cli_parser.cc
    application.cc
    monitor_unsafe_log_flag.cc
  DEPS
    Seastar::seastar
    v::cluster
    v::finjector
    v::sql
    v::metrics
    v::migrations
    v::funesproxy_rest
    v::funesproxy_schema_registry
    v::storage_resource_mgmt
    v::syschecks
    v::version 
  )

add_subdirectory(tests)

add_executable(funes
    main.cc)
target_link_libraries(funes PUBLIC v::application v::raft v::sql)
set_property(TARGET funes PROPERTY POSITION_INDEPENDENT_CODE ON)

if(CMAKE_BUILD_TYPE MATCHES Release)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET funes PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  endif()
endif()

install(TARGETS funes DESTINATION bin)
