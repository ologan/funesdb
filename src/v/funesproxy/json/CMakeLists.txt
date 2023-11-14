v_cc_library(
  NAME funesproxy_json
  SRCS
    error.cc
  DEPS
    Seastar::seastar
)

add_subdirectory(test)
add_subdirectory(requests)
