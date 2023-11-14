v_cc_library(
  NAME funesproxy_parsing
  SRCS
    error.cc
  DEPS
    absl::btree
    Seastar::seastar
)

add_subdirectory(test)
