# Redirect control for internal Funes builds
if(VECTORIZED_CMAKE_DIR)
  cmake_minimum_required(VERSION 3.22)
  list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
  include(${VECTORIZED_CMAKE_DIR}/main.cmake)
  return()
endif()

cmake_minimum_required(VERSION 3.24)
project(funes LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# make "lld" be the default since a full statically linked build using "ld" will
# often fail due to being oom killed at the end of the build when many large
# targets are being linked in parallel.
include(CheckLinkerFlag)
set(Funes_LINKER "lld" CACHE STRING "Linker to use")
set(Funes_LINKER_FLAGS "-fuse-ld=${Funes_LINKER}")
check_linker_flag(CXX ${Funes_LINKER_FLAGS} HAVE_Funes_LINKER_FLAGS)
if(NOT HAVE_Funes_LINKER_FLAGS)
  message(FATAL_ERROR "Linker ${Funes_LINKER} not found or not supported")
endif()
add_link_options(${Funes_LINKER_FLAGS})

# enable sanitizers. will propogate to all targets, including dependencies
option(Funes_ENABLE_SANITIZERS "Enable sanitizers (address, leak, undefined)" OFF)
if(Funes_ENABLE_SANITIZERS)
  add_link_options(-fsanitize=address,leak,undefined)
  add_compile_options(-fsanitize=address,leak,undefined)
endif()

include(dependencies)
include(v_library)
include(testing)

option(Funes_ENABLE_COVERAGE "Enable coverage" OFF)
if(Funes_ENABLE_COVERAGE)
  add_compile_options(-O0 -g --coverage)
  add_link_options(--coverage)
endif()

add_subdirectory(src)
