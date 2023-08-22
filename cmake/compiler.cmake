#
# Copyright (C) 2020 Codership Oy <info@codership.com>
#
# Common compiler and preprocessor options.
#

message(STATUS "CMAKE_SYSTEM_NAME: ${CMAKE_SYSTEM_NAME}")
message(STATUS "CMAKE_SYSTEM_PROCESSOR: ${CMAKE_SYSTEM_PROCESSOR}")

# TODO: Should this be moved into separate module?
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING
    "Build type: Debug, RelWithDebInfo, Release" FORCE)
endif()

set(CMAKE_C_STANDARD 99)
if (CMAKE_VERSION VERSION_LESS "3.1")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99")
endif()

set(CMAKE_CXX_STANDARD 11)
if (CMAKE_VERSION VERSION_LESS "3.1")
  if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND
      CMAKE_COMPILER_VERSION VERSION_LESS "4.8")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
  else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
  endif()
endif()

# Everything will be compiled with -fPIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

#
# Basic warning flags are set here. For more detailed settings for warnings,
# see maintainer_mode.cmake.
#

# C flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g -Wno-vla")
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_XOPEN_SOURCE=600")
endif()
# CXX flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Woverloaded-virtual -g")

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  # To detect STD library misuse with Debug builds.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_ASSERTIONS")
  # Enable debug sync points
  add_definitions(-DGU_DBUG_ON)
else()
  # Due to liberal use of assert() in some modules, make sure that
  # non-debug builds have -DNDEBUG enabled.
  add_definitions(-DNDEBUG)
endif()

if (GALERA_GU_DEBUG_MUTEX)
  add_definitions(-DGU_DEBUG_MUTEX)
endif()

add_definitions(-DPXC)

# GALERA_PSI_INTERFACE comes from percona-xtradb-cluster-galera/CMakeLists.txt
# WITH_PERFSCHEMA_STORAGE_ENGINE comes from server's cmake files
if (GALERA_PSI_INTERFACE AND DEFINED WITH_PERFSCHEMA_STORAGE_ENGINE)
  add_definitions(-DHAVE_PSI_INTERFACE=1)
endif()


#
# Instead using -Wno-some-flag we can use this macro to remove -Wsome-flag from compiler flags
# usage: remove_compile_flags(-Wvla -Wextra-semi)
#
MACRO (remove_compile_flags)
  FOREACH (flag ${ARGN})
    IF(CMAKE_C_FLAGS MATCHES ${flag})
      STRING(REGEX REPLACE "${flag}( |$)" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
    ENDIF(CMAKE_C_FLAGS MATCHES ${flag})
    IF(CMAKE_CXX_FLAGS MATCHES ${flag})
      STRING(REGEX REPLACE "${flag}( |$)" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    ENDIF(CMAKE_CXX_FLAGS MATCHES ${flag})
  ENDFOREACH (flag)
ENDMACRO (remove_compile_flags)


#
# Append flags CMAKE_C_FLAGS to CMAKE_CXX_FLAGS if supported by compiler
# usage: append_cflags_if_supported(-Wno-return-stack-address)
#
MACRO (append_cflags_if_supported)
  FOREACH (flag ${ARGN})
    STRING (REGEX REPLACE "-" "_" temp_flag ${flag})
    check_c_compiler_flag (${flag} HAVE_C_${temp_flag})
    IF (HAVE_C_${temp_flag})
      SET (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${flag}")
    ENDIF ()
    check_cxx_compiler_flag (${flag} HAVE_CXX_${temp_flag})
    IF (HAVE_CXX_${temp_flag})
      SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${flag}")
    ENDIF ()
  ENDFOREACH (flag)
ENDMACRO (append_cflags_if_supported)
