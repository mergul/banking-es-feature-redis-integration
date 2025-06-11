#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "RdKafka::rdkafka" for configuration "Debug"
set_property(TARGET RdKafka::rdkafka APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(RdKafka::rdkafka PROPERTIES
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/librdkafka.so.1"
  IMPORTED_SONAME_DEBUG "librdkafka.so.1"
  )

list(APPEND _cmake_import_check_targets RdKafka::rdkafka )
list(APPEND _cmake_import_check_files_for_RdKafka::rdkafka "${_IMPORT_PREFIX}/lib/librdkafka.so.1" )

# Import target "RdKafka::rdkafka++" for configuration "Debug"
set_property(TARGET RdKafka::rdkafka++ APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(RdKafka::rdkafka++ PROPERTIES
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/librdkafka++.so.1"
  IMPORTED_SONAME_DEBUG "librdkafka++.so.1"
  )

list(APPEND _cmake_import_check_targets RdKafka::rdkafka++ )
list(APPEND _cmake_import_check_files_for_RdKafka::rdkafka++ "${_IMPORT_PREFIX}/lib/librdkafka++.so.1" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
