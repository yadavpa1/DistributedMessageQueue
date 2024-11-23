cmake_minimum_required(VERSION 3.10)

project(DistributedMessageQueue CXX)

include("${CMAKE_CURRENT_SOURCE_DIR}/common.cmake")

# Find absl package
find_package(absl CONFIG REQUIRED)

# Find RocksDB
find_package(RocksDB CONFIG REQUIRED)
message(STATUS "RocksDB found at: ${ROCKSDB_LIB}")

# Proto file
get_filename_component(dmq_proto "./src/main/proto/message_queue.proto" ABSOLUTE)
get_filename_component(dmq_proto_path "${dmq_proto}" PATH)

# Generated sources
set(dmq_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/message_queue.pb.cc")
set(dmq_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/message_queue.pb.h")
set(dmq_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/message_queue.grpc.pb.cc")
set(dmq_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/message_queue.grpc.pb.h")

add_custom_command(
      OUTPUT "${dmq_proto_srcs}" "${dmq_proto_hdrs}" "${dmq_grpc_srcs}" "${dmq_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${dmq_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${dmq_proto}"
      DEPENDS "${dmq_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/common")
# include_directories("${CMAKE_CURRENT_SOURCE_DIR}/message_queue_server")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/producer")
include_directories("${CMAKE_CURRENT_BINARY_DIR}/consumer")

# dmq_grpc_proto library
add_library(dmq_grpc_proto
    "${dmq_proto_srcs}"
    "${dmq_grpc_srcs}"
    "${dmq_proto_hdrs}"
    "${dmq_grpc_hdrs}")

target_link_libraries(dmq_grpc_proto
    absl::absl_log
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    -static-libstdc++ -static-libgcc)

# RocksDB wrapper library
# add_library(rocksdb_wrapper
#   message_queue_server/rocksdb_wrapper.cc)
# target_link_libraries(rocksdb_wrapper
#   RocksDB::rocksdb 
#   -static-libstdc++ -static-libgcc)

# Message queue server
# add_executable(message_queue_server
#   message_queue_server/main.cc
#   message_queue_server/message_queue_service.cc)
# target_link_libraries(message_queue_server
#   rocksdb_wrapper
#   dmq_grpc_proto
#   absl::flags_parse
#   absl::log_initialize
#   absl::log_globals
#   ${CMAKE_THREAD_LIBS_INIT}
#   -static-libstdc++ -static-libgcc)

# Consumer library
add_library(consumer 
    consumer/consumer.cc
    common/router.cc
)

target_link_libraries(consumer
    dmq_grpc_proto
    absl::flags_parse
    absl::log_initialize
    absl::log_globals
)

# Consumer group library
add_library(consumer_group SHARED 
    consumer/consumer_group.cc
)

target_link_libraries(consumer_group
    consumer
    dmq_grpc_proto
    absl::flags_parse
    absl::log_initialize
    absl::log_globals
    -static-libstdc++ -static-libgcc
)

# Consumer client executable
add_executable(consumer_client
consumer/consumer_client.cc)
target_link_libraries(consumer_client
    consumer_group
)

# Producer library
add_library(producer SHARED 
    producer/producer.cc
    common/router.cc
)
target_link_libraries(producer
    dmq_grpc_proto
    absl::flags_parse
    absl::log_initialize
    absl::log_globals
)

# Producer client executable
add_executable(producer_client
    producer/producer_client.cc
)
target_link_libraries(producer_client
    producer
)

# Set compiler flags for position-independent code for building shared libraries
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
