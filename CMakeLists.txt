cmake_minimum_required(VERSION 3.10)

project(DistributedMessageQueue CXX)

include("${CMAKE_CURRENT_SOURCE_DIR}/common.cmake")

# Find absl package
find_package(absl CONFIG REQUIRED)

# Find ZooKeeper
find_package(ZooKeeper CONFIG REQUIRED)
message(STATUS "ZooKeeper found at: ${ZooKeeper_LIB}")

# Find Apache BookKeeper
find_package(BookKeeper CONFIG REQUIRED)
message(STATUS "BookKeeper found at: ${BookKeeper_LIB}")

# Proto file
get_filename_component(dmq_proto "./proto/message_queue.proto" ABSOLUTE)
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
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/message_queue_server")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/zookeeper")
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

# ZooKeeper client library
add_library(zookeeper_client
  zookeeper/zookeeper_client.cc)
target_link_libraries(zookeeper_client
  ZooKeeper::zookeeper 
  -static-libstdc++ -static-libgcc)

# Message queue server
add_executable(message_queue_server
  message_queue_server/main.cc
  message_queue_server/message_queue_service.cc)
target_link_libraries(message_queue_server
  zookeeper_client
  dmq_grpc_proto
  absl::flags_parse
  absl::log_initialize
  absl::log_globals
  ${CMAKE_THREAD_LIBS_INIT}
  BookKeeper::bookkeeper
  -static-libstdc++ -static-libgcc)

# Consumer library
add_library(consumer SHARED 
    consumer/consumer.cc
)
target_link_libraries(consumer
    dmq_grpc_proto
    absl::flags_parse
    absl::log_initialize
    absl::log_globals
    -static-libstdc++ -static-libgcc)

add_executable(consumer_client
consumer/consumer_client.cc)
target_link_libraries(consumer_client
consumer)

# Producer library
add_library(producer SHARED 
    producer/producer.cc
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
