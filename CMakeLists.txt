cmake_minimum_required(VERSION 3.8)

project(MessageQueue C CXX)

# Include common CMake settings
include("${CMAKE_CURRENT_SOURCE_DIR}/common.cmake")

# abseil-cpp (Google's common libraries) is required
find_package(absl CONFIG REQUIRED)

# Proto file
get_filename_component(dmq_proto "proto/message_queue.proto" ABSOLUTE)
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
include_directories("${CMAKE_CURRENT_BINARY_DIR}/consumer")

# dmq_grpc_proto
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

# Set compiler flags for position-independent code for building shared libraries
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

# Create shared library for the consumer
add_library(consumer SHARED 
    consumer/consumer.cc
)

target_link_libraries(consumer
    dmq_grpc_proto
    absl::flags_parse,
    absl::log_initialize,
    absl::log_globals,
    -static-libstdc++ -static-libgcc)

