# Copyright 2015 gRPC authors.
# Licensed under the Apache License, Version 2.0.

# Workaround for protobuf not listing all absl dependencies
PROTOBUF_ABSL_DEPS = absl_absl_check absl_absl_log absl_algorithm absl_base absl_bind_front absl_bits absl_btree absl_cleanup absl_cord absl_core_headers absl_debugging absl_die_if_null absl_flags absl_flat_hash_map absl_flat_hash_set absl_function_ref absl_hash absl_layout absl_log_initialize absl_log_severity absl_memory absl_node_hash_map absl_node_hash_set absl_optional absl_span absl_status absl_statusor absl_strings absl_synchronization absl_time absl_type_traits absl_utility absl_variant

# Workaround for utf8_range missing pkgconfig file
PROTOBUF_UTF8_RANGE_LINK_LIBS = -lutf8_validity

# ✅ YOUR CUSTOM PATH
export PKG_CONFIG_PATH = /home/csce438/.local/lib/pkgconfig:/usr/local/lib/pkgconfig:/home/csce438/grpc/third_party/re2:/home/csce438/.local/share/pkgconfig

HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)

CXX = g++
CPPFLAGS += -I/usr/local/include \
            -I/home/csce438/.local/include \
            -I/home/csce438/grpc/include \
            -Wnon-virtual-dtor -DNOMINMAX

CXXFLAGS += -std=c++17

ifeq ($(SYSTEM),Darwin)
LDFLAGS += -L/usr/local/lib -L/home/csce438/.local/lib `pkg-config --libs --static protobuf grpc++ absl_flags absl_flags_parse $(PROTOBUF_ABSL_DEPS)` \
           $(PROTOBUF_UTF8_RANGE_LINK_LIBS) \
           -pthread -lgrpc++_reflection -lrabbitmq -ljsoncpp
else
LDFLAGS += -L/usr/local/lib -L/home/csce438/.local/lib `pkg-config --libs --static protobuf grpc++ absl_flags absl_flags_parse $(PROTOBUF_ABSL_DEPS)` \
           $(PROTOBUF_UTF8_RANGE_LINK_LIBS) \
           -pthread -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed \
           -ldl -lglog -lrabbitmq -ljsoncpp
endif

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTOS_PATH = .

vpath %.proto $(PROTOS_PATH)

all: system-check tsc tsd cord sync

tsc: coordinator.pb.o coordinator.grpc.pb.o client.o sns.pb.o sns.grpc.pb.o tsc.o
	$(CXX) $^ $(LDFLAGS) -g -o $@

tsd: coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o tsd.o
	$(CXX) $^ $(LDFLAGS) -g -o $@

cord: coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o coordinator.o
	$(CXX) $^ $(LDFLAGS) -g -o $@

sync: coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o synchronizer.o
	$(CXX) $^ $(LDFLAGS) -g -o $@

.PRECIOUS: %.grpc.pb.cc
%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

.PRECIOUS: %.pb.cc
%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -f *~ *.o *.pb.cc *.pb.h tsc tsd cord sync

# SYSTEM CHECKS

PROTOC_CMD = which $(PROTOC)
PROTOC_CHECK_CMD = $(PROTOC) --version | grep -q 'libprotoc.3\|libprotoc [0-9][0-9]\.'
PLUGIN_CHECK_CMD = which $(GRPC_CPP_PLUGIN)
HAS_PROTOC = $(shell $(PROTOC_CMD) > /dev/null && echo true || echo false)

ifeq ($(HAS_PROTOC),true)
HAS_VALID_PROTOC = $(shell $(PROTOC_CHECK_CMD) 2> /dev/null && echo true || echo false)
endif

HAS_PLUGIN = $(shell $(PLUGIN_CHECK_CMD) > /dev/null && echo true || echo false)

SYSTEM_OK = false
ifeq ($(HAS_VALID_PROTOC),true)
ifeq ($(HAS_PLUGIN),true)
SYSTEM_OK = true
endif
endif

system-check:
ifneq ($(HAS_VALID_PROTOC),true)
	@echo " DEPENDENCY ERROR"
	@echo
	@echo "You don't have protoc 3.0.0 or newer installed in your path."
	@echo "Download it here: https://github.com/protocolbuffers/protobuf/releases"
	-$(PROTOC) --version
endif
ifneq ($(HAS_PLUGIN),true)
	@echo " DEPENDENCY ERROR"
	@echo "grpc_cpp_plugin not found. Install grpc from: https://github.com/grpc/grpc"
	-which $(GRPC_CPP_PLUGIN)
endif
ifneq ($(SYSTEM_OK),true)
	@false
endif
