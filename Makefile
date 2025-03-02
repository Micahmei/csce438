# Hack: utf8_range (which is protobuf's dependency) currently doesn't have a pkgconfig file, so we need to explicitly
# tweak the list of libraries to link against to fix the build.

export PKG_CONFIG_PATH = /home/csce438/.local/lib/pkgconfig:/usr/local/lib/pkgconfig:/home/csce438/grpc/third_party/re2:/home/csce438/.local/share/pkgconfig


CXX = g++
PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH = $(shell which $(GRPC_CPP_PLUGIN))
PROTOS_PATH = .


CPPFLAGS += `pkg-config --cflags protobuf grpc`
CXXFLAGS += -std=c++17 -g
LDFLAGS += -L/home/csce438/.local/lib -L/usr/local/lib `pkg-config --libs --static protobuf grpc++` \
           -lutf8_validity -lpthread -ldl -lglog -lgrpc++_reflection


BINARIES = tsc tsd coordinator


all: system-check $(BINARIES)

# tsc
tsc: client.o coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o tsc.o
	$(CXX) coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o client.o tsc.o $(LDFLAGS) -o tsc

# tsd
tsd: coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o tsd.o
	$(CXX) coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o tsd.o $(LDFLAGS) -o tsd

# coordinator
coordinator: coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o coordinator.o
	$(CXX) coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o coordinator.o $(LDFLAGS) -o coordinator


.PRECIOUS: %.grpc.pb.cc
%.grpc.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<


.PRECIOUS: %.pb.cc
%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<


clean:
	rm -f *.o *.pb.cc *.pb.h $(BINARIES)

# The following is to test your system and ensure a smoother experience.
# They are by no means necessary to actually compile a grpc-enabled software.

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
	@echo "DEPENDENCY ERROR"
	@echo
	@echo "You don't have protoc 3.0.0 or newer installed in your path."
	@echo "Please install Google Protocol Buffers 3.0.0 and its compiler."
	@echo "You can find it here:"
	@echo
	@echo "   https://github.com/google/protobuf/releases/tag/v3.0.0"
	@echo
	@echo "Here is what I get when trying to evaluate your version of protoc:"
	@echo
	-$(PROTOC) --version
	@echo
	@false
endif
ifneq ($(HAS_PLUGIN),true)
	@echo "DEPENDENCY ERROR"
	@echo
	@echo "You don't have the gRPC C++ protobuf plugin installed in your path."
	@echo "Please install gRPC. You can find it here:"
	@echo
	@echo "   https://github.com/grpc/grpc"
	@echo
	@echo "Here is what I get when trying to detect if you have the plugin:"
	@echo
	-which $(GRPC_CPP_PLUGIN)
	@echo
	@false
endif
ifneq ($(SYSTEM_OK),true)
	@false
endif
