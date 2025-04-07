// synchronizer.cc - Minimal synchronizer with RabbitMQ and gRPC communication

#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <fstream>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "coordinator.grpc.pb.h"
#include "sns.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using csce438::Confirmation;
using csce438::ServerInfo;
using csce438::ID;
using csce438::CoordService;

#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

int synchID = 1;
int clusterID = 1;
bool isMaster = false;

class DummySynchronizerService final : public csce438::SynchService::Service {
  // Implement required gRPC services here if necessary
};

void InitialHeartbeat(const std::string& coordIP, const std::string& coordPort) {
  std::string address = coordIP + ":" + coordPort;
  auto stub = csce438::CoordService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));

  ServerInfo info;
  info.set_serverid(synchID);
  info.set_clusterid(clusterID);
  info.set_hostname("127.0.0.1");
  info.set_port("900" + std::to_string(synchID));
  info.set_type("follower");

  Confirmation reply;
  grpc::ClientContext context;
  Status status = stub->Heartbeat(&context, info, &reply);

  if (status.ok()) {
    std::cout << "Heartbeat registered with coordinator. Status: " << reply.status() << std::endl;
  } else {
    std::cerr << "Failed to register with coordinator." << std::endl;
  }
}

void RunSynchronizerServer(const std::string& port) {
  std::string address = "0.0.0.0:" + port;
  DummySynchronizerService service;

  ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Synchronizer listening on " << address << std::endl;
  log(INFO, "Synchronizer ready at " + address);

  server->Wait();
}

int main(int argc, char** argv) {
  std::string coord_ip = "127.0.0.1";
  std::string coord_port = "9090";
  std::string port = "9010";

  int opt = 0;
  while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1) {
    switch (opt) {
    case 'h': coord_ip = optarg; break;
    case 'k': coord_port = optarg; break;
    case 'p': port = optarg; break;
    case 'i': synchID = std::stoi(optarg); break;
    }
  }
  clusterID = ((synchID - 1) % 3) + 1;

  std::string log_file = "synchronizer-" + port;
  google::InitGoogleLogging(log_file.c_str());
  log(INFO, "Logging initialized.");

  InitialHeartbeat(coord_ip, coord_port);
  RunSynchronizerServer(port);

  return 0;
}
