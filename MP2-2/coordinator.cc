// coordinator.cc - Handles registration and coordination between clients, servers, and synchronizers

#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <chrono>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "coordinator.grpc.pb.h"
#include "sns.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;
using csce438::SNSService;

#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

std::mutex registry_mutex;

struct NodeEntry {
  ServerInfo info;
  std::chrono::steady_clock::time_point last_heartbeat;
  bool is_active;
};

std::unordered_map<int, NodeEntry> server_nodes;   // key: server ID
std::unordered_map<int, NodeEntry> synch_nodes;    // key: synchronizer ID

class CoordServiceImpl final : public CoordService::Service {
  Status Heartbeat(ServerContext* context, const ServerInfo* request, Confirmation* response) override {
    std::lock_guard<std::mutex> lock(registry_mutex);
    int id = request->serverid();
    std::string type = request->type();

    NodeEntry& entry = (type == "follower") ? synch_nodes[id] : server_nodes[id];
    entry.info = *request;
    entry.last_heartbeat = std::chrono::steady_clock::now();
    entry.is_active = true;

    std::cout << "[Heartbeat] " << type << " ID: " << id << " @ " << request->hostname() << ":" << request->port() << std::endl;
    response->set_status(true);
    return Status::OK;
  }

  Status GetAllFollowerServers(ServerContext* context, const ID* request, ServerList* response) override {
    std::lock_guard<std::mutex> lock(registry_mutex);
    for (const auto& [id, entry] : synch_nodes) {
      if (entry.is_active) {
        response->add_serverid(entry.info.serverid());
        response->add_hostname(entry.info.hostname());
        response->add_port(entry.info.port());
        response->add_type(entry.info.type());
      }
    }
    return Status::OK;
  }

  Status GetServer(ServerContext* context, const ID* request, ServerInfo* response) override {
    std::lock_guard<std::mutex> lock(registry_mutex);
    int cluster_id = ((request->id() - 1) % 3) + 1;
    for (const auto& [id, entry] : server_nodes) {
      if (entry.info.clusterid() == cluster_id && entry.is_active) {
        *response = entry.info;
        return Status::OK;
      }
    }
    return Status::CANCELLED;
  }

  Status GetFollowerServer(ServerContext* context, const ID* request, ServerInfo* response) override {
    std::lock_guard<std::mutex> lock(registry_mutex);
    int synch_id = request->id();
    if (synch_nodes.count(synch_id) && synch_nodes[synch_id].is_active) {
      *response = synch_nodes[synch_id].info;
      return Status::OK;
    }
    return Status::CANCELLED;
  }
};

void HeartbeatMonitor() {
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(registry_mutex);
    for (auto& [id, entry] : server_nodes) {
      if (std::chrono::duration_cast<std::chrono::seconds>(now - entry.last_heartbeat).count() > 10) {
        entry.is_active = false;
      }
    }
    for (auto& [id, entry] : synch_nodes) {
      if (std::chrono::duration_cast<std::chrono::seconds>(now - entry.last_heartbeat).count() > 10) {
        entry.is_active = false;
      }
    }
  }
}

void RunCoordinator(const std::string& port) {
  CoordServiceImpl service;
  ServerBuilder builder;
  builder.AddListeningPort("0.0.0.0:" + port, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Coordinator listening on port " << port << std::endl;
  log(INFO, "Coordinator ready on port " + port);

  std::thread(HeartbeatMonitor).detach();
  server->Wait();
}

int main(int argc, char** argv) {
  std::string port = "9090";
  int opt;
  while ((opt = getopt(argc, argv, "p:")) != -1) {
    if (opt == 'p') port = optarg;
  }
  std::string log_file_name = "coordinator-" + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Coordinator log started");
  RunCoordinator(port);
  return 0;
}
