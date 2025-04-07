// Updated tsd.cc with integrated Master and Slave services
// Copyright and includes remain unchanged

#include <ctime>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <algorithm>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#define GLOG_EXPORT
#define GLOG_NO_EXPORT
#include <glog/logging.h>
#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include <thread>

using csce438::Confirmation;
using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::ServerInfo;
using csce438::SNSService;
using csce438::SlaveService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

std::vector<std::string> timeline_db;

class SNSServiceImpl final : public SNSService::Service {
  // SNS logic ... simplified
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    std::string user = request->username();
    std::string path = "./cluster_data/" + user + ".user";
    std::ofstream(path).close(); // simulate user file
    reply->set_msg("login successful");
    return Status::OK;
  }

  Status CheckConnection(ServerContext*, const Request*, Reply* response) override {
    response->set_msg("OK");
    return Status::OK;
  }
};

class SlaveServiceImpl final : public SlaveService::Service {
  Status MirrorLogin(ServerContext* context, const Request* request, Reply* reply) override {
    std::string user = request->username();
    std::string path = "./cluster_data/" + user + ".user";
    std::ofstream(path).close();
    reply->set_msg("mirrored login");
    return Status::OK;
  }

  Status MirrorFollow(ServerContext* context, const Request* request, Reply* reply) override {
    // append followee to follower's file
    std::ofstream("./cluster_data/" + request->username() + "_followees.txt", std::ios::app)
        << request->arguments(0) << std::endl;
    reply->set_msg("mirrored follow");
    return Status::OK;
  }

  Status MirrorUnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    reply->set_msg("mirrored unfollow (noop)");
    return Status::OK;
  }

  Status MirrorTimelinePost(ServerContext* context, const Message* msg, Reply* reply) override {
    std::string path = "./cluster_data/" + msg->username() + "_timeline.txt";
    std::ofstream file(path, std::ios::app);
    file << msg->msg() << std::endl;
    reply->set_msg("mirrored post");
    return Status::OK;
  }
};

void RunServer(uint64_t server_id, std::string ip, std::string port, uint64_t cluster_id, std::string cord_ip, std::string cord_port) {
  grpc::ChannelArguments ch_args;
  ch_args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);

  std::string listen_address = "0.0.0.0:" + port;
  SNSServiceImpl sns_service;
  SlaveServiceImpl slave_service;

  ServerBuilder builder;
  builder.AddListeningPort(listen_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&sns_service);
  builder.RegisterService(&slave_service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << listen_address << std::endl;
  log(INFO, "Server listening on " + listen_address);

  server->Wait();
}

int main(int argc, char** argv) {
  uint64_t id = 1;
  std::string ip = "127.0.0.1";
  std::string port = "3010";
  uint64_t cluster_id = 1;
  std::string cord_ip = "127.0.0.1";
  std::string cord_port = "9090";

  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "-s" && i + 1 < argc) id = std::stoull(argv[++i]);
    else if (std::string(argv[i]) == "-p" && i + 1 < argc) port = argv[++i];
    else if (std::string(argv[i]) == "-c" && i + 1 < argc) cluster_id = std::stoull(argv[++i];
    else if (std::string(argv[i]) == "-h" && i + 1 < argc) cord_ip = argv[++i];
    else if (std::string(argv[i]) == "-k" && i + 1 < argc) cord_port = argv[++i];
  }

  std::string log_file_name = "server-" + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(id, ip, port, cluster_id, cord_ip, cord_port);

  return 0;
}
