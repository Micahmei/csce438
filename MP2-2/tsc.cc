#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <algorithm>
#include <grpc++/grpc++.h>
#include "client.h"

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"

using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::SNSService;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;

Message MakeMessage(const std::string &username, const std::string &msg) {
  Message m;
  m.set_username(username);
  m.set_msg(msg);
  google::protobuf::Timestamp *timestamp = new google::protobuf::Timestamp();
  timestamp->set_seconds(time(NULL));
  timestamp->set_nanos(0);
  m.set_allocated_timestamp(timestamp);
  return m;
}

class Client : public IClient {
public:
  Client(const std::string &hname, const std::string &uname, const std::string &p)
      : coord_ip(hname), username(uname), coord_port(p) {}

protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string &input);
  virtual void processTimeline();

private:
  std::string coord_ip;
  std::string coord_port;
  std::string username;

  std::unique_ptr<SNSService::Stub> stub_;
  std::unique_ptr<csce438::CoordService::Stub> coordinator_stub;

  IReply Login();
  IReply List();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void Timeline(const std::string &username);
};

int Client::connectTo() {
  grpc::ChannelArguments ch_args;
  ch_args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
  auto coord_channel = grpc::CreateCustomChannel(coord_ip + ":" + coord_port, grpc::InsecureChannelCredentials(), ch_args);
  coordinator_stub = csce438::CoordService::NewStub(coord_channel);

  ClientContext context;
  csce438::ID id;
  id.set_id(std::stoi(username));
  csce438::ServerInfo server_info;
  Status status = coordinator_stub->GetServer(&context, id, &server_info);
  if (status.ok()) {
    if (server_info.serverid() == 0) return -1;
    auto server_channel = grpc::CreateCustomChannel(server_info.hostname() + ":" + server_info.port(), grpc::InsecureChannelCredentials(), ch_args);
    stub_ = SNSService::NewStub(server_channel);
    IReply reply = Login();
    return reply.comm_status == SUCCESS ? 1 : -1;
  }
  return -1;
}

IReply Client::processCommand(std::string &input) {
  std::istringstream iss(input);
  std::string command, arg;
  iss >> command;
  IReply ire;

  if (command == "FOLLOW") {
    iss >> arg;
    ire = Follow(arg);
  } else if (command == "UNFOLLOW") {
    iss >> arg;
    ire = UnFollow(arg);
  } else if (command == "LIST") {
    ire = List();
  } else if (command == "TIMELINE") {
    ClientContext context;
    Request req;
    req.set_username("ping");
    Reply rep;
    Status status = stub_->CheckConnection(&context, req, &rep);
    ire.grpc_status = status;
    ire.comm_status = status.ok() ? SUCCESS : FAILURE_UNKNOWN;
  } else {
    ire.comm_status = FAILURE_INVALID;
  }
  return ire;
}

void Client::processTimeline() {
  Timeline(username);
}

IReply Client::Login() {
  Request req;
  req.set_username(username);
  Reply rep;
  ClientContext context;
  Status status = stub_->Login(&context, req, &rep);
  IReply ire;
  ire.grpc_status = status;
  ire.comm_status = (status.ok() && rep.msg() == "login successful") ? SUCCESS : FAILURE_ALREADY_EXISTS;
  return ire;
}

IReply Client::List() {
  Request req;
  req.set_username(username);
  ListReply rep;
  ClientContext context;
  Status status = stub_->List(&context, req, &rep);
  IReply ire;
  ire.grpc_status = status;
  if (status.ok()) {
    ire.comm_status = SUCCESS;
    ire.all_users.assign(rep.all_users().begin(), rep.all_users().end());
    ire.followers.assign(rep.followers().begin(), rep.followers().end());
  } else {
    ire.comm_status = FAILURE_UNKNOWN;
  }
  return ire;
}

IReply Client::Follow(const std::string &user) {
  Request req;
  req.set_username(username);
  req.add_arguments(user);
  Reply rep;
  ClientContext context;
  Status status = stub_->Follow(&context, req, &rep);
  IReply ire;
  ire.grpc_status = status;
  if (status.ok()) {
    if (rep.msg() == "followed successful") ire.comm_status = SUCCESS;
    else if (rep.msg() == "cannot follow self") ire.comm_status = FAILURE_ALREADY_EXISTS;
    else ire.comm_status = FAILURE_INVALID_USERNAME;
  } else ire.comm_status = FAILURE_UNKNOWN;
  return ire;
}

IReply Client::UnFollow(const std::string &user) {
  Request req;
  req.set_username(username);
  req.add_arguments(user);
  Reply rep;
  ClientContext context;
  Status status = stub_->UnFollow(&context, req, &rep);
  IReply ire;
  ire.grpc_status = status;
  if (status.ok()) {
    if (rep.msg() == "unfollow successful") ire.comm_status = SUCCESS;
    else ire.comm_status = FAILURE_INVALID_USERNAME;
  } else ire.comm_status = FAILURE_UNKNOWN;
  return ire;
}

void Client::Timeline(const std::string &user) {
  ClientContext context;
  auto stream = stub_->Timeline(&context);
  Message init;
  init.set_username(user);
  stream->Write(init);

  std::thread reader([stream]() mutable {
    Message msg;
    while (stream->Read(&msg)) {
      displayPostMessage(msg.username(), msg.msg(), msg.timestamp().seconds());
    }
  });

  while (true) {
    std::string msg = getPostMessage();
    if (!msg.empty()) stream->Write(MakeMessage(user, msg));
  }
  reader.join();
}

int main(int argc, char **argv) {
  std::string username = "default", coord_ip = "127.0.0.1", coord_port = "9090";
  int opt;
  while ((opt = getopt(argc, argv, "h:u:k:")) != -1) {
    switch (opt) {
      case 'h': coord_ip = optarg; break;
      case 'u': username = optarg; break;
      case 'k': coord_port = optarg; break;
      default: std::cerr << "Invalid argument\n";
    }
  }
  Client client(coord_ip, username, coord_port);
  client.run();
  return 0;
}
