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
#include "coordinator.grpc.pb.h"  // 需要与 Coordinator 交互
#include "coordinator.pb.h"
using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::SNSService;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

/* ======= HEARTBEAT 代码 ======= */
using csce438::Coordinator;
using csce438::ClientInfo;
using csce438::Confirmation;

class ClientNode {
public:
    ClientNode(std::shared_ptr<Channel> channel)
        : stub_(Coordinator::NewStub(channel)) {}

    void SendHeartbeat(const std::string& username) {
        ClientInfo request;
        request.set_username(username);

        Confirmation reply;
        ClientContext context;

        Status status = stub_->ClientHeartbeat(&context, request, &reply);
        if (status.ok()) {
            std::cout << "[HEARTBEAT] Client " << username << " is active." << std::endl;
        } else {
            std::cerr << "[HEARTBEAT ERROR] Failed to send heartbeat to Coordinator." << std::endl;
        }
    }

private:
    std::unique_ptr<Coordinator::Stub> stub_;
};

// 让 Heartbeat 在后台运行
void RunHeartbeat(ClientNode* client, const std::string& username) {
    while (true) {
        client->SendHeartbeat(username);
        std::this_thread::sleep_for(std::chrono::seconds(5));  // 每 5 秒发送一次心跳
    }
}

/* ======= 原来的 `tsc.cc` 代码保留 ======= */

void sig_ignore(int sig)
{
  std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string &username, const std::string &msg)
{
  Message m;
  m.set_username(username);
  m.set_msg(msg);
  google::protobuf::Timestamp *timestamp = new google::protobuf::Timestamp();
  timestamp->set_seconds(time(NULL));
  timestamp->set_nanos(0);
  m.set_allocated_timestamp(timestamp);
  return m;
}

class Client : public IClient
{
public:
  Client(const std::string &hname,
         const std::string &uname,
         const std::string &p)
      : hostname(hname), username(uname), port(p) {}

protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string &input);
  virtual void processTimeline();

private:
  std::string hostname;
  std::string username;
  std::string port;

  std::unique_ptr<SNSService::Stub> stub_;

  IReply Login();
  IReply List();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void Timeline(const std::string &username);
};

int Client::connectTo()
{
  grpc::ChannelArguments ch_args;
  ch_args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
  std::string server_address = hostname + ":" + port;
  auto channel = grpc::CreateCustomChannel(
      hostname + ":" + port,
      grpc::InsecureChannelCredentials(),
      ch_args);

  stub_ = SNSService::NewStub(channel);
  if (!stub_)
  {
    std::cerr << "Failed to create gRPC stub." << std::endl;
    return -1;
  }
  else
  {
    std::cout << "success to create gRPC stub." << std::endl;
  }

  IReply reply = Login();
  if (reply.grpc_status.ok())
  {
    if (reply.comm_status == FAILURE_ALREADY_EXISTS)
      return -1;
    return 1;
  }
  return -1;
}

IReply Client::processCommand(std::string &input)
{
  IReply ire;
  std::istringstream iss(input);
  std::string command, argument;
  iss >> command;

  if (command == "FOLLOW")
  {
    iss >> argument;
    ire = Follow(argument);
  }
  else if (command == "UNFOLLOW")
  {
    iss >> argument;
    ire = UnFollow(argument);
  }
  else if (command == "LIST")
  {
    ire = List();
  }
  else if (command == "TIMELINE")
  {
    processTimeline();
  }
  else
  {
    ire.comm_status = FAILURE_INVALID;
  }

  return ire;
}

void Client::processTimeline()
{
  Timeline(username);
}

IReply Client::List()
{
  IReply ire;
  Request request;
  request.set_username(username);

  ListReply reply;
  ClientContext context;

  Status status = stub_->List(&context, request, &reply);
  ire.grpc_status = status;
  if (status.ok())
  {
    ire.comm_status = SUCCESS;
    ire.all_users.assign(reply.all_users().begin(), reply.all_users().end());
    ire.followers.assign(reply.followers().begin(), reply.followers().end());
  }
  else
  {
    ire.comm_status = FAILURE_UNKNOWN;
  }

  return ire;
}

void Client::Timeline(const std::string &username)
{
  ClientContext context;
  std::shared_ptr<ClientReaderWriter<Message, Message>> stream(
      stub_->Timeline(&context));

  Message init_msg;
  init_msg.set_username(username);
  stream->Write(init_msg);

  std::thread reader(
      [stream]()
      {
        Message msg;
        while (stream->Read(&msg))
        {
          std::time_t timestamp = msg.timestamp().seconds();
          displayPostMessage(msg.username(), msg.msg(), timestamp);
        }
      });

  while (true)
  {
    std::string user_msg = getPostMessage();
    if (!user_msg.empty())
    {
      Message new_msg = MakeMessage(username, user_msg);
      stream->Write(new_msg);
    }
  }

  reader.join();
}

int main(int argc, char **argv)
{
  std::string hostname = "127.0.0.1";
  std::string username = "default";
  std::string port = "3010";
  std::string coordinator_host = "localhost";
  int coordinator_port = 9090;

  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:p:k:")) != -1)
  {
    switch (opt)
    {
    case 'h':
      hostname = optarg;
      break;
    case 'u':
      username = optarg;
      break;
    case 'p':
      port = optarg;
      break;
    case 'k':
      coordinator_port = std::stoi(optarg);
      break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }

  Client myc(hostname, username, port);
  ClientNode client(grpc::CreateChannel(coordinator_host + ":" + std::to_string(coordinator_port), grpc::InsecureChannelCredentials()));

  std::thread heartbeat_thread(RunHeartbeat, &client, username);
  heartbeat_thread.detach();  // 让 `HEARTBEAT` 在后台运行

  myc.run();
  return 0;
}
