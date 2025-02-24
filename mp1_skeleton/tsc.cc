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
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

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
      : cord_ip(hname), username(uname), cord_port(p) {}

protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string &input);
  virtual void processTimeline();

private:
  uint64_t client_id;
  std::string cord_ip;
  std::string username;
  std::string cord_port;

  // You can have an instance of the client stub
  // as a member variable.
  std::unique_ptr<SNSService::Stub> stub_;
  std::unique_ptr<csce438::Coordinator::Stub> coordinator_stub;

  IReply Login();
  IReply List();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void Timeline(const std::string &username);
};

///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo()
{
  // ------------------------------------------------------------
  // In this function, you are supposed to create a stub so that
  // you call service methods in the processCommand/porcessTimeline
  // functions. That is, the stub should be accessible when you want
  // to call any service methods in those functions.
  // Please refer to gRpc tutorial how to create a stub.
  // ------------------------------------------------------------

  ///////////////////////////////////////////////////////////
  // YOUR CODE HERE
  //////////////////////////////////////////////////////////

  // prevent gRPC from using global http_proxy
  grpc::ChannelArguments ch_args;
  ch_args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);

  // connect to coordinator first
  auto coordinator_channel = grpc::CreateCustomChannel(cord_ip + ":" + cord_port, grpc::InsecureChannelCredentials(), ch_args);
  coordinator_stub = csce438::Coordinator::NewStub(coordinator_channel);

  if (!coordinator_stub)
  {
    std::cerr << "Failed to create coordinator stub." << std::endl;
    return -1;
  }

  ClientContext context;
  csce438::ServerInfo server_info;
  csce438::ID id;
  id.set_client_id(std::stoull(username));
  Status connect_to_coordinator_status = coordinator_stub->GetServer(&context, id, &server_info);
  if (connect_to_coordinator_status.ok())
  {
    if (server_info.server_id() == 0)
    {
      std::cerr << "no available server" << std::endl;
      return -1;
    }
    else
    {
      std::string server_address = server_info.server_ip() + ":" + server_info.server_port();
      auto channel = grpc::CreateCustomChannel(server_address, grpc::InsecureChannelCredentials(), ch_args);
      stub_ = SNSService::NewStub(channel);
      if (!stub_)
      {
        std::cerr << "Failed to create server stub." << std::endl;
        return -1;
      }
      IReply reply = Login();
      if (reply.grpc_status.ok())
      {
        if (reply.comm_status == FAILURE_ALREADY_EXISTS)
          return -1;
        return 1;
      }
    }
  }
  else
  {
    std::cerr << "RPC failed with error code " << connect_to_coordinator_status.error_code() << ": " << connect_to_coordinator_status.error_message() << std::endl;
    std::cerr << "Details: " << connect_to_coordinator_status.error_details() << std::endl;
  }
  return -1;
}

IReply Client::processCommand(std::string &input)
{
  // ------------------------------------------------------------
  // GUIDE 1:
  // In this function, you are supposed to parse the given input
  // command and create your own message so that you call an
  // appropriate service method. The input command will be one
  // of the followings:
  //
  // FOLLOW <username>
  // UNFOLLOW <username>
  // LIST
  // TIMELINE
  // ------------------------------------------------------------

  // ------------------------------------------------------------
  // GUIDE 2:
  // Then, you should create a variable of IReply structure
  // provided by the client.h and initialize it according to
  // the result. Finally you can finish this function by returning
  // the IReply.
  // ------------------------------------------------------------

  // ------------------------------------------------------------
  // HINT: How to set the IReply?
  // Suppose you have "FOLLOW" service method for FOLLOW command,
  // IReply can be set as follow:
  //
  //     // some codes for creating/initializing parameters for
  //     // service method
  //     IReply ire;
  //     grpc::Status status = stub_->FOLLOW(&context, /* some parameters */);
  //     ire.grpc_status = status;
  //     if (status.ok()) {
  //         ire.comm_status = SUCCESS;
  //     } else {
  //         ire.comm_status = FAILURE_NOT_EXISTS;
  //     }
  //
  //      return ire;
  //
  // IMPORTANT:
  // For the command "LIST", you should set both "all_users" and
  // "following_users" member variable of IReply.
  // ------------------------------------------------------------

  IReply ire;

  /*********
  YOUR CODE HERE
  **********/
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

// List Command
IReply Client::List()
{

  IReply ire;

  /*********
  YOUR CODE HERE
  **********/
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

// Follow Command
IReply Client::Follow(const std::string &username2)
{

  IReply ire;

  /***
  YOUR CODE HERE
  ***/
  Request request;
  request.set_username(username);
  request.add_arguments(username2);

  Reply reply;
  ClientContext context;

  Status status = stub_->Follow(&context, request, &reply);
  ire.grpc_status = status;

  if (status.ok())
  {
    if (reply.msg() == "followed successful")
      ire.comm_status = SUCCESS;
    else if (reply.msg() == "cannot follow self")
      ire.comm_status = FAILURE_ALREADY_EXISTS;
    else if (reply.msg() == "followed user no exist")
      ire.comm_status = FAILURE_INVALID_USERNAME;
  }
  else
  {
    ire.comm_status = FAILURE_UNKNOWN;
  }

  return ire;
}

// UNFollow Command
IReply Client::UnFollow(const std::string &username2)
{

  IReply ire;

  /***
  YOUR CODE HERE
  ***/
  Request request;
  request.set_username(username);
  request.add_arguments(username2);

  Reply reply;
  ClientContext context;

  Status status = stub_->UnFollow(&context, request, &reply);
  ire.grpc_status = status;

  if (status.ok())
  {
    if (reply.msg() == "unfollow successful")
      ire.comm_status = SUCCESS;
    else if (reply.msg() == "unfollowed user no exist")
      ire.comm_status = FAILURE_INVALID_USERNAME;
    else if (reply.msg() == "cannot unfollow self")
      ire.comm_status = FAILURE_INVALID_USERNAME;
  }
  else
  {
    ire.comm_status = FAILURE_UNKNOWN;
  }

  return ire;
}

// Login Command
IReply Client::Login()
{

  IReply ire;

  /***
   YOUR CODE HERE
  ***/
  Request request;
  request.set_username(username);

  Reply reply;
  ClientContext context;

  Status status = stub_->Login(&context, request, &reply);

  if (!status.error_message().empty())
  {
    std::cerr << status.error_message() << std::endl;
  }
  ire.grpc_status = status;

  if (status.ok())
  {
    if (reply.msg() == "login successful")
      ire.comm_status = SUCCESS;
    else if (reply.msg() == "user already exists")
      ire.comm_status = FAILURE_ALREADY_EXISTS;
  }
  else
  {
    ire.comm_status = FAILURE_UNKNOWN;
  }

  return ire;
}

// Timeline Command
void Client::Timeline(const std::string &username)
{

  // ------------------------------------------------------------
  // In this function, you are supposed to get into timeline mode.
  // You may need to call a service method to communicate with
  // the server. Use getPostMessage/displayPostMessage functions
  // in client.cc file for both getting and displaying messages
  // in timeline mode.
  // ------------------------------------------------------------

  // ------------------------------------------------------------
  // IMPORTANT NOTICE:
  //
  // Once a user enter to timeline mode , there is no way
  // to command mode. You don't have to worry about this situation,
  // and you can terminate the client program by pressing
  // CTRL-C (SIGINT)
  // ------------------------------------------------------------

  /***
  YOUR CODE HERE
  ***/
  ClientContext context;
  std::shared_ptr<ClientReaderWriter<Message, Message>> stream(
      stub_->Timeline(&context));

  // send username
  Message init_msg;
  init_msg.set_username(username);
  stream->Write(init_msg);

  // folk a new thread to read new posts from server
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

  // process the inputs and send them into stream
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

//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char **argv)
{
  std::string username = "default";

  std::string cord_ip = "127.0.0.1";
  std::string cord_port = "9090";

  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:k:")) != -1)
  {
    switch (opt)
    {
    case 'h':
      cord_ip = optarg;
      break;
    case 'u':
      username = optarg;
      break;
    case 'k':
      cord_port = optarg;
      break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }

  std::cout << "Logging Initialized. Client starting..." << std::endl;

  Client myc(cord_ip, username, cord_port);

  myc.run();

  return 0;
}
