/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

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

using csce438::Confirmation;
using csce438::Coordinator;
using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::ServerInfo;
using csce438::SNSService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

// return local time
std::time_t iso8601ToTimeT(const std::string &iso8601)
{
  std::tm tm = {};
  std::istringstream ss(iso8601);

  ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");

  if (ss.fail())
  {
    std::cerr << "Error: Failed to parse timestamp: " << iso8601 << std::endl;
    return 0;
  }

  return timegm(&tm);
}

struct Post
{
  std::string username;
  std::string post;
  std::time_t timestamp;
};

struct Client
{
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client *> client_followers;
  std::vector<Client *> client_following;
  ServerReaderWriter<Message, Message> *stream = 0;
  bool operator==(const Client &c1) const
  {
    return (username == c1.username);
  }
};

// Vector that stores every client that has been created
std::vector<Client *> client_db;

class SNSServiceImpl final : public SNSService::Service
{
  std::unordered_map<std::string, std::unordered_map<std::string, std::time_t>> follow_time;
  std::unique_ptr<Coordinator::Stub> coordinator_stub;

public:
  SNSServiceImpl(std::shared_ptr<Channel> channel) : coordinator_stub(Coordinator::NewStub(channel)) {}

  bool SendHeartbeat(const uint64_t &cluster_id, const uint64_t &server_id, const std::string &ip, const std::string &port)
  {
    ServerInfo server_info;
    server_info.set_cluster_id(cluster_id);
    server_info.set_server_id(server_id);
    server_info.set_server_ip(ip);
    server_info.set_server_port(port);

    Confirmation confirmation;
    ClientContext context;
    Status status = coordinator_stub->Heartbeat(&context, server_info, &confirmation);

    if (status.ok())
    {
      std::cout << "Heartbeat sent successfully: " << confirmation.message() << std::endl;
      return confirmation.success();
    }
    else
    {
      std::cout << "Heartbeat failed: " << status.error_message() << std::endl;
      return false;
    }
  }

private:
  Status List(ServerContext *context, const Request *request, ListReply *list_reply) override
  {
    /*********
    YOUR CODE HERE
    **********/
    std::string username = request->username();
    for (const auto &client : client_db)
    {
      list_reply->add_all_users(client->username);
    }

    Client *current_user = nullptr;
    for (const auto &client : client_db)
    {
      if (client->username == username)
      {
        current_user = client;
        break;
      }
    }
    for (const auto &follower : current_user->client_followers)
    {
      list_reply->add_followers(follower->username);
    }

    return Status::OK;
  }

  Status Follow(ServerContext *context, const Request *request, Reply *reply) override
  {

    /*********
    YOUR CODE HERE
    **********/
    // u1 follow -> u2
    std::string follower_name = request->username();   // u1
    std::string followee_name = request->arguments(0); // u2

    if (follower_name == followee_name)
    {
      reply->set_msg("cannot follow self");
      return Status::OK;
    }

    Client *follower = nullptr;
    Client *followee = nullptr;

    for (auto &client : client_db)
    {
      if (client->username == follower_name)
      {
        follower = client;
      }
      if (client->username == followee_name)
      {
        followee = client;
      }
    }

    if (followee == nullptr)
    {
      reply->set_msg("followed user no exist");
      return Status::OK;
    }

    for (const auto &following : follower->client_following)
    {
      if (following->username == followee_name)
      {
        reply->set_msg("already following user");
        return Status::OK;
      }
    }

    follower->client_following.push_back(followee);
    followee->client_followers.push_back(follower);

    follow_time[follower_name][followee_name] = std::time(nullptr);

    reply->set_msg("followed successful");

    return Status::OK;
  }

  Status UnFollow(ServerContext *context, const Request *request, Reply *reply) override
  {

    /*********
    YOUR CODE HERE
    **********/
    std::string username = request->username();
    std::string unfollow_username = request->arguments(0);

    if (username == unfollow_username)
    {
      reply->set_msg("cannot unfollow self");
      return Status::OK;
    }

    Client *user = nullptr;
    Client *unfollow_user = nullptr;

    for (auto &client : client_db)
    {
      if (client->username == username)
      {
        user = client;
      }
      if (client->username == unfollow_username)
      {
        unfollow_user = client;
      }
    }

    if (unfollow_user == nullptr)
    {
      reply->set_msg("unfollowed user no exist");
      return Status::OK;
    }

    auto it = std::find(user->client_following.begin(), user->client_following.end(), unfollow_user);
    if (it != user->client_following.end())
    {
      user->client_following.erase(it);
      auto follower_it = std::find(unfollow_user->client_followers.begin(), unfollow_user->client_followers.end(), user);
      if (follower_it != unfollow_user->client_followers.end())
      {
        unfollow_user->client_followers.erase(follower_it);
      }
      reply->set_msg("unfollow successful");
    }
    else
    {
      reply->set_msg("unfollowed user no exist");
    }

    return Status::OK;
  }

  Status Login(ServerContext *context, const Request *request, Reply *reply) override
  {

    /*********
    YOUR CODE HERE
    **********/
    std::cout << "login request from [" << request->username() << "] ";
    std::string username = request->username();
    for (Client *c : client_db)
    {
      if (c->username == username)
      {
        std::cout << "User already exists" << std::endl;
        reply->set_msg("user already exists");
        return Status::OK;
      }
    }

    // if user does not exist in the client_db, add it into client_db
    Client *new_client = new Client();
    new_client->username = username;
    new_client->connected = true;
    client_db.push_back(new_client);

    std::cout << "Login successful" << std::endl;
    reply->set_msg("login successful");
    return Status::OK;
  }

  Status Timeline(ServerContext *context,
                  ServerReaderWriter<Message, Message> *stream) override
  {

    /*********
    YOUR CODE HERE
    **********/
    std::string username;
    Message msg;

    // get username from the initial message
    if (stream->Read(&msg))
    {
      username = msg.username();
    }
    else
    {
      return Status::CANCELLED;
    }

    std::string timeline_file = "./timelines/" + username + ".txt";

    if (!std::filesystem::exists("./timelines"))
    {
      std::filesystem::create_directory("./timelines");
    }

    // get client of user
    Client *client = nullptr;
    for (Client *c : client_db)
    {
      if (c->username == username)
      {
        client = c;
        break;
      }
    }
    if (!client)
    {
      return Status::CANCELLED;
    }

    // read the posts of the followings of the user
    std::vector<Post> history_posts;
    for (Client *followed_user : client->client_following)
    {
      std::string followed_timeline = "./timelines/" + followed_user->username + ".txt";
      std::ifstream file(followed_timeline);
      std::string line, post;
      int count = 0;

      while (std::getline(file, line))
      {
        if (line.empty() && !post.empty())
        {
          // parse post
          std::istringstream post_stream(post);
          std::string line, post_username, post_content;
          std::time_t post_timestamp;

          while (std::getline(post_stream, line))
          {
            if (line[0] == 'T')
            {
              post_timestamp = iso8601ToTimeT(line.substr(2));
            }
            else if (line[0] == 'U')
            {
              post_username = line.substr(2);
            }
            else if (line[0] == 'W')
            {
              post_content = line.substr(2);
            }
          }

          std::time_t follow_timestamp = follow_time[username][post_username];
          if (post_timestamp >= follow_timestamp)
          {
            history_posts.push_back({post_username, post_content, post_timestamp});
          }
          post.clear();
          count++;
        }
        else
        {
          post += line + "\n";
        }
      }
    }

    std::sort(history_posts.begin(), history_posts.end(), [](const Post &a, const Post &b)
              { return a.timestamp > b.timestamp; });

    auto n = std::min((size_t)20, history_posts.size());
    for (int i = 0; i < n; i++)
    {
      auto post = history_posts[i];
      Message message;
      message.set_username(post.username);
      message.set_msg(post.post);
      message.mutable_timestamp()->set_seconds(post.timestamp);
      message.mutable_timestamp()->set_nanos(0);
      stream->Write(message);
    }

    // allow the user to subscribe to new messages from the followings
    client->stream = stream;

    // listen the inputs from clients
    while (stream->Read(&msg))
    {
      std::string new_post = "T " + google::protobuf::util::TimeUtil::ToString(msg.timestamp()) +
                             "\nU " + msg.username() +
                             "\nW " + msg.msg() + "\n\n";

      std::ofstream file(timeline_file, std::ios::app);
      file << new_post;
      file.close();

      // send the post to all followers of the user
      for (Client *follower : client->client_followers)
      {
        if (follower->stream)
        {
          follower->stream->Write(msg);
        }
      }
    }

    client->stream = nullptr;

    return Status::OK;
  }
  Status CheckConnection(ServerContext *context, const Request *request, Reply *response) override
  {
    return Status::OK;
  }
};

void RunServer(uint64_t server_id, std::string ip, std::string port, uint64_t cluster_id, std::string cord_ip, std::string cord_port)
{
  grpc::ChannelArguments ch_args;
  ch_args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
  SNSServiceImpl service(grpc::CreateCustomChannel(cord_ip + ":" + cord_port, grpc::InsecureChannelCredentials(), ch_args));

  std::thread heartbeat_thread(
      [&service, &cluster_id, &server_id, &ip, &port]()
      {
        while (true)
        {
          if (!service.SendHeartbeat(cluster_id, server_id, ip, port))
          {
            std::cerr << "Heartbeat failed! Retrying..." << std::endl;
          }
          std::this_thread::sleep_for(std::chrono::seconds(10));
        }
      });

  std::string listen_address = "0.0.0.0:" + port;
  ServerBuilder builder;
  builder.AddListeningPort(listen_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << listen_address << std::endl;
  log(INFO, "Server listening on " + listen_address);

  server->Wait();
  heartbeat_thread.join();
}

int main(int argc, char **argv)
{
  uint64_t id = 1;
  std::string ip = "127.0.0.1";
  std::string port = "3010";

  uint64_t cluster_id = 1;
  std::string cord_ip = "127.0.0.1";
  std::string cord_port = "9090";

  for (int i = 1; i < argc; ++i)
  {
    if (std::string(argv[i]) == "-s" && i + 1 < argc)
    {
      id = std::stoull(argv[i + 1]);
      ++i;
    }
    else if (std::string(argv[i]) == "-p" && i + 1 < argc)
    {
      port = argv[i + 1];
      ++i;
    }
    else if (std::string(argv[i]) == "-c" && i + 1 < argc)
    {
      cluster_id = std::stoull(argv[i + 1]);
      ++i;
    }
    else if (std::string(argv[i]) == "-h" && i + 1 < argc)
    {
      cord_ip = argv[i + 1];
      ++i;
    }
    else if (std::string(argv[i]) == "-k" && i + 1 < argc)
    {
      cord_port = argv[i + 1];
      ++i;
    }
    else
    {
      std::cerr << "Invalid Command Line Argument\n";
      return 0;
    }
  }

  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(id, ip, port, cluster_id, cord_ip, cord_port);

  return 0;
}
