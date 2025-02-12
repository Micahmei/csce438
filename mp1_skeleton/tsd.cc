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

using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::SNSService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

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

  std::unordered_map<std::string, std::unordered_map<std::string, std::time_t>> follow_time;

  Status Follow(ServerContext *context, const Request *request, Reply *reply) override
  {

    /*********
    YOUR CODE HERE
    **********/
    std::string follower_name = request->username();   // 关注者
    std::string followee_name = request->arguments(0); // 被关注者

    Client *follower = nullptr;
    Client *followee = nullptr;

    // 在客户端数据库 `client_db` 中查找关注者和被关注者
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

    // 如果被关注的用户不存在，返回失败
    if (followee == nullptr)
    {
      reply->set_msg("User does not exist.");
      return Status::OK;
    }

    for (const auto &following : follower->client_following)
    {
      if (following->username == followee_name)
      {
        reply->set_msg("Already following user.");
        return Status::OK;
      }
    }

    // 更新关注关系
    follower->client_following.push_back(followee);
    followee->client_followers.push_back(follower); // 确保 `LIST` 命令可以正确返回粉丝

    follow_time[follower_name][followee_name] = std::time(nullptr);

    reply->set_msg("Followed successfully.");

    return Status::OK;
  }

  Status UnFollow(ServerContext *context, const Request *request, Reply *reply) override
  {

    /*********
    YOUR CODE HERE
    **********/
    std::string username = request->username();
    std::string unfollow_username = request->arguments(0);

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

    if (!user || !unfollow_user)
    {
      reply->set_msg("User not found");
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
      reply->set_msg("Unfollow successful");
    }
    else
    {
      reply->set_msg("Not following user");
    }

    return Status::OK;
  }

  // RPC Login
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
        reply->set_msg("User already exists");
        return Status::OK;
      }
    }

    // if user does not exist in the client_db, add it into client_db
    Client *new_client = new Client();
    new_client->username = username;
    new_client->connected = true;
    client_db.push_back(new_client);

    std::cout << "Login successful" << std::endl;
    reply->set_msg("Login successful");
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

    // 读取客户端的初始消息，获取用户名
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
    std::vector<std::string> last_20_posts;
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
          last_20_posts.push_back(post);
          post.clear();
          count++;
          if (count >= 20)
            break;
        }
        else
        {
          post += line + "\n";
        }
      }
    }

    // send the last posts to user
    for (const std::string &post : last_20_posts)
    {
      // parse post recode
      // T <timestamp>
      // U <username>
      // W <post contain>
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
        Message message;
        message.set_username(post_username);
        message.set_msg(post_content);
        message.mutable_timestamp()->set_seconds(post_timestamp);
        message.mutable_timestamp()->set_nanos(0);
        stream->Write(message);
      }
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

    // 退出 TIMELINE 模式
    client->stream = nullptr;

    return Status::OK;
  }
};

void RunServer(std::string port_no)
{
  std::string server_address = "0.0.0.0:" + port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on " + server_address);

  server->Wait();
}

int main(int argc, char **argv)
{

  std::string port = "3010";

  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1)
  {
    switch (opt)
    {
    case 'p':
      port = optarg;
      break;
    default:
      std::cerr << "Invalid Command Line Argument\n";
    }
  }

  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(port);

  return 0;
}
