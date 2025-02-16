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
#include <sys/stat.h>  // 解决 stat 结构体未定义
#include <memory>      // 解决 shared_ptr 问题
#include <thread>      // 解决 std::thread 相关问题
#include <grpcpp/grpcpp.h>  // 解决 gRPC ClientWriter<Message> 问题
#include "sns.grpc.pb.h"  // 确保包含 gRPC 生成的头文件

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

  Status SNSServiceImpl::Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {
      Message init_msg;
      if (!stream->Read(&init_msg)) {
          return Status::CANCELLED;
      }

      std::string username = init_msg.username();
      std::string timeline_file = "./timelines/" + username + ".txt";

      LOG(INFO) << "[Server] Client " << username << " entered Timeline mode.";

      if (!std::filesystem::exists("./timelines")) {
          std::filesystem::create_directory("./timelines");
      }

      // 获取 `Client` 并存储 `stream`
      Client* client = nullptr;
      for (Client* c : client_db) {
          if (c->username == username) {
              client = c;
              break;
          }
      }
      if (!client) {
          return Status::CANCELLED;
      }
      client->stream = stream;

      // **发送最新 20 条 `Post`**
      SendLatestPosts(stream, timeline_file);

      // **启动 `MonitorTimeline` 线程**
      std::thread monitor_thread([stream, timeline_file]() {
          MonitorTimeline(stream, timeline_file);
      });

      // **监听 `Client` 发送的 `Post` 并写入 `Timeline` 文件**
      Message client_msg;
      while (stream->Read(&client_msg)) {
          std::ofstream file(timeline_file, std::ios::app);
          if (!file) {
              LOG(ERROR) << "[Server] Failed to open timeline file: " << timeline_file;
              return Status::CANCELLED;
          }

          std::string formatted_post = FormatPost(client_msg);
          file << formatted_post;
          file.close();

          LOG(INFO) << "[Server] New post from " << client_msg.username() << ": " << client_msg.msg();

          // **发送 `Post` 给 `Followers`**
          for (Client* follower : client->client_followers) {
              if (follower->stream) {
                  follower->stream->Write(client_msg);
              }
          }
      }

      // **`Client` 退出 `Timeline`，清空 `stream`**
      client->stream = nullptr;
      monitor_thread.join();
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
  void HeartbeatThread(std::string server_id, std::string coordinator_ip, std::string coordinator_port, std::string port) {
      while (true) {
          grpc::ClientContext context;
          csce438::ServerInfo request;
          csce438::Confirmation response;

          request.set_serverid(std::stoi(server_id));
          request.set_hostname("127.0.0.1");
          request.set_port(port);

          std::unique_ptr<csce438::CoordService::Stub> stub = 
              csce438::CoordService::NewStub(grpc::CreateChannel(
                  coordinator_ip + ":" + coordinator_port, grpc::InsecureChannelCredentials()));

          grpc::Status status = stub->Heartbeat(&context, request, &response);

          if (!status.ok()) {
              std::cerr << "[TSD] Heartbeat failed: " << status.error_message() << std::endl;
          }

          sleep(5);
      }
  }

}
void MonitorTimeline(std::string timeline_file, std::shared_ptr<ClientWriter<Message>> stream) {
    struct stat fileStat;
    while (true) {
        if (stat(timeline_file.c_str(), &fileStat) == 0) {
            double diff = difftime(getTimeNow(), fileStat.st_mtime);
            if (diff < 30) {
                // 读取最新 20 条数据并发送给客户端
                SendLatestPosts(stream);
            }
        }
        sleep(5);
    }
}


int main(int argc, char** argv) {
    std::string cluster_id, server_id, coordinator_ip, coordinator_port, port;
    
    int opt = 0;
    while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1) {
        switch (opt) {
            case 'c': cluster_id = optarg; break;
            case 's': server_id = optarg; break;
            case 'h': coordinator_ip = optarg; break;
            case 'k': coordinator_port = optarg; break;
            case 'p': port = optarg; break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
                return -1;
        }
    }

    // ✅ 确保所有参数都被正确解析
    if (cluster_id.empty() || server_id.empty() || coordinator_ip.empty() || coordinator_port.empty() || port.empty()) {
        std::cerr << "Usage: ./tsd -c <cluster_id> -s <server_id> -h <coordinator_ip> -k <coordinator_port> -p <port>\n";
        return -1;
    }

    std::cout << "Server initialized with:\n"
              << "Cluster ID: " << cluster_id << "\n"
              << "Server ID: " << server_id << "\n"
              << "Coordinator IP: " << coordinator_ip << "\n"
              << "Coordinator Port: " << coordinator_port << "\n"
              << "Port: " << port << std::endl;

    // **向 Coordinator 发送心跳**
    std::thread hb_thread(HeartbeatThread, server_id, coordinator_ip, coordinator_port, port);
    hb_thread.detach();  // 确保线程不会阻塞主线程

    RunServer(port);
    return 0;
}

