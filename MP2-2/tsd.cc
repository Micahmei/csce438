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

#include <thread>
#include <fstream>
#include <filesystem>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
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
using csce438::CoordService;
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

// Helper function to convert filepath to sem_name
std::string get_sem_name_from_filepath(const std::string &filepath)
{
  std::string sem_name = filepath;

  // If the filepath starts with './', remove it
  if (sem_name.rfind("./", 0) == 0)
  {
    sem_name = sem_name.substr(2); // Remove './'
  }

  // Replace all '/' with '_' to make it valid for sem_name
  for (char &c : sem_name)
  {
    if (c == '/')
    {
      c = '_'; // Replace '/' with '_'
    }
  }

  // Add a leading '/' to make it a valid sem_name
  sem_name = "/" + sem_name;

  return sem_name;
}

std::vector<std::string> read_users_from_file(const std::string &filename)
{
  std::vector<std::string> users;
  std::ifstream file(filename);
  if (!file)
  {
    return users;
  }

  std::string semName = get_sem_name_from_filepath(filename);
  sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);
  if (fileSem == SEM_FAILED)
  {
    perror("sem_open");
    return users;
  }

  if (file.peek() == std::ifstream::traits_type::eof())
  {
    sem_close(fileSem);
    return users;
  }

  std::string user;
  while (std::getline(file, user))
  {
    if (!user.empty())
    {
      users.push_back(user);
    }
  }

  sem_close(fileSem);
  return users;
}

bool write_new_user(const std::string &filename, const std::string &new_user)
{
  std::string semName = get_sem_name_from_filepath(filename);
  sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0644, 1);
  if (fileSem == SEM_FAILED)
  {
    perror("sem_open");
    return false;
  }

  sem_wait(fileSem);

  std::ofstream file(filename, std::ios::app);
  if (!file)
  {
    std::cerr << "Failed to open file for writing.\n";
    sem_post(fileSem);
    sem_close(fileSem);
    return false;
  }

  // 写入新用户到文件
  file << new_user << std::endl;

  // 关闭文件和信号量
  file.close();
  sem_post(fileSem);
  sem_close(fileSem);

  return true;
}

void write_follow_time(const std::string &filename, const std::string &follower, const std::string &followee, std::time_t timestamp)
{
  std::string sem_name = get_sem_name_from_filepath(filename);
  sem_t *file_sem = sem_open(sem_name.c_str(), O_CREAT, 0644, 1);
  if (file_sem == SEM_FAILED)
  {
    perror("sem_open");
    return;
  }

  sem_wait(file_sem);

  std::ofstream ofs(filename, std::ios::app);
  if (ofs.is_open())
  {
    ofs << follower << " " << followee << " " << timestamp << "\n";
  }
  else
  {
    std::cerr << "Failed to write to " << filename << "\n";
  }

  sem_post(file_sem);
  sem_close(file_sem);
}

std::unordered_map<std::string, std::unordered_map<std::string, std::time_t>> read_follow_time(const std::string &filename)
{
  std::unordered_map<std::string, std::unordered_map<std::string, std::time_t>> follow_times;

  // 获取信号量名称并打开信号量
  std::string sem_name = get_sem_name_from_filepath(filename);
  sem_t *file_sem = sem_open(sem_name.c_str(), O_CREAT, 0644, 1);
  if (file_sem == SEM_FAILED)
  {
    perror("sem_open");
    return follow_times;
  }

  // 使用信号量加锁
  sem_wait(file_sem);

  // 打开文件
  std::ifstream file(filename);
  if (!file)
  {
    std::cerr << "Failed to open file for reading.\n";
    sem_post(file_sem); // 释放信号量
    sem_close(file_sem);
    return follow_times;
  }

  std::string line;
  while (std::getline(file, line))
  {
    // 解析文件行，假设每行有 3 个部分：follower, followee, timestamp
    std::string f, fe, ts_str;
    std::time_t ts;

    // 使用 ',' 作为分隔符，分割字符串
    size_t first_comma = line.find(',');
    size_t second_comma = line.find(',', first_comma + 1);

    if (first_comma != std::string::npos && second_comma != std::string::npos)
    {
      f = line.substr(0, first_comma);
      fe = line.substr(first_comma + 1, second_comma - first_comma - 1);
      ts_str = line.substr(second_comma + 1);

      // 将时间戳字符串转换为时间类型
      ts = std::stoll(ts_str); // 假设时间戳是以秒为单位的数字

      // 将数据插入到 unordered_map
      follow_times[f][fe] = ts;
    }
  }

  // 关闭文件和信号量
  file.close();
  sem_post(file_sem); // 释放信号量
  sem_close(file_sem);

  return follow_times;
}

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

class TimelineHandler
{
private:
  std::string username;
  ServerReaderWriter<Message, Message> *stream;
  std::vector<std::string> following_users;
  std::atomic<bool> stop_thread; // 控制线程停止
  std::string data_dir;
  std::string follow_time_file;

public:
  TimelineHandler(const std::string &user, ServerReaderWriter<Message, Message> *stream_,
                  const std::vector<std::string> &following_users_, const std::string &data_dir_, const std::string &follow_time_file_)
      : username(user), stream(stream_), following_users(following_users_), data_dir(data_dir_), follow_time_file(follow_time_file_), stop_thread(false) {}

  // 启动定时读取线程
  void start()
  {
    std::thread(&TimelineHandler::readFollowingUsersTimeline, this).detach();
  }

  // 停止线程
  void stop()
  {
    stop_thread = true;
  }

  // 读取关注者的 timeline.txt 文件并推送新帖子
  void readFollowingUsersTimeline()
  {
    while (!stop_thread)
    {
      // Step 1: 读取用户的 follow_time.txt 文件
      auto follow_timestamp_map = read_follow_time(follow_time_file);

      // Step 2: 读取关注用户的 timeline.txt，筛选出新帖子
      std::vector<Post> new_posts;
      for (const auto &followed_user : following_users)
      {
        std::string followed_timeline = data_dir + "/" + followed_user + "_timeline.txt";
        std::ifstream file(followed_timeline);
        std::string line, post;
        while (std::getline(file, line))
        {
          if (line.empty() && !post.empty())
          {
            std::istringstream post_stream(post);
            std::string post_username, post_content;
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

            // Step 3: 过滤用户关注后发布的帖子
            std::time_t follow_timestamp = follow_timestamp_map[username][post_username];
            if (post_timestamp >= follow_timestamp)
            {
              new_posts.push_back({post_username, post_content, post_timestamp});
            }
            post.clear();
          }
          else
          {
            post += line + "\n";
          }
        }
      }

      // Step 4: 推送新帖子到本用户的 stream
      for (const auto &new_post : new_posts)
      {
        Message message;
        message.set_username(new_post.username);
        message.set_msg(new_post.post);
        message.mutable_timestamp()->set_seconds(new_post.timestamp);
        message.mutable_timestamp()->set_nanos(0);
        stream->Write(message);
      }

      // Step 5: 等待一段时间再执行下一次检查（例如每 5 秒检查一次）
      std::this_thread::sleep_for(std::chrono::seconds(5));
    }
  }
};

// Vector that stores every client that has been created
std::vector<Client *> client_db;

class SNSServiceImpl final : public SNSService::Service
{
  std::unordered_map<std::string, std::unordered_map<std::string, std::time_t>> follow_time;
  std::unique_ptr<CoordService::Stub> coordinator_stub;

public:
  uint64_t cluster_id;
  uint64_t server_id;
  std::string data_dir;

  SNSServiceImpl(std::shared_ptr<Channel> channel) : coordinator_stub(CoordService::NewStub(channel)) {}

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

    auto all_users = read_users_from_file(data_dir + "/all_uers.txt");
    for (const auto &usrname : all_users)
    {
      list_reply->add_all_users(username);
    }

    auto followers = read_users_from_file(data_dir + "/" + username + "_follower_list.txt");
    for (const auto &follower : followers)
    {
      list_reply->add_followers(follower);
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

    auto all_users = read_users_from_file(data_dir + "/all_users.txt");
    bool followee_exist = false;
    for (auto &user : all_users)
    {
      if (user == followee_name)
      {
        followee_exist = true;
      }
    }

    if (!followee_exist)
    {
      reply->set_msg("followed user no exist");
      return Status::OK;
    }

    auto followers = read_users_from_file(data_dir + "/" + follower_name + "_following_list.txt");
    for (const auto &following : followers)
    {
      if (following == followee_name)
      {
        reply->set_msg("already following user");
        return Status::OK;
      }
    }

    write_new_user(data_dir + "/" + follower_name + "_following_list.txt", followee_name);
    write_new_user(data_dir + "/" + followee_name + "_follower_list.txt", follower_name);

    write_follow_time(data_dir + "/follow_time.txt", follower_name, followee_name, std::time(nullptr));

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

    // for (Client *c : client_db)
    // {
    //   if (c->username == username)
    //   {
    //     std::cout << "Login successful" << std::endl;
    //     reply->set_msg("login successful");
    //     return Status::OK;
    //   }
    // }

    // std::string users_file = data_dir + "/all_users.txt";
    // auto all_users = read_users_from_file(users_file);

    // bool is_new = true;
    // for (auto user : all_users)
    // {
    //   std::cout << user << std::endl;
    //   if (user == username)
    //   {
    //     is_new = false;
    //   }
    // }

    // if (is_new)
    // {
    //   write_new_user(users_file, username);
    // }

    Client *client;
    client->username = username;
    client_db.push_back(client);

    std::cout << "Login successful" << std::endl;
    reply->set_msg("login successful");
    return Status::OK;
  }

  Status Timeline(ServerContext *context,
                  ServerReaderWriter<Message, Message> *stream) override
  {
    printf("0");
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

    std::string timeline_file = data_dir + "/" + username + "_timeline.txt";

    auto all_users = read_users_from_file(data_dir + "/all_users.txt");

    printf("1");

    Client *client;
    bool user_exist = false;
    for (auto user : all_users)
    {
      if (user == username)
        user_exist = true;
    }
    for (auto c : client_db)
    {
      if (c->username == username)
      {
        user_exist = true;
        client = c;
      }
    }
    if (!user_exist)
    {
      return Status::CANCELLED;
    }

    std::string follow_time_file = data_dir + "/follow_time.txt";
    auto follow_timestamp_map = read_follow_time(follow_time_file);

    printf("2");

    auto following_users = read_users_from_file(data_dir + "/" + username + "_following_list.txt");
    auto follower_users = read_users_from_file(data_dir + "/" + username + "_follower_list.txt");

    printf("3");

    // read the posts of the followings of the user
    std::vector<Post> history_posts;
    for (auto followed_user : following_users)
    {
      std::string followed_timeline = data_dir + "/" + followed_user + "_timeline.txt";
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

          std::time_t follow_timestamp = follow_timestamp_map[username][post_username];
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

    TimelineHandler timeline_handler(username, stream, following_users, data_dir, follow_time_file);

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

  service.cluster_id = cluster_id;
  service.server_id = server_id;

  service.data_dir = "./cluster_" + std::to_string(cluster_id) + "/" + std::to_string(server_id);
  if (!std::filesystem::exists("./cluster_" + std::to_string(cluster_id)))
  {
    std::filesystem::create_directory("./cluster_" + std::to_string(cluster_id));
    if (!std::filesystem::exists(service.data_dir))
      std::filesystem::create_directory(service.data_dir);
  }

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
