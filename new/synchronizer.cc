// NOTE: This starter code contains a primitive implementation using the default RabbitMQ protocol.
// You are recommended to look into how to make the communication more efficient,
// for example, modifying the type of exchange that publishes to one or more queues, or
// throttling how often a process consumes messages from a queue so other consumers are not starved for messages
// All the functions in this implementation are just suggestions and you can make reasonable changes as long as
// you continue to use the communication methods that the assignment requires between different processes

#include <bits/fs_fwd.h>
#include <ctime>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <semaphore.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#define GLOG_EXPORT
#define GLOG_NO_EXPORT
#include <glog/logging.h>
#include "sns.grpc.pb.h"
#include "sns.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"
#include "sem_file.h"

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <jsoncpp/json/json.h>

#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

namespace fs = std::filesystem;

using csce438::AllUsers;
using csce438::Confirmation;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using csce438::ServerList;
using csce438::SynchronizerListReply;
using csce438::SynchService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
// tl = timeline, fl = follow list
using csce438::TLFL;

int synchID = 1;
int clusterID = 1;
bool isMaster = false;
int total_number_of_registered_synchronizers = 0; // update this by asking coordinator
std::string coordAddr;
std::string clusterSubdirectory;
std::string data_dir;
std::vector<std::string> otherHosts;
std::unordered_map<std::string, int> timelineLengths;

std::vector<std::string> get_lines_from_file(std::string);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);
std::vector<std::string> getFollowersOfUser(int);
bool file_contains_user(std::string filename, std::string user);

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID);

std::unique_ptr<csce438::CoordService::Stub> coordinator_stub_;

class SynchronizerRabbitMQ
{
private:
  amqp_connection_state_t conn;
  amqp_channel_t channel;
  std::string hostname;
  int port;
  int synchID;
  std::map<std::string, int> channelMap;

  std::string extractTypeFromQueue(const std::string &queueName)
  {
    // 例如 "synch1_follower_queue" 提取出 "follower"
    size_t first = queueName.find('_');
    size_t second = queueName.find('_', first + 1);
    if (first != std::string::npos && second != std::string::npos)
    {
      return queueName.substr(first + 1, second - first - 1);
    }
    return "";
  }

  bool setupRabbitMQ()
  {
    conn = amqp_new_connection();
    if (!conn)
    {
      std::cerr << "Error: Failed to create a new RabbitMQ connection!" << std::endl;
      return false;
    }
    amqp_socket_t *socket = amqp_tcp_socket_new(conn);
    if (!socket)
    {
      std::cerr << "Error: Failed to create a new socket!" << std::endl;
      return false;
    }
    int status = amqp_socket_open(socket, hostname.c_str(), port);
    if (status != AMQP_STATUS_OK)
    {
      std::cerr << "Error: Failed to open TCP socket to " << hostname << ":" << port << std::endl;
      return false;
    }
    std::cout << "Connected to RabbitMQ at " << hostname << ":" << port << std::endl;
    amqp_rpc_reply_t login_res = amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    if (login_res.reply_type != AMQP_RESPONSE_NORMAL)
    {
      std::cerr << "Error: Failed to log in to RabbitMQ server!" << std::endl;
      return false;
    }
    std::cout << "Logged in to RabbitMQ successfully!" << std::endl;

    channelMap["users"] = 1;
    channelMap["follower"] = 2;
    channelMap["following"] = 3;
    channelMap["followtime"] = 4;
    channelMap["timeline"] = 5;

    for (auto &[name, ch] : channelMap)
    {
      amqp_channel_open_ok_t *open_channel_res = amqp_channel_open(conn, ch);
      if (!open_channel_res)
      {
        std::cerr << "Error: Failed to open channel " << ch << " for " << name << "!" << std::endl;
        return false;
      }
      std::cout << "Channel " << ch << " opened successfully for " << name << "!" << std::endl;
    }

    return true;
  }

  void declareQueue(const std::string &queueName)
  {
    std::string key = extractTypeFromQueue(queueName);
    int ch = channelMap[key];
    amqp_queue_declare(conn, ch, amqp_cstring_bytes(queueName.c_str()),
                       0, 0, 0, 0, amqp_empty_table);
  }

  void publishMessage(const std::string &queueName, const std::string &message)
  {
    std::string key = extractTypeFromQueue(queueName);
    int ch = channelMap[key];
    amqp_bytes_t queue = amqp_cstring_bytes(queueName.c_str());
    amqp_bytes_t payload = amqp_cstring_bytes(message.c_str());
    int status = amqp_basic_publish(conn, ch, amqp_empty_bytes, queue, 0, 0, NULL, payload);
    if (status != AMQP_STATUS_OK)
    {
      std::cerr << "amqp_basic_publish failed: "
                << amqp_error_string2(status) << std::endl;
      if (!reconnectRabbitMQ())
      {
        return;
      }
      status = amqp_basic_publish(conn, ch, amqp_empty_bytes, queue, 0, 0, NULL, payload);
      if (status != AMQP_STATUS_OK)
      {
        std::cerr << "[Synchronizer " << synchID << "] Retry publish failed: " << amqp_error_string2(status) << "\n";
        return;
      }
    }
    std::cout << "[PUBLISH] queue: " << queueName << ", message: " << message << std::endl;
  }

  std::string consumeMessage(const std::string &queueName, int timeout_ms = 5000)
  {
    std::string key = extractTypeFromQueue(queueName);
    int ch = channelMap[key];
    amqp_basic_consume(conn, ch,
                       amqp_cstring_bytes(queueName.c_str()),
                       amqp_cstring_bytes((queueName + "_consumer").c_str()),
                       0, 1, 0, amqp_empty_table);

    amqp_rpc_reply_t consume_reply = amqp_get_rpc_reply(conn);
    if (consume_reply.reply_type != AMQP_RESPONSE_NORMAL)
      return "";

    amqp_envelope_t envelope;
    amqp_maybe_release_buffers(conn);

    struct timeval timeout;
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;

    while (true)
    {
      amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);
      if (res.reply_type != AMQP_RESPONSE_NORMAL)
        return "";

      std::string envelope_tag((char *)envelope.consumer_tag.bytes, envelope.consumer_tag.len);
      if (envelope_tag != (queueName + "_consumer"))
      {
        amqp_destroy_envelope(&envelope);
        continue; // 丢弃不属于此同步器的消息
      }

      std::string message((char *)envelope.message.body.bytes, envelope.message.body.len);
      amqp_destroy_envelope(&envelope);
      std::cout << "[GET] queue: " << queueName << ", message: " << message << std::endl;
      return message;
    }
    // amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);

    // if (res.reply_type != AMQP_RESPONSE_NORMAL)
    // {
    //   switch (res.reply_type)
    //   {
    //   case AMQP_RESPONSE_NONE:
    //     std::cerr << "Error: No response from broker." << std::endl;
    //     break;
    //   case AMQP_RESPONSE_LIBRARY_EXCEPTION:
    //     std::cerr << "Library exception: " << amqp_error_string2(res.library_error) << std::endl;
    //     break;
    //   case AMQP_RESPONSE_SERVER_EXCEPTION:
    //     if (res.reply.id == AMQP_CHANNEL_CLOSE_METHOD)
    //     {
    //       amqp_channel_close_t *m = (amqp_channel_close_t *)res.reply.decoded;
    //       std::cerr << "Server channel error: " << m->reply_code << " - "
    //                 << std::string((char *)m->reply_text.bytes, m->reply_text.len) << std::endl;
    //     }
    //     else if (res.reply.id == AMQP_CONNECTION_CLOSE_METHOD)
    //     {
    //       amqp_connection_close_t *m = (amqp_connection_close_t *)res.reply.decoded;
    //       std::cerr << "Server connection error: " << m->reply_code << " - "
    //                 << std::string((char *)m->reply_text.bytes, m->reply_text.len) << std::endl;
    //     }
    //     else
    //     {
    //       std::cerr << "Unknown server error." << std::endl;
    //     }
    //     break;
    //   }

    //   return "";
    // }
    // std::string tag((char *)envelope.consumer_tag.bytes, envelope.consumer_tag.len);
    // if (tag != (queueName + "_consumer"))
    // {
    //   std::cerr << "tag: " << queueName + "_consumer" << " wrong tag: " << tag << std::endl;
    //   amqp_destroy_envelope(&envelope);
    //   return "";
    // }

    // std::string message(static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len);
    // amqp_destroy_envelope(&envelope);
    // return message;
  }

  bool reconnectRabbitMQ(int max_retries = 3, int retry_interval_ms = 1000)
  {
    for (int attempt = 1; attempt <= max_retries; ++attempt)
    {
      std::cout << "[Synchronizer " << synchID << "] Attempting to connect to RabbitMQ... (" << attempt << "/" << max_retries << ")\n";
      if (setupRabbitMQ())
      {
        std::cout << "[Synchronizer " << synchID << "] Connected to RabbitMQ.\n";
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_ms));
    }
    std::cerr << "[Synchronizer " << synchID << "] Failed to reconnect to RabbitMQ after " << max_retries << " attempts.\n";
    return false;
  }

public:
  // SynchronizerRabbitMQ(const std::string &host, int p, int id) : hostname(host), port(p), channel(1), synchID(id)
  SynchronizerRabbitMQ(const std::string &host, int p, int id) : hostname(host), port(p), channel(1), synchID(id)
  {
    setupRabbitMQ();
    declareQueue("synch" + std::to_string(synchID) + "_users_queue");
    declareQueue("synch" + std::to_string(synchID) + "_follower_queue");
    declareQueue("synch" + std::to_string(synchID) + "_following_queue");
    declareQueue("synch" + std::to_string(synchID) + "_followtime_queue");
    declareQueue("synch" + std::to_string(synchID) + "_timeline_queue");
    // TODO: add or modify what kind of queues exist in your clusters based on your needs
  }

  void publishUserList()
  {
    std::vector<std::string> users = get_all_users_func(synchID);
    std::sort(users.begin(), users.end());
    Json::Value userList;
    for (const auto &user : users)
    {
      userList["users"].append(user);
    }
    Json::FastWriter writer;
    std::string message = writer.write(userList);
    publishMessage("synch" + std::to_string(synchID) + "_users_queue", message);
  }

  void consumeUserLists()
  {
    std::vector<std::string> allUsers;
    // YOUR CODE HERE

    // TODO: while the number of synchronizers is harcorded as 6 right now, you need to change this
    // to use the correct number of follower synchronizers that exist overall
    // accomplish this by making a gRPC request to the coordinator asking for the list of all follower synchronizers registered with it
    for (int i = 1; i <= total_number_of_registered_synchronizers; i++)
    {
      if (i == synchID)
        continue;
      std::string queueName = "synch" + std::to_string(i) + "_users_queue";
      std::string message = consumeMessage(queueName, 1000); // 1 second timeout
      // std::cout << queueName << ": " << message << std::endl;
      if (!message.empty())
      {
        Json::Value root;
        Json::Reader reader;
        if (reader.parse(message, root))
        {
          for (const auto &user : root["users"])
          {
            allUsers.push_back(user.asString());
          }
        }
      }
    }
    for (auto user : allUsers)
    {
      bool update = write_new_user(data_dir + "/all_users.txt", user);
    }
  }

  void publishTimestamp()
  {
    std::unordered_map<std::string, std::unordered_map<std::string, std::time_t>> follow_times_0 = read_follow_time(data_dir + "/follow_time.txt");

    Json::Value follow_times;
    for (const auto &user : follow_times_0)
    {
      Json::Value followee_map;
      for (const auto &followee : user.second)
      {
        followee_map[followee.first] = static_cast<Json::Int64>(followee.second);
      }
      follow_times[user.first] = followee_map;
    }

    Json::FastWriter writer;
    std::string message = writer.write(follow_times);

    publishMessage("synch" + std::to_string(synchID) + "_followtime_queue", message);
    // std::cout << "PUT synch" + std::to_string(synchID) + "_followtime_queue " << message << std::endl;
  }

  void consumeTimestamp()
  {
    std::unordered_map<std::string, std::unordered_map<std::string, std::time_t>> all_follow_times;

    // 从每个同步器的消息队列消费 follow_time 数据
    for (int i = 1; i <= total_number_of_registered_synchronizers; i++)
    {
      if (i == synchID)
        continue;
      std::string queueName = "synch" + std::to_string(i) + "_followtime_queue";
      std::string message = consumeMessage(queueName, 1000);
      // std::cout << "GET synch" + std::to_string(i) + "_followtime_queue " << message << std::endl;
      if (!message.empty())
      {
        Json::Value root;
        Json::Reader reader;
        if (reader.parse(message, root))
        {
          // 解析每个同步器的 follow_time 数据
          for (const auto &user : root.getMemberNames())
          {
            for (const auto &followee : root[user].getMemberNames())
            {
              // 获取时间戳
              std::time_t timestamp = root[user][followee].asInt64();

              // 将数据添加到 all_follow_times 中
              all_follow_times[user][followee] = timestamp;
            }
          }
        }
      }
    }

    // 将合并后的数据写入文件
    for (const auto &user : all_follow_times)
    {
      for (const auto &followee : user.second)
      {
        write_follow_time(data_dir + "/follow_time.txt", user.first, followee.first, followee.second);
      }
    }
  }

  void publishFollowers()
  {
    Json::Value relations;
    std::vector<std::string> users = get_all_users_func(synchID);

    for (const auto &client : users)
    {
      int clientId = std::stoi(client);
      std::vector<std::string> followers = read_users_from_file(data_dir + "/" + client + "_follower_list.txt");
      if (!followers.empty())
      {
        Json::Value followerList(Json::arrayValue);
        for (const auto &follower : followers)
        {
          followerList.append(follower);
        }
        relations[client] = followerList;
      }
    }

    Json::FastWriter writer;
    std::string message = writer.write(relations);
    // std::cout << "synch" + std::to_string(synchID) + "_follower_queue<< " << message << std::endl;
    publishMessage("synch" + std::to_string(synchID) + "_follower_queue", message);
  }

  void consumeFollowers()
  {
    std::vector<std::string> allUsers = get_all_users_func(synchID);

    // YOUR CODE HERE

    // TODO: hardcoding 6 here, but you need to get list of all synchronizers from coordinator as before
    for (int i = 1; i <= total_number_of_registered_synchronizers; i++)
    {
      if (i == synchID)
        continue;
      std::string queueName = "synch" + std::to_string(i) + "_follower_queue";
      std::string message = consumeMessage(queueName, 1000); // 1 second timeout
      // std::cout << "synch" + std::to_string(i) + "_follower_queue>> " << message << std::endl;
      if (!message.empty())
      {
        Json::Value root;
        Json::Reader reader;
        if (reader.parse(message, root))
        {
          for (const auto &user : root.getMemberNames())
          {
            for (const auto &follower : root[user])
            {
              write_new_user(data_dir + "/" + user + "_follower_list.txt", follower.asString());
            }
          }
        }
      }
    }
  }

  void publishFollowings()
  {
    Json::Value relations;
    std::vector<std::string> users = get_all_users_func(synchID);

    for (const auto &client : users)
    {
      int clientId = std::stoi(client);
      std::vector<std::string> followers = read_users_from_file(data_dir + "/" + client + "_following_list.txt");

      Json::Value followerList(Json::arrayValue);
      for (const auto &follower : followers)
      {
        followerList.append(follower);
      }

      if (!followerList.empty())
      {
        relations[client] = followerList;
      }
    }

    Json::FastWriter writer;
    std::string message = writer.write(relations);
    publishMessage("synch" + std::to_string(synchID) + "_following_queue", message);
  }

  void consumeFollowings()
  {
    std::vector<std::string> allUsers = get_all_users_func(synchID);

    // YOUR CODE HERE

    // TODO: hardcoding 6 here, but you need to get list of all synchronizers from coordinator as before
    for (int i = 1; i <= total_number_of_registered_synchronizers; i++)
    {
      if (i == synchID)
        continue;
      std::string queueName = "synch" + std::to_string(i) + "_following_queue";
      std::string message = consumeMessage(queueName, 1000); // 1 second timeout

      if (!message.empty())
      {
        Json::Value root;
        Json::Reader reader;
        if (reader.parse(message, root))
        {
          for (const auto &client : root.getMemberNames())
          {
            for (const auto &follower : root[client])
            {
              write_new_user(data_dir + "/" + client + "_following_list.txt", follower.asString());
            }
          }
        }
      }
    }
  }

  // for every client in your cluster, update all their followers' timeline files
  // by publishing your user's timeline file (or just the new updates in them)
  //  periodically to the message queue of the synchronizer responsible for that client
  void publishTimelines()
  {
    std::vector<std::string> users = get_all_users_func(synchID);

    Json::Value msg;
    for (const auto &client : users)
    {
      auto timeline = read_timeline_from_file(data_dir + "/" + client + "_timeline.txt");
      if (!timeline.empty())
      {
        Json::Value post_list(Json::arrayValue);
        for (auto &post : timeline)
        {

          Json::Value post_json;
          post_json["username"] = post.username;
          post_json["timestamp"] = static_cast<Json::Int64>(post.timestamp);
          post_json["post"] = post.post;

          post_list.append(post_json);
        }
        msg[client] = post_list;
      }
    }
    Json::FastWriter writer;
    std::string message = writer.write(msg);
    publishMessage("synch" + std::to_string(synchID) + "_timeline_queue", message);
    // std::cout << "PUT synch" + std::to_string(synchID) + "_timeline_queue " << message.size() << std::endl;
  }

  // For each client in your cluster, consume messages from your timeline queue and modify your client's timeline files based on what the users they follow posted to their timeline
  void consumeTimelines()
  {
    for (int i = 1; i <= total_number_of_registered_synchronizers; i++)
    {
      if (i == synchID)
        continue;
      std::string queueName = "synch" + std::to_string(i) + "_timeline_queue";
      std::string message = consumeMessage(queueName, 1000); // 1 second timeout
      // std::cout << "GET synch" + std::to_string(i) + "_timeline_queue " << message.size() << std::endl;
      if (!message.empty())
      {
        Json::Value root;
        Json::Reader reader;
        if (reader.parse(message, root))
        {
          for (auto &user : root.getMemberNames())
          {
            for (auto &post_json : root[user])
            {
              Post post = {post_json["username"].asString(), post_json["post"].asString(), post_json["timestamp"].asInt64()};
              write_timeline_to_file(data_dir + "/" + user + "_timeline.txt", post);
            }
          }
        }
      }
    }
  }

private:
  void updateAllUsersFile(const std::vector<std::string> &users)
  {

    std::string usersFile = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/all_users.txt";
    std::string semName = get_sem_name_from_filepath(usersFile);
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);

    std::ofstream userStream(usersFile, std::ios::app | std::ios::out | std::ios::in);
    for (std::string user : users)
    {
      if (!file_contains_user(usersFile, user))
      {
        userStream << user << std::endl;
      }
    }
    sem_close(fileSem);
  }
};

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ);

class SynchServiceImpl final : public SynchService::Service
{
  // You do not need to modify this in any way
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID)
{
  // localhost = 127.0.0.1
  std::string server_address("127.0.0.1:" + port_no);
  log(INFO, "Starting synchronizer server at " + server_address);
  SynchServiceImpl service;
  // grpc::EnableDefaultHealthCheckService(true);
  // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Initialize RabbitMQ connection
  // SynchronizerRabbitMQ rabbitMQ("localhost", 5672, synchID);
  SynchronizerRabbitMQ rabbitMQ("rabbitmq_container", 5672, synchID);

  std::thread t1(run_synchronizer, coordIP, coordPort, port_no, synchID, std::ref(rabbitMQ));

  // Create a consumer thread
  std::thread consumerThread(
      [&rabbitMQ]()
      {
        while (true)
        {
          rabbitMQ.consumeUserLists();
          rabbitMQ.consumeFollowers();
          rabbitMQ.consumeFollowings();
          rabbitMQ.consumeTimelines();
          rabbitMQ.consumeTimestamp();
          std::this_thread::sleep_for(std::chrono::seconds(2));
          // you can modify this sleep period as per your choice
        }
      });

  server->Wait();

  t1.join();
  consumerThread.join();
}

int main(int argc, char **argv)
{
  int opt = 0;
  std::string coordIP;
  std::string coordPort;
  std::string port = "3029";

  while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1)
  {
    switch (opt)
    {
    case 'h':
      coordIP = optarg;
      break;
    case 'k':
      coordPort = optarg;
      break;
    case 'p':
      port = optarg;
      break;
    case 'i':
      synchID = std::stoi(optarg);
      break;
    default:
      std::cerr << "Invalid Command Line Argument\n";
    }
  }

  std::string log_file_name = std::string("synchronizer-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");

  coordAddr = coordIP + ":" + coordPort;
  clusterID = ((synchID - 1) % 3) + 1;
  clusterSubdirectory = std::to_string((synchID - 1) % 3 + 1 - clusterID + 1);

  data_dir = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory;

  ServerInfo serverInfo;
  serverInfo.set_server_ip("localhost");
  serverInfo.set_server_port(port);
  serverInfo.set_type("synchronizer");
  serverInfo.set_server_id(synchID);
  serverInfo.set_cluster_id(clusterID);
  Heartbeat(coordIP, coordPort, serverInfo, synchID);

  RunServer(coordIP, coordPort, port, synchID);
  return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ)
{
  // setup coordinator stub
  grpc::ChannelArguments ch_args;
  ch_args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
  auto coordinator_channel = grpc::CreateCustomChannel(coordIP + ":" + coordPort, grpc::InsecureChannelCredentials(), ch_args);
  auto coord_stub = csce438::CoordService::NewStub(coordinator_channel);

  ServerInfo msg;
  Confirmation c;

  msg.set_server_id(synchID);
  msg.set_server_ip("127.0.0.1");
  msg.set_server_port(port);
  msg.set_type("follower");

  // TODO: begin synchronization process
  while (true)
  {
    // the synchronizers sync files every 5 seconds
    sleep(2);

    grpc::ClientContext context;
    ServerList followerServers;
    ID id;
    id.set_client_id(synchID);

    // making a request to the coordinator to see count of follower synchronizers
    auto s = coord_stub->GetAllFollowerServers(&context, id, &followerServers);
    if (!s.ok())
    {
      std::cerr << "RPC failed with error code " << s.error_code() << ": " << s.error_message() << std::endl;
      std::cerr << "Details: " << s.error_details() << std::endl;
    }
    total_number_of_registered_synchronizers = followerServers.serverid_size();
    // std::cout << synchID << " syncs" << total_number_of_registered_synchronizers << std::endl;

    std::vector<int> server_ids;
    std::vector<std::string> hosts, ports;
    for (std::string host : followerServers.hostname())
    {
      hosts.push_back(host);
    }
    for (std::string port : followerServers.port())
    {
      ports.push_back(port);
    }
    for (int serverid : followerServers.serverid())
    {
      server_ids.push_back(serverid);
    }

    // update the count of how many follower sychronizer processes the coordinator has registered

    // below here, you run all the update functions that synchronize the state across all the clusters
    // make any modifications as necessary to satisfy the assignments requirements

    // coord_stub_->GetServer(&context, )

    // Publish user list
    rabbitMQ.publishUserList();
    rabbitMQ.publishFollowers();
    rabbitMQ.publishFollowings();
    rabbitMQ.publishTimelines();
    rabbitMQ.publishTimestamp();
  }
  return;
}

std::vector<std::string> get_lines_from_file(std::string filename)
{
  std::vector<std::string> users;
  std::string user;
  std::ifstream file;
  std::string semName = get_sem_name_from_filepath(filename);
  sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
  file.open(filename);
  if (file.peek() == std::ifstream::traits_type::eof())
  {
    // return empty vector if empty file
    // std::cout<<"returned empty vector bc empty file"<<std::endl;
    file.close();
    sem_close(fileSem);
    return users;
  }
  while (file)
  {
    getline(file, user);

    if (!user.empty())
      users.push_back(user);
  }

  file.close();
  sem_close(fileSem);

  return users;
}

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID)
{
  // For the synchronizer, a single initial heartbeat RPC acts as an initialization method which
  // servers to register the synchronizer with the coordinator and determine whether it is a master

  log(INFO, "Sending initial heartbeat to coordinator");
  std::string coordinatorInfo = coordinatorIp + ":" + coordinatorPort;
  grpc::ChannelArguments ch_args;
  ch_args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);

  // connect to coordinator first
  auto coordinator_channel = grpc::CreateCustomChannel(coordinatorIp + ":" + coordinatorPort, grpc::InsecureChannelCredentials(), ch_args);
  std::unique_ptr<CoordService::Stub> coordinator_stub = csce438::CoordService::NewStub(coordinator_channel);

  // send a heartbeat to the coordinator, which registers your follower synchronizer as either a master or a slave

  // YOUR CODE HERE
  Confirmation confirmation;
  ClientContext context;
  Status status = coordinator_stub->Heartbeat(&context, serverInfo, &confirmation);

  if (status.ok())
  {
    std::cout << "Heartbeat sent successfully: " << confirmation.message() << std::endl;
    std::cout << "sync" << syncID << ": cluster" << clusterID << " server" << clusterSubdirectory << std::endl;
  }
  else
  {
    std::cout << "Heartbeat failed: " << status.error_message() << std::endl;
  }
}

bool file_contains_user(std::string filename, std::string user)
{
  std::vector<std::string> users;
  // check username is valid
  std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + filename;
  sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
  users = read_users_from_file(filename);
  for (int i = 0; i < users.size(); i++)
  {
    // std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
    if (user == users[i])
    {
      // std::cout<<"found"<<std::endl;
      sem_close(fileSem);
      return true;
    }
  }
  // std::cout<<"not found"<<std::endl;
  sem_close(fileSem);
  return false;
}

std::vector<std::string> get_all_users_func(int synchID)
{
  // read all_users file master and client for correct serverID
  // std::string master_users_file = "./master"+std::to_string(synchID)+"/all_users";
  // std::string slave_users_file = "./slave"+std::to_string(synchID)+"/all_users";
  std::string clusterID = std::to_string(((synchID - 1) % 3) + 1);
  std::string master_users_file = "./cluster_" + clusterID + "/1/all_users.txt";
  std::string slave_users_file = "./cluster_" + clusterID + "/2/all_users.txt";
  // take longest list and package into AllUsers message
  std::vector<std::string> master_user_list = get_lines_from_file(master_users_file);
  std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file);

  if (master_user_list.size() >= slave_user_list.size())
    return master_user_list;
  else
    return slave_user_list;
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl)
{
  // std::string master_fn = "./master"+std::to_string(synchID)+"/"+std::to_string(clientID);
  // std::string slave_fn = "./slave"+std::to_string(synchID)+"/" + std::to_string(clientID);
  std::string master_fn = "cluster_" + std::to_string(clusterID) + "/1/" + std::to_string(clientID);
  std::string slave_fn = "cluster_" + std::to_string(clusterID) + "/2/" + std::to_string(clientID);
  if (tl)
  {
    master_fn.append("_timeline.txt");
    slave_fn.append("_timeline.txt");
  }
  else
  {
    master_fn.append("_followers.txt");
    slave_fn.append("_followers.txt");
  }

  std::vector<std::string> m = get_lines_from_file(master_fn);
  std::vector<std::string> s = get_lines_from_file(slave_fn);

  if (m.size() >= s.size())
  {
    return m;
  }
  else
  {
    return s;
  }
}

std::vector<std::string> getFollowersOfUser(int ID)
{
  std::vector<std::string> followers;
  std::string clientID = std::to_string(ID);
  std::vector<std::string> usersInCluster = get_all_users_func(synchID);

  for (auto userID : usersInCluster)
  { // Examine each user's following file
    std::string file = "./cluster_" + std::to_string(clusterID) + "/" + clusterSubdirectory + "/" + userID + "_follow_list.txt";
    std::string semName = get_sem_name_from_filepath(file);
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT);
    // std::cout << "Reading file " << file << std::endl;
    if (find_user_from_file(file, clientID))
    {
      followers.push_back(userID);
    }
    sem_close(fileSem);
  }

  return followers;
}
