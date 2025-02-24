// coordinator_impl.cc

#include "coordinator.grpc.pb.h"
#include "sns.grpc.pb.h"
#include <grpc++/grpc++.h>
#include <unordered_map>
#include <iostream>
#include <thread>

using csce438::Confirmation;
using csce438::ID;
using csce438::ServerInfo;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

struct zNode
{
  std::string ip;
  std::string port;
  std::string type;
  std::chrono::steady_clock::time_point last_heartbeat;
  bool is_active;
};

struct ServersMap
{
  std::unordered_map<uint64_t, std::unordered_map<uint64_t, zNode>> clusters;

  std::vector<zNode *> FindServer(uint64_t cluster_id, uint64_t server_id = 0)
  {
    if (clusters.find(cluster_id) == clusters.end())
    {
      return {};
    }
    if (server_id == 0)
    {
      std::vector<zNode *> servers;
      for (auto it = clusters[cluster_id].begin(); it != clusters[cluster_id].end(); it++)
      {
        servers.push_back(&(it->second));
      }
      return servers;
    }
    auto it = clusters[cluster_id].find(server_id);
    if (it != clusters[cluster_id].end())
    {
      std::vector<zNode *> servers;
      servers.push_back(&(it->second));
      return servers;
    }
    else
    {
      return {};
    }
  }

  void AddServer(uint64_t cluster_id, uint64_t server_id, std::string ip, std::string port)
  {
    if (clusters.find(cluster_id) == clusters.end())
    {
      clusters[cluster_id] = std::unordered_map<uint64_t, zNode>();
    }
    clusters[cluster_id][server_id] = {ip, port, "x", std::chrono::steady_clock::now(), true};
  }
};

class CoordinatorServiceImpl final : public csce438::Coordinator::Service
{
public:
  // server id --> server status
  ServersMap servers_status;
  std::mutex servers_status_mtx;
  std::chrono::seconds heartbeat_timeout{10};

  // recieve the heartbeat package from server
  Status Heartbeat(ServerContext *context, const ServerInfo *server_info, Confirmation *confirmation) override
  {
    std::cout << "Received Heartbeat from server: " << server_info->server_id()
              << "  <" << server_info->server_ip() + ":" + server_info->server_port() << ">" << std::endl;

    std::lock_guard<std::mutex> lock(servers_status_mtx);

    auto servers = servers_status.FindServer(server_info->cluster_id(), server_info->server_id());
    if (!servers.empty())
    {
      auto server = servers[0];
      server->last_heartbeat = std::chrono::steady_clock::now();
      server->is_active = true;
    }
    else
    {
      servers_status.AddServer(server_info->cluster_id(), server_info->server_id(), server_info->server_ip(),
                               server_info->server_port());
    }

    confirmation->set_success(true);
    confirmation->set_message("Heartbeat received successfully.");

    return Status::OK;
  }

  Status GetServer(ServerContext *context, const ID *request, ServerInfo *response) override
  {
    std::cout << "connect request from client: " << request->client_id() << std::endl;
    std::lock_guard<std::mutex> lock(servers_status_mtx);

    uint64_t cluster_id = (request->client_id() - 1) % 3 + 1;
    auto servers = servers_status.FindServer(cluster_id);

    if (!servers.empty() and servers[0]->is_active)
    {
      auto server = servers[0];
      // check the current status of the server
      grpc::ChannelArguments ch_args;
      ch_args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
      auto channel = grpc::CreateCustomChannel(server->ip + ":" + server->port, grpc::InsecureChannelCredentials(), ch_args);
      auto server_stub = csce438::SNSService::NewStub(channel);
      ClientContext context;
      csce438::Request request;
      request.set_username("x");
      csce438::Reply reply;
      auto status = server_stub->CheckConnection(&context, request, &reply);
      if (status.ok())
      {
        std::cout << "get server <" << server->ip + ":" + server->port << ">" << std::endl;
        response->set_server_id(cluster_id);
        response->set_server_ip(server->ip);
        response->set_server_port(server->port);
        return Status::OK;
      }
      else
      {
        std::cerr << "RPC failed with error code " << status.error_code() << ": " << status.error_message() << std::endl;
        std::cerr << "Details: " << status.error_details() << std::endl;
      }
    }

    response->set_server_id(0); // server_id == 0 means no available server
    return Status::OK;
  }

  void CheckForTimeout()
  {

    while (true)
    {
      std::this_thread::sleep_for(std::chrono::seconds(5)); // Check every 5 seconds

      std::lock_guard<std::mutex> lock(servers_status_mtx);

      for (auto &cluster_entry : servers_status.clusters)
      {
        for (auto &server_entry : cluster_entry.second)
        {
          auto &server = server_entry.second;
          auto duration = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - server.last_heartbeat);
          // std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(server.last_heartbeat.time_since_epoch()).count() << "  " << duration.count() << std::endl;

          if (duration > heartbeat_timeout)
          {
            if (server.is_active)
            {
              server.is_active = false;
              std::cout << "Server " << server_entry.first << " in cluster " << cluster_entry.first
                        << " is offline (Heartbeat timeout)" << std::endl;
            }
          }
          else
          {
            if (!server.is_active)
            {
              server.is_active = true;
              std::cout << "Server " << server_entry.first << " in cluster " << cluster_entry.first
                        << " is online again" << std::endl;
            }
          }
        }
      }
    }
  }
};

void RunCoordinatorServer(const std::string &address)
{
  CoordinatorServiceImpl service;

  std::thread timeout_check_thread(
      [&service]()
      { service.CheckForTimeout(); });

  ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());

  std::cout << "Coordinator server listening on " << address << std::endl;

  server->Wait();
}

int main(int argc, char **argv)
{
  std::string port = "9090";
  for (int i = 1; i < argc; ++i)
  {
    if (std::string(argv[i]) == "-p" && i + 1 < argc)
    {
      port = argv[i + 1];
      ++i;
    }
    else
    {
      std::cerr << "Invalid Command Line Argument\n";
      return 0;
    }
  }
  std::string server_address = "0.0.0.0:" + port;
  RunCoordinatorServer(server_address);
  return 0;
}
