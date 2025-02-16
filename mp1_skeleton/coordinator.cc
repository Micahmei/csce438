#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;
using csce438::SynchService;
using csce438::ClientInfo;

struct zNode {
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();
};

// 线程安全
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};

// 存储客户端心跳信息
std::mutex client_mutex;
std::map<std::string, std::time_t> client_heartbeats;

// 函数声明
int findServer(std::vector<zNode*> v, int id);
std::time_t getTimeNow();
void checkHeartbeat();
void checkClientHeartbeat();

bool zNode::isActive() {
    return (!missed_heartbeat || difftime(getTimeNow(), last_heartbeat) < 10);
}

class CoordServiceImpl final : public CoordService::Service {
    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        std::lock_guard<std::mutex> lock(v_mutex);

        int cluster_id = serverinfo->cluster_id();
        int server_id = serverinfo->server_id();
        std::string ip = serverinfo->ip();
        std::string port = serverinfo->port();

        std::cout << "[HEARTBEAT] Received from Server " << server_id << " in Cluster " << cluster_id << std::endl;

        if (cluster_id < 1 || cluster_id > 3) {
            return Status::CANCELLED;
        }

        auto& cluster = clusters[cluster_id - 1];

        for (auto& server : cluster) {
            if (server->serverID == server_id) {
                server->last_heartbeat = getTimeNow();
                server->missed_heartbeat = false;
                confirmation->set_msg("Heartbeat received.");
                return Status::OK;
            }
        }

        zNode* newServer = new zNode{server_id, ip, port, "server", getTimeNow(), false};
        cluster.push_back(newServer);
        confirmation->set_msg("Server registered and heartbeat received.");
        return Status::OK;
    }

    Status ClientHeartbeat(ServerContext* context, const ClientInfo* clientinfo, Confirmation* confirmation) override {
        std::lock_guard<std::mutex> lock(client_mutex);
        std::string username = clientinfo->username();
        client_heartbeats[username] = getTimeNow();
        std::cout << "[HEARTBEAT] Client " << username << " is active." << std::endl;
        confirmation->set_msg("Client heartbeat received.");
        return Status::OK;
    }

    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        int client_id = id->client_id();
        int cluster_id = (client_id - 1) % 3;
        auto& cluster = clusters[cluster_id];

        for (auto& server : cluster) {
            if (server->isActive()) {
                serverinfo->set_cluster_id(cluster_id + 1);
                serverinfo->set_server_id(server->serverID);
                serverinfo->set_ip(server->hostname);
                serverinfo->set_port(server->port);
                return Status::OK;
            }
        }

        return Status::CANCELLED;
    }
};

void RunServer(std::string port_no) {
    std::thread hb(checkHeartbeat);
    std::thread chb(checkClientHeartbeat);

    std::string server_address("127.0.0.1:" + port_no);
    CoordServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Coordinator listening on " << server_address << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1) {
        switch(opt) {
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }
    RunServer(port);
    return 0;
}

void checkHeartbeat() {
    while (true) {
        std::lock_guard<std::mutex> lock(v_mutex);

        for (auto& cluster : clusters) {
            for (auto& server : cluster) {
                if (difftime(getTimeNow(), server->last_heartbeat) > 10) {
                    std::cout << "[WARNING] Missed heartbeat from Server " << server->serverID << std::endl;
                    if (!server->missed_heartbeat) {
                        server->missed_heartbeat = true;
                        server->last_heartbeat = getTimeNow();
                    }
                }
            }
        }

        sleep(3);
    }
}

void checkClientHeartbeat() {
    while (true) {
        std::lock_guard<std::mutex> lock(client_mutex);

        for (auto it = client_heartbeats.begin(); it != client_heartbeats.end();) {
            if (difftime(getTimeNow(), it->second) > 10) {
                std::cout << "[WARNING] Client " << it->first << " is inactive." << std::endl;
                it = client_heartbeats.erase(it);
            } else {
                ++it;
            }
        }

        sleep(3);
    }
}

std::time_t getTimeNow() {
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}
