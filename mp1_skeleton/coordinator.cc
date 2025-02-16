#include <algorithm>
#include <cstdio>
#include <ctime>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
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
#include <glog/logging.h>

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
using csce438::PathAndData;
using csce438::Path;
using csce438::CoordStatus;

// ✅ 服务器节点结构
struct zNode {
    int serverID;
    std::string hostname;
    std::string port;
    std::time_t last_heartbeat;
    bool missed_heartbeat;

    bool isActive() {
        return !missed_heartbeat || (difftime(std::time(nullptr), last_heartbeat) < 10);
    }
};

// ✅ 线程安全变量
std::mutex v_mutex;
std::vector<std::vector<zNode*>> clusters(3);

// ✅ 获取当前时间
std::time_t getTimeNow() {
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

// ✅ Coordinator 服务实现
class CoordServiceImpl final : public CoordService::Service {
    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        LOG(ERROR) << "[DEBUG] Entering Heartbeat() function";
        std::lock_guard<std::mutex> lock(v_mutex);
        
        int cluster_id = (serverinfo->serverid() - 1) % 3;
        int server_id = serverinfo->serverid();
        bool found = false;
    
        std::cerr << "[Coordinator] Received Heartbeat from Server " << server_id 
                  << " at " << serverinfo->hostname() << ":" << serverinfo->port() << std::endl;
        
        for (auto& server : clusters[cluster_id]) {
            if (server->serverID == server_id) {
                server->last_heartbeat = getTimeNow();
                server->missed_heartbeat = false;
                found = true;
                LOG(INFO) << "[Coordinator] Heartbeat updated for Server " << server_id;
                break;
            }
        }
    
        if (!found) {
            zNode* new_server = new zNode{
                server_id, serverinfo->hostname(), serverinfo->port(), getTimeNow(), false
            };
            clusters[cluster_id].push_back(new_server);
            LOG(INFO) << "[Coordinator] Registered new Server " << server_id;
        }
    
        confirmation->set_status(true);
        return Status::OK;
    }



    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        
        std::lock_guard<std::mutex> lock(v_mutex);
    
        int client_id = id->id();
        int cluster_id = (client_id - 1) % 3;
    
        LOG(INFO) << "[DEBUG] Cluster " << cluster_id << " server count: " << clusters[cluster_id].size();

    
        if (clusters[cluster_id].empty()) {
            LOG(ERROR) << "[Coordinator] No available servers for Client " << client_id;
            return Status::CANCELLED;
        }
    
        zNode* server = clusters[cluster_id][0];
    
        if (!server->isActive()) {
            LOG(ERROR) << "[Coordinator] Assigned server " << server->serverID << " is inactive!";
            return Status::CANCELLED;
        }
    
        serverinfo->set_serverid(server->serverID);
        serverinfo->set_hostname(server->hostname);
        serverinfo->set_port(server->port);
    
        LOG(INFO) << "[Coordinator] 分配 Server " << server->serverID << " 给 Client " << client_id;
        return Status::OK;
    }

};

// ✅ 服务器心跳检测
void checkHeartbeat() {
    while (true) {
        std::lock_guard<std::mutex> lock(v_mutex);
        for (auto& cluster : clusters) {
            auto it = cluster.begin();
            while (it != cluster.end()) {
                if (difftime(getTimeNow(), (*it)->last_heartbeat) > 10) {
                    LOG(ERROR) << "[Coordinator] Server " << (*it)->serverID << " is removed due to heartbeat timeout!";
                    delete *it;
                    it = cluster.erase(it);
                } else {
                    ++it;
                }
            }
        }
        sleep(3);
    }
}


// ✅ 运行 Coordinator 服务器
void RunServer(std::string port_no) {
    std::thread hb_thread(checkHeartbeat);
    std::string server_address("127.0.0.1:" + port_no);
    CoordServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    LOG(INFO) << "[Coordinator] Server listening on " << server_address;
    server->Wait();
}

int main(int argc, char** argv) {
    std::string port = "3010";
    int opt;
    while ((opt = getopt(argc, argv, "p:")) != -1) {
        if (opt == 'p') port = optarg;
    }

    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    RunServer(port);
    return 0;
}
