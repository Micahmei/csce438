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
#include <glog/logging.h>  // ✅ 确保包含 glog 头文件

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

// ✅ 先定义 zNode 结构体，否则后续的 vector 定义会报错
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
std::vector<zNode*> cluster1;  // 先声明 vector
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// ✅ 先创建 `clusters`，不要在 `{}` 里直接放变量
std::vector<std::vector<zNode*>> clusters;
clusters.push_back(cluster1);
clusters.push_back(cluster2);
clusters.push_back(cluster3);

// ✅ 函数声明
std::time_t getTimeNow();
void checkHeartbeat();

// ✅ 获取当前时间
std::time_t getTimeNow() {
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

// ✅ Coordinator 服务实现
class CoordServiceImpl final : public CoordService::Service {
    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        std::lock_guard<std::mutex> lock(v_mutex);

        int cluster_id = (serverinfo->serverid() - 1) % 3;
        int server_id = serverinfo->serverid();

        for (auto& server : clusters[cluster_id]) {
            if (server->serverID == server_id) {
                server->last_heartbeat = getTimeNow();
                server->missed_heartbeat = false;
                LOG(INFO) << "[Coordinator] Received heartbeat from Server " << server_id;
                confirmation->set_status("OK");
                return Status::OK;
            }
        }

        // **注册新服务器**
        zNode* new_server = new zNode{
            server_id, serverinfo->hostname(), serverinfo->port(), getTimeNow(), false
        };
        clusters[cluster_id].push_back(new_server);

        std::time_t now = getTimeNow();
        LOG(INFO) << "[Coordinator] Registered new Server " << server_id << " at "
                  << serverinfo->hostname() << ":" << serverinfo->port() << " " << std::ctime(&now);

        confirmation->set_status("OK");
        return Status::OK;
    }

    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        std::lock_guard<std::mutex> lock(v_mutex);

        int client_id = id->id();
        int cluster_id = (client_id - 1) % 3;
        int server_id = 1;

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

        LOG(INFO) << "[Coordinator] Assigned Client " << client_id << " to Server " << server->serverID
                  << " at " << server->hostname << ":" << server->port;

        return Status::OK;
    }
};

// ✅ 服务器心跳检测
void checkHeartbeat() {
    while (true) {
        std::lock_guard<std::mutex> lock(v_mutex);

        for (auto& c : clusters) {
            for (auto& s : c) {
                if (difftime(getTimeNow(), s->last_heartbeat) > 10) {
                    if (!s->missed_heartbeat) {
                        s->missed_heartbeat = true;
                        LOG(WARNING) << "[Coordinator] Missed heartbeat from Server " << s->serverID;
                    }
                }
            }
        }
        sleep(3);
    }
}

// ✅ 运行 Coordinator 服务器
void RunServer(std::string port_no) {
    std::thread hb_thread(checkHeartbeat);  // ✅ 确保 `checkHeartbeat()` 被正确声明

    std::string server_address("127.0.0.1:" + port_no);
    CoordServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    LOG(INFO) << "[Coordinator] Server listening on " << server_address;
    server->Wait();
}

// ✅ 主函数
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

    google::InitGoogleLogging(argv[0]);  // ✅ 初始化 glog
    FLAGS_logtostderr = 1;  // ✅ 直接输出到终端
    RunServer(port);
    return 0;
}
