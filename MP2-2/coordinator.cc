#include <iostream>
#include <map>
#include <thread>
#include <chrono>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include "coordinator.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using sns::Coordinator;
using sns::HeartbeatRequest;
using sns::HeartbeatReply;
using sns::RegisterRequest;
using sns::RegisterReply;
using sns::ServerInfoRequest;
using sns::ServerInfoReply;

std::mutex mtx;

// serverInfo: IP, port, last heartbeat time, status
struct ServerInfo {
    std::string ip;
    int port;
    std::chrono::time_point<std::chrono::steady_clock> last_heartbeat;
    bool is_master;
    bool is_alive;
};

std::map<int, std::map<int, ServerInfo>> server_table; // clusterID -> serverID -> info

class CoordinatorServiceImpl final : public Coordinator::Service {
public:
    Status Register(ServerContext* context, const RegisterRequest* request, RegisterReply* reply) override {
        std::lock_guard<std::mutex> lock(mtx);
        int cluster = request->clusterid();
        int server = request->serverid();
        std::string ip = request->ip();
        int port = request->port();
        bool is_master = request->ismaster();

        server_table[cluster][server] = {ip, port, std::chrono::steady_clock::now(), is_master, true};

        std::cout << "[Register] Server (Cluster " << cluster << ", ID " << server << ") at "
                  << ip << ":" << port << " registered as " << (is_master ? "Master" : "Slave") << std::endl;

        reply->set_status("OK");
        return Status::OK;
    }

    Status Heartbeat(ServerContext* context, const HeartbeatRequest* request, HeartbeatReply* reply) override {
        std::lock_guard<std::mutex> lock(mtx);
        int cluster = request->clusterid();
        int server = request->serverid();

        if (server_table.count(cluster) && server_table[cluster].count(server)) {
            server_table[cluster][server].last_heartbeat = std::chrono::steady_clock::now();
            server_table[cluster][server].is_alive = true;
            reply->set_status("OK");
        } else {
            reply->set_status("NOT_REGISTERED");
        }

        return Status::OK;
    }

    Status GetServer(ServerContext* context, const ServerInfoRequest* request, ServerInfoReply* reply) override {
        std::lock_guard<std::mutex> lock(mtx);
        int client_id = request->clientid();
        int cluster = ((client_id - 1) % 3) + 1;

        for (auto& [server_id, info] : server_table[cluster]) {
            if (info.is_master && info.is_alive) {
                reply->set_ip(info.ip);
                reply->set_port(info.port);
                reply->set_clusterid(cluster);
                reply->set_status("OK");
                return Status::OK;
            }
        }

        reply->set_status("NO_ACTIVE_MASTER");
        return Status::OK;
    }

    // Synchronizer calls this to find the cluster ID of a client
    Status GetClusterForClient(ServerContext* context, const ServerInfoRequest* request, ServerInfoReply* reply) override {
        int client_id = request->clientid();
        int cluster = ((client_id - 1) % 3) + 1;

        reply->set_clusterid(cluster);
        reply->set_status("OK");
        return Status::OK;
    }

    void MonitorServers() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            std::lock_guard<std::mutex> lock(mtx);
            auto now = std::chrono::steady_clock::now();

            for (auto& [cluster, servers] : server_table) {
                for (auto& [sid, info] : servers) {
                    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - info.last_heartbeat).count();
                    if (duration > 10 && info.is_alive) {
                        std::cout << "[FAILURE DETECTED] Cluster " << cluster << " Server " << sid << " timed out." << std::endl;
                        info.is_alive = false;

                        // Promote Slave if Master failed
                        if (info.is_master) {
                            for (auto& [alt_sid, alt_info] : servers) {
                                if (!alt_info.is_master && alt_info.is_alive) {
                                    alt_info.is_master = true;
                                    std::cout << "-> Promoted Server " << alt_sid << " to Master in Cluster " << cluster << std::endl;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
};

void RunServer(int port) {
    std::string server_address = "0.0.0.0:" + std::to_string(port);
    CoordinatorServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "[Coordinator] Listening on " << server_address << std::endl;

    std::thread monitor_thread(&CoordinatorServiceImpl::MonitorServers, &service);
    monitor_thread.detach();

    server->Wait();
}

int main(int argc, char** argv) {
    if (argc != 3 || std::string(argv[1]) != "-p") {
        std::cerr << "Usage: ./coordinator -p <port>" << std::endl;
        return 1;
    }

    int port = std::stoi(argv[2]);
    RunServer(port);
    return 0;
}
