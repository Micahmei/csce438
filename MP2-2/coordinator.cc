#include <iostream>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <thread>
#include <string>
#include <grpc++/grpc++.h>
#include "coordinator.grpc.pb.h"
#include "sns.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ClientContext;

using csce438::Coordinator;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;

struct ServerEntry {
    std::string ip;
    std::string port;
    std::string type; // "server" or "follower"
    int cluster_id;
    int server_id;
    bool is_master;
    bool is_active;
    std::chrono::steady_clock::time_point last_heartbeat;
};

class CoordinatorServiceImpl final : public CoordService::Service {
private:
    std::mutex mtx;
    std::vector<ServerEntry> servers;

    std::chrono::seconds heartbeatTimeout = std::chrono::seconds(10);

public:
    // Heartbeat RPC: Server/Synchronizer use this to register/update
    Status Heartbeat(ServerContext* context, const ServerInfo* request, Confirmation* reply) override {
        std::lock_guard<std::mutex> lock(mtx);

        bool found = false;
        for (auto& s : servers) {
            if (s.server_id == request->serverid() &&
                s.cluster_id == request->clusterid() &&
                s.type == request->type()) {
                s.last_heartbeat = std::chrono::steady_clock::now();
                s.is_active = true;
                found = true;
                break;
            }
        }

        if (!found) {
            servers.push_back({
                request->hostname(),
                request->port(),
                request->type(),
                request->clusterid(),
                request->serverid(),
                request->type() == "server" ? request->ismaster() : false,
                true,
                std::chrono::steady_clock::now()
            });
        }

        reply->set_status(true);
        return Status::OK;
    }

    // For clients: returns available Master server
    Status GetServer(ServerContext* context, const ID* request, ServerInfo* reply) override {
        std::lock_guard<std::mutex> lock(mtx);
        int cluster_id = ((request->id() - 1) % 3) + 1;

        ServerEntry* master = nullptr;
        ServerEntry* slave = nullptr;

        for (auto& s : servers) {
            if (s.cluster_id == cluster_id && s.type == "server") {
                if (s.is_master && s.is_active)
                    master = &s;
                else if (!s.is_master && s.is_active)
                    slave = &s;
            }
        }

        if (master) {
            reply->set_hostname(master->ip);
            reply->set_port(master->port);
            reply->set_serverid(master->server_id);
            reply->set_clusterid(master->cluster_id);
            reply->set_ismaster(true);
        } else if (slave) {
            reply->set_hostname(slave->ip);
            reply->set_port(slave->port);
            reply->set_serverid(slave->server_id);
            reply->set_clusterid(slave->cluster_id);
            reply->set_ismaster(true); // Slave now acts as master
        } else {
            reply->set_serverid(0); // no server
        }

        return Status::OK;
    }

    // For synchronizers: get list of all follower synchronizer servers
    Status GetAllFollowerServers(ServerContext* context, const ID* request, ServerList* reply) override {
        std::lock_guard<std::mutex> lock(mtx);
        for (auto& s : servers) {
            if (s.type == "follower" && s.is_active) {
                reply->add_serverid(s.server_id);
                reply->add_hostname(s.ip);
                reply->add_port(s.port);
                reply->add_type(s.type);
            }
        }
        return Status::OK;
    }

    // Periodic checker for dead servers (Master failover)
    void MonitorHeartbeats() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            std::lock_guard<std::mutex> lock(mtx);

            auto now = std::chrono::steady_clock::now();
            for (auto& s : servers) {
                if (s.type == "server") {
                    auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - s.last_heartbeat);
                    if (duration > heartbeatTimeout && s.is_active) {
                        std::cout << "[FAILOVER] Server " << s.server_id << " in cluster "
                                  << s.cluster_id << " timed out. Marking inactive.\n";
                        s.is_active = false;

                        // promote slave if needed
                        if (s.is_master) {
                            for (auto& candidate : servers) {
                                if (candidate.cluster_id == s.cluster_id &&
                                    candidate.type == "server" &&
                                    !candidate.is_master) {
                                    candidate.is_master = true;
                                    std::cout << "[PROMOTE] Slave " << candidate.server_id
                                              << " promoted to Master for cluster " << candidate.cluster_id << "\n";
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

void RunCoordinator(const std::string& port) {
    std::string addr = "0.0.0.0:" + port;
    CoordinatorServiceImpl service;

    std::thread monitor_thread(&CoordinatorServiceImpl::MonitorHeartbeats, &service);

    ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Coordinator running at " << addr << std::endl;
    server->Wait();
    monitor_thread.join();
}

int main(int argc, char** argv) {
    std::string port = "9090";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1) {
        switch (opt) {
        case 'p': port = optarg; break;
        default: std::cerr << "Usage: ./coordinator -p <port>\n"; return 1;
        }
    }

    RunCoordinator(port);
    return 0;
}
