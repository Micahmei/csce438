#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>
#include <fstream>
#include <filesystem>
#include <google/protobuf/util/time_util.h>
#include <glog/logging.h>

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"

using csce438::SNSService;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using csce438::Confirmation;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ClientContext;
using grpc::ClientWriter;

// ✅ 服务器节点信息结构体
struct Client {
    std::string username;
    std::vector<Client*> client_followers;
    std::vector<Client*> client_following;
    grpc::ServerReaderWriter<csce438::Message, csce438::Message>* stream = nullptr;
};

// ✅ 维护所有客户端
std::vector<Client*> client_db;

// ✅ SNS 服务器实现
class SNSServiceImpl final : public SNSService::Service {
public:
    Status List(ServerContext* context, const csce438::Request* request, csce438::ListReply* list_reply) override {
        std::string username = request->username();
        for (const auto& client : client_db) {
            list_reply->add_all_users(client->username);
        }
        for (const auto& client : client_db) {
            if (client->username == username) {
                for (const auto& follower : client->client_followers) {
                    list_reply->add_followers(follower->username);
                }
                break;
            }
        }
        return Status::OK;
    }

    Status Follow(ServerContext* context, const csce438::Request* request, csce438::Reply* reply) override {
        std::string follower_name = request->username();
        std::string followee_name = request->arguments(0);

        if (follower_name == followee_name) {
            reply->set_msg("cannot follow self");
            return Status::OK;
        }

        Client* follower = nullptr;
        Client* followee = nullptr;
        for (auto& client : client_db) {
            if (client->username == follower_name) {
                follower = client;
            }
            if (client->username == followee_name) {
                followee = client;
            }
        }

        if (!followee) {
            reply->set_msg("followed user no exist");
            return Status::OK;
        }

        follower->client_following.push_back(followee);
        followee->client_followers.push_back(follower);
        reply->set_msg("followed successful");
        return Status::OK;
    }

    Status UnFollow(ServerContext* context, const csce438::Request* request, csce438::Reply* reply) override {
        std::string username = request->username();
        std::string unfollow_username = request->arguments(0);

        if (username == unfollow_username) {
            reply->set_msg("cannot unfollow self");
            return Status::OK;
        }

        Client* user = nullptr;
        Client* unfollow_user = nullptr;

        for (auto& client : client_db) {
            if (client->username == username) {
                user = client;
            }
            if (client->username == unfollow_username) {
                unfollow_user = client;
            }
        }

        if (!unfollow_user) {
            reply->set_msg("unfollowed user no exist");
            return Status::OK;
        }

        auto it = std::find(user->client_following.begin(), user->client_following.end(), unfollow_user);
        if (it != user->client_following.end()) {
            user->client_following.erase(it);

            auto follower_it = std::find(unfollow_user->client_followers.begin(), unfollow_user->client_followers.end(), user);
            if (follower_it != unfollow_user->client_followers.end()) {
                unfollow_user->client_followers.erase(follower_it);
            }

            reply->set_msg("unfollow successful");
        } else {
            reply->set_msg("not following user");
        }

        return Status::OK;
    }

    Status Login(ServerContext* context, const csce438::Request* request, csce438::Reply* reply) override {
        std::string username = request->username();
        for (const auto& client : client_db) {
            if (client->username == username) {
                reply->set_msg("user already exists");
                return Status::OK;
            }
        }

        Client* new_client = new Client();
        new_client->username = username;
        client_db.push_back(new_client);

        reply->set_msg("login successful");
        return Status::OK;
    }
};

// ✅ 运行 SNS 服务器
void RunServer(std::string port_no) {
    std::string server_address = "0.0.0.0:" + port_no;
    SNSServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    server->Wait();
}

// ✅ 发送心跳信号到 Coordinator
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

// ✅ 解析命令行参数并运行服务器
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
