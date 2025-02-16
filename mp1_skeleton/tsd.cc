#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <algorithm>
#include <grpc++/grpc++.h>
#include <sys/stat.h>  
#include <grpcpp/grpcpp.h>  
#include "sns.grpc.pb.h"  
#include "coordinator.grpc.pb.h"  
#include <fstream>
#include <filesystem>
#include <google/protobuf/util/time_util.h>
#include <glog/logging.h>

#define log(severity, msg) \
  LOG(severity) << msg;    \
  google::FlushLogFiles(google::severity);

using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::SNSService;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using google::protobuf::Timestamp;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

struct Client {
    std::string username;
    std::vector<Client*> client_followers;
    std::vector<Client*> client_following;
    ServerReaderWriter<Message, Message>* stream = nullptr;
};

// 维护所有客户端
std::vector<Client*> client_db;

class SNSServiceImpl final : public SNSService::Service {
public:
    Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
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

    Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
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

    Status Login(ServerContext* context, const Request* request, Reply* reply) override {
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

    Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {
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

        SendLatestPosts(stream, timeline_file);

        std::thread monitor_thread([timeline_file, stream]() {
            MonitorTimeline(stream, timeline_file);
        });

        Message client_msg;
        while (stream->Read(&client_msg)) {
            std::ofstream file(timeline_file, std::ios::app);
            if (!file) {
                LOG(ERROR) << "[Server] Failed to open timeline file: " << timeline_file;
                return Status::CANCELLED;
            }

            file << client_msg.username() << ": " << client_msg.msg() << "\n";
            file.close();

            for (Client* follower : client->client_followers) {
                if (follower->stream) {
                    follower->stream->Write(client_msg);
                }
            }
        }

        client->stream = nullptr;
        monitor_thread.join();
        return Status::OK;
    }

private:
    void SendLatestPosts(ServerReaderWriter<Message, Message>* stream, const std::string& timeline_file) {
        std::ifstream file(timeline_file);
        std::string line;
        while (std::getline(file, line)) {
            Message msg;
            msg.set_msg(line);
            stream->Write(msg);
        }
    }

    void MonitorTimeline(ServerReaderWriter<Message, Message>* stream, const std::string& timeline_file) {
        struct stat fileStat;
        while (true) {
            if (stat(timeline_file.c_str(), &fileStat) == 0) {
                sleep(5);
            }
        }
    }
};

void RunServer(std::string port_no) {
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
        ServerInfo request;
        request.set_serverid(std::stoi(server_id));
        request.set_hostname("127.0.0.1");
        request.set_port(port);

        std::unique_ptr<CoordService::Stub> stub = 
            CoordService::NewStub(grpc::CreateChannel(
                coordinator_ip + ":" + coordinator_port, grpc::InsecureChannelCredentials()));

        grpc::Status status = stub->Heartbeat(&context, request, nullptr);
        if (!status.ok()) {
            std::cerr << "[TSD] Heartbeat failed: " << status.error_message() << std::endl;
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

    std::thread hb_thread(HeartbeatThread, server_id, coordinator_ip, coordinator_port, port);
    hb_thread.detach();

    RunServer(port);
    return 0;
}
