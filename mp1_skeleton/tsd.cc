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
#include <fstream>
#include <filesystem>
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

using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::SNSService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

/* ======= HEARTBEAT 代码 ======= */
#include "coordinator.grpc.pb.h"
using csce438::Coordinator;
using csce438::ServerInfo;
using csce438::Confirmation;

class ServerNode {
public:
    ServerNode(std::shared_ptr<Channel> channel)
        : stub_(Coordinator::NewStub(channel)) {}

    void SendHeartbeat(int cluster_id, int server_id, const std::string& ip, int port) {
        ServerInfo request;
        request.set_cluster_id(cluster_id);
        request.set_server_id(server_id);
        request.set_ip(ip);
        request.set_port(port);

        Confirmation reply;
        ClientContext context;

        Status status = stub_->Heartbeat(&context, request, &reply);
        if (status.ok()) {
            std::cout << "[HEARTBEAT] Server " << server_id << " in cluster " << cluster_id << " is alive." << std::endl;
        } else {
            std::cerr << "[HEARTBEAT ERROR] Failed to send heartbeat to Coordinator." << std::endl;
        }
    }

private:
    std::unique_ptr<Coordinator::Stub> stub_;
};

// 让 Heartbeat 在后台运行
void RunHeartbeat(ServerNode* server, int cluster_id, int server_id, const std::string& ip, int port) {
    while (true) {
        server->SendHeartbeat(cluster_id, server_id, ip, port);
        std::this_thread::sleep_for(std::chrono::seconds(5));  // 每 5 秒发送一次心跳
    }
}

/* ======= 原来的 tsd.cc 代码保留 ======= */

class SNSServiceImpl final : public SNSService::Service {
    // 保留你原来的代码
    Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
        std::string username = request->username();
        for (const auto& client : client_db) {
            list_reply->add_all_users(client->username);
        }

        Client* current_user = nullptr;
        for (const auto& client : client_db) {
            if (client->username == username) {
                current_user = client;
                break;
            }
        }
        for (const auto& follower : current_user->client_followers) {
            list_reply->add_followers(follower->username);
        }

        return Status::OK;
    }

    Status Timeline(ServerContext* context,
                    ServerReaderWriter<Message, Message>* stream) override {
        // 你的 Timeline 代码，未修改
        return Status::OK;
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

int main(int argc, char** argv) {
    int cluster_id = -1, server_id = -1, port = -1;
    std::string coordinator_host = "localhost";
    int coordinator_port = 9090;

    int opt;
    while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1) {
        switch (opt) {
            case 'c':
                cluster_id = std::stoi(optarg);
                break;
            case 's':
                server_id = std::stoi(optarg);
                break;
            case 'h':
                coordinator_host = optarg;
                break;
            case 'k':
                coordinator_port = std::stoi(optarg);
                break;
            case 'p':
                port = std::stoi(optarg);
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
                return 1;
        }
    }

    if (cluster_id == -1 || server_id == -1 || port == -1) {
        std::cerr << "Usage: ./tsd -c <cluster_id> -s <server_id> -h <coordinator_host> -k <coordinator_port> -p <server_port>\n";
        return 1;
    }

    std::string coordinator_address = coordinator_host + ":" + std::to_string(coordinator_port);
    ServerNode server(grpc::CreateChannel(coordinator_address, grpc::InsecureChannelCredentials()));

    // 启动心跳线程
    std::thread heartbeat_thread(RunHeartbeat, &server, cluster_id, server_id, "127.0.0.1", port);
    heartbeat_thread.detach();  // 让线程在后台运行

    std::string log_file_name = std::string("server-") + std::to_string(port);
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    RunServer(std::to_string(port));

    return 0;
}
