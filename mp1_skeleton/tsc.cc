#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <algorithm>
#include <grpc++/grpc++.h>
#include "client.h"

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"  // ✅ 确保 Coordinator 连接可用

using csce438::ListReply;
using csce438::Message;
using csce438::Reply;
using csce438::Request;
using csce438::SNSService;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;

// 处理 Ctrl+C 信号
void sig_ignore(int sig) {
    std::cout << "Signal caught " << sig << std::endl;
}

// 生成消息
Message MakeMessage(const std::string &username, const std::string &msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp *timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}

class Client : public IClient {
public:
    Client(const std::string &hname,
           const std::string &uname,
           const std::string &p)
        : hostname(hname), username(uname), port(p) {}

protected:
    virtual int connectTo();
    virtual IReply processCommand(std::string &input);
    virtual void processTimeline();

private:
    std::string hostname;
    std::string username;
    std::string port;
    std::unique_ptr<SNSService::Stub> stub_; // 服务器端 stub

    IReply Login();
    IReply List();
    IReply Follow(const std::string &username);
    IReply UnFollow(const std::string &username);
    void Timeline(const std::string &username);
};

// **连接 Coordinator 获取 Server**
Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
    std::lock_guard<std::mutex> lock(v_mutex);
    
    LOG(INFO) << "[DEBUG] Entering Heartbeat() function";
    
    if (!serverinfo) {
        LOG(ERROR) << "[ERROR] Received null ServerInfo!";
        return Status::CANCELLED;
    }

    int cluster_id = (serverinfo->serverid() - 1) % 3;
    int server_id = serverinfo->serverid();
    bool found = false;

    LOG(INFO) << "[DEBUG] Heartbeat received from Server ID: " << server_id 
              << " at " << serverinfo->hostname() << ":" << serverinfo->port();

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

    LOG(INFO) << "[DEBUG] Cluster " << cluster_id << " size after heartbeat: " << clusters[cluster_id].size();

    confirmation->set_status(true);
    return Status::OK;
}




IReply Client::processCommand(std::string &input) {
    IReply ire;
    std::istringstream iss(input);
    std::string command, argument;
    iss >> command;

    if (command == "FOLLOW") {
        iss >> argument;
        ire = Follow(argument);
    } else if (command == "UNFOLLOW") {
        iss >> argument;
        ire = UnFollow(argument);
    } else if (command == "LIST") {
        ire = List();
    } else if (command == "TIMELINE") {
        processTimeline();
    } else {
        ire.comm_status = FAILURE_INVALID;
    }
    return ire;
}

void Client::processTimeline() {
    Timeline(username);
}

// **List**
IReply Client::List() {
    IReply ire;
    Request request;
    request.set_username(username);

    ListReply reply;
    ClientContext context;

    Status status = stub_->List(&context, request, &reply);
    ire.grpc_status = status;
    if (status.ok()) {
        ire.comm_status = SUCCESS;
        ire.all_users.assign(reply.all_users().begin(), reply.all_users().end());
        ire.followers.assign(reply.followers().begin(), reply.followers().end());
    } else {
        ire.comm_status = FAILURE_UNKNOWN;
    }
    return ire;
}

// **Follow**
IReply Client::Follow(const std::string &username2) {
    IReply ire;
    Request request;
    request.set_username(username);
    request.add_arguments(username2);

    Reply reply;
    ClientContext context;

    Status status = stub_->Follow(&context, request, &reply);
    ire.grpc_status = status;
    ire.comm_status = (reply.msg() == "followed successful") ? SUCCESS : FAILURE_INVALID_USERNAME;
    return ire;
}

// **UnFollow**
IReply Client::UnFollow(const std::string &username2) {
    IReply ire;
    Request request;
    request.set_username(username);
    request.add_arguments(username2);

    Reply reply;
    ClientContext context;

    Status status = stub_->UnFollow(&context, request, &reply);
    ire.grpc_status = status;
    ire.comm_status = (reply.msg() == "unfollow successful") ? SUCCESS : FAILURE_INVALID_USERNAME;
    return ire;
}

// **Login**
IReply Client::Login() {
    IReply ire;
    Request request;
    request.set_username(username);

    Reply reply;
    ClientContext context;

    Status status = stub_->Login(&context, request, &reply);
    ire.grpc_status = status;
    ire.comm_status = (reply.msg() == "login successful") ? SUCCESS : FAILURE_ALREADY_EXISTS;
    return ire;
}

// **Timeline**
void Client::Timeline(const std::string &username) {
    ClientContext context;
    std::shared_ptr<ClientReaderWriter<Message, Message>> stream(
        stub_->Timeline(&context));

    Message init_msg;
    init_msg.set_username(username);
    if (!stream->Write(init_msg)) {
        std::cerr << "[Client] Failed to initialize timeline session." << std::endl;
        return;
    }

    std::thread reader([stream]() {
        Message msg;
        while (stream->Read(&msg)) {
            std::time_t timestamp = msg.timestamp().seconds();
            displayPostMessage(msg.username(), msg.msg(), timestamp);
        }
    });

    while (true) {
        std::string user_msg = getPostMessage();
        if (!user_msg.empty()) {
            Message new_msg = MakeMessage(username, user_msg);
            if (!stream->Write(new_msg)) {
                std::cerr << "[Client] Failed to send message, terminating session." << std::endl;
                break;
            }
        }
    }

    stream->WritesDone();
    reader.join();
}

// **主函数**
int main(int argc, char **argv) {
    std::string hostname = "127.0.0.1", username = "default", port = "9090", coordinator_port = "9090";

    int opt;
    while ((opt = getopt(argc, argv, "h:u:p:k:")) != -1) {
        switch (opt) {
            case 'h': hostname = optarg; break;
            case 'u': username = optarg; break;
            case 'p': port = optarg; break;
            case 'k': coordinator_port = optarg; break; // ✅ 让 `-k` 可用
            default:
                std::cerr << "Invalid Command Line Argument\n";
                return -1;
        }
    }

    std::cout << "Starting Client with:\n"
              << "Hostname: " << hostname << "\n"
              << "Username: " << username << "\n"
              << "Port: " << port << "\n"
              << "Coordinator Port: " << coordinator_port << std::endl;

    Client myc(hostname, username, port);
    myc.run();
    return 0;
}

