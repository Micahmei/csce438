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
int Client::connectTo() {
    grpc::ChannelArguments ch_args;
    ch_args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
    ServerInfo serverinfo; 
    std::string coordinator_address = hostname + ":" + port;
    auto coordinator_channel = grpc::CreateCustomChannel(coordinator_address, grpc::InsecureChannelCredentials(), ch_args);
    std::cerr << "[Coordinator] Received heartbeat from Server " << serverinfo.serverid() 
          << " at " << serverinfo->hostname() << ":" << serverinfo->port() << std::endl;

    std::unique_ptr<CoordService::Stub> coordinator_stub = CoordService::NewStub(coordinator_channel);
    std::cerr << "[Client] GetServer response: " << (status.ok() ? "OK" : status.error_message()) << std::endl;

    
    ID client_id;
    client_id.set_id(stoi(username));
    ServerInfo server_info;
    ClientContext context;

    Status status = coordinator_stub->GetServer(&context, client_id, &server_info);
    if (!status.ok()) {
        std::cerr << "Failed to get Server info from Coordinator: " << status.error_message() << std::endl;
        return -1;
    }

    std::string server_address = server_info.hostname() + ":" + server_info.port();
    std::cout << "[Client] Assigned Server: " << server_address << std::endl;

    auto channel = grpc::CreateCustomChannel(server_address, grpc::InsecureChannelCredentials(), ch_args);
    stub_ = SNSService::NewStub(channel);

    if (!stub_) {
        std::cerr << "Failed to create gRPC stub for Server." << std::endl;
        return -1;
    }

    IReply reply = Login();
    return reply.grpc_status.ok() && reply.comm_status != FAILURE_ALREADY_EXISTS ? 1 : -1;
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

