#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "coordinator.grpc.pb.h"
#include "server.grpc.pb.h" // 你需要定义 Master-to-Slave proto
#include <semaphore.h>
#include <fcntl.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using sns::ServerService;
using sns::SyncRequest;
using sns::SyncReply;

std::string base_dir;
bool is_master = false;
std::string coordinator_ip;
int coordinator_port;
int server_port;
int cluster_id;
int server_id;
std::string slave_ip = "localhost";
int slave_port = -1;

// named semaphore name
std::string sem_name = "/timeline_sem";

// write file with semaphore protection
void writeToFile(const std::string& filename, const std::string& content) {
    sem_t* sem = sem_open(sem_name.c_str(), O_CREAT, 0644, 1);
    sem_wait(sem);

    std::ofstream file(filename, std::ios::app);
    if (file.is_open()) {
        file << content << std::endl;
        file.close();
    }

    sem_post(sem);
    sem_close(sem);
}

class ServerServiceImpl final : public ServerService::Service {
    // Slave 接口：接收 Master 的同步请求
    Status Sync(ServerContext* context, const SyncRequest* request, SyncReply* reply) override {
        std::string file = request->filename();
        std::string data = request->data();

        std::string fullpath = base_dir + "/" + file;
        writeToFile(fullpath, data);

        reply->set_status("OK");
        return Status::OK;
    }
};

// Master 向 Slave 发送同步调用
void sendToSlave(const std::string& file, const std::string& data) {
    grpc::ChannelArguments ch_args;
    auto channel = grpc::CreateCustomChannel(slave_ip + ":" + std::to_string(slave_port), grpc::InsecureChannelCredentials(), ch_args);
    std::unique_ptr<ServerService::Stub> stub = ServerService::NewStub(channel);

    SyncRequest request;
    SyncReply reply;
    grpc::ClientContext context;

    request.set_filename(file);
    request.set_data(data);

    Status status = stub->Sync(&context, request, &reply);
    if (!status.ok()) {
        std::cerr << "Failed to sync with slave: " << status.error_message() << std::endl;
    }
}

// 心跳线程：每 5 秒发送一次
void heartbeatThread() {
    auto stub = sns::Coordinator::NewStub(grpc::CreateChannel(coordinator_ip + ":" + std::to_string(coordinator_port), grpc::InsecureChannelCredentials()));
    while (true) {
        sns::HeartbeatRequest req;
        sns::HeartbeatReply rep;
        grpc::ClientContext ctx;

        req.set_clusterid(cluster_id);
        req.set_serverid(server_id);

        stub->Heartbeat(&ctx, req, &rep);
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

// 注册到 Coordinator
void registerWithCoordinator() {
    auto stub = sns::Coordinator::NewStub(grpc::CreateChannel(coordinator_ip + ":" + std::to_string(coordinator_port), grpc::InsecureChannelCredentials()));
    sns::RegisterRequest req;
    sns::RegisterReply rep;
    grpc::ClientContext ctx;

    req.set_clusterid(cluster_id);
    req.set_serverid(server_id);
    req.set_ip("localhost");
    req.set_port(server_port);
    req.set_ismaster(is_master);

    stub->Register(&ctx, req, &rep);
}

// 主逻辑：模拟处理客户端命令并同步给 Slave
void simulatePost(const std::string& user, const std::string& content) {
    std::string timeline_file = user + "_timeline.txt";
    writeToFile(base_dir + "/" + timeline_file, content);
    if (is_master && slave_port > 0) {
        sendToSlave(timeline_file, content);
    }
}

void runServer() {
    std::string server_address = "0.0.0.0:" + std::to_string(server_port);
    ServerServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << (is_master ? "[Master]" : "[Slave]") << " Server running at " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    if (argc != 9) {
        std::cerr << "Usage: ./tsd -c <clusterId> -s <serverId> -h <coordIP> -k <coordPort> -p <serverPort>" << std::endl;
        return 1;
    }

    cluster_id = std::stoi(argv[2]);
    server_id = std::stoi(argv[4]);
    coordinator_ip = argv[6];
    coordinator_port = std::stoi(argv[8]);
    server_port = std::stoi(argv[10]);

    is_master = (server_id == 1);
    base_dir = "./cluster" + std::to_string(cluster_id) + "/" + std::to_string(server_id);

    if (is_master) {
        slave_port = server_port + 1; // 你可以自定义
    }

    registerWithCoordinator();
    std::thread heartbeat(heartbeatThread);
    heartbeat.detach();

    runServer();
    return 0;
}
