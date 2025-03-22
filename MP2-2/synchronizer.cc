#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "coordinator.grpc.pb.h"
#include <semaphore.h>
#include <fcntl.h>
#include <amqpcpp.h>
#include <amqpcpp/libboostasio.h>
#include <boost/asio.hpp>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using sns::Coordinator;
using sns::ServerInfoRequest;
using sns::ServerInfoReply;

int cluster_id;
int sync_id;
std::string base_dir;
std::string sem_name = "/timeline_sem";

std::string coordinator_ip;
int coordinator_port;

bool is_master = false;

boost::asio::io_service io_service;
AMQP::TcpConnection* connection = nullptr;
AMQP::TcpChannel* channel = nullptr;

// ------------------------------
// 通用文件写入函数
// ------------------------------
void safeWrite(const std::string& filename, const std::string& content) {
    sem_t* sem = sem_open(sem_name.c_str(), O_CREAT, 0644, 1);
    sem_wait(sem);

    std::ofstream file(base_dir + "/" + filename, std::ios::app);
    if (file.is_open()) {
        file << content << std::endl;
        file.close();
    }

    sem_post(sem);
    sem_close(sem);
}

// ------------------------------
// 读取本地用户并广播到 RabbitMQ
// ------------------------------
void broadcastUsers() {
    std::ifstream in(base_dir + "/all_users.txt");
    std::string line;
    while (std::getline(in, line)) {
        channel->publish("amq.direct", "user_queue", line);
    }
}

// ------------------------------
// 处理收到的用户信息
// ------------------------------
void consumeUsers(const std::string& msg) {
    safeWrite("all_users.txt", msg);
}

// ------------------------------
// 启动 RabbitMQ 并订阅队列
// ------------------------------
void startRabbitMQ() {
    AMQP::LibBoostAsioHandler handler(io_service);
    AMQP::Address addr("amqp://guest:guest@localhost/");
    connection = new AMQP::TcpConnection(&handler, addr);
    channel = new AMQP::TcpChannel(connection);

    // 声明并绑定队列
    channel->declareQueue("user_queue");
    channel->consume("user_queue").onReceived([](const AMQP::Message& message, uint64_t tag, bool redelivered) {
        std::string msg(message.body(), message.bodySize());
        consumeUsers(msg);
    });
}

// ------------------------------
// 与 Coordinator 通信，获取 cluster 信息
// ------------------------------
void askCoordinator() {
    auto stub = Coordinator::NewStub(grpc::CreateChannel(coordinator_ip + ":" + std::to_string(coordinator_port), grpc::InsecureChannelCredentials()));

    ServerInfoRequest req;
    ServerInfoReply rep;
    ClientContext ctx;

    req.set_clientid(sync_id); // reuse sync_id
    stub->GetClusterForClient(&ctx, req, &rep);

    cluster_id = rep.clusterid();
    base_dir = "./cluster" + std::to_string(cluster_id) + "/" + std::to_string(sync_id <= 3 ? 1 : 2);
    is_master = (sync_id <= 3);
}

// ------------------------------
// 定时广播线程
// ------------------------------
void syncThread() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        if (is_master) {
            broadcastUsers();
        }
    }
}

// ------------------------------
// 主函数入口
// ------------------------------
int main(int argc, char** argv) {
    if (argc != 9) {
        std::cerr << "Usage: ./synchronizer -h <coordIP> -k <coordPort> -p <port> -i <syncID>" << std::endl;
        return 1;
    }

    coordinator_ip = argv[2];
    coordinator_port = std::stoi(argv[4]);
    sync_id = std::stoi(argv[8]);

    askCoordinator();
    std::cout << "[Synchronizer] Cluster " << cluster_id << ", ID " << sync_id << " (" << (is_master ? "Master" : "Slave") << ")" << std::endl;

    startRabbitMQ();

    std::thread t(syncThread);
    io_service.run();

    t.join();
    return 0;
}
