#include <iostream>
#include <thread>
#include <fstream>
#include <filesystem>
#include <unordered_set>
#include <semaphore.h>
#include <fcntl.h>
#include <unistd.h>
#include <grpc++/grpc++.h>
#include <google/protobuf/util/time_util.h>
#include <jsoncpp/json/json.h>
#include <amqp.h>
#include <amqp_tcp_socket.h>

#include "coordinator.grpc.pb.h"
#include "sns.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::ServerList;

int synchID;
int clusterID;
std::string coordAddr;
std::string subdir;

std::string getUserPath(const std::string& user, const std::string& file) {
    return "./cluster_" + std::to_string(clusterID) + "/" + subdir + "/" + user + "_" + file + ".txt";
}

bool file_contains(const std::string& filename, const std::string& user) {
    std::ifstream in(filename);
    std::string line;
    while (std::getline(in, line)) {
        if (line == user) return true;
    }
    return false;
}

void append_if_not_exists(const std::string& filename, const std::string& user) {
    if (!file_contains(filename, user)) {
        std::ofstream out(filename, std::ios::app);
        out << user << std::endl;
    }
}

class RabbitHandler {
    amqp_connection_state_t conn;
    amqp_channel_t channel;

public:
    RabbitHandler() : channel(1) {
        conn = amqp_new_connection();
        amqp_socket_t* socket = amqp_tcp_socket_new(conn);
        amqp_socket_open(socket, "rabbitmq", 5672);
        amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
        amqp_channel_open(conn, channel);
    }

    void declare(const std::string& q) {
        amqp_queue_declare(conn, channel, amqp_cstring_bytes(q.c_str()), 0, 0, 0, 0, amqp_empty_table);
    }

    void publish(const std::string& q, const std::string& message) {
        amqp_basic_publish(conn, channel, amqp_empty_bytes, amqp_cstring_bytes(q.c_str()), 0, 0, NULL, amqp_cstring_bytes(message.c_str()));
    }

    std::string consume(const std::string& q) {
        amqp_basic_consume(conn, channel, amqp_cstring_bytes(q.c_str()), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        amqp_envelope_t env;
        amqp_maybe_release_buffers(conn);

        struct timeval timeout{2, 0};
        auto res = amqp_consume_message(conn, &env, &timeout, 0);
        if (res.reply_type != AMQP_RESPONSE_NORMAL) return "";

        std::string msg((char*)env.message.body.bytes, env.message.body.len);
        amqp_destroy_envelope(&env);
        return msg;
    }
};

void publishAllUsers(RabbitHandler& rh) {
    std::unordered_set<std::string> users;
    for (const auto& entry : std::filesystem::directory_iterator("./cluster_" + std::to_string(clusterID) + "/" + subdir)) {
        auto path = entry.path().string();
        if (path.find("all_users.txt") != std::string::npos) continue;
        if (path.find("_followers.txt") != std::string::npos || path.find("_timeline.txt") != std::string::npos) continue;
        std::string name = entry.path().stem().string();
        users.insert(name.substr(0, name.find("_")));
    }
    Json::Value root;
    for (const auto& u : users) root["users"].append(u);
    Json::FastWriter writer;
    rh.publish("synch" + std::to_string(synchID) + "_users_queue", writer.write(root));
}

void consumeAllUsers(RabbitHandler& rh) {
    for (int i = 1; i <= 6; ++i) {
        auto msg = rh.consume("synch" + std::to_string(i) + "_users_queue");
        if (msg.empty()) continue;
        Json::Value root;
        Json::Reader reader;
        if (reader.parse(msg, root)) {
            for (auto& u : root["users"]) {
                std::string filename = "./cluster_" + std::to_string(clusterID) + "/" + subdir + "/all_users.txt";
                append_if_not_exists(filename, u.asString());
            }
        }
    }
}

void synchronizerLoop(RabbitHandler& rh) {
    while (true) {
        publishAllUsers(rh);
        consumeAllUsers(rh);
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

int main(int argc, char** argv) {
    std::string coord_ip = "127.0.0.1", coord_port = "9090", port = "9091";
    synchID = 1;

    int opt;
    while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1) {
        switch (opt) {
            case 'h': coord_ip = optarg; break;
            case 'k': coord_port = optarg; break;
            case 'p': port = optarg; break;
            case 'i': synchID = std::stoi(optarg); break;
        }
    }

    clusterID = ((synchID - 1) % 3) + 1;
    subdir = (synchID <= 3) ? "1" : "2";
    coordAddr = coord_ip + ":" + coord_port;

    ServerInfo info;
    info.set_serverid(synchID);
    info.set_clusterid(clusterID);
    info.set_hostname("127.0.0.1");
    info.set_port(port);
    info.set_type("synchronizer");
    info.set_ismaster(synchID <= 3);

    auto channel = grpc::CreateChannel(coordAddr, grpc::InsecureChannelCredentials());
    std::unique_ptr<CoordService::Stub> coord_stub = CoordService::NewStub(channel);
    Confirmation conf;
    ClientContext ctx;
    coord_stub->Heartbeat(&ctx, info, &conf);

    RabbitHandler rh;
    rh.declare("synch" + std::to_string(synchID) + "_users_queue");

    std::thread t(synchronizerLoop, std::ref(rh));
    t.join();
    return 0;
}
