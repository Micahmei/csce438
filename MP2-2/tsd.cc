#include <ctime>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <algorithm>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <thread>
#include <grpc++/grpc++.h>
#include <google/protobuf/util/time_util.h>
#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "slave.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;

using csce438::SNSService;
using csce438::Coordinator;
using csce438::MirrorService;
using csce438::MirrorRequest;
using csce438::MirrorPostRequest;
using csce438::MirrorReply;
using csce438::Request;
using csce438::Reply;
using csce438::Message;
using csce438::ListReply;
using csce438::ServerInfo;
using csce438::Confirmation;

struct Client {
    std::string username;
    ServerReaderWriter<Message, Message>* stream = nullptr;
    std::vector<Client*> followers;
    std::vector<Client*> following;
};

std::vector<Client*> client_db;

Client* getClient(const std::string& name) {
    for (auto* c : client_db) {
        if (c->username == name) return c;
    }
    return nullptr;
}

class SNSServiceImpl final : public SNSService::Service {
    std::unique_ptr<Coordinator::Stub> coord_stub;
    std::unique_ptr<MirrorService::Stub> slave_stub;
    bool is_master;
    int cluster_id, server_id;
    std::string port;

public:
    SNSServiceImpl(std::shared_ptr<Channel> coord_channel,
                   std::shared_ptr<Channel> slave_channel,
                   bool is_master_,
                   int cluster_id_,
                   int server_id_,
                   std::string port_)
        : coord_stub(Coordinator::NewStub(coord_channel)),
          slave_stub(MirrorService::NewStub(slave_channel)),
          is_master(is_master_),
          cluster_id(cluster_id_),
          server_id(server_id_),
          port(port_) {}

    void startHeartbeat() {
        std::thread([this]() {
            while (true) {
                ServerInfo info;
                info.set_serverid(server_id);
                info.set_clusterid(cluster_id);
                info.set_hostname("127.0.0.1");
                info.set_port(port);
                info.set_type("server");
                info.set_ismaster(is_master);
                Confirmation conf;
                ClientContext ctx;
                coord_stub->Heartbeat(&ctx, info, &conf);
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }
        }).detach();
    }

    Status Login(ServerContext* ctx, const Request* req, Reply* reply) override {
        if (!is_master) return Status::OK;

        if (getClient(req->username()) != nullptr) {
            reply->set_msg("user already exists");
        } else {
            Client* c = new Client{req->username()};
            client_db.push_back(c);
            reply->set_msg("login successful");

            MirrorRequest mreq;
            mreq.set_username(req->username());
            mreq.set_argument("");
            MirrorReply mrep;
            ClientContext ctx;
            slave_stub->MirrorLogin(&ctx, mreq, &mrep);
        }
        return Status::OK;
    }

    Status Follow(ServerContext* ctx, const Request* req, Reply* reply) override {
        if (!is_master) return Status::OK;

        Client* u = getClient(req->username());
        Client* target = getClient(req->arguments(0));
        if (!u || !target || u == target) {
            reply->set_msg("cannot follow");
            return Status::OK;
        }
        for (auto* f : u->following) {
            if (f == target) {
                reply->set_msg("already following");
                return Status::OK;
            }
        }
        u->following.push_back(target);
        target->followers.push_back(u);
        reply->set_msg("followed");

        MirrorRequest mreq;
        mreq.set_username(req->username());
        mreq.set_argument(req->arguments(0));
        MirrorReply mrep;
        ClientContext ctx;
        slave_stub->MirrorFollow(&ctx, mreq, &mrep);
        return Status::OK;
    }

    Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {
        Message init;
        if (!stream->Read(&init)) return Status::CANCELLED;
        Client* user = getClient(init.username());
        if (!user) return Status::CANCELLED;
        user->stream = stream;

        while (stream->Read(&init)) {
            std::string timestamp = google::protobuf::util::TimeUtil::ToString(init.timestamp());
            std::string msg = init.msg();

            for (Client* follower : user->followers) {
                if (follower->stream) {
                    follower->stream->Write(init);
                }
            }

            MirrorPostRequest post;
            post.set_username(init.username());
            post.set_post(msg);
            post.set_timestamp(timestamp);
            MirrorReply mrep;
            ClientContext ctx;
            slave_stub->MirrorPost(&ctx, post, &mrep);
        }

        user->stream = nullptr;
        return Status::OK;
    }
};

int main(int argc, char** argv) {
    int cluster = 1, sid = 1;
    std::string port = "10000", coord_ip = "127.0.0.1", coord_port = "9090";

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-c") cluster = std::stoi(argv[++i]);
        else if (arg == "-s") sid = std::stoi(argv[++i]);
        else if (arg == "-p") port = argv[++i];
        else if (arg == "-h") coord_ip = argv[++i];
        else if (arg == "-k") coord_port = argv[++i];
    }

    bool is_master = (sid == 1);
    std::string coord_addr = coord_ip + ":" + coord_port;
    std::string slave_addr = "127.0.0.1:" + (is_master ? "10001" : "10000");

    auto coord_channel = grpc::CreateChannel(coord_addr, grpc::InsecureChannelCredentials());
    auto slave_channel = grpc::CreateChannel(slave_addr, grpc::InsecureChannelCredentials());

    SNSServiceImpl service(coord_channel, slave_channel, is_master, cluster, sid, port);
    service.startHeartbeat();

    std::string addr = "0.0.0.0:" + port;
    ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << addr << (is_master ? " [MASTER]" : " [SLAVE]") << std::endl;
    server->Wait();
    return 0;
}
