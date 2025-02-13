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
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"


using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;


struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;


class SNSServiceImpl final : public SNSService::Service {
  
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    /*********
    YOUR CODE HERE
    **********/
   //Get the username
   std::string username = request->username();
   for(const auto& client : client_db){
    list_reply->add_user(client->username);
   }
   // add the user
   Client *cur_user = nullptr;
   for(auto& client : client_db){
    if(client->username == username){
      cur_user = client;
      break;
      }
    }
    // add the followers
    for(const auto& follower : cur_user->client_followers){
      list_reply->add_follower(follower->username);
    } 


    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {

    /*********
    YOUR CODE HERE
    **********/
    std::string u1 = request->username();
    std::string u2 = request->argements(0);
    //check if the user is already following the user
    if (u1 == u2)
    {
      reply->set_msg("cannot follow self");
      return Status::OK;
    }
    Client *follower = nullptr;
    Client *followed = nullptr;
    for(auto& client : client_db){
      if(client->username == u1){
        follower = client;
      }
      if(client->username == u2){
        followed = client;
      }
    }
    // if the followed is none
    if(followed == nullptr){
      reply->set_message("User not found");
      return Status::OK;
    }
    for(const auto &following : follower->client_following){
      if(following->username == u2){
        reply->set_message("Already following this user");
        return Status::OK;
      }
    }

    // update the follower's following list
    follower->client_following.push_back(followed);
    // update the followed's follower list
    followed->client_followers.push_back(follower);
    follow_time[u1][u2] = std::time(nullptr);
    reply->set_message("Followed successfully");
    retuen Status::OK;
  }
  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {

    /*********
    YOUR CODE HERE
    **********/
    std::string username = request->username();
    std::string unfollow_username = request->argements(0);
   
   // check
   if(username == unfollow_username){
      reply->set_message("cannot unfollow self");
      return Status::OK;
   }
    Client *user = nullptr;
    Client *unfollow_user = nullptr;
    for(auto& client : client_db){
      if(client->username == username){
        user = client;
      }
      if(client->username == unfollow_username){
        unfollow_user = client;
      }
    }
    // if the followed is none
    if(!user || !unfollow_user){
      reply->set_message("User not found");
      return Status::OK;
    }
    auto a = std::find(user->client_following.begin(), user->client_following.end(), unfollow_user);
    if (a != user->client_following.end()){
      user->client_following.erase(a);
      auto follower_a = std::find(unfollow_user->client_followers.begin(), unfollow_user->client_followers.end(), user);
      if (follower_a != unfollow_user->client_followers.end()){
        unfollow_user->client_followers.erase(follower_a);
      }
      reply->set_message("Unfollowed successfully");
    }
    else{
      reply->set_message("Not following this user");
    }
    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {

    /*********
    YOUR CODE HERE
    **********/
   //print the info
    std::cout<<"Login request received from client: ["<<request->username()<<"]";
    std::string username = request->username();
    for (Client *client : client_db){
      if (client->username == username){
        std::cout<<"User already exists"<<std::endl;
        reply->set_message("User already exists");
        return Status::OK;
      }
    }
    Client *new_client = new Client();
    new_client->username = username;
    new_client->connected = true;
    client_db.push_back(new_client);
    std::cout<<"Login successful"<<std::endl;
    reply->set_message("Login successful");
    
    return Status::OK;
  }

  Status Timeline(ServerContext *context,
                  ServerReaderWriter<Message, Message> *stream) override
  {

    /*********
    YOUR CODE HERE
    **********/
    std::string username;
    Message msg;

    // get username from the initial message
    if (stream->Read(&msg))
    {
      username = msg.username();
    }
    else
    {
      return Status::CANCELLED;
    }

    std::string timeline_file = "./timelines/" + username + ".txt";

    if (!std::filesystem::exists("./timelines"))
    {
      std::filesystem::create_directory("./timelines");
    }

    // get client of user
    Client *client = nullptr;
    for (Client *c : client_db)
    {
      if (c->username == username)
      {
        client = c;
        break;
      }
    }
    if (!client)
    {
      return Status::CANCELLED;
    }

    // read the posts of the followings of the user
    std::vector<Post> history_posts;
    for (Client *followed_user : client->client_following)
    {
      std::string followed_timeline = "./timelines/" + followed_user->username + ".txt";
      std::ifstream file(followed_timeline);
      std::string line, post;
      int count = 0;

      while (std::getline(file, line))
      {
        if (line.empty() && !post.empty())
        {
          // for the post
          std::istringstream post_stream(post);
          std::string line, post_username, post_content;
          std::time_t post_timestamp;

          while (std::getline(post_stream, line))
          {
            if (line[0] == 'T')
            {
              post_timestamp = iso8601ToTimeT(line.substr(2));
            }
            else if (line[0] == 'U')
            {
              post_username = line.substr(2);
            }
            else if (line[0] == 'W')
            {
              post_content = line.substr(2);
            }
          }

          std::time_t follow_timestamp = follow_time[username][post_username];
          if (post_timestamp >= follow_timestamp)
          {
            history_posts.push_back({post_username, post_content, post_timestamp});
          }
          post.clear();
          count++;
        }
        else
        {
          post += line + "\n";
        }
      }
    }

    std::sort(history_posts.begin(), history_posts.end(), [](const Post &a, const Post &b)
              { return a.timestamp > b.timestamp; });

    auto n = std::min((size_t)20, history_posts.size());
    for (int i = 0; i < n; i++)
    {
      auto post = history_posts[i];
      Message message;
      message.set_username(post.username);
      message.set_msg(post.post);
      message.mutable_timestamp()->set_seconds(post.timestamp);
      message.mutable_timestamp()->set_nanos(0);
      stream->Write(message);
    }

    // allow the user to subscribe to new messages from the followings
    client->stream = stream;

    // listen the inputs from clients
    while (stream->Read(&msg))
    {
      std::string new_post = "T " + google::protobuf::util::TimeUtil::ToString(msg.timestamp()) +
                             "\nU " + msg.username() +
                             "\nW " + msg.msg() + "\n\n";

      std::ofstream file(timeline_file, std::ios::app);
      file << new_post;
      file.close();

      // send the post to all followers of the user
      for (Client *follower : client->client_followers)
      {
        if (follower->stream)
        {
          follower->stream->Write(msg);
        }
      }
    }

    client->stream = nullptr;

    return Status::OK;
  }
};

void RunServer(std::string port_no) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  server->Wait();
}

int main(int argc, char** argv) {

  std::string port = "3010";
  
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(port);

  return 0;
}
