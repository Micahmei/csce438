// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: sns.proto

#include "sns.pb.h"
#include "sns.grpc.pb.h"

#include <functional>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/impl/client_unary_call.h>
#include <grpcpp/support/client_callback.h>
#include <grpcpp/support/message_allocator.h>
#include <grpcpp/support/method_handler.h>
#include <grpcpp/impl/rpc_service_method.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/impl/server_callback_handlers.h>
#include <grpcpp/server_context.h>
#include <grpcpp/impl/service_type.h>
#include <grpcpp/support/sync_stream.h>
namespace csce438 {

static const char* SNSService_method_names[] = {
  "/csce438.SNSService/Login",
  "/csce438.SNSService/List",
  "/csce438.SNSService/Follow",
  "/csce438.SNSService/UnFollow",
  "/csce438.SNSService/Timeline",
  "/csce438.SNSService/CheckConnection",
};

std::unique_ptr< SNSService::Stub> SNSService::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< SNSService::Stub> stub(new SNSService::Stub(channel, options));
  return stub;
}

SNSService::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options)
  : channel_(channel), rpcmethod_Login_(SNSService_method_names[0], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_List_(SNSService_method_names[1], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_Follow_(SNSService_method_names[2], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_UnFollow_(SNSService_method_names[3], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  , rpcmethod_Timeline_(SNSService_method_names[4], options.suffix_for_stats(),::grpc::internal::RpcMethod::BIDI_STREAMING, channel)
  , rpcmethod_CheckConnection_(SNSService_method_names[5], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  {}

::grpc::Status SNSService::Stub::Login(::grpc::ClientContext* context, const ::csce438::Request& request, ::csce438::Reply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_Login_, context, request, response);
}

void SNSService::Stub::async::Login(::grpc::ClientContext* context, const ::csce438::Request* request, ::csce438::Reply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_Login_, context, request, response, std::move(f));
}

void SNSService::Stub::async::Login(::grpc::ClientContext* context, const ::csce438::Request* request, ::csce438::Reply* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_Login_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::csce438::Reply>* SNSService::Stub::PrepareAsyncLoginRaw(::grpc::ClientContext* context, const ::csce438::Request& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::csce438::Reply, ::csce438::Request, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_Login_, context, request);
}

::grpc::ClientAsyncResponseReader< ::csce438::Reply>* SNSService::Stub::AsyncLoginRaw(::grpc::ClientContext* context, const ::csce438::Request& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncLoginRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status SNSService::Stub::List(::grpc::ClientContext* context, const ::csce438::Request& request, ::csce438::ListReply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::csce438::Request, ::csce438::ListReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_List_, context, request, response);
}

void SNSService::Stub::async::List(::grpc::ClientContext* context, const ::csce438::Request* request, ::csce438::ListReply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::csce438::Request, ::csce438::ListReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_List_, context, request, response, std::move(f));
}

void SNSService::Stub::async::List(::grpc::ClientContext* context, const ::csce438::Request* request, ::csce438::ListReply* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_List_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::csce438::ListReply>* SNSService::Stub::PrepareAsyncListRaw(::grpc::ClientContext* context, const ::csce438::Request& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::csce438::ListReply, ::csce438::Request, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_List_, context, request);
}

::grpc::ClientAsyncResponseReader< ::csce438::ListReply>* SNSService::Stub::AsyncListRaw(::grpc::ClientContext* context, const ::csce438::Request& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncListRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status SNSService::Stub::Follow(::grpc::ClientContext* context, const ::csce438::Request& request, ::csce438::Reply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_Follow_, context, request, response);
}

void SNSService::Stub::async::Follow(::grpc::ClientContext* context, const ::csce438::Request* request, ::csce438::Reply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_Follow_, context, request, response, std::move(f));
}

void SNSService::Stub::async::Follow(::grpc::ClientContext* context, const ::csce438::Request* request, ::csce438::Reply* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_Follow_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::csce438::Reply>* SNSService::Stub::PrepareAsyncFollowRaw(::grpc::ClientContext* context, const ::csce438::Request& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::csce438::Reply, ::csce438::Request, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_Follow_, context, request);
}

::grpc::ClientAsyncResponseReader< ::csce438::Reply>* SNSService::Stub::AsyncFollowRaw(::grpc::ClientContext* context, const ::csce438::Request& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncFollowRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::Status SNSService::Stub::UnFollow(::grpc::ClientContext* context, const ::csce438::Request& request, ::csce438::Reply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_UnFollow_, context, request, response);
}

void SNSService::Stub::async::UnFollow(::grpc::ClientContext* context, const ::csce438::Request* request, ::csce438::Reply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_UnFollow_, context, request, response, std::move(f));
}

void SNSService::Stub::async::UnFollow(::grpc::ClientContext* context, const ::csce438::Request* request, ::csce438::Reply* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_UnFollow_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::csce438::Reply>* SNSService::Stub::PrepareAsyncUnFollowRaw(::grpc::ClientContext* context, const ::csce438::Request& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::csce438::Reply, ::csce438::Request, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_UnFollow_, context, request);
}

::grpc::ClientAsyncResponseReader< ::csce438::Reply>* SNSService::Stub::AsyncUnFollowRaw(::grpc::ClientContext* context, const ::csce438::Request& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncUnFollowRaw(context, request, cq);
  result->StartCall();
  return result;
}

::grpc::ClientReaderWriter< ::csce438::Message, ::csce438::Message>* SNSService::Stub::TimelineRaw(::grpc::ClientContext* context) {
  return ::grpc::internal::ClientReaderWriterFactory< ::csce438::Message, ::csce438::Message>::Create(channel_.get(), rpcmethod_Timeline_, context);
}

void SNSService::Stub::async::Timeline(::grpc::ClientContext* context, ::grpc::ClientBidiReactor< ::csce438::Message,::csce438::Message>* reactor) {
  ::grpc::internal::ClientCallbackReaderWriterFactory< ::csce438::Message,::csce438::Message>::Create(stub_->channel_.get(), stub_->rpcmethod_Timeline_, context, reactor);
}

::grpc::ClientAsyncReaderWriter< ::csce438::Message, ::csce438::Message>* SNSService::Stub::AsyncTimelineRaw(::grpc::ClientContext* context, ::grpc::CompletionQueue* cq, void* tag) {
  return ::grpc::internal::ClientAsyncReaderWriterFactory< ::csce438::Message, ::csce438::Message>::Create(channel_.get(), cq, rpcmethod_Timeline_, context, true, tag);
}

::grpc::ClientAsyncReaderWriter< ::csce438::Message, ::csce438::Message>* SNSService::Stub::PrepareAsyncTimelineRaw(::grpc::ClientContext* context, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncReaderWriterFactory< ::csce438::Message, ::csce438::Message>::Create(channel_.get(), cq, rpcmethod_Timeline_, context, false, nullptr);
}

::grpc::Status SNSService::Stub::CheckConnection(::grpc::ClientContext* context, const ::csce438::Request& request, ::csce438::Reply* response) {
  return ::grpc::internal::BlockingUnaryCall< ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_CheckConnection_, context, request, response);
}

void SNSService::Stub::async::CheckConnection(::grpc::ClientContext* context, const ::csce438::Request* request, ::csce438::Reply* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_CheckConnection_, context, request, response, std::move(f));
}

void SNSService::Stub::async::CheckConnection(::grpc::ClientContext* context, const ::csce438::Request* request, ::csce438::Reply* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_CheckConnection_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::csce438::Reply>* SNSService::Stub::PrepareAsyncCheckConnectionRaw(::grpc::ClientContext* context, const ::csce438::Request& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::csce438::Reply, ::csce438::Request, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_CheckConnection_, context, request);
}

::grpc::ClientAsyncResponseReader< ::csce438::Reply>* SNSService::Stub::AsyncCheckConnectionRaw(::grpc::ClientContext* context, const ::csce438::Request& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncCheckConnectionRaw(context, request, cq);
  result->StartCall();
  return result;
}

SNSService::Service::Service() {
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      SNSService_method_names[0],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< SNSService::Service, ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](SNSService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::csce438::Request* req,
             ::csce438::Reply* resp) {
               return service->Login(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      SNSService_method_names[1],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< SNSService::Service, ::csce438::Request, ::csce438::ListReply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](SNSService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::csce438::Request* req,
             ::csce438::ListReply* resp) {
               return service->List(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      SNSService_method_names[2],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< SNSService::Service, ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](SNSService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::csce438::Request* req,
             ::csce438::Reply* resp) {
               return service->Follow(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      SNSService_method_names[3],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< SNSService::Service, ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](SNSService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::csce438::Request* req,
             ::csce438::Reply* resp) {
               return service->UnFollow(ctx, req, resp);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      SNSService_method_names[4],
      ::grpc::internal::RpcMethod::BIDI_STREAMING,
      new ::grpc::internal::BidiStreamingHandler< SNSService::Service, ::csce438::Message, ::csce438::Message>(
          [](SNSService::Service* service,
             ::grpc::ServerContext* ctx,
             ::grpc::ServerReaderWriter<::csce438::Message,
             ::csce438::Message>* stream) {
               return service->Timeline(ctx, stream);
             }, this)));
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      SNSService_method_names[5],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< SNSService::Service, ::csce438::Request, ::csce438::Reply, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](SNSService::Service* service,
             ::grpc::ServerContext* ctx,
             const ::csce438::Request* req,
             ::csce438::Reply* resp) {
               return service->CheckConnection(ctx, req, resp);
             }, this)));
}

SNSService::Service::~Service() {
}

::grpc::Status SNSService::Service::Login(::grpc::ServerContext* context, const ::csce438::Request* request, ::csce438::Reply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status SNSService::Service::List(::grpc::ServerContext* context, const ::csce438::Request* request, ::csce438::ListReply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status SNSService::Service::Follow(::grpc::ServerContext* context, const ::csce438::Request* request, ::csce438::Reply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status SNSService::Service::UnFollow(::grpc::ServerContext* context, const ::csce438::Request* request, ::csce438::Reply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status SNSService::Service::Timeline(::grpc::ServerContext* context, ::grpc::ServerReaderWriter< ::csce438::Message, ::csce438::Message>* stream) {
  (void) context;
  (void) stream;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

::grpc::Status SNSService::Service::CheckConnection(::grpc::ServerContext* context, const ::csce438::Request* request, ::csce438::Reply* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}


}  // namespace csce438

