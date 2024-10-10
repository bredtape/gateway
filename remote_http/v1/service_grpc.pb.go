// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v3.13.0
// source: remote_http/v1/service.proto

package v1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	RemoteHttpService_Get_FullMethodName = "/com.github.bredtape.gateway.remote_http.v1.RemoteHttpService/Get"
)

// RemoteHttpServiceClient is the client API for RemoteHttpService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RemoteHttpServiceClient interface {
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
}

type remoteHttpServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewRemoteHttpServiceClient(cc grpc.ClientConnInterface) RemoteHttpServiceClient {
	return &remoteHttpServiceClient{cc}
}

func (c *remoteHttpServiceClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, RemoteHttpService_Get_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RemoteHttpServiceServer is the server API for RemoteHttpService service.
// All implementations must embed UnimplementedRemoteHttpServiceServer
// for forward compatibility.
type RemoteHttpServiceServer interface {
	Get(context.Context, *GetRequest) (*GetResponse, error)
	mustEmbedUnimplementedRemoteHttpServiceServer()
}

// UnimplementedRemoteHttpServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedRemoteHttpServiceServer struct{}

func (UnimplementedRemoteHttpServiceServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (UnimplementedRemoteHttpServiceServer) mustEmbedUnimplementedRemoteHttpServiceServer() {}
func (UnimplementedRemoteHttpServiceServer) testEmbeddedByValue()                           {}

// UnsafeRemoteHttpServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RemoteHttpServiceServer will
// result in compilation errors.
type UnsafeRemoteHttpServiceServer interface {
	mustEmbedUnimplementedRemoteHttpServiceServer()
}

func RegisterRemoteHttpServiceServer(s grpc.ServiceRegistrar, srv RemoteHttpServiceServer) {
	// If the following call pancis, it indicates UnimplementedRemoteHttpServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&RemoteHttpService_ServiceDesc, srv)
}

func _RemoteHttpService_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RemoteHttpServiceServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RemoteHttpService_Get_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RemoteHttpServiceServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RemoteHttpService_ServiceDesc is the grpc.ServiceDesc for RemoteHttpService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RemoteHttpService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "com.github.bredtape.gateway.remote_http.v1.RemoteHttpService",
	HandlerType: (*RemoteHttpServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _RemoteHttpService_Get_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "remote_http/v1/service.proto",
}