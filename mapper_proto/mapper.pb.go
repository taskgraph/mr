// Code generated by protoc-gen-go.
// source: mapper.proto
// DO NOT EDIT!

/*
Package proto is a generated protocol buffer package.

It is generated from these files:
	mapper.proto

It has these top-level messages:
	MapperRequest
	MapperResponse
*/
package proto

import proto1 "github.com/golang/protobuf/proto"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto1.Marshal

type MapperRequest struct {
	Key   string `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value" json:"value,omitempty"`
}

func (m *MapperRequest) Reset()         { *m = MapperRequest{} }
func (m *MapperRequest) String() string { return proto1.CompactTextString(m) }
func (*MapperRequest) ProtoMessage()    {}

type MapperResponse struct {
	Key   string `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value" json:"value,omitempty"`
}

func (m *MapperResponse) Reset()         { *m = MapperResponse{} }
func (m *MapperResponse) String() string { return proto1.CompactTextString(m) }
func (*MapperResponse) ProtoMessage()    {}

func init() {
}

// Client API for Mapper service

type MapperClient interface {
	GetEmitResult(ctx context.Context, in *MapperRequest, opts ...grpc.CallOption) (Mapper_GetEmitResultClient, error)
}

type mapperClient struct {
	cc *grpc.ClientConn
}

func NewMapperClient(cc *grpc.ClientConn) MapperClient {
	return &mapperClient{cc}
}

func (c *mapperClient) GetEmitResult(ctx context.Context, in *MapperRequest, opts ...grpc.CallOption) (Mapper_GetEmitResultClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_Mapper_serviceDesc.Streams[0], c.cc, "/proto.Mapper/GetEmitResult", opts...)
	if err != nil {
		return nil, err
	}
	x := &mapperGetEmitResultClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Mapper_GetEmitResultClient interface {
	Recv() (*MapperResponse, error)
	grpc.ClientStream
}

type mapperGetEmitResultClient struct {
	grpc.ClientStream
}

func (x *mapperGetEmitResultClient) Recv() (*MapperResponse, error) {
	m := new(MapperResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Server API for Mapper service

type MapperServer interface {
	GetEmitResult(*MapperRequest, Mapper_GetEmitResultServer) error
}

func RegisterMapperServer(s *grpc.Server, srv MapperServer) {
	s.RegisterService(&_Mapper_serviceDesc, srv)
}

func _Mapper_GetEmitResult_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(MapperRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(MapperServer).GetEmitResult(m, &mapperGetEmitResultServer{stream})
}

type Mapper_GetEmitResultServer interface {
	Send(*MapperResponse) error
	grpc.ServerStream
}

type mapperGetEmitResultServer struct {
	grpc.ServerStream
}

func (x *mapperGetEmitResultServer) Send(m *MapperResponse) error {
	return x.ServerStream.SendMsg(m)
}

var _Mapper_serviceDesc = grpc.ServiceDesc{
	ServiceName: "proto.Mapper",
	HandlerType: (*MapperServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "GetEmitResult",
			Handler:       _Mapper_GetEmitResult_Handler,
			ServerStreams: true,
		},
	},
}
