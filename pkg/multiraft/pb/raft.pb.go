// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.18.1
// source: pkg/multiraft/pb/raft.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type RaftMessageReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ReplicaID uint32 `protobuf:"varint,1,opt,name=replicaID,proto3" json:"replicaID,omitempty"`
	Message   []byte `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *RaftMessageReq) Reset() {
	*x = RaftMessageReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_multiraft_pb_raft_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RaftMessageReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RaftMessageReq) ProtoMessage() {}

func (x *RaftMessageReq) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_multiraft_pb_raft_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RaftMessageReq.ProtoReflect.Descriptor instead.
func (*RaftMessageReq) Descriptor() ([]byte, []int) {
	return file_pkg_multiraft_pb_raft_proto_rawDescGZIP(), []int{0}
}

func (x *RaftMessageReq) GetReplicaID() uint32 {
	if x != nil {
		return x.ReplicaID
	}
	return 0
}

func (x *RaftMessageReq) GetMessage() []byte {
	if x != nil {
		return x.Message
	}
	return nil
}

var File_pkg_multiraft_pb_raft_proto protoreflect.FileDescriptor

var file_pkg_multiraft_pb_raft_proto_rawDesc = []byte{
	0x0a, 0x1b, 0x70, 0x6b, 0x67, 0x2f, 0x6d, 0x75, 0x6c, 0x74, 0x69, 0x72, 0x61, 0x66, 0x74, 0x2f,
	0x70, 0x62, 0x2f, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x02, 0x70,
	0x62, 0x22, 0x48, 0x0a, 0x0e, 0x52, 0x61, 0x66, 0x74, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x52, 0x65, 0x71, 0x12, 0x1c, 0x0a, 0x09, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x49, 0x44,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x09, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x49,
	0x44, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0c, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x42, 0x07, 0x5a, 0x05, 0x2e,
	0x2f, 0x3b, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_multiraft_pb_raft_proto_rawDescOnce sync.Once
	file_pkg_multiraft_pb_raft_proto_rawDescData = file_pkg_multiraft_pb_raft_proto_rawDesc
)

func file_pkg_multiraft_pb_raft_proto_rawDescGZIP() []byte {
	file_pkg_multiraft_pb_raft_proto_rawDescOnce.Do(func() {
		file_pkg_multiraft_pb_raft_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_multiraft_pb_raft_proto_rawDescData)
	})
	return file_pkg_multiraft_pb_raft_proto_rawDescData
}

var file_pkg_multiraft_pb_raft_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_pkg_multiraft_pb_raft_proto_goTypes = []interface{}{
	(*RaftMessageReq)(nil), // 0: pb.RaftMessageReq
}
var file_pkg_multiraft_pb_raft_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_pkg_multiraft_pb_raft_proto_init() }
func file_pkg_multiraft_pb_raft_proto_init() {
	if File_pkg_multiraft_pb_raft_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_multiraft_pb_raft_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RaftMessageReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_multiraft_pb_raft_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_multiraft_pb_raft_proto_goTypes,
		DependencyIndexes: file_pkg_multiraft_pb_raft_proto_depIdxs,
		MessageInfos:      file_pkg_multiraft_pb_raft_proto_msgTypes,
	}.Build()
	File_pkg_multiraft_pb_raft_proto = out.File
	file_pkg_multiraft_pb_raft_proto_rawDesc = nil
	file_pkg_multiraft_pb_raft_proto_goTypes = nil
	file_pkg_multiraft_pb_raft_proto_depIdxs = nil
}
