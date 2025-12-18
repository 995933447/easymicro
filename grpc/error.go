package grpc

import (
	"github.com/995933447/easymicro/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const ErrCodeUnknown = -1

func newErrFromEnumWithMsg(err protoreflect.Enum, errMsg string) error {
	if errMsg == "" {
		options := err.Descriptor().Values().ByNumber(err.Number()).Options()
		if proto.HasExtension(options, pb.E_ErrMessage) {
			errMsg = proto.GetExtension(options, pb.E_ErrMessage).(*pb.ErrMessage).Message
		}
		if errMsg == "" {
			errMsg = string(err.Descriptor().Values().ByNumber(err.Number()).Name())
		}
	}
	return status.Errorf(codes.Code(err.Number()), errMsg)
}

func newErrFromEnum(err protoreflect.Enum) error {
	return newErrFromEnumWithMsg(err, "")
}

func NewRPCErrWithMsg(err protoreflect.Enum, errMsg string) error {
	return newErrFromEnumWithMsg(err, errMsg)
}

func NewRPCErr(err protoreflect.Enum) error {
	return newErrFromEnum(err)
}

func IsUnknownError(err error) bool {
	code := GetRPCErrCode(err)
	return code == protoreflect.EnumNumber(codes.Unknown) || code == -1
}

func GetRPCErrCode(err error) protoreflect.EnumNumber {
	st, ok := status.FromError(err)
	if ok {
		return protoreflect.EnumNumber(st.Code())
	}
	return ErrCodeUnknown
}

func GetRPCErrMsg(err error) string {
	st, ok := status.FromError(err)
	if ok {
		return st.Message()
	}
	return err.Error()
}

func IsRPCErr(err error, errCode protoreflect.EnumNumber) bool {
	return GetRPCErrCode(err) == errCode
}
