package grpc

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ParseGRPCMethod 把/<proto_package>.<ServiceName>/<MethodName> 解析成 <proto_package>.<ServiceName>和<MethodName>
func ParseGRPCMethod(fullMethod string) (service string, method string) {
	parts := strings.Split(fullMethod, "/")
	if len(parts) < 3 {
		return "", ""
	}

	// example: full method: /myapp.user.v1.UserService/GetUser
	// parts[1] = myapp.user.v1.UserService
	// parts[2] = GetUser
	return parts[1], parts[2]
}

func NewOutCtxFromInCtx(inCtx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(inCtx)
	return metadata.NewOutgoingContext(inCtx, md) // 完全继承inCtx的cancel deadline,以及md
}

func NewOutCtxReplaceInCtxMD(outCtx, inCtx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(inCtx)
	return metadata.NewOutgoingContext(outCtx, md) // 完全把out context的md设置为income context md
}

func MergeInCtxMDToOutCtx(outCtx, inCtx context.Context) context.Context {
	inMD, _ := metadata.FromIncomingContext(inCtx)
	outMD, _ := metadata.FromOutgoingContext(outCtx)
	mergedMD := metadata.Join(inMD, outMD)               // 合并，同名的key会追加在值数组
	return metadata.NewOutgoingContext(outCtx, mergedMD) // 仅合并了inCtx的md,其他还是保留outCtx,如cancel,deadline等
}

func MergeAndReplaceInCtxMDToOutCtx(outCtx, inCtx context.Context) context.Context {
	inMD, _ := metadata.FromIncomingContext(inCtx)
	outMD, _ := metadata.FromOutgoingContext(outCtx)
	merged := outMD.Copy()
	for k, vals := range inMD {
		merged[k] = vals // 覆盖
	}

	return metadata.NewOutgoingContext(outCtx, merged)
}

// FindMethodDescriptor 每个编译后的 proto Go 包会自动注册 protoreflect.FileDescriptor。
// 你只要导入那个包，就可以在 protoregistry.GlobalFiles 里查到。
func FindMethodDescriptor(serviceName, methodName string) (protoreflect.MethodDescriptor, bool) {
	var result protoreflect.MethodDescriptor

	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		serviceNum := fd.Services().Len()
		for i := 0; i < serviceNum; i++ {
			svc := fd.Services().Get(i)
			if string(svc.FullName()) == serviceName {
				m := svc.Methods().ByName(protoreflect.Name(methodName))
				if m != nil {
					result = m
					return false // stop iteration
				}
			}
		}
		return true
	})

	if result == nil {
		return nil, false
	}

	return result, true
}

// NewDynamicMessagesFromMethod 根据 gRPC 方法描述符构建输入输出 dynamicpb.Message
func NewDynamicMessagesFromMethod(methodDesc protoreflect.MethodDescriptor) (in, out *dynamicpb.Message) {
	in = dynamicpb.NewMessage(methodDesc.Input())
	out = dynamicpb.NewMessage(methodDesc.Output())
	return
}

func ConvertDynamicToReal(methodDesc protoreflect.MethodDescriptor, dyn proto.Message) (proto.Message, error) {
	full := string(methodDesc.Input().FullName())

	// 在全局 Message 类型注册器中查找
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(full))
	if err != nil {
		return nil, fmt.Errorf("find message type %s: %w", full, err)
	}

	// 创建一个新的具体实例（实现 proto.Message）
	concrete := mt.New().Interface().(proto.Message)

	// 使用 proto binary 或者 JSON(protojson) 作为中间格式都可以的
	b, err := proto.Marshal(dyn)
	if err != nil {
		return nil, fmt.Errorf("marshal dynamic to json: %w", err)
	}

	if err := proto.Unmarshal(b, concrete); err != nil {
		return nil, fmt.Errorf("unmarshal json to concrete %s: %w", full, err)
	}

	return concrete, nil
}
