package service

import (
	"context"
	"reflect"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func createPBRspWithPBMessageType[T any](ctx context.Context, code codes.Code, data interface{}) *T {
	var obj T
	pbMsg, ok := any(&obj).(proto.Message)
	if ok {
		fillReponseWithGRPCCode(ctx, pbMsg, code, data)
	}
	return &obj
}

func fillReponseWithGRPCCode(ctx context.Context, rsp proto.Message, code codes.Code, data interface{}) {
	setPbField(ctx, rsp, "err_code", int32(code))
	setPbField(ctx, rsp, "err_msg", code.String())
	if !isInterfaceValueNil(ctx, data) && hasFieldDefined(ctx, rsp, "data") {
		setPbField(ctx, rsp, "data", data)
	}
}

func setPbField(ctx context.Context, msg proto.Message, fieldName string, value any) bool {
	r := msg.ProtoReflect()
	fd := r.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	if fd == nil {
		return false
	}

	var setVal protoreflect.Value
	switch fd.Kind() {
	case protoreflect.MessageKind:
		// 嵌套protobuf message字段，必须用ValueOfMessage构造
		if pbMsg, ok := value.(proto.Message); ok {
			setVal = protoreflect.ValueOfMessage(pbMsg.ProtoReflect())
		} else {
			return false
		}
	default:
		// 普通基础类型 int32/string 直接用ValueOf
		setVal = protoreflect.ValueOf(value)
	}

	r.Set(fd, setVal)
	return true
}

func getPbField(ctx context.Context, msg proto.Message, fieldName string) (any, bool) {
	r := msg.ProtoReflect()
	fd := r.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	if fd == nil {
		return nil, false
	}
	val := r.Get(fd)
	return val.Interface(), true
}

func hasFieldDefined(ctx context.Context, msg proto.Message, fieldName string) bool {
	desc := msg.ProtoReflect().Descriptor()
	fd := desc.Fields().ByName(protoreflect.Name(fieldName))
	return fd != nil
}

func isInterfaceValueNil(ctx context.Context, v interface{}) bool {
	if v == nil {
		return true
	}
	val := reflect.ValueOf(v)
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
		return val.IsNil()
	default:
		// 值类型 int/string/struct 等不可能为nil
		return false
	}
}
