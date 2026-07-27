package service

import (
	"context"
	"log/slog"
	"sync"

	"github.com/KitHub/kitdb/logic"
	"github.com/KitHub/protocols/kitdb"
	"google.golang.org/grpc/codes"
)

var (
	storeServiceInstance *StoreService
	storeServiceOnce     sync.Once
)

type StoreService struct {
	kitdb.UnimplementedStoreEngineAPIServer
	storeLogic *logic.StoreLogic
}

// CreateDB implements [kitdb.StoreEngineAPIServer].
func (s *StoreService) CreateDB(ctx context.Context, req *kitdb.CreateDBRequest) (rsp *kitdb.CreateDBResponse, err error) {
	slog.InfoContext(ctx, "create db", slog.String("db", req.GetDb()))

	err = req.Validate()
	if err != nil {
		slog.WarnContext(ctx, "invalid request", slog.Any("req", req), slog.Any("error", err))
		rsp = createPBRspWithPBMessageType[kitdb.CreateDBResponse](ctx, codes.InvalidArgument, nil)
		return rsp, nil
	}

	err = s.storeLogic.CreateDB(ctx, req.GetDb())
	if err != nil {
		slog.ErrorContext(ctx, "create db failed", slog.String("db", req.GetDb()), slog.Any("error", err))
		rsp = createPBRspWithPBMessageType[kitdb.CreateDBResponse](ctx, codes.Internal, nil)
		return rsp, nil
	}

	rsp = createPBRspWithPBMessageType[kitdb.CreateDBResponse](ctx, codes.OK, kitdb.CreateDBResponseData{})

	slog.InfoContext(ctx, "create db done", slog.String("db", req.GetDb()))
	return rsp, nil
}

// ReadKey implements [kitdb.StoreAPIServer].
func (s *StoreService) ReadKey(ctx context.Context, req *kitdb.ReadKeyRequest) (rsp *kitdb.ReadKeyResponse, err error) {
	slog.InfoContext(ctx, "read key", slog.String("db", req.GetDb()), slog.String("key", req.GetKey()))

	err = req.Validate()
	if err != nil {
		slog.WarnContext(ctx, "invalid request", slog.Any("req", req), slog.Any("error", err))
		rsp = createPBRspWithPBMessageType[kitdb.ReadKeyResponse](ctx, codes.InvalidArgument, &kitdb.ReadKeyResponseData{})
		return rsp, nil
	}

	value, err := s.storeLogic.ReadKey(ctx, req.GetDb(), req.GetKey())
	if err != nil {
		slog.ErrorContext(ctx, "read key failed", slog.String("db", req.GetDb()), slog.String("key", req.GetKey()), slog.Any("error", err))
		rsp = createPBRspWithPBMessageType[kitdb.ReadKeyResponse](ctx, codes.Internal, nil)
		return rsp, nil
	}

	rsp = createPBRspWithPBMessageType[kitdb.ReadKeyResponse](ctx, codes.OK, &kitdb.ReadKeyResponseData{
		Value: value,
	})

	slog.InfoContext(ctx, "read key done", slog.String("db", req.GetDb()), slog.String("key", req.GetKey()), slog.String("value", rsp.GetData().GetValue()))
	return rsp, nil
}

// WriteKeyValue implements [kitdb.StoreAPIServer].
func (s *StoreService) WriteKeyValue(ctx context.Context, req *kitdb.WriteKeyValueRequest) (rsp *kitdb.WriteKeyValueResponse, err error) {
	slog.InfoContext(ctx, "write key value", slog.String("db", req.GetDb()), slog.String("key", req.GetKey()), slog.String("value", req.GetValue()))

	err = req.Validate()
	if err != nil {
		slog.WarnContext(ctx, "invalid request", slog.Any("req", req), slog.Any("error", err))
		rsp = createPBRspWithPBMessageType[kitdb.WriteKeyValueResponse](ctx, codes.InvalidArgument, nil)
		return rsp, nil
	}

	err = s.storeLogic.WriteKeyValue(ctx, req.GetDb(), req.GetKey(), req.GetValue())
	if err != nil {
		slog.ErrorContext(ctx, "write key value failed", slog.String("db", req.GetDb()), slog.String("key", req.GetKey()), slog.String("value", req.GetValue()), slog.Any("error", err))
		rsp = createPBRspWithPBMessageType[kitdb.WriteKeyValueResponse](ctx, codes.Internal, nil)
		return rsp, nil
	}

	rsp = createPBRspWithPBMessageType[kitdb.WriteKeyValueResponse](ctx, codes.OK, &kitdb.WriteKeyValueResponseData{})

	slog.InfoContext(ctx, "write key done", slog.String("db", req.GetDb()), slog.String("key", req.GetKey()), slog.String("value", req.GetValue()))
	return rsp, nil
}

func NewStoreService(ctx context.Context, storeLogic *logic.StoreLogic) *StoreService {
	storeServiceOnce.Do(func() {
		storeServiceInstance = &StoreService{
			storeLogic: storeLogic,
		}
	})
	return storeServiceInstance
}
