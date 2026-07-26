package service

import (
	"context"
	"log/slog"
	"sync"

	"github.com/KitHub/kitdb/logic"
	"github.com/KitHub/protocols/kitdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	storeServiceInstance *StoreService
	storeServiceOnce     sync.Once
)

type StoreService struct {
	kitdb.UnimplementedStoreAPIServer
	demoLogic *logic.DemoLogic
}

// ReadKey implements [kitdb.StoreAPIServer].
func (s *StoreService) ReadKey(ctx context.Context, req *kitdb.ReadKeyRequest) (rsp *kitdb.ReadKeyResponse, err error) {
	slog.InfoContext(ctx, "read key", slog.String("db", req.GetDb()), slog.String("key", req.GetKey()))

	err = req.Validate()
	if err != nil {
		slog.WarnContext(ctx, "invalid request", slog.Any("req", req), slog.Any("error", err))
		return nil, status.Error(codes.InvalidArgument, "invalid request parameters")
	}

	slog.InfoContext(ctx, "read key done", slog.String("db", req.GetDb()), slog.String("key", req.GetKey()), slog.String("value", rsp.GetData().GetValue()))
	return rsp, nil
}

// WriteKeyValue implements [kitdb.StoreAPIServer].
func (s *StoreService) WriteKeyValue(ctx context.Context, req *kitdb.WriteKeyValueRequest) (rsp *kitdb.WriteKeyValueResponse, err error) {
	slog.InfoContext(ctx, "write key value", slog.String("db", req.GetDb()), slog.String("key", req.GetKey()), slog.String("value", req.GetValue()))

	err = req.Validate()
	if err != nil {
		slog.WarnContext(ctx, "invalid request", slog.Any("req", req), slog.Any("error", err))
		return nil, status.Error(codes.InvalidArgument, "invalid request parameters")
	}

	slog.InfoContext(ctx, "read key done", slog.String("db", req.GetDb()), slog.String("key", req.GetKey()), slog.String("value", req.GetValue()))
	return rsp, nil
}

func NewStoreService(ctx context.Context, demoLogic *logic.DemoLogic) *StoreService {
	storeServiceOnce.Do(func() {
		storeServiceInstance = &StoreService{
			demoLogic: demoLogic,
		}
	})
	return storeServiceInstance
}
