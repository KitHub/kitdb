package service

import (
	"context"
	"sync"

	"github.com/KitHub/kitdb/logic"
	"github.com/KitHub/protocols/kitdb"
)

var (
	demoServiceInstance *DemoService
	demoServiceOnce     sync.Once
)

type DemoService struct {
    kitdb.UnimplementedDemoAPIServer
	demoLogic *logic.DemoLogic
}

// Demo implements [kitdb.DemoAPIServer].
func (d *DemoService) Demo(context.Context, *kitdb.DemoRequest) (*kitdb.DemoResponse, error) {
	panic("unimplemented")
}

func NewDemoService(ctx context.Context, demoLogic *logic.DemoLogic) *DemoService {
	demoServiceOnce.Do(func() {
		demoServiceInstance = &DemoService{
			demoLogic: demoLogic,
		}
	})
	return demoServiceInstance
}
