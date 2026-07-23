package logic

import (
	"context"
	"sync"
)

var demoLogic *DemoLogic
var onceDemoLogic sync.Once

type DemoLogic struct {
}

// Hello implements [DemoLogic].
func (d *DemoLogic) Hello(ctx context.Context) error {
	panic("unimplemented")
}

func NewDemoLogic(ctx context.Context) *DemoLogic {
	onceDemoLogic.Do(func() {
		demoLogic = &DemoLogic{}
	})
	return demoLogic
}
