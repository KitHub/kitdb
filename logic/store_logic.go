package logic

import (
	"context"
	"sync"
)

var storeLogic *StoreLogic
var onceStoreLogic sync.Once

type StoreLogic struct {
}

func NewStoreLogic(ctx context.Context) *StoreLogic {
	onceStoreLogic.Do(func() {
		storeLogic = &StoreLogic{}
	})
	return storeLogic
}
