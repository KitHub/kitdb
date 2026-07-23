package component

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/robfig/cron/v3"
)

var cronComponentInstance *CronComponent
var onceForCronComponentInstance sync.Once = sync.Once{}

// CronTask 任务接口
type CronTask interface {
	Run()
	GetCronSpec() string // cron表达式
	GetName() string     // 任务名
}

type CronComponent struct {
	cron *cron.Cron
	// 保存任务ID，用于后续停止任务
	taskMap map[string]cron.EntryID
}

func NewCronConponent(ctx context.Context) *CronComponent {
	onceForCronComponentInstance.Do(func() {
		cronComponentInstance = &CronComponent{
			cron:    cron.New(cron.WithSeconds()),
			taskMap: make(map[string]cron.EntryID),
		}
	})
	return cronComponentInstance
}

// Register 注册定时任务
func (s *CronComponent) Register(ctx context.Context, task CronTask) error {
	entryID, err := s.cron.AddFunc(task.GetCronSpec(), func() {
		innerCtx := context.Background()
		defer func() {
			if err := recover(); err != nil {
				slog.ErrorContext(innerCtx, "task panic, recovered", slog.String("task_name", task.GetName()), slog.Any("error", err))
			}
		}()
		slog.InfoContext(innerCtx, "task run", slog.String("task_name", task.GetName()))
		task.Run()
		slog.InfoContext(innerCtx, "task run done", slog.String("task_name", task.GetName()))
	})
	if err != nil {
		slog.ErrorContext(ctx, "register task failed", slog.Any("error", err))
		return err
	}

	_, existed := s.taskMap[task.GetName()]
	if existed {
		slog.ErrorContext(ctx, "register task failed, dup task name", slog.String("task_name", task.GetName()))
		return errors.New("dup task name")
	}

	s.taskMap[task.GetName()] = entryID
	slog.InfoContext(ctx, "register task done", slog.String("task_name", task.GetName()), slog.Any("entryId", entryID))
	return nil
}

// Start 启动所有定时任务
func (s *CronComponent) Start(ctx context.Context) {
	s.cron.Start()
	slog.InfoContext(ctx, "CronComponent started")
}

// Stop 停止调度器
func (s *CronComponent) Stop(ctx context.Context) {
	cronCtx := s.cron.Stop()
	// 等待正在运行的任务执行完毕
	<-cronCtx.Done()
	slog.InfoContext(ctx, "CronComponent stopped")
}

// RemoveTask 移除单个任务
func (s *CronComponent) RemoveTask(ctx context.Context, taskName string) bool {
	entryID, ok := s.taskMap[taskName]
	if !ok {
		slog.WarnContext(ctx, "remove task failed, task not found", slog.String("task_name", taskName))
		return false
	}
	s.cron.Remove(entryID)
	delete(s.taskMap, taskName)
	slog.InfoContext(ctx, "remove task done", slog.String("task_name", taskName))
	return true
}
