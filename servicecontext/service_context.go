package servicecontext

import (
	"context"
	"log/slog"
	"sync"

	"github.com/KitHub/kitdb/component"
	"github.com/KitHub/kitdb/config"
	"github.com/KitHub/kitdb/logic"
	"github.com/KitHub/kitdb/service"
    "gopkg.in/natefinch/lumberjack.v2"
)

type ServiceContext struct {
	Logger         *slog.Logger
    CronComponent  *component.CronComponent
    InitComponent  *component.InitComponent
	ShutdownComponent  *component.ShutdownComponent
	DemoLogic   *logic.DemoLogic
	DemoService *service.DemoService
}

var gServiceCtx *ServiceContext
var once sync.Once

func InitServiceContext(ctx context.Context, configEntity *config.ConfigEntity) (
	serviceCtx *ServiceContext, err error) {
	slog.InfoContext(ctx, "init service context")

	once.Do(func() {
		logger, innerErr := initLog(ctx, configEntity.LogConfig)
		if innerErr != nil {
			slog.ErrorContext(ctx, "init log failed", slog.Any("error", innerErr))
			err = innerErr
			return
		}

        cronComponent := component.NewCronConponent(ctx)
        initComponent := component.NewInitComponent(ctx)
		shutdownComponent := component.NewShutdownComponent(ctx)
		demoLogic := logic.NewDemoLogic(ctx)
		demoService := service.NewDemoService(ctx, demoLogic)

		gServiceCtx = &ServiceContext{
			ShutdownComponent: shutdownComponent,
            InitComponent: initComponent,
			DemoLogic:     demoLogic,
			DemoService:   demoService,
			Logger:        logger,
            CronComponent:  cronComponent,
		}
	})

	slog.InfoContext(ctx, "init service context done")
	return gServiceCtx, err
}

func initLog(ctx context.Context, logConfig *config.LogConfigEntity) (
	*slog.Logger, error) {
	log := &lumberjack.Logger{
		Filename:   logConfig.Filename,   // 日志文件路径
		MaxSize:    logConfig.MaxSize,    // 每个日志文件的最大大小（以MB为单位）
		MaxBackups: logConfig.MaxBackups, // 保留旧文件的最大数量
		MaxAge:     logConfig.MaxAge,     // 保留旧文件的最大天数
		Compress:   logConfig.Compress,   // 是否压缩旧文件
		LocalTime:  logConfig.LocalTime,  // 是否使用本地时间戳
	}
	serviceLogger := slog.New(slog.NewTextHandler(log, nil))
	slog.SetDefault(serviceLogger)
	return serviceLogger, nil
}

func GetServiceContext() *ServiceContext {
	return gServiceCtx
}
