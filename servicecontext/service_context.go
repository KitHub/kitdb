package servicecontext

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/KitHub/kitdb/component"
	"github.com/KitHub/kitdb/config"
	"github.com/KitHub/kitdb/dao"
	"github.com/KitHub/kitdb/logic"
	"github.com/KitHub/kitdb/service"
	"gopkg.in/natefinch/lumberjack.v2"
)

type ServiceContext struct {
	Logger            *slog.Logger
	CronComponent     *component.CronComponent
	InitComponent     *component.InitComponent
	ShutdownComponent *component.ShutdownComponent
	DBFileDao         *dao.DBFileDao
	DemoLogic         *logic.DemoLogic
	DemoService       *service.DemoService
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

		dataDir, innerErr := relToAbs(configEntity.DataConfig.Dir, 0)
		if innerErr != nil {
			slog.ErrorContext(ctx, "resolve data dir failed", slog.Any("error", innerErr))
			err = innerErr
			return
		}

		cronComponent := component.NewCronConponent(ctx)
		initComponent := component.NewInitComponent(ctx)
		shutdownComponent := component.NewShutdownComponent(ctx)
		dbFileDao := dao.NewDBFileDao(ctx, dataDir)
		demoLogic := logic.NewDemoLogic(ctx)
		demoService := service.NewDemoService(ctx, demoLogic)

		gServiceCtx = &ServiceContext{
			ShutdownComponent: shutdownComponent,
			InitComponent:     initComponent,
			Logger:            logger,
			CronComponent:     cronComponent,
			DBFileDao:         dbFileDao,
			DemoLogic:         demoLogic,
			DemoService:       demoService,
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

// relToAbs 将相对路径转为绝对路径，可选基于程序目录/当前工作目录
// baseMode: 0=当前工作目录  1=程序exe所在目录
func relToAbs(relPath string, baseMode int) (string, error) {
	var baseDir string
	var err error

	switch baseMode {
	case 1:
		exePath, err := os.Executable()
		if err != nil {
			return "", err
		}
		baseDir = filepath.Dir(exePath)
	default:
		baseDir, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}

	full := filepath.Join(baseDir, relPath)
	// 解析软链接并清理
	realPath, err := filepath.EvalSymlinks(full)
	if err != nil {
		return filepath.Abs(full)
	}
	return filepath.Clean(realPath), nil
}
