package servicecontext

import (
	"context"
	"fmt"
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
	StoreEngine       dao.StorageEngine
	StoreLogic        *logic.StoreLogic
	StoreService      *service.StoreService
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

		dataDir, innerErr := prepareDir(ctx, configEntity.DataConfig.Dir)
		if innerErr != nil {
			slog.ErrorContext(ctx, "resolve data dir failed", slog.Any("error", innerErr))
			err = innerErr
			return
		}

		cronComponent := component.NewCronConponent(ctx)
		initComponent := component.NewInitComponent(ctx)
		shutdownComponent := component.NewShutdownComponent(ctx)
		storeEngine := dao.NewNineSunsStorageEngine(ctx, dataDir, initComponent, shutdownComponent)
		storeLogic := logic.NewStoreLogic(ctx, initComponent, storeEngine)
		storeService := service.NewStoreService(ctx, storeLogic)

		gServiceCtx = &ServiceContext{
			ShutdownComponent: shutdownComponent,
			InitComponent:     initComponent,
			Logger:            logger,
			CronComponent:     cronComponent,
			StoreEngine:       storeEngine,
			StoreLogic:        storeLogic,
			StoreService:      storeService,
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
	slog.InfoContext(ctx, "init logger done")
	return serviceLogger, nil
}

func GetServiceContext() *ServiceContext {
	return gServiceCtx
}

func prepareDir(ctx context.Context, dirPath string) (string, error) {
	var fullPath string

	isAbs := filepath.IsAbs(dirPath)
	if !isAbs {
		exePath, err := os.Executable()
		if err != nil {
			slog.ErrorContext(ctx, "cannot get current executale command", slog.Any("error", err))
			return "", err
		}
		fullPath = filepath.Join(filepath.Dir(exePath), dirPath)
	} else {
		fullPath = dirPath
	}

	err := ensureDirExists(ctx, fullPath, 0755)
	if err != nil {
		slog.ErrorContext(ctx, "ensure dir existeds failed", slog.String("fullPath", fullPath), slog.String("permission", fmt.Sprintf("%#o", 0775)), slog.Any("error", err))
		return "", err
	}

	realPath, err := filepath.EvalSymlinks(fullPath)
	if err != nil {
		slog.ErrorContext(ctx, "eval symlinks failed", slog.Any("error", err))
		return "", err
	}

	return realPath, err
}

// ensureDirExists
// 1. 路径不存在：递归创建多级目录
// 2. 路径存在但不是目录：返回冲突错误
// 3. 任意步骤权限不足：返回权限错误
// 4. 其他系统错误原样返回
func ensureDirExists(ctx context.Context, dirPath string, perm os.FileMode) error {
	statInfo, err := os.Stat(dirPath)
	if err != nil {
		// 情况1：目录完全不存在 → 执行创建
		if os.IsNotExist(err) {
			errCreate := os.MkdirAll(dirPath, perm)
			if errCreate != nil {
				// 创建失败，判断是否权限问题
				if os.IsPermission(errCreate) {
					slog.ErrorContext(ctx, "permission error when creating path", slog.String("path", dirPath), slog.Any("error", errCreate))
					return fmt.Errorf("permission error when creating path: %s", dirPath)
				}
				slog.ErrorContext(ctx, "creating path failed", slog.String("path", dirPath), slog.Any("error", errCreate))
				return fmt.Errorf("mkdir all failed: %w", errCreate)
			}
			return nil
		}

		// 情况2：Stat 就权限不足（目录上层无访问权限）
		if os.IsPermission(err) {
			slog.ErrorContext(ctx, "permission error when accessing path", slog.String("path", dirPath), slog.Any("error", err))
			return fmt.Errorf("permission error when accessing path: %s", dirPath)
		}

		// 情况3：其他未知错误（磁盘损坏、路径非法等）
		slog.ErrorContext(ctx, "stat path failed", slog.String("dir", dirPath), slog.Any("error", err))
		return fmt.Errorf("stat path failed: %w", err)
	}

	// 路径已存在，但不是文件夹（是文件/软链接）
	if !statInfo.IsDir() {
		slog.ErrorContext(ctx, "path existed, but not a dir", slog.String("dir", dirPath))
		return fmt.Errorf("path existed, but not a dir: %s", dirPath)
	}

	return nil
}
