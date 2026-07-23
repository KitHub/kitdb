package config

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.yaml.in/yaml/v3"
)

var defaultConfigFile string = "./server.yaml"
var gConfigFile string = defaultConfigFile
var configFileLock sync.RWMutex = sync.RWMutex{}
var gConfigEntity ConfigEntity = ConfigEntity{}
var configReadWriteLock sync.RWMutex = sync.RWMutex{}
var refreshInterval time.Duration = time.Duration(30) * time.Second
var refreshIntervalLock sync.RWMutex = sync.RWMutex{}
var timer *time.Timer = time.NewTimer(refreshInterval)
var onceForReloadConfig sync.Once = sync.Once{}

func setConfigFile(file string) {
	configFileLock.Lock()
	gConfigFile = file
	configFileLock.Unlock()
}

func getConfigFile() string {
	configFileLock.RLock()
	defer configFileLock.RUnlock()
	return gConfigFile
}

func setRefreshInterval(interval time.Duration) {
	refreshIntervalLock.Lock()
	refreshInterval = interval
	refreshIntervalLock.Unlock()
}

func getRefreshInterval() time.Duration {
	refreshIntervalLock.RLock()
	defer refreshIntervalLock.RUnlock()
	return refreshInterval
}

func setGConfigEntity(config ConfigEntity) {
	configReadWriteLock.Lock()
	gConfigEntity = config
	configReadWriteLock.Unlock()
}

func getGConfigEntity() ConfigEntity {
	configReadWriteLock.RLock()
	defer configReadWriteLock.RUnlock()
	return gConfigEntity
}

func SetRefreshInterval(interval time.Duration) {
	setRefreshInterval(interval)
}

func loadConfigInternal(ctx context.Context, configFile string) (ConfigEntity, error) {
	dataBytes, err := os.ReadFile(configFile)
	if err != nil {
		slog.ErrorContext(ctx, "read config failed",
			slog.String("configFile", configFile), slog.Any("err", err))
		return ConfigEntity{}, err
	}

	config := ConfigEntity{}
	err = yaml.Unmarshal(dataBytes, &config)
	if err != nil {
		slog.ErrorContext(ctx, "parse config failed",
			slog.String("configFile", configFile), slog.Any("err", err))
		return ConfigEntity{}, err
	}
	configMarshalBytes, err := yaml.Marshal(config)
	if err != nil {
		slog.ErrorContext(ctx, "marshal config failed",
			slog.String("configFile", configFile), slog.Any("err", err))
		return ConfigEntity{}, err
	}
	slog.InfoContext(ctx, "load config success",
		slog.String("configFile", configFile),
		slog.String("config", string(configMarshalBytes)))
	return config, nil
}

// LoadConfig loads the config file and returns the ConfigEntity struct.
// Return the config entity and error if any, otherwise return nil error.
// The config file is expected to be in yaml format.
// After loading the config, it will set the global config entity and start a
// goroutine to reload the config every refreshInterval.
func LoadConfig(ctx context.Context, configFile string) (ConfigEntity, error) {
	if configFile == "" {
		configFile = defaultConfigFile
		slog.WarnContext(ctx, "config file is not specified, using default config file", slog.String("configFile", configFile))
	} else {
		slog.InfoContext(ctx, "using specified config file", slog.String("configFile", configFile))
		setConfigFile(configFile)
	}

	config, err := loadConfigInternal(ctx, configFile)
	if err != nil {
		return ConfigEntity{}, err
	}
	setGConfigEntity(config)

	// start a goroutine to reload config every refreshInterval
	onceForReloadConfig.Do(func() {
		go func() {
			for {
				timer.Reset(getRefreshInterval())
				tmpConfigFile := getConfigFile()
				slog.InfoContext(context.Background(),
					"start to reload config",
					slog.String("configFile", tmpConfigFile),
					slog.Duration("refreshInterval", getRefreshInterval()))
				<-timer.C
				_, err := loadConfigInternal(context.Background(), tmpConfigFile)
				if err != nil {
					slog.ErrorContext(context.Background(),
						"reload config failed",
						slog.String("configFile", tmpConfigFile),
						slog.Any("err", err))
				}
			}
		}()
	})

	return config, nil
}

func GetConfig(ctx context.Context) ConfigEntity {
	return getGConfigEntity()
}
