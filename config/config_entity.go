package config

type ServiceConfigEntity struct {
	Host string `yaml:"host"` // 服务器主机地址
	Port int    `yaml:"port"` // 服务器端口号
}

type ServerConfigEntity struct {
	Name        string               `yaml:"name"`         // 服务名称
	GrpcService *ServiceConfigEntity `yaml:"grpc_service"` // gRPC 服务器配置
	HttpService *ServiceConfigEntity `yaml:"http_service"` // HTTP 服务器配置
}

type DBConfigEntity struct {
	// 驱动名称,如：mysql、mssql等，参见 https://github.com/xormplus/xorm/blob/master/dialects/dialect.go#L196
	DriverName      string `yaml:"driver_name"`
	DataSourceName  string `yaml:"data_source_name"`  // 数据库地址
	MaxIdleConns    int    `yaml:"max_idle_conns"`    // 最大空闲连接数
	MaxOpenConns    int    `yaml:"max_open_conns"`    // 最大连接数
	ConnMaxLifetime int    `yaml:"conn_max_lifetime"` // 连接最大生命周期（以秒为单位）
}

type LogConfigEntity struct {
	LogLevel   string `yaml:"log_level"`   // 日志级别（如：debug、info、warn、error）
	Filename   string `yaml:"filename"`    // 日志文件的位置
	MaxSize    int    `yaml:"max_size"`    // 文件最大尺寸（以MB为单位）
	MaxBackups int    `yaml:"max_backups"` // 保留的最大旧文件数量
	MaxAge     int    `yaml:"max_age"`     // 保留旧文件的最大天数
	Compress   bool   `yaml:"compress"`    // 是否压缩/归档旧文件
	LocalTime  bool   `yaml:"local_time"`  // 使用本地时间创建时间戳
}

type TraceConfigEntity struct {
	ExporterEndpoint string  `yaml:"exporter_endpoint"` // OpenTelemetry Collector 的地址
	SamplerType      string  `yaml:"sampler_type"`      // 采样器类型（如 parentbased_traceidratio）
	SamplerRatio     float64 `yaml:"sampler_ratio"`     // 采样器参数（如采样率）
}

type ConfigEntity struct {
	LogConfig    *LogConfigEntity    `yaml:"log"`
	DBConfig     *DBConfigEntity     `yaml:"db"`
	ServerConfig *ServerConfigEntity `yaml:"server"`
	TraceConfig  *TraceConfigEntity  `yaml:"trace"`
}
