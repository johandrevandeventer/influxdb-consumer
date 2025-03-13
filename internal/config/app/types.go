package app

// ======================== App ======================== //

type AppConfig struct {
	Runtime  RuntimeConfig  `mapstructure:"runtime" yaml:"runtime"`
	Logging  LoggingConfig  `mapstructure:"logging" yaml:"logging"`
	InfluxDB InfluxDBConfig `mapstructure:"influxdb" yaml:"influxdb"`
}

type RuntimeConfig struct {
	RootDir                string `mapstructure:"root_dir" yaml:"root_dir"`
	TmpDir                 string `mapstructure:"tmp_dir" yaml:"tmp_dir"`
	PersistFilePath        string `mapstructure:"persist_file_path" yaml:"persist_file_path"`
	StopFileFilepath       string `mapstructure:"stop_file_filepath" yaml:"stop_file_filepath"`
	ConnectionsLogFilePath string `mapstructure:"connections_log_file_path" yaml:"connections_log_file_path"`
}

type LoggingConfig struct {
	Level      string `mapstructure:"level" yaml:"level"`
	FilePath   string `mapstructure:"file_path" yaml:"file_path"`
	MaxSize    int    `mapstructure:"max_size" yaml:"max_size"`
	MaxBackups int    `mapstructure:"max_backups" yaml:"max_backups"`
	MaxAge     int    `mapstructure:"max_age" yaml:"max_age"`
	Compress   bool   `mapstructure:"compress" yaml:"compress"`
	AddTime    bool   `mapstructure:"add_time" yaml:"add_time"`
}

type InfluxDBConfig struct {
	URL    string `mapstructure:"url" yaml:"url"`
	Org    string `mapstructure:"org" yaml:"org"`
	Bucket string `mapstructure:"bucket" yaml:"bucket"`
	Token  string `mapstructure:"token" yaml:"token"`
}
