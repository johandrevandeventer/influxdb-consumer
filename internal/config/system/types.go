package system

// ======================== System ======================== //

type SystemConfig struct {
	AppName      string   `mapstructure:"app_name" yaml:"app_name"`
	AppVersion   string   `mapstructure:"app_version" yaml:"app_version"`
	ReleaseDate  string   `mapstructure:"release_date" yaml:"release_date"`
	Contributors []string `mapstructure:"contributors" yaml:"contributors"`
}
