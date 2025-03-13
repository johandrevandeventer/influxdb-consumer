package flags

// Default pattern to match files which trigger a build
const FilePattern = `(.+\.go|.+\.c)$`

var (
	FlagEnvironment     string
	FlagDebugMode       bool
	FlagLogPrefix       bool
	FlagVerbose         bool
	FlagKafkaLogging    bool
	FlagWorkersLogging  bool
	FlagInfluxDBLogging bool
)
