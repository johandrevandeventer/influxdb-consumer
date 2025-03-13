/*
Copyright © 2025 Johandré van Deventer <johandre.vandeventer@rubiconsa.com>
*/
package cmd

import (
	"os"

	"github.com/johandrevandeventer/influxdb-consumer/internal/config"
	"github.com/johandrevandeventer/influxdb-consumer/internal/flags"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   RootCmdUse,
	Short: RootCmdShort,
	Long:  RootCmdLong,

	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if cmd.CalledAs() == RootCmdUse {
			config.PrintInfo(false)
		} else {
			config.PrintInfo(true)
			os.Exit(0)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		// cmd.Help()
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}

	// Exit if the help flag is set
	helpFlag, _ := rootCmd.Flags().GetBool("help")
	if helpFlag {
		os.Exit(0)
	}
}

func init() {
	// Define persistent flags for the root command
	rootCmd.PersistentFlags().StringVarP(&flags.FlagEnvironment, "environment", "e", "development", "Environment to run the application in (e.g. development, production) (default development)")
	rootCmd.PersistentFlags().BoolVarP(&flags.FlagDebugMode, "debug", "x", false, "Enable debug mode (default false)")
	rootCmd.PersistentFlags().BoolVarP(&flags.FlagVerbose, "verbose", "v", false, "Log verbose output (default false)")
	rootCmd.PersistentFlags().BoolVar(&flags.FlagLogPrefix, "log-prefix", true, "Add timestamps to logs and subprocess stderr/stdout output")
	rootCmd.PersistentFlags().BoolVar(&flags.FlagKafkaLogging, "kafka-logging", false, "Enable Kafka logging (default false)")
	rootCmd.PersistentFlags().BoolVar(&flags.FlagWorkersLogging, "workers-logging", false, "Enable workers logging (default false)")
	rootCmd.PersistentFlags().BoolVar(&flags.FlagInfluxDBLogging, "influxdb-logging", false, "Enable InfluxDB logging (default false)")
}
