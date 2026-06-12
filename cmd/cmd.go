package cmd

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	ContextCancelFunc string
	Logger            string
)

const (
	ContextCancelFuncKey ContextCancelFunc = "ctxCancel"
	LoggerKey            Logger            = "logger"
)

var (
	pprofServerPort string

	rootCmd = &cobra.Command{
		Use:           "udp [command]",
		Long:          "UDP",
		Short:         "UDP",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := cmd.Context().Value(LoggerKey).(*zap.Logger)
			logger.Info("Starting pprof server on :" + pprofServerPort)
			logger.Info("pprof is available at http://localhost:" + pprofServerPort + "/debug/pprof/")
			return http.ListenAndServe(":"+pprofServerPort, nil)
		},
	}
)

func Execute() error {
	rootCmd.PersistentFlags().StringVar(&pprofServerPort, "pprof-server-port", "6060", "pprof server port")

	signalContext, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx := context.WithValue(signalContext, ContextCancelFuncKey, cancel)

	logLevel := zap.NewAtomicLevelAt(zapcore.InfoLevel)
	logFonfig := zap.NewProductionConfig()
	logFonfig.Level = logLevel
	logger, err := logFonfig.Build()
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, LoggerKey, logger)

	return rootCmd.ExecuteContext(ctx)
}
