package cmd

import (
	"context"
	"fmt"

	"github.com/pianoyeg94/multiplexed_udp/pkg/app"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	remoteAddr       string
	remotePort       int
	clientWindowSize uint16

	clientCmd = &cobra.Command{
		Use:   "client",
		Long:  "Run multiplexed udp server",
		Short: "Run multiplexed udp server",
		RunE: func(cmd *cobra.Command, args []string) error {
			go func() { cmd.Parent().RunE(cmd, args) }() // run pprof server

			logger := cmd.Context().Value(LoggerKey).(*zap.Logger)
			ctxCancel := cmd.Context().Value(ContextCancelFuncKey).(context.CancelFunc)
			defer ctxCancel()
			_ = logger

			clnt := app.NewClient(remoteAddr, remotePort, clientWindowSize, logger)
			errsCh := make(chan error, 1)
			defer func() { close(errsCh) }()
			go func() { errsCh <- clnt.Run(cmd.Context()) }()

			select {
			case err := <-errsCh:
				fmt.Printf("Got error from client %#v\n", err)
				return err
			case <-cmd.Context().Done():
			}
			return nil
		},
	}
)

func init() {
	clientCmd.Flags().StringVar(&remoteAddr, "remote-addr", "127.0.0.1", "Remote address")
	clientCmd.Flags().IntVar(&remotePort, "remote-port", 8080, "Remote port")
	clientCmd.Flags().Uint16Var(&clientWindowSize, "window-size", 1<<16-1, "Single stream receive window size in bytes")
	rootCmd.AddCommand(clientCmd)
}
