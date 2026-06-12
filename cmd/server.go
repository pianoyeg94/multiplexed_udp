package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/pianoyeg94/multiplexed_udp/pkg/app"
	"github.com/pianoyeg94/multiplexed_udp/pkg/server"
)

var (
	port             int
	serverWindowSize uint16

	serverCmd = &cobra.Command{
		Use:   "serve",
		Long:  "Run multiplexed udp server",
		Short: "Run multiplexed udp server",
		RunE: func(cmd *cobra.Command, args []string) error {
			go func() { cmd.Parent().RunE(cmd, args) }() // run pprof server

			logger := cmd.Context().Value(LoggerKey).(*zap.Logger)
			ctxCancel := cmd.Context().Value(ContextCancelFuncKey).(context.CancelFunc)
			defer ctxCancel()
			_ = logger

			errsCh := make(chan error, 1)
			defer func() { close(errsCh) }()
			srvr := server.NewServer(port, serverWindowSize, logger)
			go func() { errsCh <- srvr.ListenAndServe(server.HandleFunc(app.HandleServerMessage)) }()

			select {
			case err := <-errsCh:
				fmt.Printf("Got error from server %#v\n", err)
			case <-cmd.Context().Done():
			}
			// return srvr.Close()
			return nil
		},
	}
)

func init() {
	serverCmd.Flags().IntVar(&port, "port", 8080, "Server port")
	serverCmd.Flags().Uint16Var(&serverWindowSize, "window-size", 1<<16-1, "Single stream receive window size in bytes")
	rootCmd.AddCommand(serverCmd)
}
