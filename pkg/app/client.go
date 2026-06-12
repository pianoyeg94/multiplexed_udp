package app

import (
	"context"
	"math/rand/v2"
	"sync"

	"github.com/pianoyeg94/multiplexed_udp/pkg/client"
	"go.uber.org/zap"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

type Client struct {
	client *client.Client
}

func NewClient(remoteAddr string, remotePort int, windowSize uint16, logger *zap.Logger) *Client {
	return &Client{
		client: client.NewClient(remoteAddr, remotePort, windowSize, logger),
	}
}

func (c *Client) Run(ctx context.Context) error {
	if err := c.client.Connect(); err != nil {
		return err
	}

	sequenceNumberCh := make(chan int, 10)
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				sequenceNumber, ok := <-sequenceNumberCh
				if !ok {
					return
				}
				c.client.Send(sequenceNumber, []byte(generateRandomString(10000)))
			}

		}()
	}

	for sequenceNumber := range 100000 {
		sequenceNumberCh <- sequenceNumber
	}
	close(sequenceNumberCh)
	wg.Wait()

	<-ctx.Done()

	return nil
}

func generateRandomString(size int) string {
	bytes := make([]byte, size)
	for i := range bytes {
		bytes[i] = charset[rand.IntN(len(charset))]
	}
	return string(bytes)
}
