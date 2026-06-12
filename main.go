package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"runtime/debug"
	"time"

	"github.com/pkg/errors"

	"github.com/pianoyeg94/multiplexed_udp/cmd"
)

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func init() {
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Lshortfile | log.LUTC)
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(stackTracer); ok {
				for _, f := range err.StackTrace() {
					log.Printf("%v | func %n()\n", f, f)
				}
			}
			log.Fatalf("Panic recovered: %v\nStacktrace: %s\n", r, string(debug.Stack()))
		}
	}()

	if err := cmd.Execute(); err != nil {
		if err, ok := err.(stackTracer); ok {
			for _, f := range err.StackTrace() {
				log.Printf("%v | func %n()\n", f, f)
			}
		}
		log.Println(err.Error())
	}
}

func server() {
	addr, err := net.ResolveUDPAddr("udp", ":8080")
	if err != nil {
		fmt.Printf("Failed to resolve address: %v\n", err)
		os.Exit(1)
	}

	// 2. Open the UDP connection socket to listen on the address
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("Failed to listen: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("UDP Server listening on :8080...")

	// Allocate a buffer to temporarily store incoming packet data
	buffer := make([]byte, 2048)

	for {
		// 3. Read incoming packets (Blocks until data arrives)
		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Printf("Error reading packet: %v\n", err)
			continue // Skip to next packet on error
		}

		// Process data safely up to 'n' bytes read
		message := buffer[:n]
		fmt.Printf("Received %d bytes from %s: %s\n", n, remoteAddr, string(message))

		// 4. Send a response back to the client's remote address
		response := append([]byte("Echo: "), message...)
		_, err = conn.WriteToUDP(response, remoteAddr)
		if err != nil {
			fmt.Printf("Failed to send reply to %s: %v\n", remoteAddr, err)
		}
	}
}

func client() {
	// Define the remote UDP server address
	serverAddrStr := "127.0.0.1:8080"

	// 1. Resolve the string address into a UDP address structure
	raddr, err := net.ResolveUDPAddr("udp", serverAddrStr)
	if err != nil {
		fmt.Printf("Failed to resolve remote address: %v\n", err)
		return
	}

	// 2. Connect to the UDP server.
	// Because UDP is connectionless, DialUDP doesn't perform a handshake;
	// it simply registers the remote address for the socket.
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		fmt.Printf("Failed to dial UDP server: %v\n", err)
		return
	}
	defer conn.Close()

	// 3. Send data to the server
	message := []byte("Hello from Go UDP Client!")
	_, err = conn.Write(message)
	if err != nil {
		fmt.Printf("Failed to send data: %v\n", err)
		return
	}
	fmt.Printf("Sent to %s: %s\n", serverAddrStr, string(message))

	// 4. Set a read deadline (Crucial for UDP since packets can drop silently)
	err = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if err != nil {
		fmt.Printf("Failed to set read deadline: %v\n", err)
		return
	}

	// 5. Read the response from the server
	buffer := make([]byte, 1024)
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		// Checks if the error was due to the timeout deadline expiring
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			fmt.Println("Error: Read timed out. Server didn't respond.")
		} else {
			fmt.Printf("Failed to read data: %v\n", err)
		}
		return
	}

	fmt.Printf("Received reply: %s\n", string(buffer[:n]))
}
