/*

A trivial application to illustrate how the fdlib library can be used
in assignment 1 for UBC CS 416 2018W1.

Usage:
go run client.go
*/

package main

// Expects fdlib.go to be in the ./fdlib/ dir, relative to
// this client.go file
import (
	"../fdlib"
)

import "fmt"
import "time"
import "./common"


// Start monitoring server and detect a failure when server shuts down
func main() {
	// Local (127.0.0.1) hardcoded IPs to simplify testing.
	localIpPort := "127.0.0.1:8080"
	var lostMsgThresh uint8 = 3

	// TODO: generate a new random epoch nonce on each run
	var epochNonce uint64 = 12345
	var chCapacity uint8 = 5


	fd, notifyCh, err := fdlib.Initialize(epochNonce, chCapacity)
	if common.CheckError(err) != nil {
		return
	}

	// Stop monitoring and stop responding on exit.
	// Defers are really cool, check out: https://blog.golang.org/defer-panic-and-recover
	defer fd.StopMonitoring()
	//defer fd.StopResponding()

	err = fd.StartResponding(localIpPort)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started responding to heartbeats.")

	// Add a monitor for a remote node.
	localIpPortMon := "127.0.0.1:9001"
	err = fd.AddMonitor(localIpPortMon, localIpPort, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node: ", localIpPort)

	// Wait indefinitely, blocking on the notify channel, to detect a
	// failure.
	stopRespondingTicker := time.NewTicker(5*time.Second)
	for {
		select {
		case notify := <-notifyCh:
			fmt.Println("Success - Detected a failure of", notify)
			return
		case <-stopRespondingTicker.C:
			stopRespondingTicker.Stop()
			fd.StopResponding()
		}
	}

}

