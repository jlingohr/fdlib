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
	"./common"

)

import "fmt"
import "time"


// Multiple monitors on the same node
// Should stop when Remove monitor called
func main() {
	// Local (127.0.0.1) hardcoded IPs to simplify testing.
	localIpPort := "127.0.0.1:8080"
	localIpPortB := "127.0.0.1:8081"
	toMonitorIpPort := "127.0.0.1:9000" // TODO: change this to remote node
	var lostMsgThresh uint8 = 5

	// TODO: generate a new random epoch nonce on each run
	var epochNonce uint64 = 12345
	var chCapacity uint8 = 5

	// Initialize fdlib. Note the use of multiple assignment:
	// https://gobyexample.com/multiple-return-values
	fd, notifyCh, err := fdlib.Initialize(epochNonce, chCapacity)
	if common.CheckError(err) != nil {
		return
	}

	// Stop monitoring and stop responding on exit.
	// Defers are really cool, check out: https://blog.golang.org/defer-panic-and-recover
	defer fd.StopMonitoring()
	defer fd.StopResponding()

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

	// Add a monitor for a remote node.
	localIpPortMonB := "127.0.0.1:9002"
	err = fd.AddMonitor(localIpPortMonB, localIpPortB, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node: ", localIpPortB)

	// Wait indefinitely, blocking on the notify channel, to detect a
	// failure.
	for {
		select {
		case notify := <-notifyCh:
			fmt.Println("Detected a failure of", notify)
			// Re-add the remote node for monitoring.
			err := fd.AddMonitor(localIpPortMon, localIpPort, lostMsgThresh)
			if common.CheckError(err) != nil {
				return
			}
			fmt.Println("Started to monitor node: ", toMonitorIpPort)
		case <-time.After(5 * time.Second):
			// case <-time.After(time.Second):
			fmt.Println("No failures detected")
			fd.RemoveMonitor(localIpPort)
		}
	}

}


//func common.CheckError(err error) error {
//	if err != nil {
//		fmt.Fprintf(os.Stderr, "Error ", err.Error())
//		return err
//	}
//	return nil
//}