
package main


import "../fdlib"

import "fmt"
import "time"
import "./common"

// Run a client. Assumes another node is already setup
func main() {
	// Local (127.0.0.1) hardcoded IPs to simplify testing.
	localIpPort := "127.0.0.1:8080"
	toMonitorIpPort := "127.0.0.1:9000" // TODO: change this to remote node
	var lostMsgThresh uint8 = 5

	var epochNonce uint64 = 12345
	var chCapacity uint8 = 5

	// Initialize fdlib. Note the use of multiple assignment:
	// https://gobyexample.com/multiple-return-values
	fd, notifyCh, err := fdlib.Initialize(epochNonce, chCapacity)
	if common.CheckError(err) != nil {
		return
	}

	defer fd.StopMonitoring()

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

	fmt.Println("Started to monitor node: ", toMonitorIpPort)

	// Wait indefinitely, blocking on the notify channel, to detect a
	// failure.
	done := time.NewTicker(10*time.Second)
	restartMonitor := time.NewTicker(2*time.Second)
	failures := 0
	for {
		select {
		case notify := <-notifyCh:
			failures += 1
			fmt.Println("Detected a failure of", notify)

		case <- restartMonitor.C:
			restartMonitor.Stop()
			err = fd.AddMonitor(localIpPortMon, localIpPort, 0)
			if common.CheckError(err) != nil {
				fmt.Println("Problem re-adding monitor")
				return
			}
			fmt.Println("Started to monitor node: ", toMonitorIpPort)

		case <- done.C:
			done.Stop()
			if failures == 1 {
				fmt.Println("Success")
			}
			return
		}
	}

}
