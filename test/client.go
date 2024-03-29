
package main


import "../fdlib"

import "fmt"
import "os"
import "time"

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
	if checkError(err) != nil {
		return
	}

	defer fd.StopMonitoring()
	defer fd.StopResponding()

	err = fd.StartResponding(localIpPort)
	if checkError(err) != nil {
		return
	}

	fmt.Println("Started responding to heartbeats.")

	// Add a monitor for a remote node.
	localIpPortMon := "127.0.0.1:9001"
	err = fd.AddMonitor(localIpPortMon, localIpPort, lostMsgThresh)
	if checkError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node: ", toMonitorIpPort)

	// Wait indefinitely, blocking on the notify channel, to detect a
	// failure.
	done := time.NewTicker(60*time.Second)
	for {
		select {
		case notify := <-notifyCh:
			fmt.Println("Failed - Detected a failure of", notify)
			return

		case <- done.C:
			done.Stop()
			fmt.Println("Success")
			return
		}
	}

}

// If error is non-nil, print it out and return it.
func checkError(err error) error {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}