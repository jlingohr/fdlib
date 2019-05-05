
package main


import "../fdlib"

import "fmt"
import "time"
import "./common"

// Run a client. Assumes another node is already setup
func main() {
	// Local (127.0.0.1) hardcoded IPs to simplify testing.
	toMonitorIpPortA := "127.0.0.1:9000"
	toMonitorIpPortB := "127.0.0.1:9001"
	toMonitorIpPortC := "127.0.0.1:9002"
	toMonitorIpPortD := "127.0.0.1:9003"
	toMonitorIpPortE := "127.0.0.1:9004"
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


	// Add a monitor for a remote node.
	localIpPortMon := "127.0.0.1:8080"
	err = fd.AddMonitor(localIpPortMon, toMonitorIpPortA, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node: ", toMonitorIpPortA)

	err = fd.AddMonitor(localIpPortMon, toMonitorIpPortB, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node: ", toMonitorIpPortB)

	err = fd.AddMonitor(localIpPortMon, toMonitorIpPortC, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node: ", toMonitorIpPortC)

	err = fd.AddMonitor(localIpPortMon, toMonitorIpPortD, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node: ", toMonitorIpPortD)

	err = fd.AddMonitor(localIpPortMon, toMonitorIpPortE, lostMsgThresh)
	if common.CheckError(err) != nil {
		return
	}

	fmt.Println("Started to monitor node: ", toMonitorIpPortE)


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

