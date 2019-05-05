
package main


import "../fdlib"

import "fmt"
import "time"
import "./common"

// Run a client. Assumes muiltiple other node is already setup and one failes
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
	failures := 0
	for {
		select {
		case notify := <-notifyCh:
			fmt.Println("Detected a failure of", notify)
			failures += 1

		case <- done.C:
			done.Stop()
			if failures == 1 {
				fmt.Println("Success")
			}else {
				fmt.Println("Failure")
			}
			return
		}
	}

}

